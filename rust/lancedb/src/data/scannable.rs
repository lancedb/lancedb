// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Data source abstraction for LanceDB.
//!
//! This module provides a [`Scannable`] trait that allows input data sources to express
//! capabilities (row count, rescannability) so the insert pipeline can make
//! better decisions about write parallelism and retry strategies.

use std::sync::Arc;

use arrow_array::RecordBatchIterator;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::once;
use futures::StreamExt;
use lance_datafusion::utils::StreamingWriteSource;

use crate::arrow::{
    SendableRecordBatchStream, SendableRecordBatchStreamExt, SimpleRecordBatchStream,
};
use crate::embeddings::{
    compute_embeddings_for_batch, compute_output_schema, EmbeddingDefinition, EmbeddingFunction,
    EmbeddingRegistry,
};
use crate::table::{ColumnKind, TableDefinition};
use crate::{Error, Result};

pub trait Scannable: Send {
    /// Returns the schema of the data.
    fn schema(&self) -> SchemaRef;

    /// Read data as a stream of record batches.
    ///
    /// For rescannable sources (in-memory data like RecordBatch, Vec<RecordBatch>),
    /// this can be called multiple times and returns cloned data each time.
    ///
    /// For non-rescannable sources (streams, readers), the first call returns data
    /// and subsequent calls return an empty stream.
    fn scan_as_stream(&mut self) -> SendableRecordBatchStream;

    /// Optional hint about the number of rows.
    ///
    /// When available, this allows the pipeline to estimate total data size
    /// and choose appropriate partitioning.
    fn num_rows(&self) -> Option<usize> {
        None
    }

    /// Whether the source can be re-read from the beginning.
    ///
    /// `true` for in-memory data (Tables, DataFrames) and disk-based sources (Datasets).
    /// `false` for streaming sources (DuckDB results, network streams).
    ///
    /// When true, the pipeline can retry failed writes by rescanning.
    fn rescannable(&self) -> bool {
        false
    }
}

impl std::fmt::Debug for dyn Scannable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scannable")
            .field("schema", &self.schema())
            .field("num_rows", &self.num_rows())
            .field("rescannable", &self.rescannable())
            .finish()
    }
}

impl Scannable for RecordBatch {
    fn schema(&self) -> SchemaRef {
        Self::schema(self)
    }

    fn scan_as_stream(&mut self) -> SendableRecordBatchStream {
        let batch = self.clone();
        let schema = batch.schema();
        Box::pin(SimpleRecordBatchStream {
            schema,
            stream: once(async move { Ok(batch) }),
        })
    }

    fn num_rows(&self) -> Option<usize> {
        Some(Self::num_rows(self))
    }

    fn rescannable(&self) -> bool {
        true
    }
}

impl Scannable for Vec<RecordBatch> {
    fn schema(&self) -> SchemaRef {
        if self.is_empty() {
            Arc::new(arrow_schema::Schema::empty())
        } else {
            self[0].schema()
        }
    }

    fn scan_as_stream(&mut self) -> SendableRecordBatchStream {
        let schema = Scannable::schema(self);
        let batches = self.clone();
        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        Box::pin(SimpleRecordBatchStream { schema, stream })
    }

    fn num_rows(&self) -> Option<usize> {
        Some(self.iter().map(|b| b.num_rows()).sum())
    }

    fn rescannable(&self) -> bool {
        true
    }
}

impl Scannable for Box<dyn RecordBatchReader + Send> {
    fn schema(&self) -> SchemaRef {
        RecordBatchReader::schema(self.as_ref())
    }

    fn scan_as_stream(&mut self) -> SendableRecordBatchStream {
        let schema = RecordBatchReader::schema(self.as_ref());
        let empty_reader: Box<dyn RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(std::iter::empty(), schema.clone()));
        let reader = std::mem::replace(self, empty_reader);

        // Use a channel to bridge blocking RecordBatchReader to async stream.
        // Buffer size of 2 provides some pipelining while limiting memory use.
        let (tx, rx) = tokio::sync::mpsc::channel::<crate::Result<RecordBatch>>(2);

        // Spawn blocking task to read from the reader
        tokio::task::spawn_blocking(move || {
            let mut reader = reader;
            for batch_result in reader.by_ref() {
                let result = batch_result.map_err(Into::into);
                // If receiver is dropped, stop reading
                if tx.blocking_send(result).is_err() {
                    break;
                }
            }
        });

        // Convert the receiver into a stream using unfold
        let stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|batch| (batch, rx))
        });

        Box::pin(SimpleRecordBatchStream { schema, stream })
    }

    fn num_rows(&self) -> Option<usize> {
        None
    }

    fn rescannable(&self) -> bool {
        false
    }
}

impl Scannable for SendableRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.as_ref().schema()
    }

    fn scan_as_stream(&mut self) -> SendableRecordBatchStream {
        let schema = self.as_ref().schema();
        let empty_stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream {
            schema: schema.clone(),
            stream: futures::stream::empty(),
        });
        std::mem::replace(self, empty_stream)
    }

    fn num_rows(&self) -> Option<usize> {
        None
    }

    fn rescannable(&self) -> bool {
        false
    }
}

#[async_trait]
impl StreamingWriteSource for Box<dyn Scannable> {
    fn arrow_schema(&self) -> SchemaRef {
        self.schema()
    }

    fn into_stream(mut self) -> datafusion_physical_plan::SendableRecordBatchStream {
        self.scan_as_stream().into_df_stream()
    }
}

/// A scannable that applies embeddings to the stream.
pub struct WithEmbeddingsScannable {
    inner: Box<dyn Scannable>,
    embeddings: Vec<(EmbeddingDefinition, Arc<dyn EmbeddingFunction>)>,
    output_schema: SchemaRef,
}

impl WithEmbeddingsScannable {
    /// Create a new WithEmbeddingsScannable.
    ///
    /// The embeddings are applied to the inner scannable's data as new columns.
    pub fn try_new(
        inner: Box<dyn Scannable>,
        embeddings: Vec<(EmbeddingDefinition, Arc<dyn EmbeddingFunction>)>,
    ) -> Result<Self> {
        let output_schema = compute_output_schema(&inner.schema(), &embeddings)?;
        Ok(Self {
            inner,
            embeddings,
            output_schema,
        })
    }
}

impl Scannable for WithEmbeddingsScannable {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn scan_as_stream(&mut self) -> SendableRecordBatchStream {
        let inner_stream = self.inner.scan_as_stream();
        let embeddings = self.embeddings.clone();
        let output_schema = self.output_schema.clone();

        let mapped_stream = inner_stream.then(move |batch_result| {
            let embeddings = embeddings.clone();
            async move {
                let batch = batch_result?;
                // Run embedding computation in a blocking task to avoid blocking async runtime
                let result = tokio::task::spawn_blocking(move || {
                    compute_embeddings_for_batch(batch, &embeddings)
                })
                .await
                .map_err(|e| Error::Runtime {
                    message: format!("Task panicked during embedding computation: {}", e),
                })??;
                Ok(result)
            }
        });

        Box::pin(SimpleRecordBatchStream {
            schema: output_schema,
            stream: mapped_stream,
        })
    }

    fn num_rows(&self) -> Option<usize> {
        self.inner.num_rows()
    }

    fn rescannable(&self) -> bool {
        self.inner.rescannable()
    }
}

/// A scannable that might have embeddings applied to it.
pub enum MaybeEmbeddedScannable {
    /// Embeddings are applied to the scannable
    Yes(WithEmbeddingsScannable),
    /// No embeddings, passthrough to inner scannable
    No(Box<dyn Scannable>),
}

impl MaybeEmbeddedScannable {
    /// Create a new MaybeEmbeddedScannable.
    ///
    /// If the table definition specifies embedding columns and the registry contains
    /// the required embedding functions, embeddings will be applied. Otherwise, this
    /// is a no-op and the inner scannable is returned as-is.
    pub fn try_new(
        inner: Box<dyn Scannable>,
        table_definition: &TableDefinition,
        registry: Option<&Arc<dyn EmbeddingRegistry>>,
    ) -> Result<Self> {
        if let Some(registry) = registry {
            let mut embeddings = Vec::with_capacity(table_definition.column_definitions.len());
            for cd in table_definition.column_definitions.iter() {
                if let ColumnKind::Embedding(embedding_def) = &cd.kind {
                    match registry.get(&embedding_def.embedding_name) {
                        Some(func) => {
                            embeddings.push((embedding_def.clone(), func));
                        }
                        None => {
                            return Err(Error::EmbeddingFunctionNotFound {
                                name: embedding_def.embedding_name.clone(),
                                reason: format!(
                                    "Table was defined with an embedding column `{}` but no embedding function was found with that name within the registry.",
                                    embedding_def.embedding_name
                                ),
                            });
                        }
                    }
                }
            }

            if !embeddings.is_empty() {
                return Ok(Self::Yes(WithEmbeddingsScannable::try_new(
                    inner, embeddings,
                )?));
            }
        }

        Ok(Self::No(inner))
    }
}

impl Scannable for MaybeEmbeddedScannable {
    fn schema(&self) -> SchemaRef {
        match self {
            Self::Yes(inner) => inner.schema(),
            Self::No(inner) => inner.schema(),
        }
    }

    fn scan_as_stream(&mut self) -> SendableRecordBatchStream {
        match self {
            Self::Yes(inner) => inner.scan_as_stream(),
            Self::No(inner) => inner.scan_as_stream(),
        }
    }

    fn num_rows(&self) -> Option<usize> {
        match self {
            Self::Yes(inner) => inner.num_rows(),
            Self::No(inner) => inner.num_rows(),
        }
    }

    fn rescannable(&self) -> bool {
        match self {
            Self::Yes(inner) => inner.rescannable(),
            Self::No(inner) => inner.rescannable(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::record_batch;
    use futures::TryStreamExt;

    #[tokio::test]
    async fn test_record_batch_rescannable() {
        let mut batch = record_batch!(("id", Int64, [0, 1, 2])).unwrap();

        let stream1 = batch.scan_as_stream();
        let batches1: Vec<RecordBatch> = stream1.try_collect().await.unwrap();
        assert_eq!(batches1.len(), 1);
        assert_eq!(batches1[0], batch);

        assert!(batch.rescannable());
        let stream2 = batch.scan_as_stream();
        let batches2: Vec<RecordBatch> = stream2.try_collect().await.unwrap();
        assert_eq!(batches2.len(), 1);
        assert_eq!(batches2[0], batch);
    }

    #[tokio::test]
    async fn test_vec_batch_rescannable() {
        let mut batches = vec![
            record_batch!(("id", Int64, [0, 1])).unwrap(),
            record_batch!(("id", Int64, [2, 3, 4])).unwrap(),
        ];

        let stream1 = batches.scan_as_stream();
        let result1: Vec<RecordBatch> = stream1.try_collect().await.unwrap();
        assert_eq!(result1.len(), 2);
        assert_eq!(result1[0], batches[0]);
        assert_eq!(result1[1], batches[1]);

        assert!(batches.rescannable());
        let stream2 = batches.scan_as_stream();
        let result2: Vec<RecordBatch> = stream2.try_collect().await.unwrap();
        assert_eq!(result2.len(), 2);
        assert_eq!(result2[0], batches[0]);
        assert_eq!(result2[1], batches[1]);
    }

    #[tokio::test]
    async fn test_reader_not_rescannable() {
        let batch = record_batch!(("id", Int64, [0, 1, 2])).unwrap();
        let schema = batch.schema();
        let mut reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            vec![Ok(batch.clone())],
            schema.clone(),
        ));

        let stream1 = reader.scan_as_stream();
        let result1: Vec<RecordBatch> = stream1.try_collect().await.unwrap();
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0], batch);

        assert!(!reader.rescannable());
        let stream2 = reader.scan_as_stream();
        let result2: Vec<RecordBatch> = stream2.try_collect().await.unwrap();
        assert_eq!(result2.len(), 0);
    }

    #[tokio::test]
    async fn test_stream_not_rescannable() {
        let batch = record_batch!(("id", Int64, [0, 1, 2])).unwrap();
        let schema = batch.schema();
        let inner_stream = futures::stream::iter(vec![Ok(batch.clone())]);
        let mut stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream {
            schema: schema.clone(),
            stream: inner_stream,
        });

        let stream1 = stream.scan_as_stream();
        let result1: Vec<RecordBatch> = stream1.try_collect().await.unwrap();
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0], batch);

        assert!(!stream.rescannable());
        let stream2 = stream.scan_as_stream();
        let result2: Vec<RecordBatch> = stream2.try_collect().await.unwrap();
        assert_eq!(result2.len(), 0);
    }

    mod embedding_tests {
        use super::*;
        use crate::embeddings::MemoryRegistry;
        use crate::table::{ColumnDefinition, ColumnKind};
        use arrow_array::Array as _;
        use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use std::borrow::Cow;

        /// A mock embedding function that returns a fixed-size vector for each input string.
        #[derive(Debug)]
        struct MockEmbeddingFunction {
            dim: usize,
        }

        impl MockEmbeddingFunction {
            fn new(dim: usize) -> Self {
                Self { dim }
            }
        }

        impl EmbeddingFunction for MockEmbeddingFunction {
            fn name(&self) -> &str {
                "mock"
            }

            fn source_type(&self) -> crate::Result<Cow<'_, DataType>> {
                Ok(Cow::Owned(DataType::Utf8))
            }

            fn dest_type(&self) -> crate::Result<Cow<'_, DataType>> {
                Ok(Cow::Owned(DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    self.dim as i32,
                )))
            }

            fn compute_source_embeddings(
                &self,
                source: Arc<dyn arrow_array::Array>,
            ) -> crate::Result<Arc<dyn arrow_array::Array>> {
                let strings = source.as_any().downcast_ref::<StringArray>().unwrap();
                let num_rows = strings.len();

                // Create a vector of length dim for each row, filled with 1.0
                let values: Vec<f32> = (0..num_rows * self.dim).map(|_| 1.0f32).collect();
                let values_array = Float32Array::from(values);

                let list = FixedSizeListArray::try_new(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    self.dim as i32,
                    Arc::new(values_array) as ArrayRef,
                    None,
                )
                .unwrap();

                Ok(Arc::new(list))
            }

            fn compute_query_embeddings(
                &self,
                input: Arc<dyn arrow_array::Array>,
            ) -> crate::Result<Arc<dyn arrow_array::Array>> {
                self.compute_source_embeddings(input)
            }
        }

        #[tokio::test]
        async fn test_with_embeddings_scannable() {
            let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
            let text_array = StringArray::from(vec!["hello", "world", "test"]);
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(text_array) as ArrayRef])
                    .unwrap();

            let mock_embedding = Arc::new(MockEmbeddingFunction::new(4));
            let embedding_def = EmbeddingDefinition::new("text", "mock", Some("text_embedding"));

            let mut scannable = WithEmbeddingsScannable::try_new(
                Box::new(batch.clone()),
                vec![(embedding_def, mock_embedding)],
            )
            .unwrap();

            // Check that schema has the embedding column
            let output_schema = scannable.schema();
            assert_eq!(output_schema.fields().len(), 2);
            assert_eq!(output_schema.field(0).name(), "text");
            assert_eq!(output_schema.field(1).name(), "text_embedding");

            // Check num_rows and rescannable are preserved
            assert_eq!(scannable.num_rows(), Some(3));
            assert!(scannable.rescannable());

            // Read the data
            let stream = scannable.scan_as_stream();
            let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();
            assert_eq!(results.len(), 1);

            let result_batch = &results[0];
            assert_eq!(result_batch.num_rows(), 3);
            assert_eq!(result_batch.num_columns(), 2);

            // Verify the embedding column is present and has the right shape
            let embedding_col = result_batch.column(1);
            assert_eq!(embedding_col.len(), 3);
        }

        #[tokio::test]
        async fn test_maybe_embedded_scannable_no_embeddings() {
            let batch = record_batch!(("id", Int64, [1, 2, 3])).unwrap();

            // Create a table definition with no embedding columns
            let table_def = TableDefinition::new_from_schema(batch.schema());

            // Even with a registry, if there are no embedding columns, it's a passthrough
            let registry: Arc<dyn EmbeddingRegistry> = Arc::new(MemoryRegistry::new());
            let mut scannable = MaybeEmbeddedScannable::try_new(
                Box::new(batch.clone()),
                &table_def,
                Some(&registry),
            )
            .unwrap();

            // Should be a No variant (passthrough)
            assert!(matches!(scannable, MaybeEmbeddedScannable::No(_)));

            // Check that data passes through unchanged
            let stream = scannable.scan_as_stream();
            let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], batch);
        }

        #[tokio::test]
        async fn test_maybe_embedded_scannable_with_embeddings() {
            let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
            let text_array = StringArray::from(vec!["hello", "world"]);
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(text_array) as ArrayRef])
                    .unwrap();

            // Create a table definition with an embedding column
            let embedding_def = EmbeddingDefinition::new("text", "mock", Some("text_embedding"));
            let embedding_schema = Arc::new(Schema::new(vec![
                Field::new("text", DataType::Utf8, false),
                Field::new(
                    "text_embedding",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        4,
                    ),
                    false,
                ),
            ]));
            let table_def = TableDefinition::new(
                embedding_schema,
                vec![
                    ColumnDefinition {
                        kind: ColumnKind::Physical,
                    },
                    ColumnDefinition {
                        kind: ColumnKind::Embedding(embedding_def.clone()),
                    },
                ],
            );

            // Register the mock embedding function
            let registry: Arc<dyn EmbeddingRegistry> = Arc::new(MemoryRegistry::new());
            let mock_embedding: Arc<dyn EmbeddingFunction> =
                Arc::new(MockEmbeddingFunction::new(4));
            registry.register("mock", mock_embedding).unwrap();

            let mut scannable =
                MaybeEmbeddedScannable::try_new(Box::new(batch), &table_def, Some(&registry))
                    .unwrap();

            // Should be a Yes variant
            assert!(matches!(scannable, MaybeEmbeddedScannable::Yes(_)));

            // Read and verify the data has embeddings
            let stream = scannable.scan_as_stream();
            let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();
            assert_eq!(results.len(), 1);

            let result_batch = &results[0];
            assert_eq!(result_batch.num_columns(), 2);
            assert_eq!(result_batch.schema().field(1).name(), "text_embedding");
        }

        #[tokio::test]
        async fn test_maybe_embedded_scannable_missing_function() {
            let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
            let text_array = StringArray::from(vec!["hello"]);
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(text_array) as ArrayRef])
                    .unwrap();

            // Create a table definition with an embedding column
            let embedding_def =
                EmbeddingDefinition::new("text", "nonexistent", Some("text_embedding"));
            let embedding_schema = Arc::new(Schema::new(vec![
                Field::new("text", DataType::Utf8, false),
                Field::new(
                    "text_embedding",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        4,
                    ),
                    false,
                ),
            ]));
            let table_def = TableDefinition::new(
                embedding_schema,
                vec![
                    ColumnDefinition {
                        kind: ColumnKind::Physical,
                    },
                    ColumnDefinition {
                        kind: ColumnKind::Embedding(embedding_def),
                    },
                ],
            );

            // Registry has no embedding functions registered
            let registry: Arc<dyn EmbeddingRegistry> = Arc::new(MemoryRegistry::new());

            let result =
                MaybeEmbeddedScannable::try_new(Box::new(batch), &table_def, Some(&registry));

            // Should fail because the embedding function is not found
            assert!(result.is_err());
            let err = result.err().unwrap();
            assert!(
                matches!(err, Error::EmbeddingFunctionNotFound { .. }),
                "Expected EmbeddingFunctionNotFound"
            );
        }
    }
}
