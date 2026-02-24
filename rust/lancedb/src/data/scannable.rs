// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Data source abstraction for LanceDB.
//!
//! This module provides a [`Scannable`] trait that allows input data sources to express
//! capabilities (row count, rescannability) so the insert pipeline can make
//! better decisions about write parallelism and retry strategies.

use std::sync::Arc;

use crate::arrow::{
    SendableRecordBatchStream, SendableRecordBatchStreamExt, SimpleRecordBatchStream,
};
use crate::embeddings::{
    compute_embeddings_for_batch, compute_output_schema, EmbeddingDefinition, EmbeddingFunction,
    EmbeddingRegistry,
};
use crate::table::{ColumnDefinition, ColumnKind, TableDefinition};
use crate::{Error, Result};
use arrow_array::{ArrayRef, RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use async_trait::async_trait;
use futures::stream::once;
use futures::StreamExt;
use lance_datafusion::utils::StreamingWriteSource;

pub trait Scannable: Send {
    /// Returns the schema of the data.
    fn schema(&self) -> SchemaRef;

    /// Read data as a stream of record batches.
    ///
    /// For rescannable sources (in-memory data like RecordBatch, Vec<RecordBatch>),
    /// this can be called multiple times and returns cloned data each time.
    ///
    /// For non-rescannable sources (streams, readers), this can only be called once.
    /// Calling it a second time returns a stream whose first item is an error.
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
        if self.is_empty() {
            let schema = Scannable::schema(self);
            return Box::pin(SimpleRecordBatchStream {
                schema,
                stream: once(async {
                    Err(Error::InvalidInput {
                        message: "Cannot scan an empty Vec<RecordBatch>".to_string(),
                    })
                }),
            });
        }
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
        let schema = Scannable::schema(self);

        // Swap self with a reader that errors on iteration, so a second call
        // produces a clear error instead of silently returning empty data.
        let err_reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            vec![Err(ArrowError::InvalidArgumentError(
                "Reader has already been consumed".into(),
            ))],
            schema.clone(),
        ));
        let reader = std::mem::replace(self, err_reader);

        // Bridge the blocking RecordBatchReader to an async stream via a channel.
        let (tx, rx) = tokio::sync::mpsc::channel::<crate::Result<RecordBatch>>(2);
        tokio::task::spawn_blocking(move || {
            for batch_result in reader {
                let result = batch_result.map_err(Into::into);
                if tx.blocking_send(result).is_err() {
                    break;
                }
            }
        });

        let stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|batch| (batch, rx))
        })
        .fuse();

        Box::pin(SimpleRecordBatchStream { schema, stream })
    }
}

impl Scannable for SendableRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.as_ref().schema()
    }

    fn scan_as_stream(&mut self) -> SendableRecordBatchStream {
        let schema = Scannable::schema(self);

        // Swap self with an error stream so a second call produces a clear error.
        let error_stream = Box::pin(SimpleRecordBatchStream {
            schema: schema.clone(),
            stream: once(async {
                Err(Error::InvalidInput {
                    message: "Stream has already been consumed".to_string(),
                })
            }),
        });
        std::mem::replace(self, error_stream)
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

        // Build column definitions: Physical for base columns, Embedding for new ones
        let base_col_count = inner.schema().fields().len();
        let column_definitions: Vec<ColumnDefinition> = (0..base_col_count)
            .map(|_| ColumnDefinition {
                kind: ColumnKind::Physical,
            })
            .chain(embeddings.iter().map(|(ed, _)| ColumnDefinition {
                kind: ColumnKind::Embedding(ed.clone()),
            }))
            .collect();

        let table_definition = TableDefinition::new(output_schema, column_definitions);
        let output_schema = table_definition.into_rich_schema();

        Self::with_schema(inner, embeddings, output_schema)
    }

    /// Create a WithEmbeddingsScannable with a specific output schema.
    ///
    /// Use this when the table schema is already known (e.g. during add) to
    /// avoid nullability mismatches between the embedding function's declared
    /// type and the table's stored type.
    pub fn with_schema(
        inner: Box<dyn Scannable>,
        embeddings: Vec<(EmbeddingDefinition, Arc<dyn EmbeddingFunction>)>,
        output_schema: SchemaRef,
    ) -> Result<Self> {
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
        let stream_schema = output_schema.clone();

        let mapped_stream = inner_stream.then(move |batch_result| {
            let embeddings = embeddings.clone();
            let output_schema = output_schema.clone();
            async move {
                let batch = batch_result?;
                let result = tokio::task::spawn_blocking(move || {
                    compute_embeddings_for_batch(batch, &embeddings)
                })
                .await
                .map_err(|e| Error::Runtime {
                    message: format!("Task panicked during embedding computation: {}", e),
                })??;
                // Cast columns to match the declared output schema. The data is
                // identical but field metadata (e.g. nested nullability) may
                // differ between the embedding function output and the table.
                let columns: Vec<ArrayRef> = result
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(i, col)| {
                        let target_type = output_schema.field(i).data_type();
                        if col.data_type() == target_type {
                            Ok(col.clone())
                        } else {
                            arrow_cast::cast(col, target_type).map_err(Error::from)
                        }
                    })
                    .collect::<Result<_>>()?;
                let result = RecordBatch::try_new(output_schema, columns)?;
                Ok(result)
            }
        });

        Box::pin(SimpleRecordBatchStream {
            schema: stream_schema,
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

pub fn scannable_with_embeddings(
    inner: Box<dyn Scannable>,
    table_definition: &TableDefinition,
    registry: Option<&Arc<dyn EmbeddingRegistry>>,
) -> Result<Box<dyn Scannable>> {
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
            // Use the table's schema so embedding column types (including nested
            // nullability) match what's stored, avoiding mismatches with the
            // embedding function's declared dest_type.
            return Ok(Box::new(WithEmbeddingsScannable::with_schema(
                inner,
                embeddings,
                table_definition.schema.clone(),
            )?));
        }
    }

    Ok(inner)
}

/// A wrapper that buffers the first RecordBatch from a Scannable so we can
/// inspect it (e.g. to estimate data size) without losing it.
pub(crate) struct PeekedScannable {
    inner: Box<dyn Scannable>,
    peeked: Option<RecordBatch>,
    /// The first item from the stream, if it was an error. Stored so we can
    /// re-emit it from `scan_as_stream` instead of silently dropping it.
    first_error: Option<crate::Error>,
    stream: Option<SendableRecordBatchStream>,
}

impl PeekedScannable {
    pub fn new(inner: Box<dyn Scannable>) -> Self {
        Self {
            inner,
            peeked: None,
            first_error: None,
            stream: None,
        }
    }

    /// Reads and buffers the first batch from the inner scannable.
    /// Returns a clone of it. Subsequent calls return the same batch.
    ///
    /// Returns `None` if the stream is empty or the first item is an error.
    /// Errors are preserved and re-emitted by `scan_as_stream`.
    pub async fn peek(&mut self) -> Option<RecordBatch> {
        if self.peeked.is_some() {
            return self.peeked.clone();
        }
        // Already peeked and got an error or empty stream.
        if self.stream.is_some() || self.first_error.is_some() {
            return None;
        }
        let mut stream = self.inner.scan_as_stream();
        match stream.next().await {
            Some(Ok(batch)) => {
                self.peeked = Some(batch.clone());
                self.stream = Some(stream);
                Some(batch)
            }
            Some(Err(e)) => {
                self.first_error = Some(e);
                self.stream = Some(stream);
                None
            }
            None => {
                self.stream = Some(stream);
                None
            }
        }
    }
}

impl Scannable for PeekedScannable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn num_rows(&self) -> Option<usize> {
        self.inner.num_rows()
    }

    fn rescannable(&self) -> bool {
        self.inner.rescannable()
    }

    fn scan_as_stream(&mut self) -> SendableRecordBatchStream {
        let schema = self.inner.schema();

        // If peek() hit an error, prepend it so downstream sees the error.
        let error_item = self.first_error.take().map(Err);

        match (self.peeked.take(), self.stream.take()) {
            (Some(batch), Some(rest)) => {
                let prepend = futures::stream::once(std::future::ready(Ok(batch)));
                Box::pin(SimpleRecordBatchStream {
                    schema,
                    stream: prepend.chain(rest),
                })
            }
            (Some(batch), None) => Box::pin(SimpleRecordBatchStream {
                schema,
                stream: futures::stream::once(std::future::ready(Ok(batch))),
            }),
            (None, Some(rest)) => {
                if let Some(err) = error_item {
                    let stream = futures::stream::once(std::future::ready(err));
                    Box::pin(SimpleRecordBatchStream { schema, stream })
                } else {
                    rest
                }
            }
            (None, None) => {
                // peek() was never called — just delegate
                self.inner.scan_as_stream()
            }
        }
    }
}

/// Compute the number of write partitions based on data size estimates.
///
/// `sample_bytes` and `sample_rows` come from a representative batch and are
/// used to estimate per-row size. `total_rows_hint` is the total row count
/// when known; otherwise `sample_rows` row count is used as a lower bound
/// estimate.
///
/// Targets roughly 1 million rows or 2 GB per partition, capped at
/// `max_partitions` (typically the number of available CPU cores).
pub(crate) fn estimate_write_partitions(
    sample_bytes: usize,
    sample_rows: usize,
    total_rows_hint: Option<usize>,
    max_partitions: usize,
) -> usize {
    if sample_rows == 0 {
        return 1;
    }
    let bytes_per_row = sample_bytes / sample_rows;
    let total_rows = total_rows_hint.unwrap_or(sample_rows);
    let total_bytes = total_rows * bytes_per_row;
    let by_rows = total_rows.div_ceil(1_000_000);
    let by_bytes = total_bytes.div_ceil(2 * 1024 * 1024 * 1024);
    by_rows.max(by_bytes).max(1).min(max_partitions)
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
    async fn test_vec_batch_empty_errors() {
        let mut empty: Vec<RecordBatch> = vec![];
        let mut stream = empty.scan_as_stream();
        let result = stream.next().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_reader_not_rescannable() {
        let batch = record_batch!(("id", Int64, [0, 1, 2])).unwrap();
        let schema = batch.schema();
        let mut reader: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
            RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone()),
        );

        let stream1 = reader.scan_as_stream();
        let result1: Vec<RecordBatch> = stream1.try_collect().await.unwrap();
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0], batch);

        assert!(!reader.rescannable());
        // Second call returns a stream whose first item is an error
        let mut stream2 = reader.scan_as_stream();
        let result2 = stream2.next().await;
        assert!(result2.is_some());
        assert!(result2.unwrap().is_err());
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
        // Second call returns a stream whose first item is an error
        let mut stream2 = stream.scan_as_stream();
        let result2 = stream2.next().await;
        assert!(result2.is_some());
        assert!(result2.unwrap().is_err());
    }

    mod peeked_scannable_tests {
        use crate::test_utils::TestCustomError;

        use super::*;

        #[tokio::test]
        async fn test_peek_returns_first_batch() {
            let batch = record_batch!(("id", Int64, [1, 2, 3])).unwrap();
            let mut peeked = PeekedScannable::new(Box::new(batch.clone()));

            let first = peeked.peek().await.unwrap();
            assert_eq!(first, batch);
        }

        #[tokio::test]
        async fn test_peek_is_idempotent() {
            let batch = record_batch!(("id", Int64, [1, 2, 3])).unwrap();
            let mut peeked = PeekedScannable::new(Box::new(batch.clone()));

            let first = peeked.peek().await.unwrap();
            let second = peeked.peek().await.unwrap();
            assert_eq!(first, second);
        }

        #[tokio::test]
        async fn test_scan_after_peek_returns_all_data() {
            let batches = vec![
                record_batch!(("id", Int64, [1, 2])).unwrap(),
                record_batch!(("id", Int64, [3, 4, 5])).unwrap(),
            ];
            let mut peeked = PeekedScannable::new(Box::new(batches.clone()));

            let first = peeked.peek().await.unwrap();
            assert_eq!(first, batches[0]);

            let result: Vec<RecordBatch> = peeked.scan_as_stream().try_collect().await.unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0], batches[0]);
            assert_eq!(result[1], batches[1]);
        }

        #[tokio::test]
        async fn test_scan_without_peek_passes_through() {
            let batch = record_batch!(("id", Int64, [1, 2, 3])).unwrap();
            let mut peeked = PeekedScannable::new(Box::new(batch.clone()));

            let result: Vec<RecordBatch> = peeked.scan_as_stream().try_collect().await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], batch);
        }

        #[tokio::test]
        async fn test_delegates_num_rows() {
            let batches = vec![
                record_batch!(("id", Int64, [1, 2])).unwrap(),
                record_batch!(("id", Int64, [3])).unwrap(),
            ];
            let peeked = PeekedScannable::new(Box::new(batches));
            assert_eq!(peeked.num_rows(), Some(3));
        }

        #[tokio::test]
        async fn test_non_rescannable_stream_data_preserved() {
            let batches = vec![
                record_batch!(("id", Int64, [1, 2])).unwrap(),
                record_batch!(("id", Int64, [3])).unwrap(),
            ];
            let schema = batches[0].schema();
            let inner = futures::stream::iter(batches.clone().into_iter().map(Ok));
            let stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream {
                schema,
                stream: inner,
            });

            let mut peeked = PeekedScannable::new(Box::new(stream));
            assert!(!peeked.rescannable());
            assert_eq!(peeked.num_rows(), None);

            let first = peeked.peek().await.unwrap();
            assert_eq!(first, batches[0]);

            // All data is still available via scan_as_stream
            let result: Vec<RecordBatch> = peeked.scan_as_stream().try_collect().await.unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0], batches[0]);
            assert_eq!(result[1], batches[1]);
        }

        #[tokio::test]
        async fn test_error_in_first_batch_propagates() {
            let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "id",
                arrow_schema::DataType::Int64,
                false,
            )]));
            let inner = futures::stream::iter(vec![Err(Error::External {
                source: Box::new(TestCustomError),
            })]);
            let stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream {
                schema,
                stream: inner,
            });

            let mut peeked = PeekedScannable::new(Box::new(stream));

            // peek returns None for errors
            assert!(peeked.peek().await.is_none());

            // But the error should come through when scanning
            let mut stream = peeked.scan_as_stream();
            let first = stream.next().await.unwrap();
            assert!(first.is_err());
            let err = first.unwrap_err();
            assert!(
                matches!(&err, Error::External { source } if source.downcast_ref::<TestCustomError>().is_some()),
                "Expected TestCustomError to be preserved, got: {err}"
            );
        }

        #[tokio::test]
        async fn test_error_in_later_batch_propagates() {
            let good_batch = record_batch!(("id", Int64, [1, 2])).unwrap();
            let schema = good_batch.schema();
            let inner = futures::stream::iter(vec![
                Ok(good_batch.clone()),
                Err(Error::External {
                    source: Box::new(TestCustomError),
                }),
            ]);
            let stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream {
                schema,
                stream: inner,
            });

            let mut peeked = PeekedScannable::new(Box::new(stream));

            // peek succeeds with the first batch
            let first = peeked.peek().await.unwrap();
            assert_eq!(first, good_batch);

            // scan_as_stream should yield the first batch, then the error
            let mut stream = peeked.scan_as_stream();
            let batch1 = stream.next().await.unwrap().unwrap();
            assert_eq!(batch1, good_batch);

            let batch2 = stream.next().await.unwrap();
            assert!(batch2.is_err());
            let err = batch2.unwrap_err();
            assert!(
                matches!(&err, Error::External { source } if source.downcast_ref::<TestCustomError>().is_some()),
                "Expected TestCustomError to be preserved, got: {err}"
            );
        }

        #[tokio::test]
        async fn test_empty_stream_returns_none() {
            let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "id",
                arrow_schema::DataType::Int64,
                false,
            )]));
            let inner = futures::stream::empty();
            let stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream {
                schema,
                stream: inner,
            });

            let mut peeked = PeekedScannable::new(Box::new(stream));
            assert!(peeked.peek().await.is_none());

            // Scanning an empty (post-peek) stream should yield nothing
            let result: Vec<RecordBatch> = peeked.scan_as_stream().try_collect().await.unwrap();
            assert!(result.is_empty());
        }
    }

    mod estimate_write_partitions_tests {
        use super::*;

        #[test]
        fn test_small_data_single_partition() {
            // 100 rows * 24 bytes/row = 2400 bytes — well under both thresholds
            assert_eq!(estimate_write_partitions(2400, 100, Some(100), 8), 1);
        }

        #[test]
        fn test_scales_by_row_count() {
            // 2.5M rows at 24 bytes/row — row threshold dominates
            // ceil(2_500_000 / 1_000_000) = 3
            assert_eq!(estimate_write_partitions(72, 3, Some(2_500_000), 8), 3);
        }

        #[test]
        fn test_scales_by_byte_size() {
            // 100k rows at 40KB/row = ~4GB total → ceil(4GB / 2GB) = 2
            let sample_bytes = 40_000 * 10;
            assert_eq!(
                estimate_write_partitions(sample_bytes, 10, Some(100_000), 8),
                2
            );
        }

        #[test]
        fn test_capped_at_max_partitions() {
            // 10M rows would want 10 partitions, but capped at 4
            assert_eq!(estimate_write_partitions(72, 3, Some(10_000_000), 4), 4);
        }

        #[test]
        fn test_zero_sample_rows_returns_one() {
            assert_eq!(estimate_write_partitions(0, 0, Some(1_000_000), 8), 1);
        }

        #[test]
        fn test_no_row_hint_uses_sample_size() {
            // Without a hint, uses sample_rows (3), which is small
            assert_eq!(estimate_write_partitions(72, 3, None, 8), 1);
        }

        #[test]
        fn test_always_at_least_one() {
            assert_eq!(estimate_write_partitions(24, 1, Some(1), 8), 1);
        }
    }

    mod embedding_tests {
        use super::*;
        use crate::embeddings::MemoryRegistry;
        use crate::table::{ColumnDefinition, ColumnKind};
        use crate::test_utils::embeddings::MockEmbed;
        use arrow_array::Array as _;
        use arrow_array::{ArrayRef, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        #[tokio::test]
        async fn test_with_embeddings_scannable() {
            let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
            let text_array = StringArray::from(vec!["hello", "world", "test"]);
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(text_array) as ArrayRef])
                    .unwrap();

            let mock_embedding: Arc<dyn EmbeddingFunction> = Arc::new(MockEmbed::new("mock", 4));
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
            let mut scannable =
                scannable_with_embeddings(Box::new(batch.clone()), &table_def, Some(&registry))
                    .unwrap();

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
            let mock_embedding: Arc<dyn EmbeddingFunction> = Arc::new(MockEmbed::new("mock", 4));
            registry.register("mock", mock_embedding).unwrap();

            let mut scannable =
                scannable_with_embeddings(Box::new(batch), &table_def, Some(&registry)).unwrap();

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

            let result = scannable_with_embeddings(Box::new(batch), &table_def, Some(&registry));

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
