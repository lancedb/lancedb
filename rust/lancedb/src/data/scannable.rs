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
use lance_datafusion::utils::StreamingWriteSource;

use crate::arrow::{
    SendableRecordBatchStream, SendableRecordBatchStreamExt, SimpleRecordBatchStream,
};

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
}
