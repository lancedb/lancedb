// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Data source abstraction for LanceDB.
//!
//! This module provides a [`Scannable`] trait that allows input data sources to express
//! capabilities (row count, rescannability) so the insert pipeline can make
//! better decisions about write parallelism and retry strategies.

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion_common::DataFusionError;
use futures::stream::once;
use lance_datafusion::utils::StreamingWriteSource;

use crate::{
    arrow::{SendableRecordBatchStream, SendableRecordBatchStreamExt, SimpleRecordBatchStream},
    Result,
};

pub trait Scannable: Send {
    /// Returns the schema of the data.
    fn schema(&self) -> SchemaRef;

    /// Read data as a stream of record batches.
    ///
    /// This consumes the data source. The returned stream produces batches
    /// matching the schema from [`Self::schema()`].
    fn read(self: Box<Self>) -> Result<SendableRecordBatchStream>;

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

    fn read(self: Box<Self>) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        Ok(Box::pin(SimpleRecordBatchStream {
            schema,
            stream: once(async move { Ok(*self) }),
        }))
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

    fn read(self: Box<Self>) -> Result<SendableRecordBatchStream> {
        let schema = Scannable::schema(self.as_ref());
        let batches = *self;
        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(SimpleRecordBatchStream { schema, stream }))
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

    fn read(self: Box<Self>) -> Result<SendableRecordBatchStream> {
        let inner: Box<dyn RecordBatchReader + Send> = *self;
        let schema = RecordBatchReader::schema(inner.as_ref());
        // TODO: run the reader in tokio::task::spawn_blocking to avoid blocking async runtime
        // Create a lazy stream that pulls batches on demand
        let stream = futures::stream::unfold(inner, |mut reader| async move {
            match reader.next() {
                Some(Ok(batch)) => Some((Ok(batch), reader)),
                Some(Err(e)) => Some((Err(e.into()), reader)),
                None => None,
            }
        });
        Ok(Box::pin(SimpleRecordBatchStream { schema, stream }))
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

    fn read(self: Box<Self>) -> Result<SendableRecordBatchStream> {
        Ok(*self)
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

    fn into_stream(self) -> datafusion_physical_plan::SendableRecordBatchStream {
        let schema = self.schema();
        match self.read() {
            Ok(stream) => stream.into_df_stream(),
            Err(err) => {
                let err = DataFusionError::External(Box::new(err));
                let err_fut = futures::future::err(err);
                let err_stream = futures::stream::once(err_fut);
                Box::pin(
                    datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(
                        schema.clone(),
                        err_stream,
                    ),
                )
            }
        }
    }
}
