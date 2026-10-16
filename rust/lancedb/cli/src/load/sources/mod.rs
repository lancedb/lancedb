// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Data source implementations for importing data.

pub mod huggingface;
pub mod lance;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use std::collections::HashMap;

use super::schema::{detect_vector_columns, VectorColumnInfo};

/// Default batch size for vector-column detection in the trait default impl.
/// Only used as a peek size; not part of the load batch size.
const VECTOR_DETECT_PEEK_BATCH_SIZE: usize = 1024;

/// Trait for data sources that can be imported into LanceDB.
#[async_trait]
pub trait DataSource: Send + Sync {
    /// Get the schema of the data source.
    async fn schema(&self) -> Result<SchemaRef>;

    /// Get the total number of rows in the data source, if known.
    ///
    /// Returns `Ok(Some(count))` if the total is known, `Ok(None)` if unknown.
    async fn total_rows(&self) -> Result<Option<u64>>;

    /// Stream record batches from the source.
    ///
    /// The stream should yield batches of approximately `batch_size` rows.
    /// Implementations may yield smaller batches at the end of the data.
    fn stream_batches(&self, batch_size: usize) -> BoxStream<'_, Result<RecordBatch>>;

    /// Detect vector columns (`List<Float>` / `FixedSizeList<Float>`) on this
    /// source. The default implementation peeks one batch and inspects values
    /// to determine `List<Float>` dimensions; sources whose schema already
    /// carries dims (e.g. native `FixedSizeList`) can override to avoid the
    /// peek and the redundant I/O it implies.
    async fn detect_vector_columns(&self) -> Result<HashMap<String, VectorColumnInfo>> {
        let mut stream = self.stream_batches(VECTOR_DETECT_PEEK_BATCH_SIZE);
        match stream.next().await {
            Some(Ok(batch)) => Ok(detect_vector_columns(&batch)),
            Some(Err(e)) => Err(e),
            None => Ok(HashMap::new()),
        }
    }
}
