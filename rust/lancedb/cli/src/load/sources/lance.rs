// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! OSS Lance dataset source.
//!
//! Streams record batches from an existing Lance dataset (local or
//! object-store URI). Memory-efficient: each batch is produced lazily
//! via the Lance scanner and dropped once the writer consumes it.

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use lance::Dataset;
// Bound synthesized all-null columns to the existing data-plane read budget.
const MAX_DATA_PLANE_READ_BATCH_ROWS_USIZE: usize = 16384;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use super::DataSource;
use crate::load::schema::{detect_vector_columns_from_schema, VectorColumnInfo};

/// Data source for an existing Lance dataset.
pub struct LanceSource {
    /// Source URI. `file:///abs/path`, `s3://bucket/path`, `gs://...`, etc.
    /// Lance hands this to object_store, so anything object_store can open
    /// is fair game.
    uri: String,
    /// Cached opened dataset. `Dataset::open` reads the manifest; we do it
    /// once per `LanceSource` instance and reuse the handle for `schema`,
    /// `total_rows`, and the streaming scan.
    dataset: Arc<Mutex<Option<Arc<Dataset>>>>,
}

impl LanceSource {
    pub fn new(uri: String) -> Result<Self> {
        Ok(Self {
            uri,
            dataset: Arc::new(Mutex::new(None)),
        })
    }

    /// Open the dataset once, cache the handle.
    async fn dataset(&self) -> Result<Arc<Dataset>> {
        let mut cached = self.dataset.lock().await;
        if let Some(ds) = cached.as_ref() {
            return Ok(ds.clone());
        }
        info!("Opening Lance dataset at {}", self.uri);
        let ds = Dataset::open(&self.uri)
            .await
            .with_context(|| format!("failed to open Lance dataset at {}", self.uri))?;
        let arc = Arc::new(ds);
        *cached = Some(arc.clone());
        Ok(arc)
    }
}

#[async_trait]
impl DataSource for LanceSource {
    async fn schema(&self) -> Result<SchemaRef> {
        let ds = self.dataset().await?;
        let arrow: ArrowSchema = ds.schema().into();
        Ok(Arc::new(arrow))
    }

    async fn total_rows(&self) -> Result<Option<u64>> {
        let ds = self.dataset().await?;
        let count = ds
            .count_rows(None)
            .await
            .context("failed to count rows in Lance dataset")?;
        Ok(Some(count as u64))
    }

    /// Lance datasets store vectors as `FixedSizeList<Float>`, so the dims
    /// live in the schema. Skip the default peek -- it would re-scan the
    /// first batch over the object store before the real load starts.
    async fn detect_vector_columns(&self) -> Result<HashMap<String, VectorColumnInfo>> {
        let schema = self.schema().await?;
        Ok(detect_vector_columns_from_schema(&schema))
    }

    fn stream_batches(&self, batch_size: usize) -> BoxStream<'_, Result<RecordBatch>> {
        let dataset = self.dataset.clone();
        let uri = self.uri.clone();
        Box::pin(async_stream::try_stream! {
            // Open or reuse the cached dataset handle. Done inside the
            // stream so callers can call `stream_batches` without pre-awaiting.
            let ds = {
                let mut cached = dataset.lock().await;
                if let Some(ds) = cached.as_ref() {
                    ds.clone()
                } else {
                    let opened = Arc::new(
                        Dataset::open(&uri)
                            .await
                            .with_context(|| format!("failed to open Lance dataset at {}", uri))?,
                    );
                    *cached = Some(opened.clone());
                    opened
                }
            };

            let mut scanner = ds.scan();
            // `--batch-size` is user-supplied, and the source table may hold
            // a column added all-null, which lance synthesizes at
            // width × batch_rows bytes. `add_columns` admits a schema
            // against this ceiling, so reading wider than it would let an
            // admitted column allocate past the intended budget.
            scanner.batch_size(batch_size.min(MAX_DATA_PLANE_READ_BATCH_ROWS_USIZE));
            let mut stream = scanner
                .try_into_stream()
                .await
                .context("failed to build Lance scan stream")?;
            let mut total_rows = 0usize;
            while let Some(batch) = stream.try_next().await
                .context("failed to read Lance batch")? {
                total_rows += batch.num_rows();
                yield batch;
            }
            info!("Streamed {} total rows from Lance dataset", total_rows);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatchIterator};

    #[tokio::test]
    async fn streams_local_dataset_with_bounded_batches() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("source.lance");
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int32Array::from_iter_values(0..20_000)) as arrow_array::ArrayRef,
        )])
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        Dataset::write(reader, uri.to_str().unwrap(), None)
            .await
            .unwrap();
        let source = LanceSource::new(uri.to_string_lossy().into()).unwrap();
        assert_eq!(source.total_rows().await.unwrap(), Some(20_000));
        let batches: Vec<_> = source.stream_batches(100_000).try_collect().await.unwrap();
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            20_000
        );
        assert!(batches
            .iter()
            .all(|batch| batch.num_rows() <= MAX_DATA_PLANE_READ_BATCH_ROWS_USIZE));
    }
}
