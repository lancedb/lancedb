// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! HuggingFace dataset source.
//!
//! Downloads and streams parquet files from HuggingFace Hub.

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use hf_hub::api::tokio::Api;
use hf_hub::Repo;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use std::sync::Arc;
use tokio::fs::File;
use tokio::sync::Mutex;
use tracing::info;

use super::DataSource;

/// Check if a filename matches the given split.
///
/// HuggingFace datasets use various naming conventions for parquet files:
/// - `data/train-00000.parquet` (data/{split}-*)
/// - `train/data.parquet` ({split}/*)
/// - `train-00000.parquet` ({split}-*)
/// - `some/path/train/file.parquet` (*/{split}/*)
/// - `train.parquet` ({split}.parquet)
fn matches_split(filename: &str, split: &str) -> bool {
    let prefixes = [
        format!("data/{}-", split),
        format!("{}/", split),
        format!("{}-", split),
    ];

    prefixes.iter().any(|p| filename.starts_with(p))
        || filename.contains(&format!("/{}/", split))
        || filename == format!("{}.parquet", split)
}

/// Data source for HuggingFace datasets.
pub struct HuggingFaceSource {
    /// Dataset identifier (e.g., "KShivendu/dbpedia-entities-openai-1M")
    dataset_id: String,
    /// Dataset split (e.g., "train", "test")
    split: String,
    /// Cached list of parquet file paths
    parquet_files: Arc<Mutex<Option<Vec<std::path::PathBuf>>>>,
    /// Cached schema
    cached_schema: Arc<Mutex<Option<SchemaRef>>>,
}

impl HuggingFaceSource {
    /// Create a new HuggingFace data source.
    pub fn new(dataset_id: String, split: String) -> Result<Self> {
        Ok(Self {
            dataset_id,
            split,
            parquet_files: Arc::new(Mutex::new(None)),
            cached_schema: Arc::new(Mutex::new(None)),
        })
    }

    /// Download dataset files from HuggingFace Hub.
    async fn ensure_files_downloaded(&self) -> Result<Vec<std::path::PathBuf>> {
        let mut cached = self.parquet_files.lock().await;
        if let Some(ref files) = *cached {
            return Ok(files.clone());
        }

        info!(
            "Downloading dataset {} from HuggingFace Hub...",
            self.dataset_id
        );

        let api = Api::new().context("Failed to create HuggingFace Hub API client")?;
        let repo = Repo::dataset(self.dataset_id.clone());
        let repo_api = api.repo(repo);

        let filenames = self.discover_parquet_files(&repo_api).await?;
        let downloaded = self.download_files(&repo_api, &filenames).await?;

        *cached = Some(downloaded.clone());
        Ok(downloaded)
    }

    /// Discover parquet files for the specified split.
    async fn discover_parquet_files(
        &self,
        repo_api: &hf_hub::api::tokio::ApiRepo,
    ) -> Result<Vec<String>> {
        let files = repo_api
            .info()
            .await
            .context("Failed to get dataset info")?;

        // Find parquet files for the specified split
        let split_files: Vec<_> = files
            .siblings
            .iter()
            .filter(|s| {
                s.rfilename.ends_with(".parquet") && matches_split(&s.rfilename, &self.split)
            })
            .map(|s| s.rfilename.clone())
            .collect();

        if !split_files.is_empty() {
            info!(
                "Found {} parquet files for split '{}'",
                split_files.len(),
                self.split
            );
            return Ok(split_files);
        }

        // Fall back to all parquet files if split-specific ones aren't found
        let all_parquet: Vec<_> = files
            .siblings
            .iter()
            .filter(|s| s.rfilename.ends_with(".parquet"))
            .map(|s| s.rfilename.clone())
            .collect();

        if all_parquet.is_empty() {
            anyhow::bail!(
                "No parquet files found in dataset {}. Available files: {:?}",
                self.dataset_id,
                files
                    .siblings
                    .iter()
                    .map(|s| &s.rfilename)
                    .collect::<Vec<_>>()
            );
        }

        info!(
            "No files found for split '{}', using all {} parquet files",
            self.split,
            all_parquet.len()
        );
        Ok(all_parquet)
    }

    /// Download the specified files from the repository.
    async fn download_files(
        &self,
        repo_api: &hf_hub::api::tokio::ApiRepo,
        filenames: &[String],
    ) -> Result<Vec<std::path::PathBuf>> {
        let mut downloaded = Vec::with_capacity(filenames.len());
        for filename in filenames {
            info!("Downloading {}...", filename);
            let path = repo_api
                .get(filename)
                .await
                .with_context(|| format!("Failed to download {}", filename))?;
            downloaded.push(path);
        }
        Ok(downloaded)
    }
}

#[async_trait]
impl DataSource for HuggingFaceSource {
    async fn schema(&self) -> Result<SchemaRef> {
        let mut cached = self.cached_schema.lock().await;
        if let Some(ref schema) = *cached {
            return Ok(schema.clone());
        }

        let files = self.ensure_files_downloaded().await?;
        let first_file = files
            .first()
            .ok_or_else(|| anyhow::anyhow!("No files found"))?;

        let file = File::open(first_file)
            .await
            .context("Failed to open parquet file")?;

        let builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .context("Failed to read parquet metadata")?;

        let schema = builder.schema().clone();
        *cached = Some(schema.clone());
        Ok(schema)
    }

    async fn total_rows(&self) -> Result<Option<u64>> {
        let files = self.ensure_files_downloaded().await?;
        let mut total = 0u64;

        for file_path in &files {
            let file = File::open(file_path)
                .await
                .context("Failed to open parquet file")?;

            let builder = ParquetRecordBatchStreamBuilder::new(file)
                .await
                .context("Failed to read parquet metadata")?;

            let metadata = builder.metadata();
            for rg in metadata.row_groups() {
                total += rg.num_rows() as u64;
            }
        }

        Ok(Some(total))
    }

    fn stream_batches(&self, batch_size: usize) -> BoxStream<'_, Result<RecordBatch>> {
        let source = self.clone_for_stream();

        Box::pin(async_stream::try_stream! {
            let files = source.ensure_files_downloaded().await?;
            let mut total_rows = 0usize;

            for file_path in files {
                let file = File::open(&file_path)
                    .await
                    .context("Failed to open parquet file")?;

                let builder = ParquetRecordBatchStreamBuilder::new(file)
                    .await
                    .context("Failed to read parquet file")?;

                let mut stream = builder
                    .with_batch_size(batch_size)
                    .build()
                    .context("Failed to build parquet stream")?;

                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result.context("Failed to read batch")?;
                    total_rows += batch.num_rows();
                    yield batch;
                }
            }

            info!("Streamed {} total rows", total_rows);
        })
    }
}

impl HuggingFaceSource {
    fn clone_for_stream(&self) -> Self {
        Self {
            dataset_id: self.dataset_id.clone(),
            split: self.split.clone(),
            parquet_files: self.parquet_files.clone(),
            cached_schema: self.cached_schema.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    #[tokio::test]
    async fn discovers_requested_split_and_streams_cached_parquet() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new().fallback(|| async {
            axum::Json(serde_json::json!({"sha":"fixture", "siblings":[
                {"rfilename":"README.md"}, {"rfilename":"data/test-00000.parquet"},
                {"rfilename":"data/train-00000.parquet"}
            ]}))
        });
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let api = hf_hub::api::tokio::ApiBuilder::new()
            .with_endpoint(endpoint)
            .with_cache_dir(dir.path().join("cache"))
            .with_token(None)
            .build()
            .unwrap();
        let source = HuggingFaceSource::new("org/dataset".into(), "train".into()).unwrap();
        assert_eq!(
            source
                .discover_parquet_files(&api.repo(Repo::dataset("org/dataset".into())))
                .await
                .unwrap(),
            ["data/train-00000.parquet"]
        );
        server.abort();

        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        let path = dir.path().join("train.parquet");
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            batch.schema(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        *source.parquet_files.lock().await = Some(vec![path]);
        assert_eq!(source.total_rows().await.unwrap(), Some(3));
        assert_eq!(source.schema().await.unwrap(), batch.schema());
        let batches: Vec<_> = source.stream_batches(2).try_collect().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        assert!(batches.iter().all(|batch| batch.num_rows() <= 2));
    }
}
