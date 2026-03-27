// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Helper for reading files into LanceDB tables.
//!
//! This module provides the [`read_files`] function for reading Parquet, CSV,
//! and Lance files (including glob patterns) as streaming data sources compatible
//! with [`crate::Table::add`].
//!
//! # Example
//!
//! ```no_run
//! # async fn example(table: &lancedb::Table) -> lancedb::Result<()> {
//! let data = lancedb::read_files("./data/*.parquet").await?;
//! table.add(data).execute().await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext};
use futures::StreamExt;
use lance::dataset::builder::DatasetBuilder;

use crate::{
    Error, Result,
    arrow::{SendableRecordBatchStream, SimpleRecordBatchStream},
    data::scannable::Scannable,
};

/// Create a `SessionContext` configured for file reading.
///
/// Disables `schema_force_view_types` so that string columns are returned as
/// `Utf8` (not `Utf8View`), which is required for Lance compatibility.
fn make_session_context() -> SessionContext {
    let mut config = datafusion::prelude::SessionConfig::new();
    config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = false;
    SessionContext::new_with_config(config)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum FileFormat {
    Parquet,
    Csv,
    Lance,
}

/// Detect the file format from a path or glob pattern.
///
/// Glob wildcard characters are replaced before extension detection so that
/// patterns like `"data/*.parquet"` resolve to `FileFormat::Parquet`.
fn detect_format(pattern: &str) -> Result<FileFormat> {
    // Replace glob wildcards so Path::extension() works on the resulting string.
    // e.g. "data/*.parquet" → "data/x.parquet" → extension "parquet"
    let clean: String = pattern
        .chars()
        .map(|c| if matches!(c, '*' | '?') { 'x' } else { c })
        .collect();
    // Strip bracket expressions per path segment: "data/[abc].parquet" → "data/.parquet"
    let clean = clean
        .split('/')
        .map(|seg| seg.split('[').next().unwrap_or(seg))
        .collect::<Vec<_>>()
        .join("/");

    let ext = std::path::Path::new(&clean)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    match ext.as_str() {
        "parquet" => Ok(FileFormat::Parquet),
        "csv" => Ok(FileFormat::Csv),
        "lance" => Ok(FileFormat::Lance),
        other => Err(Error::InvalidInput {
            message: format!(
                "Unsupported file format '.{other}'. \
                 Supported formats: .parquet, .csv, .lance"
            ),
        }),
    }
}

enum FileSourceInner {
    DataFusion {
        ctx: Arc<SessionContext>,
        pattern: String,
        format: FileFormat,
    },
    Lance {
        uri: String,
    },
}

impl std::fmt::Debug for FileSourceInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DataFusion {
                pattern, format, ..
            } => f
                .debug_struct("DataFusion")
                .field("pattern", pattern)
                .field("format", format)
                .finish(),
            Self::Lance { uri } => f.debug_struct("Lance").field("uri", uri).finish(),
        }
    }
}

/// A streaming data source backed by files on disk or object storage.
///
/// Created by [`read_files`] and accepted by [`crate::Table::add`].
#[derive(Debug)]
pub struct FileSource {
    inner: FileSourceInner,
    schema: SchemaRef,
}

impl Scannable for FileSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan_as_stream(&mut self) -> SendableRecordBatchStream {
        let schema = self.schema.clone();

        match &self.inner {
            FileSourceInner::DataFusion {
                ctx,
                pattern,
                format,
            } => {
                let ctx = ctx.clone();
                let pattern = pattern.clone();
                let format = *format;

                let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(16);

                tokio::spawn(async move {
                    let df_result = match format {
                        FileFormat::Parquet => ctx
                            .read_parquet(&pattern, ParquetReadOptions::default())
                            .await
                            .map_err(Error::from),
                        FileFormat::Csv => ctx
                            .read_csv(&pattern, CsvReadOptions::default())
                            .await
                            .map_err(Error::from),
                        FileFormat::Lance => unreachable!(),
                    };

                    match df_result {
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                        }
                        Ok(df) => match df.execute_stream().await {
                            Err(e) => {
                                let _ = tx.send(Err(e.into())).await;
                            }
                            Ok(mut stream) => {
                                while let Some(result) = stream.next().await {
                                    if tx.send(result.map_err(Into::into)).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        },
                    }
                });

                let stream = futures::stream::unfold(rx, |mut rx| async move {
                    rx.recv().await.map(|batch| (batch, rx))
                })
                .fuse();

                Box::pin(SimpleRecordBatchStream { schema, stream })
            }

            FileSourceInner::Lance { uri } => {
                let uri = uri.clone();

                let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(16);

                tokio::spawn(async move {
                    match DatasetBuilder::from_uri(&uri).load().await {
                        Err(e) => {
                            let _ = tx.send(Err(Error::Lance { source: e })).await;
                        }
                        Ok(dataset) => match dataset.scan().try_into_stream().await {
                            Err(e) => {
                                let _ = tx.send(Err(Error::Lance { source: e })).await;
                            }
                            Ok(mut stream) => {
                                while let Some(result) = stream.next().await {
                                    if tx
                                        .send(result.map_err(|e| Error::Lance { source: e }))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                            }
                        },
                    }
                });

                let stream = futures::stream::unfold(rx, |mut rx| async move {
                    rx.recv().await.map(|batch| (batch, rx))
                })
                .fuse();

                Box::pin(SimpleRecordBatchStream { schema, stream })
            }
        }
    }

    fn rescannable(&self) -> bool {
        // Files on disk can always be re-read from the beginning.
        true
    }
}

/// Read files matching a glob pattern or path as a streaming data source.
///
/// The format is auto-detected from the file extension:
/// - `.parquet` — Apache Parquet
/// - `.csv` — Comma-separated values
/// - `.lance` — Lance dataset (single path only, glob not supported)
///
/// The returned [`FileSource`] can be passed directly to [`crate::Table::add`].
///
/// # Example
///
/// ```no_run
/// # async fn example(table: &lancedb::Table) -> lancedb::Result<()> {
/// // Single file
/// let data = lancedb::read_files("./data/records.parquet").await?;
/// table.add(data).execute().await?;
///
/// // Glob pattern
/// let data = lancedb::read_files("./data/*.parquet").await?;
/// table.add(data).execute().await?;
/// # Ok(())
/// # }
/// ```
pub async fn read_files(pattern: impl Into<String>) -> Result<FileSource> {
    let pattern = pattern.into();
    let format = detect_format(&pattern)?;

    match format {
        FileFormat::Parquet | FileFormat::Csv => {
            let ctx = Arc::new(make_session_context());
            let df = match format {
                FileFormat::Parquet => ctx
                    .read_parquet(&pattern, ParquetReadOptions::default())
                    .await
                    .map_err(Error::from)?,
                FileFormat::Csv => ctx
                    .read_csv(&pattern, CsvReadOptions::default())
                    .await
                    .map_err(Error::from)?,
                FileFormat::Lance => unreachable!(),
            };
            let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
            Ok(FileSource {
                inner: FileSourceInner::DataFusion {
                    ctx,
                    pattern,
                    format,
                },
                schema,
            })
        }
        FileFormat::Lance => {
            if pattern.contains('*') || pattern.contains('?') || pattern.contains('[') {
                return Err(Error::InvalidInput {
                    message: "Glob patterns are not supported for Lance format. \
                              Provide a single Lance dataset path."
                        .to_string(),
                });
            }
            let dataset = DatasetBuilder::from_uri(&pattern)
                .load()
                .await
                .map_err(|e| Error::Lance { source: e })?;
            let schema: SchemaRef = Arc::new(dataset.schema().into());
            Ok(FileSource {
                inner: FileSourceInner::Lance { uri: pattern },
                schema,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::parquet::arrow::ArrowWriter;
    use futures::TryStreamExt;
    use tempfile::TempDir;

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    fn write_parquet(dir: &TempDir, filename: &str, batch: &RecordBatch) -> String {
        let path = dir.path().join(filename);
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        path.to_str().unwrap().to_owned()
    }

    fn write_csv(dir: &TempDir, filename: &str) -> String {
        let path = dir.path().join(filename);
        // Write CSV manually — avoids needing a separate CSV writer dependency.
        std::fs::write(&path, "id,name\n1,a\n2,b\n3,c\n").unwrap();
        path.to_str().unwrap().to_owned()
    }

    async fn collect_source(source: &mut FileSource) -> Vec<RecordBatch> {
        source
            .scan_as_stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_detect_format_single_file() {
        assert_eq!(detect_format("file.parquet").unwrap(), FileFormat::Parquet);
        assert_eq!(detect_format("file.csv").unwrap(), FileFormat::Csv);
        assert_eq!(detect_format("dataset.lance").unwrap(), FileFormat::Lance);
    }

    #[tokio::test]
    async fn test_detect_format_glob() {
        assert_eq!(
            detect_format("data/*.parquet").unwrap(),
            FileFormat::Parquet
        );
        assert_eq!(detect_format("data/*.csv").unwrap(), FileFormat::Csv);
        assert_eq!(
            detect_format("s3://bucket/prefix/*.parquet").unwrap(),
            FileFormat::Parquet
        );
    }

    #[tokio::test]
    async fn test_detect_format_unknown_returns_error() {
        let err = detect_format("file.json").unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
        assert!(err.to_string().contains(".json"));
    }

    #[tokio::test]
    async fn test_read_single_parquet() {
        let dir = TempDir::new().unwrap();
        let batch = make_batch();
        let path = write_parquet(&dir, "test.parquet", &batch);

        let mut source = read_files(&path).await.unwrap();

        assert_eq!(source.schema().fields().len(), 2);
        assert!(source.rescannable());

        let batches = collect_source(&mut source).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_read_single_csv() {
        let dir = TempDir::new().unwrap();
        let path = write_csv(&dir, "test.csv");

        let mut source = read_files(&path).await.unwrap();

        assert_eq!(source.schema().fields().len(), 2);
        assert!(source.rescannable());

        let batches = collect_source(&mut source).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_read_parquet_glob() {
        let dir = TempDir::new().unwrap();
        let batch = make_batch();
        write_parquet(&dir, "part1.parquet", &batch);
        write_parquet(&dir, "part2.parquet", &batch);

        let pattern = format!("{}/*.parquet", dir.path().to_str().unwrap());
        let mut source = read_files(&pattern).await.unwrap();

        assert!(source.rescannable());

        let batches = collect_source(&mut source).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 6); // 3 rows × 2 files
    }

    #[tokio::test]
    async fn test_read_is_rescannable() {
        let dir = TempDir::new().unwrap();
        let batch = make_batch();
        let path = write_parquet(&dir, "test.parquet", &batch);

        let mut source = read_files(&path).await.unwrap();

        // First scan
        let batches1 = collect_source(&mut source).await;
        let rows1: usize = batches1.iter().map(|b| b.num_rows()).sum();

        // Second scan — must succeed
        let batches2 = collect_source(&mut source).await;
        let rows2: usize = batches2.iter().map(|b| b.num_rows()).sum();

        assert_eq!(rows1, rows2);
        assert_eq!(rows1, 3);
    }

    #[tokio::test]
    async fn test_read_lance() {
        let dir = TempDir::new().unwrap();
        let uri = dir.path().to_str().unwrap().to_owned() + "/test.lance";

        // Create a lance dataset directly.
        let batch = make_batch();
        let schema = batch.schema();
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        lance::dataset::Dataset::write(reader, &uri, Some(lance::dataset::WriteParams::default()))
            .await
            .unwrap();

        let mut source = read_files(&uri).await.unwrap();

        assert_eq!(source.schema().fields().len(), 2);
        assert!(source.rescannable());

        let batches = collect_source(&mut source).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_lance_glob_returns_error() {
        let err = read_files("./data/*.lance").await.unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
        assert!(err.to_string().contains("Glob"));
    }

    #[tokio::test]
    async fn test_unknown_format_returns_error() {
        let err = read_files("data.json").await.unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
    }

    #[tokio::test]
    async fn test_add_from_read_files() {
        let dir = TempDir::new().unwrap();
        let batch = make_batch();
        let path = write_parquet(&dir, "test.parquet", &batch);

        let db_dir = TempDir::new().unwrap();
        let db = crate::connect(db_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();

        let source = read_files(&path).await.unwrap();
        let table = db.create_table("test", source).execute().await.unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 3);
    }
}
