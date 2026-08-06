// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Dataset file inspection APIs.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::RecordBatch;
use arrow_array::builder::{
    Int64Builder, StringBuilder, StringDictionaryBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::types::Int32Type;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use either::Either;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt, TryStreamExt};
use lance_table::format::IndexMetadata;
use lance_table::utils::LanceIteratorExtension;
use object_store::path::Path;
use uuid::Uuid;

use crate::Dataset;
use crate::dataset::files::arrow::{TRACKED_FILES_SCHEMA, TrackedFileBatch};
use crate::dataset::files::file_types::FileType;
use crate::dataset::{DATA_DIR, INDICES_DIR, TRANSACTIONS_DIR};
use lance_core::Result;
use lance_table::io::deletion::relative_deletion_file_path;
use lance_table::io::manifest::{read_manifest, read_manifest_indexes};

mod arrow;
mod file_types;

const BATCH_SIZE: usize = 4096;
/// Memory budget for in-flight manifests (estimated in-memory size).
const MANIFEST_MEMORY_BUDGET: usize = 1024 * 1024 * 1024; // 1 GB
/// Estimated ratio of in-memory size to on-disk size for manifests. Found
/// empirically; manifests are protobuf with significant decompression and
/// allocator overhead once parsed.
const MANIFEST_DECOMPRESSION_RATIO: usize = 4;

fn remove_prefix(path: &Path, prefix: &Path) -> Path {
    match path.prefix_match(prefix) {
        Some(parts) => Path::from_iter(parts),
        None => path.clone(),
    }
}

/// A single row destined for the `tracked_files` output.
struct FileRow<'a> {
    version: u64,
    base_uri: Cow<'a, str>,
    path: Cow<'a, str>,
    file_type: FileType,
}

struct ResolvedFileBase<'a> {
    uri: &'a str,
    is_dataset_root: bool,
}

/// Resolve the base a file lives under. Files referenced from a shallow clone
/// carry a `base_id` pointing into `manifest.base_paths`; otherwise they live
/// under this dataset's own root.
fn resolve_file_base<'a>(
    manifest: &'a lance_table::format::Manifest,
    base_id: Option<u32>,
    base_uri: &'a str,
) -> ResolvedFileBase<'a> {
    base_id
        .and_then(|id| manifest.base_paths.get(&id))
        .map(|base_path| ResolvedFileBase {
            uri: base_path.path.as_str(),
            is_dataset_root: base_path.is_dataset_root,
        })
        .unwrap_or(ResolvedFileBase {
            uri: base_uri,
            is_dataset_root: true,
        })
}

fn manifest_file_rows<'a>(
    manifest: &'a lance_table::format::Manifest,
    base_uri: &'a str,
    manifest_path: &'a str,
) -> Box<dyn ExactSizeIterator<Item = FileRow<'a>> + Send + 'a> {
    let mut files = 1;
    let manifest_row = FileRow {
        version: manifest.version,
        base_uri: Cow::Borrowed(base_uri),
        path: Cow::Borrowed(manifest_path),
        file_type: FileType::Manifest,
    };
    let iter = std::iter::once(manifest_row);

    let iter = if let Some(txn_file) = &manifest.transaction_file {
        files += 1;
        let txn_row = FileRow {
            version: manifest.version,
            base_uri: Cow::Borrowed(base_uri),
            path: Cow::Owned(format!("{}/{}", TRANSACTIONS_DIR, txn_file)),
            file_type: FileType::TransactionFile,
        };
        Either::Left(iter.chain(std::iter::once(txn_row)))
    } else {
        Either::Right(iter)
    };

    for fragment in manifest.fragments.iter() {
        files += fragment.files.len();

        if fragment.deletion_file.is_some() {
            files += 1;
        }
    }

    let data_files = manifest.fragments.iter().flat_map(move |fragment| {
        fragment.files.iter().map(move |data_file| {
            let resolved_base = resolve_file_base(manifest, data_file.base_id, base_uri);
            let path = if resolved_base.is_dataset_root {
                Cow::Owned(format!("{}/{}", DATA_DIR, data_file.path))
            } else {
                Cow::Borrowed(data_file.path.as_str())
            };
            FileRow {
                version: manifest.version,
                base_uri: Cow::Borrowed(resolved_base.uri),
                path,
                file_type: FileType::DataFile,
            }
        })
    });

    let deletion_files = manifest.fragments.iter().filter_map(|fragment| {
        fragment.deletion_file.as_ref().map(|del_file| FileRow {
            version: manifest.version,
            base_uri: Cow::Borrowed(resolve_file_base(manifest, del_file.base_id, base_uri).uri),
            path: Cow::Owned(relative_deletion_file_path(fragment.id, del_file)),
            file_type: FileType::DeletionFile,
        })
    });

    Box::new(
        iter.chain(data_files)
            .chain(deletion_files)
            .exact_size(files),
    )
}

fn manifest_file_batches<'a>(
    manifest: &'a lance_table::format::Manifest,
    base_uri: &'a str,
    manifest_path: &'a str,
) -> Box<dyn ExactSizeIterator<Item = Result<RecordBatch>> + Send + 'a> {
    let mut builder = TrackedFileBatch::with_capacity(BATCH_SIZE);

    let mut iter = manifest_file_rows(manifest, base_uri, manifest_path);
    let size = iter.len().div_ceil(BATCH_SIZE);

    let mut flushed = false;
    Box::new(
        std::iter::from_fn(move || {
            if flushed {
                return None;
            }
            while let Some(row) = iter.next() {
                builder.append(&row);
                if builder.len() == BATCH_SIZE {
                    let next_size = iter.len().div_ceil(BATCH_SIZE);
                    let old_builder =
                        std::mem::replace(&mut builder, TrackedFileBatch::with_capacity(next_size));
                    return Some(old_builder.finish());
                }
            }
            // Flush the remaining partial batch.
            flushed = true;
            if builder.len() != 0 {
                let partial = std::mem::replace(&mut builder, TrackedFileBatch::with_capacity(0));
                Some(partial.finish())
            } else {
                None
            }
        })
        .exact_size(size),
    )
}

async fn get_index_files(
    uuids: impl IntoIterator<Item = Uuid>,
    base: &Path,
    object_store: &lance_io::object_store::ObjectStore,
    cache: &mut HashMap<Uuid, Vec<object_store::ObjectMeta>>,
) -> Result<Vec<Path>> {
    let uuids: Vec<Uuid> = uuids.into_iter().collect();

    // Phase 1: list uncached UUID directories concurrently.
    let uncached: Vec<Uuid> = uuids
        .iter()
        .filter(|uuid| !cache.contains_key(*uuid))
        .copied()
        .collect();
    if !uncached.is_empty() {
        let parallelism = object_store.io_parallelism();
        // Clone for use in async move closures (ObjectStore is Arc-backed).
        let base_owned = base.clone();
        let os = object_store.clone();
        let new_entries: Vec<(Uuid, Vec<object_store::ObjectMeta>)> =
            futures::stream::iter(uncached)
                .map(|uuid| {
                    let base = base_owned.clone();
                    let os = os.clone();
                    async move {
                        let prefix = base.join(INDICES_DIR).join(uuid.to_string());
                        let files: Vec<object_store::ObjectMeta> =
                            os.list(Some(prefix)).try_collect().await?;
                        lance_core::Result::Ok((uuid, files))
                    }
                })
                .buffer_unordered(parallelism)
                .try_collect()
                .await?;

        // Phase 2: insert results into cache (serial, no contention).
        cache.extend(new_entries);
    }

    // Phase 3: collect paths for the requested UUIDs in order.
    let mut paths = Vec::new();
    for uuid in &uuids {
        paths.extend(
            cache[uuid]
                .iter()
                .map(|meta| remove_prefix(&meta.location, base)),
        );
    }
    Ok(paths)
}

async fn index_file_batch(version: u64, base_uri: &str, paths: &[Path]) -> Result<RecordBatch> {
    let mut builder = TrackedFileBatch::with_capacity(paths.len());
    for path in paths {
        builder.append(&FileRow {
            version,
            base_uri: Cow::Borrowed(base_uri),
            path: Cow::Owned(path.to_string()),
            file_type: FileType::IndexFile,
        });
    }
    builder.finish()
}

/// Progress update for [`Dataset::tracked_files_with_options`].
#[derive(Debug, Clone)]
pub struct TrackedFilesProgress {
    /// Number of manifests processed so far.
    pub manifests_processed: usize,
    /// Total number of manifests, if known. This becomes `Some` once the
    /// listing stream is exhausted; until then it is `None`.
    pub manifests_total: Option<usize>,
}

/// Options for [`Dataset::tracked_files_with_options`].
#[derive(Default)]
pub struct TrackedFilesOptions {
    /// If set, only include manifests with `version >= min_version`.
    pub min_version: Option<u64>,
    /// If set, called each time a manifest has been fully processed. The
    /// callback runs on a background tokio task, so it must not block (it
    /// will stall the manifest reader pipeline). Order is the order in which
    /// manifests finish processing, which is not the version order.
    pub progress: Option<Box<dyn Fn(TrackedFilesProgress) + Send + Sync>>,
}

// A `ManifestLocation` is ~100 bytes, so a 50k-slot mpsc channel costs ~5 MB
// in the worst case. That's enough headroom for the lister to run well ahead
// of the reader on datasets with hundreds of thousands of manifests, while
// still bounding memory.
const MAX_BUFFERED_LOCATIONS: usize = 50_000;

impl Dataset {
    /// Returns one row per (version, file) for every file referenced in any manifest.
    ///
    /// Each row contains the manifest version, the storage root URI, the file path
    /// relative to that URI, and the file type.
    ///
    /// # Schema
    ///
    /// | Column     | Type                              | Notes |
    /// |------------|-----------------------------------|-------|
    /// | `version`  | `Int64` (non-null)                | Manifest version number |
    /// | `base_uri` | `Dictionary(Int32, Utf8)` (non-null) | Storage root for this file |
    /// | `path`     | `Utf8` (non-null)                 | Relative to `base_uri` |
    /// | `type`     | `Dictionary(Int8, Utf8)` (non-null)  | One of: `data file`, `manifest`, `deletion file`, `transaction file`, `index file` |
    ///
    /// Output order is non-deterministic.
    pub async fn tracked_files(&self) -> SendableRecordBatchStream {
        self.tracked_files_with_options(TrackedFilesOptions::default())
            .await
    }

    /// Like [`Self::tracked_files`], but with additional options for filtering
    /// and progress reporting.
    pub async fn tracked_files_with_options(
        &self,
        options: TrackedFilesOptions,
    ) -> SendableRecordBatchStream {
        use lance_table::io::commit::ManifestLocation;

        let base = self.base.clone();
        let uri = self.uri().to_string();
        let object_store = self.object_store.clone();
        let commit_handler = self.commit_handler.clone();

        // Pipeline architecture:
        //
        // Lister ──► tx_locations ──► Reader ──┬──► tx_manifest ──► Emitter ──► tx (output)
        //                                      └──► tx_indexes  ──► IndexLister ──► tx (output)

        // Output channel: Emitter and IndexLister both send batches here.
        let (tx, rx) = tokio::sync::mpsc::channel::<datafusion::error::Result<RecordBatch>>(4);
        // Location channel: Lister -> Reader. Large buffer since locations are
        // small (~100 bytes each) and we want the lister to run ahead.
        let (tx_locations, mut rx_locations) =
            tokio::sync::mpsc::channel::<ManifestLocation>(MAX_BUFFERED_LOCATIONS);
        // Manifest channel: Reader -> Emitter (small buffer for backpressure
        // since manifests can be large).
        let (tx_manifest, mut rx_manifest) =
            tokio::sync::mpsc::channel::<(Arc<lance_table::format::Manifest>, String, usize)>(2);
        // Index channel: Reader -> IndexLister.
        let (tx_indexes, mut rx_indexes) =
            tokio::sync::mpsc::channel::<(u64, Vec<IndexMetadata>)>(8);

        // Tracks estimated in-memory size of in-flight manifests. Reader adds
        // before sending; Emitter subtracts after processing.
        let inflight_mem = Arc::new(AtomicUsize::new(0));
        let mem_notify = Arc::new(tokio::sync::Notify::new());

        // Progress: total is set by Lister once listing finishes, read by Emitter.
        let total_manifests: Arc<std::sync::OnceLock<usize>> = Arc::new(std::sync::OnceLock::new());

        // --- Lister task ---
        // Lists manifest locations, applies min_version filter, and counts the
        // total. Locations are lightweight so we buffer up to MAX_BUFFERED_LOCATIONS.
        let tx_err_lister = tx.clone();
        let os_lister = object_store.clone();
        let base_lister = base.clone();
        let total_manifests_lister = total_manifests.clone();
        let min_version = options.min_version;
        tokio::spawn(async move {
            let result: lance_core::Result<()> = async {
                let mut locations =
                    commit_handler.list_manifest_locations(&base_lister, &os_lister, false);
                let mut count = 0usize;
                while let Some(loc) = locations.next().await {
                    let loc = loc?;
                    if let Some(min_v) = min_version
                        && loc.version < min_v
                    {
                        continue;
                    }
                    count += 1;
                    if tx_locations.send(loc).await.is_err() {
                        return Ok(());
                    }
                }
                let _ = total_manifests_lister.set(count);
                Ok(())
            }
            .await;
            if let Err(e) = result {
                let _ = tx_err_lister
                    .send(Err(datafusion::error::DataFusionError::from(e)))
                    .await;
            }
        });

        // --- Reader task ---
        // Reads manifests with memory-aware parallelism and fans out to
        // Emitter (file batches) and IndexLister (index metadata).
        let tx_err_reader = tx.clone();
        let os_reader = object_store.clone();
        let base_reader = base.clone();
        let inflight_mem_reader = inflight_mem.clone();
        let mem_notify_reader = mem_notify.clone();
        tokio::spawn(async move {
            let result: lance_core::Result<()> = async {
                let max_parallelism = os_reader.io_parallelism();

                type ManifestResult = lance_core::Result<(
                    Arc<lance_table::format::Manifest>,
                    String,
                    Vec<IndexMetadata>,
                    usize,
                )>;
                let mut in_flight: FuturesUnordered<
                    std::pin::Pin<Box<dyn Future<Output = ManifestResult> + Send>>,
                > = FuturesUnordered::new();
                let mut locations_exhausted = false;

                loop {
                    let can_launch = !locations_exhausted
                        && in_flight.len() < max_parallelism
                        && (in_flight.is_empty()
                            || inflight_mem_reader.load(Ordering::Acquire)
                                < MANIFEST_MEMORY_BUDGET);

                    if in_flight.is_empty() && !can_launch {
                        break;
                    }

                    tokio::select! {
                        biased;
                        // Always drain completed reads first.
                        Some(item) = in_flight.next(), if !in_flight.is_empty() => {
                            let (manifest, manifest_path, indexes, estimated) = item?;
                            let version = manifest.version;
                            if tx_manifest
                                .send((manifest, manifest_path, estimated))
                                .await
                                .is_err()
                            {
                                return Ok(());
                            }
                            if !indexes.is_empty()
                                && tx_indexes.send((version, indexes)).await.is_err()
                            {
                                return Ok(());
                            }
                        }
                        // Receive next location and start a read.
                        loc = rx_locations.recv(), if can_launch => {
                            match loc {
                                Some(loc) => {
                                    let estimated =
                                        loc.size.unwrap_or(0) as usize
                                            * MANIFEST_DECOMPRESSION_RATIO;
                                    inflight_mem_reader.fetch_add(estimated, Ordering::AcqRel);

                                    let os = os_reader.clone();
                                    let base = base_reader.clone();
                                    in_flight.push(Box::pin(async move {
                                        let manifest =
                                            read_manifest(&os, &loc.path, loc.size).await?;
                                        let indexes =
                                            read_manifest_indexes(&os, &loc, &manifest).await?;
                                        let manifest_path =
                                            remove_prefix(&loc.path, &base).to_string();
                                        lance_core::Result::Ok((
                                            Arc::new(manifest),
                                            manifest_path,
                                            indexes,
                                            estimated,
                                        ))
                                    }));
                                }
                                None => {
                                    locations_exhausted = true;
                                }
                            }
                        }
                        // Wake up when Emitter frees memory.
                        _ = mem_notify_reader.notified(),
                            if !can_launch && !in_flight.is_empty() => {}
                    }
                }
                Ok(())
            }
            .await;

            if let Err(e) = result {
                let _ = tx_err_reader
                    .send(Err(datafusion::error::DataFusionError::from(e)))
                    .await;
            }
        });

        // --- Emitter task ---
        // Converts manifests into file-row batches, releases memory budget,
        // and reports progress.
        let tx_emitter = tx.clone();
        let uri_emitter = uri.clone();
        let progress_cb = options.progress;
        tokio::spawn(async move {
            let mut processed = 0usize;
            while let Some((manifest, manifest_path, estimated)) = rx_manifest.recv().await {
                let batches = manifest_file_batches(&manifest, &uri_emitter, &manifest_path);
                for batch_result in batches {
                    let df_result = batch_result.map_err(datafusion::error::DataFusionError::from);
                    if tx_emitter.send(df_result).await.is_err() {
                        return;
                    }
                }
                drop(manifest);
                inflight_mem.fetch_sub(estimated, Ordering::AcqRel);
                mem_notify.notify_one();

                processed += 1;
                if let Some(ref cb) = progress_cb {
                    cb(TrackedFilesProgress {
                        manifests_processed: processed,
                        manifests_total: total_manifests.get().copied(),
                    });
                }
            }
        });

        // --- IndexLister task ---
        // Lists index directories and emits index file batches.
        let tx_idx = tx;
        let uri_idx = uri;
        let os_idx = object_store;
        let base_idx = base;
        tokio::spawn(async move {
            let mut uuid_cache: HashMap<Uuid, Vec<object_store::ObjectMeta>> = HashMap::new();
            while let Some((version, indexes)) = rx_indexes.recv().await {
                let uuids: Vec<Uuid> = indexes.iter().map(|idx| idx.uuid).collect();
                match get_index_files(uuids, &base_idx, &os_idx, &mut uuid_cache).await {
                    Ok(index_paths) if !index_paths.is_empty() => {
                        match index_file_batch(version, &uri_idx, &index_paths).await {
                            Ok(batch) => {
                                if tx_idx.send(Ok(batch)).await.is_err() {
                                    return;
                                }
                            }
                            Err(e) => {
                                let _ = tx_idx
                                    .send(Err(datafusion::error::DataFusionError::from(e)))
                                    .await;
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx_idx
                            .send(Err(datafusion::error::DataFusionError::from(e)))
                            .await;
                        return;
                    }
                    _ => {}
                }
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        Box::pin(RecordBatchStreamAdapter::new(
            TRACKED_FILES_SCHEMA.clone(),
            stream,
        ))
    }

    /// Returns one row per file that physically exists at the dataset's base URI.
    ///
    /// This scans the primary object store root only. Additional `base_paths`
    /// entries in the manifest (for externally-located data files) are not
    /// scanned by this method.
    ///
    /// # Schema
    ///
    /// | Column          | Type                                       | Notes |
    /// |-----------------|--------------------------------------------|-------|
    /// | `base_uri`      | `Dictionary(Int32, Utf8)` (non-null)       | Storage root |
    /// | `path`          | `Utf8` (non-null)                          | Relative to `base_uri` |
    /// | `size_bytes`    | `Int64` (non-null)                         | File size in bytes |
    /// | `last_modified` | `Timestamp(Microsecond, "UTC")` (non-null) | Last modification time |
    pub async fn all_files(&self) -> SendableRecordBatchStream {
        let base = self.base.clone();
        let uri = self.uri().to_string();
        let object_store = self.object_store.clone();

        let stream = object_store
            .list(Some(base.clone()))
            .try_chunks(4000)
            .map_err(|err| err.1)
            .and_then(
                move |chunk| match build_all_files_batch(&chunk, &base, &uri) {
                    Ok(batch) => futures::future::ok(batch),
                    Err(e) => futures::future::err(e),
                },
            )
            .map_err(datafusion::error::DataFusionError::from);

        Box::pin(RecordBatchStreamAdapter::new(
            arrow::ALL_FILES_SCHEMA.clone(),
            stream,
        ))
    }
}

fn build_all_files_batch(
    chunk: &[object_store::ObjectMeta],
    base: &Path,
    uri: &str,
) -> Result<RecordBatch> {
    let n = chunk.len();
    let mut base_uri_builder = StringDictionaryBuilder::<Int32Type>::with_capacity(n, 1, uri.len());
    let path_capacity = chunk.iter().map(|m| m.location.as_ref().len()).sum();
    let mut path_builder = StringBuilder::with_capacity(n, path_capacity);
    let mut size_builder = Int64Builder::with_capacity(n);
    let mut ts_builder = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");

    for meta in chunk {
        let rel = remove_prefix(&meta.location, base);
        base_uri_builder.append_value(uri);
        path_builder.append_value(rel.as_ref());
        size_builder.append_value(meta.size as i64);
        ts_builder.append_value(meta.last_modified.timestamp_micros());
    }

    RecordBatch::try_new(
        arrow::ALL_FILES_SCHEMA.clone(),
        vec![
            Arc::new(base_uri_builder.finish()),
            Arc::new(path_builder.finish()),
            Arc::new(size_builder.finish()),
            Arc::new(ts_builder.finish()),
        ],
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Dataset;
    use crate::index::DatasetIndexExt;
    use crate::index::vector::VectorIndexParams;
    use arrow_array::{Array, Int32Array, RecordBatchIterator, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use futures::TryStreamExt;
    use lance_index::IndexType;
    use lance_linalg::distance::MetricType;
    use lance_testing::datagen::some_batch;
    use std::collections::HashSet;

    async fn collect_rows(stream: SendableRecordBatchStream) -> Vec<RecordBatch> {
        stream.try_collect::<Vec<_>>().await.unwrap()
    }

    fn count_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    fn dict_value_at(col: &dyn arrow_array::Array, i: usize) -> String {
        if let Some(dict) = col
            .as_any()
            .downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::Int8Type>>()
        {
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            values.value(dict.keys().value(i) as usize).to_string()
        } else if let Some(dict) = col
            .as_any()
            .downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::Int32Type>>()
        {
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            values.value(dict.keys().value(i) as usize).to_string()
        } else {
            panic!("expected a dictionary array with Int8 or Int32 keys");
        }
    }

    fn collect_column_values(batches: &[RecordBatch], col: &str) -> Vec<String> {
        batches
            .iter()
            .flat_map(|b| {
                let col = b.column_by_name(col).unwrap();
                (0..col.len()).map(|i| dict_value_at(col.as_ref(), i))
            })
            .collect()
    }

    fn make_simple_batch() -> impl arrow_array::RecordBatchReader {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        RecordBatchIterator::new(vec![Ok(batch)], schema)
    }

    #[tokio::test]
    async fn test_tracked_files_basic() {
        let uri = "memory://test_tracked_files_basic";

        // Create then append twice to get 3 manifest versions.
        let mut ds = Dataset::write(make_simple_batch(), uri, None)
            .await
            .unwrap();
        ds.append(make_simple_batch(), None).await.unwrap();
        ds.append(make_simple_batch(), None).await.unwrap();

        let stream = ds.tracked_files().await;
        let schema = stream.schema();
        let batches = collect_rows(stream).await;

        // Schema is correct.
        assert_eq!(schema.field(0).name(), "version");
        assert_eq!(schema.field(1).name(), "base_uri");
        assert_eq!(schema.field(2).name(), "path");
        assert_eq!(schema.field(3).name(), "type");

        let n = count_rows(&batches);
        // At minimum: 3 manifests + 3 data files = 6 rows
        assert!(n >= 6, "expected at least 6 rows, got {n}");

        let types: HashSet<String> = collect_column_values(&batches, "type")
            .into_iter()
            .collect();
        assert!(types.contains("manifest"), "missing 'manifest' rows");
        assert!(types.contains("data file"), "missing 'data file' rows");
    }

    #[tokio::test]
    async fn test_tracked_files_deletion() {
        let uri = "memory://test_tracked_files_deletion";

        let mut ds = Dataset::write(make_simple_batch(), uri, None)
            .await
            .unwrap();
        ds.delete("id = 2").await.unwrap();

        let stream = ds.tracked_files().await;
        let batches = collect_rows(stream).await;

        let types: HashSet<String> = collect_column_values(&batches, "type")
            .into_iter()
            .collect();
        assert!(
            types.contains("deletion file"),
            "missing 'deletion file' rows after delete; got types: {:?}",
            types
        );
    }

    #[tokio::test]
    async fn test_tracked_files_transaction() {
        let uri = "memory://test_tracked_files_transaction";

        // Normal writes record transaction files by default.
        let mut ds = Dataset::write(make_simple_batch(), uri, None)
            .await
            .unwrap();
        ds.append(make_simple_batch(), None).await.unwrap();

        let stream = ds.tracked_files().await;
        let batches = collect_rows(stream).await;

        let types: HashSet<String> = collect_column_values(&batches, "type")
            .into_iter()
            .collect();
        assert!(
            types.contains("transaction file"),
            "expected 'transaction file' rows; got types: {:?}",
            types
        );
    }

    #[tokio::test]
    async fn test_tracked_files_index() {
        let uri = "memory://test_tracked_files_index";

        let mut ds = Dataset::write(some_batch(), uri, None).await.unwrap();
        let params = VectorIndexParams::ivf_pq(2, 8, 2, MetricType::L2, 5);
        ds.create_index(&["indexable"], IndexType::Vector, None, &params, true)
            .await
            .unwrap();

        let stream = ds.tracked_files().await;
        let batches = collect_rows(stream).await;

        let types: HashSet<String> = collect_column_values(&batches, "type")
            .into_iter()
            .collect();
        assert!(
            types.contains("index file"),
            "expected 'index file' rows after vector index creation; got types: {:?}",
            types
        );
    }

    fn collect_versions(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                let col = b
                    .column_by_name("version")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap();
                (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn test_tracked_files_min_version() {
        let uri = "memory://test_tracked_files_min_version";

        // Create 3 versions.
        let mut ds = Dataset::write(make_simple_batch(), uri, None)
            .await
            .unwrap();
        ds.append(make_simple_batch(), None).await.unwrap();
        ds.append(make_simple_batch(), None).await.unwrap();

        // Without filter: should have rows from versions 1, 2, 3.
        let stream = ds.tracked_files().await;
        let all_batches = collect_rows(stream).await;
        let all_versions: HashSet<i64> = collect_versions(&all_batches).into_iter().collect();
        assert!(all_versions.contains(&1));
        assert!(all_versions.contains(&2));
        assert!(all_versions.contains(&3));

        // With min_version=3: should only have version 3.
        let stream = ds
            .tracked_files_with_options(TrackedFilesOptions {
                min_version: Some(3),
                ..Default::default()
            })
            .await;
        let filtered_batches = collect_rows(stream).await;
        let filtered_versions: HashSet<i64> =
            collect_versions(&filtered_batches).into_iter().collect();
        assert_eq!(filtered_versions, HashSet::from([3]));

        // With min_version=2: should have versions 2 and 3.
        let stream = ds
            .tracked_files_with_options(TrackedFilesOptions {
                min_version: Some(2),
                ..Default::default()
            })
            .await;
        let filtered_batches = collect_rows(stream).await;
        let filtered_versions: HashSet<i64> =
            collect_versions(&filtered_batches).into_iter().collect();
        assert_eq!(filtered_versions, HashSet::from([2, 3]));
    }

    #[tokio::test]
    async fn test_tracked_files_progress() {
        let uri = "memory://test_tracked_files_progress";

        let mut ds = Dataset::write(make_simple_batch(), uri, None)
            .await
            .unwrap();
        ds.append(make_simple_batch(), None).await.unwrap();
        ds.append(make_simple_batch(), None).await.unwrap();

        let updates = Arc::new(std::sync::Mutex::new(Vec::new()));
        let updates_clone = updates.clone();

        let stream = ds
            .tracked_files_with_options(TrackedFilesOptions {
                progress: Some(Box::new(move |p| {
                    updates_clone.lock().unwrap().push(p);
                })),
                ..Default::default()
            })
            .await;
        // Consume the full stream to drive all tasks to completion.
        let _batches = collect_rows(stream).await;

        let updates = updates.lock().unwrap();
        // Should have exactly 3 progress updates (one per manifest).
        assert_eq!(updates.len(), 3, "expected 3 progress updates");
        // Processed counts should be monotonically increasing.
        for (i, u) in updates.iter().enumerate() {
            assert_eq!(u.manifests_processed, i + 1);
        }
        // The last update should know the total.
        let last = updates.last().unwrap();
        assert_eq!(last.manifests_total, Some(3));
    }

    fn make_multi_row_batch(rows: usize) -> impl arrow_array::RecordBatchReader {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..rows as i32))],
        )
        .unwrap();
        RecordBatchIterator::new(vec![Ok(batch)], schema)
    }

    /// Multi-fragment scenario: write 6 rows split across 3 fragments, delete
    /// one row to produce a deletion file, then assert that every path
    /// `tracked_files` emits for the latest version actually exists in the
    /// `all_files` listing of the dataset directory.
    #[tokio::test]
    async fn test_tracked_files_paths_match_disk() {
        use crate::dataset::WriteParams;

        let uri = "memory://test_tracked_files_paths_match_disk";

        let write_params = WriteParams {
            max_rows_per_file: 2,
            ..Default::default()
        };
        let mut ds = Dataset::write(make_multi_row_batch(6), uri, Some(write_params))
            .await
            .unwrap();
        // Triggers a deletion file on one of the fragments.
        ds.delete("id = 1").await.unwrap();
        let latest_version = ds.version().version as i64;

        // Sanity-check the multi-fragment setup: 3 data files in the latest manifest.
        assert_eq!(
            ds.get_fragments().len(),
            3,
            "expected 3 fragments from max_rows_per_file=2 over 6 rows"
        );

        let tracked = collect_rows(ds.tracked_files().await).await;
        let all = collect_rows(ds.all_files().await).await;

        let all_paths: HashSet<String> = all
            .iter()
            .flat_map(|b| {
                let col = b
                    .column_by_name("path")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..col.len()).map(|i| col.value(i).to_string())
            })
            .collect();

        // Collect tracked paths grouped by type for the latest version only.
        let mut tracked_at_latest: HashMap<String, Vec<String>> = HashMap::new();
        for batch in &tracked {
            let versions = batch
                .column_by_name("version")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            let paths = batch
                .column_by_name("path")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let types = batch.column_by_name("type").unwrap();
            for i in 0..batch.num_rows() {
                if versions.value(i) == latest_version {
                    tracked_at_latest
                        .entry(dict_value_at(types.as_ref(), i))
                        .or_default()
                        .push(paths.value(i).to_string());
                }
            }
        }

        // Every file tracked at the latest version must exist on disk.
        for (file_type, paths) in &tracked_at_latest {
            for p in paths {
                assert!(
                    all_paths.contains(p),
                    "tracked {file_type} path {p:?} not present in all_files (got {all_paths:?})"
                );
            }
        }

        // The latest manifest references one manifest, 3 data files, and 1 deletion file.
        assert_eq!(
            tracked_at_latest.get("manifest").map(Vec::len),
            Some(1),
            "expected 1 manifest row at latest version"
        );
        assert_eq!(
            tracked_at_latest.get("data file").map(Vec::len),
            Some(3),
            "expected 3 data files at latest version"
        );
        assert_eq!(
            tracked_at_latest.get("deletion file").map(Vec::len),
            Some(1),
            "expected 1 deletion file at latest version"
        );

        // Path shapes are as documented (relative to base_uri, no leading slash).
        for p in tracked_at_latest.get("data file").unwrap() {
            assert!(
                p.starts_with("data/"),
                "data file path {p:?} should start with data/"
            );
        }
        let manifest_path = &tracked_at_latest.get("manifest").unwrap()[0];
        assert!(
            manifest_path.starts_with("_versions/") && manifest_path.ends_with(".manifest"),
            "manifest path {manifest_path:?} should match _versions/<n>.manifest"
        );
        let deletion_path = &tracked_at_latest.get("deletion file").unwrap()[0];
        assert!(
            deletion_path.starts_with("_deletions/"),
            "deletion path {deletion_path:?} should start with _deletions/"
        );
    }

    /// Each `DataFile` inside a fragment carries its own `base_id`; both the
    /// emitted `base_uri` and relative path must honor that base's layout.
    #[test]
    fn test_manifest_file_rows_per_file_base_id() {
        use lance_core::datatypes::{Field as LanceField, Schema as LanceSchema};
        use lance_io::utils::CachedFileSize;
        use lance_table::format::{
            BasePath, DataFile, DataStorageFormat, DeletionFile, DeletionFileType, Fragment,
            Manifest,
        };

        let schema = LanceSchema {
            fields: vec![LanceField::try_from(&Field::new("id", DataType::Int32, false)).unwrap()],
            metadata: Default::default(),
        };

        let mk_file = |path: &str, base_id: Option<u32>| DataFile {
            path: path.to_string(),
            fields: Arc::from(vec![0]),
            column_indices: Arc::from(Vec::<i32>::new()),
            file_major_version: 2,
            file_minor_version: 0,
            file_size_bytes: CachedFileSize::unknown(),
            base_id,
        };

        let fragment = Fragment {
            id: 0,
            files: vec![
                mk_file("a.lance", Some(1)),
                mk_file("b.lance", Some(2)),
                // No base_id -> falls back to the dataset base_uri.
                mk_file("c.lance", None),
            ],
            overlays: vec![],
            // Deletion files also carry a base_id when they originate from a
            // shallow clone, and must resolve against base_paths too.
            deletion_file: Some(DeletionFile {
                read_version: 1,
                id: 7,
                file_type: DeletionFileType::Bitmap,
                num_deleted_rows: Some(1),
                base_id: Some(2),
            }),
            row_id_meta: None,
            physical_rows: Some(3),
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        };

        let mut base_paths = HashMap::new();
        base_paths.insert(
            1,
            BasePath::new(1, "s3://bucket-a/root".to_string(), None, false),
        );
        base_paths.insert(
            2,
            BasePath::new(2, "s3://bucket-b/root".to_string(), None, true),
        );

        let manifest = Manifest::new(
            schema,
            Arc::new(vec![fragment]),
            DataStorageFormat::default(),
            base_paths,
        );

        let rows: Vec<_> =
            manifest_file_rows(&manifest, "memory://main", "_versions/1.manifest").collect();
        let by_path: HashMap<&str, &str> = rows
            .iter()
            .filter(|r| matches!(r.file_type, FileType::DataFile))
            .map(|r| (r.path.as_ref(), r.base_uri.as_ref()))
            .collect();

        assert_eq!(by_path.get("a.lance"), Some(&"s3://bucket-a/root"));
        assert_eq!(by_path.get("data/b.lance"), Some(&"s3://bucket-b/root"));
        assert_eq!(by_path.get("data/c.lance"), Some(&"memory://main"));

        let deletion = rows
            .iter()
            .find(|r| matches!(r.file_type, FileType::DeletionFile))
            .expect("deletion file row");
        assert_eq!(deletion.path.as_ref(), "_deletions/0-1-7.bin");
        assert_eq!(deletion.base_uri.as_ref(), "s3://bucket-b/root");
    }

    #[tokio::test]
    async fn test_all_files_basic() {
        let uri = "memory://test_all_files_basic";
        let ds = Dataset::write(make_simple_batch(), uri, None)
            .await
            .unwrap();

        let stream = ds.all_files().await;
        let schema = stream.schema();
        let batches = collect_rows(stream).await;

        assert_eq!(schema.field(0).name(), "base_uri");
        assert_eq!(schema.field(1).name(), "path");
        assert_eq!(schema.field(2).name(), "size_bytes");
        assert_eq!(schema.field(3).name(), "last_modified");

        let n = count_rows(&batches);
        // A dataset always has at least a manifest and a data file.
        assert!(n >= 2, "expected at least 2 physical files, got {n}");

        // Verify sizes and timestamps are populated (non-zero).
        for batch in &batches {
            let sizes = batch
                .column_by_name("size_bytes")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            for i in 0..sizes.len() {
                assert!(
                    sizes.value(i) > 0,
                    "size_bytes should be positive, got {}",
                    sizes.value(i)
                );
            }

            let ts = batch
                .column_by_name("last_modified")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                .unwrap();
            for i in 0..ts.len() {
                assert!(
                    ts.value(i) > 0,
                    "last_modified should be positive, got {}",
                    ts.value(i)
                );
            }
        }
    }

    #[tokio::test]
    async fn test_all_files_schema() {
        let uri = "memory://test_all_files_schema";
        let ds = Dataset::write(make_simple_batch(), uri, None)
            .await
            .unwrap();

        let stream = ds.all_files().await;
        let schema = stream.schema();

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "base_uri");
        assert!(matches!(
            schema.field(0).data_type(),
            DataType::Dictionary(_, _)
        ));
        assert_eq!(schema.field(1).name(), "path");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).name(), "size_bytes");
        assert_eq!(schema.field(2).data_type(), &DataType::Int64);
        assert_eq!(schema.field(3).name(), "last_modified");
        assert!(matches!(
            schema.field(3).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ));
    }
}
