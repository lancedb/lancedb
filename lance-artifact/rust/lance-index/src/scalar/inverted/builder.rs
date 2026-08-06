// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::{InvertedIndexParams, index::*};
use crate::scalar::inverted::document_tokenizer::DocType;
use crate::scalar::inverted::json::JsonTextStream;
use crate::scalar::inverted::tokenizer::LEGACY_BLOCK_SIZE;
use crate::scalar::inverted::tokenizer::document_tokenizer::LanceTokenizer;
#[cfg(test)]
use crate::scalar::lance_format::LanceIndexStore;
use crate::scalar::{IndexFile, IndexStore, OldIndexDataFilter};
use crate::vector::graph::OrderedFloat;
use crate::{progress::IndexBuildProgress, progress::noop_progress};
use arrow::array::AsArray;
use arrow::datatypes;
use arrow_array::{Array, BinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use fst::Streamer;
use futures::{StreamExt, TryStreamExt};
use lance_arrow::json::JSON_EXT_NAME;
use lance_arrow::{ARROW_EXT_NAME_KEY, iter_str_array};
use lance_bitpacking::{BitPacker, BitPacker4x};
use lance_core::cache::LanceCache;
use lance_core::deepsize::DeepSizeOf;
use lance_core::error::LanceOptionExt;
use lance_core::utils::row_addr_remap::RowAddrRemap;
use lance_core::utils::tokio::{IO_CORE_RESERVATION, get_num_compute_intensive_cpus, spawn_cpu};
use lance_core::{Error, ROW_ID, Result};
use lance_io::object_store::ObjectStore;
use lance_select::RowSetOps;
use object_store::path::Path;
use roaring::RoaringBitmap;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::LazyLock;
use std::{fmt::Debug, sync::atomic::AtomicU64};
use tracing::instrument;

// The legacy bitpacking block size. Position streams still use this block size;
// FTS posting blocks choose their physical bitpacker from the configured
// InvertedIndexParams::block_size.
pub const BLOCK_SIZE: usize = BitPacker4x::BLOCK_LEN;

// The default number of workers to use for FTS builds.
// By default this is roughly `num_cpus / 2`, but it can be overridden
// with `LANCE_FTS_NUM_SHARDS`.
pub static LANCE_FTS_NUM_SHARDS: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("LANCE_FTS_NUM_SHARDS")
        .unwrap_or_else(|_| default_num_workers().to_string())
        .parse()
        .expect("failed to parse LANCE_FTS_NUM_SHARDS")
});
// The default per-worker memory limit in MiB for FTS builds.
pub static LANCE_FTS_PARTITION_SIZE: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("LANCE_FTS_PARTITION_SIZE")
        .unwrap_or_else(|_| "2048".to_string())
        .parse()
        .expect("failed to parse LANCE_FTS_PARTITION_SIZE")
});
static LANCE_FTS_WRITE_QUEUE_SIZE: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("LANCE_FTS_WRITE_QUEUE_SIZE")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .expect("failed to parse LANCE_FTS_WRITE_QUEUE_SIZE")
});
static LANCE_FTS_POSTING_BATCH_ROWS: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("LANCE_FTS_POSTING_BATCH_ROWS")
        .unwrap_or_else(|_| "256".to_string())
        .parse()
        .expect("failed to parse LANCE_FTS_POSTING_BATCH_ROWS")
});
const MAX_RETAINED_TOKEN_IDS: usize = 8 * 1024;

fn default_num_workers() -> usize {
    let total_cpus = get_num_compute_intensive_cpus() + *IO_CORE_RESERVATION;
    std::cmp::max(1, total_cpus / 2)
}

fn resolve_num_workers(params: &InvertedIndexParams) -> usize {
    let max_workers = get_num_compute_intensive_cpus().max(1);
    params
        .num_workers
        .unwrap_or(*LANCE_FTS_NUM_SHARDS)
        .clamp(1, max_workers)
}

fn resolve_worker_memory_limit_bytes(params: &InvertedIndexParams, num_workers: usize) -> u64 {
    let default_worker_memory_limit_bytes = *LANCE_FTS_PARTITION_SIZE << 20;
    params
        .memory_limit_mb
        .map(|memory_limit_mb| (memory_limit_mb << 20) / num_workers as u64)
        .unwrap_or(default_worker_memory_limit_bytes)
}

/// Merge the workers' leftover tail builders into as few partitions as the
/// memory budget allows. Folding unconditionally would collapse every build
/// whose workers never hit the flush threshold into a single partition, which
/// destroys intra-query parallelism; splitting by the same per-partition
/// budget as the flush path makes the final partition count converge to
/// roughly total_builder_memory / memory_limit_bytes regardless of worker
/// layout.
fn merge_all_tail_partitions(
    tails: Vec<TailPartition>,
    memory_limit_bytes: u64,
) -> Result<Vec<InnerBuilder>> {
    let mut merged_builders: Vec<InnerBuilder> = Vec::new();
    let mut merged: Option<InnerBuilder> = None;
    let mut empty_coordinate_builder: Option<InnerBuilder> = None;
    for tail in tails {
        let builder = tail.builder;
        if builder.is_empty() {
            if builder.docs.coordinate_rank() > 0 && empty_coordinate_builder.is_none() {
                empty_coordinate_builder = Some(builder);
            }
            continue;
        }
        match &mut merged {
            Some(current) => {
                let would_exceed_memory =
                    current.memory_size().saturating_add(builder.memory_size())
                        >= memory_limit_bytes;
                let would_exceed_doc_ids =
                    current.docs.len().saturating_add(builder.docs.len()) > u32::MAX as usize;
                if would_exceed_memory || would_exceed_doc_ids {
                    merged_builders.push(std::mem::replace(current, builder));
                } else {
                    current.merge_from(builder)?;
                }
            }
            None => merged = Some(builder),
        }
    }
    if let Some(builder) = merged {
        merged_builders.push(builder);
    }
    if merged_builders.is_empty()
        && let Some(builder) = empty_coordinate_builder
    {
        merged_builders.push(builder);
    }
    Ok(merged_builders)
}

#[derive(Debug)]
pub struct InvertedIndexBuilder {
    params: InvertedIndexParams,
    pub(crate) partitions: Vec<u64>,
    new_partitions: Vec<u64>,
    fragment_mask: Option<u64>,
    token_set_format: TokenSetFormat,
    format_version: InvertedListFormatVersion,
    posting_tail_codec: PostingTailCodec,
    src_store: Option<Arc<dyn IndexStore>>,
    progress: Arc<dyn IndexBuildProgress>,
    deleted_fragments: RoaringBitmap,
}

impl InvertedIndexBuilder {
    pub fn new(params: InvertedIndexParams) -> Self {
        Self::new_with_fragment_mask(params, None)
    }

    pub fn new_with_fragment_mask(params: InvertedIndexParams, fragment_mask: Option<u64>) -> Self {
        Self::from_existing_index(
            params,
            None,
            Vec::new(),
            TokenSetFormat::default(),
            fragment_mask,
            RoaringBitmap::new(),
        )
    }

    /// Creates an InvertedIndexBuilder from existing index with fragment filtering.
    /// This method is used to create a builder from an existing index while applying
    /// fragment-based filtering for distributed indexing scenarios.
    /// fragment_mask Optional mask with fragment_id in high 32 bits for filtering.
    /// Constructed as `(fragment_id as u64) << 32`.
    /// When provided, ensures that generated IDs belong to the specified fragment.
    pub fn from_existing_index(
        params: InvertedIndexParams,
        store: Option<Arc<dyn IndexStore>>,
        partitions: Vec<u64>,
        token_set_format: TokenSetFormat,
        fragment_mask: Option<u64>,
        deleted_fragments: RoaringBitmap,
    ) -> Self {
        let format_version = params.resolved_format_version();
        Self {
            params,
            partitions,
            new_partitions: Vec::new(),
            src_store: store,
            token_set_format,
            fragment_mask,
            format_version,
            posting_tail_codec: format_version.posting_tail_codec(),
            progress: noop_progress(),
            deleted_fragments,
        }
    }

    pub fn with_posting_tail_codec(mut self, posting_tail_codec: PostingTailCodec) -> Self {
        self.format_version = InvertedListFormatVersion::from_posting_tail_codec_and_block_size(
            posting_tail_codec,
            self.params.block_size,
        )
        .expect("invalid posting tail codec for posting block size");
        self.posting_tail_codec = posting_tail_codec;
        self
    }

    pub fn with_format_version(mut self, format_version: InvertedListFormatVersion) -> Self {
        self.format_version = format_version;
        self.posting_tail_codec = format_version.posting_tail_codec();
        self
    }

    pub fn with_token_set_format(mut self, token_set_format: TokenSetFormat) -> Self {
        self.token_set_format = token_set_format;
        self
    }

    pub fn with_progress(mut self, progress: Arc<dyn IndexBuildProgress>) -> Self {
        self.progress = progress;
        self
    }

    pub async fn update(
        &mut self,
        new_data: SendableRecordBatchStream,
        dest_store: &dyn IndexStore,
        old_data_filter: Option<crate::scalar::OldIndexDataFilter>,
    ) -> Result<Vec<IndexFile>> {
        validate_format_version_block_size(self.format_version, self.params.block_size)?;
        let schema = new_data.schema();
        let doc_col = schema.field(0).name();

        // infer lance_tokenizer based on document type
        if self.params.lance_tokenizer.is_none() {
            let schema = new_data.schema();
            let field = schema.column_with_name(doc_col).expect_ok()?.1;
            let doc_type = DocType::try_from(field)?;
            self.params.lance_tokenizer = Some(doc_type.as_ref().to_string());
        }

        let new_data = document_input(new_data, doc_col)?;

        self.progress
            .stage_start("tokenize_docs", None, "rows")
            .await?;
        let mut files = self.update_index(new_data, dest_store).await?;

        if let Some(OldIndexDataFilter::Fragments { to_remove, .. }) = old_data_filter {
            self.deleted_fragments.extend(to_remove);
        }

        self.progress.stage_complete("tokenize_docs").await?;
        files.extend(self.write(dest_store).await?);
        Ok(files)
    }

    pub async fn update_from_segments(
        &mut self,
        new_data: SendableRecordBatchStream,
        dest_store: &dyn IndexStore,
        old_segments: &[Arc<InvertedIndex>],
        old_data_filter: Option<crate::scalar::OldIndexDataFilter>,
    ) -> Result<Vec<IndexFile>> {
        validate_format_version_block_size(self.format_version, self.params.block_size)?;
        let schema = new_data.schema();
        let doc_col = schema.field(0).name();

        if self.params.lance_tokenizer.is_none() {
            let field = schema.column_with_name(doc_col).expect_ok()?.1;
            let doc_type = DocType::try_from(field)?;
            self.params.lance_tokenizer = Some(doc_type.as_ref().to_string());
        }

        let mut files = self
            .merge_existing_segments(dest_store, old_segments, old_data_filter.as_ref())
            .await?;

        let new_data = document_input(new_data, doc_col)?;

        self.progress
            .stage_start("tokenize_docs", None, "rows")
            .await?;
        files.extend(self.update_index(new_data, dest_store).await?);
        self.progress.stage_complete("tokenize_docs").await?;

        files.extend(self.write(dest_store).await?);
        Ok(files)
    }

    async fn merge_existing_segments(
        &mut self,
        dest_store: &dyn IndexStore,
        old_segments: &[Arc<InvertedIndex>],
        old_data_filter: Option<&crate::scalar::OldIndexDataFilter>,
    ) -> Result<Vec<IndexFile>> {
        let num_workers = resolve_num_workers(&self.params);
        let memory_limit_bytes = resolve_worker_memory_limit_bytes(&self.params, num_workers);
        let mut merged: Option<InnerBuilder> = None;
        let mut files = Vec::new();
        for index in old_segments {
            if old_data_filter.is_none() {
                self.deleted_fragments
                    .extend(index.deleted_fragments().iter());
            }
            for partition in &index.partitions {
                let mut partition_builder = partition.as_ref().clone().into_builder().await?;
                if let Some(filter) = old_data_filter {
                    partition_builder.filter_old_data(filter).await?;
                }
                if partition_builder.is_empty() {
                    continue;
                }
                match &mut merged {
                    Some(merged) => {
                        let would_exceed_memory = merged
                            .memory_size()
                            .saturating_add(partition_builder.memory_size())
                            >= memory_limit_bytes;
                        let would_exceed_doc_ids = merged
                            .docs
                            .len()
                            .saturating_add(partition_builder.docs.len())
                            > u32::MAX as usize;
                        if would_exceed_memory || would_exceed_doc_ids {
                            let builder = std::mem::replace(merged, partition_builder);
                            files.extend(self.write_new_partition(dest_store, builder).await?);
                        } else {
                            merged.merge_from(partition_builder)?;
                        }
                    }
                    None => merged = Some(partition_builder),
                }
            }
        }

        if let Some(builder) = merged {
            files.extend(self.write_new_partition(dest_store, builder).await?);
        }
        Ok(files)
    }

    async fn write_new_partition(
        &mut self,
        dest_store: &dyn IndexStore,
        mut builder: InnerBuilder,
    ) -> Result<Vec<IndexFile>> {
        let partition_id = self.next_partition_id() | self.fragment_mask.unwrap_or(0);
        builder.set_id(partition_id);
        let files = builder
            .write_to(dest_store, self.partition_write_target())
            .await?;
        self.new_partitions.push(partition_id);
        Ok(files)
    }

    fn partition_write_target(&self) -> PartitionWriteTarget {
        if self.fragment_mask.is_some() {
            PartitionWriteTarget::Staged
        } else {
            PartitionWriteTarget::Final
        }
    }

    fn next_partition_id(&self) -> u64 {
        self.partitions
            .iter()
            .chain(self.new_partitions.iter())
            .map(|id| id + 1)
            .max()
            .unwrap_or(0)
    }

    #[instrument(level = "debug", skip_all)]
    async fn update_index(
        &mut self,
        stream: SendableRecordBatchStream,
        dest_store: &dyn IndexStore,
    ) -> Result<Vec<IndexFile>> {
        let num_workers = resolve_num_workers(&self.params);
        let tokenizer = self.params.build()?;
        let with_position = self.params.with_position;
        let worker_memory_limit_bytes =
            resolve_worker_memory_limit_bytes(&self.params, num_workers);
        let worker_config = IndexWorkerConfig {
            with_position,
            format_version: self.format_version,
            fragment_mask: self.fragment_mask,
            token_set_format: self.token_set_format,
            worker_memory_limit_bytes,
            block_size: self.params.block_size,
            coordinate_rank: document_coordinate_rank(&stream.schema()),
        };
        let next_id = self.next_partition_id();
        let id_alloc = Arc::new(AtomicU64::new(next_id));
        let tokenized_count = Arc::new(AtomicU64::new(0));
        let (sender, receiver) = async_channel::bounded(num_workers);
        let dest_store = dest_store.clone_arc();
        let mut index_tasks = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let tokenizer = tokenizer.clone();
            let receiver: async_channel::Receiver<RecordBatch> = receiver.clone();
            let dest_store = dest_store.clone();
            let id_alloc = id_alloc.clone();
            let progress = self.progress.clone();
            let tokenized_count = tokenized_count.clone();
            index_tasks.push(tokio::task::spawn(async move {
                let mut worker =
                    IndexWorker::new(tokenizer, dest_store, id_alloc, worker_config).await?;
                while let Ok(batch) = receiver.recv().await {
                    let num_rows = batch.num_rows();
                    worker.process_batch(batch).await?;
                    let tokenized_count = tokenized_count
                        .fetch_add(num_rows as u64, std::sync::atomic::Ordering::Relaxed)
                        + num_rows as u64;
                    progress
                        .stage_progress("tokenize_docs", tokenized_count)
                        .await?;
                }
                worker.finish().await
            }));
        }

        let index_build = async {
            // Keep the channel lifetime tied to the worker tasks so senders observe
            // worker exits instead of blocking on an orphaned receiver handle.
            drop(receiver);

            let mut stream = Box::pin(stream);
            log::info!("indexing FTS with {} workers", num_workers);

            let mut last_num_rows = 0;
            let mut total_num_rows = 0;
            let start = std::time::Instant::now();
            while let Some(batch) = stream.try_next().await? {
                let num_rows = batch.num_rows();

                if sender.send(batch).await.is_err() {
                    // this only happens if all workers have exited,
                    // so we don't return the send error here,
                    // avoiding hiding the real error from workers.
                    break;
                }

                total_num_rows += num_rows;
                if total_num_rows >= last_num_rows + 1_000_000 {
                    log::debug!(
                        "indexed {} documents, elapsed: {:?}, speed: {}rows/s",
                        total_num_rows,
                        start.elapsed(),
                        total_num_rows as f32 / start.elapsed().as_secs_f32()
                    );
                    last_num_rows = total_num_rows;
                }
            }
            // drop the sender to stop receivers
            drop(stream);
            drop(sender);
            log::info!("dispatching elapsed: {:?}", start.elapsed());

            // wait for the workers to finish
            let start = std::time::Instant::now();
            let mut tail_partitions = Vec::new();
            let mut files = Vec::new();
            for index_task in index_tasks {
                let output = index_task.await??;
                self.new_partitions.extend(output.partitions);
                files.extend(output.files);
                if let Some(tail_partition) = output.tail_partition {
                    tail_partitions.push(tail_partition);
                }
            }
            let merged_tail_partitions = spawn_cpu(move || {
                merge_all_tail_partitions(tail_partitions, worker_memory_limit_bytes)
            })
            .await?;
            // Tail partitions hold most of the data when workers rarely hit the
            // flush threshold; writing them one at a time serializes the
            // posting-list compression of nearly the whole index behind a
            // single producer thread. Compress and write them concurrently.
            let write_target = self.partition_write_target();
            let mut tail_writes =
                futures::stream::iter(merged_tail_partitions.into_iter().map(|mut builder| {
                    let dest_store = dest_store.clone();
                    async move {
                        let partition_id = builder.id();
                        let files = builder.write_to(dest_store.as_ref(), write_target).await?;
                        Result::Ok((partition_id, files))
                    }
                }))
                .buffer_unordered(get_num_compute_intensive_cpus().clamp(1, 16));
            while let Some((partition_id, partition_files)) = tail_writes.try_next().await? {
                self.new_partitions.push(partition_id);
                files.extend(partition_files);
            }
            log::info!("wait workers indexing elapsed: {:?}", start.elapsed());
            Result::Ok(files)
        };

        index_build.await
    }

    pub async fn remap(
        &mut self,
        mapping: &RowAddrRemap,
        src_store: Arc<dyn IndexStore>,
        dest_store: &dyn IndexStore,
    ) -> Result<Vec<IndexFile>> {
        let mut files = Vec::new();
        for part in self.partitions.iter() {
            let part = InvertedPartition::load(
                src_store.clone(),
                *part,
                None,
                &LanceCache::no_cache(),
                self.token_set_format,
            )
            .await?;
            let mut builder = part.into_builder().await?;
            builder.remap(mapping).await?;
            files.extend(
                builder
                    .write_to(dest_store, self.partition_write_target())
                    .await?,
            );
        }
        if self.fragment_mask.is_none() {
            files.push(self.write_metadata(dest_store, &self.partitions).await?);
        } else {
            // in distributed mode, the staged partition metadata is written by the worker
            for &partition_id in &self.partitions {
                files.push(self.write_part_metadata(dest_store, partition_id).await?);
            }
        }
        Ok(files)
    }

    async fn write_metadata(
        &self,
        dest_store: &dyn IndexStore,
        partitions: &[u64],
    ) -> Result<IndexFile> {
        validate_format_version_block_size(self.format_version, self.params.block_size)?;
        let mut serialized_deleted_fragments =
            Vec::with_capacity(self.deleted_fragments.serialized_size());
        self.deleted_fragments
            .serialize_into(&mut serialized_deleted_fragments)?;

        let mut metadata = HashMap::from_iter(vec![
            ("partitions".to_owned(), serde_json::to_string(&partitions)?),
            ("params".to_owned(), serde_json::to_string(&self.params)?),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                self.token_set_format.to_string(),
            ),
            (
                POSTING_TAIL_CODEC_KEY.to_owned(),
                self.posting_tail_codec.as_str().to_owned(),
            ),
            (
                FTS_FORMAT_VERSION_KEY.to_owned(),
                self.format_version.index_version().to_string(),
            ),
            (
                POSTING_BLOCK_SIZE_KEY.to_owned(),
                self.params.block_size.to_string(),
            ),
        ]);

        if self.params.with_position && self.format_version.uses_shared_position_stream() {
            metadata.insert(
                POSITIONS_LAYOUT_KEY.to_owned(),
                POSITIONS_LAYOUT_SHARED_STREAM_V2.to_owned(),
            );
            metadata.insert(
                POSITIONS_CODEC_KEY.to_owned(),
                self.format_version
                    .position_codec()
                    .expect("shared positions require a codec")
                    .as_str()
                    .to_owned(),
            );
        }

        let metadata_file_schema = Arc::new(Schema::new(vec![Field::new(
            DELETED_FRAGMENTS_COL,
            DataType::Binary,
            false,
        )]));
        let deleted_fragments_col = Arc::new(BinaryArray::from(vec![
            serialized_deleted_fragments.as_slice(),
        ])) as Arc<dyn Array>;
        let record_batch =
            RecordBatch::try_new(metadata_file_schema.clone(), vec![deleted_fragments_col])?;

        let mut writer = dest_store
            .new_index_file(METADATA_FILE, metadata_file_schema)
            .await?;
        writer.write_record_batch(record_batch).await?;
        writer.finish_with_metadata(metadata).await
    }

    /// Write partition metadata file for a single partition
    ///
    /// In a distributed environment, each worker node can write partition metadata files for the partitions it processes,
    /// which are then merged into a final metadata file using the `merge_metadata_files` function.
    pub(crate) async fn write_part_metadata(
        &self,
        dest_store: &dyn IndexStore,
        partition: u64, // Modify parameter type
    ) -> Result<IndexFile> {
        validate_format_version_block_size(self.format_version, self.params.block_size)?;
        let partitions = vec![partition];
        let mut metadata = HashMap::from_iter(vec![
            ("partitions".to_owned(), serde_json::to_string(&partitions)?),
            ("params".to_owned(), serde_json::to_string(&self.params)?),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                self.token_set_format.to_string(),
            ),
            (
                POSTING_TAIL_CODEC_KEY.to_owned(),
                self.posting_tail_codec.as_str().to_owned(),
            ),
            (
                FTS_FORMAT_VERSION_KEY.to_owned(),
                self.format_version.index_version().to_string(),
            ),
            (
                POSTING_BLOCK_SIZE_KEY.to_owned(),
                self.params.block_size.to_string(),
            ),
        ]);
        if self.params.with_position && self.format_version.uses_shared_position_stream() {
            metadata.insert(
                POSITIONS_LAYOUT_KEY.to_owned(),
                POSITIONS_LAYOUT_SHARED_STREAM_V2.to_owned(),
            );
            metadata.insert(
                POSITIONS_CODEC_KEY.to_owned(),
                self.format_version
                    .position_codec()
                    .expect("shared positions require a codec")
                    .as_str()
                    .to_owned(),
            );
        }
        // Use partition ID to generate a unique temporary filename
        let file_name = part_metadata_file_path(partition);
        let mut writer = dest_store
            .new_index_file(&file_name, Arc::new(Schema::empty()))
            .await?;
        writer.finish_with_metadata(metadata).await
    }

    async fn write_metadata_with_progress(
        &self,
        dest_store: &dyn IndexStore,
        partitions: &[u64],
    ) -> Result<Vec<IndexFile>> {
        let total = if self.fragment_mask.is_none() {
            Some(1)
        } else {
            Some(partitions.len() as u64)
        };
        let mut files = Vec::new();
        self.progress
            .stage_start("write_metadata", total, "files")
            .await?;
        if self.fragment_mask.is_none() {
            files.push(self.write_metadata(dest_store, partitions).await?);
            self.progress.stage_progress("write_metadata", 1).await?;
        } else {
            let mut completed = 0;
            for &partition_id in partitions {
                files.push(self.write_part_metadata(dest_store, partition_id).await?);
                completed += 1;
                self.progress
                    .stage_progress("write_metadata", completed)
                    .await?;
            }
        }
        self.progress.stage_complete("write_metadata").await?;
        Ok(files)
    }

    async fn write(&self, dest_store: &dyn IndexStore) -> Result<Vec<IndexFile>> {
        let mut partitions = Vec::with_capacity(self.partitions.len() + self.new_partitions.len());
        partitions.extend_from_slice(&self.partitions);
        partitions.extend_from_slice(&self.new_partitions);
        partitions.sort_unstable();

        self.progress
            .stage_start(
                "copy_partitions",
                Some(partitions.len() as u64),
                "partitions",
            )
            .await?;
        let mut copied = 0;
        let mut files = Vec::new();
        let target = self.partition_write_target();
        for part in self.partitions.iter() {
            files.push(
                self.src_store
                    .as_ref()
                    .expect("existing partitions require a source store")
                    .copy_index_file_to(
                        &token_file_path(*part),
                        &target.token_path(*part),
                        dest_store,
                    )
                    .await?,
            );
            files.push(
                self.src_store
                    .as_ref()
                    .expect("existing partitions require a source store")
                    .copy_index_file_to(
                        &posting_file_path(*part),
                        &target.posting_path(*part),
                        dest_store,
                    )
                    .await?,
            );
            files.push(
                self.src_store
                    .as_ref()
                    .expect("existing partitions require a source store")
                    .copy_index_file_to(&doc_file_path(*part), &target.doc_path(*part), dest_store)
                    .await?,
            );
            copied += 1;
            self.progress
                .stage_progress("copy_partitions", copied)
                .await?;
        }
        for _part in self.new_partitions.iter() {
            copied += 1;
            self.progress
                .stage_progress("copy_partitions", copied)
                .await?;
        }
        self.progress.stage_complete("copy_partitions").await?;

        files.extend(
            self.write_metadata_with_progress(dest_store, &partitions)
                .await?,
        );
        Ok(files)
    }
}

impl Default for InvertedIndexBuilder {
    fn default() -> Self {
        let params = InvertedIndexParams::default();
        Self::new(params)
    }
}

// builder for single partition
#[derive(Debug)]
pub struct InnerBuilder {
    id: u64,
    with_position: bool,
    token_set_format: TokenSetFormat,
    format_version: InvertedListFormatVersion,
    posting_tail_codec: PostingTailCodec,
    block_size: usize,
    pub(crate) tokens: TokenSet,
    pub(crate) posting_lists: Vec<PostingListBuilder>,
    pub(crate) docs: DocSet,
}

impl InnerBuilder {
    pub fn new(id: u64, with_position: bool, token_set_format: TokenSetFormat) -> Self {
        Self::new_with_format_version(
            id,
            with_position,
            token_set_format,
            current_fts_format_version(),
        )
    }

    pub fn new_with_format_version(
        id: u64,
        with_position: bool,
        token_set_format: TokenSetFormat,
        format_version: InvertedListFormatVersion,
    ) -> Self {
        Self::new_with_format_version_and_block_size(
            id,
            with_position,
            token_set_format,
            format_version,
            LEGACY_BLOCK_SIZE,
        )
    }

    pub fn new_with_block_size(
        id: u64,
        with_position: bool,
        token_set_format: TokenSetFormat,
        block_size: usize,
    ) -> Self {
        let format_version = default_fts_format_version_for_block_size(block_size)
            .expect("invalid posting list block size");
        Self::new_with_format_version_and_block_size(
            id,
            with_position,
            token_set_format,
            format_version,
            block_size,
        )
    }

    pub fn new_with_format_version_and_block_size(
        id: u64,
        with_position: bool,
        token_set_format: TokenSetFormat,
        format_version: InvertedListFormatVersion,
        block_size: usize,
    ) -> Self {
        validate_format_version_block_size(format_version, block_size)
            .expect("invalid FTS format version for posting block size");
        Self {
            id,
            with_position,
            token_set_format,
            format_version,
            posting_tail_codec: format_version.posting_tail_codec(),
            block_size,
            tokens: TokenSet::default(),
            posting_lists: Vec::new(),
            docs: DocSet::default(),
        }
    }

    pub fn new_with_posting_tail_codec(
        id: u64,
        with_position: bool,
        token_set_format: TokenSetFormat,
        posting_tail_codec: PostingTailCodec,
    ) -> Self {
        Self::new_with_posting_tail_codec_and_block_size(
            id,
            with_position,
            token_set_format,
            posting_tail_codec,
            LEGACY_BLOCK_SIZE,
        )
    }

    pub fn new_with_posting_tail_codec_and_block_size(
        id: u64,
        with_position: bool,
        token_set_format: TokenSetFormat,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
    ) -> Self {
        let format_version = InvertedListFormatVersion::from_posting_tail_codec_and_block_size(
            posting_tail_codec,
            block_size,
        )
        .expect("invalid posting tail codec for posting block size");
        let mut builder = Self::new_with_format_version_and_block_size(
            id,
            with_position,
            token_set_format,
            format_version,
            block_size,
        );
        builder.posting_tail_codec = posting_tail_codec;
        builder
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    fn set_id(&mut self, id: u64) {
        self.id = id;
    }

    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    /// Set the token set for this builder.
    pub fn set_tokens(&mut self, tokens: TokenSet) {
        self.tokens = tokens;
    }

    /// Set the document set for this builder.
    pub fn set_docs(&mut self, docs: DocSet) {
        self.docs = docs;
    }

    /// Set the posting lists for this builder.
    pub fn set_posting_lists(&mut self, posting_lists: Vec<PostingListBuilder>) {
        self.posting_lists = posting_lists;
    }

    pub async fn remap(&mut self, mapping: &RowAddrRemap) -> Result<()> {
        // for the docs, we need to remove the rows that are removed from the doc set,
        // and update the row ids of the rows that are updated
        let removed = self.docs.remap(mapping);

        // for the posting lists, we need to remap the doc ids:
        // - if the a row is removed, we need to shift the doc ids of the following rows
        // - if a row is updated (assigned a new row id), we don't need to do anything with the posting lists
        let mut token_id = 0;
        let mut removed_token_ids = Vec::new();
        self.posting_lists.retain_mut(|posting_list| {
            posting_list.remap(&removed);
            let keep = !posting_list.is_empty();
            if !keep {
                removed_token_ids.push(token_id as u32);
            }
            token_id += 1;
            keep
        });

        // for the tokens, remap the token ids if any posting list is empty
        self.tokens.remap(&removed_token_ids);

        Ok(())
    }

    async fn filter_old_data(&mut self, filter: &OldIndexDataFilter) -> Result<()> {
        let mut mapping = HashMap::new();
        for (row_id, _) in self.docs.iter() {
            let keep = match filter {
                OldIndexDataFilter::Fragments { to_keep, .. } => {
                    to_keep.contains((*row_id >> 32) as u32)
                }
                OldIndexDataFilter::RowIds(valid_row_ids) => valid_row_ids.contains(*row_id),
            };
            if !keep {
                mapping.insert(*row_id, None);
            }
        }
        self.remap(&RowAddrRemap::direct(mapping)).await
    }

    pub fn merge_from(&mut self, other: Self) -> Result<()> {
        let Self {
            id: _,
            with_position,
            token_set_format,
            format_version,
            posting_tail_codec,
            block_size,
            tokens,
            posting_lists,
            docs,
        } = other;

        if self.with_position != with_position {
            return Err(Error::index(format!(
                "cannot merge partitions with mismatched positions settings: {} vs {}",
                self.with_position, with_position
            )));
        }
        if self.token_set_format != token_set_format {
            return Err(Error::index(format!(
                "cannot merge partitions with mismatched token set formats: {:?} vs {:?}",
                self.token_set_format, token_set_format
            )));
        }
        if self.format_version != format_version {
            return Err(Error::index(format!(
                "cannot merge partitions with mismatched FTS format versions: {:?} vs {:?}",
                self.format_version, format_version
            )));
        }
        if self.posting_tail_codec != posting_tail_codec {
            return Err(Error::index(format!(
                "cannot merge partitions with mismatched posting tail codecs: {:?} vs {:?}",
                self.posting_tail_codec, posting_tail_codec
            )));
        }
        if self.block_size != block_size {
            return Err(Error::index(format!(
                "cannot merge partitions with mismatched FTS block sizes: {} vs {}",
                self.block_size, block_size
            )));
        }

        let mut token_id_map = vec![u32::MAX; posting_lists.len()];
        match tokens.tokens {
            TokenMap::HashMap(map) => {
                for (token, token_id) in map {
                    let new_token_id = self.tokens.get_or_add(token.as_str());
                    token_id_map[token_id as usize] = new_token_id;
                }
            }
            TokenMap::Fst(map) => {
                let mut stream = map.stream();
                while let Some((token, token_id)) = stream.next() {
                    let new_token_id = self
                        .tokens
                        .get_or_add(String::from_utf8_lossy(token).as_ref());
                    token_id_map[token_id as usize] = new_token_id;
                }
            }
        }

        let doc_id_offset = self.docs.len() as u32;
        for doc_id in 0..docs.len() as u32 {
            let row_id = docs.row_id(doc_id);
            let num_tokens = docs.num_tokens(doc_id);
            let doc_index = docs.doc_index(doc_id);
            if doc_index.is_empty() {
                self.docs.append(row_id, num_tokens);
            } else {
                self.docs
                    .append_with_doc_index(row_id, num_tokens, &doc_index)?;
            }
        }
        self.posting_lists.resize_with(self.tokens.len(), || {
            PostingListBuilder::new_with_posting_tail_codec_and_block_size(
                with_position,
                self.posting_tail_codec,
                self.block_size,
            )
        });

        for (token_id, posting_list) in posting_lists.into_iter().enumerate() {
            if posting_list.is_empty() {
                continue;
            }
            let new_token_id = token_id_map[token_id];
            debug_assert_ne!(new_token_id, u32::MAX);
            let merged_posting = &mut self.posting_lists[new_token_id as usize];
            posting_list.for_each_entry(|doc_id, freq, positions| {
                let positions = match positions {
                    Some(positions) => PositionRecorder::Position(positions.into()),
                    None => PositionRecorder::Count(freq),
                };
                merged_posting.add(doc_id_offset + doc_id, positions);
                Ok::<(), Error>(())
            })?;
        }

        Ok(())
    }

    fn memory_size(&self) -> u64 {
        let posting_lists_overhead =
            self.posting_lists.capacity() * std::mem::size_of::<PostingListBuilder>();
        let posting_lists_size: u64 = self
            .posting_lists
            .iter()
            .map(|posting| posting.size())
            .sum();
        (self.tokens.memory_size() + self.docs.memory_size() + posting_lists_overhead) as u64
            + posting_lists_size
    }

    pub async fn write(&mut self, store: &dyn IndexStore) -> Result<Vec<IndexFile>> {
        self.write_to(store, PartitionWriteTarget::Final).await
    }

    async fn write_to(
        &mut self,
        store: &dyn IndexStore,
        target: PartitionWriteTarget,
    ) -> Result<Vec<IndexFile>> {
        let docs = Arc::new(std::mem::take(&mut self.docs));
        let files = vec![
            self.write_posting_lists(store, docs.clone(), &target.posting_path(self.id))
                .await?,
            self.write_tokens(store, &target.token_path(self.id))
                .await?,
            self.write_docs(store, docs, &target.doc_path(self.id))
                .await?,
        ];
        Ok(files)
    }

    #[instrument(level = "debug", skip_all)]
    async fn write_posting_lists(
        &mut self,
        store: &dyn IndexStore,
        docs: Arc<DocSet>,
        path: &str,
    ) -> Result<IndexFile> {
        let id = self.id;
        let mut writer = store
            .new_index_file(
                path,
                inverted_list_schema_for_version_with_block_size(
                    self.with_position,
                    self.format_version,
                    self.block_size,
                ),
            )
            .await?;
        let posting_lists = std::mem::take(&mut self.posting_lists);

        log::info!(
            "writing {} posting lists of partition {}, with position {}",
            posting_lists.len(),
            id,
            self.with_position
        );
        let with_position = self.with_position;
        let format_version = self.format_version;
        let schema = inverted_list_schema_for_version_with_block_size(
            self.with_position,
            self.format_version,
            self.block_size,
        );
        let docs_for_batches = docs.clone();
        let schema_for_batches = schema.clone();
        let batch_rows = *LANCE_FTS_POSTING_BATCH_ROWS;
        let (tx, rx) = async_channel::bounded(*LANCE_FTS_WRITE_QUEUE_SIZE);
        // The producer builds posting-list batches on the CPU pool and hands them
        // to the writer through a bounded channel. Each batch is built inside its
        // own `spawn_cpu` call and dispatched with an async `send().await` instead
        // of a blocking send: when the channel is full the producer *yields* its
        // task rather than parking the pool thread it is running on. Parking would
        // deadlock on hosts whose CPU pool has a single thread: once the consumer
        // accumulates enough data to flush an encoded column, the flush also needs
        // that pool. The parked producer and the starved consumer would then wait
        // on each other forever.
        let producer = tokio::spawn(async move {
            let mut batch_builder = PostingListBatchBuilder::new(
                schema_for_batches,
                with_position,
                format_version,
                batch_rows,
            );
            let mut posting_lists = posting_lists.into_iter();
            loop {
                let docs_for_batches = docs_for_batches.clone();
                // Build the next batch on the CPU pool. The builder and the
                // remaining posting lists are moved in and handed back so state
                // persists across batches.
                let (next_builder, next_posting_lists, batch) = spawn_cpu(move || {
                    let mut batch_builder = batch_builder;
                    let mut posting_lists = posting_lists;
                    let mut batch = None;
                    for posting_list in posting_lists.by_ref() {
                        posting_list.append_to_batch_with_docs(
                            &docs_for_batches,
                            &mut batch_builder,
                            format_version,
                        )?;
                        if batch_builder.len() >= batch_rows {
                            batch = Some(batch_builder.finish()?);
                            break;
                        }
                    }
                    if batch.is_none() && !batch_builder.is_empty() {
                        batch = Some(batch_builder.finish()?);
                    }
                    Result::Ok((batch_builder, posting_lists, batch))
                })
                .await?;
                batch_builder = next_builder;
                posting_lists = next_posting_lists;

                let Some(batch) = batch else {
                    // No more batches: the posting lists are exhausted.
                    break;
                };
                if tx.send(batch).await.is_err() {
                    // The receiver is gone, which only happens when the writer
                    // failed; stop producing and let that error surface there.
                    break;
                }
            }

            Result::Ok(())
        });

        while let Ok(batch) = rx.recv().await {
            if let Err(err) = writer.write_record_batch(batch).await {
                drop(rx);
                // Wait for producer to stop; preserve the write error as the primary failure.
                let _ = producer.await;
                return Err(err);
            }
        }
        drop(rx);
        producer.await??;
        writer.finish().await
    }

    #[instrument(level = "debug", skip_all)]
    async fn write_tokens(&mut self, store: &dyn IndexStore, path: &str) -> Result<IndexFile> {
        log::info!("writing tokens of partition {}", self.id);
        let tokens = std::mem::take(&mut self.tokens);
        let batch = tokens.to_batch(self.token_set_format)?;
        let mut writer = store.new_index_file(path, batch.schema()).await?;
        writer.write_record_batch(batch).await?;
        writer.finish().await
    }

    #[instrument(level = "debug", skip_all)]
    async fn write_docs(
        &mut self,
        store: &dyn IndexStore,
        docs: Arc<DocSet>,
        path: &str,
    ) -> Result<IndexFile> {
        log::info!("writing docs of partition {}", self.id);
        let batch = docs.to_batch()?;
        let mut writer = store.new_index_file(path, batch.schema()).await?;
        writer.write_record_batch(batch).await?;
        writer
            .finish_with_metadata(HashMap::from([(
                super::documents::TOTAL_TOKENS_KEY.to_owned(),
                docs.total_tokens_num().to_string(),
            )]))
            .await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartitionWriteTarget {
    Final,
    Staged,
}

impl PartitionWriteTarget {
    fn file_path(self, partition_id: u64, suffix: &str) -> String {
        match self {
            Self::Final => partition_file_path(partition_id, suffix),
            Self::Staged => staged_partition_file_path(partition_id, suffix),
        }
    }

    fn token_path(self, partition_id: u64) -> String {
        self.file_path(partition_id, TOKENS_FILE)
    }

    fn posting_path(self, partition_id: u64) -> String {
        self.file_path(partition_id, INVERT_LIST_FILE)
    }

    fn doc_path(self, partition_id: u64) -> String {
        self.file_path(partition_id, DOCS_FILE)
    }
}

struct IndexWorker {
    tokenizer: Box<dyn LanceTokenizer>,
    dest_store: Arc<dyn IndexStore>,
    id_alloc: Arc<AtomicU64>,
    builder: InnerBuilder,
    partitions: Vec<u64>,
    files: Vec<IndexFile>,
    schema: SchemaRef,
    memory_size: u64,
    worker_memory_limit_bytes: u64,
    total_doc_length: usize,
    fragment_mask: Option<u64>,
    token_set_format: TokenSetFormat,
    token_ids: Vec<u32>,
    last_token_count: usize,
    coordinate_rank: usize,
}

struct TailPartition {
    builder: InnerBuilder,
}

struct WorkerOutput {
    partitions: Vec<u64>,
    files: Vec<IndexFile>,
    tail_partition: Option<TailPartition>,
}

enum DocumentSource<'a> {
    Text(&'a str),
    StringList(&'a dyn Array),
}

#[derive(Debug, Clone, Copy)]
struct IndexWorkerConfig {
    with_position: bool,
    format_version: InvertedListFormatVersion,
    fragment_mask: Option<u64>,
    token_set_format: TokenSetFormat,
    worker_memory_limit_bytes: u64,
    block_size: usize,
    coordinate_rank: usize,
}

impl IndexWorker {
    fn posting_lists_overhead_size(&self) -> u64 {
        (self.builder.posting_lists.capacity() * std::mem::size_of::<PostingListBuilder>()) as u64
    }

    fn adjust_tracked_value(tracked: &mut u64, old: u64, new: u64) {
        if new >= old {
            *tracked += new - old;
        } else {
            *tracked -= old - new;
        }
    }

    fn adjust_tracked_memory_size(&mut self, old_memory_size: u64, new_memory_size: u64) {
        Self::adjust_tracked_value(&mut self.memory_size, old_memory_size, new_memory_size);
    }

    fn apply_delta(total: &mut u64, delta: i64) {
        if delta >= 0 {
            *total += delta as u64;
        } else {
            *total -= (-delta) as u64;
        }
    }

    fn temporary_memory_size(&self) -> u64 {
        (self.token_ids.capacity() * std::mem::size_of::<u32>()) as u64
    }

    fn trim_temporary_buffers(&mut self) {
        if self.token_ids.capacity() > MAX_RETAINED_TOKEN_IDS {
            self.token_ids = Vec::with_capacity(self.last_token_count.min(MAX_RETAINED_TOKEN_IDS));
        }
    }

    async fn new(
        tokenizer: Box<dyn LanceTokenizer>,
        dest_store: Arc<dyn IndexStore>,
        id_alloc: Arc<AtomicU64>,
        config: IndexWorkerConfig,
    ) -> Result<Self> {
        let schema = inverted_list_schema_for_version_with_block_size(
            config.with_position,
            config.format_version,
            config.block_size,
        );

        let mut builder = InnerBuilder::new_with_format_version_and_block_size(
            id_alloc.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                | config.fragment_mask.unwrap_or(0),
            config.with_position,
            config.token_set_format,
            config.format_version,
            config.block_size,
        );
        builder.docs = DocSet::with_coordinate_rank(config.coordinate_rank);

        Ok(Self {
            tokenizer,
            dest_store,
            builder,
            partitions: Vec::new(),
            files: Vec::new(),
            id_alloc,
            schema,
            memory_size: 0,
            worker_memory_limit_bytes: config.worker_memory_limit_bytes,
            total_doc_length: 0,
            fragment_mask: config.fragment_mask,
            token_set_format: config.token_set_format,
            token_ids: Vec::new(),
            last_token_count: 0,
            coordinate_rank: config.coordinate_rank,
        })
    }

    fn has_position(&self) -> bool {
        self.schema
            .column_with_name(COMPRESSED_POSITION_COL)
            .is_some()
            || self.schema.column_with_name(POSITION_COL).is_some()
    }

    async fn process_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let doc_col = batch.column(0);
        let row_id_col = batch[ROW_ID].as_primitive::<datatypes::UInt64Type>();
        let doc_index_columns = (0..self.coordinate_rank)
            .map(|rank| {
                let column_name = doc_index_storage_column(rank);
                batch
                    .column_by_name(&column_name)
                    .ok_or_else(|| {
                        Error::index(format!(
                            "FTS document input is missing coordinate column {column_name}"
                        ))
                    })
                    .map(|column| column.as_primitive::<datatypes::UInt32Type>())
            })
            .collect::<Result<Vec<_>>>()?;
        match doc_col.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                for (row_index, (doc, row_id)) in iter_str_array(doc_col.as_ref())
                    .zip(row_id_col.values().iter())
                    .enumerate()
                {
                    let doc = match doc {
                        Some(doc) => doc,
                        None if self.coordinate_rank > 0 => "",
                        None => continue,
                    };
                    let doc_index = doc_index_columns
                        .iter()
                        .map(|column| column.value(row_index))
                        .collect::<Vec<_>>();
                    self.process_document(*row_id, DocumentSource::Text(doc), &doc_index)
                        .await?;
                }
            }
            DataType::List(_) => {
                if self.coordinate_rank > 0 {
                    return Err(Error::index(
                        "ListElement FTS input must be expanded to string documents before indexing"
                            .to_string(),
                    ));
                }
                self.process_string_list_batch::<i32>(doc_col, row_id_col)
                    .await?;
            }
            DataType::LargeList(_) => {
                if self.coordinate_rank > 0 {
                    return Err(Error::index(
                        "ListElement FTS input must be expanded to string documents before indexing"
                            .to_string(),
                    ));
                }
                self.process_string_list_batch::<i64>(doc_col, row_id_col)
                    .await?;
            }
            data_type => {
                return Err(Error::index(format!(
                    "expect data type String, LargeString, List(String), or LargeList(String) but got {}",
                    data_type
                )));
            }
        }

        Ok(())
    }

    async fn process_string_list_batch<Offset: arrow::array::OffsetSizeTrait>(
        &mut self,
        doc_col: &Arc<dyn Array>,
        row_id_col: &arrow_array::PrimitiveArray<datatypes::UInt64Type>,
    ) -> Result<()> {
        let docs = doc_col.as_list::<Offset>();
        match docs.value_type() {
            datatypes::DataType::Utf8 | datatypes::DataType::LargeUtf8 => {}
            data_type => {
                return Err(Error::index(format!(
                    "expect list item data type String or LargeString but got {}",
                    data_type
                )));
            }
        }

        for (doc, row_id) in docs.iter().zip(row_id_col.values().iter()) {
            let Some(doc) = doc else {
                continue;
            };

            self.process_document(*row_id, DocumentSource::StringList(doc.as_ref()), &[])
                .await?;
        }

        Ok(())
    }

    fn checked_token_position(row_id: u64, token_position: usize) -> Result<u32> {
        u32::try_from(token_position).map_err(|_| {
            Error::invalid_input(format!(
                "token position overflow for row_id={row_id}: token_position={token_position}"
            ))
        })
    }

    fn materialize_string_list(elements: &dyn Array) -> String {
        let mut doc = String::new();
        for element in iter_str_array(elements).flatten() {
            if !doc.is_empty() {
                doc.push(' ');
            }
            doc.push_str(element);
        }
        doc
    }

    async fn process_document(
        &mut self,
        row_id: u64,
        document: DocumentSource<'_>,
        doc_index: &[u32],
    ) -> Result<()> {
        let with_position = self.has_position();
        let builder_was_empty = self.builder.docs.is_empty();
        let old_temporary_memory_size = self.temporary_memory_size();
        let old_token_memory_size = self.builder.tokens.memory_size() as u64;
        let doc_id = self.builder.docs.len() as u32;
        let mut token_num: u32 = 0;
        let mut doc_length_bytes = 0usize;
        let mut posting_memory_delta = 0i64;
        if with_position {
            {
                if self.token_ids.capacity() < self.last_token_count {
                    self.token_ids
                        .reserve(self.last_token_count - self.token_ids.capacity());
                }
                self.token_ids.clear();
                let tokenizer = &mut self.tokenizer;
                let builder = &mut self.builder;
                let token_ids = &mut self.token_ids;
                let memory_size = &mut self.memory_size;
                let posting_tail_codec = builder.posting_tail_codec;

                let block_size = builder.block_size;
                let mut process_text = |text: &str| -> Result<()> {
                    doc_length_bytes += text.len();
                    let mut token_stream = tokenizer.token_stream_for_doc(text);
                    while token_stream.advance() {
                        let token = token_stream.token();
                        let position = Self::checked_token_position(row_id, token.position)?;
                        let token_id = builder.tokens.get_or_add(&token.text);
                        if token_id as usize == builder.posting_lists.len() {
                            let old_posting_lists_overhead_size = (builder.posting_lists.capacity()
                                * std::mem::size_of::<PostingListBuilder>())
                                as u64;
                            builder.posting_lists.push(
                                PostingListBuilder::new_with_posting_tail_codec_and_block_size(
                                    true,
                                    posting_tail_codec,
                                    block_size,
                                ),
                            );
                            let new_posting_lists_overhead_size = (builder.posting_lists.capacity()
                                * std::mem::size_of::<PostingListBuilder>())
                                as u64;
                            Self::adjust_tracked_value(
                                memory_size,
                                old_posting_lists_overhead_size,
                                new_posting_lists_overhead_size,
                            );
                        }
                        let posting_list = &mut builder.posting_lists[token_id as usize];
                        let old_posting_memory_size = posting_list.size();
                        if posting_list.add_occurrence(doc_id, position)? {
                            token_ids.push(token_id);
                        }
                        let new_posting_memory_size = posting_list.size();
                        posting_memory_delta +=
                            new_posting_memory_size as i64 - old_posting_memory_size as i64;
                        token_num += 1;
                    }
                    Ok(())
                };

                match document {
                    DocumentSource::Text(doc) => {
                        process_text(doc)?;
                    }
                    DocumentSource::StringList(elements) => {
                        let doc = Self::materialize_string_list(elements);
                        process_text(&doc)?;
                    }
                }
            }
        } else {
            {
                if self.token_ids.capacity() < self.last_token_count {
                    self.token_ids
                        .reserve(self.last_token_count - self.token_ids.capacity());
                }
                self.token_ids.clear();

                let tokenizer = &mut self.tokenizer;
                let builder = &mut self.builder;
                let token_ids = &mut self.token_ids;
                let mut process_text = |text: &str| {
                    doc_length_bytes += text.len();
                    let mut token_stream = tokenizer.token_stream_for_doc(text);
                    while token_stream.advance() {
                        let token_id = builder.tokens.get_or_add(&token_stream.token().text);
                        token_ids.push(token_id);
                        token_num += 1;
                    }
                };

                match document {
                    DocumentSource::Text(doc) => process_text(doc),
                    DocumentSource::StringList(elements) => {
                        let doc = Self::materialize_string_list(elements);
                        process_text(&doc);
                    }
                }
            }
        }
        self.adjust_tracked_memory_size(
            old_token_memory_size,
            self.builder.tokens.memory_size() as u64,
        );

        // Row indexes omit zero-token documents from corpus statistics.
        // ListElement indexes retain them so their physical coordinates remain
        // part of the document corpus even when they cannot match a term.
        if token_num == 0 && self.coordinate_rank == 0 {
            self.last_token_count = 0;
            self.trim_temporary_buffers();
            self.adjust_tracked_memory_size(
                old_temporary_memory_size,
                self.temporary_memory_size(),
            );
            return Ok(());
        }

        if !with_position {
            let old_posting_lists_overhead_size = self.posting_lists_overhead_size();
            self.builder
                .posting_lists
                .resize_with(self.builder.tokens.len(), || {
                    PostingListBuilder::new_with_posting_tail_codec_and_block_size(
                        false,
                        self.builder.posting_tail_codec,
                        self.builder.block_size,
                    )
                });
            let new_posting_lists_overhead_size = self.posting_lists_overhead_size();
            Self::adjust_tracked_value(
                &mut self.memory_size,
                old_posting_lists_overhead_size,
                new_posting_lists_overhead_size,
            );
        }

        let old_doc_memory_size = self.builder.docs.memory_size() as u64;
        let appended_doc_id = if doc_index.is_empty() {
            self.builder.docs.append(row_id, token_num)
        } else {
            self.builder
                .docs
                .append_with_doc_index(row_id, token_num, doc_index)?
        };
        debug_assert_eq!(appended_doc_id, doc_id);
        self.adjust_tracked_memory_size(
            old_doc_memory_size,
            self.builder.docs.memory_size() as u64,
        );
        self.total_doc_length += doc_length_bytes;

        if with_position {
            for &token_id in &self.token_ids {
                let (old_posting_memory_size, new_posting_memory_size) = {
                    let posting_list = &mut self.builder.posting_lists[token_id as usize];
                    let old_posting_memory_size = posting_list.size();
                    posting_list.finish_open_doc(doc_id)?;
                    let new_posting_memory_size = posting_list.size();
                    (old_posting_memory_size, new_posting_memory_size)
                };
                posting_memory_delta +=
                    new_posting_memory_size as i64 - old_posting_memory_size as i64;
            }
            Self::apply_delta(&mut self.memory_size, posting_memory_delta);
        } else if token_num > 0 {
            self.token_ids.sort_unstable();
            let mut iter = self.token_ids.iter();
            let mut current = *iter.next().unwrap();
            let mut count = 1u32;
            for &token_id in iter {
                if token_id == current {
                    count += 1;
                    continue;
                }

                let (old_posting_memory_size, new_posting_memory_size) = {
                    let posting_list = &mut self.builder.posting_lists[current as usize];
                    let old_posting_memory_size = posting_list.size();
                    posting_list.add(doc_id, PositionRecorder::Count(count));
                    let new_posting_memory_size = posting_list.size();
                    (old_posting_memory_size, new_posting_memory_size)
                };
                posting_memory_delta +=
                    new_posting_memory_size as i64 - old_posting_memory_size as i64;

                current = token_id;
                count = 1;
            }
            let (old_posting_memory_size, new_posting_memory_size) = {
                let posting_list = &mut self.builder.posting_lists[current as usize];
                let old_posting_memory_size = posting_list.size();
                posting_list.add(doc_id, PositionRecorder::Count(count));
                let new_posting_memory_size = posting_list.size();
                (old_posting_memory_size, new_posting_memory_size)
            };
            posting_memory_delta += new_posting_memory_size as i64 - old_posting_memory_size as i64;
            Self::apply_delta(&mut self.memory_size, posting_memory_delta);
        }
        self.last_token_count = self.token_ids.len();
        self.trim_temporary_buffers();
        self.adjust_tracked_memory_size(old_temporary_memory_size, self.temporary_memory_size());

        if self.builder.docs.len() == 1 && self.memory_size > self.worker_memory_limit_bytes {
            return Err(Error::invalid_input(format!(
                "single document row_id={} exceeds worker memory limit: {} > {} bytes",
                row_id, self.memory_size, self.worker_memory_limit_bytes
            )));
        }

        if self.builder.docs.len() as u32 == u32::MAX
            || (!builder_was_empty && self.memory_size >= self.worker_memory_limit_bytes)
        {
            self.flush().await?;
        }

        Ok(())
    }

    #[instrument(level = "debug", skip_all)]
    async fn flush(&mut self) -> Result<()> {
        if self.builder.docs.is_empty() {
            return Ok(());
        }

        log::info!(
            "flushing posting lists, memory size: {} MiB",
            self.memory_size / (1024 * 1024)
        );
        self.memory_size = self.temporary_memory_size();
        let with_position = self.has_position();
        let format_version = self.builder.format_version;
        let block_size = self.builder.block_size;
        let mut replacement = InnerBuilder::new_with_format_version_and_block_size(
            self.id_alloc
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                | self.fragment_mask.unwrap_or(0),
            with_position,
            self.token_set_format,
            format_version,
            block_size,
        );
        replacement.docs = DocSet::with_coordinate_rank(self.coordinate_rank);
        let builder = std::mem::replace(&mut self.builder, replacement);
        let written_partition_id = builder.id();
        let mut builder = builder;
        let target = if self.fragment_mask.is_some() {
            PartitionWriteTarget::Staged
        } else {
            PartitionWriteTarget::Final
        };
        let files = builder
            .write_to(self.dest_store.as_ref(), target)
            .await
            .map_err(|err| {
                Error::execution(format!(
                    "failed to write finalized partition {}: {err}",
                    written_partition_id
                ))
            })?;
        self.files.extend(files);
        self.partitions.push(written_partition_id);
        Ok(())
    }

    async fn finish(self) -> Result<WorkerOutput> {
        let tail_partition = if self.builder.docs.is_empty() && self.coordinate_rank == 0 {
            None
        } else {
            Some(TailPartition {
                builder: self.builder,
            })
        };
        Ok(WorkerOutput {
            partitions: self.partitions,
            files: self.files,
            tail_partition,
        })
    }
}

#[derive(Debug, Clone)]
pub enum PositionRecorder {
    Position(SmallVec<[u32; 2]>),
    Count(u32),
}

impl PositionRecorder {
    pub fn len(&self) -> u32 {
        match self {
            Self::Position(positions) => positions.len() as u32,
            Self::Count(count) => *count,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_vec(self) -> Vec<u32> {
        match self {
            Self::Position(positions) => positions.into_vec(),
            Self::Count(_) => vec![0],
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, DeepSizeOf)]
pub struct ScoredDoc {
    pub row_id: u64,
    pub doc_index: Vec<u32>,
    pub score: OrderedFloat,
}

impl ScoredDoc {
    pub fn new(row_id: u64, score: f32) -> Self {
        Self {
            row_id,
            doc_index: Vec::new(),
            score: OrderedFloat(score),
        }
    }

    pub fn with_doc_index(row_id: u64, doc_index: Vec<u32>, score: f32) -> Self {
        Self {
            row_id,
            doc_index,
            score: OrderedFloat(score),
        }
    }
}

impl PartialOrd for ScoredDoc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredDoc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score.cmp(&other.score)
    }
}

pub fn legacy_inverted_list_schema(with_position: bool) -> SchemaRef {
    let mut fields = vec![
        arrow_schema::Field::new(ROW_ID, arrow_schema::DataType::UInt64, false),
        arrow_schema::Field::new(FREQUENCY_COL, arrow_schema::DataType::Float32, false),
    ];
    if with_position {
        fields.push(arrow_schema::Field::new(
            POSITION_COL,
            arrow_schema::DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::Int32,
                true,
            ))),
            false,
        ));
    }
    Arc::new(arrow_schema::Schema::new(fields))
}

pub fn inverted_list_schema(with_position: bool) -> SchemaRef {
    inverted_list_schema_for_version(with_position, current_fts_format_version())
}

pub fn inverted_list_schema_for_version(
    with_position: bool,
    format_version: InvertedListFormatVersion,
) -> SchemaRef {
    inverted_list_schema_for_version_with_block_size(
        with_position,
        format_version,
        LEGACY_BLOCK_SIZE,
    )
}

pub fn inverted_list_schema_for_version_with_block_size(
    with_position: bool,
    format_version: InvertedListFormatVersion,
    block_size: usize,
) -> SchemaRef {
    inverted_list_schema_for_version_with_block_size_and_impacts(
        with_position,
        format_version,
        block_size,
        true,
    )
}

pub(crate) fn inverted_list_schema_for_version_with_block_size_and_impacts(
    with_position: bool,
    format_version: InvertedListFormatVersion,
    block_size: usize,
    with_impacts: bool,
) -> SchemaRef {
    validate_format_version_block_size(format_version, block_size)
        .expect("invalid FTS format version for posting block size");
    match format_version {
        InvertedListFormatVersion::V1 => {
            inverted_list_schema_v1(with_position, block_size, with_impacts)
        }
        InvertedListFormatVersion::V2 | InvertedListFormatVersion::V3 => {
            inverted_list_schema_with_tail_codec_and_position_codec(
                with_position,
                format_version,
                PostingTailCodec::VarintDelta,
                Some(PositionStreamCodec::PackedDelta),
                block_size,
                with_impacts,
            )
        }
    }
}

fn inverted_list_schema_v1(
    with_position: bool,
    block_size: usize,
    with_impacts: bool,
) -> SchemaRef {
    let mut fields = vec![
        arrow_schema::Field::new(
            POSTING_COL,
            datatypes::DataType::List(Arc::new(Field::new(
                "item",
                datatypes::DataType::LargeBinary,
                true,
            ))),
            false,
        ),
        arrow_schema::Field::new(MAX_SCORE_COL, datatypes::DataType::Float32, false),
        arrow_schema::Field::new(LENGTH_COL, datatypes::DataType::UInt32, false),
    ];
    if with_impacts {
        fields.push(arrow_schema::Field::new(
            IMPACT_COL,
            datatypes::DataType::List(Arc::new(Field::new(
                "item",
                datatypes::DataType::LargeBinary,
                true,
            ))),
            false,
        ));
    }
    if with_position {
        fields.push(arrow_schema::Field::new(
            POSITION_COL,
            arrow_schema::DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    arrow_schema::DataType::LargeBinary,
                    true,
                ))),
                true,
            ))),
            false,
        ));
    }
    Arc::new(arrow_schema::Schema::new_with_metadata(
        fields,
        HashMap::from([
            (POSTING_BLOCK_SIZE_KEY.to_owned(), block_size.to_string()),
            (
                FTS_FORMAT_VERSION_KEY.to_owned(),
                InvertedListFormatVersion::V1.index_version().to_string(),
            ),
        ]),
    ))
}

pub fn inverted_list_schema_with_tail_codec(
    with_position: bool,
    posting_tail_codec: PostingTailCodec,
) -> SchemaRef {
    let format_version = InvertedListFormatVersion::from_posting_tail_codec_and_block_size(
        posting_tail_codec,
        LEGACY_BLOCK_SIZE,
    )
    .expect("invalid posting tail codec for posting block size");
    inverted_list_schema_with_tail_codec_and_position_codec(
        with_position,
        format_version,
        posting_tail_codec,
        Some(PositionStreamCodec::PackedDelta),
        LEGACY_BLOCK_SIZE,
        false,
    )
}

fn inverted_list_schema_with_tail_codec_and_position_codec(
    with_position: bool,
    format_version: InvertedListFormatVersion,
    posting_tail_codec: PostingTailCodec,
    position_codec: Option<PositionStreamCodec>,
    block_size: usize,
    with_impacts: bool,
) -> SchemaRef {
    let mut fields = vec![
        // we compress the posting lists (including row ids and frequencies),
        // and store the compressed posting lists, so it's a large binary array
        arrow_schema::Field::new(
            POSTING_COL,
            datatypes::DataType::List(Arc::new(Field::new(
                "item",
                datatypes::DataType::LargeBinary,
                true,
            ))),
            false,
        ),
        arrow_schema::Field::new(MAX_SCORE_COL, datatypes::DataType::Float32, false),
        arrow_schema::Field::new(LENGTH_COL, datatypes::DataType::UInt32, false),
    ];
    if with_impacts {
        fields.push(arrow_schema::Field::new(
            IMPACT_COL,
            datatypes::DataType::List(Arc::new(Field::new(
                "item",
                datatypes::DataType::LargeBinary,
                true,
            ))),
            false,
        ));
    }
    if with_position {
        fields.push(arrow_schema::Field::new(
            COMPRESSED_POSITION_COL,
            arrow_schema::DataType::LargeBinary,
            false,
        ));
        fields.push(arrow_schema::Field::new(
            POSITION_BLOCK_OFFSET_COL,
            arrow_schema::DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::UInt32,
                true,
            ))),
            false,
        ));
    }
    let mut metadata = HashMap::from([(
        POSTING_TAIL_CODEC_KEY.to_owned(),
        posting_tail_codec.as_str().to_owned(),
    )]);
    metadata.insert(
        FTS_FORMAT_VERSION_KEY.to_owned(),
        format_version.index_version().to_string(),
    );
    metadata.insert(POSTING_BLOCK_SIZE_KEY.to_owned(), block_size.to_string());
    if let Some(position_codec) = position_codec.filter(|_| with_position) {
        metadata.insert(
            POSITIONS_LAYOUT_KEY.to_owned(),
            POSITIONS_LAYOUT_SHARED_STREAM_V2.to_owned(),
        );
        metadata.insert(
            POSITIONS_CODEC_KEY.to_owned(),
            position_codec.as_str().to_owned(),
        );
    }
    Arc::new(arrow_schema::Schema::new_with_metadata(fields, metadata))
}

pub(crate) fn token_file_path(partition_id: u64) -> String {
    format!("part_{}_{}", partition_id, TOKENS_FILE)
}

pub(crate) fn posting_file_path(partition_id: u64) -> String {
    format!("part_{}_{}", partition_id, INVERT_LIST_FILE)
}

pub(crate) fn doc_file_path(partition_id: u64) -> String {
    format!("part_{}_{}", partition_id, DOCS_FILE)
}

pub(crate) fn part_metadata_file_path(partition_id: u64) -> String {
    staged_partition_file_path(partition_id, METADATA_FILE)
}

const PARTITION_FILE_SUFFIXES: [&str; 3] = [TOKENS_FILE, INVERT_LIST_FILE, DOCS_FILE];
const STAGED_PARTITION_DIR: &str = "staging";

fn partition_file_path(partition_id: u64, suffix: &str) -> String {
    format!("part_{}_{}", partition_id, suffix)
}

fn staged_partition_file_path(partition_id: u64, suffix: &str) -> String {
    format!(
        "{}/{}",
        STAGED_PARTITION_DIR,
        partition_file_path(partition_id, suffix)
    )
}

pub async fn merge_index_files(
    object_store: &ObjectStore,
    index_dir: &Path,
    store: Arc<dyn IndexStore>,
    progress: Arc<dyn IndexBuildProgress>,
) -> Result<()> {
    let metadata_path = index_dir.clone().join(METADATA_FILE);
    if object_store.exists(&metadata_path).await? {
        return Ok(());
    }

    // List all staged partition metadata files in the index directory
    let index_files = list_index_files(object_store, index_dir).await?;
    let part_metadata_files = metadata_files(&index_files);
    if part_metadata_files.is_empty() {
        return Err(Error::invalid_input_source(
            format!(
                "No partition metadata files found in index directory: {}",
                index_dir
            )
            .into(),
        ));
    }

    // Call merge_metadata_files function for inverted index
    merge_metadata_files(store, &part_metadata_files, progress).await
}

async fn list_index_files(object_store: &ObjectStore, index_dir: &Path) -> Result<Vec<String>> {
    let mut index_files = Vec::new();
    let mut list_stream = object_store.read_dir_all(index_dir, None);

    while let Some(item) = list_stream.next().await {
        match item {
            Ok(meta) => {
                let location = meta.location.as_ref().trim_start_matches('/');
                let index_dir = index_dir.as_ref().trim_start_matches('/');
                let relative_path = location
                    .strip_prefix(index_dir)
                    .map(|s| s.trim_start_matches('/').to_string())
                    .unwrap_or_else(|| meta.location.filename().unwrap_or("").to_string());
                index_files.push(relative_path);
            }
            Err(err) => return Err(err),
        }
    }

    Ok(index_files)
}

fn metadata_files(index_files: &[String]) -> Vec<String> {
    index_files
        .iter()
        .filter(|file_name| {
            file_name.starts_with(&format!("{}/part_", STAGED_PARTITION_DIR))
                && file_name.ends_with("_metadata.lance")
        })
        .cloned()
        .collect()
}

#[cfg(test)]
async fn list_metadata_files(object_store: &ObjectStore, index_dir: &Path) -> Result<Vec<String>> {
    let index_files = list_index_files(object_store, index_dir).await?;
    let part_metadata_files = metadata_files(&index_files);
    if part_metadata_files.is_empty() {
        return Err(Error::invalid_input_source(
            format!(
                "No partition metadata files found in index directory: {}",
                index_dir
            )
            .into(),
        ));
    }

    Ok(part_metadata_files)
}

/// Merge partition metadata files with partition ID remapping to sequential IDs starting from 0
async fn merge_metadata_files(
    store: Arc<dyn IndexStore>,
    part_metadata_files: &[String],
    progress: Arc<dyn IndexBuildProgress>,
) -> Result<()> {
    // Collect all partition IDs and params
    let mut all_partitions = Vec::new();
    let mut params = None;
    let mut token_set_format = None;
    let mut format_version = None;
    let mut deleted_fragments = RoaringBitmap::new();
    progress
        .stage_start(
            "read_partition_metadata",
            Some(part_metadata_files.len() as u64),
            "files",
        )
        .await?;

    for (idx, file_name) in part_metadata_files.iter().enumerate() {
        let reader = store.open_index_file(file_name).await?;
        let metadata = &reader.schema().metadata;

        let partitions_str = metadata.get("partitions").ok_or(Error::index(format!(
            "partitions not found in {}",
            file_name
        )))?;

        let partition_ids: Vec<u64> = serde_json::from_str(partitions_str)
            .map_err(|e| Error::index(format!("Failed to parse partitions: {}", e)))?;

        all_partitions.extend(partition_ids);

        if params.is_none() {
            let params_str = metadata
                .get("params")
                .ok_or(Error::index(format!("params not found in {}", file_name)))?;
            params = Some(
                serde_json::from_str::<InvertedIndexParams>(params_str)
                    .map_err(|e| Error::index(format!("Failed to parse params: {}", e)))?,
            );
        }

        if token_set_format.is_none()
            && let Some(name) = metadata.get(TOKEN_SET_FORMAT_KEY)
        {
            token_set_format = Some(TokenSetFormat::from_str(name)?);
        }
        if format_version.is_none() {
            format_version = Some(parse_format_version_from_metadata(metadata)?);
        }

        if reader.num_rows() > 0 {
            let metadata_batch = reader.read_range(0..1, None).await?;
            let deleted_fragments_col = metadata_batch
                .column_by_name(DELETED_FRAGMENTS_COL)
                .expect_ok()?;
            let deleted_fragments_arr = deleted_fragments_col
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect_ok()?;
            let part_deleted_fragments =
                RoaringBitmap::deserialize_from(deleted_fragments_arr.value(0))?;
            deleted_fragments.extend(part_deleted_fragments);
        }
        progress
            .stage_progress("read_partition_metadata", idx as u64 + 1)
            .await?;
    }
    progress.stage_complete("read_partition_metadata").await?;

    // Create ID mapping: sorted original IDs -> 0,1,2...
    let mut sorted_ids = all_partitions;
    sorted_ids.sort();
    sorted_ids.dedup();

    let id_mapping: Vec<(u64, u64)> = sorted_ids
        .iter()
        .enumerate()
        .map(|(new_id, &old_id)| (old_id, new_id as u64))
        .collect();

    let total_copies = id_mapping.len() as u64 * PARTITION_FILE_SUFFIXES.len() as u64;
    progress
        .stage_start("remap_partition_files", Some(total_copies), "files")
        .await?;

    let mut copied_files = 0u64;

    for &(old_id, new_id) in &id_mapping {
        for suffix in PARTITION_FILE_SUFFIXES {
            let staged_path = staged_partition_file_path(old_id, suffix);
            let final_path = partition_file_path(new_id, suffix);
            store
                .copy_index_file_to(&staged_path, &final_path, store.as_ref())
                .await?;
            copied_files += 1;
            progress
                .stage_progress("remap_partition_files", copied_files)
                .await?;
        }
    }
    progress.stage_complete("remap_partition_files").await?;

    // Write merged metadata with remapped IDs
    let remapped_partitions: Vec<u64> = (0..id_mapping.len() as u64).collect();
    let params = params.unwrap_or_default();
    let token_set_format = token_set_format.unwrap_or(TokenSetFormat::Arrow);
    let builder = InvertedIndexBuilder::from_existing_index(
        params,
        None,
        remapped_partitions.clone(),
        token_set_format,
        None,
        deleted_fragments,
    )
    .with_format_version(format_version.unwrap_or(InvertedListFormatVersion::V1));
    progress
        .stage_start("write_merged_metadata", Some(1), "files")
        .await?;
    builder
        .write_metadata(&*store, &remapped_partitions)
        .await?;
    progress.stage_progress("write_merged_metadata", 1).await?;
    progress.stage_complete("write_merged_metadata").await?;

    // Cleanup staged partition metadata files
    for file_name in part_metadata_files {
        let _ = store.delete_index_file(file_name).await;
    }
    for &(old_id, _) in &id_mapping {
        for suffix in PARTITION_FILE_SUFFIXES {
            let _ = store
                .delete_index_file(&staged_partition_file_path(old_id, suffix))
                .await;
        }
    }

    Ok(())
}

/// Convert input stream into a stream of documents.
///
/// The input stream must be one of:
/// 1. Document in Utf8 or LargeUtf8 format.
/// 2. Document in List(Utf8) or List(LargeUtf8) format.
/// 3. Json document in LargeBinary format.
pub fn document_input(
    input: SendableRecordBatchStream,
    column: &str,
) -> Result<SendableRecordBatchStream> {
    let schema = input.schema();
    let field = schema.column_with_name(column).expect_ok()?.1;
    match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(input),
        DataType::List(field) | DataType::LargeList(field)
            if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            Ok(input)
        }
        DataType::LargeBinary => match field.metadata().get(ARROW_EXT_NAME_KEY) {
            Some(name) if name.as_str() == JSON_EXT_NAME => {
                Ok(Box::pin(JsonTextStream::new(input, column.to_string())))
            }
            _ => Err(Error::invalid_input_source(
                format!("column {} is not json", column).into(),
            )),
        },
        _ => Err(Error::invalid_input_source(
            format!(
                "column {} has type {}, is not utf8, large utf8 type/list, or large binary",
                column,
                field.data_type()
            )
            .into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Index;
    use crate::metrics::NoOpMetricsCollector;
    use crate::progress::IndexBuildProgress;
    use crate::scalar::inverted::{MemBM25Scorer, Scorer};
    use crate::scalar::{IndexFile, IndexReader, IndexWriter, ScalarIndex};
    use arrow_array::{RecordBatch, StringArray, UInt32Array, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use bytes::Bytes;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream::{self, BoxStream};
    use lance_core::ROW_ID;
    use lance_core::cache::LanceCache;
    use lance_core::utils::tempfile::TempDir;
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore as OSObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
        Result as OSResult,
    };
    use std::any::Any;
    use std::fmt::{Display, Formatter};
    use std::ops::Range;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::time::Duration;

    fn make_doc_batch(doc: &str, row_id: u64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("doc", DataType::Utf8, true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let docs = Arc::new(StringArray::from(vec![Some(doc)]));
        let row_ids = Arc::new(UInt64Array::from(vec![row_id]));
        RecordBatch::try_new(schema, vec![docs, row_ids]).unwrap()
    }

    fn make_doc_batch_from_docs(docs: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("doc", DataType::Utf8, true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let num_rows = docs.len();
        let docs = Arc::new(StringArray::from(docs));
        let row_ids = Arc::new(UInt64Array::from_iter_values(0..num_rows as u64));
        RecordBatch::try_new(schema, vec![docs, row_ids]).unwrap()
    }

    struct FailingListObjectStore {
        inner: InMemory,
    }

    impl Default for FailingListObjectStore {
        fn default() -> Self {
            Self {
                inner: InMemory::new(),
            }
        }
    }

    impl Display for FailingListObjectStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingListObjectStore")
        }
    }

    impl Debug for FailingListObjectStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("FailingListObjectStore").finish()
        }
    }

    #[async_trait]
    impl OSObjectStore for FailingListObjectStore {
        async fn put_opts(
            &self,
            location: &Path,
            bytes: PutPayload,
            opts: PutOptions,
        ) -> OSResult<PutResult> {
            self.inner.put_opts(location, bytes, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OSResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OSResult<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, OSResult<Path>>,
        ) -> BoxStream<'static, OSResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
            stream::iter(vec![Err(object_store::Error::Generic {
                store: "failing-list",
                source: "boom listing metadata".into(),
            })])
            .boxed()
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> OSResult<()> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

    #[tokio::test]
    async fn test_list_metadata_files_propagates_list_error() -> Result<()> {
        let mut object_store = ObjectStore::memory();
        object_store.inner = Arc::new(FailingListObjectStore::default());

        let err = list_metadata_files(&object_store, &Path::from("index"))
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("boom listing metadata"),
            "expected original list error, got: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_list_metadata_files_empty_directory_returns_no_files_error() -> Result<()> {
        let object_store = ObjectStore::memory();

        let err = list_metadata_files(&object_store, &Path::from("empty-index"))
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("No partition metadata files found"),
            "expected empty-directory error, got: {err}"
        );
        Ok(())
    }

    #[derive(Debug, Default, Clone)]
    struct CountingStore {
        write_count: Arc<AtomicUsize>,
    }

    impl CountingStore {
        fn new() -> Self {
            Self {
                write_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn write_count(&self) -> usize {
            self.write_count.load(Ordering::SeqCst)
        }
    }

    impl DeepSizeOf for CountingStore {
        fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
            0
        }
    }

    #[derive(Debug, Clone)]
    struct NoRenameStore {
        inner: Arc<dyn IndexStore>,
        final_delete_count: Option<Arc<AtomicUsize>>,
    }

    impl NoRenameStore {
        fn new(inner: Arc<dyn IndexStore>) -> Self {
            Self {
                inner,
                final_delete_count: None,
            }
        }

        fn with_final_delete_tracking(inner: Arc<dyn IndexStore>) -> Self {
            Self {
                inner,
                final_delete_count: Some(Arc::new(AtomicUsize::new(0))),
            }
        }

        fn final_delete_count(&self) -> usize {
            self.final_delete_count
                .as_ref()
                .map(|count| count.load(Ordering::SeqCst))
                .unwrap_or_default()
        }

        fn unwrap_dest_store(dest_store: &dyn IndexStore) -> &dyn IndexStore {
            dest_store
                .as_any()
                .downcast_ref::<Self>()
                .map(|store| store.inner.as_ref())
                .unwrap_or(dest_store)
        }
    }

    impl DeepSizeOf for NoRenameStore {
        fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
            self.inner.deep_size_of_children(context)
        }
    }

    #[async_trait]
    impl IndexStore for NoRenameStore {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn clone_arc(&self) -> Arc<dyn IndexStore> {
            Arc::new(self.clone())
        }

        fn io_parallelism(&self) -> usize {
            self.inner.io_parallelism()
        }

        fn with_io_priority(&self, io_priority: u64) -> Arc<dyn IndexStore> {
            self.inner.with_io_priority(io_priority)
        }

        async fn new_index_file(
            &self,
            name: &str,
            schema: Arc<Schema>,
        ) -> Result<Box<dyn IndexWriter>> {
            self.inner.new_index_file(name, schema).await
        }

        async fn open_index_file(&self, name: &str) -> Result<Arc<dyn IndexReader>> {
            self.inner.open_index_file(name).await
        }

        async fn copy_index_file(
            &self,
            name: &str,
            dest_store: &dyn IndexStore,
        ) -> Result<IndexFile> {
            self.inner
                .copy_index_file(name, Self::unwrap_dest_store(dest_store))
                .await
        }

        async fn copy_index_file_to(
            &self,
            name: &str,
            new_name: &str,
            dest_store: &dyn IndexStore,
        ) -> Result<IndexFile> {
            self.inner
                .copy_index_file_to(name, new_name, Self::unwrap_dest_store(dest_store))
                .await
        }

        async fn rename_index_file(&self, name: &str, new_name: &str) -> Result<IndexFile> {
            Err(Error::internal(format!(
                "merge_index_files should not rename partition file {name} to {new_name}"
            )))
        }

        async fn delete_index_file(&self, name: &str) -> Result<()> {
            if name.starts_with("part_")
                && let Some(count) = &self.final_delete_count
            {
                count.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.delete_index_file(name).await
        }

        async fn list_files_with_sizes(&self) -> Result<Vec<IndexFile>> {
            self.inner.list_files_with_sizes().await
        }
    }

    #[derive(Debug)]
    struct FailMetadataStore {
        inner: Arc<dyn IndexStore>,
    }

    impl FailMetadataStore {
        fn new(inner: Arc<dyn IndexStore>) -> Self {
            Self { inner }
        }

        fn unwrap_dest_store(dest_store: &dyn IndexStore) -> &dyn IndexStore {
            dest_store
                .as_any()
                .downcast_ref::<Self>()
                .map(|store| store.inner.as_ref())
                .unwrap_or(dest_store)
        }
    }

    impl DeepSizeOf for FailMetadataStore {
        fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
            self.inner.deep_size_of_children(context)
        }
    }

    #[async_trait]
    impl IndexStore for FailMetadataStore {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn clone_arc(&self) -> Arc<dyn IndexStore> {
            Arc::new(Self {
                inner: self.inner.clone(),
            })
        }

        fn io_parallelism(&self) -> usize {
            self.inner.io_parallelism()
        }

        fn with_io_priority(&self, io_priority: u64) -> Arc<dyn IndexStore> {
            self.inner.with_io_priority(io_priority)
        }

        async fn new_index_file(
            &self,
            name: &str,
            schema: Arc<Schema>,
        ) -> Result<Box<dyn IndexWriter>> {
            let writer = self.inner.new_index_file(name, schema).await?;
            if name == METADATA_FILE {
                Ok(Box::new(FailFinishWriter { inner: writer }))
            } else {
                Ok(writer)
            }
        }

        async fn open_index_file(&self, name: &str) -> Result<Arc<dyn IndexReader>> {
            self.inner.open_index_file(name).await
        }

        async fn copy_index_file(
            &self,
            name: &str,
            dest_store: &dyn IndexStore,
        ) -> Result<IndexFile> {
            self.inner
                .copy_index_file(name, Self::unwrap_dest_store(dest_store))
                .await
        }

        async fn copy_index_file_to(
            &self,
            name: &str,
            new_name: &str,
            dest_store: &dyn IndexStore,
        ) -> Result<IndexFile> {
            self.inner
                .copy_index_file_to(name, new_name, Self::unwrap_dest_store(dest_store))
                .await
        }

        async fn rename_index_file(&self, name: &str, new_name: &str) -> Result<IndexFile> {
            self.inner.rename_index_file(name, new_name).await
        }

        async fn delete_index_file(&self, name: &str) -> Result<()> {
            self.inner.delete_index_file(name).await
        }

        async fn list_files_with_sizes(&self) -> Result<Vec<IndexFile>> {
            self.inner.list_files_with_sizes().await
        }
    }

    struct FailFinishWriter {
        inner: Box<dyn IndexWriter>,
    }

    #[async_trait]
    impl IndexWriter for FailFinishWriter {
        async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<u64> {
            self.inner.write_record_batch(batch).await
        }

        async fn add_global_buffer(&mut self, data: Bytes) -> Result<u32> {
            self.inner.add_global_buffer(data).await
        }

        async fn finish(&mut self) -> Result<IndexFile> {
            Err(Error::internal("injected metadata write failure"))
        }

        async fn finish_with_metadata(
            &mut self,
            _metadata: HashMap<String, String>,
        ) -> Result<IndexFile> {
            Err(Error::internal("injected metadata write failure"))
        }
    }

    #[derive(Debug)]
    struct CountingWriter {
        path: String,
        write_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl IndexWriter for CountingWriter {
        async fn write_record_batch(&mut self, _batch: RecordBatch) -> Result<u64> {
            Ok(self.write_count.fetch_add(1, Ordering::SeqCst) as u64)
        }

        async fn add_global_buffer(&mut self, _data: Bytes) -> Result<u32> {
            // Mirror the real writer's 1-indexed return value.
            Ok(1)
        }

        async fn finish(&mut self) -> Result<IndexFile> {
            Ok(IndexFile {
                path: self.path.clone(),
                size_bytes: 0,
            })
        }

        async fn finish_with_metadata(
            &mut self,
            _metadata: HashMap<String, String>,
        ) -> Result<IndexFile> {
            Ok(IndexFile {
                path: self.path.clone(),
                size_bytes: 0,
            })
        }
    }

    #[async_trait]
    impl IndexStore for CountingStore {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn clone_arc(&self) -> Arc<dyn IndexStore> {
            Arc::new(self.clone())
        }

        fn io_parallelism(&self) -> usize {
            1
        }

        fn with_io_priority(&self, _io_priority: u64) -> Arc<dyn IndexStore> {
            // No backing scheduler, so priority is meaningless here.
            self.clone_arc()
        }

        async fn new_index_file(
            &self,
            name: &str,
            _schema: Arc<Schema>,
        ) -> Result<Box<dyn IndexWriter>> {
            Ok(Box::new(CountingWriter {
                path: name.to_string(),
                write_count: self.write_count.clone(),
            }))
        }

        async fn open_index_file(&self, _name: &str) -> Result<Arc<dyn IndexReader>> {
            Err(Error::not_supported(
                "CountingStore does not support reading",
            ))
        }

        async fn copy_index_file(
            &self,
            _name: &str,
            _dest_store: &dyn IndexStore,
        ) -> Result<IndexFile> {
            Err(Error::not_supported(
                "CountingStore does not support copying",
            ))
        }

        async fn rename_index_file(&self, _name: &str, _new_name: &str) -> Result<IndexFile> {
            Err(Error::not_supported(
                "CountingStore does not support renaming",
            ))
        }

        async fn delete_index_file(&self, _name: &str) -> Result<()> {
            Err(Error::not_supported(
                "CountingStore does not support deleting",
            ))
        }

        async fn list_files_with_sizes(&self) -> Result<Vec<IndexFile>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn test_write_posting_lists_batches_multiple_rows() -> Result<()> {
        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        for doc_id in 0..3u64 {
            builder.docs.append(doc_id, 1);
        }

        for doc_id in 0..3u32 {
            let mut posting_list = PostingListBuilder::new(false);
            posting_list.add(doc_id, PositionRecorder::Count(1));
            builder.posting_lists.push(posting_list);
        }

        let store = CountingStore::new();
        let docs = Arc::new(std::mem::take(&mut builder.docs));
        builder
            .write_posting_lists(&store, docs, &posting_file_path(0))
            .await?;

        assert_eq!(store.write_count(), 1);
        Ok(())
    }

    async fn write_partition_file_marker(
        store: &dyn IndexStore,
        path: &str,
        partition_id: u64,
    ) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "partition_id",
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![partition_id]))],
        )?;
        let mut writer = store.new_index_file(path, schema).await?;
        writer.write_record_batch(batch).await?;
        writer.finish().await?;
        Ok(())
    }

    async fn write_partition_files(
        store: &dyn IndexStore,
        partition_id: u64,
        target: PartitionWriteTarget,
    ) -> Result<()> {
        write_partition_file_marker(store, &target.token_path(partition_id), partition_id).await?;
        write_partition_file_marker(store, &target.posting_path(partition_id), partition_id)
            .await?;
        write_partition_file_marker(store, &target.doc_path(partition_id), partition_id).await?;
        Ok(())
    }

    async fn read_partition_file_marker(store: &dyn IndexStore, path: &str) -> Result<u64> {
        let reader = store.open_index_file(path).await?;
        let batch = reader.read_range(0..1, None).await?;
        let partition_ids = batch.column(0).as_primitive::<datatypes::UInt64Type>();
        Ok(partition_ids.value(0))
    }

    async fn assert_partition_file_markers(
        store: &dyn IndexStore,
        partition_id: u64,
        expected_marker: u64,
    ) -> Result<()> {
        assert_eq!(
            read_partition_file_marker(store, &token_file_path(partition_id)).await?,
            expected_marker
        );
        assert_eq!(
            read_partition_file_marker(store, &posting_file_path(partition_id)).await?,
            expected_marker
        );
        assert_eq!(
            read_partition_file_marker(store, &doc_file_path(partition_id)).await?,
            expected_marker
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_index_files_remaps_staged_partitions_without_rename() -> Result<()> {
        let index_dir = TempDir::default();
        let object_store = Arc::new(ObjectStore::local());
        let base_store: Arc<dyn IndexStore> = Arc::new(LanceIndexStore::new(
            object_store.clone(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));
        let store = Arc::new(NoRenameStore::new(base_store.clone()));
        let partitions = vec![5_u64, 1_u64, (17_u64 << 32) | 2];
        let metadata_builder = InvertedIndexBuilder::from_existing_index(
            InvertedIndexParams::default(),
            None,
            Vec::new(),
            TokenSetFormat::default(),
            None,
            RoaringBitmap::new(),
        );

        for partition_id in &partitions {
            write_partition_files(
                base_store.as_ref(),
                *partition_id,
                PartitionWriteTarget::Staged,
            )
            .await?;
            metadata_builder
                .write_part_metadata(base_store.as_ref(), *partition_id)
                .await?;
        }

        merge_index_files(
            object_store.as_ref(),
            &index_dir.obj_path(),
            store,
            noop_progress(),
        )
        .await?;

        let metadata_reader = base_store.open_index_file(METADATA_FILE).await?;
        let metadata = &metadata_reader.schema().metadata;
        let written_partitions: Vec<u64> = serde_json::from_str(
            metadata
                .get("partitions")
                .expect("partitions missing from metadata"),
        )?;
        let mut expected_partitions = partitions.clone();
        expected_partitions.sort_unstable();
        expected_partitions.dedup();
        let remapped_partitions = (0..expected_partitions.len() as u64).collect::<Vec<_>>();
        assert_eq!(written_partitions, remapped_partitions);
        assert_eq!(
            parse_format_version_from_metadata(metadata)?,
            InvertedListFormatVersion::V2
        );

        for (new_id, old_id) in expected_partitions.iter().enumerate() {
            assert_partition_file_markers(base_store.as_ref(), new_id as u64, *old_id).await?;
            assert!(
                base_store
                    .open_index_file(&part_metadata_file_path(*old_id))
                    .await
                    .is_err(),
                "partition metadata should be cleaned up after final metadata is written"
            );
            for suffix in PARTITION_FILE_SUFFIXES {
                assert!(
                    base_store
                        .open_index_file(&staged_partition_file_path(*old_id, suffix))
                        .await
                        .is_err(),
                    "staged partition files should be cleaned up after final metadata is written"
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_merge_index_files_rewrites_partial_final_files_from_staging() -> Result<()> {
        let index_dir = TempDir::default();
        let object_store = Arc::new(ObjectStore::local());
        let base_store: Arc<dyn IndexStore> = Arc::new(LanceIndexStore::new(
            object_store.clone(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));
        let store = Arc::new(NoRenameStore::with_final_delete_tracking(
            base_store.clone(),
        ));
        let partitions = vec![1_u64, 5_u64];
        let metadata_builder = InvertedIndexBuilder::from_existing_index(
            InvertedIndexParams::default(),
            None,
            Vec::new(),
            TokenSetFormat::default(),
            None,
            RoaringBitmap::new(),
        );

        for partition_id in &partitions {
            write_partition_files(
                base_store.as_ref(),
                *partition_id,
                PartitionWriteTarget::Staged,
            )
            .await?;
            metadata_builder
                .write_part_metadata(base_store.as_ref(), *partition_id)
                .await?;
        }

        for suffix in PARTITION_FILE_SUFFIXES {
            write_partition_file_marker(base_store.as_ref(), &partition_file_path(1, suffix), 999)
                .await?;
        }

        merge_index_files(
            object_store.as_ref(),
            &index_dir.obj_path(),
            store.clone(),
            noop_progress(),
        )
        .await?;

        assert_partition_file_markers(base_store.as_ref(), 0, 1).await?;
        assert_partition_file_markers(base_store.as_ref(), 1, 5).await?;
        assert_eq!(
            store.final_delete_count(),
            0,
            "merge should overwrite final partition files without deleting them first"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_distributed_from_existing_copies_existing_partitions_to_staging_and_finalizes()
    -> Result<()> {
        let object_store = Arc::new(ObjectStore::local());
        let source_dir = TempDir::default();
        let dest_dir = TempDir::default();
        let source_store: Arc<dyn IndexStore> = Arc::new(LanceIndexStore::new(
            object_store.clone(),
            source_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));
        let dest_store: Arc<dyn IndexStore> = Arc::new(LanceIndexStore::new(
            object_store.clone(),
            dest_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));
        let merge_store = Arc::new(NoRenameStore::new(dest_store.clone()));
        let fragment_mask = 7_u64 << 32;
        let partitions = vec![fragment_mask | 5, fragment_mask | 1];

        for partition_id in &partitions {
            write_partition_files(
                source_store.as_ref(),
                *partition_id,
                PartitionWriteTarget::Final,
            )
            .await?;
        }

        let builder = InvertedIndexBuilder::from_existing_index(
            InvertedIndexParams::default(),
            Some(source_store.clone()),
            partitions.clone(),
            TokenSetFormat::default(),
            Some(fragment_mask),
            RoaringBitmap::new(),
        );
        builder.write(dest_store.as_ref()).await?;

        for partition_id in &partitions {
            assert_partition_file_markers(source_store.as_ref(), *partition_id, *partition_id)
                .await?;
            for suffix in PARTITION_FILE_SUFFIXES {
                let staged_path = staged_partition_file_path(*partition_id, suffix);
                assert_eq!(
                    read_partition_file_marker(dest_store.as_ref(), &staged_path).await?,
                    *partition_id
                );
                assert!(
                    dest_store
                        .open_index_file(&partition_file_path(*partition_id, suffix))
                        .await
                        .is_err(),
                    "distributed existing partition should be staged instead of copied to root"
                );
            }
            dest_store
                .open_index_file(&part_metadata_file_path(*partition_id))
                .await?;
        }

        merge_index_files(
            object_store.as_ref(),
            &dest_dir.obj_path(),
            merge_store,
            noop_progress(),
        )
        .await?;

        let mut expected_partitions = partitions.clone();
        expected_partitions.sort_unstable();
        for (new_id, old_id) in expected_partitions.iter().enumerate() {
            assert_partition_file_markers(dest_store.as_ref(), new_id as u64, *old_id).await?;
            for suffix in PARTITION_FILE_SUFFIXES {
                assert!(
                    dest_store
                        .open_index_file(&staged_partition_file_path(*old_id, suffix))
                        .await
                        .is_err(),
                    "staged partition files should be cleaned after final metadata is written"
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_merge_index_files_keeps_staging_when_final_metadata_write_fails() -> Result<()> {
        let index_dir = TempDir::default();
        let object_store = Arc::new(ObjectStore::local());
        let base_store: Arc<dyn IndexStore> = Arc::new(LanceIndexStore::new(
            object_store.clone(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));
        let failing_store = Arc::new(FailMetadataStore::new(base_store.clone()));
        let partitions = vec![1_u64, 5_u64];
        let metadata_builder = InvertedIndexBuilder::from_existing_index(
            InvertedIndexParams::default(),
            None,
            Vec::new(),
            TokenSetFormat::default(),
            None,
            RoaringBitmap::new(),
        );

        for partition_id in &partitions {
            write_partition_files(
                base_store.as_ref(),
                *partition_id,
                PartitionWriteTarget::Staged,
            )
            .await?;
            metadata_builder
                .write_part_metadata(base_store.as_ref(), *partition_id)
                .await?;
        }

        let err = merge_index_files(
            object_store.as_ref(),
            &index_dir.obj_path(),
            failing_store,
            noop_progress(),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("metadata write failure"),
            "expected injected metadata failure, got: {err}"
        );

        for partition_id in &partitions {
            base_store
                .open_index_file(&part_metadata_file_path(*partition_id))
                .await?;
            for suffix in PARTITION_FILE_SUFFIXES {
                let staged_path = staged_partition_file_path(*partition_id, suffix);
                assert_eq!(
                    read_partition_file_marker(base_store.as_ref(), &staged_path).await?,
                    *partition_id
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_distributed_build_writes_partition_data_to_staging() -> Result<()> {
        let index_dir = TempDir::default();
        let object_store = ObjectStore::local();
        let store = Arc::new(LanceIndexStore::new(
            object_store.into(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let fragment_mask = 7_u64 << 32;
        let batch = make_doc_batch("hello world", fragment_mask);
        let stream = RecordBatchStreamAdapter::new(batch.schema(), stream::iter(vec![Ok(batch)]));
        let stream = Box::pin(stream);
        let mut builder = InvertedIndexBuilder::new_with_fragment_mask(
            InvertedIndexParams::default(),
            Some(fragment_mask),
        );
        builder.update(stream, store.as_ref(), None).await?;

        let part_metadata_files =
            list_metadata_files(&ObjectStore::local(), &index_dir.obj_path()).await?;
        assert_eq!(part_metadata_files.len(), 1);
        assert!(
            part_metadata_files[0].starts_with("staging/part_"),
            "partition metadata should be written to staging"
        );
        let reader = store.open_index_file(&part_metadata_files[0]).await?;
        let partition_ids: Vec<u64> = serde_json::from_str(
            reader
                .schema()
                .metadata
                .get("partitions")
                .expect("partitions missing from metadata"),
        )?;
        assert_eq!(partition_ids.len(), 1);
        let partition_id = partition_ids[0];

        store
            .open_index_file(&staged_partition_file_path(partition_id, TOKENS_FILE))
            .await?;
        assert!(
            store
                .open_index_file(&partition_file_path(partition_id, METADATA_FILE))
                .await
                .is_err(),
            "distributed build-only metadata should not be written to root partition metadata paths"
        );
        assert!(
            store
                .open_index_file(&token_file_path(partition_id))
                .await
                .is_err(),
            "distributed build-only data should not be written to final partition paths"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_merge_index_files_is_noop_when_metadata_exists() -> Result<()> {
        let index_dir = TempDir::default();
        let object_store = Arc::new(ObjectStore::local());
        let store: Arc<dyn IndexStore> = Arc::new(LanceIndexStore::new(
            object_store.clone(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));
        let metadata_builder = InvertedIndexBuilder::from_existing_index(
            InvertedIndexParams::default(),
            None,
            vec![42],
            TokenSetFormat::default(),
            None,
            RoaringBitmap::new(),
        );
        metadata_builder
            .write_metadata(store.as_ref(), &[42])
            .await?;

        merge_index_files(
            object_store.as_ref(),
            &index_dir.obj_path(),
            store,
            noop_progress(),
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_build_only_path_writes_partitions_as_is() -> Result<()> {
        let src_dir = TempDir::default();
        let dest_dir = TempDir::default();
        let src_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            src_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));
        let dest_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            dest_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let params = InvertedIndexParams::default();
        let tokenizer = params.build()?;
        let token_set_format = TokenSetFormat::default();
        let id_alloc = Arc::new(AtomicU64::new(0));

        let mut worker1 = IndexWorker::new(
            tokenizer.clone(),
            src_store.clone(),
            id_alloc.clone(),
            IndexWorkerConfig {
                with_position: params.with_position,
                format_version: InvertedListFormatVersion::V1,
                fragment_mask: None,
                token_set_format,
                worker_memory_limit_bytes: u64::MAX,
                block_size: params.block_size,
                coordinate_rank: 0,
            },
        )
        .await?;
        worker1
            .process_batch(make_doc_batch("hello world", 0))
            .await?;
        let output1 = worker1.finish().await?;
        let mut partitions = output1.partitions;
        if let Some(mut tail_partition) = output1.tail_partition {
            partitions.push(tail_partition.builder.id());
            tail_partition.builder.write(src_store.as_ref()).await?;
        }

        let mut worker2 = IndexWorker::new(
            tokenizer.clone(),
            src_store.clone(),
            id_alloc.clone(),
            IndexWorkerConfig {
                with_position: params.with_position,
                format_version: InvertedListFormatVersion::V1,
                fragment_mask: None,
                token_set_format,
                worker_memory_limit_bytes: u64::MAX,
                block_size: params.block_size,
                coordinate_rank: 0,
            },
        )
        .await?;
        worker2
            .process_batch(make_doc_batch("goodbye world", 1))
            .await?;
        let output2 = worker2.finish().await?;
        partitions.extend(output2.partitions);
        if let Some(mut tail_partition) = output2.tail_partition {
            partitions.push(tail_partition.builder.id());
            tail_partition.builder.write(src_store.as_ref()).await?;
        }
        partitions.sort_unstable();
        assert_eq!(partitions.len(), 2);
        assert_ne!(partitions[0], partitions[1]);

        let builder = InvertedIndexBuilder::from_existing_index(
            InvertedIndexParams::default(),
            Some(src_store.clone()),
            partitions.clone(),
            token_set_format,
            None,
            RoaringBitmap::new(),
        )
        .with_format_version(InvertedListFormatVersion::V1);
        builder.write(dest_store.as_ref()).await?;

        let metadata_reader = dest_store.open_index_file(METADATA_FILE).await?;
        let metadata = &metadata_reader.schema().metadata;
        let partitions_str = metadata
            .get("partitions")
            .expect("partitions missing from metadata");
        let written_partitions: Vec<u64> = serde_json::from_str(partitions_str).unwrap();
        assert_eq!(written_partitions, partitions);

        for id in &partitions {
            dest_store.open_index_file(&token_file_path(*id)).await?;
            dest_store.open_index_file(&posting_file_path(*id)).await?;
            dest_store.open_index_file(&doc_file_path(*id)).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_update_preserves_existing_posting_tail_codec() -> Result<()> {
        let src_dir = TempDir::default();
        let dest_dir = TempDir::default();
        let src_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            src_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));
        let dest_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            dest_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let posting_tail_codec = PostingTailCodec::Fixed32;
        let mut partition = InnerBuilder::new_with_posting_tail_codec(
            0,
            false,
            TokenSetFormat::default(),
            posting_tail_codec,
        );
        partition.tokens.add("hello".to_owned());
        let mut posting_list =
            PostingListBuilder::new_with_posting_tail_codec(false, posting_tail_codec);
        posting_list.add(0, PositionRecorder::Count(1));
        partition.posting_lists.push(posting_list);
        partition.docs.append(100, 1);
        partition.write(src_store.as_ref()).await?;

        let metadata_writer = InvertedIndexBuilder::from_existing_index(
            InvertedIndexParams::default(),
            Some(src_store.clone()),
            vec![0],
            TokenSetFormat::default(),
            None,
            RoaringBitmap::new(),
        )
        .with_posting_tail_codec(posting_tail_codec);
        metadata_writer
            .write_metadata(src_store.as_ref(), &[0])
            .await?;

        let index = InvertedIndex::load(src_store, None, &LanceCache::no_cache()).await?;
        let derived_params = index.derive_index_params()?;
        let derived_params: InvertedIndexParams =
            serde_json::from_str(derived_params.params.as_deref().unwrap())?;
        assert_eq!(
            derived_params.format_version,
            Some(InvertedListFormatVersion::V1)
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("doc", DataType::Utf8, true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let docs = Arc::new(StringArray::from(vec![Some("hello again")]));
        let row_ids = Arc::new(UInt64Array::from(vec![101u64]));
        let batch = RecordBatch::try_new(schema.clone(), vec![docs, row_ids])?;
        let stream = RecordBatchStreamAdapter::new(schema, stream::iter(vec![Ok(batch)]));
        index
            .update(Box::pin(stream), dest_store.as_ref(), None)
            .await?;

        let updated =
            InvertedIndex::load(dest_store.clone(), None, &LanceCache::no_cache()).await?;
        assert_eq!(updated.partitions.len(), 2);
        for partition in &updated.partitions {
            assert_eq!(
                partition.inverted_list.posting_tail_codec(),
                posting_tail_codec
            );
        }

        let metadata = dest_store.open_index_file(METADATA_FILE).await?;
        assert_eq!(
            metadata.schema().metadata.get(POSTING_TAIL_CODEC_KEY),
            Some(&posting_tail_codec.as_str().to_owned())
        );

        Ok(())
    }

    #[test]
    fn test_with_posting_tail_codec_syncs_format_version() {
        let builder = InvertedIndexBuilder::from_existing_index(
            InvertedIndexParams::default(),
            None,
            Vec::new(),
            TokenSetFormat::default(),
            None,
            RoaringBitmap::new(),
        )
        .with_format_version(InvertedListFormatVersion::V2)
        .with_posting_tail_codec(PostingTailCodec::Fixed32);
        assert_eq!(builder.format_version, InvertedListFormatVersion::V1);
        assert_eq!(builder.posting_tail_codec, PostingTailCodec::Fixed32);

        let builder = builder.with_posting_tail_codec(PostingTailCodec::VarintDelta);
        assert_eq!(builder.format_version, InvertedListFormatVersion::V2);
        assert_eq!(builder.posting_tail_codec, PostingTailCodec::VarintDelta);
    }

    #[test]
    fn test_v3_128_reuses_v2_physical_layout() {
        for with_position in [false, true] {
            for with_impacts in [false, true] {
                let v2 = inverted_list_schema_for_version_with_block_size_and_impacts(
                    with_position,
                    InvertedListFormatVersion::V2,
                    LEGACY_BLOCK_SIZE,
                    with_impacts,
                );
                let v3 = inverted_list_schema_for_version_with_block_size_and_impacts(
                    with_position,
                    InvertedListFormatVersion::V3,
                    LEGACY_BLOCK_SIZE,
                    with_impacts,
                );

                assert_eq!(v2.fields(), v3.fields());
                let mut v2_metadata = v2.metadata.clone();
                let mut v3_metadata = v3.metadata.clone();
                assert_eq!(
                    v2_metadata.remove(FTS_FORMAT_VERSION_KEY).as_deref(),
                    Some("2")
                );
                assert_eq!(
                    v3_metadata.remove(FTS_FORMAT_VERSION_KEY).as_deref(),
                    Some("3")
                );
                assert_eq!(v2_metadata, v3_metadata);
            }
        }
    }

    #[tokio::test]
    async fn test_inverted_index_without_positions_tracks_frequency() -> Result<()> {
        let index_dir = TempDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let schema = Arc::new(Schema::new(vec![
            Field::new("doc", DataType::Utf8, true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let docs = Arc::new(StringArray::from(vec![Some("hello hello world")]));
        let row_ids = Arc::new(UInt64Array::from(vec![0u64]));
        let batch = RecordBatch::try_new(schema.clone(), vec![docs, row_ids])?;
        let stream = RecordBatchStreamAdapter::new(schema, stream::iter(vec![Ok(batch)]));
        let stream = Box::pin(stream);

        let params =
            InvertedIndexParams::new("whitespace".to_string(), lance_tokenizer::Language::English)
                .with_position(false)
                .remove_stop_words(false)
                .stem(false)
                .max_token_length(None);

        let mut builder = InvertedIndexBuilder::new(params);
        builder.update(stream, store.as_ref(), None).await?;

        let index = InvertedIndex::load(store, None, &LanceCache::no_cache()).await?;
        assert_eq!(index.partitions.len(), 1);
        let partition = &index.partitions[0];
        let token_id = partition.tokens.get("hello").unwrap();
        let posting = partition
            .inverted_list
            .posting_list(token_id, false, &NoOpMetricsCollector)
            .await?;

        let mut iter = posting.iter();
        let (doc_id, freq, positions) = iter.next().unwrap();
        assert_eq!(doc_id, 0);
        assert_eq!(freq, 2);
        assert!(positions.is_none());
        assert!(iter.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_zero_token_string_documents_are_skipped_in_corpus_stats() -> Result<()> {
        let index_dir = TempDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let batch = make_doc_batch_from_docs(vec![
            Some(""),
            Some("   "),
            Some("the"),
            Some("overlength"),
            None,
            Some("hello"),
        ]);
        let stream = RecordBatchStreamAdapter::new(batch.schema(), stream::iter(vec![Ok(batch)]));
        let params =
            InvertedIndexParams::new("whitespace".to_string(), lance_tokenizer::Language::English)
                .with_position(false)
                .remove_stop_words(true)
                .stem(false)
                .max_token_length(Some(6))
                .num_workers(1);

        let mut builder = InvertedIndexBuilder::new(params);
        builder
            .update(Box::pin(stream), store.as_ref(), None)
            .await?;

        let index = InvertedIndex::load(store, None, &LanceCache::no_cache()).await?;
        let (total_tokens, num_docs, token_docs) = index
            .bm25_stats_for_terms(&["hello".to_string()], None)
            .await?;
        assert_eq!(total_tokens, 1);
        assert_eq!(num_docs, 1);
        assert_eq!(token_docs, vec![1]);

        let actual_scorer = MemBM25Scorer::new(
            total_tokens,
            num_docs,
            HashMap::from([("hello".to_string(), token_docs[0])]),
        );
        let expected_scorer = MemBM25Scorer::new(1, 1, HashMap::from([("hello".to_string(), 1)]));
        assert_eq!(
            actual_scorer.avg_doc_length(),
            expected_scorer.avg_doc_length()
        );
        assert_eq!(
            actual_scorer.query_weight("hello"),
            expected_scorer.query_weight("hello")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_all_empty_string_documents_build_empty_index() -> Result<()> {
        let index_dir = TempDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let batch = make_doc_batch_from_docs(vec![Some(""), Some("   "), None]);
        let stream = RecordBatchStreamAdapter::new(batch.schema(), stream::iter(vec![Ok(batch)]));
        let params =
            InvertedIndexParams::new("whitespace".to_string(), lance_tokenizer::Language::English)
                .with_position(false)
                .remove_stop_words(false)
                .stem(false)
                .max_token_length(None)
                .num_workers(1);

        let mut builder = InvertedIndexBuilder::new(params);
        builder
            .update(Box::pin(stream), store.as_ref(), None)
            .await?;

        let index = InvertedIndex::load(store, None, &LanceCache::no_cache()).await?;
        assert!(index.partitions.is_empty());
        let statistics = index.statistics()?;
        assert_eq!(statistics["num_tokens"], 0);
        assert_eq!(statistics["num_docs"], 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_zero_token_coordinate_documents_are_preserved_in_corpus_stats() -> Result<()> {
        let index_dir = TempDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("doc", DataType::Utf8, true),
            Field::new(doc_index_storage_column(0), DataType::UInt32, false),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    None,
                    Some(""),
                    Some("   "),
                    Some("the"),
                    Some("overlength"),
                ])),
                Arc::new(UInt32Array::from(vec![0, 1, 2, 3, 4])),
                Arc::new(UInt64Array::from(vec![7, 7, 7, 7, 7])),
            ],
        )?;
        let stream = RecordBatchStreamAdapter::new(batch.schema(), stream::iter(vec![Ok(batch)]));
        let params =
            InvertedIndexParams::new("whitespace".to_string(), lance_tokenizer::Language::English)
                .with_position(false)
                .remove_stop_words(true)
                .stem(false)
                .max_token_length(Some(6))
                .num_workers(1);

        let mut builder = InvertedIndexBuilder::new(params);
        builder
            .update(Box::pin(stream), store.as_ref(), None)
            .await?;

        let index = InvertedIndex::load(store, None, &LanceCache::no_cache()).await?;
        let statistics = index.statistics()?;
        assert_eq!(statistics["num_tokens"], 0);
        assert_eq!(statistics["num_docs"], 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_all_empty_string_documents_do_not_create_tail_partition() -> Result<()> {
        let tokenizer = InvertedIndexParams::default().build()?;
        let store = Arc::new(CountingStore::new());
        let id_alloc = Arc::new(AtomicU64::new(0));
        let mut worker = IndexWorker::new(
            tokenizer,
            store,
            id_alloc,
            IndexWorkerConfig {
                with_position: false,
                format_version: InvertedListFormatVersion::V1,
                fragment_mask: None,
                token_set_format: TokenSetFormat::default(),
                worker_memory_limit_bytes: u64::MAX,
                block_size: InvertedIndexParams::default().block_size,
                coordinate_rank: 0,
            },
        )
        .await?;

        worker
            .process_batch(make_doc_batch_from_docs(vec![Some(""), Some("   "), None]))
            .await?;
        let output = worker.finish().await?;

        assert!(output.partitions.is_empty());
        assert!(output.tail_partition.is_none());

        Ok(())
    }

    lance_testing::define_stage_event_progress!(RecordingProgress, IndexBuildProgress, Result<()>);

    #[derive(Debug, Default)]
    struct FailingProgress;

    #[async_trait]
    impl IndexBuildProgress for FailingProgress {
        async fn stage_start(&self, _stage: &str, _total: Option<u64>, _unit: &str) -> Result<()> {
            Ok(())
        }

        async fn stage_progress(&self, _stage: &str, _completed: u64) -> Result<()> {
            Err(Error::io("injected progress failure"))
        }

        async fn stage_complete(&self, _stage: &str) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_builder_reports_progress_stages() -> Result<()> {
        let index_dir = TempDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let batch1 = make_doc_batch("hello world", 0);
        let batch2 = make_doc_batch("goodbye world", 1);
        let total_rows = 2u64;
        let stream = RecordBatchStreamAdapter::new(
            batch1.schema(),
            stream::iter(vec![Ok(batch1), Ok(batch2)]),
        );
        let stream = Box::pin(stream);

        let progress = Arc::new(RecordingProgress::default());
        let mut builder = InvertedIndexBuilder::new(InvertedIndexParams::default())
            .with_progress(progress.clone());
        builder.update(stream, store.as_ref(), None).await?;

        let events = progress.recorded_events();
        let tags = events
            .iter()
            .map(|(kind, stage, _)| format!("{kind}:{stage}"))
            .collect::<Vec<_>>();
        let tokenize_progress = events
            .iter()
            .filter_map(|(kind, stage, completed)| {
                if kind == "progress" && stage == "tokenize_docs" {
                    Some(*completed)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let tokenize_start = tags
            .iter()
            .position(|e| e == "start:tokenize_docs")
            .expect("missing tokenize_docs start");
        let tokenize_complete = tags
            .iter()
            .position(|e| e == "complete:tokenize_docs")
            .expect("missing tokenize_docs complete");
        let copy_start = tags
            .iter()
            .position(|e| e == "start:copy_partitions")
            .expect("missing copy_partitions start");
        let copy_complete = tags
            .iter()
            .position(|e| e == "complete:copy_partitions")
            .expect("missing copy_partitions complete");
        let metadata_start = tags
            .iter()
            .position(|e| e == "start:write_metadata")
            .expect("missing write_metadata start");
        let metadata_complete = tags
            .iter()
            .position(|e| e == "complete:write_metadata")
            .expect("missing write_metadata complete");

        assert!(tokenize_start < tokenize_complete);
        assert!(tokenize_complete < copy_start);
        assert!(copy_start < copy_complete);
        assert!(copy_complete < metadata_start);
        assert!(metadata_start < metadata_complete);

        assert!(
            tags.iter().any(|e| e == "progress:tokenize_docs"),
            "expected progress callback for tokenize_docs"
        );
        assert!(
            tokenize_progress.len() >= 2,
            "expected at least two progress callbacks for tokenize_docs, got {tokenize_progress:?}"
        );
        assert_eq!(
            tokenize_progress.iter().copied().max().unwrap_or_default(),
            total_rows,
            "expected tokenize_docs progress to reach all rows"
        );
        assert!(
            tags.iter().any(|e| e == "progress:copy_partitions"),
            "expected progress callback for copy_partitions"
        );
        assert!(
            tags.iter().any(|e| e == "progress:write_metadata"),
            "expected progress callback for write_metadata"
        );
        assert!(
            !tags.iter().any(|e| e == "start:merge_partitions"),
            "merge_partitions should not run in the build-only path"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_builder_default_path_skips_merge_stage() -> Result<()> {
        let index_dir = TempDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let batch = make_doc_batch("hello world", 0);
        let stream = RecordBatchStreamAdapter::new(batch.schema(), stream::iter(vec![Ok(batch)]));
        let stream = Box::pin(stream);

        let progress = Arc::new(RecordingProgress::default());
        let mut builder = InvertedIndexBuilder::new(InvertedIndexParams::default())
            .with_progress(progress.clone());
        builder.update(stream, store.as_ref(), None).await?;

        let tags = progress
            .recorded_events()
            .iter()
            .map(|(kind, stage, _)| format!("{kind}:{stage}"))
            .collect::<Vec<_>>();

        assert!(
            tags.iter().any(|e| e == "start:copy_partitions"),
            "default path should copy finalized partitions"
        );
        assert!(
            !tags.iter().any(|e| e == "start:merge_partitions"),
            "default path should not run merge_partitions"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_index_files_reports_progress_stages() -> Result<()> {
        let index_dir = TempDir::default();
        let index_path = index_dir.obj_path();
        let object_store = ObjectStore::local();
        let store = Arc::new(LanceIndexStore::new(
            object_store.clone().into(),
            index_path.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        for (fragment_id, row_id, doc) in [
            (1_u64 << 32, 0_u64, "hello world"),
            (2_u64 << 32, 1_u64, "goodbye world"),
        ] {
            let batch = make_doc_batch(doc, row_id);
            let stream =
                RecordBatchStreamAdapter::new(batch.schema(), stream::iter(vec![Ok(batch)]));
            let stream = Box::pin(stream);
            let mut builder = InvertedIndexBuilder::new_with_fragment_mask(
                InvertedIndexParams::default(),
                Some(fragment_id),
            )
            .with_progress(noop_progress());
            builder.update(stream, store.as_ref(), None).await?;
        }

        let progress = Arc::new(RecordingProgress::default());
        merge_index_files(&object_store, &index_path, store.clone(), progress.clone()).await?;

        let events = progress.recorded_events();
        let tags = events
            .iter()
            .map(|(kind, stage, _)| format!("{kind}:{stage}"))
            .collect::<Vec<_>>();
        let remap_progress = events
            .iter()
            .filter_map(|(kind, stage, completed)| {
                if kind == "progress" && stage == "remap_partition_files" {
                    Some(*completed)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let read_start = tags
            .iter()
            .position(|e| e == "start:read_partition_metadata")
            .expect("missing read_partition_metadata start");
        let read_complete = tags
            .iter()
            .position(|e| e == "complete:read_partition_metadata")
            .expect("missing read_partition_metadata complete");
        let remap_start = tags
            .iter()
            .position(|e| e == "start:remap_partition_files")
            .expect("missing remap_partition_files start");
        let remap_complete = tags
            .iter()
            .position(|e| e == "complete:remap_partition_files")
            .expect("missing remap_partition_files complete");
        let metadata_start = tags
            .iter()
            .position(|e| e == "start:write_merged_metadata")
            .expect("missing write_merged_metadata start");
        let metadata_complete = tags
            .iter()
            .position(|e| e == "complete:write_merged_metadata")
            .expect("missing write_merged_metadata complete");

        assert!(read_start < read_complete);
        assert!(read_complete < remap_start);
        assert!(remap_start < remap_complete);
        assert!(remap_complete < metadata_start);
        assert!(metadata_start < metadata_complete);

        assert!(
            tags.iter().any(|e| e == "progress:read_partition_metadata"),
            "expected progress callback for read_partition_metadata"
        );
        assert_eq!(
            remap_progress.last().copied().unwrap_or_default(),
            6,
            "expected remap_partition_files progress to cover staged-to-final copies"
        );
        assert!(
            tags.iter().any(|e| e == "progress:write_merged_metadata"),
            "expected progress callback for write_merged_metadata"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_worker_memory_limit_rejects_single_large_doc() {
        let index_dir = TempDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let batch = make_doc_batch("hello world", 42);
        let stream = RecordBatchStreamAdapter::new(batch.schema(), stream::iter(vec![Ok(batch)]));
        let stream = Box::pin(stream);

        let mut builder =
            InvertedIndexBuilder::new(InvertedIndexParams::default().memory_limit_mb(0));
        let err = builder
            .update(stream, store.as_ref(), None)
            .await
            .expect_err("single doc should exceed zero worker memory limit");
        assert!(
            err.to_string().contains("row_id=42"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_worker_trims_position_temp_buffers() -> Result<()> {
        let tokenizer = InvertedIndexParams::default().with_position(true).build()?;
        let store = Arc::new(CountingStore::new());
        let id_alloc = Arc::new(AtomicU64::new(0));
        let mut worker = IndexWorker::new(
            tokenizer,
            store,
            id_alloc,
            IndexWorkerConfig {
                with_position: true,
                format_version: InvertedListFormatVersion::V1,
                fragment_mask: None,
                token_set_format: TokenSetFormat::default(),
                worker_memory_limit_bytes: u64::MAX,
                block_size: InvertedIndexParams::default().block_size,
                coordinate_rank: 0,
            },
        )
        .await?;

        let doc = (0..(MAX_RETAINED_TOKEN_IDS * 2))
            .map(|i| format!("tok{i}"))
            .collect::<Vec<_>>()
            .join(" ");
        worker.process_batch(make_doc_batch(&doc, 0)).await?;

        assert!(worker.token_ids.is_empty());
        assert!(worker.token_ids.capacity() <= MAX_RETAINED_TOKEN_IDS);
        assert!(worker.memory_size >= worker.temporary_memory_size());
        Ok(())
    }

    #[tokio::test]
    async fn test_worker_flush_keeps_position_temp_memory_bounded() -> Result<()> {
        let tokenizer = InvertedIndexParams::default().with_position(true).build()?;
        let store = Arc::new(CountingStore::new());
        let id_alloc = Arc::new(AtomicU64::new(0));
        let mut worker = IndexWorker::new(
            tokenizer,
            store,
            id_alloc,
            IndexWorkerConfig {
                with_position: true,
                format_version: InvertedListFormatVersion::V1,
                fragment_mask: None,
                token_set_format: TokenSetFormat::default(),
                worker_memory_limit_bytes: u64::MAX,
                block_size: InvertedIndexParams::default().block_size,
                coordinate_rank: 0,
            },
        )
        .await?;

        let doc = std::iter::repeat_n("common", 32_768)
            .collect::<Vec<_>>()
            .join(" ");
        let mut observed_post_flush_memory = Vec::new();
        for row_id in 0..8 {
            worker.process_batch(make_doc_batch(&doc, row_id)).await?;
            worker.flush().await?;
            observed_post_flush_memory.push(worker.memory_size);
        }

        let max_memory = *observed_post_flush_memory.iter().max().unwrap();
        let min_memory = *observed_post_flush_memory.iter().min().unwrap();
        assert!(
            max_memory <= min_memory.saturating_add(256 * 1024),
            "post-flush worker memory drifted upward: {observed_post_flush_memory:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_worker_flush_writes_partition_directly() -> Result<()> {
        let tokenizer = InvertedIndexParams::default().with_position(true).build()?;
        let store = Arc::new(CountingStore::new());
        let id_alloc = Arc::new(AtomicU64::new(0));
        let mut worker = IndexWorker::new(
            tokenizer,
            store.clone(),
            id_alloc,
            IndexWorkerConfig {
                with_position: true,
                format_version: InvertedListFormatVersion::V1,
                fragment_mask: None,
                token_set_format: TokenSetFormat::default(),
                worker_memory_limit_bytes: u64::MAX,
                block_size: InvertedIndexParams::default().block_size,
                coordinate_rank: 0,
            },
        )
        .await?;
        worker
            .process_batch(make_doc_batch("alpha beta gamma", 0))
            .await?;
        worker.flush().await?;
        assert!(store.write_count() > 0);
        Ok(())
    }

    #[test]
    fn test_resolve_worker_memory_limit_uses_default_when_unset() {
        let params = InvertedIndexParams::default();
        assert_eq!(
            resolve_worker_memory_limit_bytes(&params, 8),
            *LANCE_FTS_PARTITION_SIZE << 20
        );
    }

    #[test]
    fn test_resolve_num_workers_uses_default_when_unset() {
        let expected = default_num_workers().clamp(1, get_num_compute_intensive_cpus().max(1));
        assert_eq!(
            resolve_num_workers(&InvertedIndexParams::default()),
            expected
        );
    }

    #[test]
    fn test_resolve_num_workers_clamps_requested_value() {
        let max_workers = get_num_compute_intensive_cpus().max(1);
        assert_eq!(
            resolve_num_workers(&InvertedIndexParams::default().num_workers(0)),
            1
        );
        assert_eq!(
            resolve_num_workers(&InvertedIndexParams::default().num_workers(max_workers + 10)),
            max_workers
        );
    }

    #[test]
    fn test_resolve_worker_memory_limit_splits_total_memory_limit() {
        let params = InvertedIndexParams::default().memory_limit_mb(4096);
        assert_eq!(resolve_worker_memory_limit_bytes(&params, 16), 256 << 20);
    }

    fn tail_with_docs(id: u64, num_docs: u64) -> TailPartition {
        let mut builder = InnerBuilder::new(id, false, TokenSetFormat::default());
        let token = builder.tokens.add(format!("token{}", id));
        builder
            .posting_lists
            .resize_with(builder.tokens.len(), || PostingListBuilder::new(false));
        for row in 0..num_docs {
            let doc = builder.docs.append(row, 1);
            builder.posting_lists[token as usize].add(doc, PositionRecorder::Count(1));
        }
        TailPartition { builder }
    }

    #[test]
    fn test_merge_all_tail_partitions_combines_under_budget() -> Result<()> {
        let merged = merge_all_tail_partitions(
            vec![
                tail_with_docs(0, 4),
                tail_with_docs(1, 4),
                tail_with_docs(2, 4),
            ],
            u64::MAX,
        )?;
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].id(), 0);
        assert_eq!(merged[0].docs.len(), 12);
        Ok(())
    }

    #[test]
    fn test_merge_all_tail_partitions_splits_on_memory_budget() -> Result<()> {
        let tails = vec![
            tail_with_docs(0, 64),
            tail_with_docs(1, 64),
            tail_with_docs(2, 64),
            tail_with_docs(3, 64),
        ];
        let single = tails[0].builder.memory_size();
        // A budget below two builders' footprint must keep them separate.
        let merged = merge_all_tail_partitions(tails, single + 1)?;
        assert_eq!(merged.len(), 4);
        assert!(merged.iter().all(|builder| builder.docs.len() == 64));
        Ok(())
    }

    #[test]
    fn test_merge_all_tail_partitions_returns_none_for_empty_input() -> Result<()> {
        assert!(merge_all_tail_partitions(Vec::new(), u64::MAX)?.is_empty());
        Ok(())
    }

    /// Build an inverted index over `batches` with an explicit worker/memory
    /// layout and return it loaded, so tests can compare query behavior
    /// across partition shapes of the same corpus.
    async fn build_fuzzy_corpus_index(
        batches: Vec<RecordBatch>,
        num_workers: usize,
        memory_limit_mb: u64,
    ) -> Result<(TempDir, Arc<InvertedIndex>)> {
        let index_dir = TempDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));
        let schema = batches[0].schema();
        let stream =
            RecordBatchStreamAdapter::new(schema, stream::iter(batches.into_iter().map(Ok)));
        let params =
            InvertedIndexParams::new("whitespace".to_string(), lance_tokenizer::Language::English)
                .with_position(false)
                .remove_stop_words(false)
                .stem(false)
                .max_token_length(None)
                .num_workers(num_workers)
                .memory_limit_mb(memory_limit_mb);
        let mut builder = InvertedIndexBuilder::new(params);
        builder
            .update(Box::pin(stream), store.as_ref(), None)
            .await?;
        let index = InvertedIndex::load(store, None, &LanceCache::no_cache()).await?;
        // The caller keeps the TempDir alive for as long as it queries the
        // loaded index.
        Ok((index_dir, index))
    }

    /// Regression test for tail-partition splitting vs fuzzy queries
    /// (https://github.com/lance-format/lance/pull/7601#pullrequestreview):
    /// splitting the leftover tails into budget-sized partitions must not
    /// change fuzzy results while `max_expansions` is not the binding
    /// constraint. Every doc carries one member of two dense fuzzy families
    /// plus unique filler tokens, so a small worker memory budget forces
    /// flushes and a tail split, while a fuzzy query matches every doc.
    #[tokio::test]
    async fn test_tail_partition_split_preserves_fuzzy_results() -> Result<()> {
        use std::collections::HashMap;

        use crate::prefilter::NoFilter;
        use crate::scalar::inverted::document_tokenizer::DocType;
        use crate::scalar::inverted::query::{FtsSearchParams, Operator, Tokens};

        const NUM_DOCS: usize = 4000;
        const DOCS_PER_BATCH: usize = 20;
        let alpha_variants = ["alpha", "alphb", "alphc", "alphd", "alphe"];
        let beta_variants = ["beta", "betb", "betc", "betd", "bete"];

        let schema = Arc::new(Schema::new(vec![
            Field::new("doc", DataType::Utf8, true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let batches = (0..NUM_DOCS / DOCS_PER_BATCH)
            .map(|batch_idx| {
                let mut docs = Vec::with_capacity(DOCS_PER_BATCH);
                let mut row_ids = Vec::with_capacity(DOCS_PER_BATCH);
                for row in 0..DOCS_PER_BATCH {
                    let i = batch_idx * DOCS_PER_BATCH + row;
                    // 8 unique filler tokens per doc grow the vocabulary so
                    // the corpus comfortably exceeds the small worker budget.
                    docs.push(format!(
                        "{} {} f{i}a f{i}b f{i}c f{i}d f{i}e f{i}f f{i}g f{i}h",
                        alpha_variants[i % alpha_variants.len()],
                        beta_variants[i % beta_variants.len()],
                    ));
                    row_ids.push(i as u64);
                }
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(StringArray::from(docs)),
                        Arc::new(UInt64Array::from(row_ids)),
                    ],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        // Reference shape: everything in one partition.
        let (_ref_dir, ref_index) = build_fuzzy_corpus_index(batches.clone(), 1, 1024).await?;
        assert_eq!(
            ref_index.partitions.len(),
            1,
            "reference build should stay in a single partition"
        );

        // Tail-heavy shape: a 1MB budget across 2 workers forces flushes and
        // splits the leftover tails by the same budget.
        let (_split_dir, split_index) = build_fuzzy_corpus_index(batches, 2, 1).await?;
        assert!(
            split_index.partitions.len() > 1,
            "small worker budget should produce multiple partitions, got {}",
            split_index.partitions.len()
        );

        async fn fuzzy_search(
            index: &InvertedIndex,
            tokens: &[&str],
            operator: Operator,
        ) -> HashMap<u64, f32> {
            let tokens = Arc::new(Tokens::new(
                tokens.iter().map(|t| t.to_string()).collect(),
                DocType::Text,
            ));
            // max_expansions=50 is far above the 10 family variants, so the
            // per-partition vs global cap distinction cannot bind here.
            let params = Arc::new(
                FtsSearchParams::new()
                    .with_limit(Some(NUM_DOCS))
                    .with_fuzziness(Some(1))
                    .with_max_expansions(50),
            );
            let (row_ids, scores) = index
                .bm25_search(
                    tokens,
                    params,
                    operator,
                    Arc::new(NoFilter),
                    Arc::new(NoOpMetricsCollector),
                    None,
                )
                .await
                .unwrap();
            row_ids.into_iter().zip(scores).collect()
        }

        for (tokens, operator) in [
            (vec!["alphx"], Operator::Or),
            (vec!["alphx", "betx"], Operator::Or),
            (vec!["alphx", "betx"], Operator::And),
        ] {
            let reference = fuzzy_search(&ref_index, &tokens, operator).await;
            let split = fuzzy_search(&split_index, &tokens, operator).await;
            assert_eq!(
                reference.len(),
                NUM_DOCS,
                "every doc carries a family variant, {tokens:?} {operator:?} should match all"
            );
            assert_eq!(
                reference, split,
                "fuzzy {operator:?} results must not depend on the tail partition shape for {tokens:?}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_merge_tail_partition_group_combines_tail_builders() -> Result<()> {
        let mut first = InnerBuilder::new(0, false, TokenSetFormat::default());
        let hello = first.tokens.add("hello".to_owned());
        first
            .posting_lists
            .resize_with(first.tokens.len(), || PostingListBuilder::new(false));
        let first_doc = first.docs.append(10, 1);
        first.posting_lists[hello as usize].add(first_doc, PositionRecorder::Count(1));

        let mut second = InnerBuilder::new(1, false, TokenSetFormat::default());
        let world = second.tokens.add("world".to_owned());
        second
            .posting_lists
            .resize_with(second.tokens.len(), || PostingListBuilder::new(false));
        let second_doc = second.docs.append(20, 2);
        second.posting_lists[world as usize].add(second_doc, PositionRecorder::Count(2));

        let merged = merge_all_tail_partitions(
            vec![
                TailPartition { builder: first },
                TailPartition { builder: second },
            ],
            u64::MAX,
        )?;
        assert_eq!(merged.len(), 1);
        let merged = &merged[0];

        assert_eq!(merged.id(), 0);
        assert_eq!(merged.docs.len(), 2);
        assert_eq!(merged.tokens.len(), 2);
        assert_eq!(merged.posting_lists.len(), 2);
        assert_eq!(
            merged.posting_lists[merged.tokens.get("hello").unwrap() as usize].len(),
            1
        );
        assert_eq!(
            merged.posting_lists[merged.tokens.get("world").unwrap() as usize].len(),
            1
        );
        Ok(())
    }

    #[test]
    fn test_merge_from_after_remap_does_not_panic() {
        // `first` is the merge accumulator. Give it three tokens, then remap away the
        // middle one, mirroring filter_old_data dropping a token whose postings emptied.
        let mut first = InnerBuilder::new(0, false, TokenSetFormat::default());
        for token in ["a", "b", "c"] {
            first.tokens.add(token.to_owned());
        }
        first
            .posting_lists
            .resize_with(first.tokens.len(), || PostingListBuilder::new(false));
        let first_doc = first.docs.append(10, 1);
        first.posting_lists[0].add(first_doc, PositionRecorder::Count(1)); // "a"
        first.posting_lists[2].add(first_doc, PositionRecorder::Count(1)); // "c"

        // Remove token "b" (id 1) and compact its (empty) posting list to match.
        first.tokens.remap(&[1]);
        first.posting_lists.remove(1);
        assert_eq!(first.tokens.len(), first.posting_lists.len());

        // `second` contributes a brand-new token absent from `first`. Before the fix,
        // get_or_add returned the stale next_id, indexing past posting_lists.
        let mut second = InnerBuilder::new(1, false, TokenSetFormat::default());
        let zeta = second.tokens.add("zeta".to_owned());
        second
            .posting_lists
            .resize_with(second.tokens.len(), || PostingListBuilder::new(false));
        let second_doc = second.docs.append(20, 1);
        second.posting_lists[zeta as usize].add(second_doc, PositionRecorder::Count(1));

        first.merge_from(second).unwrap();

        assert_eq!(first.tokens.len(), 3);
        assert_eq!(first.posting_lists.len(), 3);
        let zeta_id = first.tokens.get("zeta").expect("zeta should be merged in");
        assert!((zeta_id as usize) < first.posting_lists.len());
    }

    // FST token file with a stale next_id (above the token count), as a pre-#7115 writer left.
    async fn write_stale_next_id_token_file(store: &dyn IndexStore, partition_id: u64) {
        let mut tokens = TokenSet::default();
        tokens.add("alpha".to_owned());
        tokens.add("gamma".to_owned());
        assert_eq!(tokens.len(), 2);
        tokens.next_id = 9;
        let batch = tokens.to_batch(TokenSetFormat::Fst).unwrap();
        let mut writer = store
            .new_index_file(&token_file_path(partition_id), batch.schema())
            .await
            .unwrap();
        writer.write_record_batch(batch).await.unwrap();
        writer.finish().await.unwrap();
    }

    // load_fst recomputes next_id from the token count rather than trusting the persisted value.
    #[tokio::test]
    async fn test_load_fst_recomputes_stale_next_id() {
        let index_dir = TempDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        write_stale_next_id_token_file(store.as_ref(), 0).await;
        let reader = store.open_index_file(&token_file_path(0)).await.unwrap();
        let tokens = TokenSet::load(reader, TokenSetFormat::Fst).await.unwrap();
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens.next_id(), 2);
    }

    // A stale next_id loaded from disk must not leak an out-of-range token id into a merge.
    #[tokio::test]
    async fn test_merge_with_stale_next_id_token_file_does_not_panic() {
        let index_dir = TempDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        write_stale_next_id_token_file(store.as_ref(), 0).await;
        let reader = store.open_index_file(&token_file_path(0)).await.unwrap();
        let tokens = TokenSet::load(reader, TokenSetFormat::Fst)
            .await
            .unwrap()
            .into_mutable();

        let mut first = InnerBuilder::new(0, false, TokenSetFormat::Fst);
        first.set_tokens(tokens);
        first
            .posting_lists
            .resize_with(first.tokens.len(), || PostingListBuilder::new(false));
        let doc = first.docs.append(10, 1);
        first.posting_lists[0].add(doc, PositionRecorder::Count(1));
        first.posting_lists[1].add(doc, PositionRecorder::Count(1));

        let mut second = InnerBuilder::new(1, false, TokenSetFormat::Fst);
        let zeta = second.tokens.add("zeta".to_owned());
        second
            .posting_lists
            .resize_with(second.tokens.len(), || PostingListBuilder::new(false));
        let second_doc = second.docs.append(20, 1);
        second.posting_lists[zeta as usize].add(second_doc, PositionRecorder::Count(1));

        first.merge_from(second).unwrap();
        assert_eq!(first.tokens.len(), 3);
        assert_eq!(first.posting_lists.len(), 3);
        let zeta_id = first.tokens.get("zeta").expect("zeta should be merged in");
        assert!((zeta_id as usize) < first.posting_lists.len());
    }

    #[tokio::test]
    async fn test_update_index_returns_worker_error_when_workers_exit_during_dispatch() {
        let num_batches = (*LANCE_FTS_NUM_SHARDS * 2 + 1) as u64;
        let index_dir = TempDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));
        let schema = make_doc_batch("hello world", 0).schema();
        let stream = RecordBatchStreamAdapter::new(
            schema,
            stream::iter((0..num_batches).map(|row_id| Ok(make_doc_batch("hello world", row_id)))),
        );
        let stream = Box::pin(stream);

        let mut builder = InvertedIndexBuilder::new(InvertedIndexParams::default())
            .with_progress(Arc::new(FailingProgress));

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            builder.update_index(stream, store.as_ref()),
        )
        .await
        .expect("update_index should not hang")
        .expect_err("worker failure should be returned");

        assert!(
            result.to_string().contains("injected progress failure"),
            "unexpected error: {result}"
        );
    }

    #[tokio::test]
    async fn test_new_index_has_empty_deleted_fragments() {
        let index_dir = TempDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let batch = make_doc_batch("hello world", 0);
        let stream = RecordBatchStreamAdapter::new(batch.schema(), stream::iter(vec![Ok(batch)]));
        let stream = Box::pin(stream);

        let mut builder = InvertedIndexBuilder::new(InvertedIndexParams::default());
        builder.update(stream, store.as_ref(), None).await.unwrap();

        let index = InvertedIndex::load(store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        assert!(
            index.deleted_fragments().is_empty(),
            "new index should have empty deleted fragments, got {:?}",
            index.deleted_fragments()
        );
    }

    #[tokio::test]
    async fn test_remap_preserves_deleted_fragments() {
        let src_dir = TempDir::default();
        let dest_dir = TempDir::default();
        let src_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            src_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));
        let dest_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            dest_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Build an initial index with some deleted fragments
        let batch = make_doc_batch("hello world", 0);
        let stream = RecordBatchStreamAdapter::new(batch.schema(), stream::iter(vec![Ok(batch)]));
        let stream = Box::pin(stream);

        let initial_deleted = RoaringBitmap::from_iter([5, 10, 42]);
        let mut builder = InvertedIndexBuilder::from_existing_index(
            InvertedIndexParams::default(),
            None,
            Vec::new(),
            TokenSetFormat::default(),
            None,
            initial_deleted.clone(),
        );
        builder
            .update(stream, src_store.as_ref(), None)
            .await
            .unwrap();

        // Load it back and confirm the invalidated fragments are set
        let index = InvertedIndex::load(src_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();
        assert_eq!(index.deleted_fragments(), &initial_deleted);

        // Remap the index via the ScalarIndex trait method
        use crate::scalar::ScalarIndex;
        let mapping = HashMap::from([(0u64, Some(50 << 32))]);
        index
            .remap(&RowAddrRemap::direct(mapping), dest_store.as_ref())
            .await
            .unwrap();

        // Reload from dest and verify deleted fragments are preserved
        let remapped_index = InvertedIndex::load(dest_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();
        assert_eq!(
            remapped_index.deleted_fragments(),
            &initial_deleted,
            "remap should preserve deleted fragments"
        );
    }

    #[tokio::test]
    async fn test_update_grows_deleted_fragments_from_old_data_filter() {
        let index_dir = TempDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Build an initial index with no deleted fragments
        let batch = make_doc_batch("hello world", 0);
        let stream = RecordBatchStreamAdapter::new(batch.schema(), stream::iter(vec![Ok(batch)]));
        let stream = Box::pin(stream);

        let mut builder = InvertedIndexBuilder::new(InvertedIndexParams::default());
        builder.update(stream, store.as_ref(), None).await.unwrap();

        // Load the index and update it with an old_data_filter that invalidates fragments
        let index = InvertedIndex::load(store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();
        assert!(index.deleted_fragments().is_empty());

        let update_dir = TempDir::default();
        let update_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            update_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let batch2 = make_doc_batch("new document", 1 << 32 | 1);
        let stream2 =
            RecordBatchStreamAdapter::new(batch2.schema(), stream::iter(vec![Ok(batch2)]));
        let stream2 = Box::pin(stream2);

        let old_data_filter = Some(crate::scalar::OldIndexDataFilter::Fragments {
            to_keep: RoaringBitmap::from_iter([0]),
            to_remove: RoaringBitmap::from_iter([3, 7]),
        });

        // Use ScalarIndex::update trait method
        use crate::scalar::ScalarIndex;
        index
            .update(stream2, update_store.as_ref(), old_data_filter)
            .await
            .unwrap();

        let updated_index =
            InvertedIndex::load(update_store.clone(), None, &LanceCache::no_cache())
                .await
                .unwrap();
        assert_eq!(
            updated_index.deleted_fragments(),
            &RoaringBitmap::from_iter([3, 7]),
            "update should add deleted fragments from old_data_filter"
        );
    }

    #[tokio::test]
    async fn test_update_accumulates_deleted_fragments() {
        let dir1 = TempDir::default();
        let store1 = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            dir1.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Build initial index
        let batch = make_doc_batch("hello world", 0);
        let stream = RecordBatchStreamAdapter::new(batch.schema(), stream::iter(vec![Ok(batch)]));
        let stream = Box::pin(stream);

        let mut builder = InvertedIndexBuilder::new(InvertedIndexParams::default());
        builder.update(stream, store1.as_ref(), None).await.unwrap();

        // First update: delete fragments 3 and 7
        let index = InvertedIndex::load(store1.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        let dir2 = TempDir::default();
        let store2 = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            dir2.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let batch2 = make_doc_batch("second doc", 1 << 32 | 1);
        let stream2 =
            RecordBatchStreamAdapter::new(batch2.schema(), stream::iter(vec![Ok(batch2)]));
        let stream2 = Box::pin(stream2);

        use crate::scalar::ScalarIndex;
        index
            .update(
                stream2,
                store2.as_ref(),
                Some(crate::scalar::OldIndexDataFilter::Fragments {
                    to_keep: RoaringBitmap::from_iter([0]),
                    to_remove: RoaringBitmap::from_iter([3, 7]),
                }),
            )
            .await
            .unwrap();

        // Second update: invalidate additional fragments 12 and 15
        let index2 = InvertedIndex::load(store2.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();
        assert_eq!(
            index2.deleted_fragments(),
            &RoaringBitmap::from_iter([3, 7])
        );

        let dir3 = TempDir::default();
        let store3 = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            dir3.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let batch3 = make_doc_batch("third doc", 2 << 32 | 2);
        let stream3 =
            RecordBatchStreamAdapter::new(batch3.schema(), stream::iter(vec![Ok(batch3)]));
        let stream3 = Box::pin(stream3);

        index2
            .update(
                stream3,
                store3.as_ref(),
                Some(crate::scalar::OldIndexDataFilter::Fragments {
                    to_keep: RoaringBitmap::from_iter([0, 1]),
                    to_remove: RoaringBitmap::from_iter([12, 15]),
                }),
            )
            .await
            .unwrap();

        let index3 = InvertedIndex::load(store3.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();
        assert_eq!(
            index3.deleted_fragments(),
            &RoaringBitmap::from_iter([3, 7, 12, 15]),
            "deleted fragments should accumulate across updates"
        );
    }

    #[tokio::test]
    async fn test_update_with_rowid_filter_does_not_grow_deleted_fragments() {
        let index_dir = TempDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            index_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let batch = make_doc_batch("hello world", 0);
        let stream = RecordBatchStreamAdapter::new(batch.schema(), stream::iter(vec![Ok(batch)]));
        let stream = Box::pin(stream);

        let mut builder = InvertedIndexBuilder::new(InvertedIndexParams::default());
        builder.update(stream, store.as_ref(), None).await.unwrap();

        let index = InvertedIndex::load(store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        let update_dir = TempDir::default();
        let update_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            update_dir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let batch2 = make_doc_batch("new doc", 1);
        let stream2 =
            RecordBatchStreamAdapter::new(batch2.schema(), stream::iter(vec![Ok(batch2)]));
        let stream2 = Box::pin(stream2);

        // Use RowIds filter instead of Fragments — should not affect deleted_fragments
        let mut valid_ids = lance_select::RowAddrTreeMap::new();
        valid_ids.insert(0);
        let old_data_filter = Some(crate::scalar::OldIndexDataFilter::RowIds(valid_ids));

        use crate::scalar::ScalarIndex;
        index
            .update(stream2, update_store.as_ref(), old_data_filter)
            .await
            .unwrap();

        let updated_index =
            InvertedIndex::load(update_store.clone(), None, &LanceCache::no_cache())
                .await
                .unwrap();
        assert!(
            updated_index.deleted_fragments().is_empty(),
            "RowIds filter should not add to deleted fragments"
        );
    }
}
