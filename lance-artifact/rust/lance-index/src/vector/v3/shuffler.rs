// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Shuffler is a component that takes a stream of record batches and shuffles them into
//! the corresponding IVF partitions.

use std::ops::Range;
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::datatypes::UInt64Type;
use arrow::{array::AsArray, compute::sort_to_indices};
use arrow_array::{RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use futures::{future::try_join_all, prelude::*};
use lance_arrow::{RecordBatchExt, SchemaExt, interleave_batches};
use lance_core::{
    Error, Result,
    cache::LanceCache,
    utils::tokio::{get_num_compute_intensive_cpus, spawn_cpu},
};
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::reader::{FileReader, FileReaderOptions};
use lance_file::version::ConcreteFileVersion;
use lance_file::version::LanceFileVersion;
use lance_file::versions;
use lance_file::writer::FileWriterOptions;
use lance_io::{
    ReadBatchParams,
    object_store::ObjectStore,
    scheduler::{ScanScheduler, SchedulerConfig},
    stream::{RecordBatchStream, RecordBatchStreamAdapter},
    utils::CachedFileSize,
};
use object_store::path::Path;

use crate::vector::{LOSS_METADATA_KEY, PART_ID_COLUMN};

#[async_trait::async_trait]
/// A reader that can read the shuffled partitions.
pub trait ShuffleReader: Send + Sync {
    /// Read a partition by partition_id
    /// will return Ok(None) if partition_size is 0
    /// check reader.partition_size(partition_id) before calling this function
    async fn read_partition(
        &self,
        partition_id: usize,
    ) -> Result<Option<Box<dyn RecordBatchStream + Unpin + 'static>>>;

    /// Get the size of the partition by partition_id
    fn partition_size(&self, partition_id: usize) -> Result<usize>;

    /// Get the total loss,
    /// if the loss is not available, return None,
    /// in such case, the caller should sum up the losses from each batch's metadata.
    /// Must be called after all partitions are read.
    fn total_loss(&self) -> Option<f64>;
}

#[async_trait::async_trait]
/// A shuffler that can shuffle the incoming stream of record batches into IVF partitions.
/// Returns a IvfShuffleReader that can be used to read the shuffled partitions.
pub trait Shuffler: Send + Sync {
    /// Shuffle the incoming stream of record batches into IVF partitions.
    /// Returns a IvfShuffleReader that can be used to read the shuffled partitions.
    async fn shuffle(
        &self,
        data: Box<dyn RecordBatchStream + Unpin + 'static>,
    ) -> Result<Box<dyn ShuffleReader>>;
}

pub struct IvfShuffler {
    object_store: Arc<ObjectStore>,
    output_dir: Path,
    num_partitions: usize,
    format_version: LanceFileVersion,

    progress: Arc<dyn crate::progress::IndexBuildProgress>,
}

impl IvfShuffler {
    pub fn new(output_dir: Path, num_partitions: usize) -> Self {
        Self {
            object_store: Arc::new(ObjectStore::local()),
            output_dir,
            num_partitions,
            format_version: LanceFileVersion::V2_0,
            progress: crate::progress::noop_progress(),
        }
    }

    pub fn with_format_version(mut self, format_version: LanceFileVersion) -> Self {
        self.format_version = format_version;
        self
    }

    pub fn with_progress(mut self, progress: Arc<dyn crate::progress::IndexBuildProgress>) -> Self {
        self.progress = progress;
        self
    }
}

#[async_trait::async_trait]
impl Shuffler for IvfShuffler {
    async fn shuffle(
        &self,
        data: Box<dyn RecordBatchStream + Unpin + 'static>,
    ) -> Result<Box<dyn ShuffleReader>> {
        let num_partitions = self.num_partitions;
        let mut partition_sizes = vec![0; num_partitions];
        let schema = data.schema().without_column(PART_ID_COLUMN);
        let mut writers = stream::iter(0..num_partitions)
            .map(|partition_id| {
                let part_path = self
                    .output_dir
                    .clone()
                    .join(format!("ivf_{}.lance", partition_id));
                let spill_path = self
                    .output_dir
                    .clone()
                    .join(format!("ivf_{}.spill", partition_id));
                let object_store = self.object_store.clone();
                let schema = schema.clone();
                let format_version = self.format_version;
                async move {
                    let writer = object_store.create(&part_path).await?;
                    let file_writer = versions::create_writer(
                        ConcreteFileVersion::from(format_version),
                        writer,
                        lance_core::datatypes::Schema::try_from(&schema)?,
                        FileWriterOptions::default(),
                    )?
                    .with_page_metadata_spill(object_store.clone(), spill_path);
                    Result::Ok(file_writer)
                }
            })
            .buffered(self.object_store.io_parallelism())
            .try_collect::<Vec<_>>()
            .await?;
        let mut parallel_sort_stream = data
            .map(|batch| {
                spawn_cpu(move || {
                    let batch = batch?;

                    let loss = batch
                        .metadata()
                        .get(LOSS_METADATA_KEY)
                        .map(|s| s.parse::<f64>().unwrap_or_default())
                        .unwrap_or_default();

                    let part_ids: &UInt32Array = batch[PART_ID_COLUMN].as_primitive();

                    let indices = sort_to_indices(&part_ids, None, None)?;
                    let batch = batch.take(&indices)?;

                    let part_ids: &UInt32Array = batch[PART_ID_COLUMN].as_primitive();
                    let batch = batch.drop_column(PART_ID_COLUMN)?;

                    let mut partition_buffers = vec![Vec::new(); num_partitions];

                    let mut start = 0;
                    while start < batch.num_rows() {
                        let part_id: u32 = part_ids.value(start);
                        let mut end = start + 1;
                        while end < batch.num_rows() && part_ids.value(end) == part_id {
                            end += 1;
                        }

                        let part_batches = &mut partition_buffers[part_id as usize];
                        part_batches.push(batch.slice(start, end - start));
                        start = end;
                    }

                    Ok::<(Vec<Vec<RecordBatch>>, f64), Error>((partition_buffers, loss))
                })
            })
            .buffered(get_num_compute_intensive_cpus());

        let mut total_loss = 0.0;
        let mut num_rows = 0u64;
        while let Some(shuffled) = parallel_sort_stream.next().await {
            let (shuffled, loss) = shuffled?;
            total_loss += loss;

            let mut futs = Vec::new();
            for (part_id, (writer, batches)) in writers.iter_mut().zip(shuffled.iter()).enumerate()
            {
                if !batches.is_empty() {
                    let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                    partition_sizes[part_id] += rows;
                    num_rows += rows as u64;
                    futs.push(writer.write_batches(batches.iter()));
                }
            }
            try_join_all(futs).await?;

            self.progress.stage_progress("shuffle", num_rows).await?;
        }

        // finish all writers
        for writer in writers.iter_mut() {
            writer.finish().await?;
        }

        Ok(Box::new(IvfShufflerReader::new(
            self.object_store.clone(),
            self.output_dir.clone(),
            partition_sizes,
            total_loss,
        )))
    }
}

pub struct IvfShufflerReader {
    scheduler: Arc<ScanScheduler>,
    output_dir: Path,
    partition_sizes: Vec<usize>,
    loss: f64,
}

impl IvfShufflerReader {
    pub fn new(
        object_store: Arc<ObjectStore>,
        output_dir: Path,
        partition_sizes: Vec<usize>,
        loss: f64,
    ) -> Self {
        let scheduler_config = SchedulerConfig::max_bandwidth(&object_store);
        let scheduler = ScanScheduler::new(object_store, scheduler_config);
        Self {
            scheduler,
            output_dir,
            partition_sizes,
            loss,
        }
    }
}

#[async_trait::async_trait]
impl ShuffleReader for IvfShufflerReader {
    async fn read_partition(
        &self,
        partition_id: usize,
    ) -> Result<Option<Box<dyn RecordBatchStream + Unpin + 'static>>> {
        if partition_id >= self.partition_sizes.len() {
            return Ok(None);
        }

        let partition_path = self
            .output_dir
            .clone()
            .join(format!("ivf_{}.lance", partition_id));

        let reader = FileReader::try_open(
            self.scheduler
                .open_file(&partition_path, &CachedFileSize::unknown())
                .await?,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await?;
        let schema: Schema = reader.schema().as_ref().into();
        let stream = reader
            .read_stream(
                lance_io::ReadBatchParams::RangeFull,
                u32::MAX,
                16,
                FilterExpression::no_filter(),
            )
            .await?;
        Ok(Some(Box::new(RecordBatchStreamAdapter::new(
            Arc::new(schema),
            stream,
        ))))
    }

    fn partition_size(&self, partition_id: usize) -> Result<usize> {
        Ok(self.partition_sizes.get(partition_id).copied().unwrap_or(0))
    }

    fn total_loss(&self) -> Option<f64> {
        Some(self.loss)
    }
}

pub struct EmptyReader;

#[async_trait::async_trait]
impl ShuffleReader for EmptyReader {
    async fn read_partition(
        &self,
        _partition_id: usize,
    ) -> Result<Option<Box<dyn RecordBatchStream + Unpin + 'static>>> {
        Ok(None)
    }

    fn partition_size(&self, _partition_id: usize) -> Result<usize> {
        Ok(0)
    }

    fn total_loss(&self) -> Option<f64> {
        None
    }
}

/// Create an IVF shuffler. Uses [`TwoFileShuffler`] by default, which writes
/// all data to just two files (data + offsets) instead of one file per partition.
/// Set `LANCE_LEGACY_SHUFFLER=1` to fall back to [`IvfShuffler`], which opens
/// one file per partition.
///
/// An optional `progress` callback can be provided to receive shuffle progress
/// updates.
pub fn create_ivf_shuffler(
    output_dir: Path,
    num_partitions: usize,
    format_version: LanceFileVersion,
    progress: Option<Arc<dyn crate::progress::IndexBuildProgress>>,
) -> Box<dyn Shuffler> {
    let use_legacy = std::env::var("LANCE_LEGACY_SHUFFLER")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if use_legacy {
        let mut shuffler =
            IvfShuffler::new(output_dir, num_partitions).with_format_version(format_version);
        if let Some(progress) = progress {
            shuffler = shuffler.with_progress(progress);
        }
        Box::new(shuffler)
    } else {
        let mut shuffler = TwoFileShuffler::new(output_dir, num_partitions);
        if let Some(progress) = progress {
            shuffler = shuffler.with_progress(progress);
        }
        Box::new(shuffler)
    }
}

const DEFAULT_SHUFFLE_BATCH_BYTES: usize = 128 * 1024 * 1024;

/// Number of rows per output batch when streaming sorted data via interleave.
/// Small enough to keep the output chunk's memory footprint modest relative to
/// the accumulated source data.
const SHUFFLE_WRITE_CHUNK_ROWS: usize = 8 * 1024;

/// Limit of how much transformed data we accumulate before spilling to disk.
///
/// A larger value will use more RAM but require less random access during the
/// read phase.
///
/// This default is likely to be fine for most use cases.
fn shuffle_batch_bytes() -> usize {
    let batch_size = std::env::var("LANCE_SHUFFLE_BATCH_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_SHUFFLE_BATCH_BYTES);
    if batch_size == 0 {
        log::warn!(
            "LANCE_SHUFFLE_BATCH_BYTES is 0, using default of {}",
            DEFAULT_SHUFFLE_BATCH_BYTES
        );
        DEFAULT_SHUFFLE_BATCH_BYTES
    } else {
        batch_size
    }
}

/// A shuffler that writes all data to just two files (data + offsets) instead
/// of one file per partition. This avoids hitting OS file descriptor limits
/// when there are many partitions.
///
/// First we accumulate data in memory until we reach the batch size limit.
/// Then we sort the data by partition ID and compute an offset per partition.
/// Then we write the data to a data file and the offsets to an offsets file.
///
/// To read the data back, we read every Nth value from the offsets file to get
/// the start and end of each partition.
///
/// Then we read those ranges from the data file.
pub struct TwoFileShuffler {
    object_store: Arc<ObjectStore>,
    output_dir: Path,
    num_partitions: usize,
    batch_size_bytes: usize,

    progress: Arc<dyn crate::progress::IndexBuildProgress>,
}

impl TwoFileShuffler {
    pub fn new(output_dir: Path, num_partitions: usize) -> Self {
        Self {
            object_store: Arc::new(ObjectStore::local()),
            output_dir,
            num_partitions,
            batch_size_bytes: shuffle_batch_bytes(),
            progress: crate::progress::noop_progress(),
        }
    }

    pub fn with_progress(mut self, progress: Arc<dyn crate::progress::IndexBuildProgress>) -> Self {
        self.progress = progress;
        self
    }

    #[cfg(test)]
    fn with_batch_size_bytes(mut self, batch_size_bytes: usize) -> Self {
        self.batch_size_bytes = batch_size_bytes;
        self
    }
}

/// `(batch_idx, row_idx)` pairs produced by [`sort_to_interleave_indices`], paired with
/// per-partition row counts.
type InterleaveResult = (Vec<(usize, usize)>, Vec<u64>);

/// Sorts rows from multiple batches by partition ID and returns interleave indices.
///
/// Builds a sort key of `(part_id, batch_idx, row_idx)` for every row across all
/// batches, sorts by `part_id`, then emits `(batch_idx, row_idx)` pairs in that
/// order. This avoids concatenating the full data: only the `UInt32` partition-ID
/// columns are touched here.
///
/// Also returns per-partition row counts (derived from the same sorted keys at no
/// extra cost).
///
/// Returns an error if any partition ID is out of range `[0, num_partitions)`.
fn sort_to_interleave_indices(
    part_id_columns: &[&UInt32Array],
    num_partitions: usize,
) -> Result<InterleaveResult> {
    let total_rows: usize = part_id_columns.iter().map(|a| a.len()).sum();
    let mut keys: Vec<(u32, u32, u32)> = Vec::with_capacity(total_rows);
    for (batch_idx, col) in part_id_columns.iter().enumerate() {
        let batch_idx = batch_idx as u32;
        for (row_idx, &part_id) in col.values().iter().enumerate() {
            keys.push((part_id, batch_idx, row_idx as u32));
        }
    }
    keys.sort_unstable_by_key(|k| k.0);

    let mut partition_counts = vec![0u64; num_partitions];
    let mut interleave_indices = Vec::with_capacity(total_rows);
    for (part_id, batch_idx, row_idx) in &keys {
        let pid = *part_id as usize;
        if pid >= num_partitions {
            return Err(Error::invalid_input(format!(
                "partition ID {} is out of range [0, {})",
                pid, num_partitions
            )));
        }
        partition_counts[pid] += 1;
        interleave_indices.push((*batch_idx as usize, *row_idx as usize));
    }
    Ok((interleave_indices, partition_counts))
}

#[async_trait::async_trait]
impl Shuffler for TwoFileShuffler {
    async fn shuffle(
        &self,
        data: Box<dyn RecordBatchStream + Unpin + 'static>,
    ) -> Result<Box<dyn ShuffleReader>> {
        let num_partitions = self.num_partitions;
        // No need to write partition ids since we can infer this from offsets
        let schema = data.schema().without_column(PART_ID_COLUMN);
        let offsets_schema = Arc::new(Schema::new(vec![Field::new(
            "offset",
            DataType::UInt64,
            false,
        )]));
        let batch_size_bytes = self.batch_size_bytes;

        // Create data file writer
        let data_path = self.output_dir.clone().join("shuffle_data.lance");
        let spill_path = self.output_dir.clone().join("shuffle_data.spill");
        let writer = self.object_store.create(&data_path).await?;
        let mut file_writer = versions::v2_1::create_writer(
            writer,
            lance_core::datatypes::Schema::try_from(&schema)?,
            Default::default(),
        )?
        .with_page_metadata_spill(self.object_store.clone(), spill_path);

        // Create offsets file writer
        let offsets_path = self.output_dir.clone().join("shuffle_offsets.lance");
        let spill_path = self.output_dir.clone().join("shuffle_offsets.spill");
        let writer = self.object_store.create(&offsets_path).await?;
        let mut offsets_writer = versions::v2_1::create_writer(
            writer,
            lance_core::datatypes::Schema::try_from(offsets_schema.as_ref())?,
            Default::default(),
        )?
        .with_page_metadata_spill(self.object_store.clone(), spill_path);

        let mut num_batches: u64 = 0;
        let mut partition_counts: Vec<u64> = vec![0; num_partitions];
        let mut global_row_count: u64 = 0;
        let mut rows_processed: u64 = 0;
        let mut total_loss = 0.0f64;
        let mut accumulated: Vec<RecordBatch> = Vec::new();
        let mut acc_bytes: usize = 0;

        let mut data = std::pin::pin!(data);
        while let Some(batch) = data.next().await {
            let batch = batch?;
            total_loss += batch
                .metadata()
                .get(LOSS_METADATA_KEY)
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            acc_bytes += batch.get_array_memory_size();
            accumulated.push(batch);

            if acc_bytes >= batch_size_bytes {
                let (total_rows, counts) = flush_shuffle_batch(
                    std::mem::take(&mut accumulated),
                    &mut file_writer,
                    &mut offsets_writer,
                    offsets_schema.clone(),
                    num_partitions,
                    global_row_count,
                )
                .await?;
                acc_bytes = 0;
                for (p, c) in counts.iter().enumerate() {
                    partition_counts[p] += c;
                }
                global_row_count += total_rows;
                rows_processed += total_rows;
                num_batches += 1;
                self.progress
                    .stage_progress("shuffle", rows_processed)
                    .await?;
            }
        }

        if !accumulated.is_empty() {
            let (total_rows, counts) = flush_shuffle_batch(
                accumulated,
                &mut file_writer,
                &mut offsets_writer,
                offsets_schema,
                num_partitions,
                global_row_count,
            )
            .await?;
            for (p, c) in counts.iter().enumerate() {
                partition_counts[p] += c;
            }
            rows_processed += total_rows;
            num_batches += 1;
            self.progress
                .stage_progress("shuffle", rows_processed)
                .await?;
        }

        // Finish files
        file_writer.finish().await?;
        offsets_writer.finish().await?;

        TwoFileShuffleReader::try_new(
            self.object_store.clone(),
            self.output_dir.clone(),
            num_partitions,
            num_batches,
            partition_counts,
            total_loss,
        )
        .await
    }
}

/// Sorts `accumulated` batches by partition ID and writes the result to the data
/// and offsets files.
///
/// Returns `(total_rows_written, per_partition_row_counts)`.
async fn flush_shuffle_batch(
    accumulated: Vec<RecordBatch>,
    file_writer: &mut versions::v2_1::Writer,
    offsets_writer: &mut versions::v2_1::Writer,
    offsets_schema: Arc<Schema>,
    num_partitions: usize,
    global_row_count: u64,
) -> Result<(u64, Vec<u64>)> {
    let total_rows: u64 = accumulated.iter().map(|b| b.num_rows() as u64).sum();

    // Clone part-id columns into the CPU task (cheap: Arc ref bump, not data copy).
    let part_id_cols: Vec<UInt32Array> = accumulated
        .iter()
        .map(|b| {
            let col: &UInt32Array = b[PART_ID_COLUMN].as_primitive();
            col.clone()
        })
        .collect();

    let np = num_partitions;
    let (interleave_indices, batch_partition_counts) =
        spawn_cpu(move || sort_to_interleave_indices(&part_id_cols.iter().collect::<Vec<_>>(), np))
            .await?;

    // Drop part-id column from source batches before interleaving.
    let source_batches: Vec<RecordBatch> = accumulated
        .into_iter()
        .map(|b| b.drop_column(PART_ID_COLUMN).map_err(Error::from))
        .collect::<Result<_>>()?;

    // Stream sorted output to the data file in fixed-size chunks so the peak
    // memory for the interleave output stays small relative to the source data.
    for chunk in interleave_indices.chunks(SHUFFLE_WRITE_CHUNK_ROWS) {
        let out = interleave_batches(&source_batches, chunk)?;
        file_writer.write_batch(&out).await?;
    }

    // Compute cumulative end-row offsets (adjusted by global position) and write
    // one offsets batch for this flush group.
    let mut adjusted_offsets = Vec::with_capacity(num_partitions);
    let mut running = 0u64;
    for count in &batch_partition_counts {
        running += count;
        adjusted_offsets.push(global_row_count + running);
    }
    let offsets_batch = RecordBatch::try_new(
        offsets_schema,
        vec![Arc::new(UInt64Array::from(adjusted_offsets))],
    )?;
    offsets_writer.write_batch(&offsets_batch).await?;

    Ok((total_rows, batch_partition_counts))
}

pub struct TwoFileShuffleReader {
    _scheduler: Arc<ScanScheduler>,
    file_reader: FileReader,
    offsets_reader: FileReader,
    num_partitions: usize,
    num_batches: u64,
    partition_counts: Vec<u64>,
    total_loss: f64,
}

impl TwoFileShuffleReader {
    async fn try_new(
        object_store: Arc<ObjectStore>,
        output_dir: Path,
        num_partitions: usize,
        num_batches: u64,
        partition_counts: Vec<u64>,
        total_loss: f64,
    ) -> Result<Box<dyn ShuffleReader>> {
        if num_batches == 0 {
            return Ok(Box::new(EmptyReader));
        }

        let scheduler_config = SchedulerConfig::max_bandwidth(&object_store);
        let scheduler = ScanScheduler::new(object_store, scheduler_config);

        let data_path = output_dir.clone().join("shuffle_data.lance");
        let file_reader = FileReader::try_open(
            scheduler
                .open_file(&data_path, &CachedFileSize::unknown())
                .await?,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await?;

        let offsets_path = output_dir.clone().join("shuffle_offsets.lance");
        let offsets_reader = FileReader::try_open(
            scheduler
                .open_file(&offsets_path, &CachedFileSize::unknown())
                .await?,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await?;

        Ok(Box::new(Self {
            _scheduler: scheduler,
            file_reader,
            offsets_reader,
            num_partitions,
            num_batches,
            partition_counts,
            total_loss,
        }))
    }

    async fn partition_ranges(&self, partition_id: usize) -> Result<Vec<Range<u64>>> {
        let mut positions = Vec::with_capacity(self.num_batches as usize * 2);
        for batch_idx in 0..self.num_batches {
            let end_pos = u32::try_from(batch_idx as usize * self.num_partitions + partition_id)
                .map_err(|_| Error::invalid_input("There are more than 2^32 partition offsets in the spill file.  Need to support 64-bit take"))?;
            if end_pos != 0 {
                positions.push(end_pos - 1);
            }
            positions.push(end_pos);
        }
        let positions = UInt32Array::from(positions);
        let num_positions = positions.len() as u32;
        let offsets_stream = self
            .offsets_reader
            .read_stream(
                ReadBatchParams::Indices(positions),
                num_positions,
                1,
                FilterExpression::no_filter(),
            )
            .await?;
        let schema = offsets_stream.schema().clone();
        let offsets = offsets_stream.try_collect::<Vec<_>>().await?;
        let offsets = if offsets.is_empty() {
            // We should not hit this path if there is no batches
            unreachable!()
        } else if offsets.len() == 1 {
            offsets.into_iter().next().unwrap()
        } else {
            concat_batches(&schema, &offsets)?
        };

        let offsets = offsets.column(0).as_primitive::<UInt64Type>();
        let mut offsets_iter = offsets.values().iter().copied();

        let mut ranges = Vec::with_capacity(self.num_batches as usize);
        for batch_idx in 0..self.num_batches {
            if batch_idx == 0 && partition_id == 0 {
                // Implicit 0 for start-of-file
                ranges.push(0..offsets_iter.next().unwrap());
            } else {
                ranges.push(offsets_iter.next().unwrap()..offsets_iter.next().unwrap());
            }
        }
        Ok(ranges)
    }
}

#[async_trait::async_trait]
impl ShuffleReader for TwoFileShuffleReader {
    async fn read_partition(
        &self,
        partition_id: usize,
    ) -> Result<Option<Box<dyn RecordBatchStream + Unpin + 'static>>> {
        if partition_id >= self.num_partitions {
            return Ok(None);
        }
        if self.partition_counts[partition_id] == 0 {
            return Ok(None);
        }

        let ranges = self.partition_ranges(partition_id).await?;
        if ranges.is_empty() {
            return Ok(None);
        }

        let schema: Schema = self.file_reader.schema().as_ref().into();
        let stream = self
            .file_reader
            .read_stream(
                ReadBatchParams::Ranges(ranges.into()),
                u32::MAX,
                16,
                FilterExpression::no_filter(),
            )
            .await?;
        Ok(Some(Box::new(RecordBatchStreamAdapter::new(
            Arc::new(schema),
            stream,
        ))))
    }

    fn partition_size(&self, partition_id: usize) -> Result<usize> {
        Ok(self
            .partition_counts
            .get(partition_id)
            .copied()
            .unwrap_or(0) as usize)
    }

    fn total_loss(&self) -> Option<f64> {
        Some(self.total_loss)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Int32Array, RecordBatch, UInt32Array};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use futures::stream;
    use lance_arrow::RecordBatchExt;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_io::stream::RecordBatchStreamAdapter;

    use crate::vector::{LOSS_METADATA_KEY, PART_ID_COLUMN};

    /// Create a test batch with partition IDs, an int column, and optional loss metadata.
    fn make_batch(part_ids: &[u32], values: &[i32], loss: Option<f64>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(PART_ID_COLUMN, DataType::UInt32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from(part_ids.to_vec())),
                Arc::new(Int32Array::from(values.to_vec())),
            ],
        )
        .unwrap();
        if let Some(loss_val) = loss {
            batch
                .add_metadata(LOSS_METADATA_KEY.to_owned(), loss_val.to_string())
                .unwrap()
        } else {
            batch
        }
    }

    fn batches_to_stream(
        batches: Vec<RecordBatch>,
    ) -> Box<dyn RecordBatchStream + Unpin + 'static> {
        let schema = batches[0].schema();
        let stream = stream::iter(batches.into_iter().map(Ok));
        Box::new(RecordBatchStreamAdapter::new(schema, stream))
    }

    /// Collect all rows from a partition into a single RecordBatch.
    async fn collect_partition(
        reader: &dyn ShuffleReader,
        partition_id: usize,
    ) -> Option<RecordBatch> {
        let stream = reader.read_partition(partition_id).await.unwrap()?;
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        if batches.is_empty() {
            return None;
        }
        Some(arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap())
    }

    #[tokio::test]
    async fn test_two_file_shuffler_round_trip() {
        let dir = TempStrDir::default();
        let output_dir = Path::from(dir.as_ref());
        let num_partitions = 3;

        // Partition 0: rows with values 10, 40
        // Partition 1: rows with values 20, 50
        // Partition 2: rows with values 30
        let batch = make_batch(&[0, 1, 2, 0, 1], &[10, 20, 30, 40, 50], None);

        let shuffler = TwoFileShuffler::new(output_dir, num_partitions);
        let stream = batches_to_stream(vec![batch]);
        let reader = shuffler.shuffle(stream).await.unwrap();

        // Verify partition sizes
        assert_eq!(reader.partition_size(0).unwrap(), 2);
        assert_eq!(reader.partition_size(1).unwrap(), 2);
        assert_eq!(reader.partition_size(2).unwrap(), 1);

        // Verify partition 0 data
        let p0 = collect_partition(reader.as_ref(), 0).await.unwrap();
        let vals: &Int32Array = p0.column_by_name("val").unwrap().as_primitive();
        let mut v: Vec<i32> = vals.iter().map(|x| x.unwrap()).collect();
        v.sort();
        assert_eq!(v, vec![10, 40]);

        // Verify partition 1 data
        let p1 = collect_partition(reader.as_ref(), 1).await.unwrap();
        let vals: &Int32Array = p1.column_by_name("val").unwrap().as_primitive();
        let mut v: Vec<i32> = vals.iter().map(|x| x.unwrap()).collect();
        v.sort();
        assert_eq!(v, vec![20, 50]);

        // Verify partition 2 data
        let p2 = collect_partition(reader.as_ref(), 2).await.unwrap();
        let vals: &Int32Array = p2.column_by_name("val").unwrap().as_primitive();
        let v: Vec<i32> = vals.iter().map(|x| x.unwrap()).collect();
        assert_eq!(v, vec![30]);

        // Out of range partition returns None
        assert!(reader.read_partition(3).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_two_file_shuffler_empty_partitions() {
        let dir = TempStrDir::default();
        let output_dir = Path::from(dir.as_ref());
        let num_partitions = 5;

        // Only use partitions 0 and 3, leaving 1, 2, 4 empty
        let batch = make_batch(&[0, 3, 0, 3], &[10, 20, 30, 40], None);

        let shuffler = TwoFileShuffler::new(output_dir, num_partitions);
        let stream = batches_to_stream(vec![batch]);
        let reader = shuffler.shuffle(stream).await.unwrap();

        assert_eq!(reader.partition_size(0).unwrap(), 2);
        assert_eq!(reader.partition_size(1).unwrap(), 0);
        assert_eq!(reader.partition_size(2).unwrap(), 0);
        assert_eq!(reader.partition_size(3).unwrap(), 2);
        assert_eq!(reader.partition_size(4).unwrap(), 0);

        assert!(reader.read_partition(1).await.unwrap().is_none());
        assert!(reader.read_partition(2).await.unwrap().is_none());
        assert!(reader.read_partition(4).await.unwrap().is_none());

        let p0 = collect_partition(reader.as_ref(), 0).await.unwrap();
        assert_eq!(p0.num_rows(), 2);
        let p3 = collect_partition(reader.as_ref(), 3).await.unwrap();
        assert_eq!(p3.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_two_file_shuffler_loss_tracking() {
        let dir = TempStrDir::default();
        let output_dir = Path::from(dir.as_ref());
        let num_partitions = 2;

        let batch1 = make_batch(&[0, 1], &[10, 20], Some(1.5));
        let batch2 = make_batch(&[0, 1], &[30, 40], Some(2.5));
        let batch3 = make_batch(&[0], &[50], Some(0.25));

        let shuffler = TwoFileShuffler::new(output_dir, num_partitions);
        let stream = batches_to_stream(vec![batch1, batch2, batch3]);
        let reader = shuffler.shuffle(stream).await.unwrap();

        let loss = reader.total_loss().unwrap();
        assert!((loss - 4.25).abs() < 1e-10, "expected 4.25, got {}", loss);
    }

    #[tokio::test]
    async fn test_two_file_shuffler_single_batch() {
        let dir = TempStrDir::default();
        let output_dir = Path::from(dir.as_ref());
        let num_partitions = 2;

        let batch = make_batch(&[1, 0], &[100, 200], Some(3.0));

        let shuffler = TwoFileShuffler::new(output_dir, num_partitions);
        let stream = batches_to_stream(vec![batch]);
        let reader = shuffler.shuffle(stream).await.unwrap();

        assert_eq!(reader.partition_size(0).unwrap(), 1);
        assert_eq!(reader.partition_size(1).unwrap(), 1);

        let p0 = collect_partition(reader.as_ref(), 0).await.unwrap();
        let vals: &Int32Array = p0.column_by_name("val").unwrap().as_primitive();
        assert_eq!(vals.value(0), 200);

        let p1 = collect_partition(reader.as_ref(), 1).await.unwrap();
        let vals: &Int32Array = p1.column_by_name("val").unwrap().as_primitive();
        assert_eq!(vals.value(0), 100);

        assert!((reader.total_loss().unwrap() - 3.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_two_file_shuffler_multiple_batches() {
        let dir = TempStrDir::default();
        let output_dir = Path::from(dir.as_ref());
        let num_partitions = 3;

        // Use a very small batch size to force multiple write batches
        // Each i32 is 4 bytes, each u32 is 4 bytes, so ~8 bytes/row.
        // With a small batch_size_bytes, we get multiple rechunked batches.
        let batch1 = make_batch(&[0, 1, 2], &[10, 20, 30], Some(1.0));
        let batch2 = make_batch(&[2, 0, 1], &[40, 50, 60], Some(2.0));
        let batch3 = make_batch(&[1, 2, 0], &[70, 80, 90], Some(3.0));

        let shuffler = TwoFileShuffler::new(output_dir, num_partitions)
            // Set very small batch size to force multiple batches
            .with_batch_size_bytes(16);
        let stream = batches_to_stream(vec![batch1, batch2, batch3]);
        let reader = shuffler.shuffle(stream).await.unwrap();

        // Partition 0 should have values: 10, 50, 90
        assert_eq!(reader.partition_size(0).unwrap(), 3);
        let p0 = collect_partition(reader.as_ref(), 0).await.unwrap();
        let vals: &Int32Array = p0.column_by_name("val").unwrap().as_primitive();
        let mut v: Vec<i32> = vals.iter().map(|x| x.unwrap()).collect();
        v.sort();
        assert_eq!(v, vec![10, 50, 90]);

        // Partition 1 should have values: 20, 60, 70
        assert_eq!(reader.partition_size(1).unwrap(), 3);
        let p1 = collect_partition(reader.as_ref(), 1).await.unwrap();
        let vals: &Int32Array = p1.column_by_name("val").unwrap().as_primitive();
        let mut v: Vec<i32> = vals.iter().map(|x| x.unwrap()).collect();
        v.sort();
        assert_eq!(v, vec![20, 60, 70]);

        // Partition 2 should have values: 30, 40, 80
        assert_eq!(reader.partition_size(2).unwrap(), 3);
        let p2 = collect_partition(reader.as_ref(), 2).await.unwrap();
        let vals: &Int32Array = p2.column_by_name("val").unwrap().as_primitive();
        let mut v: Vec<i32> = vals.iter().map(|x| x.unwrap()).collect();
        v.sort();
        assert_eq!(v, vec![30, 40, 80]);

        assert!((reader.total_loss().unwrap() - 6.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_two_file_shuffler_multi_batch_single_flush() {
        // All three batches fit within the default batch_size_bytes, so they
        // accumulate and are interleaved in a single flush group. This exercises
        // the cross-batch interleave path.
        let dir = TempStrDir::default();
        let output_dir = Path::from(dir.as_ref());
        let num_partitions = 3;

        let batch1 = make_batch(&[0, 1, 2], &[10, 20, 30], None);
        let batch2 = make_batch(&[2, 0, 1], &[40, 50, 60], None);
        let batch3 = make_batch(&[1, 2, 0], &[70, 80, 90], None);

        // Large batch_size_bytes so all three batches flush together.
        let shuffler =
            TwoFileShuffler::new(output_dir, num_partitions).with_batch_size_bytes(1024 * 1024);
        let stream = batches_to_stream(vec![batch1, batch2, batch3]);
        let reader = shuffler.shuffle(stream).await.unwrap();

        assert_eq!(reader.partition_size(0).unwrap(), 3);
        assert_eq!(reader.partition_size(1).unwrap(), 3);
        assert_eq!(reader.partition_size(2).unwrap(), 3);

        let p0 = collect_partition(reader.as_ref(), 0).await.unwrap();
        let vals: &Int32Array = p0.column_by_name("val").unwrap().as_primitive();
        let mut v: Vec<i32> = vals.iter().map(|x| x.unwrap()).collect();
        v.sort();
        assert_eq!(v, vec![10, 50, 90]);

        let p1 = collect_partition(reader.as_ref(), 1).await.unwrap();
        let vals: &Int32Array = p1.column_by_name("val").unwrap().as_primitive();
        let mut v: Vec<i32> = vals.iter().map(|x| x.unwrap()).collect();
        v.sort();
        assert_eq!(v, vec![20, 60, 70]);

        let p2 = collect_partition(reader.as_ref(), 2).await.unwrap();
        let vals: &Int32Array = p2.column_by_name("val").unwrap().as_primitive();
        let mut v: Vec<i32> = vals.iter().map(|x| x.unwrap()).collect();
        v.sort();
        assert_eq!(v, vec![30, 40, 80]);
    }

    #[tokio::test]
    async fn test_two_file_shuffler_out_of_range_partition_id() {
        let dir = TempStrDir::default();
        let output_dir = Path::from(dir.as_ref());

        // Row with partition ID 5 is out of range for num_partitions=3.
        let batch = make_batch(&[0, 5, 1], &[10, 20, 30], None);

        let shuffler = TwoFileShuffler::new(output_dir, 3);
        let stream = batches_to_stream(vec![batch]);
        let Err(err) = shuffler.shuffle(stream).await else {
            panic!("expected an error for out-of-range partition ID");
        };
        assert!(
            err.to_string().contains("partition ID 5 is out of range"),
            "unexpected error: {err}"
        );
    }
}
