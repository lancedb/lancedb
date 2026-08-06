// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Disk-based shuffle a stream of [RecordBatch] into each IVF partition.
//!
//! 1. write the entire stream to a file
//! 2. count the number of rows in each partition
//! 3. read the data back into memory and shuffle into grouped vectors
//!
//! Problems for the future:
//! 1. while groupby column will stay the same, we may want to include extra data columns in the future
//! 2. shuffling into memory is fast but we should add disk buffer to support bigger datasets

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, FixedSizeListBuilder, StructBuilder, UInt8Builder, UInt32Builder, UInt64Builder,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::compute::sort_to_indices;
use arrow::datatypes::UInt32Type;
use arrow_array::{Array, RecordBatch, UInt32Array, cast::AsArray, types::UInt64Type};
use arrow_array::{FixedSizeListArray, UInt8Array};
use arrow_array::{ListArray, StructArray, UInt64Array};
use arrow_schema::{DataType, Field, Fields};
use futures::stream::repeat_with;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt, stream};
use lance_arrow::RecordBatchExt;
use lance_core::cache::LanceCache;
use lance_core::utils::futures::StreamOnDropExt;
use lance_core::utils::tokio::get_num_compute_intensive_cpus;
use lance_core::{Error, ROW_ID, Result, datatypes::Schema};
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::reader::{FileReader as Lancev2FileReader, FileReaderOptions};
use lance_file::version::ConcreteFileVersion;
use lance_file::version::LanceFileVersion;
use lance_file::versions;
use lance_file::versions::v1::reader::FileReader as V1FileReader;
use lance_file::versions::v1::writer::FileWriter as V1FileWriter;
use lance_file::writer::FileWriterOptions;
use lance_io::ReadBatchParams;
use lance_io::object_store::ObjectStore;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::stream::RecordBatchStream;
use lance_io::utils::CachedFileSize;
use lance_table::format::SelfDescribingFileReader;
use lance_table::io::manifest::ManifestDescribing;
use log::info;
use object_store::path::Path;

use crate::vector::PART_ID_COLUMN;
use crate::vector::ivf::IvfTransformer;
use crate::vector::transform::Transformer;

const UNSORTED_BUFFER: &str = "unsorted.lance";
const SHUFFLE_BATCH_SIZE: usize = 1024;

/// Returns the temp dir path plus a guard whose `Drop` removes the directory.
fn get_temp_dir() -> Result<(Path, tempfile::TempDir)> {
    let dir = tempfile::TempDir::new()?;
    let tmp_dir_path =
        Path::from_filesystem_path(dir.path()).map_err(|e| Error::io_source(Box::new(e)))?;
    Ok((tmp_dir_path, dir))
}

/// A builder for a partition of data
///
/// After we sort a batch of data into partitions we append those slices into this builder.
///
/// The builder is pre-allocated and so this extend operation should only be a memcpy
#[derive(Debug)]
struct PartitionBuilder {
    builder: StructBuilder,
}

// Fork of arrow_array::builder::make_builder that handles FixedSizeList >_<
//
// Not really suitable for upstreaming because FixedSizeListBuilder<Box<dyn ArrayBuilder>> is
// awkward and the entire make_builder function needs some overhaul (dyn ArrayBuilder should have
// an extend(array: &dyn Array) method).
fn make_builder(datatype: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    if let DataType::FixedSizeList(inner, dim) = datatype {
        let inner_builder =
            arrow_array::builder::make_builder(inner.data_type(), capacity * (*dim) as usize);
        Box::new(FixedSizeListBuilder::new(inner_builder, *dim))
    } else {
        arrow_array::builder::make_builder(datatype, capacity)
    }
}

// Fork of StructBuilder::from_fields that handles FixedSizeList >_<
fn from_fields(fields: impl Into<Fields>, capacity: usize) -> StructBuilder {
    let fields = fields.into();
    let mut builders = Vec::with_capacity(fields.len());
    for field in &fields {
        builders.push(make_builder(field.data_type(), capacity));
    }
    StructBuilder::new(fields, builders)
}

impl PartitionBuilder {
    fn new(schema: &arrow_schema::Schema, initial_capacity: usize) -> Self {
        let builder = from_fields(schema.fields.clone(), initial_capacity);
        Self { builder }
    }

    fn extend(&mut self, batch: &RecordBatch) {
        for _ in 0..batch.num_rows() {
            self.builder.append(true);
        }
        let schema = batch.schema_ref();
        for (field_idx, (col, field)) in batch.columns().iter().zip(schema.fields()).enumerate() {
            match field.data_type() {
                DataType::UInt32 => {
                    let col = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                    self.builder
                        .field_builder::<UInt32Builder>(field_idx)
                        .unwrap()
                        .append_slice(col.values());
                }
                DataType::UInt64 => {
                    let col = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                    self.builder
                        .field_builder::<UInt64Builder>(field_idx)
                        .unwrap()
                        .append_slice(col.values());
                }
                DataType::FixedSizeList(inner, _) => {
                    let col = col.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                    match inner.data_type() {
                        DataType::UInt8 => {
                            let values =
                                col.values().as_any().downcast_ref::<UInt8Array>().unwrap();
                            let fsl_builder = self
                                .builder
                                .field_builder::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>(
                                    field_idx,
                                )
                                .unwrap();
                            // TODO: Upstream an append_many to FSL builder
                            for _ in 0..col.len() {
                                fsl_builder.append(true);
                            }
                            fsl_builder
                                .values()
                                .as_any_mut()
                                .downcast_mut::<UInt8Builder>()
                                .unwrap()
                                .append_slice(values.values());
                        }
                        _ => panic!("Unexpected fixed size list item type in shuffled file"),
                    }
                }
                _ => panic!("Unexpected column type in shuffled file"),
            }
        }
    }

    // Convert the partition builder into a list array with 1 row
    fn finish(mut self) -> Result<ListArray> {
        let struct_array = Arc::new(self.builder.finish());

        let item_field = Arc::new(Field::new("item", struct_array.data_type().clone(), true));

        Ok(ListArray::try_new(
            item_field,
            OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![
                0,
                struct_array.len() as i32,
            ])),
            struct_array,
            None,
        )?)
    }
}

struct PartitionListBuilder {
    partitions: Vec<Option<PartitionBuilder>>,
    partition_sizes: Vec<u64>,
}

impl PartitionListBuilder {
    fn new(partition_sizes: Vec<u64>) -> Self {
        Self {
            partitions: Vec::default(),
            partition_sizes,
        }
    }

    fn extend(&mut self, batch: &RecordBatch) {
        if batch.num_rows() == 0 {
            return;
        }

        if self.partitions.is_empty() {
            let schema = batch.schema();
            self.partitions = Vec::from_iter(self.partition_sizes.iter().map(|part_size| {
                if *part_size == 0 {
                    None
                } else {
                    Some(PartitionBuilder::new(schema.as_ref(), *part_size as usize))
                }
            }))
        }

        let part_ids = batch[PART_ID_COLUMN].as_primitive::<UInt32Type>();

        let part_id = part_ids.value(0) as usize;

        let builder = &mut self.partitions[part_id];
        builder
            .as_mut()
            .expect("partition size was zero but received data for partition")
            .extend(batch);
    }

    fn finish(self) -> Result<Vec<ListArray>> {
        self.partitions
            .into_iter()
            .filter_map(|builder| builder.map(|builder| builder.finish()))
            .collect()
    }
}

/// Disk-based shuffle for a stream of [RecordBatch] into each IVF partition.
/// Sub-quantizer will be applied if provided.
///
/// Parameters
/// ----------
///   *data*: input data stream.
///   *column*: column name of the vector column.
///   *ivf*: IVF model.
///   *num_partitions*: number of IVF partitions.
///   *num_sub_vectors*: number of PQ sub-vectors.
///
/// Returns
/// -------
///   `Result<Vec<impl Stream<Item = Result<RecordBatch>>>>`: a vector of streams
///   of shuffled partitioned data. Each stream corresponds to a partition and
///   is sorted within the stream. Consumer of these streams is expected to merge
///   the streams into a single stream by k-list merge algo.
///
#[allow(clippy::too_many_arguments)]
pub async fn shuffle_dataset(
    data: impl RecordBatchStream + Unpin + 'static,
    ivf: Arc<IvfTransformer>,
    precomputed_partitions: Option<HashMap<u64, u32>>,
    num_partitions: u32,
    shuffle_partition_batches: usize,
    shuffle_partition_concurrency: usize,
    precomputed_shuffle_buffers: Option<(Path, Vec<String>)>,
) -> Result<Vec<impl Stream<Item = Result<RecordBatch>>>> {
    // step 1: either use precomputed shuffle files or write shuffle data to a file
    let shuffler = if let Some((path, buffers)) = precomputed_shuffle_buffers {
        info!("Precomputed shuffle files provided, skip calculation of IVF partition.");
        let mut shuffler = IvfShuffler::try_new(num_partitions, Some(path), true, None)?;
        unsafe {
            shuffler.set_unsorted_buffers(&buffers);
        }

        shuffler
    } else {
        info!(
            "Calculating IVF partitions for vectors (num_partitions={}, precomputed_partitions={})",
            num_partitions,
            precomputed_partitions.is_some()
        );
        let mut shuffler = IvfShuffler::try_new(num_partitions, None, true, None)?;

        let precomputed_partitions = precomputed_partitions.map(Arc::new);
        let stream = data
            .zip(repeat_with(move || ivf.clone()))
            .map(move |(b, ivf)| {
                // If precomputed_partitions map is provided, use it
                // for fast partitions.
                let partition_map = precomputed_partitions
                    .as_ref()
                    .cloned()
                    .unwrap_or(Arc::new(HashMap::new()));

                tokio::task::spawn(async move {
                    let mut batch = b?;

                    if !partition_map.is_empty() {
                        let row_ids = batch
                            .column_by_name(ROW_ID)
                            .ok_or(Error::index("column does not exist".to_string()))?;
                        let part_ids = UInt32Array::from_iter(
                            row_ids
                                .as_primitive::<UInt64Type>()
                                .values()
                                .iter()
                                .map(|row_id| partition_map.get(row_id).copied()),
                        );
                        let part_ids = UInt32Array::from(part_ids);
                        batch = batch
                            .try_with_column(
                                Field::new(PART_ID_COLUMN, part_ids.data_type().clone(), true),
                                Arc::new(part_ids.clone()),
                            )
                            .expect("failed to add part id column");

                        if part_ids.null_count() > 0 {
                            info!(
                                "Filter out rows without valid partition IDs: null_count={}",
                                part_ids.null_count()
                            );
                            let indices = UInt32Array::from_iter(
                                part_ids
                                    .iter()
                                    .enumerate()
                                    .filter_map(|(idx, v)| v.map(|_| idx as u32)),
                            );
                            assert_eq!(indices.len(), batch.num_rows() - part_ids.null_count());
                            batch = batch.take(&indices)?;
                        }
                    }
                    ivf.transform(&batch)
                })
            })
            .buffer_unordered(get_num_compute_intensive_cpus())
            .map(|res| match res {
                Ok(Ok(batch)) => Ok(batch),
                Ok(Err(err)) => Err(err),
                Err(join_err) => Err(Error::execution(join_err.to_string())),
            })
            .boxed();

        let start = std::time::Instant::now();
        shuffler.write_unsorted_stream(stream).await?;
        info!(
            "wrote partition assignment to unsorted tmp file in {:?}",
            start.elapsed()
        );

        shuffler
    };

    // step 2: stream in the shuffle data in chunks and write sorted chunks out
    let start = std::time::Instant::now();
    let partition_files = shuffler
        .write_partitioned_shuffles(shuffle_partition_batches, shuffle_partition_concurrency)
        .await?;
    info!("created sorted chunks in {:?}", start.elapsed());

    // step 3: load the sorted chunks, consumers are expect to be responsible for merging the streams
    let start = std::time::Instant::now();
    let streams =
        IvfShuffler::load_partitioned_shuffles(&shuffler.output_dir, partition_files).await?;
    info!("merged partitioned shuffles in {:?}", start.elapsed());

    // Clone the temp-dir guard into each returned stream so the shuffle
    // files are removed only after the consumer drops every stream.
    let temp_dir_guard = shuffler.owned_temp_dir.clone();
    let guarded_streams = streams
        .into_iter()
        .map(|stream| {
            let guard = temp_dir_guard.clone();
            stream.on_drop(move || drop(guard))
        })
        .collect::<Vec<_>>();

    Ok(guarded_streams)
}

pub async fn shuffle_vectors(
    unsorted_filenames: Vec<String>,
    dir_path: Path,
    ivf_centroids: FixedSizeListArray,
    shuffle_output_root_filename: &str,
) -> Result<Vec<String>> {
    let num_partitions = ivf_centroids.len() as u32;
    let shuffle_partition_batches = SHUFFLE_BATCH_SIZE * 10;
    let shuffle_partition_concurrency = 2;
    let mut shuffler = IvfShuffler::try_new(
        num_partitions,
        Some(dir_path),
        false,
        Some(shuffle_output_root_filename.to_string()),
    )?;

    unsafe {
        shuffler.set_unsorted_buffers(&unsorted_filenames);
    }

    let partition_files = shuffler
        .write_partitioned_shuffles(shuffle_partition_batches, shuffle_partition_concurrency)
        .await?;

    Ok(partition_files)
}

#[derive(Clone)]
pub struct IvfShuffler {
    unsorted_buffers: Vec<String>,

    num_partitions: u32,

    output_dir: Path,

    // `Some` for an auto-created `output_dir`; cleanup runs when the last
    // clone of this `Arc` is dropped. `None` when the caller owns cleanup.
    owned_temp_dir: Option<Arc<tempfile::TempDir>>,

    // whether the lance file is v1 (legacy) or v2
    is_legacy: bool,

    shuffle_output_root_filename: String,

    format_version: LanceFileVersion,
}

/// Represents a range of batches in a file that should be shuffled
struct ShuffleInput {
    // the idx of the file in IvfShuffler::unsorted_buffers
    file_idx: usize,
    // the start index of the batch in the file
    start: usize,
    // the end index of the batch in the file
    end: usize,
}

impl IvfShuffler {
    pub fn try_new(
        num_partitions: u32,
        output_dir: Option<Path>,
        is_legacy: bool,
        shuffle_output_root_filename: Option<String>,
    ) -> Result<Self> {
        let (output_dir, owned_temp_dir) = match output_dir {
            Some(output_dir) => (output_dir, None),
            None => {
                let (path, dir) = get_temp_dir()?;
                (path, Some(Arc::new(dir)))
            }
        };

        let shuffle_output_root_filename = match shuffle_output_root_filename {
            Some(shuffle_output_root_filename) => shuffle_output_root_filename,
            None => "sorted".to_string(),
        };

        Ok(Self {
            num_partitions,
            output_dir,
            owned_temp_dir,
            unsorted_buffers: vec![],
            is_legacy,
            shuffle_output_root_filename,
            format_version: LanceFileVersion::V2_0,
        })
    }

    pub fn with_format_version(mut self, format_version: LanceFileVersion) -> Self {
        self.format_version = format_version;
        self
    }

    /// Set the unsorted buffers to be shuffled.
    ///
    /// # Safety
    ///
    /// user must ensure the buffers are valid.
    pub unsafe fn set_unsorted_buffers(&mut self, unsorted_buffers: &[impl ToString]) {
        self.unsorted_buffers = unsorted_buffers.iter().map(|x| x.to_string()).collect();
    }

    pub async fn write_unsorted_stream(
        &mut self,
        data: impl Stream<Item = Result<RecordBatch>>,
    ) -> Result<()> {
        let object_store = ObjectStore::local();
        let path = self.output_dir.clone().join(UNSORTED_BUFFER);
        let writer = object_store.create(&path).await?;

        let mut data = Box::pin(data.peekable());
        let schema = match data.as_mut().peek_mut().await {
            Some(Ok(batch)) => batch.schema(),
            Some(Err(err)) => {
                // Using Error::Stop as dummy value to take the error out.
                return Err(std::mem::replace(err, Error::Stop));
            }
            None => {
                return Err(Error::invalid_input_source("data must not be empty".into()));
            }
        };

        // validate the schema,
        // we need to have row ID and partition ID column
        schema
            .column_with_name(ROW_ID)
            .ok_or(Error::io("row ID column not found".to_owned()))?;
        schema
            .column_with_name(PART_ID_COLUMN)
            .ok_or(Error::io("partition ID column not found".to_owned()))?;

        info!("Writing unsorted data to disk at {}", path);
        info!("with schema: {:?}", schema);

        let mut file_writer = V1FileWriter::<ManifestDescribing>::with_object_writer(
            writer,
            Schema::try_from(schema.as_ref())?,
            &Default::default(),
        )?;

        let mut batches_processed = 0;
        while let Some(batch) = data.next().await {
            if batches_processed % 1000 == 0 {
                info!("Partition assignment progress {}/?", batches_processed);
            }
            batches_processed += 1;
            file_writer.write(&[batch?]).await?;
        }

        file_writer.finish().await?;

        unsafe {
            self.set_unsorted_buffers(&[UNSORTED_BUFFER]);
        }

        Ok(())
    }

    async fn total_batches(&self) -> Result<Vec<usize>> {
        let mut total_batches = vec![];
        for buffer in &self.unsorted_buffers {
            let object_store = ObjectStore::local();
            let path = self.output_dir.clone().join(buffer.as_str());

            if self.is_legacy {
                let reader =
                    V1FileReader::try_new_self_described(&object_store, &path, None).await?;
                total_batches.push(reader.num_batches());
            } else {
                let scheduler_config = SchedulerConfig::max_bandwidth(&object_store);
                let scheduler = ScanScheduler::new(object_store.into(), scheduler_config);
                let file = scheduler
                    .open_file(&path, &CachedFileSize::unknown())
                    .await?;
                let cache = lance_core::cache::LanceCache::with_capacity(128 * 1024 * 1024);

                let reader = Lancev2FileReader::try_open(
                    file,
                    None,
                    Default::default(),
                    &cache,
                    FileReaderOptions::default(),
                )
                .await?;
                let num_batches = reader.metadata().num_rows / (SHUFFLE_BATCH_SIZE as u64);
                total_batches.push(num_batches as usize);
            }
        }
        Ok(total_batches)
    }

    async fn count_partition_size(&self, inputs: &[ShuffleInput]) -> Result<Vec<u64>> {
        let object_store = ObjectStore::local();
        let mut partition_sizes = vec![0; self.num_partitions as usize];
        let scheduler = ScanScheduler::new(
            Arc::new(object_store.clone()),
            SchedulerConfig::max_bandwidth(&object_store),
        );

        for &ShuffleInput {
            file_idx,
            start,
            end,
        } in inputs
        {
            let file_name = &self.unsorted_buffers[file_idx];
            let path = self.output_dir.clone().join(file_name.as_str());

            if self.is_legacy {
                let reader =
                    V1FileReader::try_new_self_described(&object_store, &path, None).await?;
                let lance_schema = reader
                    .schema()
                    .project(&[PART_ID_COLUMN])
                    .expect("part id should exist");

                let mut stream = stream::iter(start..end)
                    .map(|i| reader.read_batch(i as i32, .., &lance_schema))
                    .buffer_unordered(16);

                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    let part_ids: &UInt32Array = batch
                        .column_by_name(PART_ID_COLUMN)
                        .expect("Partition ID column not found")
                        .as_primitive();
                    part_ids.values().iter().for_each(|part_id| {
                        partition_sizes[*part_id as usize] += 1;
                    });
                }
            } else {
                let file = scheduler
                    .open_file(&path, &CachedFileSize::unknown())
                    .await?;
                let reader = Lancev2FileReader::try_open(
                    file,
                    None,
                    Default::default(),
                    &LanceCache::no_cache(),
                    FileReaderOptions::default(),
                )
                .await?;
                let mut stream = reader
                    .read_stream(
                        lance_io::ReadBatchParams::Range(
                            (start * SHUFFLE_BATCH_SIZE)..(end * SHUFFLE_BATCH_SIZE),
                        ),
                        SHUFFLE_BATCH_SIZE as u32,
                        16,
                        FilterExpression::no_filter(),
                    )
                    .await
                    .unwrap();

                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    let part_ids: &UInt32Array = batch
                        .column_by_name(PART_ID_COLUMN)
                        .expect("Partition ID column not found")
                        .as_primitive();
                    part_ids.values().iter().for_each(|part_id| {
                        partition_sizes[*part_id as usize] += 1;
                    });
                }
            }
        }

        Ok(partition_sizes)
    }

    async fn shuffle_to_partitions(
        &self,
        inputs: &[ShuffleInput],
        partition_size: Vec<u64>,
        num_batches_to_sort: usize,
    ) -> Result<Vec<ListArray>> {
        info!("Shuffling into memory");

        let mut num_processed = 0;
        let mut partitions_builder = PartitionListBuilder::new(partition_size);

        for &ShuffleInput {
            file_idx,
            start,
            end,
        } in inputs
        {
            let object_store = ObjectStore::local();
            let file_name = &self.unsorted_buffers[file_idx];
            let path = self.output_dir.clone().join(file_name.as_str());
            let mut _reader_handle = None;

            let mut stream = if self.is_legacy {
                _reader_handle =
                    Some(V1FileReader::try_new_self_described(&object_store, &path, None).await?);

                stream::iter(start..end)
                    .map(|i| {
                        let reader = _reader_handle.as_ref().unwrap();
                        reader.read_batch(i as i32, ReadBatchParams::RangeFull, reader.schema())
                    })
                    .buffered(16)
                    .boxed()
            } else {
                let scheduler_config = SchedulerConfig::max_bandwidth(&object_store);
                let scheduler = ScanScheduler::new(Arc::new(object_store), scheduler_config);
                let file = scheduler
                    .open_file(&path, &CachedFileSize::unknown())
                    .await?;
                let reader = Lancev2FileReader::try_open(
                    file,
                    None,
                    Default::default(),
                    &LanceCache::no_cache(),
                    FileReaderOptions::default(),
                )
                .await?;
                reader
                    .read_stream(
                        lance_io::ReadBatchParams::Range(
                            (start * SHUFFLE_BATCH_SIZE)..(end * SHUFFLE_BATCH_SIZE),
                        ),
                        SHUFFLE_BATCH_SIZE as u32,
                        16,
                        FilterExpression::no_filter(),
                    )
                    .await?
                    .boxed()
            };

            while let Some(batch) = stream.next().await {
                if num_processed % 100 == 0 {
                    info!("Shuffle Progress {}/{}", num_processed, num_batches_to_sort);
                }
                num_processed += 1;

                let mut batch = batch?;

                if batch.num_rows() == 0 {
                    continue;
                }

                if let Some((row_id_idx, _)) = batch.schema().column_with_name("row_id") {
                    batch = batch.rename_column(row_id_idx, ROW_ID)?;
                }

                let part_ids: &UInt32Array = batch[PART_ID_COLUMN].as_primitive();
                let indices = sort_to_indices(&part_ids, None, None)?;
                let batch = batch.take(&indices)?;

                let sorted_part_ids: &UInt32Array = batch[PART_ID_COLUMN].as_primitive();

                let mut start = 0;
                let mut prev_id = sorted_part_ids.value(0);
                for (idx, part_id) in sorted_part_ids.values().iter().enumerate() {
                    if *part_id != prev_id {
                        partitions_builder.extend(&batch.slice(start, idx - start));
                        start = idx;
                        prev_id = *part_id;
                    }
                }
                partitions_builder.extend(&batch.slice(start, sorted_part_ids.len() - start));
            }
        }

        partitions_builder.finish()
    }

    pub async fn write_partitioned_shuffles(
        &self,
        batches_per_partition: usize,
        concurrent_jobs: usize,
    ) -> Result<Vec<String>> {
        let num_batches = self.total_batches().await?;
        let total_batches = num_batches.iter().sum();
        info!(
            "Sorting unsorted data into sorted chunks (batches_per_chunk={} concurrent_jobs={})",
            batches_per_partition, concurrent_jobs
        );
        stream::iter((0..total_batches).step_by(batches_per_partition))
            .zip(stream::repeat(num_batches))
            .map(|(i, num_batches)| {
                let this = self.clone();
                tokio::spawn(async move {
                    // first, calculate which files and ranges needs to be processed
                    let start = i;
                    let end = std::cmp::min(i + batches_per_partition, total_batches);
                    let num_batches_to_sort = end - start;
                    let mut input = vec![];

                    let mut cumulative_size = 0;
                    for (file_idx, partition_size) in num_batches.iter().enumerate() {
                        let cur_start = cumulative_size;
                        let cur_end = cumulative_size + partition_size;

                        cumulative_size += partition_size;

                        let should_include_file = start < cur_end && end > cur_start;

                        if !should_include_file {
                            continue;
                        }

                        // the current part doesn't overlap with the current batch
                        if start >= cur_end {
                            continue;
                        }

                        let local_start = start.saturating_sub(cur_start);
                        let local_end = std::cmp::min(end - cur_start, *partition_size);

                        input.push(ShuffleInput {
                            file_idx,
                            start: local_start,
                            end: local_end,
                        });
                    }

                    // second, count the number of rows in each partition
                    let size_counts = this.count_partition_size(&input).await?;

                    // third, shuffle the data into each partition
                    let shuffled = this
                        .shuffle_to_partitions(&input, size_counts, num_batches_to_sort)
                        .await?;

                    // finally, write the shuffled data to disk
                    let object_store = ObjectStore::local();
                    let output_file = format!("{}_{}.lance", this.shuffle_output_root_filename, i);
                    let path = this.output_dir.clone().join(output_file.clone());
                    let writer = object_store.create(&path).await?;

                    info!(
                        "Chunk loaded into memory and sorted, writing to disk at {}",
                        path
                    );

                    let sorted_file_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
                        "partitions",
                        shuffled.first().unwrap().data_type().clone(),
                        true,
                    )]));
                    let lance_schema = Schema::try_from(sorted_file_schema.as_ref())?;
                    let mut file_writer = versions::create_writer(
                        ConcreteFileVersion::from(this.format_version),
                        writer,
                        lance_schema,
                        FileWriterOptions::default(),
                    )?;

                    for partition_and_idx in shuffled.into_iter().enumerate() {
                        let (idx, partition) = partition_and_idx;
                        if idx % 1000 == 0 {
                            info!("Writing partition {}/{}", idx, this.num_partitions);
                        }
                        let batch = RecordBatch::try_new(
                            sorted_file_schema.clone(),
                            vec![Arc::new(partition)],
                        )?;
                        file_writer.write_batch(&batch).await?;
                    }

                    file_writer.finish().await?;

                    Ok(output_file) as Result<String>
                })
                .map(|join_res| join_res.unwrap())
            })
            .buffered(concurrent_jobs)
            .try_collect()
            .await
    }

    pub async fn load_partitioned_shuffles(
        basedir: &Path,
        files: Vec<String>,
    ) -> Result<Vec<impl Stream<Item = Result<RecordBatch>> + use<>>> {
        // impl RecordBatchStream
        let mut streams = vec![];

        for file in files {
            let object_store = Arc::new(ObjectStore::local());
            let path = basedir.clone().join(file);
            let scheduler_config = SchedulerConfig::max_bandwidth(&object_store);
            let scan_scheduler = ScanScheduler::new(object_store, scheduler_config);
            let file_scheduler = scan_scheduler
                .open_file(&path, &CachedFileSize::unknown())
                .await?;
            let reader = lance_file::reader::FileReader::try_open(
                file_scheduler,
                None,
                Arc::<DecoderPlugins>::default(),
                &LanceCache::no_cache(),
                FileReaderOptions::default(),
            )
            .await?;
            let stream = reader
                .read_stream(
                    ReadBatchParams::RangeFull,
                    /*batch_size=*/ 1,
                    /*batch_readahead=*/ 32,
                    FilterExpression::no_filter(),
                )
                .await?
                .and_then(|batch| {
                    let list_array = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .expect("ListArray expected");
                    let struct_array = list_array
                        .values()
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .expect("StructArray expected")
                        .clone();
                    let batch: RecordBatch = struct_array.into();
                    std::future::ready(Ok(batch))
                });

            streams.push(stream);
        }

        Ok(streams)
    }
}

#[cfg(test)]
mod test {
    use arrow_array::{
        FixedSizeListArray, UInt8Array, UInt64Array,
        types::{UInt8Type, UInt32Type},
    };
    use arrow_schema::DataType;
    use lance_arrow::FixedSizeListArrayExt;
    use lance_core::ROW_ID_FIELD;
    use lance_io::stream::RecordBatchStreamAdapter;
    use rand::RngCore;

    use crate::vector::PQ_CODE_COLUMN;

    use super::*;

    fn make_schema(pq_dim: u32) -> Arc<arrow_schema::Schema> {
        Arc::new(arrow_schema::Schema::new(vec![
            ROW_ID_FIELD.clone(),
            arrow_schema::Field::new(PART_ID_COLUMN, DataType::UInt32, true),
            arrow_schema::Field::new(
                PQ_CODE_COLUMN,
                DataType::FixedSizeList(
                    Arc::new(arrow_schema::Field::new("item", DataType::UInt8, true)),
                    pq_dim as i32,
                ),
                false,
            ),
        ]))
    }

    fn make_stream_and_shuffler(
        include_empty_batches: bool,
    ) -> (impl RecordBatchStream, IvfShuffler) {
        let schema = make_schema(32);

        let schema2 = schema.clone();

        let stream =
            stream::iter(0..if include_empty_batches { 101 } else { 100 }).map(move |idx| {
                if include_empty_batches && idx == 100 {
                    return Ok(RecordBatch::try_new(
                        schema2.clone(),
                        vec![
                            Arc::new(UInt64Array::from_iter_values([])),
                            Arc::new(UInt32Array::from_iter_values([])),
                            Arc::new(
                                FixedSizeListArray::try_new_from_values(
                                    Arc::new(UInt8Array::from_iter_values([])) as Arc<dyn Array>,
                                    32,
                                )
                                .unwrap(),
                            ),
                        ],
                    )
                    .unwrap());
                }
                let start_idx = idx * (SHUFFLE_BATCH_SIZE as u64);
                let end_idx = (idx + 1) * (SHUFFLE_BATCH_SIZE as u64);
                let row_ids = Arc::new(UInt64Array::from_iter(start_idx..end_idx));

                let part_id = Arc::new(UInt32Array::from_iter(
                    (start_idx..end_idx).map(|_| idx as u32),
                ));

                let values = Arc::new(UInt8Array::from_iter(
                    (0..32 * SHUFFLE_BATCH_SIZE).map(|_| idx as u8),
                ));
                let pq_codes = Arc::new(
                    FixedSizeListArray::try_new_from_values(values as Arc<dyn Array>, 32).unwrap(),
                );

                Ok(
                    RecordBatch::try_new(schema2.clone(), vec![row_ids, part_id, pq_codes])
                        .unwrap(),
                )
            });

        let stream = RecordBatchStreamAdapter::new(schema, stream);

        let shuffler = IvfShuffler::try_new(100, None, true, None).unwrap();

        (stream, shuffler)
    }

    fn check_batch(batch: RecordBatch, idx: usize, num_rows: usize) {
        let row_ids = batch
            .column_by_name(ROW_ID)
            .unwrap()
            .as_primitive::<UInt64Type>();
        let part_ids = batch
            .column_by_name(PART_ID_COLUMN)
            .unwrap()
            .as_primitive::<UInt32Type>();
        let pq_codes = batch
            .column_by_name(PQ_CODE_COLUMN)
            .unwrap()
            .as_fixed_size_list()
            .values()
            .as_primitive::<UInt8Type>();

        assert_eq!(row_ids.len(), num_rows);
        assert_eq!(part_ids.len(), num_rows);
        assert_eq!(pq_codes.len(), num_rows * 32);

        for i in 0..num_rows {
            assert_eq!(part_ids.value(i), idx as u32);
        }

        for v in pq_codes.values() {
            assert_eq!(*v, idx as u8);
        }
    }

    #[tokio::test]
    async fn test_shuffler_single_partition() {
        let (stream, mut shuffler) = make_stream_and_shuffler(false);

        shuffler.write_unsorted_stream(stream).await.unwrap();
        let partition_files = shuffler.write_partitioned_shuffles(100, 1).await.unwrap();

        assert_eq!(partition_files.len(), 1);

        let mut result_stream =
            IvfShuffler::load_partitioned_shuffles(&shuffler.output_dir, partition_files)
                .await
                .unwrap();

        let mut num_batches = 0;
        let mut stream = result_stream.pop().unwrap();

        while let Some(item) = stream.next().await {
            check_batch(item.unwrap(), num_batches, SHUFFLE_BATCH_SIZE);
            num_batches += 1;
        }

        assert_eq!(num_batches, 100);
    }

    #[tokio::test]
    async fn test_shuffler_single_partition_with_empty_batch() {
        let (stream, mut shuffler) = make_stream_and_shuffler(true);

        shuffler.write_unsorted_stream(stream).await.unwrap();
        let partition_files = shuffler.write_partitioned_shuffles(101, 1).await.unwrap();

        assert_eq!(partition_files.len(), 1);

        let mut result_stream =
            IvfShuffler::load_partitioned_shuffles(&shuffler.output_dir, partition_files)
                .await
                .unwrap();

        let mut num_batches = 0;
        let mut stream = result_stream.pop().unwrap();

        while let Some(item) = stream.next().await {
            check_batch(item.unwrap(), num_batches, SHUFFLE_BATCH_SIZE);
            num_batches += 1;
        }

        assert_eq!(num_batches, 100);
    }

    #[tokio::test]
    async fn test_shuffler_multiple_partition() {
        let (stream, mut shuffler) = make_stream_and_shuffler(false);

        shuffler.write_unsorted_stream(stream).await.unwrap();
        let partition_files = shuffler.write_partitioned_shuffles(1, 100).await.unwrap();

        assert_eq!(partition_files.len(), 100);

        let mut result_stream =
            IvfShuffler::load_partitioned_shuffles(&shuffler.output_dir, partition_files)
                .await
                .unwrap();

        let mut num_batches = 0;
        result_stream.reverse();

        while let Some(mut stream) = result_stream.pop() {
            while let Some(item) = stream.next().await {
                check_batch(item.unwrap(), num_batches, SHUFFLE_BATCH_SIZE);
                num_batches += 1
            }
        }

        assert_eq!(num_batches, 100);
    }

    #[tokio::test]
    async fn test_shuffler_multi_buffer_single_partition() {
        let (stream, mut shuffler) = make_stream_and_shuffler(false);
        shuffler.write_unsorted_stream(stream).await.unwrap();

        // set the same buffer twice we should get double the data
        unsafe { shuffler.set_unsorted_buffers(&[UNSORTED_BUFFER, UNSORTED_BUFFER]) }

        let partition_files = shuffler.write_partitioned_shuffles(200, 1).await.unwrap();

        assert_eq!(partition_files.len(), 1);

        let mut result_stream =
            IvfShuffler::load_partitioned_shuffles(&shuffler.output_dir, partition_files)
                .await
                .unwrap();

        let mut num_batches = 0;
        result_stream.reverse();

        while let Some(mut stream) = result_stream.pop() {
            while let Some(item) = stream.next().await {
                check_batch(item.unwrap(), num_batches, 2048);
                num_batches += 1
            }
        }

        assert_eq!(num_batches, 100);
    }

    #[tokio::test]
    async fn test_shuffler_multi_buffer_multi_partition() {
        let (stream, mut shuffler) = make_stream_and_shuffler(false);
        shuffler.write_unsorted_stream(stream).await.unwrap();

        // set the same buffer twice we should get double the data
        unsafe { shuffler.set_unsorted_buffers(&[UNSORTED_BUFFER, UNSORTED_BUFFER]) }

        let partition_files = shuffler.write_partitioned_shuffles(1, 32).await.unwrap();
        assert_eq!(partition_files.len(), 200);

        let mut result_stream =
            IvfShuffler::load_partitioned_shuffles(&shuffler.output_dir, partition_files)
                .await
                .unwrap();

        let mut num_batches = 0;
        result_stream.reverse();

        while let Some(mut stream) = result_stream.pop() {
            while let Some(item) = stream.next().await {
                check_batch(item.unwrap(), num_batches % 100, SHUFFLE_BATCH_SIZE);
                num_batches += 1
            }
        }

        assert_eq!(num_batches, 200);
    }

    fn make_big_stream_and_shuffler(
        num_batches: u32,
        num_partitions: u32,
        pq_dim: u32,
    ) -> (impl RecordBatchStream, IvfShuffler) {
        let schema = make_schema(pq_dim);

        let schema2 = schema.clone();

        let stream = stream::iter(0..num_batches).map(move |idx| {
            let mut rng = rand::rng();
            let row_ids = Arc::new(UInt64Array::from_iter(
                (idx * 1024..(idx + 1) * 1024).map(u64::from),
            ));

            let part_id = Arc::new(UInt32Array::from_iter(
                (idx * 1024..(idx + 1) * 1024).map(|_| rng.next_u32() % num_partitions),
            ));

            let values = Arc::new(UInt8Array::from_iter((0..pq_dim * 1024).map(|_| idx as u8)));
            let pq_codes = Arc::new(
                FixedSizeListArray::try_new_from_values(values as Arc<dyn Array>, pq_dim as i32)
                    .unwrap(),
            );

            Ok(RecordBatch::try_new(schema2.clone(), vec![row_ids, part_id, pq_codes]).unwrap())
        });

        let stream = RecordBatchStreamAdapter::new(schema, stream);

        let shuffler = IvfShuffler::try_new(num_partitions, None, true, None).unwrap();

        (stream, shuffler)
    }

    // Change NUM_BATCHES = 1000 * 1024 and NUM_PARTITIONS to 35000 to test 1B shuffle
    const NUM_BATCHES: u32 = 100;
    const NUM_PARTITIONS: u32 = 1000;
    const PQ_DIM: u32 = 48;
    const BATCHES_PER_PARTITION: u32 = 10200;
    const NUM_CONCURRENT_JOBS: u32 = 16;

    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn test_big_shuffle() {
        let (stream, mut shuffler) =
            make_big_stream_and_shuffler(NUM_BATCHES, NUM_PARTITIONS, PQ_DIM);

        shuffler.write_unsorted_stream(stream).await.unwrap();
        let partition_files = shuffler
            .write_partitioned_shuffles(
                BATCHES_PER_PARTITION as usize,
                NUM_CONCURRENT_JOBS as usize,
            )
            .await
            .unwrap();

        let expected_num_part_files = NUM_BATCHES.div_ceil(BATCHES_PER_PARTITION);

        assert_eq!(partition_files.len(), expected_num_part_files as usize);

        let mut result_stream =
            IvfShuffler::load_partitioned_shuffles(&shuffler.output_dir, partition_files)
                .await
                .unwrap();

        let mut num_batches = 0;
        result_stream.reverse();

        while let Some(mut stream) = result_stream.pop() {
            while (stream.next().await).is_some() {
                num_batches += 1
            }
        }

        assert_eq!(num_batches, NUM_PARTITIONS * expected_num_part_files);
    }

    // Auto-created shuffler temp dir must be removed once the shuffler and
    // its returned streams are dropped.
    #[tokio::test]
    async fn test_shuffler_cleans_up_auto_temp_dir() {
        let (stream, mut shuffler) = make_stream_and_shuffler(false);

        // Snapshot the path without cloning the `Arc` — a clone here would
        // block cleanup on drop.
        let temp_dir_path = shuffler
            .owned_temp_dir
            .as_ref()
            .expect("shuffler built with output_dir = None should own a TempDir guard")
            .path()
            .to_path_buf();

        assert!(
            temp_dir_path.is_dir(),
            "auto-created shuffler temp dir should exist while shuffler is alive: {:?}",
            temp_dir_path,
        );

        shuffler.write_unsorted_stream(stream).await.unwrap();
        let partition_files = shuffler.write_partitioned_shuffles(100, 1).await.unwrap();
        assert_eq!(partition_files.len(), 1);

        assert!(
            temp_dir_path.join("unsorted.lance").is_file(),
            "shuffler should have written unsorted.lance into its working dir: {:?}",
            temp_dir_path,
        );
        assert!(
            temp_dir_path.join("sorted_0.lance").is_file(),
            "shuffler should have written sorted_0.lance into its working dir: {:?}",
            temp_dir_path,
        );

        let mut result_streams =
            IvfShuffler::load_partitioned_shuffles(&shuffler.output_dir, partition_files)
                .await
                .unwrap();

        while let Some(mut s) = result_streams.pop() {
            while let Some(item) = s.next().await {
                let _ = item.unwrap();
            }
        }
        drop(result_streams);
        // Dropping the shuffler releases the last `Arc<TempDir>`, which
        // removes the on-disk directory.
        drop(shuffler);

        assert!(
            !temp_dir_path.exists(),
            "auto-created shuffler temp dir should be removed once the IvfShuffler and \
             its returned streams are dropped, but it still exists: {:?}",
            temp_dir_path,
        );
    }
}
