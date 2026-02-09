// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::{Arc, Mutex};

use arrow::compute::concat_batches;
use arrow_array::{RecordBatch, UInt64Array};
use futures::{StreamExt, TryStreamExt};
use lance::io::ObjectStore;
use lance_core::{cache::LanceCache, utils::futures::FinallyStreamExt};
use lance_encoding::decoder::DecoderPlugins;
use lance_file::{
    reader::{FileReader, FileReaderOptions},
    writer::{FileWriter, FileWriterOptions},
};
use lance_index::scalar::IndexReader;
use lance_io::{
    scheduler::{ScanScheduler, SchedulerConfig},
    utils::CachedFileSize,
};
use rand::{seq::SliceRandom, Rng, RngCore};

use crate::{
    arrow::{SendableRecordBatchStream, SimpleRecordBatchStream},
    dataloader::permutation::util::{non_crypto_rng, TemporaryDirectory},
    Error, Result,
};

#[derive(Debug, Clone)]
pub struct ShufflerConfig {
    /// An optional seed to make the shuffle deterministic
    pub seed: Option<u64>,
    /// The maximum number of rows to write to a single file
    ///
    /// The shuffler will need to hold at least this many rows in memory.  Setting this value
    /// extremely large could cause the shuffler to use a lot of memory (depending on row size).
    ///
    /// However, the shuffler will also need to hold total_num_rows / max_rows_per_file file
    /// writers in memory.  Each of these will consume some amount of data for column write buffers.
    /// So setting this value too small could _also_ cause the shuffler to use a lot of memory and
    /// open file handles.
    pub max_rows_per_file: u64,
    /// The temporary directory to use for writing files
    pub temp_dir: TemporaryDirectory,
    /// The size of the clumps to shuffle within
    ///
    /// If a clump size is provided, then data will be shuffled in small blocks of contiguous rows.
    /// This decreases the overall randomization but can improve I/O performance when reading from
    /// cloud storage.
    pub clump_size: Option<u64>,
}

impl Default for ShufflerConfig {
    fn default() -> Self {
        Self {
            max_rows_per_file: 1024 * 1024,
            seed: Option::default(),
            temp_dir: TemporaryDirectory::default(),
            clump_size: None,
        }
    }
}

/// A shuffler that can shuffle a stream of record batches
///
/// To do this the stream is consumed and written to temporary files.  A new stream is returned
/// which returns the shuffled data from the temporary files.
///
/// If there are fewer than max_rows_per_file rows in the input stream, then the shuffler will not
/// write any files and will instead perform an in-memory shuffle.
///
/// The number of rows in the input stream must be known in advance.
pub struct Shuffler {
    config: ShufflerConfig,
    id: String,
}

impl Shuffler {
    pub fn new(config: ShufflerConfig) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        Self { config, id }
    }

    /// Shuffles a single batch of data in memory
    fn shuffle_batch(
        batch: &RecordBatch,
        rng: &mut dyn RngCore,
        clump_size: u64,
    ) -> Result<RecordBatch> {
        let num_clumps = (batch.num_rows() as u64).div_ceil(clump_size);
        let mut indices = (0..num_clumps).collect::<Vec<_>>();
        indices.shuffle(rng);
        let indices = if clump_size == 1 {
            UInt64Array::from(indices)
        } else {
            UInt64Array::from_iter_values(indices.iter().flat_map(|&clump_index| {
                if clump_index == num_clumps - 1 {
                    clump_index * clump_size..batch.num_rows() as u64
                } else {
                    clump_index * clump_size..(clump_index + 1) * clump_size
                }
            }))
        };
        Ok(arrow::compute::take_record_batch(batch, &indices)?)
    }

    async fn in_memory_shuffle(
        &self,
        data: SendableRecordBatchStream,
        mut rng: Box<dyn RngCore + Send>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = data.schema();
        let batches = data.try_collect::<Vec<_>>().await?;
        let batch = concat_batches(&schema, &batches)?;
        let shuffled = Self::shuffle_batch(&batch, &mut rng, self.config.clump_size.unwrap_or(1))?;
        log::debug!("Shuffle job {}: in-memory shuffle complete", self.id);
        Ok(Box::pin(SimpleRecordBatchStream::new(
            futures::stream::once(async move { Ok(shuffled) }),
            schema,
        )))
    }

    async fn do_shuffle(
        &self,
        mut data: SendableRecordBatchStream,
        num_rows: u64,
        mut rng: Box<dyn RngCore + Send>,
    ) -> Result<SendableRecordBatchStream> {
        let num_files = num_rows.div_ceil(self.config.max_rows_per_file);

        let temp_dir = self.config.temp_dir.create_temp_dir()?;
        let tmp_dir = temp_dir.path().to_path_buf();

        let clump_size = self.config.clump_size.unwrap_or(1);
        if clump_size == 0 {
            return Err(Error::InvalidInput {
                message: "clump size must be greater than 0".to_string(),
            });
        }

        let object_store = ObjectStore::local();
        let arrow_schema = data.schema();
        let schema = lance::datatypes::Schema::try_from(arrow_schema.as_ref())?;

        // Create file writers
        let mut file_writers = Vec::with_capacity(num_files as usize);
        for file_index in 0..num_files {
            let path = tmp_dir.join(format!("shuffle_{}_{file_index}.lance", self.id));
            let path =
                object_store::path::Path::from_absolute_path(path).map_err(|err| Error::Other {
                    message: format!("Failed to create temporary file: {}", err),
                    source: None,
                })?;
            let object_writer = object_store.create(&path).await?;
            let writer =
                FileWriter::try_new(object_writer, schema.clone(), FileWriterOptions::default())?;
            file_writers.push(writer);
        }

        let mut num_rows_seen = 0;

        // Randomly distribute clumps to files
        while let Some(batch) = data.try_next().await? {
            num_rows_seen += batch.num_rows() as u64;
            let is_last = num_rows_seen == num_rows;
            if num_rows_seen > num_rows {
                return Err(Error::Runtime {
                    message: format!("Expected {} rows but saw {} rows", num_rows, num_rows_seen),
                });
            }
            // This is kind of an annoying limitation but if we allow runt clumps from batches then
            // clumps will get unaligned and we will mess up the clumps when we do the in-memory
            // shuffle step.  If this is a problem we can probably figure out a better way to do this.
            if !is_last && !(batch.num_rows() as u64).is_multiple_of(clump_size) {
                return Err(Error::Runtime {
                    message: format!(
                        "Expected batch size ({}) to be divisible by clump size ({})",
                        batch.num_rows(),
                        clump_size
                    ),
                });
            }
            let num_clumps = (batch.num_rows() as u64).div_ceil(clump_size);
            let mut batch_offsets_for_files =
                vec![Vec::<u64>::with_capacity(batch.num_rows()); num_files as usize];
            // Partition the batch randomly and write to the appropriate accumulator
            for clump_offset in 0..num_clumps {
                let clump_start = clump_offset * clump_size;
                let num_rows_in_clump = clump_size.min(batch.num_rows() as u64 - clump_start);
                let clump_end = clump_start + num_rows_in_clump;
                let file_index = rng.random_range(0..num_files);
                batch_offsets_for_files[file_index as usize].extend(clump_start..clump_end);
            }
            for (file_index, batch_offsets) in batch_offsets_for_files.into_iter().enumerate() {
                if batch_offsets.is_empty() {
                    continue;
                }
                let indices = UInt64Array::from(batch_offsets);
                let partition = arrow::compute::take_record_batch(&batch, &indices)?;
                file_writers[file_index].write_batch(&partition).await?;
            }
        }

        // Finish writing files
        for (file_idx, mut writer) in file_writers.into_iter().enumerate() {
            let num_written = writer.finish().await?;
            log::debug!(
                "Shuffle job {}: wrote {} rows to file {}",
                self.id,
                num_written,
                file_idx
            );
        }

        let scheduler_config = SchedulerConfig::max_bandwidth(&object_store);
        let scan_scheduler = ScanScheduler::new(Arc::new(object_store), scheduler_config);
        let job_id = self.id.clone();
        let rng = Arc::new(Mutex::new(rng));

        // Second pass, read each file as a single batch and shuffle
        let stream = futures::stream::iter(0..num_files)
            .then(move |file_index| {
                let scan_scheduler = scan_scheduler.clone();
                let rng = rng.clone();
                let tmp_dir = tmp_dir.clone();
                let job_id = job_id.clone();
                async move {
                    let path = tmp_dir.join(format!("shuffle_{}_{file_index}.lance", job_id));
                    let path = object_store::path::Path::from_absolute_path(path).unwrap();
                    let file_scheduler = scan_scheduler
                        .open_file(&path, &CachedFileSize::unknown())
                        .await?;
                    let reader = FileReader::try_open(
                        file_scheduler,
                        None,
                        Arc::<DecoderPlugins>::default(),
                        &LanceCache::no_cache(),
                        FileReaderOptions::default(),
                    )
                    .await?;
                    // Need to read the entire file in a single batch for in-memory shuffling
                    let batch = reader.read_record_batch(0, reader.num_rows()).await?;
                    let mut rng = rng.lock().unwrap();
                    Self::shuffle_batch(&batch, &mut rng, clump_size)
                }
            })
            .finally(move || drop(temp_dir))
            .boxed();

        Ok(Box::pin(SimpleRecordBatchStream::new(stream, arrow_schema)))
    }

    pub async fn shuffle(
        self,
        data: SendableRecordBatchStream,
        num_rows: u64,
    ) -> Result<SendableRecordBatchStream> {
        log::debug!(
            "Shuffle job {}: shuffling {} rows and {} columns",
            self.id,
            num_rows,
            data.schema().fields.len()
        );
        let rng = non_crypto_rng(&self.config.seed);

        if num_rows < self.config.max_rows_per_file {
            return self.in_memory_shuffle(data, rng).await;
        }

        self.do_shuffle(data, num_rows, rng).await
    }
}

#[cfg(test)]
mod tests {
    use crate::arrow::LanceDbDatagenExt;

    use super::*;
    use arrow::{array::AsArray, datatypes::Int32Type};
    use datafusion::prelude::SessionContext;
    use datafusion_expr::col;
    use futures::TryStreamExt;
    use lance_datagen::{BatchCount, BatchGeneratorBuilder, ByteCount, RowCount, Seed};
    use rand::{rngs::SmallRng, SeedableRng};

    fn test_gen() -> BatchGeneratorBuilder {
        lance_datagen::gen_batch()
            .with_seed(Seed::from(42))
            .col("id", lance_datagen::array::step::<Int32Type>())
            .col(
                "name",
                lance_datagen::array::rand_utf8(ByteCount::from(10), false),
            )
    }

    fn create_test_batch(size: RowCount) -> RecordBatch {
        test_gen().into_batch_rows(size).unwrap()
    }

    fn create_test_stream(
        num_batches: BatchCount,
        batch_size: RowCount,
    ) -> SendableRecordBatchStream {
        test_gen().into_ldb_stream(batch_size, num_batches)
    }

    #[test]
    fn test_shuffle_batch_deterministic() {
        let batch = create_test_batch(RowCount::from(10));
        let mut rng1 = SmallRng::seed_from_u64(42);
        let mut rng2 = SmallRng::seed_from_u64(42);

        let shuffled1 = Shuffler::shuffle_batch(&batch, &mut rng1, 1).unwrap();
        let shuffled2 = Shuffler::shuffle_batch(&batch, &mut rng2, 1).unwrap();

        // Same seed should produce same shuffle
        assert_eq!(shuffled1, shuffled2);
    }

    #[test]
    fn test_shuffle_with_clumps() {
        let batch = create_test_batch(RowCount::from(10));
        let mut rng = SmallRng::seed_from_u64(42);
        let shuffled = Shuffler::shuffle_batch(&batch, &mut rng, 3).unwrap();
        let values = shuffled.column(0).as_primitive::<Int32Type>();

        let mut iter = values.into_iter().map(|o| o.unwrap());
        let mut frag_seen = false;
        let mut clumps_seen = 0;
        while let Some(first) = iter.next() {
            // 9 is the last value and not a full clump
            if first != 9 {
                // Otherwise we should have a full clump
                let second = iter.next().unwrap();
                let third = iter.next().unwrap();
                assert_eq!(first + 1, second);
                assert_eq!(first + 2, third);
                clumps_seen += 1;
            } else {
                frag_seen = true;
            }
        }
        assert_eq!(clumps_seen, 3);
        assert!(frag_seen);
    }

    async fn sort_batch(batch: RecordBatch) -> RecordBatch {
        let ctx = SessionContext::new();
        let df = ctx.read_batch(batch).unwrap();
        let sorted = df.sort_by(vec![col("id")]).unwrap();
        let batches = sorted.collect().await.unwrap();
        let schema = batches[0].schema();
        concat_batches(&schema, &batches).unwrap()
    }

    #[tokio::test]
    async fn test_shuffle_batch_preserves_data() {
        let batch = create_test_batch(RowCount::from(100));
        let mut rng = SmallRng::seed_from_u64(42);

        let shuffled = Shuffler::shuffle_batch(&batch, &mut rng, 1).unwrap();

        assert_ne!(shuffled, batch);

        let sorted = sort_batch(shuffled).await;

        assert_eq!(sorted, batch);
    }

    #[test]
    fn test_shuffle_batch_empty() {
        let batch = create_test_batch(RowCount::from(0));
        let mut rng = SmallRng::seed_from_u64(42);

        let shuffled = Shuffler::shuffle_batch(&batch, &mut rng, 1).unwrap();
        assert_eq!(shuffled.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_in_memory_shuffle() {
        let config = ShufflerConfig {
            temp_dir: TemporaryDirectory::None,
            ..Default::default()
        };
        let shuffler = Shuffler::new(config);

        let stream = create_test_stream(BatchCount::from(5), RowCount::from(20));

        let result_stream = shuffler.shuffle(stream, 100).await.unwrap();
        let result_batches: Vec<RecordBatch> = result_stream.try_collect().await.unwrap();

        assert_eq!(result_batches.len(), 1);
        let result_batch = result_batches.into_iter().next().unwrap();

        let unshuffled_batches = create_test_stream(BatchCount::from(5), RowCount::from(20))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let schema = unshuffled_batches[0].schema();
        let unshuffled_batch = concat_batches(&schema, &unshuffled_batches).unwrap();

        let sorted = sort_batch(result_batch).await;

        assert_eq!(unshuffled_batch, sorted);
    }

    #[tokio::test]
    async fn test_external_shuffle() {
        let config = ShufflerConfig {
            max_rows_per_file: 100,
            ..Default::default()
        };
        let shuffler = Shuffler::new(config);

        let stream = create_test_stream(BatchCount::from(5), RowCount::from(1000));

        let result_stream = shuffler.shuffle(stream, 5000).await.unwrap();
        let result_batches: Vec<RecordBatch> = result_stream.try_collect().await.unwrap();

        let unshuffled_batches = create_test_stream(BatchCount::from(5), RowCount::from(1000))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let schema = unshuffled_batches[0].schema();
        let unshuffled_batch = concat_batches(&schema, &unshuffled_batches).unwrap();

        assert_eq!(result_batches.len(), 50);
        let result_batch = concat_batches(&schema, &result_batches).unwrap();

        let sorted = sort_batch(result_batch).await;

        assert_eq!(unshuffled_batch, sorted);
    }

    #[test_log::test(tokio::test)]
    async fn test_external_clump_shuffle() {
        let config = ShufflerConfig {
            max_rows_per_file: 100,
            clump_size: Some(30),
            ..Default::default()
        };
        let shuffler = Shuffler::new(config);

        // Batch size (900) must be multiple of clump size (30)
        let stream = create_test_stream(BatchCount::from(5), RowCount::from(900));
        let schema = stream.schema();

        // Remove 10 rows from the last batch to simulate ending with partial clump
        let mut batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let last_index = batches.len() - 1;
        let sliced_last = batches[last_index].slice(0, 890);
        batches[last_index] = sliced_last;

        let stream = Box::pin(SimpleRecordBatchStream::new(
            futures::stream::iter(batches).map(Ok).boxed(),
            schema.clone(),
        ));

        let result_stream = shuffler.shuffle(stream, 4490).await.unwrap();
        let result_batches: Vec<RecordBatch> = result_stream.try_collect().await.unwrap();
        let result_batch = concat_batches(&schema, &result_batches).unwrap();

        let ids = result_batch.column(0).as_primitive::<Int32Type>();
        let mut iter = ids.into_iter().map(|o| o.unwrap());
        while let Some(first) = iter.next() {
            let rows_left_in_clump = if first == 4470 { 19 } else { 29 };
            let mut expected_next = first + 1;
            for _ in 0..rows_left_in_clump {
                let next = iter.next().unwrap();
                assert_eq!(next, expected_next);
                expected_next += 1;
            }
        }
    }
}
