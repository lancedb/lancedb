// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{
    iter,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use arrow_array::{Array, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::hash_utils::create_hashes;
use futures::{StreamExt, TryStreamExt};
use lance::arrow::SchemaExt;

use crate::{
    arrow::{SendableRecordBatchStream, SimpleRecordBatchStream},
    dataloader::{
        shuffle::{Shuffler, ShufflerConfig},
        util::TemporaryDirectory,
    },
    query::{Query, QueryBase, Select},
    Error, Result,
};

pub const SPLIT_ID_COLUMN: &str = "split_id";

/// Strategy for assigning rows to splits
#[derive(Debug, Clone)]
pub enum SplitStrategy {
    /// All rows will have split id 0
    NoSplit,
    /// Rows will be randomly assigned to splits
    ///
    /// A seed can be provided to make the assignment deterministic.
    Random {
        seed: Option<u64>,
        sizes: SplitSizes,
    },
    /// Rows will be assigned to splits based on the values in the specified columns.
    ///
    /// This will ensure rows are always assigned to the same split if the given columns do not change.
    ///
    /// The `split_weights` are used to determine the approximate number of rows in each split.  This
    /// controls how we divide up the u64 hash space.  However, it does not guarantee any particular division
    /// of rows.  For example, if all rows have identical hash values then all rows will be assigned to the same split
    /// regardless of the weights.
    ///
    /// The `discard_weight` controls what percentage of rows should be throw away.  For example, if you want your
    /// first split to have ~5% of your rows and the second split to have ~10% of your rows then you would set
    /// split_weights to [1, 2] and discard weight to 17 (or you could set split_weights to [5, 10] and discard_weight
    /// to 85).  If you set discard_weight to 0 then all rows will be assigned to a split.
    Hash {
        columns: Vec<String>,
        split_weights: Vec<u64>,
        discard_weight: u64,
    },
    /// Rows will be assigned to splits sequentially.
    ///
    /// The first N1 rows are assigned to split 1, the next N2 rows are assigned to split 2, etc.
    ///
    /// This is mainly useful for debugging and testing.
    Sequential { sizes: SplitSizes },
    /// Rows will be assigned to splits based on a calculation of one or more columns.
    ///
    /// This is useful when the splits already exist in the base table.
    ///
    /// The provided `calculation` should be an SQL statement that returns an integer value between
    /// 0 and the number of splits - 1 (the number of splits is defined by the `splits` configuration).
    ///
    /// If this strategy is used then the counts/percentages in the SplitSizes are ignored.
    Calculated { calculation: String },
}

// The default is not to split the data
//
// All data will be assigned to a single split.
impl Default for SplitStrategy {
    fn default() -> Self {
        Self::NoSplit
    }
}

impl SplitStrategy {
    pub fn validate(&self, num_rows: u64) -> Result<()> {
        match self {
            Self::NoSplit => Ok(()),
            Self::Random { sizes, .. } => sizes.validate(num_rows),
            Self::Hash {
                split_weights,
                columns,
                ..
            } => {
                if columns.is_empty() {
                    return Err(Error::InvalidInput {
                        message: "Hash strategy requires at least one column".to_string(),
                    });
                }
                if split_weights.is_empty() {
                    return Err(Error::InvalidInput {
                        message: "Hash strategy requires at least one split weight".to_string(),
                    });
                }
                if split_weights.iter().any(|w| *w == 0) {
                    return Err(Error::InvalidInput {
                        message: "Split weights must be greater than 0".to_string(),
                    });
                }
                Ok(())
            }
            Self::Sequential { sizes } => sizes.validate(num_rows),
            Self::Calculated { .. } => Ok(()),
        }
    }
}

pub struct Splitter {
    temp_dir: TemporaryDirectory,
    strategy: SplitStrategy,
}

impl Splitter {
    pub fn new(temp_dir: TemporaryDirectory, strategy: SplitStrategy) -> Self {
        Self { temp_dir, strategy }
    }

    fn sequential_split_id(
        num_rows: u64,
        split_sizes: &[u64],
        split_index: &AtomicUsize,
        counter_in_split: &AtomicU64,
        exhausted: &AtomicBool,
    ) -> UInt64Array {
        let mut split_ids = Vec::<u64>::with_capacity(num_rows as usize);

        while split_ids.len() < num_rows as usize {
            let split_id = split_index.load(Ordering::Relaxed);
            let counter = counter_in_split.load(Ordering::Relaxed);

            let split_size = split_sizes[split_id];
            let remaining_in_split = split_size - counter;

            let remaining_in_batch = num_rows - split_ids.len() as u64;

            let mut done = false;
            let rows_to_add = if remaining_in_batch < remaining_in_split {
                counter_in_split.fetch_add(remaining_in_batch, Ordering::Relaxed);
                remaining_in_batch
            } else {
                split_index.fetch_add(1, Ordering::Relaxed);
                counter_in_split.store(0, Ordering::Relaxed);
                if split_id == split_sizes.len() - 1 {
                    exhausted.store(true, Ordering::Relaxed);
                    done = true;
                }
                remaining_in_split
            };

            split_ids.extend(iter::repeat_n(split_id as u64, rows_to_add as usize));
            if done {
                // Quit early if we've run out of splits
                break;
            }
        }

        UInt64Array::from(split_ids)
    }

    async fn apply_sequential(
        &self,
        source: SendableRecordBatchStream,
        num_rows: u64,
        sizes: &SplitSizes,
    ) -> Result<SendableRecordBatchStream> {
        let split_sizes = sizes.to_counts(num_rows);

        let split_index = AtomicUsize::new(0);
        let counter_in_split = AtomicU64::new(0);
        let exhausted = AtomicBool::new(false);

        let schema = source.schema();

        let new_schema = Arc::new(schema.try_with_column(Field::new(
            SPLIT_ID_COLUMN,
            DataType::UInt64,
            false,
        ))?);

        let new_schema_clone = new_schema.clone();
        let stream = source.filter_map(move |batch| {
            let batch = match batch {
                Ok(batch) => batch,
                Err(e) => {
                    return std::future::ready(Some(Err(e)));
                }
            };

            if exhausted.load(Ordering::Relaxed) {
                return std::future::ready(None);
            }

            let split_ids = Self::sequential_split_id(
                batch.num_rows() as u64,
                &split_sizes,
                &split_index,
                &counter_in_split,
                &exhausted,
            );

            let mut arrays = batch.columns().to_vec();
            // This can happen if we exhaust all splits in the middle of a batch
            if split_ids.len() < batch.num_rows() {
                arrays = arrays
                    .iter()
                    .map(|arr| arr.slice(0, split_ids.len()))
                    .collect();
            }
            arrays.push(Arc::new(split_ids));

            std::future::ready(Some(Ok(
                RecordBatch::try_new(new_schema.clone(), arrays).unwrap()
            )))
        });

        Ok(Box::pin(SimpleRecordBatchStream::new(
            stream,
            new_schema_clone,
        )))
    }

    fn hash_split_id(batch: &RecordBatch, thresholds: &[u64], total_weight: u64) -> UInt64Array {
        let arrays = batch
            .columns()
            .iter()
            // Don't hash the last column which should always be the row id
            .take(batch.columns().len() - 1)
            .cloned()
            .collect::<Vec<_>>();
        let mut hashes = vec![0; batch.num_rows()];
        let random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        create_hashes(&arrays, &random_state, &mut hashes).unwrap();
        // As an example, let's assume the weights are 1, 2.  Our total weight is 3.
        //
        // Our thresholds are [1, 3]
        // Our modulo output will be 0, 1, or 2.
        //
        // thresholds.binary_search(0) => Err(0) => 0
        // thresholds.binary_search(1) => Ok(0)  => 1
        // thresholds.binary_search(2) => Err(1) => 1
        let split_ids = hashes
            .iter()
            .map(|h| {
                let h = h % total_weight;
                let split_id = match thresholds.binary_search(&h) {
                    Ok(i) => (i + 1) as u64,
                    Err(i) => i as u64,
                };
                if split_id == thresholds.len() as u64 {
                    // If we're at the last threshold then we discard the row (indicated by setting
                    // the split_id to null)
                    None
                } else {
                    Some(split_id)
                }
            })
            .collect::<Vec<_>>();
        UInt64Array::from(split_ids)
    }

    async fn apply_hash(
        &self,
        source: SendableRecordBatchStream,
        weights: &[u64],
        discard_weight: u64,
    ) -> Result<SendableRecordBatchStream> {
        let row_id_index = source.schema().fields.len() - 1;
        let new_schema = Arc::new(Schema::new(vec![
            source.schema().field(row_id_index).clone(),
            Field::new(SPLIT_ID_COLUMN, DataType::UInt64, false),
        ]));

        let total_weight = weights.iter().sum::<u64>() + discard_weight;
        // Thresholds are the cumulative sum of the weights
        let mut offset = 0;
        let thresholds = weights
            .iter()
            .map(|w| {
                let value = offset + w;
                offset = value;
                value
            })
            .collect::<Vec<_>>();

        let new_schema_clone = new_schema.clone();
        let stream = source.map_ok(move |batch| {
            let split_ids = Self::hash_split_id(&batch, &thresholds, total_weight);

            if split_ids.null_count() > 0 {
                let is_valid = split_ids.nulls().unwrap().inner();
                let is_valid_mask = BooleanArray::new(is_valid.clone(), None);
                let split_ids = arrow::compute::filter(&split_ids, &is_valid_mask).unwrap();
                let row_ids = batch.column(row_id_index);
                let row_ids = arrow::compute::filter(row_ids.as_ref(), &is_valid_mask).unwrap();
                RecordBatch::try_new(new_schema.clone(), vec![row_ids, split_ids]).unwrap()
            } else {
                RecordBatch::try_new(
                    new_schema.clone(),
                    vec![batch.column(row_id_index).clone(), Arc::new(split_ids)],
                )
                .unwrap()
            }
        });

        Ok(Box::pin(SimpleRecordBatchStream::new(
            stream,
            new_schema_clone,
        )))
    }

    pub async fn apply(
        &self,
        source: SendableRecordBatchStream,
        num_rows: u64,
    ) -> Result<SendableRecordBatchStream> {
        self.strategy.validate(num_rows)?;

        match &self.strategy {
            // For consistency, even if no-split, we still give a split id column of all 0s
            SplitStrategy::NoSplit => {
                self.apply_sequential(source, num_rows, &SplitSizes::Counts(vec![num_rows]))
                    .await
            }
            SplitStrategy::Random { seed, sizes } => {
                let shuffler = Shuffler::new(ShufflerConfig {
                    seed: *seed,
                    // In this case we are only shuffling row ids so we can use a large max_rows_per_file
                    max_rows_per_file: 10 * 1024 * 1024,
                    temp_dir: self.temp_dir.clone(),
                    clump_size: None,
                });

                let shuffled = shuffler.shuffle(source, num_rows).await?;

                self.apply_sequential(shuffled, num_rows, sizes).await
            }
            SplitStrategy::Sequential { sizes } => {
                self.apply_sequential(source, num_rows, sizes).await
            }
            // Nothing to do, split is calculated in projection
            SplitStrategy::Calculated { .. } => Ok(source),
            SplitStrategy::Hash {
                split_weights,
                discard_weight,
                ..
            } => {
                self.apply_hash(source, split_weights, *discard_weight)
                    .await
            }
        }
    }

    pub fn project(&self, query: Query) -> Query {
        match &self.strategy {
            SplitStrategy::Calculated { calculation } => query.select(Select::Dynamic(vec![(
                SPLIT_ID_COLUMN.to_string(),
                calculation.clone(),
            )])),
            SplitStrategy::Hash { columns, .. } => query.select(Select::Columns(columns.clone())),
            _ => query,
        }
    }

    pub fn orders_by_split_id(&self) -> bool {
        match &self.strategy {
            SplitStrategy::Hash { .. } | SplitStrategy::Calculated { .. } => true,
            SplitStrategy::NoSplit
            | SplitStrategy::Sequential { .. }
            // It may be strange but for random we shuffle and then assign splits so the result is
            // sorted by split id
            | SplitStrategy::Random { .. } => false,
        }
    }
}

/// Split configuration - either percentages or absolute counts
///
/// If the percentages do not sum to 1.0 (or the counts do not sum to the total number of rows)
/// the remaining rows will not be included in the permutation.
///
/// The default implementation assigns all rows to a single split.
#[derive(Debug, Clone)]
pub enum SplitSizes {
    /// Percentage splits (must sum to <= 1.0)
    ///
    /// The number of rows in each split is the nearest integer to the percentage multiplied by
    /// the total number of rows.
    Percentages(Vec<f64>),
    /// Absolute row counts per split
    ///
    /// If the dataset doesn't contain enough matching rows to fill all splits then an error
    /// will be raised.
    Counts(Vec<u64>),
    /// Divides data into a fixed number of splits
    ///
    /// Will divide the data evenly.
    ///
    /// If the number of rows is not divisible by the number of splits then the rows per split
    /// is rounded down.
    Fixed(u64),
}

impl Default for SplitSizes {
    fn default() -> Self {
        Self::Percentages(vec![1.0])
    }
}

impl SplitSizes {
    pub fn validate(&self, num_rows: u64) -> Result<()> {
        match self {
            Self::Percentages(percentages) => {
                for percentage in percentages {
                    if *percentage < 0.0 || *percentage > 1.0 {
                        return Err(Error::InvalidInput {
                            message: "Split percentages must be between 0.0 and 1.0".to_string(),
                        });
                    }
                    if percentage * (num_rows as f64) < 1.0 {
                        return Err(Error::InvalidInput {
                            message: format!(
                                "One of the splits has {}% of {} rows which rounds to 0 rows",
                                percentage * 100.0,
                                num_rows
                            ),
                        });
                    }
                }
                if percentages.iter().sum::<f64>() > 1.0 {
                    return Err(Error::InvalidInput {
                        message: "Split percentages must sum to 1.0 or less".to_string(),
                    });
                }
            }
            Self::Counts(counts) => {
                if counts.iter().sum::<u64>() > num_rows {
                    return Err(Error::InvalidInput {
                        message: format!(
                            "Split counts specified {} rows but only {} are available",
                            counts.iter().sum::<u64>(),
                            num_rows
                        ),
                    });
                }
                if counts.iter().any(|c| *c == 0) {
                    return Err(Error::InvalidInput {
                        message: "Split counts must be greater than 0".to_string(),
                    });
                }
            }
            Self::Fixed(num_splits) => {
                if *num_splits > num_rows {
                    return Err(Error::InvalidInput {
                        message: format!(
                            "Split fixed config specified {} splits but only {} rows are available.  Must have at least 1 row per split.",
                            *num_splits, num_rows
                        ),
                    });
                }
                if (num_rows / num_splits) == 0 {
                    return Err(Error::InvalidInput {
                        message: format!(
                            "Split fixed config specified {} splits but only {} rows are available.  Must have at least 1 row per split.",
                            *num_splits, num_rows
                        ),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn to_counts(&self, num_rows: u64) -> Vec<u64> {
        let sizes = match self {
            Self::Percentages(percentages) => {
                let mut percentage_sum = 0.0_f64;
                let mut counts = percentages
                    .iter()
                    .map(|p| {
                        let count = (p * (num_rows as f64)).round() as u64;
                        percentage_sum += p;
                        count
                    })
                    .collect::<Vec<_>>();
                let sum = counts.iter().sum::<u64>();

                let is_basically_one =
                    (num_rows as f64 - percentage_sum * num_rows as f64).abs() < 0.5;

                // If the sum of percentages is close to 1.0 then rounding errors can add up
                // to more or less than num_rows
                //
                // Drop items from buckets until we have the correct number of rows
                let mut excess = sum as i64 - num_rows as i64;
                let mut drop_idx = 0;
                while excess > 0 {
                    if counts[drop_idx] > 0 {
                        counts[drop_idx] -= 1;
                        excess -= 1;
                    }
                    drop_idx += 1;
                    if drop_idx == counts.len() {
                        drop_idx = 0;
                    }
                }

                // On the other hand, if the percentages sum to ~1.0 then the we also shouldn't _lose_
                // rows due to rounding errors
                let mut add_idx = 0;
                while is_basically_one && excess < 0 {
                    counts[add_idx] += 1;
                    add_idx += 1;
                    if add_idx == counts.len() {
                        add_idx = 0;
                    }
                }

                counts
            }
            Self::Counts(counts) => counts.clone(),
            Self::Fixed(num_splits) => {
                let rows_per_split = num_rows / *num_splits;
                vec![rows_per_split; *num_splits as usize]
            }
        };

        assert!(sizes.iter().sum::<u64>() <= num_rows);

        sizes
    }
}

#[cfg(test)]
mod tests {
    use crate::arrow::LanceDbDatagenExt;

    use super::*;
    use arrow::{
        array::AsArray,
        compute::concat_batches,
        datatypes::{Int32Type, UInt64Type},
    };
    use arrow_array::Int32Array;
    use futures::TryStreamExt;
    use lance_datagen::{BatchCount, ByteCount, RowCount, Seed};
    use std::sync::Arc;

    const ID_COLUMN: &str = "id";

    #[test]
    fn test_split_sizes_percentages_validation() {
        // Valid percentages
        let sizes = SplitSizes::Percentages(vec![0.7, 0.3]);
        assert!(sizes.validate(100).is_ok());

        // Sum > 1.0
        let sizes = SplitSizes::Percentages(vec![0.7, 0.4]);
        assert!(sizes.validate(100).is_err());

        // Negative percentage
        let sizes = SplitSizes::Percentages(vec![-0.1, 0.5]);
        assert!(sizes.validate(100).is_err());

        // Percentage > 1.0
        let sizes = SplitSizes::Percentages(vec![1.5]);
        assert!(sizes.validate(100).is_err());

        // Percentage rounds to 0 rows
        let sizes = SplitSizes::Percentages(vec![0.001]);
        assert!(sizes.validate(100).is_err());
    }

    #[test]
    fn test_split_sizes_counts_validation() {
        // Valid counts
        let sizes = SplitSizes::Counts(vec![30, 70]);
        assert!(sizes.validate(100).is_ok());

        // Sum > num_rows
        let sizes = SplitSizes::Counts(vec![60, 50]);
        assert!(sizes.validate(100).is_err());

        // Counts are 0
        let sizes = SplitSizes::Counts(vec![0, 100]);
        assert!(sizes.validate(100).is_err());
    }

    #[test]
    fn test_split_sizes_fixed_validation() {
        // Valid fixed splits
        let sizes = SplitSizes::Fixed(5);
        assert!(sizes.validate(100).is_ok());

        // More splits than rows
        let sizes = SplitSizes::Fixed(150);
        assert!(sizes.validate(100).is_err());
    }

    #[test]
    fn test_split_sizes_to_sizes_percentages() {
        let sizes = SplitSizes::Percentages(vec![0.3, 0.7]);
        let result = sizes.to_counts(100);
        assert_eq!(result, vec![30, 70]);

        // Test rounding
        let sizes = SplitSizes::Percentages(vec![0.3, 0.41]);
        let result = sizes.to_counts(70);
        assert_eq!(result, vec![21, 29]);
    }

    #[test]
    fn test_split_sizes_to_sizes_fixed() {
        let sizes = SplitSizes::Fixed(4);
        let result = sizes.to_counts(100);
        assert_eq!(result, vec![25, 25, 25, 25]);

        // Test with remainder
        let sizes = SplitSizes::Fixed(3);
        let result = sizes.to_counts(10);
        assert_eq!(result, vec![3, 3, 3]);
    }

    fn test_data() -> SendableRecordBatchStream {
        lance_datagen::gen_batch()
            .with_seed(Seed::from(42))
            .col(ID_COLUMN, lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(10), BatchCount::from(5))
    }

    async fn verify_splitter(
        splitter: Splitter,
        data: SendableRecordBatchStream,
        num_rows: u64,
        expected_split_sizes: &[u64],
        row_ids_in_order: bool,
    ) {
        let split_batches = splitter
            .apply(data, num_rows)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let schema = split_batches[0].schema();
        let split_batch = concat_batches(&schema, &split_batches).unwrap();

        let total_split_sizes = expected_split_sizes.iter().sum::<u64>();

        assert_eq!(split_batch.num_rows(), total_split_sizes as usize);
        let mut expected = Vec::with_capacity(total_split_sizes as usize);
        for (i, size) in expected_split_sizes.iter().enumerate() {
            expected.extend(iter::repeat_n(i as u64, *size as usize));
        }
        let expected = Arc::new(UInt64Array::from(expected)) as Arc<dyn Array>;

        assert_eq!(&expected, split_batch.column(1));

        let expected_row_ids =
            Arc::new(Int32Array::from_iter_values(0..total_split_sizes as i32)) as Arc<dyn Array>;
        if row_ids_in_order {
            assert_eq!(&expected_row_ids, split_batch.column(0));
        } else {
            assert_ne!(&expected_row_ids, split_batch.column(0));
        }
    }

    #[tokio::test]
    async fn test_fixed_sequential_split() {
        let splitter = Splitter::new(
            // Sequential splitting doesn't need a temp dir
            TemporaryDirectory::None,
            SplitStrategy::Sequential {
                sizes: SplitSizes::Fixed(3),
            },
        );

        verify_splitter(splitter, test_data(), 50, &[16, 16, 16], true).await;
    }

    #[tokio::test]
    async fn test_fixed_random_split() {
        let splitter = Splitter::new(
            TemporaryDirectory::None,
            SplitStrategy::Random {
                seed: Some(42),
                sizes: SplitSizes::Fixed(3),
            },
        );

        verify_splitter(splitter, test_data(), 50, &[16, 16, 16], false).await;
    }

    #[tokio::test]
    async fn test_counts_sequential_split() {
        let splitter = Splitter::new(
            // Sequential splitting doesn't need a temp dir
            TemporaryDirectory::None,
            SplitStrategy::Sequential {
                sizes: SplitSizes::Counts(vec![5, 15, 10]),
            },
        );

        verify_splitter(splitter, test_data(), 50, &[5, 15, 10], true).await;
    }

    #[tokio::test]
    async fn test_counts_random_split() {
        let splitter = Splitter::new(
            TemporaryDirectory::None,
            SplitStrategy::Random {
                seed: Some(42),
                sizes: SplitSizes::Counts(vec![5, 15, 10]),
            },
        );

        verify_splitter(splitter, test_data(), 50, &[5, 15, 10], false).await;
    }

    #[tokio::test]
    async fn test_percentages_sequential_split() {
        let splitter = Splitter::new(
            // Sequential splitting doesn't need a temp dir
            TemporaryDirectory::None,
            SplitStrategy::Sequential {
                sizes: SplitSizes::Percentages(vec![0.217, 0.168, 0.17]),
            },
        );

        verify_splitter(splitter, test_data(), 50, &[11, 8, 9], true).await;
    }

    #[tokio::test]
    async fn test_percentages_random_split() {
        let splitter = Splitter::new(
            TemporaryDirectory::None,
            SplitStrategy::Random {
                seed: Some(42),
                sizes: SplitSizes::Percentages(vec![0.217, 0.168, 0.17]),
            },
        );

        verify_splitter(splitter, test_data(), 50, &[11, 8, 9], false).await;
    }

    #[tokio::test]
    async fn test_hash_split() {
        let data = lance_datagen::gen_batch()
            .with_seed(Seed::from(42))
            .col(
                "hash1",
                lance_datagen::array::rand_utf8(ByteCount::from(10), false),
            )
            .col("hash2", lance_datagen::array::step::<Int32Type>())
            .col(ID_COLUMN, lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(10), BatchCount::from(5));

        let splitter = Splitter::new(
            TemporaryDirectory::None,
            SplitStrategy::Hash {
                columns: vec!["hash1".to_string(), "hash2".to_string()],
                split_weights: vec![1, 2],
                discard_weight: 1,
            },
        );

        let split_batches = splitter
            .apply(data, 10)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let schema = split_batches[0].schema();
        let split_batch = concat_batches(&schema, &split_batches).unwrap();

        // These assertions are all based on fixed seed in data generation but they match
        // up roughly to what we expect (25% discarded, 25% in split 0, 50% in split 1)

        // 14 rows (28%) are discarded because discard_weight is 1
        assert_eq!(split_batch.num_rows(), 36);
        assert_eq!(split_batch.num_columns(), 2);

        let split_ids = split_batch.column(1).as_primitive::<UInt64Type>().values();
        let num_in_split_0 = split_ids.iter().filter(|v| **v == 0).count();
        let num_in_split_1 = split_ids.iter().filter(|v| **v == 1).count();

        assert_eq!(num_in_split_0, 11); // 22%
        assert_eq!(num_in_split_1, 25); // 50%
    }
}
