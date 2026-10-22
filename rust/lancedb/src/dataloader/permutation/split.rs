// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

use arrow_array::{Array, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::hash_utils::{RandomState, create_hashes};
use futures::{StreamExt, TryStreamExt};
use lance_arrow::SchemaExt;
use rand::Rng;

use lance_core::ROW_ID;

use crate::{
    Error, Result,
    arrow::{SendableRecordBatchStream, SimpleRecordBatchStream},
    dataloader::{
        permutation::shuffle::{Shuffler, ShufflerConfig},
        permutation::util::{TemporaryDirectory, non_crypto_rng},
    },
    query::{Query, QueryBase, Select},
};

pub const SPLIT_ID_COLUMN: &str = "split_id";

/// Strategy for assigning rows to splits
#[derive(Debug, Clone, Default)]
pub enum SplitStrategy {
    /// All rows will have split id 0
    #[default]
    NoSplit,
    /// Rows will be randomly assigned to splits
    ///
    /// A seed can be provided to make the assignment deterministic.
    ///
    /// A clump_size can be provided to shuffle contiguous groups of rows together,
    /// preserving I/O locality while still randomising the split assignment.
    Random {
        seed: Option<u64>,
        sizes: SplitSizes,
        clump_size: Option<u64>,
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
    /// Rows are sampled uniformly **with replacement** from the base table (combine with
    /// [`PermutationBuilder::with_filter`] to restrict the eligible population), independently
    /// per split. Named after "the bootstrap" -- the statistical technique of estimating a
    /// dataset's properties by resampling it with replacement -- the same sense `bootstrap=True`
    /// has in scikit-learn's bagging classifiers.
    ///
    /// Every other strategy partitions the table: each row goes to exactly one split, exactly
    /// once. `Bootstrap` does not partition -- it resamples. The number of output rows is
    /// `num_samples`, not the number of input rows: the same row can appear zero, one, or many
    /// times, and `num_samples` may be larger or smaller than the (filtered) input row count.
    /// Typical uses:
    ///
    /// - **Bagging / ensemble training** (e.g. random forests): each split is an independent
    ///   bootstrap resample of the training set.
    /// - **Oversampling a rare class**: filter to the minority class, then sample it up to
    ///   whatever count balances it against the majority, instead of exhausting its small set
    ///   once per epoch.
    /// - **Fixed-size training steps independent of dataset size**: `num_samples` is set
    ///   directly rather than being implicitly `len(dataset)`.
    /// - **Weighted sampling without a weight column**: filter to "eligible" rows (e.g. a
    ///   windowed-sampling workload filtering to valid window-start positions), then bootstrap
    ///   the filtered set -- an item with more eligible rows is naturally drawn proportionally
    ///   more often, with no explicit per-row weight needed.
    ///
    /// `sizes` divides `num_samples` across splits (reusing [`SplitSizes`], so `Fixed(n)` means
    /// "n splits, each getting `num_samples / n` samples", the same convention `Random` and
    /// `Sequential` use). Each split draws its own sample using a seed derived independently
    /// from `(seed, split_index)`, so a given split's content does not change if `num_samples`
    /// is redistributed across a different number of splits -- the same elastic-determinism
    /// property documented for the data loader's other strategies.
    ///
    /// Example: a 5-row base table (`row_id` 0..4), `Bootstrap { num_samples: 8, sizes:
    /// Fixed(2), seed: Some(42) }` produced (an actual run, not a hypothetical one):
    ///
    /// ```text
    /// row_id  split_id
    ///      0         0
    ///      2         0
    ///      4         0
    ///      4         0      <- row 4 repeated within split 0
    ///      4         1      <- row 4 also drawn in split 1 (independent draw)
    ///      4         1
    ///      1         1
    ///      2         1
    /// ```
    ///
    /// 8 output rows from a 5-row table, row 4 drawn 4 times total, row 3 never drawn --
    /// exactly the with-replacement behavior the name promises, not an even round-robin.
    Bootstrap {
        num_samples: u64,
        sizes: SplitSizes,
        seed: Option<u64>,
    },
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
                if split_weights.contains(&0) {
                    return Err(Error::InvalidInput {
                        message: "Split weights must be greater than 0".to_string(),
                    });
                }
                Ok(())
            }
            Self::Sequential { sizes } => sizes.validate(num_rows),
            Self::Calculated { .. } => Ok(()),
            Self::Bootstrap {
                num_samples, sizes, ..
            } => {
                if num_rows == 0 {
                    return Err(Error::InvalidInput {
                        message: "Bootstrap strategy requires at least one eligible row to \
                                  sample from (check the filter, if one is set)"
                            .to_string(),
                    });
                }
                if *num_samples == 0 {
                    return Err(Error::InvalidInput {
                        message: "Bootstrap strategy requires num_samples > 0".to_string(),
                    });
                }
                // Validated against num_samples, not num_rows: sampling is with replacement,
                // so num_samples may exceed the number of eligible rows.
                sizes.validate(*num_samples)
            }
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

            split_ids.extend(std::iter::repeat_n(split_id as u64, rows_to_add as usize));
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

            std::future::ready(Some(
                RecordBatch::try_new(new_schema.clone(), arrays).map_err(Error::from),
            ))
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
        let random_state = RandomState::with_seed(0);
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
        let stream = source.and_then(move |batch| {
            let split_ids = Self::hash_split_id(&batch, &thresholds, total_weight);

            let batch = if let Some(nulls) = split_ids.nulls().filter(|n| n.null_count() > 0) {
                let is_valid_mask = BooleanArray::new(nulls.inner().clone(), None);
                arrow::compute::filter(&split_ids, &is_valid_mask).and_then(|split_ids| {
                    let row_ids = batch.column(row_id_index);
                    let row_ids = arrow::compute::filter(row_ids.as_ref(), &is_valid_mask)?;
                    RecordBatch::try_new(new_schema.clone(), vec![row_ids, split_ids])
                })
            } else {
                RecordBatch::try_new(
                    new_schema.clone(),
                    vec![batch.column(row_id_index).clone(), Arc::new(split_ids)],
                )
            };
            std::future::ready(batch.map_err(Error::from))
        });

        Ok(Box::pin(SimpleRecordBatchStream::new(
            stream,
            new_schema_clone,
        )))
    }

    /// Materializes the full (filtered) row id stream -- random access is required since
    /// with-replacement sampling can draw any position, including ones already drawn -- then
    /// independently samples each split's share with its own derived RNG seed.
    async fn apply_bootstrap(
        &self,
        source: SendableRecordBatchStream,
        num_samples: u64,
        sizes: &SplitSizes,
        seed: Option<u64>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = source.schema();
        let row_id_field = schema.field(0).clone();
        let batches: Vec<RecordBatch> = source.try_collect().await?;
        let row_id_arrays = batches
            .iter()
            .map(|b| b.column(0).as_ref())
            .collect::<Vec<_>>();
        let row_ids = arrow::compute::concat(&row_id_arrays)?;
        // Sample against however many rows this scan actually returned, not the
        // separately computed `num_rows` count checked by `validate` -- that count and
        // this scan are two different queries, not pinned to one snapshot, so a
        // concurrent append or delete between them could otherwise push
        // `random_range` out of `row_ids`'s real bounds.
        let num_eligible_rows = row_ids.len() as u64;
        if num_eligible_rows == 0 {
            return Err(Error::InvalidInput {
                message: "Bootstrap strategy found no eligible rows to sample from in \
                          this scan (the table or its filter may have changed since \
                          validation)"
                    .to_string(),
            });
        }

        let split_sample_counts = sizes.to_counts(num_samples);

        // `try_reserve_exact` (fallible) rather than `with_capacity` (infallible --
        // aborts the whole process via `handle_alloc_error` on failure, which no
        // caller can catch) so an unreasonably large `num_samples` becomes a normal,
        // catchable error instead of a crash. This bounds the failure mode, not peak
        // memory itself: the output is still one fully materialized RecordBatch, not
        // streamed in chunks.
        let too_large = || Error::InvalidInput {
            message: format!(
                "Bootstrap strategy's num_samples ({num_samples}) is too large to \
                 allocate in memory"
            ),
        };
        let mut position_indices = Vec::<u64>::new();
        position_indices
            .try_reserve_exact(num_samples as usize)
            .map_err(|_| too_large())?;
        let mut split_ids = Vec::<u64>::new();
        split_ids
            .try_reserve_exact(num_samples as usize)
            .map_err(|_| too_large())?;
        for (split_idx, &count) in split_sample_counts.iter().enumerate() {
            // Derive an independent seed per split (rather than one RNG shared across all
            // splits) so a split's sample doesn't depend on how many other splits there are.
            let split_seed = seed.map(|s| {
                s.wrapping_add(
                    (split_idx as u64)
                        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                        .wrapping_add(1),
                )
            });
            let mut rng = non_crypto_rng(&split_seed);
            for _ in 0..count {
                position_indices.push(rng.random_range(0..num_eligible_rows));
                split_ids.push(split_idx as u64);
            }
        }

        let indices = UInt64Array::from(position_indices);
        let sampled_row_ids = arrow::compute::take(row_ids.as_ref(), &indices, None)?;
        let split_id_array: Arc<dyn Array> = Arc::new(UInt64Array::from(split_ids));

        let new_schema = Arc::new(Schema::new(vec![
            row_id_field,
            Field::new(SPLIT_ID_COLUMN, DataType::UInt64, false),
        ]));
        let result_batch =
            RecordBatch::try_new(new_schema.clone(), vec![sampled_row_ids, split_id_array])?;

        Ok(Box::pin(SimpleRecordBatchStream::new(
            futures::stream::once(std::future::ready(Ok(result_batch))),
            new_schema,
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
            SplitStrategy::Random {
                seed,
                sizes,
                clump_size,
            } => {
                let shuffler = Shuffler::new(ShufflerConfig {
                    seed: *seed,
                    // In this case we are only shuffling row ids so we can use a large max_rows_per_file
                    max_rows_per_file: 10 * 1024 * 1024,
                    temp_dir: self.temp_dir.clone(),
                    clump_size: *clump_size,
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
            SplitStrategy::Bootstrap {
                num_samples,
                sizes,
                seed,
            } => {
                self.apply_bootstrap(source, *num_samples, sizes, *seed)
                    .await
            }
        }
    }

    pub fn project(&self, query: Query) -> Query {
        match &self.strategy {
            SplitStrategy::Calculated { calculation } => query.select(Select::Dynamic(vec![
                (SPLIT_ID_COLUMN.to_string(), calculation.clone()),
                (ROW_ID.to_string(), ROW_ID.to_string()),
            ])),
            SplitStrategy::Hash { columns, .. } => {
                let mut cols = columns.clone();
                cols.push(ROW_ID.to_string());
                query.select(Select::Columns(cols))
            }
            _ => query,
        }
    }

    pub fn orders_by_split_id(&self) -> bool {
        match &self.strategy {
            // Bootstrap emits samples grouped split-by-split already (see apply_bootstrap),
            // rather than relying on the shared pipeline's sort-by-split-id step: within-split
            // draw order is meaningful to callers (e.g. a warmup/measured split within one
            // split's stream), and sort stability for ties isn't documented behavior to lean on.
            SplitStrategy::Hash { .. }
            | SplitStrategy::Calculated { .. }
            | SplitStrategy::Bootstrap { .. } => true,
            SplitStrategy::NoSplit
            | SplitStrategy::Sequential { .. }
            // It may be strange but for random we shuffle and then assign splits so the result is
            // sorted by split id
            | SplitStrategy::Random { .. } => false,
        }
    }

    /// The row count `apply` produces from `num_rows` input rows. Equal to `num_rows`
    /// for every strategy except `Bootstrap`, whose output size is `num_samples`
    /// regardless of how many (filtered) input rows it draws from. Callers that need
    /// to validate or pre-size `apply`'s output (e.g. the shuffler) must use this
    /// instead of assuming the input and output row counts match.
    ///
    /// For `Bootstrap`, this is `sizes.to_counts(num_samples)`'s sum, not
    /// `num_samples` itself: `to_counts` can emit less than its input (e.g.
    /// `Fixed(n)` when `n` does not divide `num_samples` evenly, or explicit
    /// `counts`/`ratios` that do not use every sample) and `apply_bootstrap` only
    /// ever emits exactly what `to_counts` returns.
    pub fn output_row_count(&self, num_rows: u64) -> u64 {
        match &self.strategy {
            SplitStrategy::Bootstrap {
                num_samples, sizes, ..
            } => sizes.to_counts(*num_samples).iter().sum(),
            _ => num_rows,
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
                if counts.contains(&0) {
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
                    excess += 1;
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
            expected.extend(std::iter::repeat_n(i as u64, *size as usize));
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
                clump_size: None,
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
                clump_size: None,
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
                clump_size: None,
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

        // 8 rows (16%) are discarded because discard_weight is 1
        assert_eq!(split_batch.num_rows(), 42);
        assert_eq!(split_batch.num_columns(), 2);

        let split_ids = split_batch.column(1).as_primitive::<UInt64Type>().values();
        let num_in_split_0 = split_ids.iter().filter(|v| **v == 0).count();
        let num_in_split_1 = split_ids.iter().filter(|v| **v == 1).count();

        assert_eq!(num_in_split_0, 12); // 24%
        assert_eq!(num_in_split_1, 30); // 60%
    }

    #[tokio::test]
    async fn test_bootstrap_split_schema_and_counts() {
        let splitter = Splitter::new(
            TemporaryDirectory::None,
            SplitStrategy::Bootstrap {
                num_samples: 1000,
                sizes: SplitSizes::Fixed(4),
                seed: Some(42),
            },
        );

        let split_batches = splitter
            .apply(test_data(), 50)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let schema = split_batches[0].schema();
        let split_batch = concat_batches(&schema, &split_batches).unwrap();

        // Schema stays exactly [row_id, split_id], same as every other strategy.
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["id", SPLIT_ID_COLUMN]);

        // Output row count is num_samples, not num_rows (50) -- sampling is with replacement.
        assert_eq!(split_batch.num_rows(), 1000);

        let split_ids = split_batch.column(1).as_primitive::<UInt64Type>().values();
        for split_id in [0u64, 1, 2, 3] {
            assert_eq!(split_ids.iter().filter(|v| **v == split_id).count(), 250);
        }

        // With 1000 draws from only 50 distinct rows, some row id must repeat.
        let row_ids = split_batch.column(0).as_primitive::<Int32Type>().values();
        let mut seen = std::collections::HashSet::new();
        assert!(!row_ids.iter().all(|v| seen.insert(*v)));
    }

    #[tokio::test]
    async fn test_bootstrap_split_seed_is_reproducible() {
        let make_splitter = || {
            Splitter::new(
                TemporaryDirectory::None,
                SplitStrategy::Bootstrap {
                    num_samples: 200,
                    sizes: SplitSizes::Fixed(2),
                    seed: Some(7),
                },
            )
        };

        let first = make_splitter()
            .apply(test_data(), 50)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let second = make_splitter()
            .apply(test_data(), 50)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let schema = first[0].schema();
        let first = concat_batches(&schema, &first).unwrap();
        let second = concat_batches(&schema, &second).unwrap();
        assert_eq!(first.column(0), second.column(0));
        assert_eq!(first.column(1), second.column(1));
    }

    #[tokio::test]
    async fn test_bootstrap_split_respects_filter() {
        // Bootstrap is driven through a filter the same way every other strategy is -- the
        // base table rows reaching Splitter::apply are already filtered by the time they get
        // here, so this exercises the same num_rows=0 guard a fully-filtered-out population
        // would hit.
        let splitter = Splitter::new(
            TemporaryDirectory::None,
            SplitStrategy::Bootstrap {
                num_samples: 10,
                sizes: SplitSizes::Fixed(1),
                seed: Some(1),
            },
        );
        let Err(err) = splitter.apply(test_data(), 0).await else {
            panic!("expected Bootstrap over zero eligible rows to fail");
        };
        assert!(err.to_string().contains("at least one eligible row"));
    }

    fn empty_data() -> SendableRecordBatchStream {
        lance_datagen::gen_batch()
            .with_seed(Seed::from(42))
            .col(ID_COLUMN, lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(0), BatchCount::from(1))
    }

    #[test]
    fn test_output_row_count() {
        // Bootstrap's output row count is independent of the input size -- every
        // other strategy's output matches its input. The shuffler relies on this
        // (see builder.rs) instead of assuming input and output row counts match.
        let bootstrap = Splitter::new(
            TemporaryDirectory::None,
            SplitStrategy::Bootstrap {
                num_samples: 1000,
                sizes: SplitSizes::Fixed(4),
                seed: Some(42),
            },
        );
        assert_eq!(bootstrap.output_row_count(50), 1000);

        // Not num_samples itself: `Fixed(3)` doesn't divide 1000 evenly, so
        // `to_counts` emits 333 * 3 = 999, one short of num_samples --
        // `apply_bootstrap` only ever emits exactly what `to_counts` returns.
        let bootstrap_uneven = Splitter::new(
            TemporaryDirectory::None,
            SplitStrategy::Bootstrap {
                num_samples: 1000,
                sizes: SplitSizes::Fixed(3),
                seed: Some(42),
            },
        );
        assert_eq!(bootstrap_uneven.output_row_count(50), 999);

        let sequential = Splitter::new(
            TemporaryDirectory::None,
            SplitStrategy::Sequential {
                sizes: SplitSizes::Fixed(2),
            },
        );
        assert_eq!(sequential.output_row_count(50), 50);
    }

    #[tokio::test]
    async fn test_bootstrap_rejects_empty_scan_cleanly() {
        // The eligibility count `validate` checks and this scan are two separate
        // queries against the table, not pinned to one snapshot -- a concurrent
        // delete could shrink the real scan to zero rows after `validate` already
        // passed on a nonzero count. Calling `apply_bootstrap` directly (bypassing
        // `apply`'s `validate`) reproduces exactly that: a scan that returns zero
        // rows regardless of what count was validated upstream. It must return a
        // clean `Err`, not panic on an out-of-range `take` or an empty-range
        // `random_range`.
        let splitter = Splitter::new(
            TemporaryDirectory::None,
            SplitStrategy::Bootstrap {
                num_samples: 10,
                sizes: SplitSizes::Fixed(1),
                seed: Some(1),
            },
        );
        let Err(err) = splitter
            .apply_bootstrap(empty_data(), 10, &SplitSizes::Fixed(1), Some(1))
            .await
        else {
            panic!("expected Bootstrap over an empty scan to fail cleanly, not panic");
        };
        assert!(err.to_string().contains("no eligible rows"));
    }
}
