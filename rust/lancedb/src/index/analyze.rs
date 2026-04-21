// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Sweep ANN parameters against exhaustive ground truth and report recall +
//! latency percentiles per configuration. Supports `IVF_FLAT` and `IVF_PQ`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float16Type, Float32Type, Float64Type};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float64Array, RecordBatch, StringArray, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use lance::index::DatasetIndexExt;
use rand::rngs::SmallRng;
use rand::seq::IteratorRandom;
use rand::{Rng, SeedableRng};

use crate::index::IndexType;
use crate::query::{ExecutableQuery, QueryBase, Select};
use crate::table::Table;
use crate::{DistanceType, Error, Result};

const ROW_ID_COL: &str = "_rowid";

/// Options controlling an [`analyze_index`] run.
#[derive(Debug, Clone)]
pub struct AnalyzeIndexOptions {
    /// Number of query vectors to sample, clamped to the table's row count.
    pub sample_size: usize,
    /// `K` values to sweep. Defaults to `[10, 20, 50, 100]`.
    pub k: Option<Vec<usize>>,
    /// RNG seed for reproducibility.
    pub seed: Option<u64>,
    /// `nprobes` values to sweep. Defaults to a geometric sweep capped at `num_partitions`.
    pub nprobes: Option<Vec<u32>>,
    /// `refine_factor` values to sweep. Ignored for IVF_FLAT.
    pub refine_factor: Option<Vec<u32>>,
}

impl Default for AnalyzeIndexOptions {
    fn default() -> Self {
        Self {
            sample_size: 1000,
            k: None,
            seed: None,
            nprobes: None,
            refine_factor: None,
        }
    }
}

/// Arrow schema of the [`RecordBatch`] returned by [`analyze_index`].
pub fn analyze_index_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("num_partitions", DataType::UInt32, false),
        Field::new("index_type", DataType::Utf8, false),
        Field::new("k", DataType::UInt32, false),
        Field::new("nprobes", DataType::UInt32, false),
        Field::new("refine_factor", DataType::UInt32, true),
        Field::new("recall", DataType::Float64, false),
        Field::new("latency_min_ms", DataType::Float64, false),
        Field::new("latency_p50_ms", DataType::Float64, false),
        Field::new("latency_p90_ms", DataType::Float64, false),
        Field::new("latency_p99_ms", DataType::Float64, false),
        Field::new("latency_max_ms", DataType::Float64, false),
    ]))
}

/// Analyze a vector index by sweeping ANN parameters against exhaustive ground
/// truth on a random sample of queries drawn from the table.
///
/// Queries are sampled from the table itself, so each query's true nearest
/// neighbor is itself. We fetch `K + 1` on both paths and drop the query's own
/// row id before computing recall@K.
pub async fn analyze_index(
    table: &Table,
    index_name: &str,
    options: AnalyzeIndexOptions,
) -> Result<RecordBatch> {
    let mut k_sweep: Vec<usize> = options.k.clone().unwrap_or_else(|| vec![10, 20, 50, 100]);
    k_sweep.retain(|k| *k > 0);
    k_sweep.sort_unstable();
    k_sweep.dedup();
    if k_sweep.is_empty() {
        return Err(Error::InvalidInput {
            message: "k sweep resolved to an empty set".to_string(),
        });
    }
    let max_k = *k_sweep.last().unwrap();

    let index = table
        .list_indices()
        .await?
        .into_iter()
        .find(|c| c.name == index_name)
        .ok_or_else(|| Error::InvalidInput {
            message: format!("index '{}' not found on table", index_name),
        })?;

    match index.index_type {
        IndexType::IvfFlat | IndexType::IvfPq => {}
        other => {
            return Err(Error::InvalidInput {
                message: format!(
                    "analyze_index supports IVF_FLAT and IVF_PQ only (got {}); \
                     support for other index types is planned",
                    other
                ),
            });
        }
    }
    let is_pq = matches!(index.index_type, IndexType::IvfPq);

    let vec_col = index
        .columns
        .first()
        .ok_or_else(|| Error::InvalidInput {
            message: format!("index '{}' has no indexed columns", index_name),
        })?
        .clone();

    let stats = table
        .index_stats(index_name)
        .await?
        .ok_or_else(|| Error::InvalidInput {
            message: format!("no statistics available for index '{}'", index_name),
        })?;
    let distance_type = stats.distance_type.ok_or_else(|| Error::InvalidInput {
        message: format!("vector index '{}' is missing a distance type", index_name),
    })?;

    // IndexStatistics doesn't expose num_partitions, so pull it from the raw
    // Lance JSON, which wraps per-sub-index IVF stats in `indices: [...]`.
    let ds_wrapper = table.dataset().ok_or_else(|| Error::InvalidInput {
        message: "analyze_index requires a native (local) table".to_string(),
    })?;
    let raw_stats = ds_wrapper
        .get()
        .await?
        .index_statistics(index_name)
        .await
        .map_err(Error::from)?;
    let raw: serde_json::Value =
        serde_json::from_str(&raw_stats).map_err(|e| Error::InvalidInput {
            message: format!("failed to parse index statistics: {}", e),
        })?;
    let num_partitions: u32 = raw
        .get("indices")
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|e| e.get("num_partitions"))
        .and_then(|v| v.as_u64())
        .or_else(|| raw.get("num_partitions").and_then(|v| v.as_u64()))
        .ok_or_else(|| Error::InvalidInput {
            message: format!(
                "index statistics missing num_partitions; raw stats: {}",
                raw_stats
            ),
        })?
        .try_into()
        .map_err(|_| Error::InvalidInput {
            message: "num_partitions does not fit in u32".to_string(),
        })?;

    let total_rows = table.count_rows(None).await?;
    if total_rows == 0 {
        return Err(Error::InvalidInput {
            message: "table has no rows to sample".to_string(),
        });
    }
    let sample_size = options.sample_size.min(total_rows);
    if sample_size == 0 {
        return Err(Error::InvalidInput {
            message: "sample_size must be greater than 0".to_string(),
        });
    }

    let mut rng = match options.seed {
        Some(s) => SmallRng::seed_from_u64(s),
        None => SmallRng::seed_from_u64(rand::rng().random::<u64>()),
    };
    let samples = sample_query_vectors(table, &vec_col, total_rows, sample_size, &mut rng).await?;

    let mut nprobes_sweep: Vec<u32> = options
        .nprobes
        .clone()
        .unwrap_or_else(|| default_nprobes(num_partitions))
        .into_iter()
        .filter(|n| *n > 0)
        .map(|n| n.min(num_partitions))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    nprobes_sweep.sort_unstable();
    if nprobes_sweep.is_empty() {
        return Err(Error::InvalidInput {
            message: "nprobes sweep resolved to an empty set".to_string(),
        });
    }

    let mut refine_sweep: Vec<Option<u32>> = if is_pq {
        options
            .refine_factor
            .clone()
            .unwrap_or_else(default_refine_factors)
            .into_iter()
            .filter(|r| *r > 0)
            .collect::<HashSet<_>>()
            .into_iter()
            .map(Some)
            .collect()
    } else {
        vec![None]
    };
    refine_sweep.sort_by_key(|r| r.unwrap_or(0));

    // Fetch GT once at the largest K; smaller K values take a prefix.
    let ground_truth = batch_knn_flat(table, &vec_col, &samples, max_k + 1, distance_type).await?;

    let mut out_num_partitions = Vec::new();
    let mut out_index_type = Vec::new();
    let mut out_k = Vec::new();
    let mut out_nprobes = Vec::new();
    let mut out_refine_factor: Vec<Option<u32>> = Vec::new();
    let mut out_recall = Vec::new();
    let mut out_min = Vec::new();
    let mut out_p50 = Vec::new();
    let mut out_p90 = Vec::new();
    let mut out_p99 = Vec::new();
    let mut out_max = Vec::new();

    let index_type_str = index.index_type.to_string();

    for &np in &nprobes_sweep {
        for &rf in &refine_sweep {
            for &k in &k_sweep {
                let mut latencies_ms: Vec<f64> = Vec::with_capacity(samples.len());
                let mut recalls: Vec<f64> = Vec::with_capacity(samples.len());

                for (query_idx, (self_rowid, vector)) in samples.iter().enumerate() {
                    let start = Instant::now();
                    let mut q = table
                        .query()
                        .nearest_to(vector.as_slice())?
                        .column(&vec_col)
                        .distance_type(distance_type)
                        .nprobes(np as usize)
                        .limit(k + 1)
                        .with_row_id();
                    if let Some(rf) = rf {
                        q = q.refine_factor(rf);
                    }
                    let batches: Vec<RecordBatch> = q.execute().await?.try_collect().await?;
                    let elapsed_ms = start.elapsed().as_secs_f64() * 1_000.0;
                    latencies_ms.push(elapsed_ms);

                    let indexed_ids = extract_row_ids(&batches)?;
                    let gt_ids = &ground_truth[query_idx];
                    recalls.push(recall_at_k(gt_ids, &indexed_ids, *self_rowid, k));
                }

                let avg_recall = if recalls.is_empty() {
                    0.0
                } else {
                    recalls.iter().sum::<f64>() / recalls.len() as f64
                };
                let (lmin, l50, l90, l99, lmax) = latency_percentiles(&mut latencies_ms);

                out_num_partitions.push(num_partitions);
                out_index_type.push(index_type_str.clone());
                out_k.push(k as u32);
                out_nprobes.push(np);
                out_refine_factor.push(rf);
                out_recall.push(avg_recall);
                out_min.push(lmin);
                out_p50.push(l50);
                out_p90.push(l90);
                out_p99.push(l99);
                out_max.push(lmax);
            }
        }
    }

    let schema = analyze_index_schema();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt32Array::from(out_num_partitions)),
            Arc::new(StringArray::from(out_index_type)),
            Arc::new(UInt32Array::from(out_k)),
            Arc::new(UInt32Array::from(out_nprobes)),
            Arc::new(UInt32Array::from_iter(out_refine_factor)),
            Arc::new(Float64Array::from(out_recall)),
            Arc::new(Float64Array::from(out_min)),
            Arc::new(Float64Array::from(out_p50)),
            Arc::new(Float64Array::from(out_p90)),
            Arc::new(Float64Array::from(out_p99)),
            Arc::new(Float64Array::from(out_max)),
        ],
    )
    .map_err(|e| Error::InvalidInput {
        message: format!("failed to build analyze_index RecordBatch: {}", e),
    })
}

fn default_nprobes(num_partitions: u32) -> Vec<u32> {
    [1u32, 2, 4, 8, 16, 32, 64]
        .into_iter()
        .filter(|n| *n <= num_partitions)
        .chain(std::iter::once(num_partitions))
        .collect()
}

fn default_refine_factors() -> Vec<u32> {
    vec![1, 2, 4]
}

// TODO: swap this for Lance's batch KNN primitive when it lands. This only
// affects the GT phase; the ANN latency columns are measured per-query.
async fn batch_knn_flat(
    table: &Table,
    vec_col: &str,
    samples: &[(u64, Vec<f32>)],
    k: usize,
    distance_type: DistanceType,
) -> Result<Vec<Vec<u64>>> {
    let mut results = Vec::with_capacity(samples.len());
    for (_row_id, v) in samples {
        let batches: Vec<RecordBatch> = table
            .query()
            .nearest_to(v.as_slice())?
            .column(vec_col)
            .distance_type(distance_type)
            .bypass_vector_index()
            .limit(k)
            .with_row_id()
            .execute()
            .await?
            .try_collect()
            .await?;
        results.push(extract_row_ids(&batches)?);
    }
    Ok(results)
}

async fn sample_query_vectors(
    table: &Table,
    vec_col: &str,
    total_rows: usize,
    sample_size: usize,
    rng: &mut SmallRng,
) -> Result<Vec<(u64, Vec<f32>)>> {
    let offsets: Vec<u64> = (0..total_rows as u64).choose_multiple(rng, sample_size);

    let stream = table
        .take_offsets(offsets)
        .select(Select::columns(&[vec_col]))
        .with_row_id()
        .execute()
        .await?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;

    let mut samples: Vec<(u64, Vec<f32>)> = Vec::with_capacity(sample_size);
    for batch in &batches {
        let rowid_arr = batch
            .column_by_name(ROW_ID_COL)
            .ok_or_else(|| Error::InvalidInput {
                message: "take result missing _rowid column".to_string(),
            })?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| Error::InvalidInput {
                message: "_rowid column was not UInt64".to_string(),
            })?;
        let vec_arr = batch
            .column_by_name(vec_col)
            .ok_or_else(|| Error::InvalidInput {
                message: format!("take result missing vector column '{}'", vec_col),
            })?
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| Error::InvalidInput {
                message: format!("vector column '{}' was not a FixedSizeList", vec_col),
            })?;

        for i in 0..batch.num_rows() {
            let row_id = rowid_arr.value(i);
            let inner = vec_arr.value(i);
            let v = vector_values_to_f32(&inner, vec_col)?;
            samples.push((row_id, v));
        }
    }

    if samples.len() < sample_size {
        return Err(Error::InvalidInput {
            message: format!(
                "sampled only {} of {} requested query vectors",
                samples.len(),
                sample_size
            ),
        });
    }
    Ok(samples)
}

// `nearest_to` casts to f32 internally, so we do the same for sampled vectors.
fn vector_values_to_f32(inner: &ArrayRef, col: &str) -> Result<Vec<f32>> {
    match inner.data_type() {
        DataType::Float32 => {
            let a = inner.as_primitive::<Float32Type>();
            Ok((0..a.len()).map(|i| a.value(i)).collect())
        }
        DataType::Float16 => {
            let a = inner.as_primitive::<Float16Type>();
            Ok((0..a.len()).map(|i| a.value(i).to_f32()).collect())
        }
        DataType::Float64 => {
            let a = inner.as_primitive::<Float64Type>();
            Ok((0..a.len()).map(|i| a.value(i) as f32).collect())
        }
        other => Err(Error::InvalidInput {
            message: format!(
                "vector column '{}' has unsupported element type {:?}; \
                 analyze_index currently supports Float16/Float32/Float64 vectors",
                col, other
            ),
        }),
    }
}

fn extract_row_ids(batches: &[RecordBatch]) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for batch in batches {
        let col = batch
            .column_by_name(ROW_ID_COL)
            .ok_or_else(|| Error::InvalidInput {
                message: "query result missing _rowid column".to_string(),
            })?;
        let arr =
            col.as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| Error::InvalidInput {
                    message: "_rowid column was not UInt64".to_string(),
                })?;
        ids.extend((0..arr.len()).map(|i| arr.value(i)));
    }
    Ok(ids)
}

// `gt` and `indexed` are the top-(K+1) row ids from each path. Filtering
// `self_rowid` drops the query's own row before computing recall.
fn recall_at_k(gt: &[u64], indexed: &[u64], self_rowid: u64, k: usize) -> f64 {
    let gt_set: HashSet<u64> = gt
        .iter()
        .copied()
        .filter(|id| *id != self_rowid)
        .take(k)
        .collect();
    let idx_set: HashSet<u64> = indexed
        .iter()
        .copied()
        .filter(|id| *id != self_rowid)
        .take(k)
        .collect();
    if gt_set.is_empty() {
        return 1.0;
    }
    let hits = gt_set.intersection(&idx_set).count() as f64;
    hits / gt_set.len() as f64
}

// Returns `(min, p50, p90, p99, max)`; sorts `values` in place.
fn latency_percentiles(values: &mut [f64]) -> (f64, f64, f64, f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0, 0.0, 0.0, 0.0);
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pick = |q: f64| -> f64 {
        let n = values.len();
        let idx = ((q * (n as f64 - 1.0)).round() as usize).min(n - 1);
        values[idx]
    };
    (
        values[0],
        pick(0.50),
        pick(0.90),
        pick(0.99),
        *values.last().unwrap(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_basic() {
        let mut xs = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let (min, _p50, p90, p99, max) = latency_percentiles(&mut xs);
        assert_eq!(min, 1.0);
        assert_eq!(max, 10.0);
        assert!(p90 >= 9.0);
        assert_eq!(p99, 10.0);
    }

    #[test]
    fn recall_filters_self_rowid() {
        let gt = vec![42u64, 1, 2, 3, 4, 5];
        let idx = vec![42u64, 1, 2, 3, 4, 5];
        assert!((recall_at_k(&gt, &idx, 42, 5) - 1.0).abs() < 1e-9);

        // Indexed missed one real neighbor.
        let gt = vec![42u64, 1, 2, 3, 4, 5];
        let idx = vec![42u64, 1, 2, 3, 4, 99];
        let r = recall_at_k(&gt, &idx, 42, 5);
        assert!((r - 0.8).abs() < 1e-9, "expected 0.8 got {}", r);
    }

    #[test]
    fn nprobes_default_clamps() {
        let got = default_nprobes(5);
        let set: HashSet<u32> = got.into_iter().collect();
        assert!(set.contains(&1));
        assert!(set.contains(&2));
        assert!(set.contains(&4));
        assert!(set.contains(&5));
        assert!(!set.contains(&8));
    }
}
