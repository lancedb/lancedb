// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Sweep ANN parameters against exhaustive ground truth and report recall +
//! latency percentiles per configuration. Supports all IVF index variants.

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
    /// `ef` (HNSW search beam width) values to sweep. Ignored for non-HNSW indexes.
    pub ef: Option<Vec<u32>>,
}

impl Default for AnalyzeIndexOptions {
    fn default() -> Self {
        Self {
            sample_size: 1000,
            k: None,
            seed: None,
            nprobes: None,
            refine_factor: None,
            ef: None,
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
        Field::new("ef", DataType::UInt32, true),
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

    if !is_ivf(&index.index_type) {
        return Err(Error::InvalidInput {
            message: format!(
                "analyze_index supports IVF index variants only (got {})",
                index.index_type
            ),
        });
    }
    let sweeps_refine = uses_refine_factor(&index.index_type);
    let sweeps_ef = is_hnsw(&index.index_type);

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

    let mut refine_sweep: Vec<Option<u32>> = if sweeps_refine {
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

    let mut ef_sweep: Vec<Option<u32>> = if sweeps_ef {
        options
            .ef
            .clone()
            .unwrap_or_else(|| default_ef_sweep(max_k))
            .into_iter()
            .filter(|e| *e > 0)
            .collect::<HashSet<_>>()
            .into_iter()
            .map(Some)
            .collect()
    } else {
        vec![None]
    };
    ef_sweep.sort_by_key(|e| e.unwrap_or(0));

    // Fetch GT once at the largest K; smaller K values take a prefix.
    let ground_truth = batch_knn_flat(table, &vec_col, &samples, max_k + 1, distance_type).await?;

    let mut rows: Vec<AnalyzeRow> = Vec::new();
    let index_type_str = index.index_type.to_string();

    for &np in &nprobes_sweep {
        for &rf in &refine_sweep {
            for &ef in &ef_sweep {
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
                        if let Some(ef) = ef {
                            q = q.ef(ef as usize);
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

                    rows.push(AnalyzeRow {
                        num_partitions,
                        index_type: index_type_str.clone(),
                        k: k as u32,
                        nprobes: np,
                        refine_factor: rf,
                        ef,
                        recall: avg_recall,
                        latency_min_ms: lmin,
                        latency_p50_ms: l50,
                        latency_p90_ms: l90,
                        latency_p99_ms: l99,
                        latency_max_ms: lmax,
                    });
                }
            }
        }
    }

    rows_to_record_batch(&rows)
}

struct AnalyzeRow {
    num_partitions: u32,
    index_type: String,
    k: u32,
    nprobes: u32,
    refine_factor: Option<u32>,
    ef: Option<u32>,
    recall: f64,
    latency_min_ms: f64,
    latency_p50_ms: f64,
    latency_p90_ms: f64,
    latency_p99_ms: f64,
    latency_max_ms: f64,
}

fn rows_to_record_batch(rows: &[AnalyzeRow]) -> Result<RecordBatch> {
    RecordBatch::try_new(
        analyze_index_schema(),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|r| r.num_partitions),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| &r.index_type),
            )),
            Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.k))),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|r| r.nprobes),
            )),
            Arc::new(UInt32Array::from_iter(rows.iter().map(|r| r.refine_factor))),
            Arc::new(UInt32Array::from_iter(rows.iter().map(|r| r.ef))),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.recall),
            )),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.latency_min_ms),
            )),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.latency_p50_ms),
            )),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.latency_p90_ms),
            )),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.latency_p99_ms),
            )),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.latency_max_ms),
            )),
        ],
    )
    .map_err(|e| Error::InvalidInput {
        message: format!("failed to build analyze_index RecordBatch: {}", e),
    })
}

fn is_ivf(ty: &IndexType) -> bool {
    matches!(
        ty,
        IndexType::IvfFlat
            | IndexType::IvfSq
            | IndexType::IvfPq
            | IndexType::IvfRq
            | IndexType::IvfHnswSq
            | IndexType::IvfHnswPq
    )
}

fn uses_refine_factor(ty: &IndexType) -> bool {
    !matches!(ty, IndexType::IvfFlat)
}

fn is_hnsw(ty: &IndexType) -> bool {
    matches!(ty, IndexType::IvfHnswSq | IndexType::IvfHnswPq)
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

// HNSW's `ef` must be >= the query limit to be meaningful. Sweep multiples of
// the largest k in the run so every row's ef is within a useful range.
fn default_ef_sweep(max_k: usize) -> Vec<u32> {
    let base = (max_k as u32).saturating_add(1);
    vec![base, base.saturating_mul(2), base.saturating_mul(4)]
}

/// Compute ground truth KNN for a batch of query vectors using brute-force
/// flat scan. Returns `Vec<Vec<u64>>` — nearest neighbor `_rowid` values
/// grouped by query index.
async fn batch_knn_flat(
    table: &Table,
    vec_col: &str,
    samples: &[(u64, Vec<f32>)],
    k: usize,
    distance_type: DistanceType,
) -> Result<Vec<Vec<u64>>> {
    use arrow_array::Float32Array;
    use arrow_array::types::Int32Type;

    let base = table.base_table().clone();
    let dim = samples
        .first()
        .map(|(_, v)| v.len() as i32)
        .ok_or_else(|| Error::InvalidInput {
            message: "no samples provided".to_string(),
        })?;

    // Build FixedSizeListArray from sample vectors.
    let flat_values: Vec<f32> = samples
        .iter()
        .flat_map(|(_, v)| v.iter().copied())
        .collect();
    let values = Float32Array::from(flat_values);
    let field = Arc::new(arrow_schema::Field::new("item", DataType::Float32, true));
    let fsl = Arc::new(
        FixedSizeListArray::try_new(field, dim, Arc::new(values), None)
            .map_err(|e| Error::Arrow { source: e })?,
    );

    let df = crate::table::knn::batch_knn(base, vec_col, fsl, k, distance_type, true).await?;
    let batches: Vec<RecordBatch> = df.collect().await.map_err(|e| Error::Runtime {
        message: e.to_string(),
    })?;

    // Group _rowid values by _query_index.
    let num_queries = samples.len();
    let mut results: Vec<Vec<u64>> = vec![Vec::new(); num_queries];
    for batch in &batches {
        let qi_col = batch
            .column_by_name("_query_index")
            .ok_or_else(|| Error::InvalidInput {
                message: "batch_knn result missing _query_index column".to_string(),
            })?
            .as_primitive::<Int32Type>();
        let rowid_col = batch
            .column_by_name(ROW_ID_COL)
            .ok_or_else(|| Error::InvalidInput {
                message: "batch_knn result missing _rowid column".to_string(),
            })?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| Error::InvalidInput {
                message: "_rowid column was not UInt64".to_string(),
            })?;
        for i in 0..batch.num_rows() {
            let qi = qi_col.value(i) as usize;
            results[qi].push(rowid_col.value(i));
        }
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

    #[tokio::test]
    async fn analyze_index_all_ivf_types() {
        use std::sync::Arc;
        use std::time::Duration;

        use arrow_array::builder::FixedSizeListBuilder;
        use arrow_array::builder::Float32Builder;
        use arrow_array::types::UInt32Type;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use rand::SeedableRng;
        use rand::rngs::StdRng;

        use crate::connect;
        use crate::index::Index;
        use crate::index::vector::{
            IvfFlatIndexBuilder, IvfHnswPqIndexBuilder, IvfHnswSqIndexBuilder, IvfPqIndexBuilder,
            IvfRqIndexBuilder, IvfSqIndexBuilder,
        };

        struct Case {
            name: &'static str,
            index_type_str: &'static str,
            make_index: fn() -> Index,
            refine_input: Option<&'static [u32]>,
            ef_input: Option<&'static [u32]>,
        }

        // Shared sweep: 2 nprobes × 1 k. Plus refine/ef factors per case.
        const NPROBES: &[u32] = &[1, 4];
        const K: &[usize] = &[10];
        const REFINE: &[u32] = &[1];
        const EF: &[u32] = &[16];

        let cases = [
            Case {
                name: "flat",
                index_type_str: "IVF_FLAT",
                make_index: || Index::IvfFlat(IvfFlatIndexBuilder::default().num_partitions(4)),
                refine_input: None,
                ef_input: None,
            },
            Case {
                name: "sq",
                index_type_str: "IVF_SQ",
                make_index: || Index::IvfSq(IvfSqIndexBuilder::default().num_partitions(4)),
                refine_input: Some(REFINE),
                ef_input: None,
            },
            Case {
                name: "pq",
                index_type_str: "IVF_PQ",
                make_index: || {
                    Index::IvfPq(
                        IvfPqIndexBuilder::default()
                            .num_partitions(4)
                            .num_sub_vectors(4),
                    )
                },
                refine_input: Some(REFINE),
                ef_input: None,
            },
            Case {
                name: "rq",
                index_type_str: "IVF_RQ",
                make_index: || Index::IvfRq(IvfRqIndexBuilder::default().num_partitions(4)),
                refine_input: Some(REFINE),
                ef_input: None,
            },
            Case {
                name: "hnsw_sq",
                index_type_str: "IVF_HNSW_SQ",
                make_index: || Index::IvfHnswSq(IvfHnswSqIndexBuilder::default().num_partitions(4)),
                refine_input: Some(REFINE),
                ef_input: Some(EF),
            },
            Case {
                name: "hnsw_pq",
                index_type_str: "IVF_HNSW_PQ",
                make_index: || {
                    Index::IvfHnswPq(
                        IvfHnswPqIndexBuilder::default()
                            .num_partitions(4)
                            .num_sub_vectors(4),
                    )
                },
                refine_input: Some(REFINE),
                ef_input: Some(EF),
            },
        ];

        let tmp = tempfile::tempdir().unwrap();
        let conn = connect(tmp.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();

        let dim: i32 = 16;
        let num_rows = 1024usize;
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            false,
        )]));

        let make_batch = || {
            let mut rng = StdRng::seed_from_u64(0xA11CE);
            let mut b = FixedSizeListBuilder::new(Float32Builder::new(), dim);
            for _ in 0..num_rows {
                for _ in 0..dim {
                    b.values().append_value(rng.random::<f32>());
                }
                b.append(true);
            }
            RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(b.finish())]).unwrap()
        };

        for case in &cases {
            let table = conn
                .create_table(case.name, make_batch())
                .execute()
                .await
                .unwrap_or_else(|e| panic!("{}: create_table failed: {}", case.name, e));
            table
                .create_index(&["embeddings"], (case.make_index)())
                .execute()
                .await
                .unwrap_or_else(|e| panic!("{}: create_index failed: {}", case.name, e));
            table
                .wait_for_index(&["embeddings_idx"], Duration::from_secs(60))
                .await
                .unwrap_or_else(|e| panic!("{}: wait_for_index failed: {}", case.name, e));

            let out = analyze_index(
                &table,
                "embeddings_idx",
                AnalyzeIndexOptions {
                    sample_size: 30,
                    k: Some(K.to_vec()),
                    seed: Some(0xBEEF),
                    nprobes: Some(NPROBES.to_vec()),
                    refine_factor: case.refine_input.map(<[u32]>::to_vec),
                    ef: case.ef_input.map(<[u32]>::to_vec),
                },
            )
            .await
            .unwrap_or_else(|e| panic!("{}: analyze_index failed: {}", case.name, e));

            let expected_rows = NPROBES.len()
                * K.len()
                * case.refine_input.map_or(1, <[u32]>::len)
                * case.ef_input.map_or(1, <[u32]>::len);
            assert_eq!(out.schema(), analyze_index_schema(), "{}", case.name);
            assert_eq!(out.num_rows(), expected_rows, "{}", case.name);

            let index_type_col = out.column(1).as_string::<i32>();
            let k_col = out.column(2).as_primitive::<UInt32Type>();
            let nprobes_col = out.column(3).as_primitive::<UInt32Type>();
            let refine_col = out.column(4).as_primitive::<UInt32Type>();
            let ef_col = out.column(5).as_primitive::<UInt32Type>();
            let recall_col = out.column(6).as_primitive::<Float64Type>();
            let lmin_col = out.column(7).as_primitive::<Float64Type>();

            for i in 0..out.num_rows() {
                assert_eq!(
                    index_type_col.value(i),
                    case.index_type_str,
                    "{}",
                    case.name
                );
                assert_eq!(k_col.value(i) as usize, K[0], "{}", case.name);
                assert!(
                    NPROBES.contains(&nprobes_col.value(i)),
                    "{}: unexpected nprobes {}",
                    case.name,
                    nprobes_col.value(i)
                );
                let r = recall_col.value(i);
                assert!(
                    (0.0..=1.0).contains(&r),
                    "{}: recall out of range: {}",
                    case.name,
                    r
                );
                assert!(lmin_col.value(i) >= 0.0, "{}", case.name);
            }

            assert_column_matches(refine_col, case.refine_input, case.name, "refine_factor");
            assert_column_matches(ef_col, case.ef_input, case.name, "ef");
        }

        fn assert_column_matches(
            col: &arrow_array::PrimitiveArray<UInt32Type>,
            expected: Option<&[u32]>,
            case: &str,
            field: &str,
        ) {
            match expected {
                None => assert_eq!(
                    col.null_count(),
                    col.len(),
                    "{case}: expected {field} all null"
                ),
                Some(values) => {
                    assert_eq!(col.null_count(), 0, "{case}: expected {field} non-null");
                    for i in 0..col.len() {
                        assert!(
                            values.contains(&col.value(i)),
                            "{case}: unexpected {field} value {}",
                            col.value(i)
                        );
                    }
                }
            }
        }
    }
}
