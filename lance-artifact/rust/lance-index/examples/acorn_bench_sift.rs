// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! SIFT1M variant of `acorn_bench`: compares `search_basic`, `search_acorn`,
//! and a flat scan on the ANN_SIFT1M dataset (1M x 128, L2) with synthetic
//! filter masks. The unfiltered control is checked against the official
//! ground truth.
//!
//! Run: SIFT_DIR=/path/to/sift cargo run --release -p lance-index --example acorn_bench_sift
//! Works with any texmex-format dataset, e.g. SIFT_DIR=/path/to/gist for GIST1M.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, FixedSizeListArray, Float32Array};
use lance_arrow::FixedSizeListArrayExt;
use lance_index::vector::flat::storage::FlatFloatStorage;
use lance_index::vector::graph::VisitedGenerator;
use lance_index::vector::hnsw::builder::{HNSW, HnswBuildParams, HnswQueryParams};
use lance_index::vector::storage::{DistCalculator, VectorStore};
use lance_index::vector::v3::subindex::IvfSubIndex;
use lance_linalg::distance::DistanceType;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

const K: usize = 10;
const EF: usize = 100;
const EF_HI: usize = 400;
const QUERIES_PER_CASE: usize = 20;

/// (flat values, dim, count)
fn read_fvecs(path: &str) -> (Vec<f32>, usize, usize) {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let dim = i32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let record = 4 + dim * 4;
    assert_eq!(bytes.len() % record, 0);
    let count = bytes.len() / record;
    let mut values = Vec::with_capacity(count * dim);
    for row in 0..count {
        let start = row * record + 4;
        for i in 0..dim {
            let offset = start + i * 4;
            values.push(f32::from_le_bytes(
                bytes[offset..offset + 4].try_into().unwrap(),
            ));
        }
    }
    (values, dim, count)
}

fn read_ivecs(path: &str) -> Vec<Vec<u32>> {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let dim = i32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let record = 4 + dim * 4;
    assert_eq!(bytes.len() % record, 0);
    (0..bytes.len() / record)
        .map(|row| {
            let start = row * record + 4;
            (0..dim)
                .map(|i| {
                    let offset = start + i * 4;
                    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
                })
                .collect()
        })
        .collect()
}

fn all_distances(storage: &FlatFloatStorage, total: usize, query: Arc<dyn Array>) -> Vec<f32> {
    let dist_calc = storage.dist_calculator(query, 0.0);
    (0..total as u32).map(|i| dist_calc.distance(i)).collect()
}

fn ground_truth(
    storage: &FlatFloatStorage,
    total: usize,
    query: Arc<dyn Array>,
    mask: &[bool],
) -> Vec<u32> {
    let dists = all_distances(storage, total, query);
    let mut ids: Vec<u32> = (0..total as u32).filter(|&i| mask[i as usize]).collect();
    ids.sort_by(|&a, &b| dists[a as usize].partial_cmp(&dists[b as usize]).unwrap());
    ids.truncate(K);
    ids
}

fn recall(got: &[u32], truth: &[u32]) -> f64 {
    let hits = truth.iter().filter(|id| got.contains(id)).count();
    hits as f64 / truth.len().max(1) as f64
}

struct CaseResult {
    latency_us: Vec<f64>,
    recalls: Vec<f64>,
}

impl CaseResult {
    fn new() -> Self {
        Self {
            latency_us: vec![],
            recalls: vec![],
        }
    }
    fn median_latency_ms(&mut self) -> f64 {
        self.latency_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
        self.latency_us[self.latency_us.len() / 2] / 1000.0
    }
    fn mean_recall(&self) -> f64 {
        self.recalls.iter().sum::<f64>() / self.recalls.len() as f64
    }
}

fn main() {
    let sift_dir = std::env::var("SIFT_DIR").expect("set SIFT_DIR to the extracted dataset dir");
    let prefix = std::path::Path::new(&sift_dir)
        .file_name()
        .and_then(|name| name.to_str())
        .expect("SIFT_DIR must end in the dataset name, e.g. sift or gist")
        .to_string();
    println!("loading {prefix} from {sift_dir}...");
    let (base, dim, total) = read_fvecs(&format!("{sift_dir}/{prefix}_base.fvecs"));
    let (queries, query_dim, num_queries) = read_fvecs(&format!("{sift_dir}/{prefix}_query.fvecs"));
    let official_gt = read_ivecs(&format!("{sift_dir}/{prefix}_groundtruth.ivecs"));
    assert_eq!(dim, query_dim);
    println!("base {total} x {dim}, {num_queries} queries");

    let fsl =
        FixedSizeListArray::try_new_from_values(Float32Array::from(base), dim as i32).unwrap();
    let storage = Arc::new(FlatFloatStorage::new(fsl.clone(), DistanceType::L2));
    let query_vec = |i: usize| -> Arc<dyn Array> {
        Arc::new(Float32Array::from(queries[i * dim..(i + 1) * dim].to_vec()))
    };

    println!("building HNSW graph...");
    let build_start = Instant::now();
    let hnsw = HNSW::index_vectors(storage.as_ref(), HnswBuildParams::default()).unwrap();
    println!("built in {:.1}s", build_start.elapsed().as_secs_f32());

    let mut rng = SmallRng::seed_from_u64(7);
    let params = HnswQueryParams {
        ef: EF,
        lower_bound: None,
        upper_bound: None,
        dist_q_c: 0.0,
        use_acorn: false,
    };
    let params_hi = HnswQueryParams {
        ef: EF_HI,
        ..params
    };

    // unfiltered control against the official ground truth
    {
        let mut control = CaseResult::new();
        for _ in 0..QUERIES_PER_CASE {
            let query_id = rng.random_range(0..num_queries);
            let query = query_vec(query_id);
            let truth: Vec<u32> = official_gt[query_id][..K].to_vec();
            let t = Instant::now();
            let nodes = hnsw
                .search_basic(query, K, &params, None, storage.as_ref())
                .unwrap();
            control.latency_us.push(t.elapsed().as_secs_f64() * 1e6);
            let got: Vec<u32> = nodes.iter().map(|n| n.id).collect();
            control.recalls.push(recall(&got, &truth));
        }
        println!(
            "\nunfiltered control (ef={EF}, official GT): {:.2} ms, recall@{K} {:.3}",
            control.median_latency_ms(),
            control.mean_recall()
        );
    }

    println!(
        "\n{:<10} {:>5} | {:>9} {:>7} | {:>10} {:>7} | {:>9} {:>7} | {:>10} {:>7} | {:>9} {:>7}",
        "mask",
        "sel%",
        "basic ms",
        "recall",
        "basic4x ms",
        "recall",
        "acorn ms",
        "recall",
        "acorn4x ms",
        "recall",
        "flat ms",
        "recall"
    );

    for mask_kind in ["corr-in", "corr-out", "random"] {
        for selectivity in [0.02f64, 0.05, 0.10, 0.25, 0.50] {
            let mask_size = (total as f64 * selectivity) as usize;
            let mut basic = CaseResult::new();
            let mut basic_hi = CaseResult::new();
            let mut acorn = CaseResult::new();
            let mut acorn_hi = CaseResult::new();
            let mut flat = CaseResult::new();
            let mut mask_generator = VisitedGenerator::new(total);

            for _ in 0..QUERIES_PER_CASE {
                let query_id = rng.random_range(0..num_queries);
                let query = query_vec(query_id);

                // corr-in: cluster around the query's own neighborhood
                // corr-out: cluster around an unrelated base vector
                let mut mask = vec![false; total];
                let member_ids: Vec<u32> = match mask_kind {
                    "random" => {
                        let mut ids: Vec<u32> = (0..total as u32).collect();
                        ids.shuffle(&mut rng);
                        ids.truncate(mask_size);
                        ids
                    }
                    _ => {
                        let anchor: Arc<dyn Array> = match mask_kind {
                            "corr-in" => query.clone(),
                            _ => fsl.value(rng.random_range(0..total)),
                        };
                        let dists = all_distances(&storage, total, anchor);
                        let mut ids: Vec<u32> = (0..total as u32).collect();
                        ids.sort_by(|&a, &b| {
                            dists[a as usize].partial_cmp(&dists[b as usize]).unwrap()
                        });
                        ids.truncate(mask_size);
                        ids
                    }
                };
                for &id in &member_ids {
                    mask[id as usize] = true;
                }
                let truth = ground_truth(&storage, total, query.clone(), &mask);

                // search_basic (current traversal)
                {
                    let mut bitset = mask_generator.generate(total);
                    for &id in &member_ids {
                        bitset.insert(id);
                    }
                    let t = Instant::now();
                    let nodes = hnsw
                        .search_basic(query.clone(), K, &params, Some(bitset), storage.as_ref())
                        .unwrap();
                    basic.latency_us.push(t.elapsed().as_secs_f64() * 1e6);
                    let got: Vec<u32> = nodes.iter().map(|n| n.id).collect();
                    basic.recalls.push(recall(&got, &truth));
                }

                // search_basic at 4x ef (recall-equalizing baseline)
                {
                    let mut bitset = mask_generator.generate(total);
                    for &id in &member_ids {
                        bitset.insert(id);
                    }
                    let t = Instant::now();
                    let nodes = hnsw
                        .search_basic(query.clone(), K, &params_hi, Some(bitset), storage.as_ref())
                        .unwrap();
                    basic_hi.latency_us.push(t.elapsed().as_secs_f64() * 1e6);
                    let got: Vec<u32> = nodes.iter().map(|n| n.id).collect();
                    basic_hi.recalls.push(recall(&got, &truth));
                }

                // search_acorn
                {
                    let mut bitset = mask_generator.generate(total);
                    for &id in &member_ids {
                        bitset.insert(id);
                    }
                    let t = Instant::now();
                    let nodes = hnsw
                        .search_acorn(query.clone(), K, &params, &bitset, storage.as_ref())
                        .unwrap();
                    acorn.latency_us.push(t.elapsed().as_secs_f64() * 1e6);
                    let got: Vec<u32> = nodes.iter().map(|n| n.id).collect();
                    acorn.recalls.push(recall(&got, &truth));
                }

                // search_acorn at 4x ef
                {
                    let mut bitset = mask_generator.generate(total);
                    for &id in &member_ids {
                        bitset.insert(id);
                    }
                    let t = Instant::now();
                    let nodes = hnsw
                        .search_acorn(query.clone(), K, &params_hi, &bitset, storage.as_ref())
                        .unwrap();
                    acorn_hi.latency_us.push(t.elapsed().as_secs_f64() * 1e6);
                    let got: Vec<u32> = nodes.iter().map(|n| n.id).collect();
                    acorn_hi.recalls.push(recall(&got, &truth));
                }

                // flat scan over matching rows
                {
                    let t = Instant::now();
                    let dist_calc = storage.dist_calculator(query.clone(), 0.0);
                    let mut scored: Vec<(f32, u32)> = member_ids
                        .iter()
                        .map(|&id| (dist_calc.distance(id), id))
                        .collect();
                    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                    scored.truncate(K);
                    flat.latency_us.push(t.elapsed().as_secs_f64() * 1e6);
                    let got: Vec<u32> = scored.iter().map(|&(_, id)| id).collect();
                    flat.recalls.push(recall(&got, &truth));
                }
            }

            println!(
                "{:<10} {:>5.0} | {:>9.2} {:>7.3} | {:>10.2} {:>7.3} | {:>9.2} {:>7.3} | {:>10.2} {:>7.3} | {:>9.2} {:>7.3}",
                mask_kind,
                selectivity * 100.0,
                basic.median_latency_ms(),
                basic.mean_recall(),
                basic_hi.median_latency_ms(),
                basic_hi.mean_recall(),
                acorn.median_latency_ms(),
                acorn.mean_recall(),
                acorn_hi.median_latency_ms(),
                acorn_hi.mean_recall(),
                flat.median_latency_ms(),
                flat.mean_recall(),
            );
        }
    }
}
