// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Compare the current mask-aware HNSW traversal (`search_basic`) against an
//! ACORN-1 style traversal (`search_acorn`) and a flat scan over matching
//! rows, across filter selectivities and mask shapes:
//!
//! - `corr-in`: mask is a cluster in embedding space, query is inside it
//!   (e.g. "filter to courses" with a course query)
//! - `corr-out`: mask is a cluster, query is unrelated
//! - `random`: uniform random mask (the easy case)
//!
//! Run: cargo run --release -p lance-index --example acorn_bench

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, FixedSizeListArray, types::Float32Type};
use lance_arrow::FixedSizeListArrayExt;
use lance_index::vector::flat::storage::FlatFloatStorage;
use lance_index::vector::graph::VisitedGenerator;
use lance_index::vector::hnsw::builder::{HNSW, HnswBuildParams, HnswQueryParams};
use lance_index::vector::storage::{DistCalculator, VectorStore};
use lance_index::vector::v3::subindex::IvfSubIndex;
use lance_linalg::distance::DistanceType;
use lance_testing::datagen::generate_random_array_with_seed;
use rand::rngs::SmallRng;
use rand::seq::{IndexedRandom, SliceRandom};
use rand::{Rng, SeedableRng};

const TOTAL: usize = 100_000;
const DIMENSION: usize = 768;
const K: usize = 10;
const EF: usize = 100;
const EF_HI: usize = 400;
const QUERIES_PER_CASE: usize = 20;
const SEED: [u8; 32] = [42; 32];

fn all_distances(storage: &FlatFloatStorage, query: Arc<dyn Array>) -> Vec<f32> {
    let dist_calc = storage.dist_calculator(query, 0.0);
    (0..TOTAL as u32).map(|i| dist_calc.distance(i)).collect()
}

/// Node ids sorted ascending by distance to `query`, restricted to `mask`.
fn ground_truth(storage: &FlatFloatStorage, query: Arc<dyn Array>, mask: &[bool]) -> Vec<u32> {
    let dists = all_distances(storage, query);
    let mut ids: Vec<u32> = (0..TOTAL as u32).filter(|&i| mask[i as usize]).collect();
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
    println!("generating {TOTAL} x {DIMENSION} vectors...");
    let data = generate_random_array_with_seed::<Float32Type>(TOTAL * DIMENSION, SEED);
    let fsl = FixedSizeListArray::try_new_from_values(data, DIMENSION as i32).unwrap();
    let storage = Arc::new(FlatFloatStorage::new(fsl.clone(), DistanceType::L2));

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

    // unfiltered control: this graph's recall ceiling at ef=EF
    {
        let mut control = CaseResult::new();
        let all_mask = vec![true; TOTAL];
        for _ in 0..QUERIES_PER_CASE {
            let query_id = rng.random_range(0..TOTAL as u32);
            let query = fsl.value(query_id as usize);
            let truth = ground_truth(&storage, query.clone(), &all_mask);
            let t = Instant::now();
            let nodes = hnsw
                .search_basic(query.clone(), K, &params, None, storage.as_ref())
                .unwrap();
            control.latency_us.push(t.elapsed().as_secs_f64() * 1e6);
            let got: Vec<u32> = nodes.iter().map(|n| n.id).collect();
            control.recalls.push(recall(&got, &truth));
        }
        println!(
            "\nunfiltered control (ef={EF}): {:.2} ms, recall@{K} {:.3}",
            control.median_latency_ms(),
            control.mean_recall()
        );
    }

    println!(
        "\n{:<10} {:>5} | {:>9} {:>7} | {:>10} {:>7} | {:>9} {:>7} | {:>9} {:>7}",
        "mask",
        "sel%",
        "basic ms",
        "recall",
        "basic4x ms",
        "recall",
        "acorn ms",
        "recall",
        "flat ms",
        "recall"
    );

    for mask_kind in ["corr-in", "corr-out", "random"] {
        for selectivity in [0.02f64, 0.05, 0.10, 0.25, 0.50] {
            let mask_size = (TOTAL as f64 * selectivity) as usize;
            let mut basic = CaseResult::new();
            let mut basic_hi = CaseResult::new();
            let mut acorn = CaseResult::new();
            let mut flat = CaseResult::new();
            let mut mask_generator = VisitedGenerator::new(TOTAL);

            for _ in 0..QUERIES_PER_CASE {
                let mut mask = vec![false; TOTAL];
                let anchor_id = rng.random_range(0..TOTAL as u32);
                let member_ids: Vec<u32> = match mask_kind {
                    "random" => {
                        let mut ids: Vec<u32> = (0..TOTAL as u32).collect();
                        ids.shuffle(&mut rng);
                        ids.truncate(mask_size);
                        ids
                    }
                    _ => {
                        // cluster: the nearest nodes to a random anchor
                        let dists = all_distances(&storage, fsl.value(anchor_id as usize));
                        let mut ids: Vec<u32> = (0..TOTAL as u32).collect();
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

                let query_id = match mask_kind {
                    "corr-in" => *member_ids.choose(&mut rng).unwrap(),
                    _ => rng.random_range(0..TOTAL as u32),
                };
                let query = fsl.value(query_id as usize);
                let truth = ground_truth(&storage, query.clone(), &mask);

                // search_basic (current traversal)
                {
                    let mut bitset = mask_generator.generate(TOTAL);
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
                    let mut bitset = mask_generator.generate(TOTAL);
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
                    let mut bitset = mask_generator.generate(TOTAL);
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
                "{:<10} {:>5.0} | {:>9.2} {:>7.3} | {:>10.2} {:>7.3} | {:>9.2} {:>7.3} | {:>9.2} {:>7.3}",
                mask_kind,
                selectivity * 100.0,
                basic.median_latency_ms(),
                basic.mean_recall(),
                basic_hi.median_latency_ms(),
                basic_hi.mean_recall(),
                acorn.median_latency_ms(),
                acorn.mean_recall(),
                flat.median_latency_ms(),
                flat.mean_recall(),
            );
        }
    }
}
