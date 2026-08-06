// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#![allow(clippy::print_stdout)]

use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array};
use arrow_schema::{DataType, Field};
use lance_linalg::distance::DistanceType;
use rayon::prelude::*;

#[path = "../../../../src/dataset/mem_wal/hnsw/mod.rs"]
mod hnsw;

use hnsw::{ArrowFixedSizeListVectorStore, BuildParams, HnswGraph, SearchParams, VectorSource};

#[derive(Debug, Clone)]
struct Args {
    rows: usize,
    dim: usize,
    queries: usize,
    truth_queries: usize,
    k: usize,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    threads: usize,
    seed: u64,
    clusters: usize,
    noise: f32,
    query_repeats: usize,
    /// When > 0, rebuild the graph in a loop for this many seconds (sustained
    /// write), instead of a single build.
    insert_seconds: f64,
    /// When > 0, run the query workload in a loop for this many seconds
    /// (sustained read), instead of `query_repeats` passes.
    query_seconds: f64,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            rows: 1_000_000,
            dim: 1024,
            queries: 1_000,
            truth_queries: 100,
            k: 10,
            m: 12,
            ef_construction: 64,
            ef_search: 64,
            threads: std::thread::available_parallelism().map_or(1, usize::from),
            seed: 100,
            clusters: 4096,
            noise: 0.05,
            query_repeats: 1,
            insert_seconds: 0.0,
            query_seconds: 0.0,
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;
    if args.threads > 0 {
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(args.threads)
            .build_global();
    }

    println!(
        "bench=lance_hnsw rows={} dim={} queries={} truth_queries={} k={} m={} ef_construction={} ef_search={} threads={} seed={} clusters={} noise={}",
        args.rows,
        args.dim,
        args.queries,
        args.truth_queries,
        args.k,
        args.m,
        args.ef_construction,
        args.ef_search,
        rayon::current_num_threads(),
        args.seed,
        args.clusters,
        args.noise
    );

    let generate_start = Instant::now();
    let values = generate_vectors(&args)?;
    let generate_s = generate_start.elapsed().as_secs_f64();

    let arrow_start = Instant::now();
    let values = Arc::new(Float32Array::from(values)) as ArrayRef;
    let vectors = Arc::new(FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        args.dim as i32,
        values,
        None,
    )?);
    let store = Arc::new(ArrowFixedSizeListVectorStore::try_new(
        args.rows,
        1,
        args.dim,
        DistanceType::L2,
    )?);
    let ids = store.append_batch(vectors, 0)?;
    let snapshot = store.snapshot();
    let arrow_s = arrow_start.elapsed().as_secs_f64();

    let params = BuildParams::default()
        .num_edges(args.m)
        .ef_construction(args.ef_construction)
        .seed(args.seed);

    // Sustained write: rebuild a fresh graph in a loop for `insert_seconds`
    // (or a single build when 0) so throughput reflects steady-state, not a
    // burst. Only the last graph is retained for the query phase.
    let insert_start = Instant::now();
    let mut insert_passes: u64 = 0;
    let mut insert_core = std::time::Duration::ZERO;
    let graph = loop {
        let g = HnswGraph::try_new(args.rows, params.clone())?;
        let core = Instant::now();
        g.insert_batch(ids.clone(), &snapshot)?;
        insert_core += core.elapsed();
        insert_passes += 1;
        if args.insert_seconds <= 0.0 || insert_start.elapsed().as_secs_f64() >= args.insert_seconds
        {
            break g;
        }
    };
    let insert_s = insert_start.elapsed().as_secs_f64();
    let insert_qps = (args.rows as u64 * insert_passes) as f64 / insert_s;
    // insert_core excludes graph allocation + teardown, isolating the insertion
    // algorithm from the per-build alloc/free cost.
    let insert_core_s = insert_core.as_secs_f64();
    let insert_core_qps = (args.rows as u64 * insert_passes) as f64 / insert_core_s;

    // Sustained read: run the query workload in a loop for `query_seconds`
    // (or `query_repeats` passes when 0).
    let search_query_ids = query_ids(&args, args.queries);
    let query_start = Instant::now();
    // Assigned on every loop iteration before any break; the loop always runs at
    // least once, so no dead initial store.
    let mut hits: usize;
    let mut query_passes: u64 = 0;
    loop {
        hits = search_query_ids
            .par_iter()
            .map(|row| {
                let query = snapshot.vector(*row as u32);
                let results = graph
                    .search(query, SearchParams::new(args.k, args.ef_search), &snapshot)
                    .expect("search should succeed");
                usize::from(results.iter().any(|result| result.id as usize == *row))
            })
            .sum();
        query_passes += 1;
        if args.query_seconds > 0.0 {
            if query_start.elapsed().as_secs_f64() >= args.query_seconds {
                break;
            }
        } else if query_passes >= args.query_repeats as u64 {
            break;
        }
    }
    let query_s = query_start.elapsed().as_secs_f64();
    let query_qps = (args.queries as u64 * query_passes) as f64 / query_s;
    let self_recall = hits as f64 / args.queries as f64;

    let truth_query_ids = query_ids(&args, args.truth_queries);
    let truth_start = Instant::now();
    let recall_hits: usize = truth_query_ids
        .par_iter()
        .map(|row| {
            let query = snapshot.vector(*row as u32);
            let truth = exact_top_k(query, args.k, &snapshot);
            let results = graph
                .search(query, SearchParams::new(args.k, args.ef_search), &snapshot)
                .expect("search should succeed");
            results
                .iter()
                .filter(|result| truth.contains(&result.id))
                .count()
        })
        .sum();
    let truth_s = truth_start.elapsed().as_secs_f64();
    let recall_at_k = recall_hits as f64 / (args.truth_queries * args.k) as f64;

    println!(
        "result impl=lance_hnsw rows={} dim={} generate_s={:.6} arrow_s={:.6} insert_s={:.6} insert_passes={} insert_qps={:.3} insert_core_s={:.6} insert_core_qps={:.3} query_s={:.6} query_passes={} query_qps={:.3} truth_s={:.6} recall_at_{}={:.6} self_recall_at_{}={:.6}",
        args.rows,
        args.dim,
        generate_s,
        arrow_s,
        insert_s,
        insert_passes,
        insert_qps,
        insert_core_s,
        insert_core_qps,
        query_s,
        query_passes,
        query_qps,
        truth_s,
        args.k,
        recall_at_k,
        args.k,
        self_recall
    );
    println!(
        "{{\"impl\":\"lance_hnsw\",\"rows\":{},\"dim\":{},\"queries\":{},\"truth_queries\":{},\"k\":{},\"m\":{},\"ef_construction\":{},\"ef_search\":{},\"threads\":{},\"generate_s\":{},\"arrow_s\":{},\"insert_s\":{},\"insert_qps\":{},\"query_s\":{},\"query_qps\":{},\"truth_s\":{},\"recall_at_k\":{},\"self_recall_at_k\":{}}}",
        args.rows,
        args.dim,
        args.queries,
        args.truth_queries,
        args.k,
        args.m,
        args.ef_construction,
        args.ef_search,
        rayon::current_num_threads(),
        generate_s,
        arrow_s,
        insert_s,
        insert_qps,
        query_s,
        query_qps,
        truth_s,
        recall_at_k,
        self_recall
    );
    Ok(())
}

fn parse_args() -> Result<Args, Box<dyn Error>> {
    let mut args = Args::default();
    let mut iter = std::env::args().skip(1);
    while let Some(flag) = iter.next() {
        let value = iter
            .next()
            .ok_or_else(|| format!("missing value for argument {flag}"))?;
        match flag.as_str() {
            "--rows" => args.rows = value.parse()?,
            "--dim" => args.dim = value.parse()?,
            "--queries" => args.queries = value.parse()?,
            "--truth-queries" => args.truth_queries = value.parse()?,
            "--k" => args.k = value.parse()?,
            "--m" => args.m = value.parse()?,
            "--ef-construction" => args.ef_construction = value.parse()?,
            "--ef-search" => args.ef_search = value.parse()?,
            "--threads" => args.threads = value.parse()?,
            "--seed" => args.seed = value.parse()?,
            "--clusters" => args.clusters = value.parse()?,
            "--noise" => args.noise = value.parse()?,
            "--query-repeats" => args.query_repeats = value.parse()?,
            "--insert-seconds" => args.insert_seconds = value.parse()?,
            "--query-seconds" => args.query_seconds = value.parse()?,
            _ => return Err(format!("unknown argument: {flag}").into()),
        }
    }
    if args.rows == 0
        || args.dim == 0
        || args.queries == 0
        || args.truth_queries == 0
        || args.k == 0
    {
        return Err("rows, dim, queries, truth_queries, and k must be greater than 0".into());
    }
    if args.clusters == 0 {
        return Err("clusters must be greater than 0".into());
    }
    Ok(args)
}

fn generate_vectors(args: &Args) -> Result<Vec<f32>, Box<dyn Error>> {
    let total = args
        .rows
        .checked_mul(args.dim)
        .ok_or("rows * dim overflow")?;
    let mut values = Vec::with_capacity(total);
    for row in 0..args.rows {
        for col in 0..args.dim {
            values.push(vector_value(row, col, args));
        }
    }
    Ok(values)
}

fn query_ids(args: &Args, count: usize) -> Vec<usize> {
    (0..count)
        .map(|idx| {
            splitmix64(args.seed ^ (idx as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)) as usize
                % args.rows
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ExactNeighbor {
    id: u32,
    distance: f32,
}

impl Eq for ExactNeighbor {}

impl PartialOrd for ExactNeighbor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ExactNeighbor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance.total_cmp(&other.distance)
    }
}

fn exact_top_k(query: &[f32], k: usize, vectors: &impl VectorSource) -> Vec<u32> {
    let mut heap = std::collections::BinaryHeap::with_capacity(k);
    for id in 0..vectors.len() as u32 {
        let candidate = ExactNeighbor {
            id,
            distance: vectors.distance_to(query, id),
        };
        if heap.len() < k {
            heap.push(candidate);
        } else if let Some(top) = heap.peek()
            && candidate.distance < top.distance
        {
            heap.pop();
            heap.push(candidate);
        }
    }
    heap.into_sorted_vec()
        .into_iter()
        .map(|point| point.id)
        .collect()
}

fn vector_value(row: usize, col: usize, args: &Args) -> f32 {
    let cluster = row % args.clusters;
    let base_key = args.seed
        ^ (cluster as u64).wrapping_mul(0xbf58_476d_1ce4_e5b9)
        ^ (col as u64).wrapping_mul(0x94d0_49bb_1331_11eb);
    let noise_key = args.seed
        ^ (row as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ (col as u64).wrapping_mul(0xd2b7_4407_b1ce_6e93);
    let base = unit_f32(base_key) * 2.0 - 1.0;
    let noise = (unit_f32(noise_key) * 2.0 - 1.0) * args.noise;
    base + noise
}

fn unit_f32(key: u64) -> f32 {
    let bits = splitmix64(key) >> 40;
    (bits as f32) * (1.0 / 16_777_216.0)
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
