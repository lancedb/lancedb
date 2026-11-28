// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::Rng;

use lance_linalg::distance::hamming::{
    BinaryHashValues, hamming_batch_u64, hamming_distance_batch_into,
    pairwise_hamming_distance_binary_parallel, pairwise_hamming_distance_parallel,
};

#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};

const NUM_VECTORS: usize = 16 * 1024;
const NUM_TARGETS: usize = 10_000;
const NUM_HASHES: usize = 5_000;
const THRESHOLD: u32 = 10;

fn bench_hamming_u8(c: &mut Criterion) {
    let mut rng = rand::rng();
    let mut group = c.benchmark_group("hamming_u8_batch");
    group.throughput(Throughput::Elements(NUM_VECTORS as u64));
    for dimension in [16, 32, 64, 128, 256] {
        let query: Vec<u8> = (0..dimension).map(|_| rng.random()).collect();
        let vectors: Vec<u8> = (0..dimension * NUM_VECTORS).map(|_| rng.random()).collect();
        let mut distances = vec![0.0; NUM_VECTORS];
        group.bench_function(BenchmarkId::from_parameter(dimension), |b| {
            b.iter(|| {
                hamming_distance_batch_into(
                    black_box(&query),
                    black_box(&vectors),
                    dimension,
                    &mut distances,
                );
                black_box(&distances);
            })
        });
    }
    group.finish();
}

fn bench_hamming_batch_u64(c: &mut Criterion) {
    let mut rng = rand::rng();
    let targets: Vec<u64> = (0..NUM_TARGETS).map(|_| rng.random()).collect();
    let mut results = vec![0; NUM_TARGETS];
    let mut group = c.benchmark_group("hamming_batch_u64");
    group.throughput(Throughput::Elements(NUM_TARGETS as u64));
    group.bench_function(BenchmarkId::from_parameter(NUM_TARGETS), |b| {
        b.iter(|| {
            hamming_batch_u64(black_box(rng.random()), black_box(&targets), &mut results);
            black_box(&results);
        })
    });
    group.finish();
}

/// One thread, so the row loop is measured rather than the scheduler.
fn bench_pairwise(c: &mut Criterion) {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    let mut rng = rand::rng();
    let num_pairs = (NUM_HASHES * (NUM_HASHES - 1) / 2) as u64;
    let mut group = c.benchmark_group("hamming_pairwise");
    group.throughput(Throughput::Elements(num_pairs));
    for bits in [64, 128, 256] {
        let num_lanes = bits / 64;
        let lane_values: Vec<u64> = (0..NUM_HASHES * num_lanes).map(|_| rng.random()).collect();
        let hashes = BinaryHashValues::try_new(lane_values, NUM_HASHES, bits / 8).unwrap();
        group.bench_function(BenchmarkId::from_parameter(bits), |b| {
            b.iter(|| {
                pool.install(|| {
                    if num_lanes == 1 {
                        black_box(pairwise_hamming_distance_parallel(
                            hashes.lane(0),
                            None,
                            Some(THRESHOLD),
                        ))
                    } else {
                        black_box(pairwise_hamming_distance_binary_parallel(
                            &hashes,
                            None,
                            Some(THRESHOLD),
                        ))
                    }
                })
            })
        });
    }
    group.finish();
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(20)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_hamming_u8, bench_hamming_batch_u64, bench_pairwise);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(20);
    targets = bench_hamming_u8, bench_hamming_batch_u64, bench_pairwise);

criterion_main!(benches);
