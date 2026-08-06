// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::types::Float32Type;
use criterion::{Criterion, criterion_group, criterion_main};
use lance_index::vector::kmeans::{KMeansAlgoFloat, compute_partitions};
use lance_linalg::distance::MetricType;
use lance_testing::datagen::generate_random_array_with_seed;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};

fn bench_compute_partitions(c: &mut Criterion) {
    const K: usize = 1024 * 4;
    const DIMENSION: usize = 1536;
    const INPUT_SIZE: usize = 10240;
    const SEED: [u8; 32] = [42; 32];

    let centroids = Arc::new(generate_random_array_with_seed::<Float32Type>(
        K * DIMENSION,
        SEED,
    ));
    let input = generate_random_array_with_seed::<Float32Type>(INPUT_SIZE * DIMENSION, SEED);

    c.bench_function("compute_centroids(L2)", |b| {
        b.iter(|| {
            compute_partitions::<Float32Type, KMeansAlgoFloat<Float32Type>>(
                centroids.as_ref(),
                &input,
                DIMENSION,
                MetricType::L2,
            )
        })
    });

    c.bench_function("compute_centroids(Cosine)", |b| {
        b.iter(|| {
            compute_partitions::<Float32Type, KMeansAlgoFloat<Float32Type>>(
                centroids.as_ref(),
                &input,
                DIMENSION,
                MetricType::Cosine,
            )
        })
    });
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default()
    .sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_compute_partitions);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().sample_size(10);
    targets = bench_compute_partitions);

criterion_main!(benches);
