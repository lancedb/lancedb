// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::iter::repeat_with;

use arrow_array::{
    Float32Array,
    types::{Float16Type, Float32Type, Float64Type},
};
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use num_traits::{AsPrimitive, Float};
use rand::Rng;

#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};

use lance_arrow::{ArrowFloatType, FloatArray};
use lance_linalg::distance::l2_u8::l2_u8;
use lance_linalg::distance::{L2, l2::l2, l2_distance_batch, l2_distance_uint_scalar};
use lance_testing::datagen::generate_random_array_with_seed;

const DIMENSION: usize = 1024;
const TOTAL: usize = 1024 * 1024; // 1M vectors

/// Naive scalar implementation of L2 distance.
fn l2_scalar<T: Float + AsPrimitive<f32>>(x: &[T], y: &[T]) -> T {
    let mut sum = T::zero();
    for (&xi, &yi) in x.iter().zip(y.iter()) {
        sum = sum + (xi - yi).powi(2);
    }
    sum.sqrt()
}

fn run_bench<T: ArrowFloatType>(c: &mut Criterion)
where
    T::Native: L2,
{
    const DIMENSION: usize = 1024;
    const TOTAL: usize = 1024 * 1024; // 1M vectors

    let key = generate_random_array_with_seed::<T>(DIMENSION, [0; 32]);
    // 1M of 1024 D vectors
    let target = generate_random_array_with_seed::<T>(TOTAL * DIMENSION, [42; 32]);

    let type_name = std::any::type_name::<T::Native>();

    c.bench_function(format!("L2({type_name}, scalar)").as_str(), |b| {
        b.iter(|| {
            black_box(
                target
                    .as_slice()
                    .chunks(DIMENSION)
                    .map(|arr| l2_scalar(key.as_slice(), arr).as_())
                    .fold(0.0, |acc: f32, v: f32| acc + v),
            )
        });
    });

    c.bench_function(
        format!("L2({type_name}, auto-vectorization)").as_str(),
        |b| {
            b.iter(|| {
                black_box(
                    target
                        .as_slice()
                        .chunks(DIMENSION)
                        .map(|y| black_box(l2(key.as_slice(), y)))
                        .fold(0.0, |acc: f32, v: f32| acc + v),
                );
            });
        },
    );
}

fn bench_distance(c: &mut Criterion) {
    run_bench::<Float16Type>(c);
    run_bench::<Float32Type>(c);
    let key: Float32Array = generate_random_array_with_seed::<Float32Type>(DIMENSION, [0; 32]);
    let target: Float32Array =
        generate_random_array_with_seed::<Float32Type>(TOTAL * DIMENSION, [42; 32]);
    c.bench_function("L2(f32, simd)", |b| {
        b.iter(|| {
            black_box(
                l2_distance_batch(key.values(), target.values(), DIMENSION)
                    .fold(0.0, |acc: f32, v: f32| acc + v),
            );
        })
    });

    run_bench::<Float64Type>(c);
}

fn bench_small_distance(c: &mut Criterion) {
    let key = generate_random_array_with_seed::<Float32Type>(8, [5; 32]);
    // 1M of 1024 D vectors. 4GB in memory.
    let target = generate_random_array_with_seed::<Float32Type>(TOTAL * 8, [7; 32]);
    c.bench_function("L2(simd,f32x8)", |b| {
        b.iter(|| {
            black_box(
                l2_distance_batch(key.values(), target.values(), 8)
                    .fold(0.0, |acc: f32, v: f32| acc + v),
            );
        })
    });
}

fn l2_distance_uint_scalar_auto_vectorized(key: &[u8], target: &[u8]) -> f32 {
    let mut sum = 0;
    const LANE: usize = 16;
    let x_chunks = key.chunks_exact(LANE);
    let y_chunks = target.chunks_exact(LANE);

    let x_reminder = x_chunks.remainder();
    let y_reminder = y_chunks.remainder();

    for (x, y) in x_chunks.zip(y_chunks) {
        let mut s: u32 = 0;
        for i in 0..LANE {
            s += (x[i].abs_diff(y[i]) as u32).pow(2);
        }
        sum += s;
    }
    sum += x_reminder
        .iter()
        .zip(y_reminder)
        .map(|(&x, &y)| (x.abs_diff(y) as u32).pow(2))
        .sum::<u32>();
    sum as f32
}

fn bench_uint_distance(c: &mut Criterion) {
    let mut rng = rand::rng();
    let key = repeat_with(|| rng.random::<u8>())
        .take(DIMENSION)
        .collect::<Vec<_>>();
    let target = repeat_with(|| rng.random::<u8>())
        .take(TOTAL * DIMENSION)
        .collect::<Vec<_>>();

    c.bench_function("L2(uint8, scalar)", |b| {
        b.iter(|| {
            black_box(
                target
                    .chunks_exact(DIMENSION)
                    .map(|tgt| l2_distance_uint_scalar(&key, tgt))
                    .fold(0.0, |acc, v| acc + v),
            );
        });
    });

    c.bench_function("L2(uint8, auto-vectorization)", |b| {
        b.iter(|| {
            black_box(
                target
                    .chunks_exact(DIMENSION)
                    .map(|tgt| l2_distance_uint_scalar_auto_vectorized(&key, tgt))
                    .fold(0.0, |acc, v| acc + v),
            );
        });
    });

    c.bench_function("L2(u8, SIMD)", |b| {
        b.iter(|| {
            black_box(
                target
                    .chunks_exact(DIMENSION)
                    .map(|tgt| l2_u8(&key, tgt) as f32)
                    .fold(0.0, |acc, v| acc + v),
            );
        });
    });
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_distance, bench_small_distance, bench_uint_distance);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_distance, bench_small_distance, bench_uint_distance);
criterion_main!(benches);
