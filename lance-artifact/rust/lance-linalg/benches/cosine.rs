// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_array::{
    Float32Array,
    types::{Float16Type, Float32Type, Float64Type},
};
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use lance_arrow::{ArrowFloatType, FloatArray, bfloat16::BFloat16Type};
use lance_linalg::distance::cosine::{Cosine, cosine_distance_batch};
use lance_linalg::distance::cosine_u8::{cosine_u8, cosine_u8_scalar};
use num_traits::Float;

#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};

use lance_testing::datagen::generate_random_array_with_seed;

fn cosine_scalar<T: Float>(x: &[T], y: &[T], dim: usize) -> Vec<T> {
    y.chunks_exact(dim)
        .map(|vec| {
            let mut dot = T::zero();
            let mut x_norm = T::zero();
            let mut y_norm = T::zero();

            for (&xi, &yi) in x.iter().zip(vec.iter()) {
                dot = dot + xi * yi;
                x_norm = x_norm + xi * xi;
                y_norm = y_norm + yi * yi;
            }
            dot / (x_norm * y_norm).sqrt()
        })
        .collect()
}

fn run_bench<T: ArrowFloatType>(c: &mut Criterion)
where
    T::Native: Cosine,
{
    const DIMENSION: usize = 1024;
    const TOTAL: usize = 512 * 1024;

    let type_name = std::any::type_name::<T::Native>();
    let key = generate_random_array_with_seed::<T>(DIMENSION, [0; 32]);
    let target = generate_random_array_with_seed::<T>(TOTAL * DIMENSION, [42; 32]);

    c.bench_function(format!("Cosine({}, scalar)", type_name).as_str(), |b| {
        b.iter(|| {
            black_box(cosine_scalar(key.as_slice(), target.as_slice(), DIMENSION));
        })
    });

    c.bench_function(
        format!("Cosine({}, auto-vectorized)", type_name).as_str(),
        |b| {
            b.iter(|| {
                black_box(
                    cosine_distance_batch(key.as_slice(), target.as_slice(), DIMENSION)
                        .collect::<Vec<_>>(),
                );
            })
        },
    );
}

fn bench_distance(c: &mut Criterion) {
    run_bench::<BFloat16Type>(c);
    run_bench::<Float16Type>(c);
    run_bench::<Float32Type>(c);
    run_bench::<Float64Type>(c);

    let key: Float32Array = generate_random_array_with_seed::<Float32Type>(8, [0; 32]);
    let target = generate_random_array_with_seed::<Float32Type>(1024 * 1024 * 8, [42; 32]);

    c.bench_function("Cosine(simd,f32x8) rng seed", |b| {
        b.iter(|| {
            black_box(cosine_distance_batch(key.values(), target.values(), 8).collect::<Vec<_>>())
        })
    });

    // u8 cosine benchmarks
    {
        use rand::Rng;
        use std::iter::repeat_with;

        const DIMENSION: usize = 1024;
        const TOTAL: usize = 1024 * 1024;
        let mut rng = rand::rng();
        let key_u8: Vec<u8> = repeat_with(|| rng.random()).take(DIMENSION).collect();
        let target_u8: Vec<u8> = repeat_with(|| rng.random())
            .take(TOTAL * DIMENSION)
            .collect();

        c.bench_function("Cosine(u8, scalar)", |b| {
            b.iter(|| {
                black_box(
                    target_u8
                        .chunks_exact(DIMENSION)
                        .map(|tgt| cosine_u8_scalar(&key_u8, tgt))
                        .fold(0.0, |acc: f32, v| acc + v),
                );
            });
        });

        c.bench_function("Cosine(u8, SIMD)", |b| {
            b.iter(|| {
                black_box(
                    target_u8
                        .chunks_exact(DIMENSION)
                        .map(|tgt| cosine_u8(&key_u8, tgt))
                        .fold(0.0, |acc: f32, v| acc + v),
                );
            });
        });
    }
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_distance);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_distance);
criterion_main!(benches);
