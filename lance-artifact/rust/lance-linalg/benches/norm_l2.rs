// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::iter::repeat_with;

use arrow_array::{
    Float16Array, Float32Array, Float64Array,
    types::{Float16Type, Float32Type, Float64Type},
};
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use half::{bf16, f16};
use num_traits::Float;
use rand::Rng;

use lance_arrow::{ArrowFloatType, FloatArray, bfloat16::BFloat16Type};
use lance_linalg::distance::{norm_l2, norm_l2_f64_simd, norm_l2_impl};
use lance_testing::datagen::generate_random_array_with_seed;

#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};

const DIMENSION: usize = 1024;
const TOTAL: usize = 1024 * 1024; // 1M vectors

#[allow(clippy::type_complexity)]
fn run_bench<T: ArrowFloatType>(
    c: &mut Criterion,
    target: &[T::Native],
    auto_vec_impl: fn(&[T::Native]) -> f32,
    simd_impl: Option<fn(&[T::Native]) -> f32>,
) where
    T::Native: Float,
{
    let type_name = std::any::type_name::<T::Native>();

    c.bench_function(format!("NormL2({type_name}, scalar)").as_str(), |b| {
        b.iter(|| {
            target
                .chunks(DIMENSION)
                .map(|arr| arr.iter().map(|&x| x * x).sum::<T::Native>().sqrt())
                .collect::<Vec<_>>()
        });
    });

    c.bench_function(
        format!("NormL2({type_name}, auto-vectorization)").as_str(),
        |b| {
            b.iter(|| {
                black_box(
                    target
                        .chunks_exact(DIMENSION)
                        .map(auto_vec_impl)
                        .collect::<Vec<_>>(),
                );
            });
        },
    );

    if let Some(simd_impl) = simd_impl {
        c.bench_function(
            format!("NormL2({type_name}, SIMD)", type_name = type_name).as_str(),
            |b| {
                b.iter(|| {
                    black_box(
                        target
                            .chunks_exact(DIMENSION)
                            .map(simd_impl)
                            .collect::<Vec<_>>(),
                    );
                });
            },
        );
    }
}

fn bench_distance(c: &mut Criterion) {
    let mut rng = rand::rng();
    let target = repeat_with(|| rng.random::<u16>())
        .map(bf16::from_bits)
        .take(TOTAL * DIMENSION)
        .collect::<Vec<_>>();
    // We currently don't have a SIMD implementation for bf16.
    run_bench::<BFloat16Type>(c, &target, norm_l2, None);

    let target: Float16Array =
        generate_random_array_with_seed::<Float16Type>(TOTAL * DIMENSION, [42; 32]);
    run_bench::<Float16Type>(
        c,
        target.as_slice(),
        norm_l2_impl::<f16, f32, 32>,
        Some(norm_l2),
    );

    let target: Float32Array =
        generate_random_array_with_seed::<Float32Type>(TOTAL * DIMENSION, [42; 32]);
    run_bench::<Float32Type>(
        c,
        target.as_slice(),
        norm_l2_impl::<f32, f32, 16>,
        Some(norm_l2),
    );

    let target: Float64Array =
        generate_random_array_with_seed::<Float64Type>(TOTAL * DIMENSION, [42; 32]);
    run_bench::<Float64Type>(
        c,
        target.as_slice(),
        norm_l2_impl::<f64, f32, 8>,
        Some(norm_l2_f64_simd),
    );
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
