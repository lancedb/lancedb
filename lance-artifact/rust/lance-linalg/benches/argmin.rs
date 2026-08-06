// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{sync::Arc, time::Duration};

use arrow_array::types::Float32Type;
use arrow_array::{Float32Array, UInt32Array};
use criterion::{Criterion, criterion_group, criterion_main};
use lance_linalg::kernels::argmin_opt;
use lance_testing::datagen::generate_random_array_with_seed;

#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};

#[inline]
fn argmin_arrow(x: &Float32Array) -> u32 {
    argmin_opt(x.iter()).unwrap()
}

fn argmin_arrow_batch(x: &Float32Array, dimension: usize) -> Arc<UInt32Array> {
    assert_eq!(x.len() % dimension, 0);

    let idxs = unsafe {
        UInt32Array::from_trusted_len_iter(
            (0..x.len())
                .step_by(dimension)
                .map(|start| Some(argmin_arrow(&x.slice(start, dimension)))),
        )
    };
    Arc::new(idxs)
}

fn bench_argmin(c: &mut Criterion) {
    const DIMENSION: usize = 1024 * 8;
    const TOTAL: usize = 1024;
    const SEED: [u8; 32] = [42; 32];

    let target = generate_random_array_with_seed::<Float32Type>(TOTAL * DIMENSION, SEED);

    c.bench_function("argmin(arrow)", |b| {
        b.iter(|| {
            argmin_arrow_batch(&target, DIMENSION);
        })
    });
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(32)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_argmin);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(32);
    targets = bench_argmin);

criterion_main!(benches);
