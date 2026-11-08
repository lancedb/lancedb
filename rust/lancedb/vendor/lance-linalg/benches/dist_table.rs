// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark of 4-bit LUT distance table summation (RaBitQ inner loop).
//!
//! Measures both the dispatched path (NEON on ARM, AVX2 on x86) and the
//! scalar fallback, so the speedup is visible in a single benchmark run.

use std::iter::repeat_with;

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use lance_linalg::simd::dist_table::{BATCH_SIZE, sum_4bit_dist_table, sum_4bit_dist_table_scalar};
use rand::Rng;

fn bench_sum_4bit_dist_table(c: &mut Criterion) {
    let mut rng = rand::rng();

    // code_len = dim / 8 for 1-bit quantization
    for (label, n_vectors, code_len) in [
        ("32vec_dim128", 32_usize, 16_usize),
        ("32vec_dim1536", 32, 192),
        ("32vec_dim4096", 32, 512),
        ("32vec_dim65536", 32, 8192),
        ("16Kvec_dim128", 16_000, 16),
        ("16Kvec_dim1536", 16_000, 192),
    ] {
        let n = n_vectors.div_ceil(BATCH_SIZE) * BATCH_SIZE;

        let codes: Vec<u8> = repeat_with(|| rng.random::<u8>())
            .take(n * code_len)
            .collect();

        let dist_table: Vec<u8> = repeat_with(|| rng.random::<u8>())
            .take(BATCH_SIZE * code_len)
            .collect();

        let mut dists = vec![0u16; n];

        // Dispatched path (NEON on ARM, AVX2 on x86)
        c.bench_function(&format!("sum_4bit_dist_table/simd/{}", label), |b| {
            b.iter(|| {
                dists.fill(0);
                sum_4bit_dist_table(n, code_len, &codes, &dist_table, &mut dists);
                black_box(&dists);
            })
        });

        // Scalar reference path
        c.bench_function(&format!("sum_4bit_dist_table/scalar/{}", label), |b| {
            b.iter(|| {
                dists.fill(0);
                sum_4bit_dist_table_scalar(code_len, &codes, &dist_table, &mut dists);
                black_box(&dists);
            })
        });
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_sum_4bit_dist_table
);
criterion_main!(benches);
