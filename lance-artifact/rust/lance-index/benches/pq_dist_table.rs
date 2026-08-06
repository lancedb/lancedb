// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark of building PQ distance table.

use std::iter::repeat_n;

use arrow_array::types::{Float16Type, Float32Type, Float64Type};
use arrow_array::{FixedSizeListArray, UInt8Array};
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use lance_arrow::{ArrowFloatType, FixedSizeListArrayExt, FloatArray};
use lance_index::vector::pq::ProductQuantizer;
use lance_index::vector::pq::distance::*;
use lance_linalg::distance::{DistanceType, Dot, L2};
use lance_testing::datagen::generate_random_array_with_seed;
use rand::{Rng, SeedableRng, prelude::StdRng};

#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};

const DIM: usize = 128;
const PQ: usize = DIM / 8;
const TOTAL: usize = 16 * 1000;

fn construct_dist_table(c: &mut Criterion) {
    construct_dist_table_for_type::<Float16Type>(c, "f16");
    construct_dist_table_for_type::<Float32Type>(c, "f32");
    construct_dist_table_for_type::<Float64Type>(c, "f64");
}

fn construct_dist_table_for_type<T: ArrowFloatType>(c: &mut Criterion, type_name: &str)
where
    T::Native: L2 + Dot,
    T::ArrayType: FloatArray<T>,
{
    let codebook = generate_random_array_with_seed::<T>(256 * DIM, [88; 32]);
    let query = generate_random_array_with_seed::<T>(DIM, [32; 32]);

    c.bench_function(
        format!(
            "construct_dist_table: {},PQ={},DIM={},type={}",
            DistanceType::L2,
            PQ,
            DIM,
            type_name
        )
        .as_str(),
        |b| {
            b.iter(|| {
                black_box(build_distance_table_l2(
                    codebook.as_slice(),
                    8,
                    PQ,
                    query.as_slice(),
                ));
            })
        },
    );

    c.bench_function(
        format!(
            "construct_dist_table: {},PQ={},DIM={},type={}",
            DistanceType::Dot,
            PQ,
            DIM,
            type_name
        )
        .as_str(),
        |b| {
            b.iter(|| {
                black_box(build_distance_table_dot(
                    codebook.as_slice(),
                    8,
                    PQ,
                    query.as_slice(),
                ));
            })
        },
    );
}

fn compute_distances(c: &mut Criterion) {
    compute_distances_for_type::<Float16Type>(c, "f16");
    compute_distances_for_type::<Float32Type>(c, "f32");
    compute_distances_for_type::<Float64Type>(c, "f64");
}

fn compute_distances_for_type<T: ArrowFloatType>(c: &mut Criterion, type_name: &str)
where
    T::Native: L2 + Dot,
    T::ArrayType: FloatArray<T>,
{
    let codebook = generate_random_array_with_seed::<T>(256 * DIM, [88; 32]);
    let query = generate_random_array_with_seed::<T>(DIM, [32; 32]);

    let mut rnd = StdRng::from_seed([32; 32]);
    let code = UInt8Array::from_iter_values(repeat_n(rnd.random::<u8>(), TOTAL * PQ));

    for dt in [DistanceType::L2, DistanceType::Cosine, DistanceType::Dot] {
        let pq = ProductQuantizer::new(
            PQ,
            8,
            DIM,
            FixedSizeListArray::try_new_from_values(codebook.clone(), DIM as i32).unwrap(),
            dt,
        );

        c.bench_function(
            format!(
                "compute_distances: {},{},PQ={},DIM={},type={}",
                TOTAL, dt, PQ, DIM, type_name
            )
            .as_str(),
            |b| {
                b.iter(|| {
                    black_box(pq.compute_distances(&query, &code).unwrap());
                })
            },
        );
    }
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = construct_dist_table, compute_distances);

#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = construct_dist_table, compute_distances);

criterion_main!(benches);
