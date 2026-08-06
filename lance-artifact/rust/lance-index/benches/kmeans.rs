// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow::array::AsArray;
use arrow::datatypes::Float32Type;
use arrow_array::FixedSizeListArray;
use criterion::{Criterion, criterion_group, criterion_main};

use lance_arrow::FixedSizeListArrayExt;
use lance_index::vector::utils::SimpleIndex;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};

use lance_index::vector::kmeans::{
    KMeans, KMeansAlgo, KMeansAlgoFloat, KMeansParams, compute_partitions_arrow_array,
};
use lance_linalg::distance::DistanceType;
use lance_testing::datagen::generate_random_array;

fn bench_train(c: &mut Criterion) {
    let params = [
        // (64 * 1024, 8),      // training PQ
        // (64 * 1024, 128),    // training IVF with small vectors (1M rows)
        // (64 * 1024, 1024),   // training IVF with large vectors (1M rows)
        // (256 * 1024, 1024),  // hit the threshold for using HNSW to speed up
        // (256 * 2048, 1024),  // hit the threshold for using HNSW to speed up
        // (256 * 4096, 1024),  // hit the threshold for using HNSW to speed up
        (256 * 16384, 1024), // hit the threshold for using HNSW to speed up
    ];
    for (n, dimension) in params {
        let k = n / 256;

        let values = generate_random_array(n * dimension as usize);
        let data = FixedSizeListArray::try_new_from_values(values, dimension).unwrap();

        let values = generate_random_array(k * dimension as usize);
        let centroids = FixedSizeListArray::try_new_from_values(values, dimension).unwrap();

        c.bench_function(&format!("train_{}d_{}k", dimension, k), |b| {
            let params = KMeansParams::default().with_hierarchical_k(0);
            b.iter(|| {
                KMeans::new_with_params(&data, k, &params).ok().unwrap();
            })
        });

        if k > 256 {
            for hierarchical_k in [4, 8, 16, 24, 32] {
                let params = KMeansParams::default().with_hierarchical_k(hierarchical_k);
                c.bench_function(
                    &format!(
                        "train_{}d_{}k_hierarchical_{}",
                        dimension, k, hierarchical_k
                    ),
                    |b| {
                        b.iter(|| KMeans::new_with_params(&data, k, &params).ok().unwrap());
                    },
                );
            }
        }

        let mut group = c.benchmark_group(format!("compute_membership_{}d_{}k", dimension, k));

        group.bench_function("flat", |b| {
            b.iter(|| compute_partitions_arrow_array(&centroids, &data, DistanceType::L2))
        });

        if k * dimension as usize >= 1_000_000 {
            let index = SimpleIndex::may_train_index(
                centroids.values().clone(),
                dimension as usize,
                DistanceType::L2,
            )
            .unwrap()
            .unwrap();
            group.bench_function("with_index", |b| {
                b.iter(|| {
                    KMeansAlgoFloat::<Float32Type>::compute_membership_and_loss(
                        centroids.values().as_primitive::<Float32Type>().values(),
                        data.values().as_primitive::<Float32Type>().values(),
                        dimension as usize,
                        DistanceType::L2,
                        0.0,
                        None,
                        Some(&index),
                    )
                })
            });
        }
    }
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
    .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_train);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_train);
criterion_main!(benches);
