// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark of HNSW graph.
//!
//!

use std::{collections::HashSet, sync::Arc, time::Duration};

use arrow_array::{FixedSizeListArray, RecordBatch, UInt64Array, types::Float32Type};
use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use lance_arrow::FixedSizeListArrayExt;
use lance_index::vector::v3::subindex::IvfSubIndex;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};
use rayon::ThreadPoolBuilder;

use lance_core::ROW_ID_FIELD;
use lance_index::vector::{
    flat::storage::FlatFloatStorage,
    hnsw::builder::{HNSW, HnswBuildParams, HnswQueryParams},
    pq::{PQBuildParams, ProductQuantizer},
    quantizer::Quantization,
    sq::{ScalarQuantizer, builder::SQBuildParams},
    storage::StorageBuilder,
};
use lance_linalg::distance::DistanceType;
use lance_testing::datagen::generate_random_array_with_seed;

fn bench_hnsw(c: &mut Criterion) {
    const DIMENSION: usize = 128;
    const TOTAL: usize = 100_000;
    const SEED: [u8; 32] = [42; 32];
    const K: usize = 100;

    let rt = tokio::runtime::Runtime::new().unwrap();

    let data = generate_random_array_with_seed::<Float32Type>(TOTAL * DIMENSION, SEED);
    let fsl = FixedSizeListArray::try_new_from_values(data, DIMENSION as i32).unwrap();
    let vectors = Arc::new(FlatFloatStorage::new(fsl.clone(), DistanceType::L2));

    let query = fsl.value(0);
    c.bench_function(format!("create_hnsw({TOTAL}x{DIMENSION})").as_str(), |b| {
        b.to_async(&rt).iter(|| async {
            let hnsw = HNSW::index_vectors(vectors.as_ref(), HnswBuildParams::default()).unwrap();
            let uids: HashSet<u32> = hnsw
                .search_basic(
                    query.clone(),
                    K,
                    &HnswQueryParams {
                        ef: 300,
                        lower_bound: None,
                        upper_bound: None,
                        dist_q_c: 0.0,
                        use_acorn: false,
                    },
                    None,
                    vectors.as_ref(),
                )
                .unwrap()
                .iter()
                .map(|node| node.id)
                .collect();

            assert_eq!(uids.len(), K);
        })
    });

    let search_build_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    let hnsw = search_build_pool
        .install(|| HNSW::index_vectors(vectors.as_ref(), HnswBuildParams::default()))
        .unwrap();
    c.bench_function(format!("search_hnsw{TOTAL}x{DIMENSION}").as_str(), |b| {
        b.to_async(&rt).iter(|| async {
            let uids: HashSet<u32> = hnsw
                .search_basic(
                    query.clone(),
                    K,
                    &HnswQueryParams {
                        ef: 300,
                        lower_bound: None,
                        upper_bound: None,
                        dist_q_c: 0.0,
                        use_acorn: false,
                    },
                    None,
                    vectors.as_ref(),
                )
                .unwrap()
                .iter()
                .map(|node| node.id)
                .collect();

            assert_eq!(uids.len(), K);
        })
    });
}

fn bench_hnsw_load(c: &mut Criterion) {
    const DIMENSION: usize = 128;
    const TOTAL: usize = 100_000;
    const SEED: [u8; 32] = [42; 32];
    const K: usize = 100;

    let rt = tokio::runtime::Runtime::new().unwrap();

    let data = generate_random_array_with_seed::<Float32Type>(TOTAL * DIMENSION, SEED);
    let fsl = FixedSizeListArray::try_new_from_values(data, DIMENSION as i32).unwrap();
    let vectors = Arc::new(FlatFloatStorage::new(fsl.clone(), DistanceType::L2));

    let search_build_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    let hnsw = search_build_pool
        .install(|| HNSW::index_vectors(vectors.as_ref(), HnswBuildParams::default()))
        .unwrap();
    let batch = hnsw.to_batch().unwrap();

    // Load cost -- the path #6746 targets. `RecordBatch::clone` is an Arrow
    // refcount bump (what production does anyway: each partition-cache IPC
    // read yields a fresh batch), so it does not mask the load work measured.
    c.bench_function(format!("load_hnsw({TOTAL}x{DIMENSION})").as_str(), |b| {
        b.iter(|| {
            let loaded = HNSW::load(batch.clone()).unwrap();
            assert_eq!(loaded.len(), TOTAL);
        })
    });

    // Search on the Arrow-backed loaded graph -- same TOTAL/DIMENSION/K/ef as
    // the `search_hnsw` bench, so the two are directly comparable and confirm
    // the new backend keeps search latency unchanged (issue #6746).
    let loaded = HNSW::load(batch).unwrap();
    let query = fsl.value(0);
    c.bench_function(
        format!("search_hnsw_loaded{TOTAL}x{DIMENSION}").as_str(),
        |b| {
            b.to_async(&rt).iter(|| async {
                let uids: HashSet<u32> = loaded
                    .search_basic(
                        query.clone(),
                        K,
                        &HnswQueryParams {
                            ef: 300,
                            lower_bound: None,
                            upper_bound: None,
                            dist_q_c: 0.0,
                            use_acorn: false,
                        },
                        None,
                        vectors.as_ref(),
                    )
                    .unwrap()
                    .iter()
                    .map(|node| node.id)
                    .collect();

                assert_eq!(uids.len(), K);
            })
        },
    );
}

fn bench_hnsw_sq(c: &mut Criterion) {
    const DIMENSION: usize = 128;
    const TOTAL: usize = 100_000;
    const SEED: [u8; 32] = [42; 32];
    const K: usize = 100;

    let rt = tokio::runtime::Runtime::new().unwrap();

    let data = generate_random_array_with_seed::<Float32Type>(TOTAL * DIMENSION, SEED);
    let fsl = FixedSizeListArray::try_new_from_values(data, DIMENSION as i32).unwrap();
    let quantizer =
        <ScalarQuantizer as Quantization>::build(&fsl, DistanceType::L2, &SQBuildParams::default())
            .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Field::new_list_field(DataType::Float32, true).into(),
                DIMENSION as i32,
            ),
            true,
        ),
        ROW_ID_FIELD.clone(),
    ]));
    let row_ids = UInt64Array::from_iter_values((0..TOTAL).map(|v| v as u64));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(fsl.clone()), Arc::new(row_ids)]).unwrap();
    let sq_storage = StorageBuilder::new("vector".to_owned(), DistanceType::L2, quantizer, None)
        .unwrap()
        .build(vec![batch])
        .unwrap();
    let vectors = Arc::new(sq_storage);

    let query = fsl.value(0);
    c.bench_function(
        format!("create_hnsw_sq({TOTAL}x{DIMENSION})").as_str(),
        |b| {
            b.to_async(&rt).iter(|| async {
                let hnsw =
                    HNSW::index_vectors(vectors.as_ref(), HnswBuildParams::default()).unwrap();
                let uids: HashSet<u32> = hnsw
                    .search_basic(
                        query.clone(),
                        K,
                        &HnswQueryParams {
                            ef: 300,
                            lower_bound: None,
                            upper_bound: None,
                            dist_q_c: 0.0,
                            use_acorn: false,
                        },
                        None,
                        vectors.as_ref(),
                    )
                    .unwrap()
                    .iter()
                    .map(|node| node.id)
                    .collect();

                assert_eq!(uids.len(), K);
            })
        },
    );

    let search_build_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    let hnsw = search_build_pool
        .install(|| HNSW::index_vectors(vectors.as_ref(), HnswBuildParams::default()))
        .unwrap();
    c.bench_function(format!("search_hnsw_sq{TOTAL}x{DIMENSION}").as_str(), |b| {
        b.to_async(&rt).iter(|| async {
            let uids: HashSet<u32> = hnsw
                .search_basic(
                    query.clone(),
                    K,
                    &HnswQueryParams {
                        ef: 300,
                        lower_bound: None,
                        upper_bound: None,
                        dist_q_c: 0.0,
                        use_acorn: false,
                    },
                    None,
                    vectors.as_ref(),
                )
                .unwrap()
                .iter()
                .map(|node| node.id)
                .collect();

            assert_eq!(uids.len(), K);
        })
    });
}

fn bench_hnsw_pq(c: &mut Criterion) {
    const DIMENSION: usize = 128;
    const TOTAL: usize = 100_000;
    const SEED: [u8; 32] = [42; 32];
    const K: usize = 100;

    let rt = tokio::runtime::Runtime::new().unwrap();

    let data = generate_random_array_with_seed::<Float32Type>(TOTAL * DIMENSION, SEED);
    let fsl = FixedSizeListArray::try_new_from_values(data, DIMENSION as i32).unwrap();
    let quantizer = <ProductQuantizer as Quantization>::build(
        &fsl,
        DistanceType::L2,
        &PQBuildParams::new(16, 8),
    )
    .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Field::new_list_field(DataType::Float32, true).into(),
                DIMENSION as i32,
            ),
            true,
        ),
        ROW_ID_FIELD.clone(),
    ]));
    let row_ids = UInt64Array::from_iter_values((0..TOTAL).map(|v| v as u64));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(fsl.clone()), Arc::new(row_ids)]).unwrap();
    let pq_storage = StorageBuilder::new("vector".to_owned(), DistanceType::L2, quantizer, None)
        .unwrap()
        .build(vec![batch])
        .unwrap();
    let vectors = Arc::new(pq_storage);

    let query = fsl.value(0);
    c.bench_function(
        format!("create_hnsw_pq({TOTAL}x{DIMENSION})").as_str(),
        |b| {
            b.to_async(&rt).iter(|| async {
                let hnsw =
                    HNSW::index_vectors(vectors.as_ref(), HnswBuildParams::default()).unwrap();
                let uids: HashSet<u32> = hnsw
                    .search_basic(
                        query.clone(),
                        K,
                        &HnswQueryParams {
                            ef: 300,
                            lower_bound: None,
                            upper_bound: None,
                            dist_q_c: 0.0,
                            use_acorn: false,
                        },
                        None,
                        vectors.as_ref(),
                    )
                    .unwrap()
                    .iter()
                    .map(|node| node.id)
                    .collect();

                assert_eq!(uids.len(), K);
            })
        },
    );

    let search_build_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    let hnsw = search_build_pool
        .install(|| HNSW::index_vectors(vectors.as_ref(), HnswBuildParams::default()))
        .unwrap();
    c.bench_function(format!("search_hnsw_pq{TOTAL}x{DIMENSION}").as_str(), |b| {
        b.to_async(&rt).iter(|| async {
            let uids: HashSet<u32> = hnsw
                .search_basic(
                    query.clone(),
                    K,
                    &HnswQueryParams {
                        ef: 300,
                        lower_bound: None,
                        upper_bound: None,
                        dist_q_c: 0.0,
                        use_acorn: false,
                    },
                    None,
                    vectors.as_ref(),
                )
                .unwrap()
                .iter()
                .map(|node| node.id)
                .collect();

            assert_eq!(uids.len(), K);
        })
    });
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_hnsw, bench_hnsw_load, bench_hnsw_sq, bench_hnsw_pq);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10);
    targets = bench_hnsw, bench_hnsw_load, bench_hnsw_sq, bench_hnsw_pq);

criterion_main!(benches);
