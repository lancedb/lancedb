// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{
    FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator, cast::as_primitive_array,
};
use arrow_schema::{DataType, Field, FieldRef, Schema as ArrowSchema};
use criterion::{Criterion, criterion_group, criterion_main};
use futures::TryStreamExt;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};
use rand::Rng;

use lance::dataset::{Dataset, WriteMode, WriteParams, builder::DatasetBuilder};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_arrow::{FixedSizeListArrayExt, as_fixed_size_list_array};
use lance_index::{
    IndexType,
    vector::{ivf::IvfBuildParams, pq::PQBuildParams},
};
use lance_linalg::distance::MetricType;

fn bench_ivf_pq_index(c: &mut Criterion) {
    // default tokio runtime
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        create_file(std::path::Path::new("./vec_data.lance"), WriteMode::Create).await
    });
    let dataset = rt.block_on(async { Dataset::open("./vec_data.lance").await.unwrap() });
    let first_batch = rt.block_on(async {
        dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_next()
            .await
            .unwrap()
            .unwrap()
    });

    let mut rng = rand::rng();
    let vector_column = first_batch.column_by_name("vector").unwrap();
    let value =
        as_fixed_size_list_array(&vector_column).value(rng.random_range(0..vector_column.len()));
    let q: &Float32Array = as_primitive_array(&value);

    c.bench_function(
        format!("Flat_Index(d={},top_k=10,nprobes=10)", q.len()).as_str(),
        |b| {
            b.to_async(&rt).iter(|| async {
                let results = dataset
                    .scan()
                    .nearest("vector", q, 10)
                    .unwrap()
                    .minimum_nprobes(10)
                    .try_into_stream()
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                assert!(!results.is_empty());
            })
        },
    );

    c.bench_function(
        format!("Ivf_PQ_Refine(d={},top_k=10,nprobes=10, refine=2)", q.len()).as_str(),
        |b| {
            b.to_async(&rt).iter(|| async {
                let results = dataset
                    .scan()
                    .nearest("vector", q, 10)
                    .unwrap()
                    .minimum_nprobes(10)
                    .refine(2)
                    .try_into_stream()
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                assert!(!results.is_empty());
            })
        },
    );

    // reopen with no index caching to test IO overhead
    let dataset = rt.block_on(async {
        DatasetBuilder::from_uri("./vec_data.lance")
            .with_index_cache_size_bytes(0)
            .load()
            .await
            .unwrap()
    });

    c.bench_function(
        format!(
            "Ivf_PQ_NoCache(d={},top_k=10,nprobes=32, refine=1)",
            q.len()
        )
        .as_str(),
        |b| {
            b.to_async(&rt).iter(|| async {
                let results = dataset
                    .scan()
                    .nearest("vector", q, 10)
                    .unwrap()
                    .minimum_nprobes(32)
                    .try_into_stream()
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                assert!(!results.is_empty());
            })
        },
    );
}

async fn create_file(path: &std::path::Path, mode: WriteMode) {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "vector",
        DataType::FixedSizeList(
            FieldRef::new(Field::new("item", DataType::Float32, true)),
            128,
        ),
        false,
    )]));

    let num_rows = 100_000;
    let batch_size = 10000;
    let batches: Vec<RecordBatch> = (0..(num_rows / batch_size))
        .map(|_| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        create_float32_array(num_rows * 128),
                        128,
                    )
                    .unwrap(),
                )],
            )
            .unwrap()
        })
        .collect();

    let test_uri = path.to_str().unwrap();
    std::fs::remove_dir_all(test_uri).map_or_else(|_| println!("{} not exists", test_uri), |_| {});
    let write_params = WriteParams {
        max_rows_per_file: num_rows as usize,
        max_rows_per_group: batch_size as usize,
        mode,
        ..Default::default()
    };
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let mut dataset = Dataset::write(reader, test_uri, Some(write_params))
        .await
        .unwrap();
    let ivf_params = IvfBuildParams {
        num_partitions: Some(32),
        ..Default::default()
    };
    let pq_params = PQBuildParams {
        num_bits: 8,
        num_sub_vectors: 16,
        ..Default::default()
    };
    let m_type = MetricType::L2;
    let params = VectorIndexParams::with_ivf_pq_params(m_type, ivf_params, pq_params);
    dataset
        .create_index(
            vec!["vector"].as_slice(),
            IndexType::Vector,
            Some("ivf_pq_index".to_string()),
            &params,
            true,
        )
        .await
        .unwrap();
}

fn create_float32_array(num_elements: i32) -> Float32Array {
    // generate an Arrow Float32Array with 10000*128 elements randomly
    let mut rng = rand::rng();
    let mut values = Vec::with_capacity(num_elements as usize);
    for _ in 0..num_elements {
        values.push(rng.random_range(0.0..1.0));
    }
    Float32Array::from(values)
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_ivf_pq_index);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_ivf_pq_index);
criterion_main!(benches);
