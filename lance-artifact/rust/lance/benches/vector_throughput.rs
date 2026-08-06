// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark for IVF_PQ vector search throughput
//!
//! This benchmark measures concurrent vector search performance with IVF_PQ indexes,
//! similar to the Python test_ivf_pq_throughput benchmark.

use std::sync::Arc;

use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, FieldRef, Schema as ArrowSchema};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use futures::{StreamExt, TryStreamExt};
use lance_file::version::LanceFileVersion;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};
use log::info;
use rand::Rng;

use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_arrow::FixedSizeListArrayExt;
use lance_index::{
    IndexType,
    vector::{ivf::IvfBuildParams, pq::PQBuildParams},
};
use lance_linalg::distance::MetricType;
use lance_testing::datagen::generate_random_array;
use tokio::runtime::Runtime;

// Benchmark parameters matching Python test_ivf_pq_throughput
const NUM_ROWS: usize = 1_000_000;
const DIM: usize = 768;
const NUM_QUERIES: usize = 100;
const K: usize = 50;
const NPROBES: usize = 20;
const REFINE_FACTOR: u32 = 10;

// IVF_PQ index parameters
const IVF_PARTITIONS: usize = 256;
const PQ_BITS: usize = 8;
const PQ_SUB_VECTORS: usize = DIM / 16;
const MAX_ITERATIONS: usize = 50;

/// Cached dataset with pre-generated query vectors
struct CachedDataset {
    dataset: Arc<Dataset>,
    query_vectors: Vec<Arc<Float32Array>>,
}

fn dataset_path(version: LanceFileVersion) -> String {
    format!(
        "/tmp/lance_bench_throughput_{}_{}_{}",
        NUM_ROWS, DIM, version
    )
}

/// Get or create a cached dataset with IVF_PQ index and query vectors
fn get_or_create_dataset(rt: &Runtime, version: LanceFileVersion) -> Arc<CachedDataset> {
    // Create dataset in fixed temp directory
    let uri = format!("file://{}", dataset_path(version));

    rt.block_on(async {
        // Check if dataset exists on disk with correct row count
        let mut needs_creation = true;
        let mut needs_indexing = true;

        if let Ok(dataset) = Dataset::open(&uri).await {
            let row_count = dataset.count_rows(None).await.unwrap();
            if row_count == NUM_ROWS {
                info!("Reusing existing dataset at {} ({} rows)", uri, row_count);
                needs_creation = false;

                // Check if index exists
                let indices = dataset.load_indices().await.unwrap();
                if !indices.is_empty() {
                    log::info!(
                        "Dataset already has {} index(es), skipping index creation",
                        indices.len()
                    );
                    needs_indexing = false;
                } else {
                    info!("Dataset exists but has no index, will create index");
                }
            } else {
                info!(
                    "Dataset exists but has wrong row count ({} vs {}), recreating",
                    row_count, NUM_ROWS
                );
                std::fs::remove_dir_all(&uri).ok();
            }
        } else {
            info!(
                "Creating new dataset with {} rows, {} dimensions",
                NUM_ROWS, DIM
            );
        }

        // Create dataset if needed
        if needs_creation {
            create_dataset(&uri).await;
        }

        // Open dataset
        let mut dataset = Dataset::open(&uri).await.unwrap();

        // Create index if needed
        if needs_indexing {
            create_ivf_pq_index(&mut dataset).await;
        }

        // Generate query vectors
        let query_vectors = generate_query_vectors();

        Arc::new(CachedDataset {
            dataset: Arc::new(dataset),
            query_vectors,
        })
    })
}

/// Create a dataset with random vectors
async fn create_dataset(uri: &str) {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "vector",
        DataType::FixedSizeList(
            FieldRef::new(Field::new("item", DataType::Float32, true)),
            DIM as i32,
        ),
        false,
    )]));

    let batch_size = 10_000;
    let batches: Vec<RecordBatch> = (0..(NUM_ROWS / batch_size))
        .map(|_| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        generate_random_array(batch_size * DIM),
                        DIM as i32,
                    )
                    .unwrap(),
                )],
            )
            .unwrap()
        })
        .collect();

    let write_params = WriteParams {
        max_rows_per_file: NUM_ROWS,
        max_rows_per_group: batch_size,
        mode: WriteMode::Create,
        ..Default::default()
    };

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    Dataset::write(reader, uri, Some(write_params))
        .await
        .unwrap();

    info!("Dataset created at {}", uri);
}

/// Create IVF_PQ index on the dataset
async fn create_ivf_pq_index(dataset: &mut Dataset) {
    info!("Creating IVF_PQ index...");

    let ivf_params = IvfBuildParams {
        num_partitions: Some(IVF_PARTITIONS),
        max_iters: MAX_ITERATIONS,
        ..Default::default()
    };
    let pq_params = PQBuildParams {
        num_bits: PQ_BITS,
        num_sub_vectors: PQ_SUB_VECTORS,
        ..Default::default()
    };
    let params = VectorIndexParams::with_ivf_pq_params(MetricType::L2, ivf_params, pq_params);

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

    info!("IVF_PQ index created");
}

/// Generate random query vectors
fn generate_query_vectors() -> Vec<Arc<Float32Array>> {
    let mut rng = rand::rng();
    (0..NUM_QUERIES)
        .map(|_| {
            let values: Vec<f32> = (0..DIM).map(|_| rng.random_range(0.0..1.0)).collect();
            Arc::new(Float32Array::from(values))
        })
        .collect()
}

/// Drop dataset files from OS page cache (Linux only)
#[cfg(target_os = "linux")]
fn drop_dataset_from_cache(dataset_dir: &str) -> std::io::Result<()> {
    use std::fs;
    use std::os::unix::io::AsRawFd;

    // Walk the dataset directory and drop each file from cache
    let mut num_dropped = 0;
    let entries = fs::read_dir(format!("{}/data", dataset_dir)).unwrap();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file()
            && let Ok(file) = fs::File::open(&path)
        {
            let fd = file.as_raw_fd();
            // POSIX_FADV_DONTNEED = 4
            let result = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
            if result != 0 {
                panic!(
                    "Warning: Failed to drop {:?} from cache: {}",
                    path,
                    std::io::Error::from_raw_os_error(result)
                );
            }
            num_dropped += 1;
        }
    }
    if num_dropped == 0 {
        // Sanity check to ensure that we actually dropped some files from cache.
        panic!("No files dropped from cache");
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn drop_dataset_from_cache(_path: &str) -> std::io::Result<()> {
    Ok(())
}

/// Run vector search queries
async fn run_queries(
    dataset: Arc<Dataset>,
    query_vectors: &[Arc<Float32Array>],
    concurrent_queries: usize,
) {
    // Run queries concurrently using tokio tasks
    futures::stream::iter(query_vectors)
        .map(|q| {
            let dataset = dataset.clone();
            let q = q.clone();
            tokio::spawn(async move {
                dataset
                    .scan()
                    .nearest("vector", q.as_ref(), K)
                    .unwrap()
                    .minimum_nprobes(NPROBES)
                    .maximum_nprobes(NPROBES)
                    .refine(REFINE_FACTOR)
                    .project(&["vector", "_distance"])
                    .unwrap()
                    .try_into_stream()
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
            })
        })
        .buffered(concurrent_queries)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
}

fn bench_ivf_pq_throughput(c: &mut Criterion) {
    env_logger::init();

    let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();

    let mut group = c.benchmark_group("ivf_pq_throughput");
    group.throughput(Throughput::Elements(NUM_QUERIES as u64));

    for &version in &[
        LanceFileVersion::V2_0,
        LanceFileVersion::V2_1,
        LanceFileVersion::V2_2,
    ] {
        // Get or create cached dataset
        let cached_dataset = get_or_create_dataset(&rt, version);

        for &concurrent_queries in &[1, 16] {
            for &cached in &[true, false] {
                // Skip uncached tests on non-Linux platforms
                #[cfg(not(target_os = "linux"))]
                if !cached {
                    continue;
                }

                let cache_label = if cached { "cached" } else { "nocache" };

                // One pass to warm up the index cache
                rt.block_on(run_queries(
                    cached_dataset.dataset.clone(),
                    &cached_dataset.query_vectors,
                    concurrent_queries,
                ));

                group.bench_function(
                    format!("{}_{}threads_{}", version, concurrent_queries, cache_label),
                    |b| {
                        b.iter_batched(
                            || {
                                // Setup: drop cache if uncached
                                if !cached {
                                    drop_dataset_from_cache(&dataset_path(version)).ok();
                                }
                            },
                            |_| {
                                // Run the queries
                                rt.block_on(run_queries(
                                    cached_dataset.dataset.clone(),
                                    &cached_dataset.query_vectors,
                                    concurrent_queries,
                                ));
                            },
                            BatchSize::PerIteration,
                        );
                    },
                );
            }
        }
    }
    group.finish();
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_ivf_pq_throughput
);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_ivf_pq_throughput
);

criterion_main!(benches);
