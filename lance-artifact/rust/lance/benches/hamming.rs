// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark for hamming distance clustering.
//!
//! This benchmark tests the pairwise hamming distance computation and clustering
//! performance at various scales.
//!
//! Run with: cargo bench -p lance --bench hamming
//!
//! Environment variables:
//!   - DATASET_URI: Path to a dataset with a hash column (optional, generates random if not set)
//!   - HASH_COLUMN: Name of the hash column (default: "hash")
//!   - SAMPLE_SIZE: Number of rows to sample (default: 10000)
//!   - THRESHOLD: Hamming distance threshold (default: 10)

#![allow(clippy::print_stdout)]

use std::env;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{FixedSizeListArray, RecordBatch, RecordBatchIterator, UInt8Array};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lance_arrow::FixedSizeListArrayExt;
use rand::Rng;

use lance::index::vector::hamming::{
    hamming_clustering_for_sample, hamming_clustering_from_hashes,
};
use lance::{Dataset, dataset::WriteParams};
use lance_linalg::distance::pairwise_hamming_distance_parallel;

#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};

/// Generate random 64-bit hashes.
fn generate_random_hashes(n: usize) -> Vec<u64> {
    let mut rng = rand::rng();
    (0..n).map(|_| rng.random()).collect()
}

/// Generate random hash dataset as Arrow arrays.
fn generate_hash_batch(num_rows: usize) -> RecordBatch {
    let mut rng = rand::rng();

    // Generate random bytes for the hashes (8 bytes per hash)
    let bytes: Vec<u8> = (0..num_rows * 8).map(|_| rng.random()).collect();
    let values = UInt8Array::from(bytes);

    let hash_array = FixedSizeListArray::try_new_from_values(values, 8).unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "hash",
        DataType::FixedSizeList(FieldRef::new(Field::new("item", DataType::UInt8, true)), 8),
        false,
    )]));

    RecordBatch::try_new(schema, vec![Arc::new(hash_array)]).unwrap()
}

/// Create a test dataset with random hashes.
async fn create_hash_dataset(path: &std::path::Path, num_rows: usize) {
    let batch = generate_hash_batch(num_rows);
    let schema = batch.schema();

    let write_params = WriteParams {
        max_rows_per_file: num_rows,
        max_rows_per_group: 10_000,
        ..Default::default()
    };

    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    Dataset::write(reader, path.to_str().unwrap(), Some(write_params))
        .await
        .unwrap();
}

/// Benchmark pure pairwise hamming computation (no I/O).
fn bench_pairwise_compute(c: &mut Criterion) {
    let mut group = c.benchmark_group("hamming_pairwise_compute");

    for size in [1_000, 5_000, 10_000, 20_000] {
        let hashes = generate_random_hashes(size);
        let total_pairs = (size as u64) * (size as u64 - 1) / 2;

        group.throughput(Throughput::Elements(total_pairs));
        group.bench_with_input(BenchmarkId::new("parallel", size), &hashes, |b, hashes| {
            b.iter(|| {
                pairwise_hamming_distance_parallel(hashes, None, Some(10));
            });
        });
    }

    group.finish();
}

/// Benchmark full clustering pipeline (compute + cluster).
fn bench_cluster_hashes(c: &mut Criterion) {
    let mut group = c.benchmark_group("hamming_cluster");

    for size in [1_000, 5_000, 10_000] {
        let hashes = generate_random_hashes(size);

        group.bench_with_input(
            BenchmarkId::new("full_pipeline", size),
            &hashes,
            |b, hashes| {
                b.iter(|| {
                    hamming_clustering_from_hashes(hashes, None, 10);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark with dataset I/O (if DATASET_URI is set).
fn bench_dataset_cluster(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Check if we should use an external dataset
    let dataset_uri = env::var("DATASET_URI").ok();
    let hash_column = env::var("HASH_COLUMN").unwrap_or_else(|_| "hash".to_string());
    let sample_size: usize = env::var("SAMPLE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let threshold: u32 = env::var("THRESHOLD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    let mut group = c.benchmark_group("hamming_dataset");

    if let Some(uri) = dataset_uri {
        // Use external dataset
        println!("Using external dataset: {}", uri);
        println!(
            "Column: {}, Sample: {}, Threshold: {}",
            hash_column, sample_size, threshold
        );

        let dataset = rt.block_on(async { Dataset::open(&uri).await.unwrap() });

        group.bench_function(format!("external_sample_{}", sample_size), |b| {
            b.to_async(&rt).iter(|| async {
                hamming_clustering_for_sample(&dataset, &hash_column, Some(sample_size), threshold)
                    .await
                    .unwrap()
            });
        });
    } else {
        // Create temporary dataset with random hashes
        let temp_dir = tempfile::tempdir().unwrap();
        let uri = temp_dir.path().join("bench_hashes.lance");

        rt.block_on(async {
            create_hash_dataset(&uri, 100_000).await;
        });

        let dataset = rt.block_on(async { Dataset::open(uri.to_str().unwrap()).await.unwrap() });

        for sample in [1_000, 5_000, 10_000] {
            group.bench_function(format!("generated_sample_{}", sample), |b| {
                let ds = dataset.clone();
                b.to_async(&rt).iter(|| {
                    let ds = ds.clone();
                    async move {
                        hamming_clustering_for_sample(&ds, "hash", Some(sample), 10)
                            .await
                            .unwrap()
                    }
                });
            });
        }
    }

    group.finish();
}

/// Quick standalone benchmark that prints results (for quick testing).
#[allow(dead_code)]
fn run_quick_bench() {
    println!("=== Hamming Distance Clustering Benchmark ===\n");

    let sizes = [1_000, 5_000, 10_000, 20_000];

    for &size in &sizes {
        let hashes = generate_random_hashes(size);
        let total_pairs = (size as u64) * (size as u64 - 1) / 2;

        println!("Size: {} rows, {} pairs", size, total_pairs);
        let start = Instant::now();
        let reader = hamming_clustering_from_hashes(&hashes, None, 10);
        // Consume the reader to count clusters
        let cluster_count: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        let elapsed = start.elapsed();

        let pairs_per_sec = total_pairs as f64 / elapsed.as_secs_f64();
        println!(
            "  Total time: {:?} ({:.2}M pairs/sec)",
            elapsed,
            pairs_per_sec / 1_000_000.0
        );
        println!("  Total clusters: {}", cluster_count);
        println!();
    }
}

#[cfg(target_os = "linux")]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_pairwise_compute, bench_cluster_hashes, bench_dataset_cluster
}

#[cfg(not(target_os = "linux"))]
criterion_group!(
    benches,
    bench_pairwise_compute,
    bench_cluster_hashes,
    bench_dataset_cluster
);

criterion_main!(benches);
