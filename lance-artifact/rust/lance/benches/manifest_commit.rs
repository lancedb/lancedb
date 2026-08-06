// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark for manifest commit performance with many small fragments.
//!
//! This benchmark tests how performance degrades as the number of small fragments
//! grows. Each fragment contains only 10 rows, and we measure both:
//! - Commit time (manifest write only, excludes fragment data writing)
//! - Load time (manifest read from storage, using checkout_latest)
//!
//! Key optimizations:
//! - Uses shared ObjectStoreRegistry to reuse TCP/TLS connections
//! - Disables auto-cleanup to avoid background cleanup overhead
//! - Separates fragment writing from commit measurement
//!
//! ## Running against S3 Express
//!
//! ```bash
//! export AWS_REGION=us-east-1
//! export DATASET_PREFIX=s3://your-bucket--use1-az4--x-s3/bench/manifest_commit
//! export NUM_ITERATIONS=100
//! cargo bench --bench manifest_commit
//! ```
//!
//! ## Running against local filesystem (with temp directory)
//!
//! ```bash
//! cargo bench --bench manifest_commit
//! ```
//!
//! ## Configuration
//!
//! - `DATASET_PREFIX`: Base URI for datasets (e.g. s3://bucket/prefix or /tmp/bench).
//!   If not set, uses a temporary directory.
//! - `NUM_ITERATIONS`: Number of small fragment writes to perform (default: 100).
//! - `ROWS_PER_FRAGMENT`: Number of rows per fragment (default: 10).
//! - `DELETE_DATASET`: When "true", delete the dataset after benchmark completes.
//! - `ENABLE_CACHE`: When "true", enable manifest caching for load measurements.
//!   Default is "false" to measure actual storage read latency.

#![allow(clippy::print_stdout)]

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use criterion::{Criterion, criterion_group, criterion_main};
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::{CommitBuilder, Dataset, InsertBuilder, WriteMode, WriteParams};
use lance::session::Session;
use lance_io::object_store::ObjectStoreRegistry;
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;
use uuid::Uuid;

const DEFAULT_ROWS_PER_FRAGMENT: usize = 10;
const DEFAULT_NUM_ITERATIONS: usize = 100;

fn get_rows_per_fragment() -> usize {
    std::env::var("ROWS_PER_FRAGMENT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_ROWS_PER_FRAGMENT)
}

fn get_num_iterations() -> usize {
    std::env::var("NUM_ITERATIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_NUM_ITERATIONS)
}

fn get_delete_dataset() -> bool {
    std::env::var("DELETE_DATASET")
        .map(|s| s.to_lowercase() == "true")
        .unwrap_or(false)
}

fn get_enable_cache() -> bool {
    std::env::var("ENABLE_CACHE")
        .map(|s| s.to_lowercase() == "true")
        .unwrap_or(false)
}

fn get_dataset_prefix() -> String {
    std::env::var("DATASET_PREFIX").unwrap_or_else(|_| {
        let temp_dir = std::env::temp_dir().join(format!("lance_bench_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).expect("Failed to create temp directory");
        temp_dir.to_string_lossy().to_string()
    })
}

fn get_storage_label(prefix: &str) -> &'static str {
    if prefix.starts_with("s3://") {
        "s3"
    } else if prefix.starts_with("gs://") {
        "gcs"
    } else if prefix.starts_with("az://") {
        "azure"
    } else if prefix.starts_with("memory://") {
        "memory"
    } else {
        "local"
    }
}

async fn create_initial_dataset(
    uri: &str,
    rows_per_fragment: usize,
    session: Arc<Session>,
) -> Dataset {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = create_batch(schema.clone(), 0, rows_per_fragment);
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);

    std::fs::remove_dir_all(uri).ok();

    let params = WriteParams {
        session: Some(session),
        skip_auto_cleanup: true,
        ..Default::default()
    };

    Dataset::write(reader, uri, Some(params))
        .await
        .expect("failed to create initial dataset")
}

fn create_batch(schema: Arc<ArrowSchema>, start_id: usize, num_rows: usize) -> RecordBatch {
    let ids = Int64Array::from_iter_values((start_id as i64)..((start_id + num_rows) as i64));
    let names = StringArray::from_iter_values(
        (start_id..(start_id + num_rows)).map(|i| format!("name_{}", i)),
    );

    RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)])
        .expect("failed to create batch")
}

fn bench_manifest_commit(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");

    let dataset_prefix = get_dataset_prefix();
    let num_iterations = get_num_iterations();
    let rows_per_fragment = get_rows_per_fragment();
    let delete_dataset = get_delete_dataset();
    let enable_cache = get_enable_cache();
    let storage_label = get_storage_label(&dataset_prefix);

    let short_id = &Uuid::new_v4().to_string()[..8];
    let uri = format!(
        "{}/manifest_commit_{}",
        dataset_prefix.trim_end_matches('/'),
        short_id
    );

    println!("=== Manifest Commit Benchmark Setup ===");
    println!("Storage: {} ({})", uri, storage_label);
    println!("Rows per fragment: {}", rows_per_fragment);
    println!("Number of iterations: {}", num_iterations);
    println!(
        "Total fragments (including initial): {}",
        num_iterations + 1
    );
    println!("Delete dataset: {}", delete_dataset);
    println!(
        "Cache enabled: {} ({})",
        enable_cache,
        if enable_cache {
            "using default cache size"
        } else {
            "zero cache size - measures actual storage read"
        }
    );
    println!();

    // Create a shared session for both commit and load operations
    // When cache is disabled, use zero cache size to measure actual storage read latency
    // When cache is enabled, use default cache sizes (6GB index, 1GB metadata)
    let shared_store_registry = Arc::new(ObjectStoreRegistry::default());
    let session = if enable_cache {
        Arc::new(Session::default())
    } else {
        Arc::new(Session::new(0, 0, shared_store_registry))
    };

    let initial_dataset = runtime.block_on(create_initial_dataset(
        &uri,
        rows_per_fragment,
        session.clone(),
    ));

    let uri_clone = uri.clone();
    let mut load_dataset = runtime.block_on(async {
        DatasetBuilder::from_uri(&uri_clone)
            .with_session(session.clone())
            .load()
            .await
            .expect("failed to load dataset for load measurements")
    });

    let mut current_dataset = Arc::new(initial_dataset);

    let mut commit_latencies = Vec::with_capacity(num_iterations);
    let mut load_latencies = Vec::with_capacity(num_iterations);

    println!("Running commit and load benchmarks...");
    println!("fragments,commit_ms,load_ms");

    for i in 1..=num_iterations {
        let num_fragments = i + 1;

        let (commit_time, new_dataset) = {
            let dataset = current_dataset.clone();
            let session_clone = session.clone();
            runtime.block_on(async move {
                let schema: Arc<ArrowSchema> = Arc::new((&dataset.schema().clone()).into());
                let start_id = dataset.count_rows(None).await.unwrap() as usize;
                let batch = create_batch(schema.clone(), start_id, rows_per_fragment);

                let write_params = WriteParams {
                    mode: WriteMode::Append,
                    session: Some(session_clone.clone()),
                    skip_auto_cleanup: true,
                    ..Default::default()
                };

                let transaction = InsertBuilder::new(dataset.clone())
                    .with_params(&write_params)
                    .execute_uncommitted(vec![batch])
                    .await
                    .expect("failed to write fragment");

                let start = Instant::now();
                let new_ds = CommitBuilder::new(dataset)
                    .with_session(session_clone)
                    .with_skip_auto_cleanup(true)
                    .execute(transaction)
                    .await
                    .expect("failed to commit");
                (start.elapsed(), Arc::new(new_ds))
            })
        };

        let load_time = runtime.block_on(async {
            let start = Instant::now();
            load_dataset
                .checkout_latest()
                .await
                .expect("failed to checkout latest");
            let elapsed = start.elapsed();

            assert_eq!(
                load_dataset.manifest().fragments.len(),
                num_fragments,
                "Expected {} fragments",
                num_fragments
            );
            elapsed
        });

        current_dataset = new_dataset;

        commit_latencies.push(commit_time);
        load_latencies.push(load_time);

        println!(
            "{},{:.2},{:.2}",
            num_fragments,
            commit_time.as_secs_f64() * 1000.0,
            load_time.as_secs_f64() * 1000.0
        );
    }

    println!();
    println!("=== Summary Statistics ===");

    let avg_commit: f64 = commit_latencies
        .iter()
        .map(|d| d.as_secs_f64())
        .sum::<f64>()
        / commit_latencies.len() as f64;
    let avg_load: f64 =
        load_latencies.iter().map(|d| d.as_secs_f64()).sum::<f64>() / load_latencies.len() as f64;

    let min_commit = commit_latencies.iter().min().unwrap();
    let max_commit = commit_latencies.iter().max().unwrap();
    let min_load = load_latencies.iter().min().unwrap();
    let max_load = load_latencies.iter().max().unwrap();

    println!(
        "Commit latency: avg={:.2}ms, min={:.2}ms, max={:.2}ms",
        avg_commit * 1000.0,
        min_commit.as_secs_f64() * 1000.0,
        max_commit.as_secs_f64() * 1000.0
    );
    println!(
        "Load latency:   avg={:.2}ms, min={:.2}ms, max={:.2}ms",
        avg_load * 1000.0,
        min_load.as_secs_f64() * 1000.0,
        max_load.as_secs_f64() * 1000.0
    );

    let first_10_avg_commit = commit_latencies
        .iter()
        .take(10)
        .map(|d| d.as_secs_f64())
        .sum::<f64>()
        / 10.0;
    let last_10_avg_commit = commit_latencies
        .iter()
        .rev()
        .take(10)
        .map(|d| d.as_secs_f64())
        .sum::<f64>()
        / 10.0;
    let first_10_avg_load = load_latencies
        .iter()
        .take(10)
        .map(|d| d.as_secs_f64())
        .sum::<f64>()
        / 10.0;
    let last_10_avg_load = load_latencies
        .iter()
        .rev()
        .take(10)
        .map(|d| d.as_secs_f64())
        .sum::<f64>()
        / 10.0;

    println!();
    println!(
        "First 10 iterations avg: commit={:.2}ms, load={:.2}ms",
        first_10_avg_commit * 1000.0,
        first_10_avg_load * 1000.0
    );
    println!(
        "Last 10 iterations avg:  commit={:.2}ms, load={:.2}ms",
        last_10_avg_commit * 1000.0,
        last_10_avg_load * 1000.0
    );
    println!(
        "Degradation ratio: commit={:.2}x, load={:.2}x",
        last_10_avg_commit / first_10_avg_commit,
        last_10_avg_load / first_10_avg_load
    );

    let mut group = c.benchmark_group("manifest_commit");

    group.bench_function("avg_commit_latency", |b| {
        b.iter(|| std::time::Duration::from_secs_f64(avg_commit))
    });

    group.bench_function("avg_load_latency", |b| {
        b.iter(|| std::time::Duration::from_secs_f64(avg_load))
    });

    group.finish();

    if delete_dataset {
        std::fs::remove_dir_all(&uri).ok();
        println!("Dataset deleted: {}", uri);
    } else {
        println!("Dataset preserved: {}", uri);
    }
}

criterion_group!(benches, bench_manifest_commit);
criterion_main!(benches);
