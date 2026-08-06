// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark for concurrent-append throughput against S3 / S3 Express.
//!
//! Many writers append to the same dataset at once. The output measures how
//! the version-hint optimization affects conflict resolution and overall
//! commit rate as the version count grows. Designed to be run on a single
//! large EC2 instance so the writer count itself isn't the bottleneck.
//!
//! ## Running against S3 Standard
//!
//! ```bash
//! export AWS_REGION=us-east-1
//! export DATASET_URI=s3://jack-devland-build/bench/concurrent_append
//! export NUM_WRITERS=64
//! export APPENDS_PER_WRITER=200
//! cargo bench --bench concurrent_append --release
//! ```
//!
//! ## Running against S3 Express
//!
//! ```bash
//! export AWS_REGION=us-east-1
//! export DATASET_URI=s3://jack-lancedb-devland--use1-az24--x-s3/bench/concurrent_append
//! export NUM_WRITERS=64
//! export APPENDS_PER_WRITER=200
//! cargo bench --bench concurrent_append --release
//! ```
//!
//! ## Configuration
//!
//! - `DATASET_URI`: base URI under which a uniquely-named dataset is created.
//!   Required.
//! - `NUM_WRITERS`: number of concurrent writers (default 64).
//! - `APPENDS_PER_WRITER`: appends each writer attempts (default 200).
//! - `ROWS_PER_APPEND`: rows per appended batch (default 100).
//! - `BASE_ROWS`: rows in the initial table before concurrent writes begin
//!   (default 100_000).
//! - `KEEP_DATASET`: when set to `true`, leaves the dataset in place after
//!   the run (default: deleted on S3, kept on local).

#![allow(clippy::print_stdout, clippy::print_stderr)]

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use criterion::{Criterion, criterion_group, criterion_main};
use lance::dataset::{Dataset, InsertBuilder, WriteMode, WriteParams, builder::DatasetBuilder};
use lance::session::Session;
use lance_io::object_store::{ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const DEFAULT_NUM_WRITERS: usize = 64;
const DEFAULT_APPENDS_PER_WRITER: usize = 200;
const DEFAULT_ROWS_PER_APPEND: usize = 100;
const DEFAULT_BASE_ROWS: usize = 100_000;

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str) -> bool {
    env::var(key)
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn storage_label(uri: &str) -> &'static str {
    if uri.contains("--x-s3") {
        "s3express"
    } else if uri.starts_with("s3://") {
        "s3"
    } else if uri.starts_with("gs://") {
        "gcs"
    } else if uri.starts_with("az://") {
        "azure"
    } else {
        "local"
    }
}

fn schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn batch(start_id: usize, num_rows: usize) -> RecordBatch {
    let ids = Int64Array::from_iter_values((start_id as i64)..((start_id + num_rows) as i64));
    let names = StringArray::from_iter_values(
        (start_id..(start_id + num_rows)).map(|i| format!("name_{i}")),
    );
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(names)]).expect("build batch")
}

/// Storage options that turn on S3 Express when the URI advertises it.
///
/// S3 Express directory buckets don't support GetBucketLocation, so we also
/// require the caller to set `AWS_REGION` and forward it explicitly.
fn store_params_for(uri: &str) -> Option<ObjectStoreParams> {
    if !uri.contains("--x-s3") {
        return None;
    }
    let region = env::var("AWS_REGION")
        .or_else(|_| env::var("AWS_DEFAULT_REGION"))
        .expect("AWS_REGION is required when DATASET_URI points at S3 Express");
    let storage_options: HashMap<String, String> =
        [("s3_express", "true"), ("region", region.as_str())]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
    Some(ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
            storage_options,
        ))),
        ..Default::default()
    })
}

fn write_params(session: Arc<Session>, store_params: Option<ObjectStoreParams>) -> WriteParams {
    WriteParams {
        mode: WriteMode::Append,
        session: Some(session),
        store_params,
        skip_auto_cleanup: true,
        ..Default::default()
    }
}

async fn create_base_dataset(
    uri: &str,
    base_rows: usize,
    rows_per_append: usize,
    session: Arc<Session>,
    store_params: Option<ObjectStoreParams>,
) -> Dataset {
    // When `base_rows == 0` the dataset starts empty: one create commit with a
    // zero-row batch so the writers begin at version 1 with no data.
    let initial_rows = if base_rows == 0 {
        0
    } else {
        rows_per_append.min(base_rows)
    };
    let initial = batch(0, initial_rows);
    let reader = RecordBatchIterator::new(vec![Ok(initial)], schema());
    let create_params = WriteParams {
        mode: WriteMode::Create,
        session: Some(session.clone()),
        store_params: store_params.clone(),
        skip_auto_cleanup: true,
        ..Default::default()
    };
    let mut dataset = Dataset::write(reader, uri, Some(create_params))
        .await
        .expect("create base dataset");

    // Top up to BASE_ROWS in chunks so we don't allocate one huge batch.
    let chunk = 10_000.min(base_rows);
    let mut written = initial_rows;
    while written < base_rows {
        let to_write = chunk.min(base_rows - written);
        let batch = batch(written, to_write);
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema());
        let params = write_params(session.clone(), store_params.clone());
        dataset = Dataset::write(reader, uri, Some(params))
            .await
            .expect("seed appends");
        written += to_write;
    }
    dataset
}

struct WriterStats {
    successes: usize,
    failures: usize,
    latencies: Vec<Duration>,
}

#[allow(clippy::too_many_arguments)]
async fn run_writer(
    writer_id: usize,
    uri: String,
    appends: usize,
    rows_per_append: usize,
    deadline: Option<Instant>,
    per_attempt_timeout: Option<Duration>,
    session: Arc<Session>,
    store_params: Option<ObjectStoreParams>,
) -> WriterStats {
    // Each writer keeps its own dataset handle; CommitBuilder rebases on
    // conflict so we don't need to manually reload between appends.
    let mut dataset = Arc::new(
        DatasetBuilder::from_uri(&uri)
            .with_session(session.clone())
            .load()
            .await
            .expect("writer load"),
    );

    let mut stats = WriterStats {
        successes: 0,
        failures: 0,
        latencies: Vec::with_capacity(appends),
    };

    // Disjoint id ranges per writer so the data inserted is identifiable.
    let id_base = 1_000_000 + writer_id * appends * rows_per_append;
    for i in 0..appends {
        if let Some(d) = deadline
            && Instant::now() >= d
        {
            break;
        }
        let batch = batch(id_base + i * rows_per_append, rows_per_append);
        let params = write_params(session.clone(), store_params.clone());
        let start = Instant::now();
        // Per-attempt cap keeps the slow-tail commits from extending the run
        // far past the writer-side deadline at high concurrency.
        let result = match per_attempt_timeout {
            Some(t) => {
                let ds = dataset.clone();
                let params_ref = &params;
                match tokio::time::timeout(t, async move {
                    InsertBuilder::new(ds)
                        .with_params(params_ref)
                        .execute(vec![batch])
                        .await
                })
                .await
                {
                    Ok(r) => r,
                    Err(_) => Err(lance_core::Error::io_source(Box::new(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "per-attempt timeout",
                    )))),
                }
            }
            None => {
                InsertBuilder::new(dataset.clone())
                    .with_params(&params)
                    .execute(vec![batch])
                    .await
            }
        };
        let elapsed = start.elapsed();
        match result {
            Ok(new_ds) => {
                stats.successes += 1;
                stats.latencies.push(elapsed);
                dataset = Arc::new(new_ds);
            }
            Err(e) => {
                stats.failures += 1;
                eprintln!("writer {writer_id} append {i} failed after {elapsed:?}: {e}");
                // Reload and keep going so a single failure doesn't end the run.
                dataset = Arc::new(
                    DatasetBuilder::from_uri(&uri)
                        .with_session(session.clone())
                        .load()
                        .await
                        .expect("writer reload after error"),
                );
            }
        }
    }
    stats
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn bench_concurrent_append(c: &mut Criterion) {
    let dataset_base =
        env::var("DATASET_URI").expect("DATASET_URI is required for concurrent_append bench");
    let num_writers = env_usize("NUM_WRITERS", DEFAULT_NUM_WRITERS);
    let appends_per_writer = env_usize("APPENDS_PER_WRITER", DEFAULT_APPENDS_PER_WRITER);
    let rows_per_append = env_usize("ROWS_PER_APPEND", DEFAULT_ROWS_PER_APPEND);
    let base_rows = env_usize("BASE_ROWS", DEFAULT_BASE_ROWS);
    let keep_dataset = env_bool("KEEP_DATASET");
    // Per-writer wall-clock budget. When non-zero, each writer stops looping
    // once this many seconds have elapsed since the run started, even if it
    // hasn't issued `APPENDS_PER_WRITER` commits yet. Lets us bound run time
    // at high concurrency where conflict retries make commits arbitrarily slow.
    let max_wall_secs = env_usize("MAX_WALL_SECS", 0);
    // Per-attempt timeout. Caps any single commit attempt (including its
    // internal retries) so the slow-tail of an under-contention commit doesn't
    // extend the run past the writer deadline. 0 disables it.
    let per_attempt_timeout_secs = env_usize("PER_ATTEMPT_TIMEOUT_SECS", 0);

    let uri = format!(
        "{}/concurrent_append_{}",
        dataset_base.trim_end_matches('/'),
        &Uuid::new_v4().to_string()[..8]
    );
    let label = storage_label(&uri);
    let store_params = store_params_for(&uri);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    println!("=== Concurrent Append Benchmark ===");
    println!("Storage: {uri} ({label})");
    println!(
        "Writers: {num_writers}, appends/writer: {appends_per_writer}, rows/append: {rows_per_append}"
    );
    println!("Base rows: {base_rows}, keep_dataset: {keep_dataset}");
    println!();

    // Share one ObjectStoreRegistry so all writers reuse warm TCP/TLS sessions.
    let registry = Arc::new(ObjectStoreRegistry::default());
    let session = Arc::new(Session::new(0, 0, registry));

    println!("Seeding base dataset ({base_rows} rows)...");
    let seed_start = Instant::now();
    let base_dataset = runtime.block_on(create_base_dataset(
        &uri,
        base_rows,
        rows_per_append,
        session.clone(),
        store_params.clone(),
    ));
    let starting_version = base_dataset.manifest().version;
    println!(
        "Base dataset ready in {:.2}s at version {starting_version}",
        seed_start.elapsed().as_secs_f64()
    );

    println!("Starting {num_writers} concurrent writers...");
    let wall_start = Instant::now();
    let deadline =
        (max_wall_secs > 0).then(|| wall_start + Duration::from_secs(max_wall_secs as u64));
    if let Some(d) = deadline {
        println!(
            "Per-writer wall budget: {max_wall_secs}s (deadline {:?} from now)",
            d.duration_since(wall_start)
        );
    }
    let per_attempt_timeout = (per_attempt_timeout_secs > 0)
        .then(|| Duration::from_secs(per_attempt_timeout_secs as u64));
    if let Some(t) = per_attempt_timeout {
        println!("Per-attempt timeout: {:?}", t);
    }
    let all_stats: Vec<WriterStats> = runtime.block_on(async {
        let mut tasks = Vec::with_capacity(num_writers);
        for writer_id in 0..num_writers {
            let uri = uri.clone();
            let session = session.clone();
            let store_params = store_params.clone();
            tasks.push(tokio::spawn(async move {
                run_writer(
                    writer_id,
                    uri,
                    appends_per_writer,
                    rows_per_append,
                    deadline,
                    per_attempt_timeout,
                    session,
                    store_params,
                )
                .await
            }));
        }
        let mut out = Vec::with_capacity(num_writers);
        for t in tasks {
            out.push(t.await.expect("writer task panicked"));
        }
        out
    });
    let wall = wall_start.elapsed();

    let total_attempts = all_stats
        .iter()
        .map(|s| s.successes + s.failures)
        .sum::<usize>();
    let total_success = all_stats.iter().map(|s| s.successes).sum::<usize>();
    let total_failed = all_stats.iter().map(|s| s.failures).sum::<usize>();
    let mut latencies: Vec<Duration> = all_stats
        .into_iter()
        .flat_map(|s| s.latencies.into_iter())
        .collect();
    latencies.sort();

    let throughput = total_success as f64 / wall.as_secs_f64();

    println!();
    println!("=== Results ===");
    println!("Wall time: {:.2}s", wall.as_secs_f64());
    println!(
        "Commits: {total_success} succeeded, {total_failed} failed out of {total_attempts} attempts"
    );
    println!("Throughput: {throughput:.2} commits/sec");
    if !latencies.is_empty() {
        let mean = latencies.iter().map(|d| d.as_secs_f64()).sum::<f64>() / latencies.len() as f64;
        println!(
            "Commit latency (per writer, includes any retries): \
             p50={:.2}ms p90={:.2}ms p95={:.2}ms p99={:.2}ms max={:.2}ms mean={:.2}ms",
            ms(percentile(&latencies, 0.50)),
            ms(percentile(&latencies, 0.90)),
            ms(percentile(&latencies, 0.95)),
            ms(percentile(&latencies, 0.99)),
            ms(*latencies.last().unwrap()),
            mean * 1000.0,
        );
    }

    let final_dataset = runtime.block_on(async {
        DatasetBuilder::from_uri(&uri)
            .with_session(session.clone())
            .load()
            .await
            .expect("final load")
    });
    println!(
        "Final dataset version: {} (started at {})",
        final_dataset.manifest().version,
        starting_version
    );

    // Pin the numbers into criterion so they show up in regression tracking.
    let mut group = c.benchmark_group(format!("concurrent_append_{label}"));
    group.bench_function("commits_per_sec", |b| b.iter(|| throughput));
    group.bench_function("p50_ms", |b| b.iter(|| ms(percentile(&latencies, 0.50))));
    group.bench_function("p99_ms", |b| b.iter(|| ms(percentile(&latencies, 0.99))));
    group.finish();

    if !keep_dataset && label == "local" {
        let _ = std::fs::remove_dir_all(&uri);
        println!("Local dataset removed: {uri}");
    } else {
        println!("Dataset preserved: {uri}");
    }
}

criterion_group!(benches, bench_concurrent_append);
criterion_main!(benches);
