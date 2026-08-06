// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark for MemWAL replay throughput.
//!
//! Two scenarios:
//! 1. **Raw `WalTailer::read_entry`**: time the cost of pulling N WAL
//!    entries off storage one at a time. This is the lower bound for any
//!    higher-level replay (e.g. `ShardWriter::open` MemTable-mode replay).
//! 2. **`ShardWriter::open` end-to-end replay** in MemTable mode: open a
//!    shard whose previous writer left N un-flushed WAL entries on disk,
//!    measuring how long it takes for `open` to return (replay reads +
//!    MemTable inserts + index updates).
//!
//! ## Running against S3
//!
//! ```bash
//! export AWS_DEFAULT_REGION=us-east-1
//! export DATASET_PREFIX=s3://your-bucket/bench/mem_wal_replay
//! cargo bench --bench mem_wal_replay
//! ```
//!
//! ## Running against local filesystem (with temp directory)
//!
//! ```bash
//! cargo bench --bench mem_wal_replay
//! ```
//!
//! ## Configuration
//!
//! - `DATASET_PREFIX`: Base URI for datasets (optional, e.g. s3://bucket/prefix or /tmp/bench).
//!   If not set, uses a temporary directory.
//! - `NUM_ENTRIES`: Number of WAL entries to write before the replay run (default: 1000)
//! - `BATCH_SIZE`: Rows per WAL entry (default: 20)
//! - `SAMPLE_SIZE`: Number of benchmark iterations (default: 10, minimum: 10)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriterConfig, WalTailer};
use lance::dataset::{Dataset, WriteParams};
use lance_io::object_store::ObjectStore;
use uuid::Uuid;

use arrow_array::RecordBatchIterator;

/// Default number of WAL entries to write before replay.
const DEFAULT_NUM_ENTRIES: usize = 1000;

/// Default rows per WAL entry.
const DEFAULT_BATCH_SIZE: usize = 20;

fn get_num_entries() -> usize {
    std::env::var("NUM_ENTRIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_NUM_ENTRIES)
}

fn get_batch_size() -> usize {
    std::env::var("BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BATCH_SIZE)
}

fn get_sample_size() -> usize {
    std::env::var("SAMPLE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10)
        .max(10)
}

fn get_dataset_prefix() -> String {
    std::env::var("DATASET_PREFIX").unwrap_or_else(|_| {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let path = temp_dir.path().to_string_lossy().to_string();
        std::mem::forget(temp_dir);
        format!("file://{}", path)
    })
}

fn get_storage_label(prefix: &str) -> &'static str {
    if prefix.starts_with("s3://") {
        "s3"
    } else if prefix.starts_with("gs://") {
        "gcs"
    } else if prefix.starts_with("az://") {
        "azure"
    } else {
        "local"
    }
}

fn create_test_schema() -> Arc<ArrowSchema> {
    let id_meta: std::collections::HashMap<String, String> = [(
        "lance-schema:unenforced-primary-key".to_string(),
        "1".to_string(),
    )]
    .into_iter()
    .collect();
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(id_meta),
        Field::new("text", DataType::Utf8, true),
    ]))
}

fn create_test_batch(schema: &Arc<ArrowSchema>, start_id: i64, num_rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (start_id..start_id + num_rows as i64).collect();
    let texts: Vec<String> = (0..num_rows)
        .map(|i| format!("row_{}", start_id as usize + i))
        .collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(texts)),
        ],
    )
    .unwrap()
}

/// Build a base Lance dataset and initialize MemWAL on it. Returns the
/// dataset URI.
async fn setup_dataset(schema: &Arc<ArrowSchema>, name: &str, dataset_prefix: &str) -> String {
    let dataset_uri = if let Some(stripped) = dataset_prefix.strip_prefix("file://") {
        format!("file://{}/{}", stripped, name)
    } else {
        format!("{}/{}", dataset_prefix.trim_end_matches('/'), name)
    };

    // Best-effort delete in case a prior run left state behind.
    if let Ok((object_store, base)) = ObjectStore::from_uri(&dataset_uri).await {
        let _ = object_store.remove_dir_all(base).await;
    }

    // Seed an empty Lance dataset (one tiny batch). Keeps `Dataset::open`
    // happy and lets us call `initialize_mem_wal`.
    let initial = create_test_batch(schema, -1, 1);
    let reader = RecordBatchIterator::new(vec![Ok(initial)].into_iter(), schema.clone());
    let mut dataset = Dataset::write(
        reader,
        &dataset_uri,
        Some(WriteParams {
            data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
            mode: lance::dataset::WriteMode::Create,
            ..Default::default()
        }),
    )
    .await
    .expect("Failed to seed dataset");

    dataset
        .initialize_mem_wal()
        .execute()
        .await
        .expect("Failed to initialize MemWAL");

    dataset_uri
}

/// Pre-populate a shard with `num_entries` durable WAL entries, then
/// drop the writer **without** calling `close` so the active MemTable
/// is not flushed. The WAL files persist on storage and become input
/// to the replay benchmark.
async fn populate_shard_wal(
    dataset_uri: &str,
    schema: &Arc<ArrowSchema>,
    shard_id: Uuid,
    num_entries: usize,
    batch_size: usize,
) {
    let dataset = Dataset::open(dataset_uri).await.unwrap();
    let mut config = ShardWriterConfig::new(shard_id);
    config.durable_write = true;
    let writer = dataset.mem_wal_writer(shard_id, config).await.unwrap();

    for i in 0..num_entries {
        let batch = create_test_batch(schema, (i * batch_size) as i64, batch_size);
        writer.put(vec![batch]).await.unwrap();
    }

    // Intentionally drop without close() — leaves the WAL entries on
    // disk for replay.
    drop(writer);
}

/// Bench: raw `WalTailer::read_entry` throughput. Measures how long it
/// takes to walk N entries from position 0 to the tip, end-to-end.
async fn run_tailer_replay(
    object_store: Arc<ObjectStore>,
    base_path: object_store::path::Path,
    shard_id: Uuid,
    expected_entries: usize,
) -> usize {
    let tailer = WalTailer::new(object_store, base_path, shard_id);
    let mut count = 0usize;
    let mut pos = 0u64;
    loop {
        match tailer.read_entry(pos).await.unwrap() {
            None => break,
            Some(_entry) => {
                count += 1;
                pos += 1;
            }
        }
    }
    assert_eq!(
        count, expected_entries,
        "tailer saw fewer entries than written"
    );
    count
}

/// Bench: full `ShardWriter::open` + replay path. Open the shard with the
/// pre-populated WAL, time how long until `open` returns. The returned
/// writer is closed afterwards so the next iteration is independent.
async fn run_open_replay(dataset_uri: &str, shard_id: Uuid) -> Duration {
    let dataset = Dataset::open(dataset_uri).await.unwrap();
    let config = ShardWriterConfig::new(shard_id);
    let start = Instant::now();
    let writer = dataset.mem_wal_writer(shard_id, config).await.unwrap();
    let elapsed = start.elapsed();
    // Drop without close() so the next iteration sees the same WAL state.
    drop(writer);
    elapsed
}

fn bench_replay(c: &mut Criterion) {
    let _ = env_logger::try_init();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let dataset_prefix = get_dataset_prefix();
    let storage_label = get_storage_label(&dataset_prefix);
    let num_entries = get_num_entries();
    let batch_size = get_batch_size();
    let sample_size = get_sample_size();
    let total_rows = num_entries * batch_size;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let schema = create_test_schema();

    println!("=== MemWAL Replay Benchmark Setup ===");
    println!("Storage: {}", dataset_prefix);
    println!("Schema: id (Int64, primary key), text (Utf8)");
    println!("Num WAL entries: {}", num_entries);
    println!("Batch size: {} rows/entry", batch_size);
    println!("Total rows replayed: {}", total_rows);
    println!("Iterations: {}", sample_size);
    println!();

    // Setup: one dataset for all replay variants. Each variant uses a fresh
    // shard so they don't interfere.
    let dataset_uri = rt.block_on(setup_dataset(&schema, "replay_dataset", &dataset_prefix));

    // Pre-populate two shards: one for the raw-tailer bench, one for the
    // open-replay bench. Each iteration of the open-replay bench drops the
    // writer without close(), so the WAL state is unchanged across
    // iterations and the bench is repeatable.
    let tailer_shard_id = Uuid::new_v4();
    rt.block_on(populate_shard_wal(
        &dataset_uri,
        &schema,
        tailer_shard_id,
        num_entries,
        batch_size,
    ));
    let open_shard_id = Uuid::new_v4();
    rt.block_on(populate_shard_wal(
        &dataset_uri,
        &schema,
        open_shard_id,
        num_entries,
        batch_size,
    ));

    let (object_store, base_path) = rt.block_on(ObjectStore::from_uri(&dataset_uri)).unwrap();

    let mut group = c.benchmark_group("MemWAL Replay");
    group.throughput(Throughput::Elements(num_entries as u64));
    group.sample_size(sample_size);
    group.warm_up_time(Duration::from_secs(1));

    // 1. Raw tailer read throughput.
    let label_tailer = format!("{} entries (tailer, {})", num_entries, storage_label);
    println!("Running: {}", label_tailer);
    group.bench_with_input(
        BenchmarkId::new("WalTailer::read_entry", &label_tailer),
        &num_entries,
        |b, &expected| {
            let object_store = object_store.clone();
            let base_path = base_path.clone();
            b.to_async(&rt).iter_custom(|iters| {
                let object_store = object_store.clone();
                let base_path = base_path.clone();
                async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let start = Instant::now();
                        let _count = run_tailer_replay(
                            object_store.clone(),
                            base_path.clone(),
                            tailer_shard_id,
                            expected,
                        )
                        .await;
                        total += start.elapsed();
                    }
                    total
                }
            })
        },
    );

    // 2. End-to-end ShardWriter::open replay.
    let label_open = format!("{} entries (open, {})", num_entries, storage_label);
    println!("Running: {}", label_open);
    group.bench_with_input(
        BenchmarkId::new("ShardWriter::open replay", &label_open),
        &num_entries,
        |b, _| {
            let dataset_uri = dataset_uri.clone();
            b.to_async(&rt).iter_custom(|iters| {
                let dataset_uri = dataset_uri.clone();
                async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let elapsed = run_open_replay(&dataset_uri, open_shard_id).await;
                        total += elapsed;
                    }
                    total
                }
            })
        },
    );

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().significance_level(0.05);
    targets = bench_replay
);
criterion_main!(benches);
