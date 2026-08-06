// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark for MemWAL write throughput.
//!
//! ## Running against S3
//!
//! ```bash
//! export AWS_DEFAULT_REGION=us-east-1
//! export DATASET_PREFIX=s3://your-bucket/bench/mem_wal
//! cargo bench --bench mem_wal_write
//! ```
//!
//! ## Running against local filesystem (with temp directory)
//!
//! ```bash
//! cargo bench --bench mem_wal_write
//! ```
//!
//! ## Running against specific local directory
//!
//! ```bash
//! export DATASET_PREFIX=/tmp/bench/mem_wal
//! cargo bench --bench mem_wal_write
//! ```
//!
//! ## Configuration
//!
//! - `DATASET_PREFIX`: Base URI for datasets (optional, e.g. s3://bucket/prefix or /tmp/bench). If not set, uses a temporary directory.
//! - `BATCH_SIZE`: Number of rows per write batch (default: 20)
//! - `NUM_BATCHES`: Total number of batches to write (default: 1000)
//! - `DURABLE_WRITE`: yes/no/both (default: no) - whether writes wait for WAL flush
//! - `MAX_WAL_BUFFER_SIZE`: WAL buffer size in bytes (default: 1MB from ShardWriterConfig)
//! - `MAX_FLUSH_INTERVAL_MS`: WAL flush interval in milliseconds, 0 to disable (default: 1000ms)
//! - `MAX_MEMTABLE_SIZE`: MemTable size threshold in bytes (default: 64MB from ShardWriterConfig)
//! - `VECTOR_DIM`: Vector dimension for the vector column (default: 512)
//! - `MEMWAL_MAINTAINED_INDEXES`: Comma-separated list of index names to maintain in MemWAL (default: id_btree)
//!     - Available indexes: id_btree, text_fts, vector_ivfpq (all created on base table)
//!     - Examples: `id_btree`, `id_btree,text_fts`, `vector_ivfpq`
//!     - Use `none` to disable MemWAL index maintenance entirely
//! - `ENABLE_MEMTABLE`: yes/no/both (default: yes) - whether the writer maintains an in-memory MemTable.
//!   `yes` uses the standard MemTable + WAL pipeline (with optional indexes).
//!   `no` uses WAL-only mode (no MemTable, no indexes, no Lance flushes; pure WAL throughput).
//!   `both` runs each combination twice, once per mode, side-by-side.
//!   When `no` or `both`, the WAL-only branch always runs with
//!   `MEMWAL_MAINTAINED_INDEXES=none`.
//! - `SAMPLE_SIZE`: Number of benchmark iterations (default: 10, minimum: 10)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use arrow_array::{
    FixedSizeListArray, Float32Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriterConfig};
use lance::dataset::{Dataset, WriteParams};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_arrow::FixedSizeListArrayExt;
use lance_index::IndexType;
use lance_index::scalar::ScalarIndexParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_linalg::distance::DistanceType;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};
use uuid::Uuid;

/// Default number of rows per batch.
const DEFAULT_BATCH_SIZE: usize = 20;

/// Default number of batches to write.
const DEFAULT_NUM_BATCHES: usize = 1000;

/// Get batch size from environment or use default.
fn get_batch_size() -> usize {
    std::env::var("BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BATCH_SIZE)
}

/// Get number of batches from environment or use default.
fn get_num_batches() -> usize {
    std::env::var("NUM_BATCHES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_NUM_BATCHES)
}

/// Parse yes/no/both env var, returns list of bool values to test.
fn parse_yes_no_both(var_name: &str, default: &str) -> Vec<bool> {
    let value = std::env::var(var_name)
        .unwrap_or_else(|_| default.to_string())
        .to_lowercase();
    match value.as_str() {
        "yes" | "true" | "1" => vec![true],
        "no" | "false" | "0" => vec![false],
        "both" => vec![false, true],
        _ => {
            eprintln!(
                "Invalid {} value '{}', using default '{}'",
                var_name, value, default
            );
            parse_yes_no_both(var_name, default)
        }
    }
}

/// Get durable write settings from environment.
fn get_durable_write_options() -> Vec<bool> {
    parse_yes_no_both("DURABLE_WRITE", "no")
}

/// Get enable_memtable settings from environment. Default `yes` keeps
/// existing benchmark behavior; `no` runs WAL-only mode; `both` runs both
/// modes side-by-side for comparison.
fn get_enable_memtable_options() -> Vec<bool> {
    parse_yes_no_both("ENABLE_MEMTABLE", "yes")
}

/// Get max WAL buffer size from environment or use default.
fn get_max_wal_buffer_size() -> Option<usize> {
    std::env::var("MAX_WAL_BUFFER_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
}

/// Get max flush interval from environment or use default.
fn get_max_flush_interval() -> Option<Option<Duration>> {
    std::env::var("MAX_FLUSH_INTERVAL_MS").ok().map(|s| {
        let ms: u64 = s.parse().unwrap_or(0);
        if ms == 0 {
            None
        } else {
            Some(Duration::from_millis(ms))
        }
    })
}

/// Get max memtable size from environment or use default.
fn get_max_memtable_size() -> Option<usize> {
    std::env::var("MAX_MEMTABLE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
}

/// Default vector dimension for benchmarks.
const DEFAULT_VECTOR_DIM: i32 = 512;

/// Get vector dimension from environment or use default.
fn get_vector_dim() -> i32 {
    std::env::var("VECTOR_DIM")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_VECTOR_DIM)
}

/// Parse MEMWAL_MAINTAINED_INDEXES environment variable.
/// Returns list of index names to maintain in MemWAL.
/// Use "none" to disable indexes entirely.
/// Default: "id_btree"
fn get_maintained_indexes() -> Vec<String> {
    let value =
        std::env::var("MEMWAL_MAINTAINED_INDEXES").unwrap_or_else(|_| "id_btree".to_string());

    if value.to_lowercase() == "none" {
        return vec![];
    }

    value
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Get sample size from environment or use default.
/// Minimum is 10 (Criterion requirement).
fn get_sample_size() -> usize {
    std::env::var("SAMPLE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10)
        .max(10)
}

/// Format bytes in human-readable form.
fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

/// Format throughput in human-readable form (bytes/sec).
fn format_throughput(bytes_per_sec: f64) -> String {
    if bytes_per_sec >= 1024.0 * 1024.0 * 1024.0 {
        format!("{:.2} GB/s", bytes_per_sec / (1024.0 * 1024.0 * 1024.0))
    } else if bytes_per_sec >= 1024.0 * 1024.0 {
        format!("{:.2} MB/s", bytes_per_sec / (1024.0 * 1024.0))
    } else if bytes_per_sec >= 1024.0 {
        format!("{:.2} KB/s", bytes_per_sec / 1024.0)
    } else {
        format!("{:.0} B/s", bytes_per_sec)
    }
}

/// Estimate the size of a single row in bytes.
///
/// Schema: id (Int64) + vector (Float32 * dim) + text (Utf8, ~70 bytes avg)
fn estimate_row_size_bytes(vector_dim: i32) -> usize {
    const ID_SIZE: usize = 8; // Int64
    const AVG_TEXT_SIZE: usize = 70; // Average text length including " (row N)"
    let vector_size = 4 * vector_dim as usize; // Float32 * dim
    ID_SIZE + vector_size + AVG_TEXT_SIZE
}

/// Create test schema for benchmarks.
///
/// Schema:
/// - id: Int64 (primary key, for BTree index)
/// - vector: FixedSizeList<Float32>[dim] (for IVF-PQ vector index)
/// - text: Utf8 (for FTS inverted index)
fn create_test_schema(vector_dim: i32) -> Arc<ArrowSchema> {
    use std::collections::HashMap;

    // Create id field with primary key metadata
    let mut id_metadata = HashMap::new();
    id_metadata.insert(
        "lance-schema:unenforced-primary-key".to_string(),
        "true".to_string(),
    );
    let id_field = Field::new("id", DataType::Int64, false).with_metadata(id_metadata);

    Arc::new(ArrowSchema::new(vec![
        id_field,
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                vector_dim,
            ),
            true,
        ),
        Field::new("text", DataType::Utf8, true),
    ]))
}

/// Sample text snippets for FTS benchmarking.
const SAMPLE_TEXTS: &[&str] = &[
    "The quick brown fox jumps over the lazy dog",
    "Machine learning models require large datasets for training",
    "Vector databases enable semantic search capabilities",
    "Rust provides memory safety without garbage collection",
    "Cloud native applications scale horizontally",
    "Data lakehouse combines warehouse and lake benefits",
    "Embeddings capture semantic meaning in vector space",
    "Columnar storage optimizes analytical query performance",
];

/// Create a test batch with the given parameters.
fn create_test_batch(
    schema: &ArrowSchema,
    start_id: i64,
    num_rows: usize,
    vector_dim: i32,
) -> RecordBatch {
    // Generate random vectors (deterministic based on row id for reproducibility)
    let vectors: Vec<f32> = (0..num_rows)
        .flat_map(|i| {
            let seed = (start_id as usize + i) as f32;
            (0..vector_dim as usize).map(move |d| (seed * 0.1 + d as f32 * 0.01).sin())
        })
        .collect();

    let vector_array =
        FixedSizeListArray::try_new_from_values(Float32Array::from(vectors), vector_dim).unwrap();

    // Generate text content
    let texts: Vec<String> = (0..num_rows)
        .map(|i| {
            let base_text = SAMPLE_TEXTS[(start_id as usize + i) % SAMPLE_TEXTS.len()];
            format!("{} (row {})", base_text, start_id as usize + i)
        })
        .collect();

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from_iter_values(
                start_id..start_id + num_rows as i64,
            )),
            Arc::new(vector_array),
            Arc::new(StringArray::from_iter_values(texts)),
        ],
    )
    .unwrap()
}

/// Number of rows to create in base dataset for index training.
const BASE_DATASET_ROWS: usize = 1000;

/// Get or create dataset prefix directory.
/// Uses DATASET_PREFIX environment variable if set, otherwise creates a temporary directory.
fn get_dataset_prefix() -> String {
    std::env::var("DATASET_PREFIX").unwrap_or_else(|_| {
        let temp_dir = std::env::temp_dir().join(format!("lance_bench_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).expect("Failed to create temp directory");
        temp_dir.to_string_lossy().to_string()
    })
}

/// Create a Lance dataset with indexes and MemWAL initialized.
/// Uses DATASET_PREFIX environment variable if set, otherwise uses a temporary directory.
/// Creates base table indexes (id_btree, text_fts, vector_ivfpq) and initializes MemWAL with specified indexes.
async fn create_dataset(
    schema: &ArrowSchema,
    name_prefix: &str,
    vector_dim: i32,
    maintained_indexes: &[String],
    dataset_prefix: &str,
) -> Dataset {
    use lance_index::scalar::InvertedIndexParams;

    let prefix = dataset_prefix;
    // Use short random suffix (8 chars) instead of full UUID
    let short_id = &Uuid::new_v4().to_string()[..8];
    let uri = format!(
        "{}/{}_{}",
        prefix.trim_end_matches('/'),
        name_prefix,
        short_id
    );

    println!("Creating dataset at {} with indexes...", uri);
    let start = Instant::now();

    // Create initial dataset with 1000 rows for index training
    let initial_batch = create_test_batch(schema, 0, BASE_DATASET_ROWS, vector_dim);
    let batches = RecordBatchIterator::new([Ok(initial_batch)], Arc::new(schema.clone()));
    let write_params = WriteParams {
        data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
        ..Default::default()
    };
    let mut dataset = Dataset::write(batches, &uri, Some(write_params))
        .await
        .expect("Failed to create dataset");

    // Create BTree index on id column
    let scalar_params = ScalarIndexParams::default();
    dataset
        .create_index(
            &["id"],
            IndexType::BTree,
            Some("id_btree".to_string()),
            &scalar_params,
            false,
        )
        .await
        .expect("Failed to create BTree index");

    // Create FTS index on text column
    let fts_params = InvertedIndexParams::default();
    dataset
        .create_index(
            &["text"],
            IndexType::Inverted,
            Some("text_fts".to_string()),
            &fts_params,
            false,
        )
        .await
        .expect("Failed to create FTS index");

    // Create IVF-PQ vector index on vector column
    // Use small nlist for the small training dataset
    let ivf_params = IvfBuildParams::new(16); // 16 partitions for 1000 rows
    let pq_params = PQBuildParams::new(16, 8); // 16 sub-vectors, 8 bits
    let vector_params =
        VectorIndexParams::with_ivf_pq_params(DistanceType::L2, ivf_params, pq_params);
    dataset
        .create_index(
            &["vector"],
            IndexType::IvfPq,
            Some("vector_ivfpq".to_string()),
            &vector_params,
            false,
        )
        .await
        .expect("Failed to create IVF-PQ index");

    // Initialize MemWAL with specified maintained indexes
    dataset
        .initialize_mem_wal()
        .maintained_indexes(maintained_indexes.to_vec())
        .execute()
        .await
        .expect("Failed to initialize MemWAL");

    println!(
        "Dataset created in {:?} at {}",
        start.elapsed(),
        dataset.uri()
    );

    dataset
}

/// Get storage label from dataset prefix (e.g. "s3" or "local").
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

/// Build benchmark label from config options.
fn build_label(
    num_batches: usize,
    batch_size: usize,
    durable: bool,
    enable_memtable: bool,
    storage: &str,
) -> String {
    let durable_str = if durable { "durable" } else { "nondurable" };
    let mode_str = if enable_memtable {
        "memtable"
    } else {
        "wal_only"
    };
    format!(
        "{}x{} {} {} ({})",
        num_batches, batch_size, mode_str, durable_str, storage
    )
}

/// Build dataset name prefix from config options.
fn build_name_prefix(durable: bool, enable_memtable: bool) -> String {
    let d = if durable { "d" } else { "nd" };
    let m = if enable_memtable { "mt" } else { "wo" };
    format!("{}_{}", m, d)
}

/// Benchmark Lance MemWAL write throughput.
fn bench_lance_memwal_write(c: &mut Criterion) {
    // Initialize log crate output (for informational logs in mem_wal modules)
    let _ = env_logger::try_init();

    // Initialize tracing subscriber (for stats summary logs)
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let dataset_prefix = get_dataset_prefix();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let batch_size = get_batch_size();
    let num_batches = get_num_batches();
    let vector_dim = get_vector_dim();
    let schema = create_test_schema(vector_dim);
    let storage_label = get_storage_label(&dataset_prefix);
    let maintained_indexes = get_maintained_indexes();

    let durable_options = get_durable_write_options();
    let enable_memtable_options = get_enable_memtable_options();
    let max_wal_buffer_size = get_max_wal_buffer_size();
    let max_flush_interval = get_max_flush_interval();
    let max_memtable_size = get_max_memtable_size();
    let sample_size = get_sample_size();

    // Calculate total data size for throughput measurement
    let row_size_bytes = estimate_row_size_bytes(vector_dim);
    let total_rows = batch_size * num_batches;
    let total_bytes = (total_rows * row_size_bytes) as u64;

    // Get effective config values for display
    let default_config = ShardWriterConfig::default();
    let effective_wal_buffer = max_wal_buffer_size.unwrap_or(default_config.max_wal_buffer_size);
    let effective_flush_interval =
        max_flush_interval.unwrap_or(default_config.max_wal_flush_interval);
    let effective_memtable_size = max_memtable_size.unwrap_or(default_config.max_memtable_size);

    // Print test setup summary
    println!("=== MemWAL Write Benchmark Setup ===");
    println!("Storage: {}", dataset_prefix);
    println!(
        "Schema: id (Int64), vector (Float32x{}), text (Utf8)",
        vector_dim
    );
    println!(
        "Base table: {} rows with indexes (id_btree, text_fts, vector_ivfpq)",
        BASE_DATASET_ROWS
    );
    println!(
        "MemWAL indexes: {}",
        if maintained_indexes.is_empty() {
            "none".to_string()
        } else {
            maintained_indexes.join(", ")
        }
    );
    println!("Batch size: {} rows", batch_size);
    println!("Num batches: {}", num_batches);
    println!("Total rows: {}", total_rows);
    println!("Row size: {} bytes", row_size_bytes);
    println!("Total data: {}", format_bytes(total_bytes));
    println!("WAL buffer: {}", format_bytes(effective_wal_buffer as u64));
    println!("WAL flush interval: {:?}", effective_flush_interval);
    println!(
        "MemTable size: {}",
        format_bytes(effective_memtable_size as u64)
    );
    println!("Benchmark iterations: {}", sample_size);
    println!();

    let mut group = c.benchmark_group("MemWAL Write");
    group.throughput(Throughput::Bytes(total_bytes));
    group.sample_size(sample_size);
    group.warm_up_time(Duration::from_secs(1));

    // Generate benchmarks for all combinations
    for &enable_memtable in &enable_memtable_options {
        for &durable in &durable_options {
            let label = build_label(
                num_batches,
                batch_size,
                durable,
                enable_memtable,
                storage_label,
            );
            let name_prefix = build_name_prefix(durable, enable_memtable);

            // WAL-only mode never uses indexes; force the dataset
            // setup to skip the MemWAL index list.
            let effective_indexes: Vec<String> = if enable_memtable {
                maintained_indexes.clone()
            } else {
                Vec::new()
            };

            // Create dataset ONCE before benchmark iterations
            // Each iteration will use a different shard on the same dataset
            let dataset = rt.block_on(create_dataset(
                &schema,
                &name_prefix,
                vector_dim,
                &effective_indexes,
                &dataset_prefix,
            ));
            let dataset_uri = dataset.uri().to_string();

            // Pre-generate all batches before timing (outside iter_custom)
            let batches: Arc<Vec<RecordBatch>> = Arc::new(
                (0..num_batches)
                    .map(|i| {
                        create_test_batch(&schema, (i * batch_size) as i64, batch_size, vector_dim)
                    })
                    .collect(),
            );

            println!("Running: {}", label);

            // Track if we've printed stats (only print once across all samples)
            let stats_printed = Arc::new(AtomicBool::new(false));

            group.bench_with_input(
                BenchmarkId::new("Lance MemWAL", &label),
                &(batch_size, num_batches, durable, row_size_bytes),
                |b, &(_batch_size, _num_batches, durable, row_size_bytes)| {
                    let dataset_uri = dataset_uri.clone();
                    let batches = batches.clone();
                    let stats_printed = stats_printed.clone();
                    b.to_async(&rt).iter_custom(|iters| {
                        let dataset_uri = dataset_uri.clone();
                        let batches = batches.clone();
                        let stats_printed = stats_printed.clone();
                        async move {
                            let mut total_duration = Duration::ZERO;

                            for iter in 0..iters {
                                // Re-open dataset (cheap operation)
                                let dataset = Dataset::open(&dataset_uri).await.unwrap();

                                // Create a NEW shard for each iteration
                                let shard_id = Uuid::new_v4();
                                let default_config = ShardWriterConfig::default();
                                let config = ShardWriterConfig {
                                    shard_id,
                                    shard_spec_id: 0,
                                    max_wal_persist_retries: 3,
                                    wal_persist_retry_base_delay: std::time::Duration::from_millis(
                                        50,
                                    ),
                                    durable_write: durable,
                                    max_wal_buffer_size: max_wal_buffer_size
                                        .unwrap_or(default_config.max_wal_buffer_size),
                                    max_wal_flush_interval: max_flush_interval
                                        .unwrap_or(default_config.max_wal_flush_interval),
                                    max_memtable_size: max_memtable_size
                                        .unwrap_or(default_config.max_memtable_size),
                                    max_memtable_rows: default_config.max_memtable_rows,
                                    max_memtable_batches: default_config.max_memtable_batches,
                                    manifest_scan_batch_size: default_config
                                        .manifest_scan_batch_size,
                                    max_unflushed_memtable_bytes: default_config
                                        .max_unflushed_memtable_bytes,
                                    backpressure_log_interval: default_config
                                        .backpressure_log_interval,
                                    stats_log_interval: default_config.stats_log_interval,
                                    frozen_memtable_grace: default_config.frozen_memtable_grace,
                                    enable_memtable,
                                    hnsw_params: default_config.hnsw_params,
                                    warmer: None,
                                    store_params: default_config.store_params,
                                    session: default_config.session,
                                };

                                // Get writer through Dataset API (index configs loaded automatically)
                                let writer =
                                    dataset.mem_wal_writer(shard_id, config).await.unwrap();

                                // Time writes (excluding close to measure pure put throughput)
                                let start = Instant::now();
                                for batch in batches.iter() {
                                    writer.put(vec![batch.clone()]).await.unwrap();
                                }
                                let put_duration = start.elapsed();

                                // Close writer (includes final WAL flush) - measured separately
                                let close_start = Instant::now();
                                let stats_handle = writer.stats_handle();
                                writer.close().await.unwrap();
                                let close_duration = close_start.elapsed();
                                // Get stats after close to include all WAL flushes
                                let stats = stats_handle.snapshot();

                                total_duration += put_duration + close_duration;

                                // Report stats once (first iteration of first sample only)
                                if iter == 0 && !stats_printed.swap(true, Ordering::SeqCst) {
                                    let rows_per_sec = stats.put_throughput();
                                    let bytes_per_sec = rows_per_sec * row_size_bytes as f64;
                                    println!(
                                        "  Stats: puts={} ({:.0} rows/s, {}) | avg {:?}",
                                        stats.put_count,
                                        rows_per_sec,
                                        format_throughput(bytes_per_sec),
                                        stats.avg_put_latency().unwrap_or_default(),
                                    );
                                    println!(
                                        "  WAL flushes: {} ({}) | MemTable flushes: {} ({} rows)",
                                        stats.wal_flush_count,
                                        format_bytes(stats.wal_flush_bytes),
                                        stats.memtable_flush_count,
                                        stats.memtable_flush_rows,
                                    );
                                    println!("  Close time: {:?}", close_duration);
                                }
                            }

                            total_duration
                        }
                    })
                },
            );
        }
    }

    group.finish();
}

#[cfg(target_os = "linux")]
criterion_group!(
    name = benches;
    config = Criterion::default()
        .significance_level(0.05)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_lance_memwal_write
);

#[cfg(not(target_os = "linux"))]
criterion_group!(
    name = benches;
    config = Criterion::default().significance_level(0.05);
    targets = bench_lance_memwal_write
);

criterion_main!(benches);
