// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Copy-on-write `__manifest` directory-catalog commit benchmark (S3 capable).
//!
//! Measures how fast the directory catalog commits `__manifest` mutations as the
//! manifest scales, with the inline scalar indices on or off.
//!
//! Modes:
//!   seed-large — bootstrap a `__manifest` with N rows (direct dataset write + one
//!                CoW rewrite to build indices)
//!   run        — coordinator: spawn `--concurrency` worker processes committing for
//!                either a fixed op count (continuous) or a fixed duration (steady TPS)
//!   worker     — (internal) a single committing process spawned by `run`
//!
//! Examples:
//!   # Bootstrap 100k rows with inline indices
//!   manifest_bench seed-large --root s3://bucket/bench/p --count 100000 \
//!     --inline-optimization true --storage-option aws_region=us-east-1
//!
//!   # Continuous: 100 commits, single process
//!   manifest_bench run --root s3://bucket/bench/p --operation write-create-namespace \
//!     --concurrency 1 --operations 100 --initial-entries 100000 --inline-optimization true
//!
//!   # Concurrent steady TPS: 50 processes committing for 30s
//!   manifest_bench run --root s3://bucket/bench/p --operation write-create-namespace \
//!     --concurrency 50 --duration-secs 30 --initial-entries 100000 --inline-optimization true

// A CLI benchmark tool: workers emit JSON latency records on stdout and progress on
// stderr, so stdout/stderr printing is intentional here.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::builder::{ListBuilder, StringBuilder};
use arrow::array::{RecordBatch, RecordBatchIterator, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use bytes::Bytes;
use lance::dataset::{InsertBuilder, WriteMode, WriteParams};
use lance_core::datatypes::LANCE_UNENFORCED_PRIMARY_KEY_POSITION;
use lance_namespace::LanceNamespace;
use lance_namespace::models::{
    CreateNamespaceRequest, CreateTableRequest, DeclareTableRequest, DescribeTableRequest,
    ListNamespacesRequest, ListTablesRequest,
};
use lance_namespace_impls::DirectoryNamespaceBuilder;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
struct LatencyRecord {
    operation: String,
    latency_ms: f64,
    error: bool,
}

#[derive(Serialize)]
struct BenchResult {
    variant: String,
    operation: String,
    concurrency: usize,
    initial_entries: usize,
    duration_secs: u64,
    total_operations: usize,
    total_duration_ms: f64,
    throughput_ops_per_sec: f64,
    avg_latency_ms: f64,
    p50_latency_ms: f64,
    p90_latency_ms: f64,
    p99_latency_ms: f64,
    min_latency_ms: f64,
    max_latency_ms: f64,
    errors: usize,
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[allow(clippy::too_many_arguments)]
fn compute_result(
    variant: &str,
    operation: &str,
    concurrency: usize,
    initial_entries: usize,
    duration_secs: u64,
    wall_duration: Duration,
    mut latencies: Vec<f64>,
    errors: usize,
) -> BenchResult {
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let total = latencies.len();
    let total_ms = wall_duration.as_secs_f64() * 1000.0;
    let throughput = if total_ms > 0.0 {
        total as f64 / (total_ms / 1000.0)
    } else {
        0.0
    };
    BenchResult {
        variant: variant.to_string(),
        operation: operation.to_string(),
        concurrency,
        initial_entries,
        duration_secs,
        total_operations: total,
        total_duration_ms: total_ms,
        throughput_ops_per_sec: throughput,
        avg_latency_ms: if total > 0 {
            latencies.iter().sum::<f64>() / total as f64
        } else {
            0.0
        },
        p50_latency_ms: percentile(&latencies, 0.50),
        p90_latency_ms: percentile(&latencies, 0.90),
        p99_latency_ms: percentile(&latencies, 0.99),
        min_latency_ms: latencies.first().copied().unwrap_or(0.0),
        max_latency_ms: latencies.last().copied().unwrap_or(0.0),
        errors,
    }
}

fn create_test_ipc_data() -> Vec<u8> {
    use arrow::array::Int32Array;
    use arrow_ipc::writer::StreamWriter;

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    buffer
}

/// The `__manifest` schema used by the copy-on-write directory catalog:
/// `object_id`, `object_type`, `location`, `metadata` (Utf8), `base_objects` (List<Utf8>).
fn manifest_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("object_id", DataType::Utf8, false).with_metadata(
            [(
                LANCE_UNENFORCED_PRIMARY_KEY_POSITION.to_string(),
                "0".to_string(),
            )]
            .into_iter()
            .collect(),
        ),
        Field::new("object_type", DataType::Utf8, false),
        Field::new("location", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new(
            "base_objects",
            DataType::List(Arc::new(Field::new("object_id", DataType::Utf8, true))),
            true,
        ),
    ]))
}

async fn build_namespace(
    root: &str,
    inline_optimization: bool,
    storage_options: &HashMap<String, String>,
) -> Box<dyn LanceNamespace> {
    let mut properties = HashMap::new();
    properties.insert("root".to_string(), root.to_string());
    properties.insert("dir_listing_enabled".to_string(), "false".to_string());
    properties.insert(
        "inline_optimization_enabled".to_string(),
        inline_optimization.to_string(),
    );
    for (k, v) in storage_options {
        properties.insert(format!("storage.{}", k), v.clone());
    }
    let builder = DirectoryNamespaceBuilder::from_properties(properties, None)
        .expect("Failed to create namespace builder from properties");
    Box::new(builder.build().await.expect("Failed to build namespace"))
}

// ──────────────────── seed-large mode ────────────────────
// Bootstrap a `__manifest` with N rows by writing the Lance dataset directly (fast,
// O(N) once), then trigger a single CoW rewrite via the namespace so the on-disk state
// matches what the catalog produces (single fragment + inline indices when enabled).

const SEED_LARGE_BATCH_SIZE: usize = 50_000;

fn generate_manifest_batch(start_idx: usize, batch_size: usize, total_count: usize) -> RecordBatch {
    let ns_count = total_count / 3;
    let actual_size = batch_size.min(total_count - start_idx);

    let mut object_ids = Vec::with_capacity(actual_size);
    let mut object_types = Vec::with_capacity(actual_size);
    let mut locations: Vec<Option<String>> = Vec::with_capacity(actual_size);
    let mut metadatas: Vec<Option<String>> = Vec::with_capacity(actual_size);

    for i in start_idx..start_idx + actual_size {
        if i < ns_count {
            object_ids.push(format!("ns_{}", i));
            object_types.push("namespace".to_string());
            locations.push(None);
            metadatas.push(None);
        } else {
            let table_idx = i - ns_count;
            object_ids.push(format!("table_{}", table_idx));
            object_types.push("table".to_string());
            locations.push(Some(format!("table_{}", table_idx)));
            metadatas.push(Some(r#"{"bench":"true"}"#.to_string()));
        }
    }

    // base_objects is null for every bootstrapped row.
    let mut base_objects_builder = ListBuilder::new(StringBuilder::new())
        .with_field(Arc::new(Field::new("object_id", DataType::Utf8, true)));
    for _ in 0..actual_size {
        base_objects_builder.append_null();
    }

    RecordBatch::try_new(
        manifest_schema(),
        vec![
            Arc::new(StringArray::from(object_ids)),
            Arc::new(StringArray::from(object_types)),
            Arc::new(StringArray::from(
                locations.iter().map(|l| l.as_deref()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                metadatas.iter().map(|m| m.as_deref()).collect::<Vec<_>>(),
            )),
            Arc::new(base_objects_builder.finish()),
        ],
    )
    .expect("Failed to create manifest batch")
}

async fn seed_large(
    root: &str,
    count: usize,
    inline_optimization: bool,
    storage_options: &HashMap<String, String>,
) {
    let manifest_uri = format!("{}/{}", root, "__manifest");
    eprintln!("Seed-large: writing {} rows to {}", count, manifest_uri);

    let schema = manifest_schema();
    let mut batches = Vec::new();
    let mut offset = 0;
    while offset < count {
        let batch_size = SEED_LARGE_BATCH_SIZE.min(count - offset);
        batches.push(generate_manifest_batch(offset, batch_size, count));
        offset += batch_size;
    }
    eprintln!("  generated {} batches", batches.len());

    let mut write_params = WriteParams {
        mode: WriteMode::Create,
        ..WriteParams::default()
    };
    if !storage_options.is_empty() {
        let accessor = Arc::new(
            lance_io::object_store::StorageOptionsAccessor::with_static_options(
                storage_options.clone(),
            ),
        );
        write_params.store_params = Some(lance_io::object_store::ObjectStoreParams {
            storage_options_accessor: Some(accessor),
            ..Default::default()
        });
    }

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    InsertBuilder::new(manifest_uri.as_str())
        .with_params(&write_params)
        .execute_stream(reader)
        .await
        .expect("Failed to write manifest dataset");
    eprintln!("  wrote Lance dataset");

    // Trigger one CoW rewrite so the manifest is in steady catalog form (single
    // fragment; inline indices when enabled). For the no-index variant the first real
    // commit performs this rewrite instead.
    if inline_optimization {
        eprintln!("  triggering initial CoW rewrite to build indices...");
        let start = Instant::now();
        let ns = build_namespace(root, true, storage_options).await;
        let mut req = CreateNamespaceRequest::new();
        req.id = Some(vec!["__seed_trigger__".to_string()]);
        ns.create_namespace(req)
            .await
            .expect("Failed to trigger CoW rewrite");
        eprintln!(
            "  CoW rewrite with index build took {:.1}s",
            start.elapsed().as_secs_f64()
        );
    }

    let ns_count = count / 3;
    eprintln!(
        "Seed-large complete: {} rows ({} namespaces, {} tables)",
        count,
        ns_count,
        count - ns_count
    );
}

// ──────────────────── worker mode ────────────────────

#[allow(clippy::too_many_arguments)]
async fn worker(
    root: &str,
    operation: &str,
    operations: usize,
    duration_secs: u64,
    warmup: usize,
    worker_id: usize,
    table_count: usize,
    inline_optimization: bool,
    storage_options: &HashMap<String, String>,
) {
    let ns = build_namespace(root, inline_optimization, storage_options).await;
    let ipc_data = Bytes::from(create_test_ipc_data());

    if operation.starts_with("warm-read") {
        for _ in 0..warmup {
            let _ =
                run_operation(ns.as_ref(), operation, worker_id, 0, table_count, &ipc_data).await;
        }
    }

    let emit = |op_idx: usize, start: Instant, err: bool| {
        let record = LatencyRecord {
            operation: operation.to_string(),
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
            error: err,
        };
        let _ = op_idx;
        println!("{}", serde_json::to_string(&record).unwrap());
    };

    if duration_secs > 0 {
        // Steady-TPS mode: commit continuously until the deadline.
        let deadline = Instant::now() + Duration::from_secs(duration_secs);
        let mut op_idx = 0;
        while Instant::now() < deadline {
            let start = Instant::now();
            let err = run_operation(
                ns.as_ref(),
                operation,
                worker_id,
                op_idx,
                table_count,
                &ipc_data,
            )
            .await
            .is_err();
            emit(op_idx, start, err);
            op_idx += 1;
        }
    } else {
        for op_idx in 0..operations {
            let start = Instant::now();
            let err = run_operation(
                ns.as_ref(),
                operation,
                worker_id,
                op_idx,
                table_count,
                &ipc_data,
            )
            .await
            .is_err();
            emit(op_idx, start, err);
        }
    }
}

async fn run_operation(
    ns: &dyn LanceNamespace,
    operation: &str,
    worker_id: usize,
    op_idx: usize,
    table_count: usize,
    ipc_data: &Bytes,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match operation {
        "cold-read-list-namespaces" | "warm-read-list-namespaces" => {
            let mut req = ListNamespacesRequest::new();
            req.id = Some(vec![]);
            ns.list_namespaces(req).await?;
        }
        "cold-read-list-tables" | "warm-read-list-tables" => {
            let mut req = ListTablesRequest::new();
            req.id = Some(vec![]);
            ns.list_tables(req).await?;
        }
        "cold-read-describe-table" | "warm-read-describe-table" => {
            let table_idx = (worker_id * 1_000_000 + op_idx) % table_count.max(1);
            let req = DescribeTableRequest {
                id: Some(vec![format!("table_{}", table_idx)]),
                ..Default::default()
            };
            ns.describe_table(req).await?;
        }
        "write-create-namespace" => {
            let mut req = CreateNamespaceRequest::new();
            req.id = Some(vec![format!("bench_w{}_{}", worker_id, op_idx)]);
            ns.create_namespace(req).await?;
        }
        "write-create-table" => {
            let mut req = CreateTableRequest::new();
            req.id = Some(vec![format!("bench_t{}_{}", worker_id, op_idx)]);
            ns.create_table(req, ipc_data.clone()).await?;
        }
        "write-declare-table" => {
            let req = DeclareTableRequest {
                id: Some(vec![format!("bench_d{}_{}", worker_id, op_idx)]),
                ..Default::default()
            };
            ns.declare_table(req).await?;
        }
        _ => {
            return Err(format!("unknown operation: {}", operation).into());
        }
    }
    Ok(())
}

// ──────────────────── run mode (coordinator) ────────────────────

#[allow(clippy::too_many_arguments)]
fn run_workers(
    self_exe: &str,
    root: &str,
    operation: &str,
    concurrency: usize,
    operations: usize,
    duration_secs: u64,
    warmup: usize,
    table_count: usize,
    initial_entries: usize,
    inline_optimization: bool,
    variant: &str,
    storage_options: &HashMap<String, String>,
) -> BenchResult {
    // Continuous mode splits a fixed op budget across workers; steady-TPS mode lets each
    // worker run for the full duration.
    let ops_per_worker = if duration_secs > 0 {
        0
    } else {
        operations / concurrency.max(1)
    };
    if duration_secs == 0 && ops_per_worker == 0 {
        return compute_result(
            variant,
            operation,
            concurrency,
            initial_entries,
            duration_secs,
            Duration::ZERO,
            vec![],
            0,
        );
    }

    let wall_start = Instant::now();
    let children: Vec<_> = (0..concurrency)
        .map(|worker_id| {
            let mut cmd = Command::new(self_exe);
            cmd.arg("worker")
                .arg("--root")
                .arg(root)
                .arg("--operation")
                .arg(operation)
                .arg("--operations")
                .arg(ops_per_worker.to_string())
                .arg("--duration-secs")
                .arg(duration_secs.to_string())
                .arg("--warmup")
                .arg(warmup.to_string())
                .arg("--worker-id")
                .arg(worker_id.to_string())
                .arg("--table-count")
                .arg(table_count.to_string())
                .arg("--inline-optimization")
                .arg(inline_optimization.to_string());
            for (k, v) in storage_options {
                cmd.arg("--storage-option").arg(format!("{}={}", k, v));
            }
            cmd.stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .expect("Failed to spawn worker")
        })
        .collect();

    let mut all_latencies = Vec::new();
    let mut total_errors = 0;
    for mut child in children {
        let stdout = child.stdout.take().unwrap();
        for line in BufReader::new(stdout).lines() {
            let line = line.expect("failed to read worker output");
            if let Ok(record) = serde_json::from_str::<LatencyRecord>(&line) {
                if record.error {
                    total_errors += 1;
                } else {
                    all_latencies.push(record.latency_ms);
                }
            }
        }
        let status = child.wait().expect("failed to wait for worker");
        if !status.success() {
            eprintln!("Worker exited with status: {}", status);
        }
    }

    compute_result(
        variant,
        operation,
        concurrency,
        initial_entries,
        duration_secs,
        wall_start.elapsed(),
        all_latencies,
        total_errors,
    )
}

fn parse_concurrency_list(s: &str) -> Vec<usize> {
    s.split(',')
        .filter_map(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .collect()
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: manifest_bench <seed-large|run|worker> [options]");
        std::process::exit(1);
    }

    let mode = args[1].as_str();
    let mut root = String::new();
    let mut operation = String::new();
    let mut operations: usize = 100;
    let mut duration_secs: u64 = 0;
    let mut warmup: usize = 0;
    let mut concurrency_list = vec![1];
    let mut count: usize = 1000;
    let mut worker_id: usize = 0;
    let mut table_count: usize = 667;
    let mut initial_entries: usize = 0;
    let mut inline_optimization = true;
    let mut variant = String::new();
    let mut storage_options: HashMap<String, String> = HashMap::new();

    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                root = args[i + 1].clone();
                i += 2;
            }
            "--operation" => {
                operation = args[i + 1].clone();
                i += 2;
            }
            "--operations" => {
                operations = args[i + 1].parse().unwrap();
                i += 2;
            }
            "--duration-secs" => {
                duration_secs = args[i + 1].parse().unwrap();
                i += 2;
            }
            "--warmup" => {
                warmup = args[i + 1].parse().unwrap();
                i += 2;
            }
            "--concurrency" => {
                concurrency_list = parse_concurrency_list(&args[i + 1]);
                i += 2;
            }
            "--count" => {
                count = args[i + 1].parse().unwrap();
                i += 2;
            }
            "--worker-id" => {
                worker_id = args[i + 1].parse().unwrap();
                i += 2;
            }
            "--table-count" => {
                table_count = args[i + 1].parse().unwrap();
                i += 2;
            }
            "--initial-entries" => {
                initial_entries = args[i + 1].parse().unwrap();
                i += 2;
            }
            "--inline-optimization" => {
                inline_optimization = args[i + 1].parse().unwrap();
                i += 2;
            }
            "--variant" => {
                variant = args[i + 1].clone();
                i += 2;
            }
            "--storage-option" => {
                if let Some((k, v)) = args[i + 1].split_once('=') {
                    storage_options.insert(k.to_string(), v.to_string());
                }
                i += 2;
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                std::process::exit(1);
            }
        }
    }

    if variant.is_empty() {
        variant = if inline_optimization {
            "inline_index".to_string()
        } else {
            "no_index".to_string()
        };
    }

    match mode {
        "seed-large" => {
            seed_large(&root, count, inline_optimization, &storage_options).await;
        }
        "worker" => {
            worker(
                &root,
                &operation,
                operations,
                duration_secs,
                warmup,
                worker_id,
                table_count,
                inline_optimization,
                &storage_options,
            )
            .await;
        }
        "run" => {
            let self_exe = std::env::current_exe()
                .expect("failed to get self exe path")
                .to_string_lossy()
                .to_string();
            let op = if operation.is_empty() {
                "write-create-namespace"
            } else {
                operation.as_str()
            };

            eprintln!("=== Manifest commit benchmark ===");
            eprintln!(
                "variant={} op={} root={} initial_entries={} concurrency={:?} operations={} duration_secs={}",
                variant, op, root, initial_entries, concurrency_list, operations, duration_secs
            );

            for &concurrency in &concurrency_list {
                let result = run_workers(
                    &self_exe,
                    &root,
                    op,
                    concurrency,
                    operations,
                    duration_secs,
                    warmup,
                    table_count,
                    initial_entries,
                    inline_optimization,
                    &variant,
                    &storage_options,
                );
                eprintln!(
                    "  c={} -> {:.2} ops/s ({} ops, {} errors, p50={:.0}ms p99={:.0}ms)",
                    concurrency,
                    result.throughput_ops_per_sec,
                    result.total_operations,
                    result.errors,
                    result.p50_latency_ms,
                    result.p99_latency_ms
                );
                println!("{}", serde_json::to_string(&result).unwrap());
            }
            eprintln!("=== complete ===");
        }
        _ => {
            eprintln!("Unknown mode: {}. Use seed-large, run, or worker.", mode);
            std::process::exit(1);
        }
    }
}
