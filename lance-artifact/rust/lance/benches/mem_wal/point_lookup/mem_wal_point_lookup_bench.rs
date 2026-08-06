// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Standalone CLI benchmark for PK-based point lookups across LSM levels.
//!
//! Measures lookup latency against three tiers of the LSM tree:
//!   - Base table (on-disk, compacted data)
//!   - SSTables (on-disk L0)
//!   - Active MemTable (in-memory write buffer)
//!
//! Two phases, selected with `--phase`:
//!
//!   --phase prepare   Create the base dataset and initialize MemWAL.
//!   --phase lookup    Open the dataset, ingest rows across LSM levels via
//!                     ShardWriter, then time point lookups per category.
//!
//! Example:
//!
//! ```bash
//! cargo bench -p lance --bench mem_wal_point_lookup_bench -- \
//!   --phase prepare --uri /tmp/pk_lookup_bench
//!
//! cargo bench -p lance --bench mem_wal_point_lookup_bench -- \
//!   --phase lookup --uri /tmp/pk_lookup_bench \
//!   --base-rows 1000000 --max-memtable-rows 100000 \
//!   --queries 500 --output result.json
//! ```

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::ScalarValue;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use lance::dataset::mem_wal::scanner::{
    ActiveMemTableRef, LsmDataSourceCollector, LsmPointLookupPlanner, ShardSnapshot,
};
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriterConfig};
use lance::dataset::{Dataset, WriteParams};
use lance_core::Result;
use serde_json::json;
use uuid::Uuid;

// ----------------------------------------------------------------------
// Schema / batch helpers
// ----------------------------------------------------------------------

fn make_schema() -> Arc<ArrowSchema> {
    let mut id_meta = HashMap::new();
    id_meta.insert(
        "lance-schema:unenforced-primary-key".to_string(),
        "true".to_string(),
    );
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(id_meta),
        Field::new("payload", DataType::Utf8, true),
    ]))
}

fn make_payload(id: i64) -> String {
    format!("{:0>100x}", id as u64)
}

fn make_batch(schema: Arc<ArrowSchema>, start_id: i64, count: usize) -> RecordBatch {
    let ids: Vec<i64> = (start_id..start_id + count as i64).collect();
    let payloads: Vec<String> = ids.iter().map(|id| make_payload(*id)).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(payloads)),
        ],
    )
    .unwrap()
}

// ----------------------------------------------------------------------
// Latency stats
// ----------------------------------------------------------------------

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((pct / 100.0) * (sorted.len().saturating_sub(1)) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

struct LatencyStats {
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    mean_us: f64,
    qps: f64,
}

fn compute_stats(mut latencies_us: Vec<f64>) -> LatencyStats {
    latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mean = latencies_us.iter().sum::<f64>() / latencies_us.len().max(1) as f64;
    let total_s = latencies_us.iter().sum::<f64>() / 1_000_000.0;
    let qps = if total_s > 0.0 {
        latencies_us.len() as f64 / total_s
    } else {
        0.0
    };
    LatencyStats {
        p50_us: percentile(&latencies_us, 50.0) as u64,
        p95_us: percentile(&latencies_us, 95.0) as u64,
        p99_us: percentile(&latencies_us, 99.0) as u64,
        mean_us: mean,
        qps,
    }
}

// ----------------------------------------------------------------------
// CLI args
// ----------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Prepare,
    Lookup,
}

impl Phase {
    fn parse(value: &str) -> std::result::Result<Self, String> {
        match value {
            "prepare" => Ok(Self::Prepare),
            "lookup" => Ok(Self::Lookup),
            _ => Err(format!("unknown phase '{value}', expected prepare|lookup")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Prepare => "prepare",
            Self::Lookup => "lookup",
        }
    }
}

#[derive(Debug, Clone)]
struct Args {
    phase: Phase,
    uri: String,
    base_rows: usize,
    max_memtable_rows: usize,
    sstables: usize,
    batch_rows: usize,
    queries: usize,
    output: Option<PathBuf>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            phase: Phase::Lookup,
            uri: String::new(),
            base_rows: 1_000_000,
            max_memtable_rows: 100_000,
            sstables: 2,
            batch_rows: 1_000,
            queries: 500,
            output: None,
        }
    }
}

fn parse_val<T>(flag: &str, value: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|e| lance_core::Error::invalid_input(format!("invalid {flag}: {value} ({e})")))
}

fn parse_args() -> Result<Args> {
    let mut args = Args::default();
    let mut iter = std::env::args().skip(1);
    let mut has_phase = false;
    let mut has_uri = false;
    while let Some(flag) = iter.next() {
        if flag == "--bench" {
            continue;
        }
        let value = iter
            .next()
            .ok_or_else(|| lance_core::Error::invalid_input(format!("missing value for {flag}")))?;
        match flag.as_str() {
            "--phase" => {
                args.phase = Phase::parse(&value).map_err(lance_core::Error::invalid_input)?;
                has_phase = true;
            }
            "--uri" => {
                args.uri = value;
                has_uri = true;
            }
            "--base-rows" => args.base_rows = parse_val(&flag, &value)?,
            "--max-memtable-rows" => args.max_memtable_rows = parse_val(&flag, &value)?,
            "--sstables" => args.sstables = parse_val(&flag, &value)?,
            "--batch-rows" => args.batch_rows = parse_val(&flag, &value)?,
            "--queries" => args.queries = parse_val(&flag, &value)?,
            "--output" => args.output = Some(PathBuf::from(value)),
            _ => {
                return Err(lance_core::Error::invalid_input(format!(
                    "unknown argument: {flag}"
                )));
            }
        }
    }
    if !has_phase {
        return Err(lance_core::Error::invalid_input(
            "--phase is required (prepare|lookup)",
        ));
    }
    if !has_uri {
        return Err(lance_core::Error::invalid_input("--uri is required"));
    }
    if args.batch_rows == 0 || args.base_rows == 0 || args.max_memtable_rows == 0 {
        return Err(lance_core::Error::invalid_input(
            "base-rows, max-memtable-rows, batch-rows must be > 0",
        ));
    }
    Ok(args)
}

// ----------------------------------------------------------------------
// Prepare phase
// ----------------------------------------------------------------------

async fn run_prepare(args: &Args) -> Result<()> {
    let schema = make_schema();
    let total_batches = args.base_rows.div_ceil(args.batch_rows);
    let mut batches = Vec::with_capacity(total_batches);
    for i in 0..total_batches {
        let start = (i * args.batch_rows) as i64;
        let rows = args.batch_rows.min(args.base_rows - i * args.batch_rows);
        batches.push(Ok(make_batch(schema.clone(), start, rows)));
    }
    let reader = RecordBatchIterator::new(batches.into_iter(), schema.clone());
    let start = Instant::now();
    let mut dataset = Dataset::write(
        reader,
        &args.uri,
        Some(WriteParams {
            data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
            ..Default::default()
        }),
    )
    .await?;
    let write_s = start.elapsed().as_secs_f64();
    println!("wrote {} base rows in {write_s:.1}s", args.base_rows);

    dataset.initialize_mem_wal().execute().await?;
    println!("initialized MemWAL");
    println!("prepare complete: uri={}", args.uri);
    Ok(())
}

// ----------------------------------------------------------------------
// Lookup phase
// ----------------------------------------------------------------------

fn is_cloud_uri(uri: &str) -> bool {
    uri.starts_with("s3://") || uri.starts_with("gs://") || uri.starts_with("az://")
}

fn generate_lookup_ids(
    base_rows: usize,
    max_memtable_rows: usize,
    sstables: usize,
    queries: usize,
) -> (Vec<Vec<i64>>, Vec<&'static str>) {
    let sstable_total = sstables * max_memtable_rows;
    let active_start = base_rows + sstable_total;
    let active_end = active_start + max_memtable_rows / 2;

    let mut groups = Vec::new();
    let mut names = Vec::new();

    // Base table IDs
    let base_ids: Vec<i64> = (0..queries)
        .map(|i| {
            let step = base_rows.max(1) / queries.max(1);
            ((i * step) % base_rows) as i64
        })
        .collect();
    groups.push(base_ids);
    names.push("base");

    // SSTable IDs (only if there are SSTables)
    if sstables > 0 {
        let sstable_start = base_rows;
        let sstable_end = base_rows + sstable_total;
        let sstable_ids: Vec<i64> = (0..queries)
            .map(|i| {
                let range = sstable_end - sstable_start;
                let step = range.max(1) / queries.max(1);
                (sstable_start + (i * step) % range) as i64
            })
            .collect();
        groups.push(sstable_ids);
        names.push("sstable");
    }

    // Active memtable IDs
    let active_ids: Vec<i64> = (0..queries)
        .map(|i| {
            let range = active_end - active_start;
            let step = range.max(1) / queries.max(1);
            (active_start + (i * step) % range) as i64
        })
        .collect();
    groups.push(active_ids);
    names.push("active");

    (groups, names)
}

async fn run_lookup(args: &Args) -> Result<serde_json::Value> {
    let dataset = Arc::new(Dataset::open(&args.uri).await?);
    let arrow_schema: Arc<ArrowSchema> = Arc::new(ArrowSchema::from(dataset.schema()));
    let schema = make_schema();

    let shard_id = Uuid::new_v4();
    let max_memtable_rows = args.max_memtable_rows;
    let config = ShardWriterConfig {
        shard_id,
        max_wal_persist_retries: 3,
        wal_persist_retry_base_delay: std::time::Duration::from_millis(50),
        shard_spec_id: 0,
        durable_write: false,
        max_memtable_size: max_memtable_rows * 200,
        max_memtable_rows,
        max_wal_flush_interval: Some(Duration::from_secs(60)),
        ..ShardWriterConfig::default()
    };

    let writer = dataset.mem_wal_writer(shard_id, config).await?;

    let flush_wait = if is_cloud_uri(&args.uri) {
        Duration::from_secs(5)
    } else {
        Duration::from_millis(500)
    };

    let id_base = args.base_rows as i64;
    let num_sstables = args.sstables;
    let active_rows = max_memtable_rows / 2;

    // Ingest SSTables (each triggers a flush) + 1 active (50% full)
    let mut gen_sizes: Vec<usize> = (0..num_sstables).map(|_| max_memtable_rows).collect();
    gen_sizes.push(active_rows);

    let mut cursor = 0usize;
    for (gen_idx, &gen_rows) in gen_sizes.iter().enumerate() {
        let gen_batches = gen_rows.div_ceil(args.batch_rows);
        for b in 0..gen_batches {
            let start = id_base + cursor as i64;
            let rows = args.batch_rows.min(gen_rows - b * args.batch_rows);
            let batch = make_batch(schema.clone(), start, rows);
            writer.put(vec![batch]).await?;
            cursor += rows;
        }
        let is_sstable = gen_idx < num_sstables;
        println!(
            "  gen {}: wrote {} rows ({}) cursor={}",
            gen_idx + 1,
            gen_rows,
            if is_sstable { "sstable" } else { "active" },
            cursor,
        );
        if is_sstable {
            tokio::time::sleep(flush_wait).await;
        }
    }

    println!(
        "ingested {} rows total ({} SSTables + active)",
        cursor, num_sstables
    );

    let manifest = writer.manifest().await.unwrap();
    let active_ref: ActiveMemTableRef = writer.active_memtable_ref().await.unwrap();

    // Build shard snapshot from manifest
    let mut shard_snapshot = ShardSnapshot::new(shard_id);
    if let Some(ref m) = manifest {
        shard_snapshot = shard_snapshot.with_current_generation(m.current_generation);
        for sstable in &m.sstables {
            shard_snapshot = shard_snapshot.with_sstable(sstable.generation, sstable.path.clone());
        }
    }

    let num_sstables = manifest.as_ref().map(|m| m.sstables.len()).unwrap_or(0);
    println!(
        "manifest: {} SSTables, current_generation={}",
        num_sstables,
        manifest.as_ref().map(|m| m.current_generation).unwrap_or(0)
    );

    // Build the planner
    let collector = LsmDataSourceCollector::new(dataset.clone(), vec![shard_snapshot])
        .with_active_memtable(shard_id, active_ref);
    let pk_columns = vec!["id".to_string()];
    let planner = LsmPointLookupPlanner::new(collector, pk_columns, arrow_schema);

    // Generate lookup IDs for each category
    let (id_groups, category_names) = generate_lookup_ids(
        args.base_rows,
        max_memtable_rows,
        num_sstables,
        args.queries,
    );

    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();

    let mut results = HashMap::new();

    for (cat_idx, (ids, name)) in id_groups.iter().zip(category_names.iter()).enumerate() {
        let mut latencies_us = Vec::with_capacity(ids.len());

        for (qi, &id) in ids.iter().enumerate() {
            let t0 = Instant::now();
            let plan = planner
                .plan_lookup(&[ScalarValue::Int64(Some(id))], None)
                .await?;
            let stream = plan.execute(0, task_ctx.clone())?;
            let batches: Vec<RecordBatch> = stream.try_collect().await?;
            let elapsed_us = t0.elapsed().as_micros() as f64;
            latencies_us.push(elapsed_us);

            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert!(
                total_rows <= 1,
                "point lookup for id={id} returned {total_rows} rows, expected 0 or 1"
            );

            if qi < 3 && cat_idx == 0 {
                println!(
                    "  [warmup {name} q{qi}] id={id} -> {total_rows} row(s) in {elapsed_us:.0}us"
                );
            }
        }

        let stats = compute_stats(latencies_us);
        println!(
            "  {name}: p50={p50}us p95={p95}us p99={p99}us mean={mean:.0}us qps={qps:.0}",
            p50 = stats.p50_us,
            p95 = stats.p95_us,
            p99 = stats.p99_us,
            mean = stats.mean_us,
            qps = stats.qps,
        );

        results.insert(format!("{name}_p50_us"), json!(stats.p50_us));
        results.insert(format!("{name}_p95_us"), json!(stats.p95_us));
        results.insert(format!("{name}_p99_us"), json!(stats.p99_us));
        results.insert(format!("{name}_qps"), json!(stats.qps as u64));
    }

    // Keep the writer alive so the active memtable is reachable during lookups
    std::mem::forget(writer);

    let mut output = serde_json::Map::new();
    output.insert("bench".into(), json!("mem_wal_point_lookup"));
    output.insert("phase".into(), json!("lookup"));
    output.insert("base_rows".into(), json!(args.base_rows));
    output.insert("max_memtable_rows".into(), json!(max_memtable_rows));
    output.insert("sstables".into(), json!(num_sstables));
    output.insert("active_rows".into(), json!(active_rows));
    output.insert("queries_per_category".into(), json!(args.queries));
    for (key, val) in &results {
        output.insert(key.clone(), val.clone());
    }
    Ok(serde_json::Value::Object(output))
}

// ----------------------------------------------------------------------
// Entrypoint
// ----------------------------------------------------------------------

async fn run(args: Args) -> Result<()> {
    println!(
        "bench=mem_wal_point_lookup phase={} uri={} base_rows={} max_memtable_rows={} sstables={} batch_rows={} queries={}",
        args.phase.as_str(),
        args.uri,
        args.base_rows,
        args.max_memtable_rows,
        args.sstables,
        args.batch_rows,
        args.queries,
    );

    match args.phase {
        Phase::Prepare => {
            run_prepare(&args).await?;
        }
        Phase::Lookup => {
            let result = run_lookup(&args).await?;
            let text = serde_json::to_string_pretty(&result)
                .map_err(|e| lance_core::Error::io(format!("serialize: {e}")))?;
            println!("{text}");
            if let Some(path) = &args.output {
                if let Some(parent) = path.parent()
                    && !parent.as_os_str().is_empty()
                {
                    std::fs::create_dir_all(parent).ok();
                }
                std::fs::write(path, text.as_bytes())
                    .map_err(|e| lance_core::Error::io(format!("write {}: {e}", path.display())))?;
            }
        }
    }

    println!("=== DONE ===");
    Ok(())
}

fn main() -> Result<()> {
    let args = parse_args()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| lance_core::Error::io(format!("build runtime: {e}")))?;
    runtime.block_on(run(args))
}
