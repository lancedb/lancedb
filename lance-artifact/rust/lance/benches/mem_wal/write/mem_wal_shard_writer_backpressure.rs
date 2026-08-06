// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Native MemWAL ShardWriter backpressure benchmark.
//!
//! This is the Rust-side equivalent of the LanceDB Python
//! `bench_async.py` / `bench_backpressure.py` runs.  It intentionally bypasses
//! LanceDB and PyO3 so the same FineWeb-shaped payload can be driven straight
//! through Lance's `ShardWriter`.
//!
//! Example:
//!
//! ```bash
//! cargo bench -p lance --bench mem_wal_shard_writer_backpressure -- \
//!   --uri /tmp/memwal-rust-native \
//!   --mode async_idx \
//!   --seed-rows 100000 \
//!   --batch-rows 1000 \
//!   --calls 500 \
//!   --target-rows-per-sec 1800 \
//!   --max-memtable-size 268435456 \
//!   --max-unflushed-memtable-bytes 1073741824 \
//!   --sample-interval-ms 500 \
//!   --threads 64
//! ```

#![recursion_limit = "256"]
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::HashMap;
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{
    ArrayRef, FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch,
    RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use lance::dataset::mem_wal::write::{MemTableStats, WriteStatsSnapshot};
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriterConfig};
use lance::dataset::{Dataset, WriteParams};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_arrow::FixedSizeListArrayExt;
use lance_core::Result;
use lance_index::IndexType;
use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::builder::PQBuildParams;
use lance_linalg::distance::DistanceType;
use serde_json::json;
use uuid::Uuid;

const VECTOR_COL: &str = "vec";
const VECTOR_INDEX_NAME: &str = "vec_idx";
const TEXT_COL: &str = "text";
const FTS_INDEX_NAME: &str = "text_fts";
const TEXT_BYTES: usize = 1_500;
const ROW_BYTES_FINEWEB_SHAPE: usize = 5_760;
const FINEWEB_FIXED_BYTES: usize = ROW_BYTES_FINEWEB_SHAPE - TEXT_BYTES - 1024 * size_of::<f32>();
const VECTOR_ONLY_ID_BYTES: usize = 64;

#[derive(Debug, Clone, Copy)]
enum Mode {
    AsyncNoIndex,
    AsyncIndexed,
    SyncNoIndex,
    SyncIndexed,
}

impl Mode {
    fn parse(value: &str) -> std::result::Result<Self, String> {
        match value {
            "async_noidx" => Ok(Self::AsyncNoIndex),
            "async_idx" => Ok(Self::AsyncIndexed),
            "sync_noidx" => Ok(Self::SyncNoIndex),
            "sync_idx" => Ok(Self::SyncIndexed),
            _ => Err(format!(
                "unknown mode '{value}', expected async_noidx|async_idx|sync_noidx|sync_idx"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::AsyncNoIndex => "async_noidx",
            Self::AsyncIndexed => "async_idx",
            Self::SyncNoIndex => "sync_noidx",
            Self::SyncIndexed => "sync_idx",
        }
    }

    fn indexed(self) -> bool {
        matches!(self, Self::AsyncIndexed | Self::SyncIndexed)
    }

    fn durable_write(self) -> bool {
        matches!(self, Self::SyncNoIndex | Self::SyncIndexed)
    }
}

/// Which index the MemTable maintains in the indexed (`*_idx`) modes.
/// The backpressure methodology — paced ingest, WAL-queue sampling,
/// skip-close — is identical for both; only the indexed column and the
/// index built differ, so vector and FTS results are directly comparable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexKind {
    Vector,
    Fts,
}

impl IndexKind {
    fn parse(value: &str) -> std::result::Result<Self, String> {
        match value {
            "vector" => Ok(Self::Vector),
            "fts" => Ok(Self::Fts),
            _ => Err(format!("unknown index-type '{value}', expected vector|fts")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Vector => "vector",
            Self::Fts => "fts",
        }
    }

    fn index_name(self) -> &'static str {
        match self {
            Self::Vector => VECTOR_INDEX_NAME,
            Self::Fts => FTS_INDEX_NAME,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaShape {
    FineWeb,
    VectorOnly,
}

impl SchemaShape {
    fn parse(value: &str) -> std::result::Result<Self, String> {
        match value {
            "fineweb" => Ok(Self::FineWeb),
            "vector_only" => Ok(Self::VectorOnly),
            _ => Err(format!(
                "unknown schema shape '{value}', expected fineweb|vector_only"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::FineWeb => "fineweb",
            Self::VectorOnly => "vector_only",
        }
    }

    fn default_row_bytes(self, vector_dim: usize, text_bytes: usize) -> usize {
        let vector_bytes = vector_dim.saturating_mul(size_of::<f32>());
        match self {
            Self::FineWeb => text_bytes
                .saturating_add(vector_bytes)
                .saturating_add(FINEWEB_FIXED_BYTES),
            Self::VectorOnly => vector_bytes.saturating_add(VECTOR_ONLY_ID_BYTES),
        }
    }
}

#[derive(Debug, Clone)]
struct Args {
    uri: Option<String>,
    mode: Mode,
    index_kind: IndexKind,
    schema_shape: SchemaShape,
    seed_rows: usize,
    batch_rows: usize,
    calls: usize,
    vector_dim: usize,
    text_bytes: usize,
    row_bytes: usize,
    max_memtable_size: usize,
    max_unflushed_memtable_bytes: usize,
    max_memtable_rows: Option<usize>,
    max_memtable_batches: Option<usize>,
    max_wal_buffer_size: usize,
    max_wal_flush_interval_ms: u64,
    sample_interval_ms: u64,
    target_rows_per_sec: Option<f64>,
    num_partitions: usize,
    num_sub_vectors: usize,
    threads: usize,
    tokio_threads: usize,
    skip_close: bool,
    output: Option<PathBuf>,
}

impl Default for Args {
    fn default() -> Self {
        let threads = std::thread::available_parallelism().map_or(1, usize::from);
        Self {
            uri: None,
            mode: Mode::AsyncIndexed,
            index_kind: IndexKind::Vector,
            schema_shape: SchemaShape::FineWeb,
            seed_rows: 100_000,
            batch_rows: 1_000,
            calls: 500,
            vector_dim: 1024,
            text_bytes: TEXT_BYTES,
            row_bytes: ROW_BYTES_FINEWEB_SHAPE,
            max_memtable_size: 256 * 1024 * 1024,
            max_unflushed_memtable_bytes: 1024 * 1024 * 1024,
            max_memtable_rows: None,
            max_memtable_batches: None,
            max_wal_buffer_size: 10 * 1024 * 1024,
            max_wal_flush_interval_ms: 100,
            sample_interval_ms: 500,
            target_rows_per_sec: None,
            num_partitions: 1,
            num_sub_vectors: 8,
            threads,
            tokio_threads: threads,
            skip_close: false,
            output: None,
        }
    }
}

fn main() -> Result<()> {
    let args = parse_args()?;
    if args.threads > 0 {
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(args.threads)
            .build_global();
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.tokio_threads.max(1))
        .enable_all()
        .build()
        .map_err(|e| lance_core::Error::io(format!("build tokio runtime: {e}")))?;

    runtime.block_on(run(args))
}

async fn run(args: Args) -> Result<()> {
    let memtable_limits = effective_memtable_limits(&args);
    let temp_dir = if args.uri.is_none() {
        Some(
            tempfile::tempdir()
                .map_err(|e| lance_core::Error::io(format!("create temp dir: {e}")))?,
        )
    } else {
        None
    };
    let uri = match &args.uri {
        Some(uri) => uri.clone(),
        None => temp_dir
            .as_ref()
            .expect("temporary directory must be initialized")
            .path()
            .join("memwal_native.lance")
            .display()
            .to_string(),
    };

    println!(
        "bench=mem_wal_shard_writer_backpressure uri={} mode={} index_type={} schema_shape={} seed_rows={} batch_rows={} calls={} vector_dim={} text_bytes={} row_bytes={} target_rows_per_sec={:?} max_memtable_size={} max_memtable_rows={} max_memtable_batches={} max_unflushed_memtable_bytes={} max_wal_buffer_size={} max_wal_flush_interval_ms={} rayon_threads={} tokio_threads={} skip_close={}",
        uri,
        args.mode.as_str(),
        args.index_kind.as_str(),
        args.schema_shape.as_str(),
        args.seed_rows,
        args.batch_rows,
        args.calls,
        args.vector_dim,
        args.text_bytes,
        args.row_bytes,
        args.target_rows_per_sec,
        args.max_memtable_size,
        memtable_limits.rows,
        memtable_limits.batches,
        args.max_unflushed_memtable_bytes,
        args.max_wal_buffer_size,
        args.max_wal_flush_interval_ms,
        rayon::current_num_threads(),
        args.tokio_threads,
        args.skip_close,
    );

    let schema = schema_for_shape(args.schema_shape, args.vector_dim);

    let setup_start = Instant::now();
    let seed_batch = make_batch(
        args.schema_shape,
        schema.as_ref(),
        "seed",
        0,
        args.seed_rows,
        args.vector_dim,
        args.text_bytes,
    )?;
    let batches = RecordBatchIterator::new([Ok(seed_batch)], schema.clone());
    let mut dataset = Dataset::write(
        batches,
        &uri,
        Some(WriteParams {
            data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
            ..Default::default()
        }),
    )
    .await?;

    let index_start = Instant::now();
    if args.mode.indexed() {
        match args.index_kind {
            IndexKind::Vector => create_base_vector_index(&mut dataset, &args).await?,
            IndexKind::Fts => create_base_fts_index(&mut dataset).await?,
        }
    }
    let index_setup_s = index_start.elapsed().as_secs_f64();

    dataset
        .initialize_mem_wal()
        .maintained_indexes(if args.mode.indexed() {
            vec![args.index_kind.index_name().to_string()]
        } else {
            vec![]
        })
        .execute()
        .await?;

    let shard_id = Uuid::new_v4();
    let mut config = ShardWriterConfig::new(shard_id)
        .with_durable_write(args.mode.durable_write())
        .with_max_memtable_size(args.max_memtable_size)
        .with_max_unflushed_memtable_bytes(args.max_unflushed_memtable_bytes)
        .with_max_wal_buffer_size(args.max_wal_buffer_size)
        .with_max_memtable_rows(memtable_limits.rows)
        .with_max_memtable_batches(memtable_limits.batches);
    if args.max_wal_flush_interval_ms == 0 {
        config.max_wal_flush_interval = None;
    } else {
        config = config
            .with_max_wal_flush_interval(Duration::from_millis(args.max_wal_flush_interval_ms));
    }

    let writer = dataset.mem_wal_writer(shard_id, config).await?;
    let setup_s = setup_start.elapsed().as_secs_f64();

    let stats_handle = writer.stats_handle();
    let mut latencies_ms = Vec::with_capacity(args.calls);
    let mut puts = Vec::with_capacity(args.calls);
    let mut samples = Vec::new();
    let mut batch_build_s = 0.0_f64;
    let sample_interval = if args.sample_interval_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(args.sample_interval_ms))
    };
    let mut next_sample_at = Duration::ZERO;

    let puts_start = Instant::now();
    for call in 0..args.calls {
        let batch_start = Instant::now();
        let batch = make_batch(
            args.schema_shape,
            schema.as_ref(),
            "new",
            args.seed_rows + call * args.batch_rows,
            args.batch_rows,
            args.vector_dim,
            args.text_bytes,
        )?;
        batch_build_s += batch_start.elapsed().as_secs_f64();

        let put_start = Instant::now();
        writer.put(vec![batch]).await?;
        let put_latency_ms = put_start.elapsed().as_secs_f64() * 1000.0;
        let elapsed = puts_start.elapsed();
        latencies_ms.push(put_latency_ms);
        puts.push(json!({
            "i": call,
            "t": elapsed.as_secs_f64(),
            "lat_ms": put_latency_ms,
        }));

        if let Some(interval) = sample_interval
            && elapsed >= next_sample_at
        {
            push_sample(
                &mut samples,
                "puts",
                elapsed,
                &stats_handle.snapshot(),
                writer.memtable_stats().await.ok(),
            );
            while next_sample_at <= elapsed {
                next_sample_at += interval;
            }
        }

        if let Some(target) = args.target_rows_per_sec
            && target > 0.0
        {
            let target_elapsed = ((call + 1) * args.batch_rows) as f64 / target;
            let actual_elapsed = puts_start.elapsed().as_secs_f64();
            if target_elapsed > actual_elapsed {
                tokio::time::sleep(Duration::from_secs_f64(target_elapsed - actual_elapsed)).await;
            }
        }
    }
    let elapsed_puts_s = puts_start.elapsed().as_secs_f64();
    let final_memtable_stats = writer.memtable_stats().await.ok();
    push_sample(
        &mut samples,
        "puts_done",
        puts_start.elapsed(),
        &stats_handle.snapshot(),
        final_memtable_stats.clone(),
    );

    let (elapsed_drain_s, elapsed_total_s, stats) = if args.skip_close {
        let stats = stats_handle.snapshot();
        push_sample(
            &mut samples,
            "close_skipped",
            puts_start.elapsed(),
            &stats,
            final_memtable_stats.clone(),
        );
        (0.0, elapsed_puts_s, stats)
    } else {
        let close_start = Instant::now();
        writer.close().await?;
        let elapsed_drain_s = close_start.elapsed().as_secs_f64();
        let elapsed_total_s = puts_start.elapsed().as_secs_f64();
        let stats = stats_handle.snapshot();
        push_sample(&mut samples, "closed", puts_start.elapsed(), &stats, None);
        (elapsed_drain_s, elapsed_total_s, stats)
    };

    let rows = args.calls * args.batch_rows;
    let puts_rows_s = rows as f64 / elapsed_puts_s;
    let total_rows_s = rows as f64 / elapsed_total_s;
    let puts_mb_s = puts_rows_s * args.row_bytes as f64 / 1_000_000.0;
    let total_mb_s = total_rows_s * args.row_bytes as f64 / 1_000_000.0;
    let result_rows_s = if args.skip_close {
        puts_rows_s
    } else {
        total_rows_s
    };
    let result_mb_s = if args.skip_close {
        puts_mb_s
    } else {
        total_mb_s
    };

    let p50_ms = percentile(&latencies_ms, 50.0);
    let p90_ms = percentile(&latencies_ms, 90.0);
    let p99_ms = percentile(&latencies_ms, 99.0);
    let slow_puts_1s = latencies_ms.iter().filter(|ms| **ms >= 1_000.0).count();
    let slow_puts_10s = latencies_ms.iter().filter(|ms| **ms >= 10_000.0).count();
    let final_wal_pending_batches = final_memtable_stats
        .as_ref()
        .map_or(0, |stats| stats.pending_wal_batch_count);
    let final_wal_pending_rows = final_memtable_stats
        .as_ref()
        .map_or(0, |stats| stats.pending_wal_row_count);
    let final_wal_pending_mb = final_memtable_stats.as_ref().map_or(0.0, |stats| {
        stats.pending_wal_estimated_bytes as f64 / 1_000_000.0
    });
    let final_memtable_rows = final_memtable_stats
        .as_ref()
        .map_or(0, |stats| stats.row_count);
    let final_memtable_batches = final_memtable_stats
        .as_ref()
        .map_or(0, |stats| stats.batch_count);

    println!(
        "result mode={} rows={} result_rows_s={:.1} result_mb_s={:.2} puts_rows_s={:.1} drained_rows_s={:.1} puts_mb_s={:.2} drained_mb_s={:.2} setup_s={:.3} index_setup_s={:.3} batch_build_s={:.3} puts_s={:.3} drain_s={:.3} total_s={:.3} skip_close={} p50_ms={:.2} p90_ms={:.2} p99_ms={:.2} slow_puts_1s={} slow_puts_10s={} wal_flushes={} final_wal_pending_batches={} final_wal_pending_rows={} final_wal_pending_mb={:.2} final_memtable_rows={} final_memtable_batches={} index_update_s={:.3} memtable_flush_s={:.3}",
        args.mode.as_str(),
        rows,
        result_rows_s,
        result_mb_s,
        puts_rows_s,
        total_rows_s,
        puts_mb_s,
        total_mb_s,
        setup_s,
        index_setup_s,
        batch_build_s,
        elapsed_puts_s,
        elapsed_drain_s,
        elapsed_total_s,
        args.skip_close,
        p50_ms,
        p90_ms,
        p99_ms,
        slow_puts_1s,
        slow_puts_10s,
        stats.wal_flush_count,
        final_wal_pending_batches,
        final_wal_pending_rows,
        final_wal_pending_mb,
        final_memtable_rows,
        final_memtable_batches,
        stats.index_update_time.as_secs_f64(),
        stats.memtable_flush_time.as_secs_f64(),
    );

    let output = json!({
        "uri": uri,
        "mode": args.mode.as_str(),
        "index_type": args.index_kind.as_str(),
        "schema_shape": args.schema_shape.as_str(),
        "seed_rows": args.seed_rows,
        "batch_rows": args.batch_rows,
        "calls": args.calls,
        "total_rows_written": rows,
        "vector_dim": args.vector_dim,
        "text_bytes": args.text_bytes,
        "row_bytes": args.row_bytes,
        "target_rows_per_sec": args.target_rows_per_sec,
        "rayon_threads": rayon::current_num_threads(),
        "tokio_threads": args.tokio_threads,
        "max_memtable_size": args.max_memtable_size,
        "max_unflushed_memtable_bytes": args.max_unflushed_memtable_bytes,
        "requested_max_memtable_rows": args.max_memtable_rows,
        "requested_max_memtable_batches": args.max_memtable_batches,
        "max_memtable_rows": memtable_limits.rows,
        "max_memtable_batches": memtable_limits.batches,
        "max_wal_buffer_size": args.max_wal_buffer_size,
        "max_wal_flush_interval_ms": args.max_wal_flush_interval_ms,
        "sample_interval_ms": args.sample_interval_ms,
        "skip_close": args.skip_close,
        "setup_seconds": setup_s,
        "index_setup_seconds": index_setup_s,
        "batch_build_seconds": batch_build_s,
        "elapsed_puts_seconds": elapsed_puts_s,
        "elapsed_drain_seconds": elapsed_drain_s,
        "elapsed_total_seconds": elapsed_total_s,
        "result_rows_per_sec": result_rows_s,
        "result_mb_per_sec": result_mb_s,
        "throughput_puts_rows_per_sec": puts_rows_s,
        "throughput_rows_per_sec": total_rows_s,
        "throughput_puts_mb_per_sec": puts_mb_s,
        "throughput_mb_per_sec": total_mb_s,
        "p50_ms": p50_ms,
        "p90_ms": p90_ms,
        "p99_ms": p99_ms,
        "slow_puts_1s": slow_puts_1s,
        "slow_puts_10s": slow_puts_10s,
        "final_memtable_stats": memtable_stats_json(final_memtable_stats.as_ref()),
        "puts": puts,
        "samples": samples,
        "write_stats": {
            "put_count": stats.put_count,
            "put_time_seconds": stats.put_time.as_secs_f64(),
            "wal_flush_count": stats.wal_flush_count,
            "wal_flush_time_seconds": stats.wal_flush_time.as_secs_f64(),
            "wal_flush_bytes": stats.wal_flush_bytes,
            "wal_io_count": stats.wal_io_count,
            "wal_io_time_seconds": stats.wal_io_time.as_secs_f64(),
            "index_update_count": stats.index_update_count,
            "index_update_time_seconds": stats.index_update_time.as_secs_f64(),
            "index_update_rows": stats.index_update_rows,
            "memtable_flush_count": stats.memtable_flush_count,
            "memtable_flush_time_seconds": stats.memtable_flush_time.as_secs_f64(),
            "memtable_flush_rows": stats.memtable_flush_rows,
        },
    });

    let json_text = serde_json::to_string_pretty(&output)
        .map_err(|e| lance_core::Error::io(format!("serialize output JSON: {e}")))?;
    println!("{json_text}");
    if let Some(path) = args.output {
        std::fs::write(&path, json_text)
            .map_err(|e| lance_core::Error::io(format!("write {}: {e}", path.display())))?;
    }

    Ok(())
}

async fn create_base_vector_index(dataset: &mut Dataset, args: &Args) -> Result<()> {
    let ivf_params = IvfBuildParams::new(args.num_partitions);
    let pq_params = PQBuildParams::new(args.num_sub_vectors, 8);
    let vector_params =
        VectorIndexParams::with_ivf_pq_params(DistanceType::L2, ivf_params, pq_params);
    dataset
        .create_index(
            &[VECTOR_COL],
            IndexType::IvfPq,
            Some(VECTOR_INDEX_NAME.to_string()),
            &vector_params,
            true,
        )
        .await
        .map(|_| ())
}

async fn create_base_fts_index(dataset: &mut Dataset) -> Result<()> {
    dataset
        .create_index(
            &[TEXT_COL],
            IndexType::Inverted,
            Some(FTS_INDEX_NAME.to_string()),
            &InvertedIndexParams::default(),
            true,
        )
        .await
        .map(|_| ())
}

fn push_sample(
    samples: &mut Vec<serde_json::Value>,
    phase: &'static str,
    elapsed: Duration,
    stats: &WriteStatsSnapshot,
    memtable: Option<MemTableStats>,
) {
    samples.push(json!({
        "phase": phase,
        "t": elapsed.as_secs_f64(),
        "put_count": stats.put_count,
        "wal_flush_count": stats.wal_flush_count,
        "wal_flush_bytes": stats.wal_flush_bytes,
        "wal_io_seconds": stats.wal_io_time.as_secs_f64(),
        "index_update_count": stats.index_update_count,
        "index_update_seconds": stats.index_update_time.as_secs_f64(),
        "index_update_rows": stats.index_update_rows,
        "memtable_flush_count": stats.memtable_flush_count,
        "memtable_flush_seconds": stats.memtable_flush_time.as_secs_f64(),
        "memtable_flush_rows": stats.memtable_flush_rows,
        "active_memtable_rows": memtable.as_ref().map(|stats| stats.row_count),
        "active_memtable_batches": memtable.as_ref().map(|stats| stats.batch_count),
        "active_memtable_bytes": memtable.as_ref().map(|stats| stats.estimated_size),
        "active_memtable_generation": memtable.as_ref().map(|stats| stats.generation),
        "active_memtable_max_buffered_batch_position": memtable.as_ref().and_then(|stats| stats.max_buffered_batch_position),
        "active_memtable_durable_batch_count": memtable.as_ref().map(|stats| stats.durable_batch_count),
        "wal_queue_pending_batches": memtable.as_ref().map(|stats| stats.pending_wal_batch_count),
        "wal_queue_pending_rows": memtable.as_ref().map(|stats| stats.pending_wal_row_count),
        "wal_queue_pending_bytes": memtable.as_ref().map(|stats| stats.pending_wal_estimated_bytes),
        "wal_queue_pending_start_batch_position": memtable.as_ref().and_then(|stats| stats.pending_wal_start_batch_position),
        "wal_queue_pending_end_batch_position": memtable.as_ref().and_then(|stats| stats.pending_wal_end_batch_position),
    }));
}

fn memtable_stats_json(memtable: Option<&MemTableStats>) -> serde_json::Value {
    match memtable {
        Some(stats) => json!({
            "row_count": stats.row_count,
            "batch_count": stats.batch_count,
            "estimated_size": stats.estimated_size,
            "generation": stats.generation,
            "max_buffered_batch_position": stats.max_buffered_batch_position,
            "durable_batch_count": stats.durable_batch_count,
            "wal_queue_pending_start_batch_position": stats.pending_wal_start_batch_position,
            "wal_queue_pending_end_batch_position": stats.pending_wal_end_batch_position,
            "wal_queue_pending_batches": stats.pending_wal_batch_count,
            "wal_queue_pending_rows": stats.pending_wal_row_count,
            "wal_queue_pending_bytes": stats.pending_wal_estimated_bytes,
        }),
        None => serde_json::Value::Null,
    }
}

fn schema_for_shape(shape: SchemaShape, vector_dim: usize) -> Arc<ArrowSchema> {
    match shape {
        SchemaShape::FineWeb => fineweb_schema(vector_dim),
        SchemaShape::VectorOnly => vector_only_schema(vector_dim),
    }
}

fn id_field() -> Field {
    let mut id_metadata = HashMap::new();
    id_metadata.insert(
        "lance-schema:unenforced-primary-key".to_string(),
        "true".to_string(),
    );
    Field::new("id", DataType::Utf8, false).with_metadata(id_metadata)
}

fn vector_field(vector_dim: usize) -> Field {
    Field::new(
        VECTOR_COL,
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            vector_dim as i32,
        ),
        false,
    )
}

fn fineweb_schema(vector_dim: usize) -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        id_field(),
        Field::new("text", DataType::Utf8, false),
        Field::new("dump", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("file_path", DataType::Utf8, false),
        Field::new("language", DataType::Utf8, false),
        Field::new("language_score", DataType::Float64, false),
        Field::new("token_count", DataType::Int64, false),
        vector_field(vector_dim),
    ]))
}

fn vector_only_schema(vector_dim: usize) -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![id_field(), vector_field(vector_dim)]))
}

/// Deterministic pseudo-random text of roughly `target_bytes` bytes for
/// row `row`. Drawn word-by-word from a small fixed vocabulary so the
/// `text` column carries a realistic token distribution — essential for
/// the FTS index to do representative work, and harmless for the vector
/// runs where `text` is inert payload of the same size.
fn gen_text(row: usize, target_bytes: usize) -> String {
    const VOCAB: &[&str] = &[
        "data",
        "vector",
        "search",
        "index",
        "query",
        "memory",
        "table",
        "write",
        "read",
        "shard",
        "stream",
        "batch",
        "flush",
        "token",
        "score",
        "model",
        "system",
        "record",
        "format",
        "column",
        "engine",
        "result",
        "filter",
        "metric",
        "latency",
        "throughput",
        "cache",
        "buffer",
        "segment",
        "lance",
        "arrow",
        "schema",
        "field",
        "value",
        "object",
        "store",
        "cloud",
        "remote",
        "local",
        "build",
        "merge",
        "scan",
        "rank",
        "match",
        "phrase",
        "fuzzy",
        "boolean",
        "recall",
        "corpus",
        "document",
        "passage",
        "sentence",
        "language",
        "machine",
        "learning",
        "training",
        "dataset",
        "feature",
        "embedding",
        "cluster",
        "graph",
        "node",
        "edge",
        "path",
        "weight",
        "layer",
        "tensor",
        "kernel",
    ];
    // SplitMix64-ish deterministic generator seeded by the row index.
    let mut state = (row as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(1);
    let mut next = || {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let mut out = String::with_capacity(target_bytes + 16);
    while out.len() < target_bytes {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(VOCAB[(next() as usize) % VOCAB.len()]);
    }
    out.truncate(target_bytes);
    out
}

fn make_batch(
    shape: SchemaShape,
    schema: &ArrowSchema,
    id_prefix: &str,
    start_row: usize,
    num_rows: usize,
    vector_dim: usize,
    text_bytes: usize,
) -> Result<RecordBatch> {
    let ids = StringArray::from_iter_values(
        (0..num_rows).map(|i| format!("{id_prefix}-{:012}", start_row + i)),
    );
    let mut vectors = Vec::with_capacity(num_rows * vector_dim);
    for row in start_row..start_row + num_rows {
        let cluster = row % 4096;
        let row_noise = ((row / 4096) % 97) as f32 * 0.0001;
        for dim in 0..vector_dim {
            let mixed = cluster
                .wrapping_mul(1_103_515_245)
                .wrapping_add(dim.wrapping_mul(12_345));
            vectors.push(((mixed & 0xffff) as f32 / 65_536.0) + row_noise);
        }
    }
    let vector_values = Float32Array::from(vectors);
    let vector_array = FixedSizeListArray::try_new_from_values(vector_values, vector_dim as i32)?;

    if shape == SchemaShape::VectorOnly {
        return RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(ids) as ArrayRef,
                Arc::new(vector_array) as ArrayRef,
            ],
        )
        .map_err(Into::into);
    }

    let text =
        StringArray::from_iter_values((0..num_rows).map(|i| gen_text(start_row + i, text_bytes)));
    let dump = StringArray::from_iter_values((0..num_rows).map(|i| match (start_row + i) % 5 {
        0 => "CC-MAIN-2023-50",
        1 => "CC-MAIN-2024-10",
        2 => "CC-MAIN-2024-26",
        3 => "CC-MAIN-2024-42",
        _ => "CC-MAIN-2025-05",
    }));
    let url = StringArray::from_iter_values(
        (0..num_rows).map(|i| format!("https://example.org/doc/{:012}", start_row + i)),
    );
    let file_path = StringArray::from_iter_values((0..num_rows).map(|i| {
        let row = start_row + i;
        format!("s3://bucket/cc/{:06}/{:012}.warc.gz", row / 1000, row)
    }));
    let language = StringArray::from_iter_values((0..num_rows).map(|_| "en"));
    let language_score = Float64Array::from_iter_values(
        (0..num_rows).map(|i| 0.85 + (((start_row + i) % 140) as f64 / 1000.0)),
    );
    let token_count =
        Int64Array::from_iter_values((0..num_rows).map(|i| 50 + ((start_row + i) % 4046) as i64));

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(text) as ArrayRef,
            Arc::new(dump) as ArrayRef,
            Arc::new(url) as ArrayRef,
            Arc::new(file_path) as ArrayRef,
            Arc::new(language) as ArrayRef,
            Arc::new(language_score) as ArrayRef,
            Arc::new(token_count) as ArrayRef,
            Arc::new(vector_array) as ArrayRef,
        ],
    )
    .map_err(Into::into)
}

fn percentile(values: &[f64], pct: f64) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((pct / 100.0) * (sorted.len().saturating_sub(1)) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[derive(Debug, Clone, Copy)]
struct EffectiveMemTableLimits {
    rows: usize,
    batches: usize,
}

fn effective_memtable_limits(args: &Args) -> EffectiveMemTableLimits {
    let rows = args.max_memtable_rows.unwrap_or_else(|| {
        let rows_from_bytes = args.max_memtable_size.div_ceil(args.row_bytes);
        rows_from_bytes
            .saturating_mul(2)
            .saturating_add(args.batch_rows)
            .max(args.batch_rows)
            .max(1)
    });
    let batches = args
        .max_memtable_batches
        .unwrap_or_else(|| rows.div_ceil(args.batch_rows).saturating_add(64).max(16));
    EffectiveMemTableLimits { rows, batches }
}

fn parse_args() -> Result<Args> {
    let mut args = Args::default();
    let mut row_bytes_explicit = false;
    let mut iter = std::env::args().skip(1);
    while let Some(flag) = iter.next() {
        if flag == "--bench" {
            continue;
        }
        if flag == "--skip-close" {
            args.skip_close = true;
            continue;
        }
        let value = iter.next().ok_or_else(|| {
            lance_core::Error::invalid_input(format!("missing value for argument {flag}"))
        })?;
        match flag.as_str() {
            "--uri" => args.uri = Some(value),
            "--mode" => {
                args.mode = Mode::parse(&value).map_err(lance_core::Error::invalid_input)?;
            }
            "--index-type" => {
                args.index_kind =
                    IndexKind::parse(&value).map_err(lance_core::Error::invalid_input)?;
            }
            "--schema-shape" => {
                args.schema_shape =
                    SchemaShape::parse(&value).map_err(lance_core::Error::invalid_input)?;
            }
            "--seed-rows" => args.seed_rows = parse(&flag, &value)?,
            "--batch-rows" => args.batch_rows = parse(&flag, &value)?,
            "--calls" => args.calls = parse(&flag, &value)?,
            "--vector-dim" => args.vector_dim = parse(&flag, &value)?,
            "--text-bytes" => args.text_bytes = parse(&flag, &value)?,
            "--row-bytes" => {
                args.row_bytes = parse(&flag, &value)?;
                row_bytes_explicit = true;
            }
            "--max-memtable-size" => args.max_memtable_size = parse(&flag, &value)?,
            "--max-unflushed-memtable-bytes" => {
                args.max_unflushed_memtable_bytes = parse(&flag, &value)?;
            }
            "--max-memtable-rows" => {
                args.max_memtable_rows = parse_optional_nonzero(&flag, &value)?
            }
            "--max-memtable-batches" => {
                args.max_memtable_batches = parse_optional_nonzero(&flag, &value)?
            }
            "--max-wal-buffer-size" => args.max_wal_buffer_size = parse(&flag, &value)?,
            "--max-wal-flush-interval-ms" => {
                args.max_wal_flush_interval_ms = parse(&flag, &value)?;
            }
            "--sample-interval-ms" => args.sample_interval_ms = parse(&flag, &value)?,
            "--target-rows-per-sec" => args.target_rows_per_sec = Some(parse(&flag, &value)?),
            "--num-partitions" => args.num_partitions = parse(&flag, &value)?,
            "--num-sub-vectors" => args.num_sub_vectors = parse(&flag, &value)?,
            "--threads" => args.threads = parse(&flag, &value)?,
            "--tokio-threads" => args.tokio_threads = parse(&flag, &value)?,
            "--output" => args.output = Some(PathBuf::from(value)),
            _ => {
                return Err(lance_core::Error::invalid_input(format!(
                    "unknown argument: {flag}"
                )));
            }
        }
    }

    if !row_bytes_explicit {
        args.row_bytes = args
            .schema_shape
            .default_row_bytes(args.vector_dim, args.text_bytes);
    }

    if args.seed_rows == 0
        || args.batch_rows == 0
        || args.calls == 0
        || args.vector_dim == 0
        || args.row_bytes == 0
        || args.max_memtable_size == 0
        || args.max_unflushed_memtable_bytes == 0
    {
        return Err(lance_core::Error::invalid_input(
            "seed_rows, batch_rows, calls, vector_dim, row_bytes, max_memtable_size, and max_unflushed_memtable_bytes must be greater than 0",
        ));
    }
    if args.schema_shape == SchemaShape::FineWeb && args.text_bytes == 0 {
        return Err(lance_core::Error::invalid_input(
            "text_bytes must be greater than 0 for schema_shape=fineweb",
        ));
    }
    if let Some(max_memtable_rows) = args.max_memtable_rows
        && max_memtable_rows < args.batch_rows
    {
        return Err(lance_core::Error::invalid_input(format!(
            "max_memtable_rows must be at least batch_rows so one batch fits: max_memtable_rows={}, batch_rows={}",
            max_memtable_rows, args.batch_rows
        )));
    }
    if args.mode.indexed()
        && args.index_kind == IndexKind::Vector
        && args.vector_dim % args.num_sub_vectors != 0
    {
        return Err(lance_core::Error::invalid_input(format!(
            "vector_dim must be divisible by num_sub_vectors for IVF_PQ: vector_dim={}, num_sub_vectors={}",
            args.vector_dim, args.num_sub_vectors
        )));
    }
    if args.mode.indexed()
        && args.index_kind == IndexKind::Fts
        && args.schema_shape != SchemaShape::FineWeb
    {
        return Err(lance_core::Error::invalid_input(
            "index-type=fts requires schema-shape=fineweb (it has the text column)",
        ));
    }

    Ok(args)
}

fn parse_optional_nonzero(flag: &str, value: &str) -> Result<Option<usize>> {
    let parsed: usize = parse(flag, value)?;
    Ok((parsed != 0).then_some(parsed))
}

fn parse<T>(flag: &str, value: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value.parse().map_err(|e| {
        lance_core::Error::invalid_input(format!("invalid value for {flag}: {value} ({e})"))
    })
}
