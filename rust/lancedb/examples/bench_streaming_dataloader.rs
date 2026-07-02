// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Benchmark + CPU profiler for the PermutationReader used by the elastic
//! streaming dataloader.
//!
//! Normal sweep:
//!   cargo run --release --example bench_streaming_dataloader
//!
//! Flamegraph (self-contained, no perf/dtrace needed):
//!   BENCH_PROFILE=1 BENCH_CHUNK=64 cargo run --release \
//!       --example bench_streaming_dataloader
//!   # writes flamegraph.svg in the current directory
//!
//! Environment variables:
//!   BENCH_NUM_ROWS   – total rows (default 49152 = 24 × 2048)
//!   BENCH_NUM_SPLITS – number of splits (default 24)
//!   BENCH_STEPS      – round-robin cycles per chunk-size trial (default 200)
//!   BENCH_ROW_BYTES  – bytes of payload per row (default 4096)
//!   BENCH_CHUNK      – restrict sweep to this single chunk size
//!   BENCH_PROFILE    – if set to "1", capture a pprof flamegraph SVG

use std::{sync::Arc, time::Instant};

use arrow_array::{Int32Array, LargeBinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lancedb::{
    Result, Table,
    arrow::{SendableRecordBatchStream, SimpleRecordBatchStream},
    connect,
    dataloader::permutation::{
        builder::{PermutationBuilder, ShuffleStrategy},
        reader::PermutationReader,
        split::{SplitSizes, SplitStrategy},
    },
    query::Select,
};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ---------------------------------------------------------------------------
// Table creation
// ---------------------------------------------------------------------------

async fn make_base_table(num_rows: usize, row_bytes: usize) -> Result<Table> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("payload", DataType::LargeBinary, false),
    ]));
    let payload = vec![0u8; row_bytes];
    let ids: Int32Array = (0..num_rows as i32).collect();
    let payloads: LargeBinaryArray = (0..num_rows).map(|_| Some(payload.as_slice())).collect();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(payloads)])?;
    let stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream::new(
        futures::stream::once(std::future::ready(Ok(batch))),
        schema,
    ));
    let db = connect("memory:///").execute().await?;
    db.create_table("base", stream).execute().await
}

async fn make_permutation_table(base: &Table, num_splits: usize) -> Result<Table> {
    PermutationBuilder::new(base.clone())
        .with_split_strategy(
            SplitStrategy::Random {
                seed: Some(42),
                sizes: SplitSizes::Fixed(num_splits as u64),
                clump_size: None,
            },
            None,
        )
        .with_shuffle_strategy(ShuffleStrategy::Random {
            seed: Some(42),
            clump_size: None,
        })
        .build()
        .await
}

// ---------------------------------------------------------------------------
// Round-robin hot loop (mirrors StreamingDataset.__iter__)
// ---------------------------------------------------------------------------

async fn run_hot_loop(
    readers: &[PermutationReader],
    chunk_size: usize,
    steps: usize,
) -> Result<(usize, f64)> {
    let n = readers.len();
    let split_sizes: Vec<usize> = readers.iter().map(|r| r.count_rows() as usize).collect();

    struct SplitBuf {
        batch: Option<RecordBatch>,
        row_in_batch: usize,
        consumed: usize,
    }
    let mut bufs: Vec<SplitBuf> = (0..n)
        .map(|_| SplitBuf {
            batch: None,
            row_in_batch: 0,
            consumed: 0,
        })
        .collect();

    // Pre-fill
    for i in 0..n {
        let fetch = chunk_size.min(split_sizes[i]);
        if fetch > 0 {
            let offsets: Vec<u64> = (0..fetch as u64).collect();
            bufs[i].batch = Some(readers[i].take_offsets(&offsets, Select::All).await?);
        }
    }

    let mut total_rows = 0usize;
    let t0 = Instant::now();

    'outer: for _step in 0..steps {
        for i in 0..n {
            if bufs[i].consumed >= split_sizes[i] {
                break 'outer;
            }
            let need_refill = bufs[i]
                .batch
                .as_ref()
                .map(|b| bufs[i].row_in_batch >= b.num_rows())
                .unwrap_or(true);
            if need_refill {
                let start = bufs[i].consumed as u64;
                let remaining = (split_sizes[i] - bufs[i].consumed) as u64;
                let fetch = chunk_size.min(remaining as usize);
                let offsets: Vec<u64> = (start..start + fetch as u64).collect();
                bufs[i].batch = Some(readers[i].take_offsets(&offsets, Select::All).await?);
                bufs[i].row_in_batch = 0;
            }
            bufs[i].row_in_batch += 1;
            bufs[i].consumed += 1;
            total_rows += 1;
        }
    }

    Ok((total_rows, t0.elapsed().as_secs_f64()))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let num_splits = env_usize("BENCH_NUM_SPLITS", 24);
    let num_rows = env_usize("BENCH_NUM_ROWS", num_splits * 2048);
    let steps = env_usize("BENCH_STEPS", 200);
    let row_bytes = env_usize("BENCH_ROW_BYTES", 4096);
    let single_chunk: Option<usize> = std::env::var("BENCH_CHUNK")
        .ok()
        .and_then(|v| v.parse().ok());
    let do_profile = std::env::var("BENCH_PROFILE")
        .map(|v| v == "1")
        .unwrap_or(false);

    assert_eq!(
        num_rows % num_splits,
        0,
        "NUM_ROWS must be divisible by NUM_SPLITS"
    );

    println!("Benchmark config:");
    println!(
        "  num_rows={}  num_splits={}  rows/split={}  steps={}  row_bytes={}",
        num_rows,
        num_splits,
        num_rows / num_splits,
        steps,
        row_bytes,
    );
    println!(
        "  ~{:.1} MB total",
        (num_rows * row_bytes) as f64 / (1024.0 * 1024.0)
    );
    println!();

    print!("Building base table... ");
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let base = make_base_table(num_rows, row_bytes).await?;
    println!("done");

    print!("Building permutation table... ");
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let perm = make_permutation_table(&base, num_splits).await?;
    println!("done");

    print!("Building {} PermutationReaders... ", num_splits);
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let base_inner = base.base_table().clone();
    let perm_inner = perm.base_table().clone();
    let mut readers = Vec::with_capacity(num_splits);
    for split in 0..num_splits {
        readers.push(
            PermutationReader::try_from_tables(
                base_inner.clone(),
                perm_inner.clone(),
                split as u64,
            )
            .await?,
        );
    }
    println!("done ({} rows/split)", readers[0].count_rows());
    println!();

    let chunk_sizes: Vec<usize> = if let Some(c) = single_chunk {
        vec![c]
    } else {
        vec![1, 4, 16, 64, 256, 1024, 4096, 16384]
    };

    if do_profile {
        let chunk = chunk_sizes[0];
        println!("Profiling chunk={chunk} for {steps} steps...");
        // Warm-up outside the profiler window
        let _ = run_hot_loop(&readers, chunk, 1).await?;

        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(1000)
            .build()
            .unwrap();

        let (rows, elapsed) = run_hot_loop(&readers, chunk, steps).await?;

        if let Ok(report) = guard.report().build() {
            let svg_path = "flamegraph.svg";
            let file = std::fs::File::create(svg_path).unwrap();
            report.flamegraph(file).unwrap();
            println!("Flamegraph written to {svg_path}");
        }

        let rows_per_sec = rows as f64 / elapsed;
        println!("chunk={chunk}  {rows} rows  {elapsed:.3}s  {rows_per_sec:.0} rows/s");
    } else {
        println!(
            "{:>6}  {:>7}  {:>8}  {:>11}  {:>10}",
            "chunk", "rows", "elapsed", "rows/s", "ms/step"
        );
        println!("{}", "-".repeat(52));

        for &chunk in &chunk_sizes {
            let _ = run_hot_loop(&readers, chunk, 1).await?;
            let (rows, elapsed) = run_hot_loop(&readers, chunk, steps).await?;
            let rows_per_sec = rows as f64 / elapsed;
            let ms_per_step = elapsed / steps as f64 * 1000.0;
            println!(
                "{:>6}  {:>7}  {:>7.3}s  {:>11.0}  {:>9.1}ms",
                chunk, rows, elapsed, rows_per_sec, ms_per_step,
            );
        }
    }

    println!("\nDone.");
    Ok(())
}
