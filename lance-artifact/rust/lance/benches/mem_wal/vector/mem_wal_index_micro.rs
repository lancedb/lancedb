// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Microbench for the in-memory MemTable vector index.
//!
//! Two phases:
//!
//! 1. **Write + query through `ShardWriter`** — the production write path.
//!    Builds a base table with a vector index, initializes MemWAL with that
//!    index maintained, opens `mem_wal_writer` with a `ShardWriterConfig`
//!    sized to hold the largest checkpoint without flushing, and times
//!    `writer.put(batch)` calls. Index updates therefore go through the
//!    `IndexStore::insert_batches` path, which indexes inline at or below
//!    `PARALLEL_INDEX_MIN_ROWS` rows and spawns a thread per index above it —
//!    so the checkpoint sizes here straddle that crossover. At each
//!    checkpoint, queries are issued against `active_memtable_ref()` via
//!    `MemTableScanner::nearest`.
//!
//! 2. **Flush** — populate a fresh `MemTable` directly through
//!    `MemTable::insert` (same in-memory index, same final state) and time
//!    `MemTableFlusher::flush_with_indexes`. This isolates the
//!    memory-to-disk conversion cost (HNSW graph + FLAT vectors vs IVF-PQ
//!    partition batches) from any writer/WAL coordination.
//!
//! Output is plain stdout, one line per checkpoint, captured by the runner.

#![recursion_limit = "256"]
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{
    ArrayRef, FixedSizeListArray, Int64Array, RecordBatch, RecordBatchIterator,
    builder::{FixedSizeListBuilder, Float32Builder},
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::dataset::mem_wal::write::{
    HnswIndexConfig, IndexStore, MemIndexConfig, MemTable, MemTableFlusher, MemTableScanner,
    ShardWriterConfig,
};
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardManifestStore};
use lance::dataset::{Dataset, WriteParams};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_index::IndexType;
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::builder::PQBuildParams;
use lance_io::object_store::ObjectStore;
use lance_linalg::distance::{DistanceType, MetricType};
use uuid::Uuid;

const VECTOR_COL: &str = "vector";
const VECTOR_INDEX_NAME: &str = "vector_idx";
const BASE_ROWS: usize = 1024;
const BASE_IVF_PARTITIONS: usize = 16;
const BASE_PQ_SUBVECTORS: usize = 16;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|s| {
            let s = s.to_ascii_lowercase();
            matches!(s.as_str(), "1" | "true" | "yes")
        })
        .unwrap_or(default)
}

fn env_dataset_prefix() -> Option<String> {
    std::env::var("DATASET_PREFIX")
        .ok()
        .filter(|s| !s.is_empty())
}

fn env_checkpoints() -> Vec<usize> {
    let raw =
        std::env::var("BENCH_CHECKPOINTS").unwrap_or_else(|_| "100000,500000,1000000".to_string());
    raw.split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .collect()
}

fn schema(dim: usize) -> Arc<ArrowSchema> {
    use std::collections::HashMap;
    let mut id_meta = HashMap::new();
    id_meta.insert(
        "lance-schema:unenforced-primary-key".to_string(),
        "true".to_string(),
    );
    let id = Field::new("id", DataType::Int64, false).with_metadata(id_meta);
    Arc::new(ArrowSchema::new(vec![
        id,
        Field::new(
            VECTOR_COL,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            false,
        ),
    ]))
}

fn make_batch(start_id: i64, n: usize, dim: usize) -> RecordBatch {
    let s = schema(dim);
    let ids: Vec<i64> = (start_id..start_id + n as i64).collect();
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dim as i32);
    for &id in &ids {
        for d in 0..dim {
            let v = ((id as f32) * 0.000_173 + (d as f32) * 0.000_011).fract();
            builder.values().append_value(v);
        }
        builder.append(true);
    }
    RecordBatch::try_new(
        s,
        vec![Arc::new(Int64Array::from(ids)), Arc::new(builder.finish())],
    )
    .unwrap()
}

fn make_query_fsl(dim: usize, seed: u64) -> FixedSizeListArray {
    let mut b = FixedSizeListBuilder::new(Float32Builder::new(), dim as i32);
    for d in 0..dim {
        let v = (((seed.wrapping_mul(2654435761)) as f32) * 1e-9 + (d as f32) * 7e-5).fract();
        b.values().append_value(v);
    }
    b.append(true);
    b.finish()
}

async fn build_base_dataset(uri: &str, dim: usize) -> lance_core::Result<Dataset> {
    let s = schema(dim);
    let batch = make_batch(0, BASE_ROWS, dim);
    let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), s.clone());
    let mut dataset = Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
            ..Default::default()
        }),
    )
    .await?;
    let ivf_params = IvfBuildParams::new(BASE_IVF_PARTITIONS);
    let pq_params = PQBuildParams::new(BASE_PQ_SUBVECTORS, 8);
    let params = VectorIndexParams::with_ivf_pq_params(MetricType::L2, ivf_params, pq_params);
    dataset
        .create_index(
            &[VECTOR_COL],
            IndexType::Vector,
            Some(VECTOR_INDEX_NAME.to_string()),
            &params,
            true,
        )
        .await?;
    Ok(dataset)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> lance_core::Result<()> {
    let dim = env_usize("BENCH_DIM", 1024);
    let batch_size = env_usize("BENCH_BATCH", 1000);
    let num_queries = env_usize("BENCH_NUM_QUERIES", 50);
    let checkpoints = env_checkpoints();
    let max_rows = *checkpoints.iter().max().unwrap_or(&1_000_000);
    let durable_write = env_bool("BENCH_DURABLE_WRITE", false);
    let prefix = env_dataset_prefix();

    println!("=== mem_wal_index_micro [HNSW] ===");
    println!(
        "dim={} batch={} num_queries={} checkpoints={:?} durable_write={} prefix={:?}",
        dim, batch_size, num_queries, checkpoints, durable_write, prefix
    );

    // ---- Write + query through ShardWriter (production path) ----
    let temp = tempfile::tempdir().map_err(|e| lance_core::Error::io(format!("tempdir: {}", e)))?;
    let uri = match prefix.as_deref() {
        Some(p) => format!(
            "{}/run-{}/lsm",
            p.trim_end_matches('/'),
            Uuid::new_v4().simple()
        ),
        None => format!("file://{}/lsm", temp.path().display()),
    };
    println!("base dataset: {}", uri);

    let mut dataset = build_base_dataset(&uri, dim).await?;
    dataset
        .initialize_mem_wal()
        .maintained_indexes([VECTOR_INDEX_NAME])
        .execute()
        .await?;
    let dataset = Arc::new(dataset);

    let shard_id = Uuid::new_v4();
    // Sized to hold the largest checkpoint with comfortable slack so the
    // writer never flushes during the write phase.
    let row_size_estimate = dim * 4 + 8;
    let total_batches_max = max_rows.div_ceil(batch_size);
    let writer_config = ShardWriterConfig {
        shard_id,
        max_wal_persist_retries: 3,
        wal_persist_retry_base_delay: std::time::Duration::from_millis(50),
        shard_spec_id: 0,
        durable_write,
        max_memtable_size: max_rows.saturating_mul(row_size_estimate).saturating_mul(4),
        max_memtable_rows: max_rows.saturating_mul(2),
        max_memtable_batches: total_batches_max.saturating_mul(2).max(8_000),
        // Keep the time-based WAL flush trigger short so the last batch
        // before each checkpoint isn't stranded in the buffer below the
        // size threshold (which would block the index-catchup poll).
        max_wal_flush_interval: Some(Duration::from_millis(200)),
        max_unflushed_memtable_bytes: usize::MAX / 2,
        ..ShardWriterConfig::default()
    };
    let writer = dataset
        .as_ref()
        .mem_wal_writer(shard_id, writer_config)
        .await?;

    // `writer.put()` enqueues the batch into the WAL pipeline and returns;
    // the actual index update happens in the background WAL flush handler.
    // Measuring the put() return latency would only capture queueing, so we
    // walk the wall clock from the first put to the moment the index store
    // has caught up to the expected batch position. That's the end-to-end
    // "rows successfully indexed" throughput a writer experiences when it
    // both writes data AND waits for the data to become queryable.
    let mut total_inserted: usize = 0;
    let mut next_cp_idx = 0;
    let total_batches = max_rows.div_ceil(batch_size);
    println!(
        "write phase: {} batches of {} rows via ShardWriter (wall time includes index catchup)",
        total_batches, batch_size
    );
    let phase_start = Instant::now();

    for i in 0..total_batches {
        let start = (i * batch_size) as i64;
        let rows = batch_size.min(max_rows - i * batch_size);
        let batch = make_batch(start, rows, dim);
        writer.put(vec![batch]).await?;
        total_inserted += rows;

        while next_cp_idx < checkpoints.len() && total_inserted >= checkpoints[next_cp_idx] {
            let cp = checkpoints[next_cp_idx];
            let target_indexed_count = cp / batch_size;
            // The WAL flush handler only updates the index watermark when a
            // flush is triggered, and the time-based trigger inside the
            // writer runs only when `put()` is called. After the final put
            // for the final checkpoint, no more put() comes — so we issue
            // tiny dummy puts as a heartbeat to force the time trigger to
            // fire and drain the last batch through the index pipeline.
            // The dummy rows are negligible (1 row each) but are excluded
            // from `total_inserted` so the throughput math stays accurate.
            let mut spins = 0u64;
            loop {
                let active = writer.active_memtable_ref().await?;
                if active.index_store.indexed_count() >= target_indexed_count {
                    break;
                }
                drop(active);
                tokio::time::sleep(Duration::from_millis(50)).await;
                spins += 1;
                if spins.is_multiple_of(8) {
                    // Heartbeat: poke the writer so its in-put time-trigger
                    // check fires and flushes any stragglers.
                    let dummy = make_batch(-1 - spins as i64, 1, dim);
                    writer.put(vec![dummy]).await?;
                }
            }

            let elapsed = phase_start.elapsed();
            let throughput = cp as f64 / elapsed.as_secs_f64();
            let rss_kb = read_rss_kb();
            println!(
                "[checkpoint] rows={} cumulative_wall_ms={} indexed_throughput_rows_per_sec={:.1} rss_mb={}",
                cp,
                elapsed.as_millis(),
                throughput,
                rss_kb / 1024,
            );

            let active = writer.active_memtable_ref().await?;
            let mut latencies = Vec::with_capacity(num_queries);
            for q in 0..num_queries {
                let q_fsl = make_query_fsl(dim, q as u64);
                let mut scanner = MemTableScanner::new(
                    active.batch_store.clone(),
                    active.index_store.clone(),
                    active.schema.clone(),
                );
                let q_arr: ArrayRef = Arc::new(q_fsl);
                scanner.nearest(VECTOR_COL, q_arr.as_ref(), 10)?;
                let t = Instant::now();
                let stream = scanner.try_into_stream().await?;
                let _: Vec<RecordBatch> = stream.try_collect().await?;
                latencies.push(t.elapsed());
            }
            latencies.sort();
            let median = latencies[latencies.len() / 2];
            let p99 = latencies[latencies.len() * 99 / 100];
            println!(
                "[checkpoint] rows={} query_median_us={} query_p99_us={} num_queries={}",
                cp,
                median.as_micros(),
                p99.as_micros(),
                num_queries
            );

            next_cp_idx += 1;
        }
    }

    let phase_wall = phase_start.elapsed();
    println!(
        "write phase done: total_rows={} wall={:.2}s overall_indexed_throughput={:.0} rows/sec",
        total_inserted,
        phase_wall.as_secs_f64(),
        total_inserted as f64 / phase_wall.as_secs_f64()
    );

    // Drop the writer cleanly (don't measure shutdown time as part of put).
    writer.close().await?;
    drop(dataset);

    // ---- Flush phase (direct flusher; isolates memory→disk cost) ----
    println!("flush phase:");
    let index_configs = vec![MemIndexConfig::Hnsw(Box::new(
        HnswIndexConfig::new(
            VECTOR_INDEX_NAME.to_string(),
            1,
            VECTOR_COL.to_string(),
            DistanceType::L2,
        )
        .with_build_params(HnswBuildParams::default()),
    ))];
    for &cp in &checkpoints {
        let (elapsed, disk_bytes) =
            measure_flush(cp, dim, batch_size, &index_configs, prefix.as_deref()).await?;
        println!(
            "[flush] rows={} flush_wall_ms={} throughput_rows_per_sec={:.0} on_disk_bytes={} on_disk_mb={:.1}",
            cp,
            elapsed.as_millis(),
            cp as f64 / elapsed.as_secs_f64(),
            disk_bytes,
            disk_bytes as f64 / 1024.0 / 1024.0,
        );
    }

    println!("=== DONE ===");
    Ok(())
}

async fn measure_flush(
    cp: usize,
    dim: usize,
    batch_size: usize,
    index_configs: &[MemIndexConfig],
    prefix: Option<&str>,
) -> lance_core::Result<(Duration, u64)> {
    let s = schema(dim);
    let mut memtable = MemTable::new(s.clone(), 1, vec![]).unwrap();
    let registry =
        IndexStore::from_configs(index_configs, cp, cp.div_ceil(batch_size).max(64)).unwrap();
    memtable.set_indexes(registry);

    let total_batches = cp.div_ceil(batch_size);
    for i in 0..total_batches {
        let start = (i * batch_size) as i64;
        let rows = batch_size.min(cp - i * batch_size);
        let batch = make_batch(start, rows, dim);
        let _frag_id = memtable.insert(batch).await?;
    }

    let temp_dir =
        tempfile::tempdir().map_err(|e| lance_core::Error::io(format!("tempdir: {}", e)))?;
    let temp_path: PathBuf = temp_dir.path().to_path_buf();
    let uri = match prefix {
        Some(p) => format!(
            "{}/flush-{}",
            p.trim_end_matches('/'),
            Uuid::new_v4().simple()
        ),
        None => format!("file://{}", temp_path.display()),
    };
    let (store, base_path) = ObjectStore::from_uri(&uri).await?;
    let shard_id = Uuid::new_v4();
    let manifest_store = Arc::new(ShardManifestStore::new(
        store.clone(),
        &base_path,
        shard_id,
        2,
    ));
    let (epoch, _) = manifest_store.claim_epoch(0).await?;
    let flusher = MemTableFlusher::new(store, base_path, uri, shard_id, manifest_store);

    // total_batches WAL entries were stamped at positions 1..=total_batches
    // by the loop above (1-based positions).
    let covered_wal_entry_position = total_batches as u64;
    // Every batch was appended above, so the writer-global durable count is all of them.
    let durable = total_batches;
    let t = Instant::now();
    let _result = flusher
        .flush_with_indexes(
            &memtable,
            epoch,
            index_configs,
            covered_wal_entry_position,
            durable,
        )
        .await?;
    let elapsed = t.elapsed();

    // Disk size only meaningful when local; on remote storage the temp dir
    // is empty so we report 0 (and bench output makes the storage location
    // explicit via the printed URI/prefix).
    let disk_bytes = if prefix.is_some() {
        0
    } else {
        dir_size_bytes(&temp_path)
    };
    Ok((elapsed, disk_bytes))
}

fn read_rss_kb() -> u64 {
    let Ok(s) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:")
            && let Some(num) = rest.split_whitespace().next()
            && let Ok(v) = num.parse::<u64>()
        {
            return v;
        }
    }
    0
}

fn dir_size_bytes(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(p) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&p) else {
            continue;
        };
        for entry in rd.flatten() {
            let Ok(ft) = entry.file_type() else { continue };
            if ft.is_dir() {
                stack.push(entry.path());
            } else if let Ok(md) = entry.metadata() {
                total += md.len();
            }
        }
    }
    total
}
