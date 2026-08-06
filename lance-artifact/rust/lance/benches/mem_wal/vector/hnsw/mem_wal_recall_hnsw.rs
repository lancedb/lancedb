// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! HNSW recall benchmark for the in-memory MemTable index.
//!
//! - Downloads `KShivendu/dbpedia-entities-openai-1M` parquet shards (1536-dim
//!   OpenAI ada embeddings) from HF as needed to cover the requested
//!   checkpoints.
//! - For each checkpoint size N (default 100k / 500k / 1M):
//!     * Build a fresh MemTable with rows `[0..N)` via the production
//!       ShardWriter path.
//!     * Use 200 held-out queries from rows `[max_checkpoint..max_checkpoint+200)`
//!       (constant across checkpoints so recall comparisons are apples-to-apples).
//!     * For each query: brute-force top-k against the corpus (exact ground
//!       truth) vs HNSW top-k via `MemTableScanner::nearest`.
//!     * Recall@k = |brute ∩ hnsw| / k aggregated over queries.

#![recursion_limit = "256"]
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{
    ArrayRef, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, RecordBatchIterator,
    cast::AsArray, types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::dataset::mem_wal::DatasetMemWalExt;
use lance::dataset::mem_wal::write::{MemTableScanner, ShardWriterConfig};
use lance::dataset::{Dataset, WriteParams};
use lance::index::vector::VectorIndexParams;
use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};
use lance_core::ROW_ID;
use lance_index::IndexType;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::prefilter::NoFilter;
use lance_index::vector::Query;
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::builder::PQBuildParams;
use lance_linalg::distance::{DistanceType, MetricType};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use uuid::Uuid;

const VECTOR_COL: &str = "vector";
const VECTOR_INDEX_NAME: &str = "vector_idx";
const HF_API_LISTING: &str =
    "https://huggingface.co/api/datasets/KShivendu/dbpedia-entities-openai-1M/tree/main/data";
const HF_FILE_BASE: &str =
    "https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M/resolve/main/";
const DIM: usize = 1536;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_checkpoints() -> Vec<usize> {
    let raw =
        std::env::var("BENCH_CHECKPOINTS").unwrap_or_else(|_| "100000,500000,1000000".to_string());
    raw.split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .collect()
}

#[derive(serde::Deserialize)]
struct HfTreeEntry {
    #[serde(rename = "type")]
    kind: String,
    path: String,
}

async fn list_shard_paths() -> lance_core::Result<Vec<String>> {
    let entries: Vec<HfTreeEntry> = reqwest::get(HF_API_LISTING)
        .await
        .map_err(|e| lance_core::Error::io(format!("listing HTTP: {}", e)))?
        .json()
        .await
        .map_err(|e| lance_core::Error::io(format!("listing JSON: {}", e)))?;
    let mut shards: Vec<String> = entries
        .into_iter()
        .filter(|e| e.kind == "file" && e.path.ends_with(".parquet"))
        .map(|e| e.path)
        .collect();
    shards.sort();
    Ok(shards)
}

async fn download_shard(rel_path: &str, dest: &std::path::Path) -> lance_core::Result<()> {
    if dest.exists() {
        return Ok(());
    }
    let url = format!("{}{}", HF_FILE_BASE, rel_path);
    let max_attempts = 5;
    for attempt in 1..=max_attempts {
        println!(
            "downloading {} (attempt {}/{}) ...",
            rel_path, attempt, max_attempts
        );
        let result: lance_core::Result<bytes::Bytes> = async {
            let resp = reqwest::get(&url)
                .await
                .map_err(|e| lance_core::Error::io(format!("download HTTP: {}", e)))?;
            if !resp.status().is_success() {
                return Err(lance_core::Error::io(format!(
                    "download {} → status {}",
                    url,
                    resp.status()
                )));
            }
            resp.bytes()
                .await
                .map_err(|e| lance_core::Error::io(format!("read body: {}", e)))
        }
        .await;
        match result {
            Ok(bytes) => {
                std::fs::write(dest, &bytes)
                    .map_err(|e| lance_core::Error::io(format!("write: {}", e)))?;
                println!(
                    "  wrote {:.1} MB to {}",
                    bytes.len() as f64 / 1024.0 / 1024.0,
                    dest.display()
                );
                return Ok(());
            }
            Err(e) if attempt < max_attempts => {
                let backoff = Duration::from_secs(2u64.pow(attempt as u32));
                eprintln!(
                    "  attempt {} failed: {}; retrying in {:?}",
                    attempt, e, backoff
                );
                tokio::time::sleep(backoff).await;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

/// Read up to `max_rows` rows from a single parquet file, appending to `buf`.
/// Returns the number of rows actually read from this file.
async fn read_shard_into_buf(
    path: &std::path::Path,
    buf: &mut Vec<f32>,
    max_rows: usize,
) -> lance_core::Result<usize> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|e| lance_core::Error::io(format!("open parquet: {}", e)))?;
    let builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .map_err(|e| lance_core::Error::io(format!("parquet builder: {}", e)))?;
    let mut stream = builder
        .build()
        .map_err(|e| lance_core::Error::io(format!("parquet stream: {}", e)))?;

    let cast_target = arrow_schema::DataType::FixedSizeList(
        Arc::new(Field::new("item", arrow_schema::DataType::Float32, true)),
        DIM as i32,
    );
    let mut rows_from_shard = 0usize;
    while rows_from_shard < max_rows {
        let Some(rb) = stream
            .try_next()
            .await
            .map_err(|e| lance_core::Error::io(format!("parquet read: {}", e)))?
        else {
            break;
        };
        let col = rb
            .column_by_name("openai")
            .ok_or_else(|| lance_core::Error::io("openai column missing".to_string()))?;
        let casted = if col.data_type() == &cast_target {
            col.clone()
        } else {
            arrow_cast::cast::cast(col.as_ref(), &cast_target).map_err(|e| {
                lance_core::Error::io(format!(
                    "cast openai column to FixedSizeList<Float32, {}>: {}",
                    DIM, e
                ))
            })?
        };
        let fsl = casted.as_fixed_size_list();
        let values = fsl
            .values()
            .as_primitive_opt::<Float32Type>()
            .ok_or_else(|| {
                lance_core::Error::io(format!(
                    "fsl values not Float32: {:?}",
                    fsl.values().data_type()
                ))
            })?
            .values();
        for i in 0..rb.num_rows() {
            if rows_from_shard >= max_rows {
                break;
            }
            let off = i * DIM;
            buf.extend_from_slice(&values[off..off + DIM]);
            rows_from_shard += 1;
        }
    }
    Ok(rows_from_shard)
}

async fn load_corpus(needed_rows: usize) -> lance_core::Result<Vec<f32>> {
    let shards = list_shard_paths().await?;
    println!("dataset has {} parquet shards", shards.len());

    let cache_dir = std::env::temp_dir().join("mem_wal_recall_hnsw_cache");
    std::fs::create_dir_all(&cache_dir)
        .map_err(|e| lance_core::Error::io(format!("mkdir cache: {}", e)))?;

    let mut buf: Vec<f32> = Vec::with_capacity(needed_rows * DIM);
    let mut total_rows: usize = 0;

    for rel_path in &shards {
        if total_rows >= needed_rows {
            break;
        }
        let local_name = rel_path.rsplit('/').next().unwrap_or(rel_path);
        let local = cache_dir.join(local_name);
        download_shard(rel_path, &local).await?;
        let want = needed_rows - total_rows;
        let got = read_shard_into_buf(&local, &mut buf, want).await?;
        total_rows += got;
        println!(
            "  shard {} → {} rows (cumulative {})",
            local_name, got, total_rows
        );
    }
    if total_rows < needed_rows {
        println!(
            "  note: dataset exhausted at {} rows (asked {}); caller will adjust",
            total_rows, needed_rows
        );
    }
    Ok(buf)
}

fn make_schema() -> Arc<ArrowSchema> {
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
                DIM as i32,
            ),
            false,
        ),
    ]))
}

fn make_batch(start_id: i64, vectors: &[f32], schema: Arc<ArrowSchema>) -> RecordBatch {
    let n = vectors.len() / DIM;
    let ids: Vec<i64> = (start_id..start_id + n as i64).collect();
    let id_arr = Arc::new(Int64Array::from(ids));
    let inner = Arc::new(Float32Array::from(vectors.to_vec()));
    let inner_field = Arc::new(Field::new("item", DataType::Float32, true));
    let fsl = Arc::new(FixedSizeListArray::try_new(inner_field, DIM as i32, inner, None).unwrap());
    RecordBatch::try_new(schema, vec![id_arr, fsl as ArrayRef]).unwrap()
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    DistanceType::Cosine.func()(a, b)
}

fn brute_force_top_k(corpus: &[f32], n: usize, query: &[f32], k: usize) -> Vec<(f32, i64)> {
    let mut all: Vec<(f32, i64)> = (0..n)
        .map(|i| {
            let off = i * DIM;
            (cosine_distance(query, &corpus[off..off + DIM]), i as i64)
        })
        .collect();
    all.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    all.truncate(k);
    all
}

async fn build_base_dataset(uri: &str, schema: Arc<ArrowSchema>) -> lance_core::Result<()> {
    // Tiny base table — content is irrelevant for the recall test (we only
    // query the MemTable), but it can't be all-zero: IVF/PQ training rejects
    // zero-norm vectors under cosine. Use a deterministic non-zero pattern.
    let base_n = 1024usize;
    let mut base_vec = Vec::with_capacity(base_n * DIM);
    for i in 0..base_n {
        for d in 0..DIM {
            base_vec.push((((i * 31 + d) as f32) * 0.000_173_f32).sin());
        }
    }
    let base_batch = make_batch(0, &base_vec, schema.clone());
    let reader = RecordBatchIterator::new(std::iter::once(Ok(base_batch)), schema.clone());
    let mut dataset = Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
            ..Default::default()
        }),
    )
    .await?;
    let ivf = IvfBuildParams::new(16);
    let pq = PQBuildParams::new(16, 8);
    let params = VectorIndexParams::with_ivf_pq_params(MetricType::Cosine, ivf, pq);
    dataset
        .create_index(
            &[VECTOR_COL],
            IndexType::Vector,
            Some(VECTOR_INDEX_NAME.to_string()),
            &params,
            true,
        )
        .await?;
    dataset
        .initialize_mem_wal()
        .maintained_indexes([VECTOR_INDEX_NAME])
        .execute()
        .await?;
    Ok(())
}

struct CheckpointResult {
    rows: usize,
    write_wall: Duration,
    mean_recall: f64,
    min_recall: f64,
    median_query_us: u128,
    p99_query_us: u128,
    bf_total: Duration,
    hnsw_total: Duration,
}

async fn run_checkpoint(
    cp: usize,
    corpus: &[f32],
    queries: &[f32],
    num_queries: usize,
    k: usize,
    schema: Arc<ArrowSchema>,
) -> lance_core::Result<CheckpointResult> {
    println!("\n=== checkpoint rows={} ===", cp);
    let temp = tempfile::tempdir().map_err(|e| lance_core::Error::io(format!("tempdir: {}", e)))?;
    // When BENCH_URI_BASE is set, write to a persistent path and flush the
    // MemTable to an on-disk generation (instead of querying the active
    // MemTable), printing the SSTable's dataset path for a
    // downstream direct-read benchmark.
    let flush_base = std::env::var("BENCH_URI_BASE").ok();
    let local_dir = flush_base.as_ref().map(|b| format!("{}/cp_{}", b, cp));
    let uri = match &local_dir {
        Some(d) => format!("file://{}", d),
        None => format!("file://{}/lsm", temp.path().display()),
    };
    build_base_dataset(&uri, schema.clone()).await?;
    let dataset = Arc::new(Dataset::open(&uri).await?);

    let shard_id = Uuid::new_v4();
    let row_size_estimate = DIM * 4 + 8;
    let total_batches_max = cp.div_ceil(1000);
    // Optionally override the maintained HNSW index's graph degree (num_edges)
    // and ef_construction on the writer config so we can sweep build params and
    // run matched comparisons vs FAISS. 0 keeps the library default.
    let mut hnsw_params = std::collections::HashMap::new();
    let num_edges = env_usize("BENCH_HNSW_NUM_EDGES", 0);
    if num_edges > 0 {
        let efc = env_usize("BENCH_HNSW_EFC", 200);
        hnsw_params.insert(
            VECTOR_INDEX_NAME.to_string(),
            HnswBuildParams::default()
                .num_edges(num_edges)
                .ef_construction(efc),
        );
    }
    let writer_config = ShardWriterConfig {
        shard_id,
        max_wal_persist_retries: 3,
        wal_persist_retry_base_delay: std::time::Duration::from_millis(50),
        shard_spec_id: 0,
        durable_write: false,
        max_memtable_size: cp.saturating_mul(row_size_estimate).saturating_mul(4),
        max_memtable_rows: cp.saturating_mul(2),
        max_memtable_batches: total_batches_max.saturating_mul(2).max(8_000),
        max_wal_flush_interval: Some(Duration::from_millis(200)),
        max_unflushed_memtable_bytes: usize::MAX / 2,
        hnsw_params,
        ..ShardWriterConfig::default()
    };
    let writer = dataset
        .as_ref()
        .mem_wal_writer(shard_id, writer_config)
        .await?;

    // Skip the base-table rows (1024) when assigning corpus IDs.
    let id_offset: i64 = 1024 + 1;
    let batch_size = 1000;
    let total_batches = cp.div_ceil(batch_size);
    let write_start = Instant::now();
    for i in 0..total_batches {
        let start_row = i * batch_size;
        let n = batch_size.min(cp - start_row);
        let batch_vec = &corpus[start_row * DIM..(start_row + n) * DIM];
        let batch = make_batch(id_offset + start_row as i64, batch_vec, schema.clone());
        writer.put(vec![batch]).await?;
    }

    let target_indexed_count = total_batches;
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
            let dummy_vec = vec![0.0f32; DIM];
            let dummy = make_batch(-1 - spins as i64, &dummy_vec, schema.clone());
            writer.put(vec![dummy]).await?;
        }
    }
    let write_wall = write_start.elapsed();
    println!(
        "  wrote {} rows in {:.2}s (incl. index catchup)",
        cp,
        write_wall.as_secs_f64()
    );
    use std::io::Write;
    std::io::stdout().flush().ok();

    // Flush mode: seal the active MemTable to a persistent on-disk generation
    // (single-partition IVF_HNSW_SQ at the base dataset's storage version) and
    // print its dataset path. Validates we can flush cp=100k/500k/1M.
    if let Some(dir) = &local_dir {
        let seal_start = Instant::now();
        writer.force_seal_active().await?;
        let mut waited = 0u64;
        let gen_path = loop {
            if let Some(m) = writer.manifest().await?
                && let Some(sstable) = m.sstables.last()
            {
                break format!(
                    "{}/_mem_wal/{}/{}",
                    dir,
                    shard_id.as_hyphenated(),
                    sstable.path
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            waited += 1;
            if waited.is_multiple_of(50) {
                println!("  ... waiting for flush cp={cp} ({}s)", waited / 10);
                std::io::stdout().flush().ok();
            }
            if waited > 6000 {
                return Err(lance_core::Error::io(format!(
                    "flush timed out for cp={cp}"
                )));
            }
        };
        println!(
            "SSTABLE_OK cp={} id_offset={} flush_s={:.2} path={}",
            cp,
            id_offset,
            seal_start.elapsed().as_secs_f64(),
            gen_path
        );
        std::io::stdout().flush().ok();
        writer.close().await?;

        // Item 4: open the SSTable directly from its dataset path and
        // benchmark its on-disk IVF_HNSW_SQ read in Rust (single-thread per-query
        // latency + recall vs brute force), sweeping ef. nprobes=1 (single part).
        let gen_uri = format!("file://{}", gen_path);
        let gen_ds = Dataset::open(&gen_uri).await?;
        // Precompute brute-force ground truth once per query (corpus indices).
        let gt_sets: Vec<std::collections::HashSet<i64>> = (0..num_queries)
            .map(|q| {
                let q_vec = &queries[q * DIM..(q + 1) * DIM];
                brute_force_top_k(corpus, cp, q_vec, k)
                    .iter()
                    .map(|(_, id)| *id)
                    .collect()
            })
            .collect();
        let efs = [16usize, 64, 256];
        let mut best: Option<(usize, f64, u128, u128)> = None;
        for &ef in &efs {
            let mut recall_sum = 0.0f64;
            let mut lat: Vec<u128> = Vec::with_capacity(num_queries);
            for q in 0..num_queries {
                let q_vec = &queries[q * DIM..(q + 1) * DIM];
                let bf_set = &gt_sets[q];
                let q_arr = Float32Array::from(q_vec.to_vec());
                let mut scanner = gen_ds.scan();
                scanner.nearest(VECTOR_COL, &q_arr, k)?;
                scanner.nprobes(1);
                scanner.ef(ef);
                let h_t = Instant::now();
                let batches: Vec<RecordBatch> =
                    scanner.try_into_stream().await?.try_collect().await?;
                lat.push(h_t.elapsed().as_micros());
                let mut hits = 0usize;
                for b in &batches {
                    if let Some(c) = b.column_by_name("id") {
                        let col = c.as_primitive::<arrow_array::types::Int64Type>();
                        for i in 0..col.len() {
                            if bf_set.contains(&(col.value(i) - id_offset)) {
                                hits += 1;
                            }
                        }
                    }
                }
                recall_sum += hits as f64 / k as f64;
            }
            lat.sort();
            let median_q = lat[lat.len() / 2];
            let p99_q = lat[lat.len() * 99 / 100];
            let mean_recall = recall_sum / num_queries as f64;
            println!(
                "SSTABLE_READ cp={} ef={} mean_recall={:.4} median_us={} p99_us={}",
                cp, ef, mean_recall, median_q, p99_q
            );
            std::io::stdout().flush().ok();
            if mean_recall >= 0.95 && best.is_none() {
                best = Some((ef, mean_recall, median_q, p99_q));
            }
        }

        // Raw-index path: call VectorIndex::search() directly (partition-find +
        // HNSW+SQ search, returns _rowid/_distance) bypassing the DataFusion
        // scanner/take/projection. The RAW_READ vs SSTABLE_READ latency delta at
        // matched ef isolates the DataFusion per-query overhead from the actual
        // index search cost.
        let idx_metas = gen_ds.load_indices_by_name(VECTOR_INDEX_NAME).await?;
        let uuid = idx_metas
            .first()
            .ok_or_else(|| lance_core::Error::io("SSTable has no vector index".to_string()))?
            .uuid;
        let vidx = gen_ds
            .open_vector_index(VECTOR_COL, &uuid, &NoOpMetricsCollector)
            .await?;
        // _rowid -> corpus index, to score recall on the raw path (built once,
        // outside the timed region).
        let mut rowid_to_corpus: std::collections::HashMap<u64, i64> =
            std::collections::HashMap::new();
        {
            let mut scan = gen_ds.scan();
            scan.with_row_id();
            scan.project(&["id"])?;
            let mut stream = scan.try_into_stream().await?;
            while let Some(b) = stream.try_next().await? {
                let rid = b
                    .column_by_name(ROW_ID)
                    .ok_or_else(|| lance_core::Error::io("row id missing".to_string()))?
                    .as_primitive::<arrow_array::types::UInt64Type>();
                let idc = b
                    .column_by_name("id")
                    .ok_or_else(|| lance_core::Error::io("id missing".to_string()))?
                    .as_primitive::<arrow_array::types::Int64Type>();
                for i in 0..b.num_rows() {
                    rowid_to_corpus.insert(rid.value(i), idc.value(i) - id_offset);
                }
            }
        }
        for &ef in &efs {
            let mut recall_sum = 0.0f64;
            let mut lat: Vec<u128> = Vec::with_capacity(num_queries);
            for q in 0..num_queries {
                let q_vec = &queries[q * DIM..(q + 1) * DIM];
                let bf_set = &gt_sets[q];
                let query = Query {
                    column: VECTOR_COL.to_string(),
                    key: Arc::new(Float32Array::from(q_vec.to_vec())),
                    k,
                    lower_bound: None,
                    upper_bound: None,
                    minimum_nprobes: 1,
                    maximum_nprobes: Some(1),
                    ef: Some(ef),
                    refine_factor: None,
                    metric_type: Some(DistanceType::Cosine),
                    use_index: true,
                    query_parallelism: 1,
                    dist_q_c: 0.0,
                    approx_mode: Default::default(),
                };
                // IVFIndex::search is intentionally unimplemented (top-level does
                // partition-aware search); replicate the ANN exec node: pick the
                // closest partition then search it. Single-partition SSTable.
                let t = Instant::now();
                let (parts, _) = vidx.find_partitions(&query)?;
                let pid = parts.value(0) as usize;
                let batch = vidx
                    .search_in_partition(pid, &query, Arc::new(NoFilter), &NoOpMetricsCollector)
                    .await?;
                lat.push(t.elapsed().as_micros());
                let mut hits = 0usize;
                if let Some(c) = batch.column_by_name(ROW_ID) {
                    let col = c.as_primitive::<arrow_array::types::UInt64Type>();
                    for i in 0..col.len() {
                        if let Some(corpus_idx) = rowid_to_corpus.get(&col.value(i))
                            && bf_set.contains(corpus_idx)
                        {
                            hits += 1;
                        }
                    }
                }
                recall_sum += hits as f64 / k as f64;
            }
            lat.sort();
            println!(
                "RAW_READ cp={} ef={} mean_recall={:.4} median_us={} p99_us={}",
                cp,
                ef,
                recall_sum / num_queries as f64,
                lat[lat.len() / 2],
                lat[lat.len() * 99 / 100]
            );
            std::io::stdout().flush().ok();
        }

        let (_, mr, med, p99) = best.unwrap_or((0, 0.0, 0, 0));
        return Ok(CheckpointResult {
            rows: cp,
            write_wall,
            mean_recall: mr,
            min_recall: 0.0,
            median_query_us: med,
            p99_query_us: p99,
            bf_total: Duration::ZERO,
            hnsw_total: Duration::ZERO,
        });
    }

    let active = writer.active_memtable_ref().await?;
    let mut recall_sum: f64 = 0.0;
    let mut min_recall: f64 = 1.0;
    let mut bf_total = Duration::ZERO;
    let mut hnsw_total = Duration::ZERO;
    let mut latencies_us: Vec<u128> = Vec::with_capacity(num_queries);
    let recall_phase_start = Instant::now();

    for q in 0..num_queries {
        let q_off = q * DIM;
        let q_vec = &queries[q_off..q_off + DIM];

        let bf_t = Instant::now();
        let bf = brute_force_top_k(corpus, cp, q_vec, k);
        bf_total += bf_t.elapsed();

        let inner = Arc::new(Float32Array::from(q_vec.to_vec()));
        let inner_field = Arc::new(Field::new("item", DataType::Float32, true));
        let q_fsl = FixedSizeListArray::try_new(inner_field, DIM as i32, inner, None).unwrap();

        let mut scanner = MemTableScanner::new(
            active.batch_store.clone(),
            active.index_store.clone(),
            active.schema.clone(),
        );
        let q_arr: ArrayRef = Arc::new(q_fsl);
        scanner.nearest(VECTOR_COL, q_arr.as_ref(), k)?;
        let h_t = Instant::now();
        let stream = scanner.try_into_stream().await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let elapsed = h_t.elapsed();
        hnsw_total += elapsed;
        latencies_us.push(elapsed.as_micros());

        let mut hnsw_ids: Vec<i64> = Vec::with_capacity(k);
        for b in &batches {
            let id_col = b
                .column_by_name("id")
                .ok_or_else(|| lance_core::Error::invalid_input("id missing"))?
                .as_primitive::<arrow_array::types::Int64Type>();
            for i in 0..id_col.len() {
                hnsw_ids.push(id_col.value(i) - id_offset);
            }
        }

        let bf_set: std::collections::HashSet<i64> = bf.iter().map(|(_, id)| *id).collect();
        let hits = hnsw_ids.iter().filter(|id| bf_set.contains(id)).count();
        let recall = (hits as f64) / (k as f64);
        recall_sum += recall;
        if recall < min_recall {
            min_recall = recall;
        }
        if (q + 1) % 25 == 0 || q + 1 == num_queries {
            println!(
                "    progress: {}/{} queries (running mean recall = {:.4})",
                q + 1,
                num_queries,
                recall_sum / (q + 1) as f64
            );
            use std::io::Write;
            std::io::stdout().flush().ok();
        }
    }

    let recall_phase_wall = recall_phase_start.elapsed();
    latencies_us.sort();
    let median_q = latencies_us[latencies_us.len() / 2];
    let p99_q = latencies_us[latencies_us.len() * 99 / 100];
    let mean_recall = recall_sum / num_queries as f64;
    println!(
        "  recall phase: cp={} mean_recall={:.4} min_recall={:.4} q_median_us={} q_p99_us={} bf_s={:.2} hnsw_s={:.2} wall={:.2}s",
        cp,
        mean_recall,
        min_recall,
        median_q,
        p99_q,
        bf_total.as_secs_f64(),
        hnsw_total.as_secs_f64(),
        recall_phase_wall.as_secs_f64()
    );
    std::io::stdout().flush().ok();

    drop(active);
    writer.close().await?;
    Ok(CheckpointResult {
        rows: cp,
        write_wall,
        mean_recall,
        min_recall,
        median_query_us: median_q,
        p99_query_us: p99_q,
        bf_total,
        hnsw_total,
    })
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> lance_core::Result<()> {
    let mut checkpoints = env_checkpoints();
    let num_queries = env_usize("BENCH_NUM_QUERIES", 200);
    let k = env_usize("BENCH_K", 10);
    let ef = env_usize("BENCH_EF", 64);

    // Probe how many rows the dataset actually contains by listing shards
    // and estimating from the file count, then read all of them. The DBpedia
    // dataset is exactly 1,000,000 rows across 26 shards; we want to leave
    // room for `num_queries` queries at the tail.
    println!(
        "=== mem_wal_recall_hnsw === checkpoints={:?} num_queries={} k={} ef={} dim={}",
        checkpoints, num_queries, k, ef, DIM
    );

    // Pull the entire dataset; queries come from the last `num_queries` rows
    // and corpus from `[0, total - num_queries)`. Any checkpoint larger than
    // the available corpus size is clamped down.
    let max_cp_requested = *checkpoints.iter().max().unwrap_or(&1_000_000);
    // load_corpus returns exactly the rows it could read, capped at max
    // available; ask for max_cp_requested + num_queries up front so the
    // common case (max_cp + queries <= dataset) avoids extra shards.
    let all_vectors = load_corpus(max_cp_requested + num_queries).await?;
    let total_rows_loaded = all_vectors.len() / DIM;
    let max_corpus = total_rows_loaded.saturating_sub(num_queries);
    println!(
        "loaded {} rows × {} dim ({:.2} GB); corpus available = {}, queries = last {} rows",
        total_rows_loaded,
        DIM,
        (total_rows_loaded * DIM * 4) as f64 / 1024.0 / 1024.0 / 1024.0,
        max_corpus,
        num_queries
    );
    // Clamp any checkpoint that exceeds available corpus rows.
    for cp in checkpoints.iter_mut() {
        if *cp > max_corpus {
            println!("  clamping checkpoint {} → {}", cp, max_corpus);
            *cp = max_corpus;
        }
    }
    let corpus = &all_vectors[..max_corpus * DIM];
    let queries = &all_vectors
        [max_corpus * DIM..(max_corpus + num_queries.min(total_rows_loaded - max_corpus)) * DIM];

    let schema = make_schema();
    let mut results: Vec<CheckpointResult> = Vec::with_capacity(checkpoints.len());
    for &cp in &checkpoints {
        let r = run_checkpoint(cp, corpus, queries, num_queries, k, schema.clone()).await?;
        results.push(r);
    }

    println!("\n=== RESULTS ===");
    println!(
        "{:<10} {:>14} {:>12} {:>12} {:>12} {:>12} {:>10} {:>10}",
        "rows",
        "write_wall_s",
        "mean_recall",
        "min_recall",
        "q_median_us",
        "q_p99_us",
        "bf_s",
        "hnsw_s"
    );
    for r in &results {
        println!(
            "{:<10} {:>14.2} {:>12.4} {:>12.4} {:>12} {:>12} {:>10.2} {:>10.2}",
            r.rows,
            r.write_wall.as_secs_f64(),
            r.mean_recall,
            r.min_recall,
            r.median_query_us,
            r.p99_query_us,
            r.bf_total.as_secs_f64(),
            r.hnsw_total.as_secs_f64(),
        );
    }
    println!("=== DONE ===");
    Ok(())
}
