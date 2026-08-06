// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! MemWAL FTS benchmark on real FineWeb text.
//!
//! FTS-specialized sibling of `mem_wal_shard_writer_backpressure`: same
//! CLI-arg shape, same `Mode` matrix (async/sync × index/no-index), same
//! `ShardWriter` wiring and JSON output, but the payload is real
//! HuggingFace FineWeb `text` and the maintained index is the in-memory
//! FTS index instead of IVF/PQ.
//!
//! Two phases, selected with `--phase`; each invocation does exactly one
//! phase so a process never holds two `ShardWriter` lifecycles (that is
//! what deadlocked the first iteration of this bench):
//!
//!   --phase write   throughput panel: ingest `calls × batch-rows` rows
//!                   through `ShardWriter`, report rows/s + latency
//!                   percentiles.
//!   --phase read    MemTable FTS read panel: ingest `read-rows` rows into
//!                   an auto-flush-disabled MemTable, time the FTS queries
//!                   against the live MemTable, force a flush, replay the
//!                   queries against the on-disk FTS index, and report the
//!                   per-query top-K overlap ("consistency").
//!
//! Example:
//!
//! ```bash
//! cargo bench -p lance --bench mem_wal_fineweb_fts -- \
//!   --phase write --mode async_idx \
//!   --uri /tmp/mem-fts-fineweb/run1/w_async_idx_mt100k \
//!   --seed-rows 1000000 --batch-rows 1000 --calls 1000 \
//!   --max-memtable-rows 100000 \
//!   --cache-dir /tmp/fineweb-cache --output result.json
//! ```

#![recursion_limit = "256"]
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::dataset::mem_wal::{DatasetMemWalExt, MemTableScanner, ShardWriterConfig};
use lance::dataset::{Dataset, WriteParams};
use lance::index::DatasetIndexExt;
use lance_core::Result;
use lance_index::IndexType;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;
use lance_tokenizer::TokenStream;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use serde_json::json;
use uuid::Uuid;

const TEXT_COL: &str = "text";
const FTS_INDEX_NAME: &str = "text_fts";
/// Seed-row count for the read phase. Kept tiny so the on-disk corpus is
/// effectively just the ingested rows: the MemTable FTS index covers only
/// the ingested rows, so the on-disk `full_text_search` it is compared
/// against must too. The on-disk query is additionally prefiltered to
/// `id >= READ_SEED_ROWS` to drop these few seed rows entirely.
const READ_SEED_ROWS: usize = 1000;
const HF_API_LISTING: &str =
    "https://huggingface.co/api/datasets/HuggingFaceFW/fineweb/tree/main/sample/10BT";
const HF_FILE_BASE: &str = "https://huggingface.co/datasets/HuggingFaceFW/fineweb/resolve/main/";

// ----------------------------------------------------------------------
// Mode / Phase
// ----------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    /// FTS index maintained in the MemTable.
    fn indexed(self) -> bool {
        matches!(self, Self::AsyncIndexed | Self::SyncIndexed)
    }

    /// Each `put` waits for WAL durability before returning.
    fn durable_write(self) -> bool {
        matches!(self, Self::SyncNoIndex | Self::SyncIndexed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Write,
    Read,
}

impl Phase {
    fn parse(value: &str) -> std::result::Result<Self, String> {
        match value {
            "write" => Ok(Self::Write),
            "read" => Ok(Self::Read),
            _ => Err(format!("unknown phase '{value}', expected write|read")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Write => "write",
            Self::Read => "read",
        }
    }
}

// ----------------------------------------------------------------------
// Args
// ----------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Args {
    phase: Phase,
    mode: Mode,
    uri: Option<String>,
    seed_rows: usize,
    batch_rows: usize,
    calls: usize,
    read_rows: usize,
    max_memtable_rows: Option<usize>,
    max_memtable_size: usize,
    max_unflushed_memtable_bytes: usize,
    max_wal_flush_interval_ms: u64,
    /// Paced ingest target for the write phase. `None` = unpaced (puts
    /// issued as fast as possible). Used for the backpressure sweep.
    target_rows_per_sec: Option<f64>,
    cache_dir: PathBuf,
    num_token_queries: usize,
    num_phrase_queries: usize,
    top_k: usize,
    tokio_threads: usize,
    output: Option<PathBuf>,
}

impl Default for Args {
    fn default() -> Self {
        let threads = std::thread::available_parallelism().map_or(1, usize::from);
        Self {
            phase: Phase::Write,
            mode: Mode::AsyncIndexed,
            uri: None,
            seed_rows: 1_000_000,
            batch_rows: 1_000,
            calls: 1_000,
            read_rows: 100_000,
            max_memtable_rows: None,
            max_memtable_size: 16 * 1024 * 1024 * 1024,
            max_unflushed_memtable_bytes: 8 * 1024 * 1024 * 1024,
            max_wal_flush_interval_ms: 100,
            target_rows_per_sec: None,
            cache_dir: std::env::temp_dir().join("mem_wal_fineweb_fts_cache"),
            num_token_queries: 100,
            num_phrase_queries: 50,
            top_k: 10,
            tokio_threads: threads,
            output: None,
        }
    }
}

// ----------------------------------------------------------------------
// HuggingFace FineWeb loading
// ----------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct HfTreeEntry {
    #[serde(rename = "type")]
    kind: String,
    path: String,
}

async fn list_shard_paths() -> Result<Vec<String>> {
    let entries: Vec<HfTreeEntry> = reqwest::get(HF_API_LISTING)
        .await
        .map_err(|e| lance_core::Error::io(format!("listing HTTP: {e}")))?
        .json()
        .await
        .map_err(|e| lance_core::Error::io(format!("listing JSON: {e}")))?;
    let mut shards: Vec<String> = entries
        .into_iter()
        .filter(|e| e.kind == "file" && e.path.ends_with(".parquet"))
        .map(|e| e.path)
        .collect();
    shards.sort();
    Ok(shards)
}

async fn download_shard(rel_path: &str, dest: &std::path::Path) -> Result<()> {
    if dest.exists() {
        return Ok(());
    }
    let url = format!("{HF_FILE_BASE}{rel_path}");
    let tmp = dest.with_extension("part");
    for attempt in 1..=5u32 {
        println!("downloading {rel_path} (attempt {attempt}/5) ...");
        let result: Result<bytes::Bytes> = async {
            let resp = reqwest::get(&url)
                .await
                .map_err(|e| lance_core::Error::io(format!("download HTTP: {e}")))?;
            if !resp.status().is_success() {
                return Err(lance_core::Error::io(format!(
                    "download {url} -> status {}",
                    resp.status()
                )));
            }
            resp.bytes()
                .await
                .map_err(|e| lance_core::Error::io(format!("read body: {e}")))
        }
        .await;
        match result {
            Ok(bytes) => {
                std::fs::write(&tmp, &bytes)
                    .map_err(|e| lance_core::Error::io(format!("write: {e}")))?;
                std::fs::rename(&tmp, dest)
                    .map_err(|e| lance_core::Error::io(format!("rename: {e}")))?;
                println!(
                    "  wrote {:.1} MB to {}",
                    bytes.len() as f64 / 1024.0 / 1024.0,
                    dest.display()
                );
                return Ok(());
            }
            Err(e) if attempt < 5 => {
                eprintln!("  attempt {attempt} failed: {e}; retrying");
                tokio::time::sleep(Duration::from_secs(2u64.pow(attempt))).await;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

async fn read_shard_text(
    path: &std::path::Path,
    out: &mut Vec<String>,
    max_rows: usize,
) -> Result<usize> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|e| lance_core::Error::io(format!("open parquet: {e}")))?;
    let builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .map_err(|e| lance_core::Error::io(format!("parquet builder: {e}")))?;
    let mut stream = builder
        .build()
        .map_err(|e| lance_core::Error::io(format!("parquet stream: {e}")))?;
    let mut taken = 0usize;
    while taken < max_rows {
        let Some(rb) = stream
            .try_next()
            .await
            .map_err(|e| lance_core::Error::io(format!("parquet read: {e}")))?
        else {
            break;
        };
        let col = rb
            .column_by_name("text")
            .ok_or_else(|| lance_core::Error::io("text column missing".to_string()))?;
        let strs = col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| lance_core::Error::io("text not StringArray".to_string()))?;
        for i in 0..strs.len() {
            if taken >= max_rows {
                break;
            }
            if strs.is_null(i) {
                continue;
            }
            out.push(strs.value(i).to_string());
            taken += 1;
        }
    }
    Ok(taken)
}

/// Load `needed_rows` text rows from cached FineWeb shards, downloading
/// shards on demand.
async fn load_corpus(needed_rows: usize, cache_dir: &std::path::Path) -> Result<Vec<String>> {
    std::fs::create_dir_all(cache_dir)
        .map_err(|e| lance_core::Error::io(format!("mkdir cache: {e}")))?;
    let shards = list_shard_paths().await?;
    println!("fineweb sample/10BT: {} shards", shards.len());
    let mut buf: Vec<String> = Vec::with_capacity(needed_rows);
    for rel in &shards {
        if buf.len() >= needed_rows {
            break;
        }
        let name = rel.rsplit('/').next().unwrap_or(rel);
        let local = cache_dir.join(name);
        download_shard(rel, &local).await?;
        let want = needed_rows - buf.len();
        let got = read_shard_text(&local, &mut buf, want).await?;
        println!("  shard {name} -> {got} rows (cumulative {})", buf.len());
    }
    if buf.len() < needed_rows {
        return Err(lance_core::Error::io(format!(
            "fineweb yielded only {} rows, need {needed_rows}",
            buf.len()
        )));
    }
    Ok(buf)
}

// ----------------------------------------------------------------------
// Schema / batches
// ----------------------------------------------------------------------

fn make_schema() -> Arc<ArrowSchema> {
    let mut id_meta = HashMap::new();
    id_meta.insert(
        "lance-schema:unenforced-primary-key".to_string(),
        "true".to_string(),
    );
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(id_meta),
        Field::new(TEXT_COL, DataType::Utf8, true),
    ]))
}

fn make_batch(schema: Arc<ArrowSchema>, start_id: i64, texts: &[&str]) -> RecordBatch {
    let ids: Vec<i64> = (start_id..start_id + texts.len() as i64).collect();
    let id_arr: ArrayRef = Arc::new(Int64Array::from(ids));
    let text_arr: ArrayRef = Arc::new(StringArray::from_iter_values(texts.iter().copied()));
    RecordBatch::try_new(schema, vec![id_arr, text_arr]).unwrap()
}

/// Write a seed dataset of `seed_texts.len()` rows, optionally create the
/// base FTS index, and initialize MemWAL.
async fn build_seed_dataset(
    uri: &str,
    schema: Arc<ArrowSchema>,
    seed_texts: &[String],
    batch_rows: usize,
    indexed: bool,
) -> Result<f64> {
    let start = Instant::now();
    let mut batches = Vec::with_capacity(seed_texts.len().div_ceil(batch_rows));
    let mut lo = 0usize;
    while lo < seed_texts.len() {
        let hi = (lo + batch_rows).min(seed_texts.len());
        let slice: Vec<&str> = seed_texts[lo..hi].iter().map(|s| s.as_str()).collect();
        batches.push(Ok(make_batch(schema.clone(), lo as i64, &slice)));
        lo = hi;
    }
    let reader = RecordBatchIterator::new(batches.into_iter(), schema.clone());
    let mut dataset = Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
            ..Default::default()
        }),
    )
    .await?;
    if indexed {
        dataset
            .create_index(
                &[TEXT_COL],
                IndexType::Inverted,
                Some(FTS_INDEX_NAME.to_string()),
                &InvertedIndexParams::default(),
                true,
            )
            .await?;
    }
    dataset
        .initialize_mem_wal()
        .maintained_indexes(if indexed {
            vec![FTS_INDEX_NAME.to_string()]
        } else {
            vec![]
        })
        .execute()
        .await?;
    Ok(start.elapsed().as_secs_f64())
}

fn shard_writer_config(args: &Args, shard_id: Uuid, disable_auto_flush: bool) -> ShardWriterConfig {
    let max_rows = if disable_auto_flush {
        args.read_rows.saturating_mul(4).max(4_000_000)
    } else {
        // When no explicit row cap is given, derive a generous but
        // BOUNDED cap from the byte budget. `usize::MAX/2` here would make
        // `max_memtable_batches` (computed below) astronomically large and
        // the writer would try to preallocate a petabyte-scale Vec.
        args.max_memtable_rows.unwrap_or_else(|| {
            (args.max_memtable_size / 2048).clamp(args.batch_rows.max(1), 16_000_000)
        })
    };
    let mut config = ShardWriterConfig::new(shard_id)
        .with_durable_write(args.mode.durable_write())
        .with_max_memtable_size(args.max_memtable_size)
        .with_max_unflushed_memtable_bytes(args.max_unflushed_memtable_bytes)
        .with_max_memtable_rows(max_rows)
        .with_max_memtable_batches(max_rows.div_ceil(args.batch_rows).saturating_add(64));
    if args.max_wal_flush_interval_ms == 0 {
        config.max_wal_flush_interval = None;
    } else {
        config = config
            .with_max_wal_flush_interval(Duration::from_millis(args.max_wal_flush_interval_ms));
    }
    config
}

fn percentile(values: &[f64], pct: f64) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((pct / 100.0) * (sorted.len().saturating_sub(1)) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// ----------------------------------------------------------------------
// Query set
// ----------------------------------------------------------------------

struct QuerySet {
    tokens: Vec<String>,
    phrases: Vec<String>,
}

const STOPWORDS: &[&str] = &[
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "as", "by", "is", "was",
    "are", "were", "be", "been", "being", "this", "that", "these", "those", "it", "its", "but",
    "not", "no", "if", "then", "than", "so", "do", "does", "did", "have", "has", "had", "will",
    "would", "should", "could", "can", "may", "might", "must", "i", "you", "he", "she", "we",
    "they", "them", "his", "her", "their", "our", "us", "me", "my", "your", "him", "at", "from",
];

fn build_query_set(sample: &[&str], args: &Args) -> QuerySet {
    let mut tokenizer = InvertedIndexParams::default()
        .build()
        .expect("default tokenizer builds");
    let mut freq: HashMap<String, u64> = HashMap::new();
    for t in sample.iter().take(50_000) {
        let mut stream = tokenizer.token_stream_for_doc(t);
        while let Some(tok) = stream.next() {
            if tok.text.len() < 3 || tok.text.len() > 24 || STOPWORDS.contains(&tok.text.as_str()) {
                continue;
            }
            *freq.entry(tok.text.clone()).or_default() += 1;
        }
    }
    let mut by_freq: Vec<(String, u64)> = freq.into_iter().collect();
    by_freq.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    // Skip the most-frequent tokens: terms like "all"/"one"/"time" appear
    // in a huge fraction of docs, so thousands of documents tie on BM25
    // and the top-10 is an unstable near-tie that does not meaningfully
    // exercise FTS correctness. Mid-frequency terms have a well-determined
    // top-10. The skip is capped so a small vocabulary still yields queries.
    let skip = (by_freq.len() / 4).min(300);
    let tokens: Vec<String> = by_freq
        .into_iter()
        .skip(skip)
        .map(|(t, _)| t)
        .take(args.num_token_queries)
        .collect();

    let mut phrases = Vec::with_capacity(args.num_phrase_queries);
    let stride = (sample.len().max(1) / args.num_phrase_queries.max(1)).max(1);
    let mut idx = 0usize;
    while phrases.len() < args.num_phrase_queries && idx < sample.len() {
        let mut stream = tokenizer.token_stream_for_doc(sample[idx]);
        let mut acc: Vec<String> = Vec::new();
        while let Some(tok) = stream.next() {
            if tok.text.len() < 3 || tok.text.len() > 24 || STOPWORDS.contains(&tok.text.as_str()) {
                continue;
            }
            acc.push(tok.text.clone());
            if acc.len() == 2 {
                phrases.push(format!("{} {}", acc[0], acc[1]));
                break;
            }
        }
        idx += stride;
    }
    QuerySet { tokens, phrases }
}

// ----------------------------------------------------------------------
// Write phase
// ----------------------------------------------------------------------

async fn run_write(args: &Args, uri: &str, corpus: &[String]) -> Result<serde_json::Value> {
    let schema = make_schema();
    let indexed = args.mode.indexed();

    let seed = &corpus[..args.seed_rows.min(corpus.len())];
    let setup_s = build_seed_dataset(uri, schema.clone(), seed, args.batch_rows, indexed).await?;

    let dataset = Dataset::open(uri).await?;
    let shard_id = Uuid::new_v4();
    let config = shard_writer_config(args, shard_id, false);
    let writer = dataset.mem_wal_writer(shard_id, config).await?;

    // Ingest rows come from the corpus after the seed, cycled if needed.
    let ingest_pool = &corpus[args.seed_rows.min(corpus.len())..];
    let pool_len = ingest_pool.len().max(1);
    let mut latencies_ms = Vec::with_capacity(args.calls);
    let id_base = args.seed_rows as i64;

    let puts_start = Instant::now();
    for call in 0..args.calls {
        let lo = (call * args.batch_rows) % pool_len;
        let mut slice: Vec<&str> = Vec::with_capacity(args.batch_rows);
        for j in 0..args.batch_rows {
            slice.push(ingest_pool[(lo + j) % pool_len].as_str());
        }
        let batch = make_batch(
            schema.clone(),
            id_base + (call * args.batch_rows) as i64,
            &slice,
        );
        let put_start = Instant::now();
        writer.put(vec![batch]).await?;
        latencies_ms.push(put_start.elapsed().as_secs_f64() * 1000.0);
        if (call + 1) % 100 == 0 {
            let rate = ((call + 1) * args.batch_rows) as f64 / puts_start.elapsed().as_secs_f64();
            println!("  put {}/{} ({rate:.0} rows/s)", call + 1, args.calls);
        }
        // Pace the ingest to `target_rows_per_sec` for the backpressure
        // sweep: sleep so the (call+1)-th put completes no earlier than
        // its scheduled time. The MemWAL writer's own backpressure can
        // still delay a put past schedule — that delay is what the sweep
        // is looking for.
        if let Some(target) = args.target_rows_per_sec
            && target > 0.0
        {
            let scheduled = ((call + 1) * args.batch_rows) as f64 / target;
            let actual = puts_start.elapsed().as_secs_f64();
            if scheduled > actual {
                tokio::time::sleep(Duration::from_secs_f64(scheduled - actual)).await;
            }
        }
    }
    let elapsed_puts_s = puts_start.elapsed().as_secs_f64();

    // Backlog still buffered when the put loop ends: if the writer kept
    // up with the ingest rate this is small; a large backlog means the
    // flush/index pipeline fell behind (accumulating backpressure).
    let backlog = writer.memtable_stats().await.ok();

    let close_start = Instant::now();
    writer.close().await?;
    let elapsed_close_s = close_start.elapsed().as_secs_f64();
    let elapsed_total_s = puts_start.elapsed().as_secs_f64();

    let slow_puts_1s = latencies_ms.iter().filter(|ms| **ms >= 1_000.0).count();
    let slow_puts_10s = latencies_ms.iter().filter(|ms| **ms >= 10_000.0).count();
    let rows = args.calls * args.batch_rows;
    Ok(json!({
        "phase": "write",
        "mode": args.mode.as_str(),
        "uri": uri,
        "seed_rows": args.seed_rows,
        "batch_rows": args.batch_rows,
        "calls": args.calls,
        "ingested_rows": rows,
        "max_memtable_rows": args.max_memtable_rows,
        "target_rows_per_sec": args.target_rows_per_sec,
        "setup_seconds": setup_s,
        "elapsed_puts_seconds": elapsed_puts_s,
        "elapsed_close_seconds": elapsed_close_s,
        "elapsed_total_seconds": elapsed_total_s,
        "slow_puts_ge_1s": slow_puts_1s,
        "slow_puts_ge_10s": slow_puts_10s,
        "backlog_memtable_rows": backlog.as_ref().map(|s| s.row_count),
        "backlog_pending_wal_rows": backlog.as_ref().map(|s| s.pending_wal_row_count),
        "backlog_pending_wal_mb":
            backlog.as_ref().map(|s| s.pending_wal_estimated_bytes as f64 / 1.0e6),
        // Throughput including the final close()/flush — comparable across
        // configs with different flush cadences.
        "throughput_rows_per_sec": rows as f64 / elapsed_total_s,
        // Loop-only throughput (puts returned, flush may be outstanding).
        "throughput_puts_rows_per_sec": rows as f64 / elapsed_puts_s,
        "put_p50_ms": percentile(&latencies_ms, 50.0),
        "put_p90_ms": percentile(&latencies_ms, 90.0),
        "put_p99_ms": percentile(&latencies_ms, 99.0),
        "put_max_ms": latencies_ms.iter().copied().fold(0.0_f64, f64::max),
    }))
}

// ----------------------------------------------------------------------
// Read phase
// ----------------------------------------------------------------------

async fn run_read(args: &Args, uri: &str, corpus: &[String]) -> Result<serde_json::Value> {
    let schema = make_schema();

    // Tiny seed dataset with an FTS base index (read phase is FTS-only).
    // See READ_SEED_ROWS for why the seed is kept small.
    let read_seed = READ_SEED_ROWS.min(corpus.len());
    let seed = &corpus[..read_seed];
    let setup_s = build_seed_dataset(uri, schema.clone(), seed, args.batch_rows, true).await?;

    let dataset = Dataset::open(uri).await?;
    let shard_id = Uuid::new_v4();
    // Auto-flush disabled so the MemTable holds the full read_rows.
    // durable_write is forced on: the read
    // panel measures FTS read latency and consistency, which are
    // properties of the *fully visible* MemTable. The MemTableScanner
    // respects the visibility watermark, and that watermark only advances
    // as the WAL becomes durable — so without durable_write the scanner
    // would query a partially-visible MemTable and report garbage. Both
    // are properties of ingestion, not of the read path, so forcing them
    // does not bias the latency/consistency measurement.
    let config = shard_writer_config(args, shard_id, true).with_durable_write(true);
    let writer = dataset.mem_wal_writer(shard_id, config).await?;

    let ingest_pool = &corpus[read_seed..];
    let n = args.read_rows.min(ingest_pool.len());
    let id_base = read_seed as i64;
    let total_batches = n.div_ceil(args.batch_rows);
    let ingest_start = Instant::now();
    for b in 0..total_batches {
        let lo = b * args.batch_rows;
        let hi = (lo + args.batch_rows).min(n);
        let slice: Vec<&str> = ingest_pool[lo..hi].iter().map(|s| s.as_str()).collect();
        writer
            .put(vec![make_batch(
                schema.clone(),
                id_base + lo as i64,
                &slice,
            )])
            .await?;
    }
    let ingest_s = ingest_start.elapsed().as_secs_f64();
    println!("  read phase: ingested {n} rows in {ingest_s:.1}s");

    // Build the query set from the ingested slice. Each query is a
    // (is_phrase, query_string) pair so the same set drives both the
    // MemTableScanner and the reference Dataset scanner.
    let sample: Vec<&str> = ingest_pool[..n].iter().map(|s| s.as_str()).collect();
    let queries = build_query_set(&sample, args);
    let num_token_queries = queries.tokens.len();
    let num_phrase_queries = queries.phrases.len();
    let all_queries: Vec<(bool, String)> = queries
        .tokens
        .iter()
        .map(|t| (false, t.clone()))
        .chain(queries.phrases.iter().map(|p| (true, p.clone())))
        .collect();
    println!("  query set: {num_token_queries} tokens + {num_phrase_queries} phrases");

    // ---- MemTable read phase: query through the production MemTableScanner ----
    // The scanner returns RecordBatches projected to `id`, so there is no
    // need to map FtsMemIndex row positions back to ids by hand.
    let active = writer.active_memtable_ref().await?;
    let mt_batch_store = active.batch_store.clone();
    let mt_index_store = active.index_store.clone();
    let mt_schema = active.schema.clone();
    drop(active);

    let mut latencies_us = Vec::with_capacity(all_queries.len());
    let mut mt_top: Vec<HashSet<i64>> = Vec::with_capacity(all_queries.len());
    for (qi, (is_phrase, q)) in all_queries.iter().enumerate() {
        let mut scanner = MemTableScanner::new(
            mt_batch_store.clone(),
            mt_index_store.clone(),
            mt_schema.clone(),
        );
        if *is_phrase {
            scanner.full_text_phrase(TEXT_COL, q, 0);
        } else {
            scanner.full_text_search(
                FullTextSearchQuery::new(q.to_string())
                    .with_column(TEXT_COL.to_string())
                    .unwrap(),
            )?;
        }
        scanner.project(&["id"])?;
        scanner.limit(Some(args.top_k as i64), None)?;
        let t0 = Instant::now();
        let stream = scanner.try_into_stream().await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        latencies_us.push(t0.elapsed().as_micros() as f64);
        let ids = collect_ids(&batches)?;
        if qi < 5 {
            println!(
                "  [debug mt q{qi}] phrase={is_phrase} '{q}' -> {} ids {:?}",
                ids.len(),
                ids.iter().take(5).collect::<Vec<_>>()
            );
        }
        mt_top.push(ids);
    }
    let lat_ms: Vec<f64> = latencies_us.iter().map(|us| us / 1000.0).collect();
    let mt_avg_ms = lat_ms.iter().sum::<f64>() / lat_ms.len().max(1) as f64;

    // ---- Reference: a plain on-disk dataset over the identical ingested
    // rows + a normal FTS index. The MemTable FTS results are validated
    // against this. No MemWAL flush is involved: the flushed data lives in
    // the MemWAL LSM structure which a plain `Dataset::scan` does not see,
    // so a separate reference dataset is the apples-to-apples comparison.
    drop(writer);
    let ref_uri = format!("{uri}_ref");
    let ref_build_s =
        build_reference_dataset(&ref_uri, schema.clone(), &sample, id_base, args.batch_rows)
            .await?;
    let ref_ds = Dataset::open(&ref_uri).await?;

    let mut consistencies = Vec::with_capacity(all_queries.len());
    for (qi, ((is_phrase, q), mt_ids)) in all_queries.iter().zip(mt_top.iter()).enumerate() {
        let query_str = if *is_phrase {
            format!("\"{q}\"")
        } else {
            q.clone()
        };
        let mut scanner = ref_ds.scan();
        scanner.full_text_search(FullTextSearchQuery::new(query_str.clone()))?;
        scanner.limit(Some(args.top_k as i64), None)?;
        scanner.project(&["id"])?;
        let stream = scanner.try_into_stream().await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let disk_ids = collect_ids(&batches)?;
        let inter: usize = mt_ids.intersection(&disk_ids).count();
        let denom = mt_ids.len().max(disk_ids.len()).max(1);
        if qi < 5 {
            println!(
                "  [debug ref q{qi}] '{query_str}' -> ref={} mt={} inter={inter}",
                disk_ids.len(),
                mt_ids.len()
            );
        }
        consistencies.push(inter as f64 / denom as f64);
    }
    let cons_mean = consistencies.iter().sum::<f64>() / consistencies.len().max(1) as f64;
    let cons_min = consistencies.iter().copied().fold(1.0_f64, f64::min);

    Ok(json!({
        "phase": "read",
        "mode": args.mode.as_str(),
        "uri": uri,
        "read_rows": n,
        "max_memtable_rows": args.max_memtable_rows,
        "setup_seconds": setup_s,
        "ingest_seconds": ingest_s,
        "ref_build_seconds": ref_build_s,
        "num_queries": all_queries.len(),
        "num_token_queries": num_token_queries,
        "num_phrase_queries": num_phrase_queries,
        "mt_latency_avg_ms": mt_avg_ms,
        "mt_latency_p50_ms": percentile(&lat_ms, 50.0),
        "mt_latency_p95_ms": percentile(&lat_ms, 95.0),
        "mt_latency_p99_ms": percentile(&lat_ms, 99.0),
        "consistency_mean": cons_mean,
        "consistency_min": cons_min,
    }))
}

/// Extract the `id` column values from a list of result batches.
fn collect_ids(batches: &[RecordBatch]) -> Result<HashSet<i64>> {
    let mut ids = HashSet::new();
    for b in batches {
        let id_arr = b
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| lance_core::Error::invalid_input("id column missing in result"))?;
        for i in 0..id_arr.len() {
            ids.insert(id_arr.value(i));
        }
    }
    Ok(ids)
}

/// Build a plain Lance dataset (no MemWAL) over `texts` with ids starting
/// at `id_base`, plus a normal FTS index. Used as the read-phase
/// comparison reference.
async fn build_reference_dataset(
    ref_uri: &str,
    schema: Arc<ArrowSchema>,
    texts: &[&str],
    id_base: i64,
    batch_rows: usize,
) -> Result<f64> {
    let start = Instant::now();
    let mut batches = Vec::with_capacity(texts.len().div_ceil(batch_rows));
    let mut lo = 0usize;
    while lo < texts.len() {
        let hi = (lo + batch_rows).min(texts.len());
        batches.push(Ok(make_batch(
            schema.clone(),
            id_base + lo as i64,
            &texts[lo..hi],
        )));
        lo = hi;
    }
    let reader = RecordBatchIterator::new(batches.into_iter(), schema.clone());
    let mut dataset = Dataset::write(
        reader,
        ref_uri,
        Some(WriteParams {
            data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
            ..Default::default()
        }),
    )
    .await?;
    dataset
        .create_index(
            &[TEXT_COL],
            IndexType::Inverted,
            Some(FTS_INDEX_NAME.to_string()),
            &InvertedIndexParams::default(),
            true,
        )
        .await?;
    Ok(start.elapsed().as_secs_f64())
}

// ----------------------------------------------------------------------
// CLI
// ----------------------------------------------------------------------

fn parse<T>(flag: &str, value: &str) -> Result<T>
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
    while let Some(flag) = iter.next() {
        if flag == "--bench" {
            continue;
        }
        let value = iter
            .next()
            .ok_or_else(|| lance_core::Error::invalid_input(format!("missing value for {flag}")))?;
        match flag.as_str() {
            "--phase" => {
                args.phase = Phase::parse(&value).map_err(lance_core::Error::invalid_input)?
            }
            "--mode" => {
                args.mode = Mode::parse(&value).map_err(lance_core::Error::invalid_input)?
            }
            "--uri" => args.uri = Some(value),
            "--seed-rows" => args.seed_rows = parse(&flag, &value)?,
            "--batch-rows" => args.batch_rows = parse(&flag, &value)?,
            "--calls" => args.calls = parse(&flag, &value)?,
            "--read-rows" => args.read_rows = parse(&flag, &value)?,
            "--max-memtable-rows" => {
                let v: usize = parse(&flag, &value)?;
                args.max_memtable_rows = (v != 0).then_some(v);
            }
            "--max-memtable-size" => args.max_memtable_size = parse(&flag, &value)?,
            "--max-unflushed-memtable-bytes" => {
                args.max_unflushed_memtable_bytes = parse(&flag, &value)?
            }
            "--max-wal-flush-interval-ms" => args.max_wal_flush_interval_ms = parse(&flag, &value)?,
            "--target-rows-per-sec" => {
                let v: f64 = parse(&flag, &value)?;
                args.target_rows_per_sec = (v > 0.0).then_some(v);
            }
            "--cache-dir" => args.cache_dir = PathBuf::from(value),
            "--num-token-queries" => args.num_token_queries = parse(&flag, &value)?,
            "--num-phrase-queries" => args.num_phrase_queries = parse(&flag, &value)?,
            "--top-k" => args.top_k = parse(&flag, &value)?,
            "--tokio-threads" => args.tokio_threads = parse(&flag, &value)?,
            "--output" => args.output = Some(PathBuf::from(value)),
            _ => {
                return Err(lance_core::Error::invalid_input(format!(
                    "unknown argument: {flag}"
                )));
            }
        }
    }
    if args.batch_rows == 0 || args.calls == 0 || args.seed_rows == 0 {
        return Err(lance_core::Error::invalid_input(
            "seed-rows, batch-rows, calls must be > 0",
        ));
    }
    Ok(args)
}

async fn run(args: Args) -> Result<()> {
    let temp = if args.uri.is_none() {
        Some(tempfile::tempdir().map_err(|e| lance_core::Error::io(format!("tempdir: {e}")))?)
    } else {
        None
    };
    let uri = match &args.uri {
        Some(u) => u.clone(),
        None => temp
            .as_ref()
            .unwrap()
            .path()
            .join("fineweb_fts.lance")
            .display()
            .to_string(),
    };

    println!(
        "bench=mem_wal_fineweb_fts phase={} mode={} uri={} seed_rows={} batch_rows={} calls={} read_rows={} max_memtable_rows={:?}",
        args.phase.as_str(),
        args.mode.as_str(),
        uri,
        args.seed_rows,
        args.batch_rows,
        args.calls,
        args.read_rows,
        args.max_memtable_rows,
    );

    // Corpus size differs by phase: the write phase seeds with
    // `seed_rows` and cycles the ingest pool; the read phase uses only a
    // tiny `READ_SEED_ROWS` seed plus `read_rows` ingested rows.
    let corpus_rows = match args.phase {
        Phase::Write => (args.seed_rows + args.calls * args.batch_rows).min(2_000_000),
        Phase::Read => (READ_SEED_ROWS + args.read_rows).min(2_000_000),
    };
    let corpus = load_corpus(corpus_rows, &args.cache_dir).await?;

    let result = match args.phase {
        Phase::Write => run_write(&args, &uri, &corpus).await?,
        Phase::Read => run_read(&args, &uri, &corpus).await?,
    };

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
    println!("=== DONE ===");
    Ok(())
}

fn main() -> Result<()> {
    let args = parse_args()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.tokio_threads.max(1))
        .enable_all()
        .build()
        .map_err(|e| lance_core::Error::io(format!("build runtime: {e}")))?;
    runtime.block_on(run(args))
}
