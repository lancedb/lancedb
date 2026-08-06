// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Standalone CLI benchmark for FTS read across LSM levels.
//!
//! Sibling of `mem_wal_vector_bench.rs` / `mem_wal_point_lookup_bench.rs`:
//! same `--phase prepare|search` shape, same `ShardWriter`-based ingestion
//! of SSTables + an active memtable, same `--uri` cloud/local
//! detection, and the same JSON output contract. The payload is real
//! HuggingFace FineWeb `text` and the query path is
//! [`LsmFtsSearchPlanner`] (local scoring) over the base table + SSTables
//! + active memtable.
//!
//! Each `search` invocation times a query set against the LSM hierarchy
//! and reports latency percentiles. With `--with-baseline`, it also builds
//! a single-merged-index ground truth (all rows in one Lance dataset with
//! one FTS index, queried via `scanner.full_text_search`) and reports the
//! top-k Jaccard between local-LSM scoring and the merged index — i.e. how
//! far per-source local BM25 drifts from a globally-consistent ranking.
//! The merged baseline is always built on local disk under `--cache-dir`,
//! independent of the LSM `--uri` storage tier.
//!
//! Two phases, selected with `--phase`:
//!
//!   --phase prepare   Load FineWeb text, write the base dataset, create an
//!                     inverted (FTS) index, and initialize MemWAL with the
//!                     index maintained.
//!   --phase search    Ingest rows across LSM levels via ShardWriter, then run
//!                     the local-scoring FTS query panel (+ optional baseline).
//!
//! Example:
//!
//! ```bash
//! cargo bench -p lance --bench mem_wal_fts_read_bench -- \
//!   --phase prepare --uri /tmp/fts_read_bench \
//!   --base-rows 1000000 --cache-dir /tmp/fineweb-cache
//!
//! cargo bench -p lance --bench mem_wal_fts_read_bench -- \
//!   --phase search --uri /tmp/fts_read_bench \
//!   --base-rows 1000000 --max-memtable-rows 100000 \
//!   --queries 200 --k 10 --with-baseline \
//!   --cache-dir /tmp/fineweb-cache --output result.json
//! ```

#![recursion_limit = "256"]
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use lance::dataset::mem_wal::scanner::{
    LsmDataSourceCollector, LsmFtsSearchPlanner, ShardSnapshot,
};
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriterConfig};
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
const HF_API_LISTING: &str =
    "https://huggingface.co/api/datasets/HuggingFaceFW/fineweb/tree/main/sample/10BT";
const HF_FILE_BASE: &str = "https://huggingface.co/datasets/HuggingFaceFW/fineweb/resolve/main/";

// ----------------------------------------------------------------------
// Phase / Args
// ----------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Prepare,
    Search,
}

impl Phase {
    fn parse(value: &str) -> std::result::Result<Self, String> {
        match value {
            "prepare" => Ok(Self::Prepare),
            "search" => Ok(Self::Search),
            _ => Err(format!("unknown phase '{value}', expected prepare|search")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Prepare => "prepare",
            Self::Search => "search",
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
    k: usize,
    /// Build a single-merged-index ground truth and report the top-k
    /// Jaccard between local-LSM scoring and the merged index.
    with_baseline: bool,
    cache_dir: PathBuf,
    output: Option<PathBuf>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            phase: Phase::Search,
            uri: String::new(),
            base_rows: 1_000_000,
            max_memtable_rows: 100_000,
            sstables: 2,
            batch_rows: 1_000,
            queries: 200,
            k: 10,
            with_baseline: false,
            cache_dir: std::env::temp_dir().join("mem_wal_fineweb_fts_cache"),
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
        // Value-less boolean flags handled before consuming a value.
        if flag == "--with-baseline" {
            args.with_baseline = true;
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
            "--k" => args.k = parse_val(&flag, &value)?,
            "--cache-dir" => args.cache_dir = PathBuf::from(value),
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
            "--phase is required (prepare|search)",
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

fn is_cloud_uri(uri: &str) -> bool {
    uri.starts_with("s3://") || uri.starts_with("gs://") || uri.starts_with("az://")
}

// ----------------------------------------------------------------------
// FineWeb loading (mirrors mem_wal_fineweb_fts.rs)
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
            .column_by_name(TEXT_COL)
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
        Field::new(TEXT_COL, DataType::Utf8, true),
    ]))
}

fn make_batch(schema: Arc<ArrowSchema>, start_id: i64, texts: &[String]) -> RecordBatch {
    let ids = Int64Array::from_iter_values(start_id..start_id + texts.len() as i64);
    let text = StringArray::from_iter_values(texts.iter().map(String::as_str));
    RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(text)]).unwrap()
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
// Query set: mid-frequency single terms from the corpus
// ----------------------------------------------------------------------

const STOPWORDS: &[&str] = &[
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "as", "by", "is", "was",
    "are", "were", "be", "been", "being", "this", "that", "these", "those", "it", "its", "but",
    "not", "no", "if", "then", "than", "so", "do", "does", "did", "have", "has", "had", "will",
    "would", "should", "could", "can", "may", "might", "must", "i", "you", "he", "she", "we",
    "they", "them", "his", "her", "their", "our", "us", "me", "my", "your", "him", "at", "from",
];

fn build_query_terms(sample: &[String], n: usize) -> Vec<String> {
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
    // Skip the most-frequent tokens (near-ties in BM25), keep mid-frequency.
    let skip = (by_freq.len() / 4).min(300);
    by_freq
        .into_iter()
        .skip(skip)
        .map(|(t, _)| t)
        .take(n)
        .collect()
}

// ----------------------------------------------------------------------
// Prepare phase
// ----------------------------------------------------------------------

async fn run_prepare(args: &Args) -> Result<()> {
    let start = Instant::now();
    let corpus = load_corpus(args.base_rows, &args.cache_dir).await?;
    let schema = make_schema();

    let total_batches = corpus.len().div_ceil(args.batch_rows);
    let mut batches = Vec::with_capacity(total_batches);
    let mut lo = 0usize;
    while lo < corpus.len() {
        let hi = (lo + args.batch_rows).min(corpus.len());
        batches.push(Ok(make_batch(schema.clone(), lo as i64, &corpus[lo..hi])));
        lo = hi;
    }
    let reader = RecordBatchIterator::new(batches.into_iter(), schema.clone());
    let write_start = Instant::now();
    let mut dataset = Dataset::write(
        reader,
        &args.uri,
        Some(WriteParams {
            data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
            ..Default::default()
        }),
    )
    .await?;
    println!(
        "wrote {} base rows in {:.1}s",
        args.base_rows,
        write_start.elapsed().as_secs_f64()
    );

    let index_start = Instant::now();
    dataset
        .create_index(
            &[TEXT_COL],
            IndexType::Inverted,
            Some(FTS_INDEX_NAME.to_string()),
            &InvertedIndexParams::default(),
            true,
        )
        .await?;
    println!(
        "created FTS index in {:.1}s",
        index_start.elapsed().as_secs_f64()
    );

    dataset
        .initialize_mem_wal()
        .maintained_indexes([FTS_INDEX_NAME])
        .execute()
        .await?;
    println!(
        "prepare complete in {:.1}s: uri={}",
        start.elapsed().as_secs_f64(),
        args.uri
    );
    Ok(())
}

// ----------------------------------------------------------------------
// Search phase
// ----------------------------------------------------------------------

/// Per-query top-k row-id sets + the latency distribution.
struct ModeRun {
    top_ids: Vec<HashSet<i64>>,
    latencies_us: Vec<f64>,
}

/// Run the local-scoring FTS panel through the LSM planner.
async fn run_local(planner: &LsmFtsSearchPlanner, queries: &[String], k: usize) -> Result<ModeRun> {
    let ctx = SessionContext::new();
    let mut top_ids = Vec::with_capacity(queries.len());
    let mut latencies_us = Vec::with_capacity(queries.len());
    for q in queries {
        let t0 = Instant::now();
        let plan = planner
            .plan_search(TEXT_COL, FullTextSearchQuery::new(q.clone()), Some(k), None)
            .await?;
        let stream = plan.execute(0, ctx.task_ctx())?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        latencies_us.push(t0.elapsed().as_micros() as f64);
        top_ids.push(collect_ids(&batches));
    }
    Ok(ModeRun {
        top_ids,
        latencies_us,
    })
}

fn collect_ids(batches: &[RecordBatch]) -> HashSet<i64> {
    let mut ids: HashSet<i64> = HashSet::new();
    for b in batches {
        if let Some(col) = b
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        {
            for i in 0..col.len() {
                ids.insert(col.value(i));
            }
        }
    }
    ids
}

/// Build a single-merged-index ground truth on local disk and return the
/// top-k row-id set per query.
///
/// The merged dataset is the union of every LSM row — base-table rows
/// (scanned back from `base_dataset`) plus the memtable rows (ids
/// `id_base..`, text `mt_text`) — written into one Lance dataset with one
/// FTS index, then queried via the standard `scanner.full_text_search`.
/// Always built on local disk under `cache_dir` regardless of the LSM
/// storage tier, since this measures ranking accuracy, not storage perf.
async fn build_and_query_baseline(
    base_dataset: &Dataset,
    mt_text: &[String],
    id_base: i64,
    queries: &[String],
    k: usize,
    cache_dir: &std::path::Path,
    batch_rows: usize,
) -> Result<Vec<HashSet<i64>>> {
    let schema = make_schema();

    // 1. Pull base rows (id, text) back out of the base dataset.
    let mut base_scanner = base_dataset.scan();
    base_scanner.project(&["id", TEXT_COL])?;
    let base_batches: Vec<RecordBatch> =
        base_scanner.try_into_stream().await?.try_collect().await?;

    let merged_uri = cache_dir
        .join(format!("merged_baseline_{}", Uuid::new_v4()))
        .to_string_lossy()
        .to_string();

    // 2. Write base rows + memtable rows into one dataset.
    let mut batches: Vec<RecordBatch> = base_batches;
    let mut lo = 0usize;
    while lo < mt_text.len() {
        let hi = (lo + batch_rows).min(mt_text.len());
        batches.push(make_batch(
            schema.clone(),
            id_base + lo as i64,
            &mt_text[lo..hi],
        ));
        lo = hi;
    }
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    let mut merged = Dataset::write(
        reader,
        &merged_uri,
        Some(WriteParams {
            data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
            ..Default::default()
        }),
    )
    .await?;
    merged
        .create_index(
            &[TEXT_COL],
            IndexType::Inverted,
            Some(FTS_INDEX_NAME.to_string()),
            &InvertedIndexParams::default(),
            true,
        )
        .await?;

    // 3. Query the merged index with the same query set.
    let mut top_ids = Vec::with_capacity(queries.len());
    for q in queries {
        let mut scanner = merged.scan();
        scanner.project(&["id", TEXT_COL])?;
        scanner.full_text_search(
            FullTextSearchQuery::new(q.clone())
                .with_column(TEXT_COL.to_string())?
                .limit(Some(k as i64)),
        )?;
        let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
        top_ids.push(collect_ids(&batches));
    }
    Ok(top_ids)
}

fn mean_jaccard(a: &[HashSet<i64>], b: &[HashSet<i64>]) -> f64 {
    let pairs: Vec<f64> = a
        .iter()
        .zip(b.iter())
        .filter_map(|(x, y)| {
            if x.is_empty() && y.is_empty() {
                None
            } else {
                let inter = x.intersection(y).count() as f64;
                let union = x.union(y).count() as f64;
                Some(inter / union)
            }
        })
        .collect();
    if pairs.is_empty() {
        0.0
    } else {
        pairs.iter().sum::<f64>() / pairs.len() as f64
    }
}

async fn run_search(args: &Args) -> Result<serde_json::Value> {
    let dataset = Arc::new(Dataset::open(&args.uri).await?);
    let arrow_schema: Arc<ArrowSchema> = Arc::new(ArrowSchema::from(dataset.schema()));
    let schema = make_schema();

    // Memtable text is drawn from FineWeb; row *ids* are assigned past the
    // base slice (via `id_base`) so they don't collide with base-table ids,
    // but the *text content* can reuse FineWeb rows freely — BM25 latency
    // doesn't depend on content novelty. Load just enough rows once,
    // covering both the memtable payload and the query-term sample, instead
    // of re-reading the whole base corpus from parquet.
    let active_rows = args.max_memtable_rows / 2;
    let total_memtable_rows = args.sstables * args.max_memtable_rows + active_rows;
    let sample_rows = args.base_rows.min(50_000);
    let load_rows = total_memtable_rows.max(sample_rows);
    println!("loading {load_rows} FineWeb rows for memtable payload + query sample ...");
    let mt_corpus = load_corpus(load_rows, &args.cache_dir).await?;
    let mt_text = &mt_corpus[..total_memtable_rows];

    let shard_id = Uuid::new_v4();
    let row_bytes = 2048; // rough FineWeb text row size
    // The memtable flush trigger is `estimated_size >= max_memtable_size ||
    // batch_store_full`. FineWeb text rows vary in size, so a byte threshold
    // is an unreliable way to flush exactly one generation per
    // `max_memtable_rows`. Instead make the *batch-count* cap the trigger:
    // set `max_memtable_batches` to one generation's worth of batches so the
    // store fills (and flushes) precisely at each generation boundary,
    // independent of text length. Keep `max_memtable_size` high so it never
    // pre-empts the batch-count trigger.
    let batches_per_gen = (args.max_memtable_rows / args.batch_rows).max(1);
    let config = ShardWriterConfig {
        shard_id,
        max_wal_persist_retries: 3,
        wal_persist_retry_base_delay: std::time::Duration::from_millis(50),
        shard_spec_id: 0,
        durable_write: false,
        max_memtable_size: args.max_memtable_rows * row_bytes * 100,
        max_memtable_rows: args.max_memtable_rows,
        max_memtable_batches: batches_per_gen,
        max_unflushed_memtable_bytes: args.max_memtable_rows * row_bytes * 20,
        max_wal_flush_interval: Some(Duration::from_secs(60)),
        ..ShardWriterConfig::default()
    };
    let writer = dataset.mem_wal_writer(shard_id, config).await?;

    let flush_wait = if is_cloud_uri(&args.uri) {
        Duration::from_secs(5)
    } else {
        Duration::from_millis(500)
    };

    // Ingest SSTables + 1 active (50% full).
    let mut gen_sizes: Vec<usize> = (0..args.sstables).map(|_| args.max_memtable_rows).collect();
    gen_sizes.push(active_rows);

    let id_base = args.base_rows as i64;
    let mut cursor = 0usize;
    let ingest_start = Instant::now();
    for (gen_idx, &gen_rows) in gen_sizes.iter().enumerate() {
        let mut written = 0usize;
        while written < gen_rows {
            let chunk = args.batch_rows.min(gen_rows - written);
            let start = id_base + (cursor) as i64;
            let slice = &mt_text[cursor..cursor + chunk];
            let batch = make_batch(schema.clone(), start, slice);
            writer.put(vec![batch]).await?;
            cursor += chunk;
            written += chunk;
        }
        let is_sstable = gen_idx < args.sstables;
        println!(
            "  gen {}: wrote {} rows ({})",
            gen_idx + 1,
            gen_rows,
            if is_sstable { "sstable" } else { "active" }
        );
        if is_sstable {
            tokio::time::sleep(flush_wait).await;
        }
    }
    // Wait for any triggered (sealed) memtable flushes to commit to the
    // manifest before we snapshot it — otherwise the SSTables
    // race the read and may not all be visible yet.
    writer.wait_for_flush_drain().await?;
    println!(
        "ingested {} memtable rows in {:.1}s",
        cursor,
        ingest_start.elapsed().as_secs_f64()
    );

    let manifest = writer.manifest().await?;
    let in_memory_refs = writer.in_memory_memtable_refs().await?;
    let mut shard_snapshot = ShardSnapshot::new(shard_id);
    if let Some(ref m) = manifest {
        shard_snapshot = shard_snapshot.with_current_generation(m.current_generation);
        for sstable in &m.sstables {
            shard_snapshot = shard_snapshot.with_sstable(sstable.generation, sstable.path.clone());
        }
    }
    let num_sstables = manifest.as_ref().map(|m| m.sstables.len()).unwrap_or(0);
    println!("manifest: {num_sstables} SSTables");

    // SSTables carry the same maintained secondary indexes as
    // the active memtable: the flush handler builds them during flush
    // (lance #6901), so each generation already has the FTS index and
    // both scoring modes use the fast indexed path. No manual indexing
    // step is needed here. (The index-less flat fallback in the rescore
    // planner is still exercised by unit tests for the no-maintained-index
    // case.)

    let collector = LsmDataSourceCollector::new(dataset.clone(), vec![shard_snapshot])
        .with_in_memory_memtables(shard_id, in_memory_refs);
    let pk_columns = vec!["id".to_string()];
    let planner = LsmFtsSearchPlanner::new(collector, pk_columns, arrow_schema);

    // Query set: mid-frequency terms from a sample of the loaded corpus.
    println!("building query set ...");
    let sample_end = mt_corpus
        .len()
        .min(sample_rows.max(total_memtable_rows.min(50_000)));
    let queries = build_query_terms(&mt_corpus[..sample_end], args.queries);
    println!("picked {} query terms", queries.len());

    // Run the local-scoring panel.
    println!(
        "running Local mode ({} queries, k={}) ...",
        queries.len(),
        args.k
    );
    let local = run_local(&planner, &queries, args.k).await?;
    let local_stats = compute_stats(local.latencies_us.clone());
    println!(
        "local:   p50={}us p95={}us p99={}us mean={:.0}us qps={:.0}",
        local_stats.p50_us,
        local_stats.p95_us,
        local_stats.p99_us,
        local_stats.mean_us,
        local_stats.qps
    );

    // Optional accuracy: top-k Jaccard vs a single-merged-index ground truth.
    let baseline_jaccard = if args.with_baseline {
        println!("building merged-index baseline (local disk) ...");
        let baseline_ids = build_and_query_baseline(
            &dataset,
            mt_text,
            id_base,
            &queries,
            args.k,
            &args.cache_dir,
            args.batch_rows,
        )
        .await?;
        let j = mean_jaccard(&local.top_ids, &baseline_ids);
        println!("top-{} jaccard local-vs-merged = {:.4}", args.k, j);
        Some(j)
    } else {
        None
    };

    // Keep writer alive so the active memtable stays reachable.
    std::mem::forget(writer);

    Ok(json!({
        "bench": "mem_wal_fts_read",
        "phase": "search",
        "uri_kind": if is_cloud_uri(&args.uri) { "cloud" } else { "local" },
        "base_rows": args.base_rows,
        "max_memtable_rows": args.max_memtable_rows,
        "sstables": num_sstables,
        "active_rows": active_rows,
        "k": args.k,
        "queries": queries.len(),
        "jaccard_local_vs_merged": baseline_jaccard,
        "local": {
            "p50_us": local_stats.p50_us,
            "p95_us": local_stats.p95_us,
            "p99_us": local_stats.p99_us,
            "mean_us": local_stats.mean_us as u64,
            "qps": local_stats.qps as u64,
        },
    }))
}

// ----------------------------------------------------------------------
// Entrypoint
// ----------------------------------------------------------------------

async fn run(args: Args) -> Result<()> {
    println!(
        "bench=mem_wal_fts_read phase={} uri={} base_rows={} max_memtable_rows={} sstables={} queries={} k={} with_baseline={}",
        args.phase.as_str(),
        args.uri,
        args.base_rows,
        args.max_memtable_rows,
        args.sstables,
        args.queries,
        args.k,
        args.with_baseline,
    );

    match args.phase {
        Phase::Prepare => run_prepare(&args).await?,
        Phase::Search => {
            let result = run_search(&args).await?;
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
