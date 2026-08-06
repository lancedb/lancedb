// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Head-to-head KV point-lookup benchmark: Lance MemTable vs RocksDB.
//!
//! One binary times **both** engines with identical key/value/query sets and
//! identical timing code, so the comparison is apples-to-apples. The RocksDB
//! arm is compiled only with `--features bench-rocksdb` (bundled librocksdb);
//! without it the bench runs the Lance arm alone.
//!
//! Both engines hold all `--rows` rows in a **single in-memory write buffer**
//! (Lance: one active MemTable, ShardWriter configured to never flush;
//! RocksDB: one skiplist memtable, `write_buffer_size` above the dataset so no
//! SST flush). The table has a **BTree index on the key column**; the Lance
//! MemTable maintains it. We measure:
//!
//!   - **write throughput** (rows/sec) for a fixed shuffled insert order
//!   - **read latency** (p50/p95/p99/mean, single-thread) and **QPS**
//!     (single- and N-thread) for a query set mixing hits and guaranteed
//!     misses (`--miss-ratio`)
//!   - **CPU** (getrusage user+sys per phase) and **peak RSS** (sampled from
//!     `/proc/self/statm` on Linux)
//!
//! Example:
//!
//! ```bash
//! cargo bench -p lance --bench mem_wal_kv_point_lookup --features bench-rocksdb -- \
//!   --rows 1000000 --value-size 100 --queries 5000 --miss-ratio 0.5 \
//!   --threads 8 --engine both --uri /tmp/kv_bench --output result.json
//! ```

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::ScalarValue;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use lance::dataset::mem_wal::scanner::{
    InMemoryMemTableRef, LsmDataSourceCollector, LsmPointLookupPlanner, ShardSnapshot, SsTableCache,
};
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriterConfig};
use lance::dataset::{Dataset, WriteParams};
use lance::index::DatasetIndexExt;
use lance::index::DatasetIndexInternalExt;
use lance_core::Result;
use lance_index::IndexType;
use lance_index::scalar::ScalarIndexParams;
use serde_json::json;
use uuid::Uuid;

const KEY_COL: &str = "id";
const VALUE_COL: &str = "value";
const BTREE_INDEX_NAME: &str = "id_btree";

// ----------------------------------------------------------------------
// Deterministic PRNG (SplitMix64) — no external rand dependency, identical
// key/query streams across engines and across runs given the same seed.
// ----------------------------------------------------------------------

struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    /// Uniform in `[0, n)`.
    fn next_below(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        self.next_u64() % n
    }
}

/// Fisher-Yates shuffle of `0..n` driven by the deterministic PRNG.
fn shuffled_keys(n: usize, seed: u64) -> Vec<i64> {
    let mut rng = SplitMix64::new(seed);
    let mut v: Vec<i64> = (0..n as i64).collect();
    for i in (1..n).rev() {
        let j = rng.next_below(i as u64 + 1) as usize;
        v.swap(i, j);
    }
    v
}

/// Fixed-size ASCII payload derived from the key (valid UTF-8 so it can be
/// stored in both a Lance `Utf8` column and a RocksDB byte value unchanged).
fn make_value(key: i64, value_size: usize) -> Vec<u8> {
    let mut buf = vec![0u8; value_size];
    let base = key as u64;
    for (i, b) in buf.iter_mut().enumerate() {
        *b = b'a' + ((base.wrapping_add(i as u64)) % 26) as u8;
    }
    buf
}

// ----------------------------------------------------------------------
// Query set: a mix of guaranteed hits (existing keys) and guaranteed misses
// (keys in [rows, 2*rows)). Same set fed to both engines.
// ----------------------------------------------------------------------

fn build_queries(rows: usize, queries: usize, miss_ratio: f64, seed: u64) -> Vec<(i64, bool)> {
    let mut rng = SplitMix64::new(seed ^ 0xD1B54A32D192ED03);
    let misses = ((queries as f64) * miss_ratio).round() as usize;
    let mut out = Vec::with_capacity(queries);
    for i in 0..queries {
        if i < misses {
            // Guaranteed absent: [rows, 2*rows)
            let k = rows as i64 + rng.next_below(rows.max(1) as u64) as i64;
            out.push((k, false));
        } else {
            let k = rng.next_below(rows.max(1) as u64) as i64;
            out.push((k, true));
        }
    }
    // Interleave so hits and misses aren't phase-separated.
    for i in (1..out.len()).rev() {
        let j = rng.next_below(i as u64 + 1) as usize;
        out.swap(i, j);
    }
    out
}

// ----------------------------------------------------------------------
// Lance direct BTree fast-path (bypasses DataFusion)
// ----------------------------------------------------------------------

/// Resolve a single key against the active MemTable's BTree index without
/// building a DataFusion plan: probe the index, honor the MVCC visibility
/// watermark, pick the newest matching row position, and slice that one row
/// out of the BatchStore. Returns `None` if the key isn't present/visible.
///
/// This mirrors what `BTreeIndexExec` does internally, minus the plan/stream
/// machinery — it is the lower bound on how fast the current MemTable index
/// can answer a point lookup. Single-active-memtable only (the bench never
/// flushes), `KEY_COL` BTree assumed present.
fn fast_lookup(active: &InMemoryMemTableRef, key: i64, key_type: KeyType) -> Option<RecordBatch> {
    use arrow_array::Array;

    let btree = active.index_store.get_btree_by_column(KEY_COL)?;
    let max_vbp = active.index_store.max_visible_batch_position();

    // Highest visible row (exclusive end) across batches whose position is
    // within the watermark. Batch position == iteration index for a
    // never-flushed store.
    let mut visible_end: u64 = 0;
    for (bp, sb) in active.batch_store.iter().enumerate() {
        if bp <= max_vbp {
            visible_end += sb.num_rows as u64;
        } else {
            break;
        }
    }
    if visible_end == 0 {
        return None;
    }
    let max_visible_row = visible_end - 1;

    // Newest visible row position for this key — seek-and-stop on the skiplist.
    let pos = btree.get_newest_visible(&key_scalar(key, key_type), max_visible_row)?;

    // Map the global position to (batch, row) and slice one row.
    let mut start: u64 = 0;
    for sb in active.batch_store.iter() {
        let end = start + sb.num_rows as u64;
        if pos >= start && pos < end {
            let row = (pos - start) as usize;
            let cols: Vec<_> = sb.data.columns().iter().map(|c| c.slice(row, 1)).collect();
            return RecordBatch::try_new(sb.data.schema(), cols).ok();
        }
        start = end;
    }
    None
}

/// Batch get: resolve many keys against the active MemTable BTree in one pass,
/// gathering all matching rows per source batch with a single vectorized
/// `take`, then concatenating into one RecordBatch. This amortizes per-call
/// overhead across the whole batch and uses Arrow's columnar gather — the
/// shape where the batch system should beat row-at-a-time point lookups.
/// Returns the found rows (missing keys omitted).
fn fast_lookup_batch(active: &InMemoryMemTableRef, keys: &[i64], key_type: KeyType) -> RecordBatch {
    use arrow_array::UInt32Array;
    use arrow_select::concat::concat_batches;
    use arrow_select::take::take;

    let schema = active.schema.clone();
    let Some(btree) = active.index_store.get_btree_by_column(KEY_COL) else {
        return RecordBatch::new_empty(schema);
    };
    let len = active.batch_store.len();
    if len == 0 {
        return RecordBatch::new_empty(schema);
    }
    let max_vbp = active.index_store.max_visible_batch_position();
    let last_visible_idx = max_vbp.min(len - 1);
    let last = active.batch_store.get(last_visible_idx).unwrap();
    let visible_end = last.row_offset + last.num_rows as u64;

    // Group target rows by their owning batch so each batch is gathered once.
    let mut by_batch: HashMap<usize, Vec<u32>> = HashMap::new();
    for &k in keys {
        let Some(pos) = btree.get_newest_visible(&key_scalar(k, key_type), visible_end - 1) else {
            continue;
        };
        let (mut lo, mut hi) = (0usize, last_visible_idx);
        while lo < hi {
            let mid = lo + (hi - lo).div_ceil(2);
            if active.batch_store.get(mid).unwrap().row_offset <= pos {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        let stored = active.batch_store.get(lo).unwrap();
        by_batch
            .entry(lo)
            .or_default()
            .push((pos - stored.row_offset) as u32);
    }

    let mut out = Vec::with_capacity(by_batch.len());
    for (bid, rows) in by_batch {
        let stored = active.batch_store.get(bid).unwrap();
        let idx = UInt32Array::from(rows);
        let cols: Vec<_> = stored
            .data
            .columns()
            .iter()
            .map(|c| take(c.as_ref(), &idx, None).unwrap())
            .collect();
        out.push(RecordBatch::try_new(stored.data.schema(), cols).unwrap());
    }
    if out.is_empty() {
        return RecordBatch::new_empty(schema);
    }
    concat_batches(&out[0].schema(), &out).unwrap()
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
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    mean_us: f64,
}

/// `latencies_us` carries sub-microsecond precision (nanoseconds / 1000), so
/// RocksDB's sub-µs point gets don't collapse to 0.
fn compute_stats(mut latencies_us: Vec<f64>) -> LatencyStats {
    latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mean = latencies_us.iter().sum::<f64>() / latencies_us.len().max(1) as f64;
    LatencyStats {
        p50_us: percentile(&latencies_us, 50.0),
        p95_us: percentile(&latencies_us, 95.0),
        p99_us: percentile(&latencies_us, 99.0),
        mean_us: mean,
    }
}

// ----------------------------------------------------------------------
// Profiling: CPU (getrusage) + peak RSS (/proc/self/statm sampler)
// ----------------------------------------------------------------------

/// User+sys CPU seconds consumed by the whole process so far.
fn process_cpu_secs() -> f64 {
    // SAFETY: getrusage with a zeroed rusage out-param is always sound.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
            return 0.0;
        }
        let u = ru.ru_utime.tv_sec as f64 + ru.ru_utime.tv_usec as f64 / 1e6;
        let s = ru.ru_stime.tv_sec as f64 + ru.ru_stime.tv_usec as f64 / 1e6;
        u + s
    }
}

/// Current resident set size in bytes (0 if unavailable, e.g. non-Linux).
fn current_rss_bytes() -> u64 {
    // /proc/self/statm: field 2 is resident pages.
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
        return 0;
    };
    let mut it = statm.split_whitespace();
    let _total = it.next();
    let Some(resident) = it.next().and_then(|s| s.parse::<u64>().ok()) else {
        return 0;
    };
    let page = 4096u64; // Linux default page size
    resident * page
}

/// Background thread sampling peak RSS until stopped.
struct RssSampler {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicU64>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl RssSampler {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicU64::new(current_rss_bytes()));
        let stop_c = stop.clone();
        let peak_c = peak.clone();
        let handle = std::thread::spawn(move || {
            while !stop_c.load(Ordering::Relaxed) {
                let rss = current_rss_bytes();
                peak_c.fetch_max(rss, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(2));
            }
        });
        Self {
            stop,
            peak,
            handle: Some(handle),
        }
    }
    fn peak_mb(&self) -> f64 {
        self.peak.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0)
    }
    fn stop(mut self) -> f64 {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        self.peak_mb()
    }
}

// ----------------------------------------------------------------------
// Engine result
// ----------------------------------------------------------------------

#[derive(Clone)]
struct EngineResult {
    engine: &'static str,
    write_rows_per_s: f64,
    write_cpu_s: f64,
    read_p50_us: f64,
    read_p95_us: f64,
    read_p99_us: f64,
    read_mean_us: f64,
    read_qps_1t: f64,
    read_qps_nt: f64,
    read_cpu_s: f64,
    hits: usize,
    misses_resolved: usize,
    peak_rss_mb: f64,
    rss_after_load_mb: f64,
}

impl EngineResult {
    fn to_json(&self, args: &Args) -> serde_json::Value {
        json!({
            "engine": self.engine,
            "rows": args.rows,
            "value_size": args.value_size,
            "queries": args.queries,
            "miss_ratio": args.miss_ratio,
            "threads": args.threads,
            "write_rows_per_s": self.write_rows_per_s as u64,
            "write_cpu_s": format!("{:.3}", self.write_cpu_s),
            "read_p50_us": (self.read_p50_us * 1000.0).round() / 1000.0,
            "read_p95_us": (self.read_p95_us * 1000.0).round() / 1000.0,
            "read_p99_us": (self.read_p99_us * 1000.0).round() / 1000.0,
            "read_mean_us": (self.read_mean_us * 1000.0).round() / 1000.0,
            "read_qps_1t": self.read_qps_1t as u64,
            "read_qps_nt": self.read_qps_nt as u64,
            "read_cpu_s": format!("{:.3}", self.read_cpu_s),
            "hits": self.hits,
            "misses_resolved": self.misses_resolved,
            "peak_rss_mb": self.peak_rss_mb as u64,
            "rss_after_load_mb": self.rss_after_load_mb as u64,
        })
    }
}

// ----------------------------------------------------------------------
// CLI args
// ----------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Engine {
    Lance,
    Rocksdb,
    Both,
}

/// How the Lance arm resolves a point lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LanceReadMode {
    /// Build + execute a DataFusion `ExecutionPlan` per lookup (production
    /// `LsmPointLookupPlanner::plan_lookup` path).
    Plan,
    /// Probe the active MemTable's BTree index directly and materialize the
    /// row from the BatchStore, bypassing DataFusion. Single-active-memtable
    /// fast path (no SSTables); misses fall through as "not found".
    Fast,
    /// Call the production `LsmPointLookupPlanner::lookup` API, which uses the
    /// direct BTree fast path internally and falls back to the plan path for
    /// on-disk sources. Measures the real shipped point-lookup latency.
    Api,
}

impl LanceReadMode {
    fn parse(v: &str) -> std::result::Result<Self, String> {
        match v {
            "plan" => Ok(Self::Plan),
            "fast" => Ok(Self::Fast),
            "api" => Ok(Self::Api),
            _ => Err(format!(
                "unknown lance-read-mode '{v}', expected plan|fast|api"
            )),
        }
    }
    fn as_str(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Fast => "fast",
            Self::Api => "api",
        }
    }
}

impl Engine {
    fn parse(v: &str) -> std::result::Result<Self, String> {
        match v {
            "lance" => Ok(Self::Lance),
            "rocksdb" => Ok(Self::Rocksdb),
            "both" => Ok(Self::Both),
            _ => Err(format!("unknown engine '{v}', expected lance|rocksdb|both")),
        }
    }
}

/// Key column type. `Int` exercises the Lance `FixedKey` backend (8-byte key);
/// `Uuid` stores `FixedSizeBinary(16)` and exercises the `BytesKey` backend.
#[derive(Debug, Clone, Copy, PartialEq)]
enum KeyType {
    Int,
    Uuid,
}

impl KeyType {
    fn parse(v: &str) -> std::result::Result<Self, String> {
        match v {
            "int" | "i64" => Ok(Self::Int),
            "uuid" => Ok(Self::Uuid),
            _ => Err(format!("unknown key-type '{v}', expected int|uuid")),
        }
    }
    fn as_str(self) -> &'static str {
        match self {
            Self::Int => "int",
            Self::Uuid => "uuid",
        }
    }
    fn arrow_type(self) -> DataType {
        match self {
            Self::Int => DataType::Int64,
            Self::Uuid => DataType::FixedSizeBinary(16),
        }
    }
}

/// Where the data under test lives. `Active` = the in-memory active MemTable
/// (never flushed). `SsTable` = an on-disk SSTable (a Lance data
/// file + on-disk BTree index, read via the indexed-scan path) vs a single
/// RocksDB SST on disk.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Storage {
    Active,
    SsTable,
}

impl Storage {
    fn parse(v: &str) -> std::result::Result<Self, String> {
        match v {
            "active" => Ok(Self::Active),
            "sstable" => Ok(Self::SsTable),
            _ => Err(format!("unknown storage '{v}', expected active|sstable")),
        }
    }
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::SsTable => "sstable",
        }
    }
}

/// Deterministic 16-byte UUID for a logical key: a scrambled high half (so keys
/// scatter across the byte space like real UUIDs) plus the key in the low half
/// (so distinct keys never collide). Same key → same bytes, so lookups resolve.
fn uuid_bytes(key: i64) -> [u8; 16] {
    let mut rng = SplitMix64::new((key as u64) ^ 0xA5A5_5A5A_DEAD_BEEF);
    *Uuid::from_u64_pair(rng.next_u64(), key as u64).as_bytes()
}

/// The Lance lookup key (`ScalarValue`) for a logical key under `key_type`.
fn key_scalar(key: i64, key_type: KeyType) -> ScalarValue {
    match key_type {
        KeyType::Int => ScalarValue::Int64(Some(key)),
        KeyType::Uuid => ScalarValue::FixedSizeBinary(16, Some(uuid_bytes(key).to_vec())),
    }
}

/// The RocksDB key bytes for a logical key under `key_type`.
#[cfg(feature = "bench-rocksdb")]
fn rocks_key(key: i64, key_type: KeyType) -> Vec<u8> {
    match key_type {
        KeyType::Int => key.to_be_bytes().to_vec(),
        KeyType::Uuid => uuid_bytes(key).to_vec(),
    }
}

#[derive(Debug, Clone)]
struct Args {
    rows: usize,
    value_size: usize,
    queries: usize,
    miss_ratio: f64,
    threads: usize,
    batch_rows: usize,
    engine: Engine,
    key_type: KeyType,
    storage: Storage,
    /// Number of SSTables below the single active MemTable (Lance) /
    /// immutable SSTs below the active memtable (RocksDB). 0 = the existing
    /// single-tier behavior. >0 builds a full LSM: rows are split into
    /// `generations+1` parts, the first `generations` are flushed to on-disk
    /// generations/SSTs and the last stays active; lookups traverse the tiers.
    generations: usize,
    lance_read_mode: LanceReadMode,
    /// When > 0, the read phase measures **batch get** of this many keys per
    /// call (Lance: one vectorized BTree gather; RocksDB: `multi_get`) instead
    /// of single-key point lookups, reporting keys/sec. Sync on both sides
    /// (std threads) to compare the engines' batch primitives directly.
    batch_get: usize,
    uri: String,
    seed: u64,
    /// Skip the RocksDB WAL on writes. Off by default so RocksDB writes a WAL
    /// like Lance's durable MemTable path, keeping the write comparison fair.
    rocksdb_disable_wal: bool,
    /// Cold-storage mode: assume the dataset is larger than RAM so reads miss
    /// the caches and hit NVMe. Caps the RocksDB write buffer + uses a small
    /// block cache + compacts to one SST, and drops the OS page cache before
    /// the read phase (both engines). Use with a `--rows`×`--value-size` larger
    /// than RAM. Only affects the `--storage sstable` path.
    cold: bool,
    /// Prewarm all SSTables (open + warm indexes) into the dataset
    /// session before the read phase, via `DatasetMemWalExt::prewarm_mem_wal`.
    /// Default on. `--prewarm false` disables it to measure the lazy-warm
    /// baseline (the SSTable cache is still set, so each generation is opened
    /// on its first gen-key lookup instead of up front). Only affects the Lance
    /// `--storage active` LSM path.
    prewarm: bool,
    output: Option<PathBuf>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            rows: 1_000_000,
            value_size: 100,
            queries: 5_000,
            miss_ratio: 0.5,
            threads: 8,
            batch_rows: 1_000,
            engine: Engine::Both,
            key_type: KeyType::Int,
            storage: Storage::Active,
            generations: 0,
            lance_read_mode: LanceReadMode::Plan,
            batch_get: 0,
            uri: String::new(),
            seed: 0x5EED,
            rocksdb_disable_wal: false,
            cold: false,
            prewarm: true,
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
    let mut has_uri = false;
    while let Some(flag) = iter.next() {
        if flag == "--bench" {
            continue;
        }
        if flag == "--rocksdb-disable-wal" {
            args.rocksdb_disable_wal = true;
            continue;
        }
        if flag == "--cold" {
            args.cold = true;
            continue;
        }
        let value = iter
            .next()
            .ok_or_else(|| lance_core::Error::invalid_input(format!("missing value for {flag}")))?;
        match flag.as_str() {
            "--rows" => args.rows = parse_val(&flag, &value)?,
            "--value-size" => args.value_size = parse_val(&flag, &value)?,
            "--queries" => args.queries = parse_val(&flag, &value)?,
            "--miss-ratio" => args.miss_ratio = parse_val(&flag, &value)?,
            "--threads" => args.threads = parse_val(&flag, &value)?,
            "--batch-rows" => args.batch_rows = parse_val(&flag, &value)?,
            "--batch-get" => args.batch_get = parse_val(&flag, &value)?,
            "--engine" => {
                args.engine = Engine::parse(&value).map_err(lance_core::Error::invalid_input)?
            }
            "--key-type" => {
                args.key_type = KeyType::parse(&value).map_err(lance_core::Error::invalid_input)?
            }
            "--storage" => {
                args.storage = Storage::parse(&value).map_err(lance_core::Error::invalid_input)?
            }
            "--generations" => args.generations = parse_val(&flag, &value)?,
            "--prewarm" => args.prewarm = parse_val(&flag, &value)?,
            "--lance-read-mode" => {
                args.lance_read_mode =
                    LanceReadMode::parse(&value).map_err(lance_core::Error::invalid_input)?
            }
            "--uri" => {
                args.uri = value;
                has_uri = true;
            }
            "--seed" => args.seed = parse_val(&flag, &value)?,
            "--output" => args.output = Some(PathBuf::from(value)),
            _ => {
                return Err(lance_core::Error::invalid_input(format!(
                    "unknown argument: {flag}"
                )));
            }
        }
    }
    if !has_uri {
        return Err(lance_core::Error::invalid_input("--uri is required"));
    }
    if args.rows == 0 || args.batch_rows == 0 || args.value_size == 0 || args.queries == 0 {
        return Err(lance_core::Error::invalid_input(
            "rows, batch-rows, value-size, queries must be > 0",
        ));
    }
    if !(0.0..=1.0).contains(&args.miss_ratio) {
        return Err(lance_core::Error::invalid_input(
            "miss-ratio must be in [0, 1]",
        ));
    }
    Ok(args)
}

// ----------------------------------------------------------------------
// Schema / batch helpers (Lance)
// ----------------------------------------------------------------------

fn make_schema(key_type: KeyType) -> Arc<ArrowSchema> {
    let mut id_meta = HashMap::new();
    id_meta.insert(
        "lance-schema:unenforced-primary-key".to_string(),
        "true".to_string(),
    );
    Arc::new(ArrowSchema::new(vec![
        Field::new(KEY_COL, key_type.arrow_type(), false).with_metadata(id_meta),
        Field::new(VALUE_COL, DataType::Utf8, true),
    ]))
}

fn make_batch(
    schema: Arc<ArrowSchema>,
    keys: &[i64],
    value_size: usize,
    key_type: KeyType,
) -> RecordBatch {
    use arrow_array::Array;
    let id_arr: Arc<dyn Array> = match key_type {
        KeyType::Int => Arc::new(Int64Array::from_iter_values(keys.iter().copied())),
        KeyType::Uuid => Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_iter(keys.iter().map(|k| uuid_bytes(*k)))
                .unwrap(),
        ),
    };
    let values: Vec<String> = keys
        .iter()
        .map(|k| {
            // make_value is valid ASCII, so from_utf8 never fails.
            String::from_utf8(make_value(*k, value_size)).unwrap()
        })
        .collect();
    let value_arr = StringArray::from_iter_values(values);
    RecordBatch::try_new(schema, vec![id_arr, Arc::new(value_arr)]).unwrap()
}

// ----------------------------------------------------------------------
// Lance engine
// ----------------------------------------------------------------------

async fn run_lance(
    args: &Args,
    insert_order: &[i64],
    queries: &[(i64, bool)],
) -> Result<EngineResult> {
    let sampler = RssSampler::start();
    let key_type = args.key_type;
    let schema = make_schema(key_type);

    // 1-row sentinel base dataset (id = -1) so the lookup path is effectively
    // MemTable-only: query keys are 0..rows, never in the base table. The
    // base only ever contributes a 1-row scan on the miss path.
    let base_uri = format!("{}/base", args.uri.trim_end_matches('/'));
    let sentinel = make_batch(schema.clone(), &[-1], args.value_size, key_type);
    let reader = RecordBatchIterator::new([Ok(sentinel)], schema.clone());
    let mut dataset = Dataset::write(reader, &base_uri, Some(WriteParams::default())).await?;

    // BTree index on the key column, maintained by the MemTable.
    dataset
        .create_index(
            &[KEY_COL],
            IndexType::BTree,
            Some(BTREE_INDEX_NAME.to_string()),
            &ScalarIndexParams::default(),
            true,
        )
        .await?;
    dataset
        .initialize_mem_wal()
        .maintained_indexes([BTREE_INDEX_NAME])
        .execute()
        .await?;

    let dataset = Arc::new(dataset);
    let arrow_schema: Arc<ArrowSchema> = Arc::new(ArrowSchema::from(dataset.schema()));

    // No-flush config: every *memtable*-flush threshold is set above the
    // dataset so the single active MemTable holds all rows (no generation is
    // sealed to disk). Read visibility is gated on the WAL durability
    // watermark (`max_visible_batch_position`), which only advances on a WAL
    // flush — so we use `durable_write=true`: each put flushes its batch to
    // the WAL and awaits, which both populates the maintained BTree and
    // advances the watermark, leaving every row visible the moment the write
    // loop ends (no background-drain race). This is the durable ingestion
    // path; per the goal it is acceptable for writes to be slower than
    // RocksDB. The RocksDB arm keeps its WAL on by default too (see
    // `--rocksdb-disable-wal`) so the write comparison is apples-to-apples.
    let shard_id = Uuid::new_v4();
    let big = args.rows.saturating_mul(args.value_size + 256).max(1 << 30);
    let config = ShardWriterConfig {
        shard_id,
        max_wal_persist_retries: 3,
        wal_persist_retry_base_delay: std::time::Duration::from_millis(50),
        shard_spec_id: 0,
        durable_write: true,
        max_memtable_size: big,
        max_memtable_rows: args.rows * 4 + 1_000_000,
        max_memtable_batches: args.rows / args.batch_rows + 1_000_000,
        max_unflushed_memtable_bytes: big,
        max_wal_flush_interval: Some(Duration::from_millis(100)),
        ..ShardWriterConfig::default()
    };
    let writer = dataset.mem_wal_writer(shard_id, config).await?;

    // --- write phase ---
    // LSM: split rows into `generations+1` parts; seal+flush after each of the
    // first `generations` parts (each becomes an on-disk generation) and leave
    // the last part in the active MemTable.
    let gens = args.generations;
    let part = (insert_order.len() / (gens + 1)).max(1);
    let cpu0 = process_cpu_secs();
    let t_write = Instant::now();
    let mut lo = 0usize;
    for g in 0..=gens {
        let part_end = if g < gens {
            ((g + 1) * part).min(insert_order.len())
        } else {
            insert_order.len()
        };
        while lo < part_end {
            let hi = (lo + args.batch_rows).min(part_end);
            let batch = make_batch(
                schema.clone(),
                &insert_order[lo..hi],
                args.value_size,
                key_type,
            );
            writer.put(vec![batch]).await?;
            lo = hi;
        }
        if g < gens {
            writer.force_seal_active().await?;
            for _ in 0..600 {
                let n = writer
                    .manifest()
                    .await?
                    .map(|m| m.sstables.len())
                    .unwrap_or(0);
                if n > g {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
    let write_s = t_write.elapsed().as_secs_f64();
    let write_cpu_s = process_cpu_secs() - cpu0;
    let write_rows_per_s = args.rows as f64 / write_s.max(1e-9);
    let rss_after_load_mb = sampler.peak_mb();
    let n_gens = writer
        .manifest()
        .await?
        .map(|m| m.sstables.len())
        .unwrap_or(0);
    println!(
        "[lance] wrote {} rows in {:.2}s = {:.0} rows/s (cpu {:.2}s, sstables={n_gens}+active)",
        args.rows, write_s, write_rows_per_s, write_cpu_s
    );

    // Build the point-lookup planner over base + active MemTable.
    let manifest = writer.manifest().await?;
    let in_memory_refs = writer.in_memory_memtable_refs().await?;
    let mut shard_snapshot = ShardSnapshot::new(shard_id);
    if let Some(ref m) = manifest {
        shard_snapshot = shard_snapshot.with_current_generation(m.current_generation);
        for sstable in &m.sstables {
            shard_snapshot = shard_snapshot.with_sstable(sstable.generation, sstable.path.clone());
        }
    }
    // Keep a handle to the active MemTable for the direct fast path before
    // the collector takes ownership of the refs.
    let active = Arc::new(in_memory_refs.active.clone());
    let collector = LsmDataSourceCollector::new(dataset.clone(), vec![shard_snapshot.clone()])
        .with_in_memory_memtables(shard_id, in_memory_refs);
    // Thread the dataset session + an SSTable cache into the planner, and
    // prewarm every SSTable (open + warm its indexes) up front via
    // the general MemWAL API, so gen-key lookups never re-open a generation per
    // query (the equivalent of RocksDB keeping its DB + SSTs resident). Without
    // this, each plan-path lookup pays a fresh manifest read + Dataset open — a
    // fixed per-lookup cost independent of generation count.
    let sstable_cache = Arc::new(SsTableCache::new((gens as u64).max(1)));
    if args.prewarm {
        dataset
            .prewarm_mem_wal(std::slice::from_ref(&shard_snapshot), Some(&sstable_cache))
            .await?;
    }
    let planner = Arc::new(
        LsmPointLookupPlanner::new(collector, vec![KEY_COL.to_string()], arrow_schema)
            .with_session(dataset.session())
            .with_sstable_cache(sstable_cache),
    );

    // Warmup + correctness: a hit key must resolve to exactly one row under
    // whichever read mode we're timing.
    {
        let probe = insert_order[insert_order.len() / 2];
        let n = match args.lance_read_mode {
            LanceReadMode::Plan => {
                let plan = planner
                    .plan_lookup(&[key_scalar(probe, key_type)], None)
                    .await?;
                let ctx = SessionContext::new();
                let batches: Vec<RecordBatch> =
                    plan.execute(0, ctx.task_ctx())?.try_collect().await?;
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            }
            LanceReadMode::Fast => fast_lookup(&active, probe, key_type)
                .map(|b| b.num_rows())
                .unwrap_or(0),
            LanceReadMode::Api => planner
                .lookup(&[key_scalar(probe, key_type)], None)
                .await?
                .map(|b| b.num_rows())
                .unwrap_or(0),
        };
        assert_eq!(n, 1, "warmup lookup for key {probe} returned {n} rows");
    }

    // --- batch-get path: one vectorized BTree gather per `batch_get` keys ---
    if args.batch_get > 0 {
        let bg = args.batch_get;
        let hit_keys: Vec<i64> = queries
            .iter()
            .filter(|(_, h)| *h)
            .map(|(k, _)| *k)
            .collect();
        // `api` mode exercises the production `lookup_many` (async); `fast`/`plan`
        // use the bench's direct `fast_lookup_batch`. Both gather columnar.
        let use_api = matches!(args.lance_read_mode, LanceReadMode::Api);
        let to_scalars = |chunk: &[i64]| -> Vec<ScalarValue> {
            chunk.iter().map(|k| key_scalar(*k, key_type)).collect()
        };
        let cpu1 = process_cpu_secs();
        let mut latencies_us = Vec::with_capacity(hit_keys.len().div_ceil(bg));
        let mut found_total = 0usize;
        let t = Instant::now();
        for chunk in hit_keys.chunks(bg) {
            let t0 = Instant::now();
            let n = if use_api {
                planner
                    .lookup_many(&to_scalars(chunk), None)
                    .await?
                    .num_rows()
            } else {
                fast_lookup_batch(&active, chunk, key_type).num_rows()
            };
            latencies_us.push(t0.elapsed().as_nanos() as f64 / 1000.0);
            found_total += n;
        }
        let s1 = t.elapsed().as_secs_f64();
        let read_qps_1t = hit_keys.len() as f64 / s1.max(1e-9); // keys/sec
        let read_cpu_s = process_cpu_secs() - cpu1;
        assert_eq!(
            found_total,
            hit_keys.len(),
            "batch get must find all hit keys"
        );

        let read_qps_nt = if args.threads > 1 {
            let keys = Arc::new(hit_keys);
            let nchunks = keys.len().div_ceil(bg);
            let t = Instant::now();
            if use_api {
                let mut handles = Vec::with_capacity(args.threads);
                for shard in 0..args.threads {
                    let keys = keys.clone();
                    let planner = planner.clone();
                    let threads = args.threads;
                    handles.push(tokio::spawn(async move {
                        let mut ci = shard;
                        while ci < nchunks {
                            let lo = ci * bg;
                            let hi = (lo + bg).min(keys.len());
                            let scalars: Vec<ScalarValue> = keys[lo..hi]
                                .iter()
                                .map(|k| key_scalar(*k, key_type))
                                .collect();
                            std::hint::black_box(
                                planner.lookup_many(&scalars, None).await.unwrap(),
                            );
                            ci += threads;
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            } else {
                let mut handles = Vec::with_capacity(args.threads);
                for shard in 0..args.threads {
                    let keys = keys.clone();
                    let active = active.clone();
                    let threads = args.threads;
                    handles.push(std::thread::spawn(move || {
                        let mut ci = shard;
                        while ci < nchunks {
                            let lo = ci * bg;
                            let hi = (lo + bg).min(keys.len());
                            std::hint::black_box(fast_lookup_batch(
                                &active,
                                &keys[lo..hi],
                                key_type,
                            ));
                            ci += threads;
                        }
                    }));
                }
                for h in handles {
                    h.join().unwrap();
                }
            }
            keys.len() as f64 / t.elapsed().as_secs_f64().max(1e-9)
        } else {
            read_qps_1t
        };
        let stats = compute_stats(latencies_us);
        let peak_rss_mb = sampler.stop();
        println!(
            "[lance] batch_get={bg} keys/s_1t={read_qps_1t:.0} keys/s_{}t={read_qps_nt:.0} per_batch p50={:.2}us p99={:.2}us (found={found_total})",
            args.threads, stats.p50_us, stats.p99_us
        );
        drop(writer);
        return Ok(EngineResult {
            engine: "lance-batch",
            write_rows_per_s,
            write_cpu_s,
            read_p50_us: stats.p50_us,
            read_p95_us: stats.p95_us,
            read_p99_us: stats.p99_us,
            read_mean_us: stats.mean_us,
            read_qps_1t,
            read_qps_nt,
            read_cpu_s,
            hits: found_total,
            misses_resolved: 0,
            peak_rss_mb,
            rss_after_load_mb,
        });
    }

    // Cold mode: drop the OS page cache so the on-disk generations are read
    // from NVMe (the active MemTable stays in memory either way).
    if args.cold {
        drop_page_cache();
        println!("[lance] dropped page cache (cold reads from NVMe)");
    }

    // --- read phase: single-thread latency + hit/miss accounting ---
    let cpu1 = process_cpu_secs();
    let ctx = SessionContext::new();
    let task_ctx = ctx.task_ctx();
    let mut latencies_us = Vec::with_capacity(queries.len());
    let mut hits = 0usize;
    let mut misses_resolved = 0usize;
    let t_read = Instant::now();
    for &(key, expect_hit) in queries {
        let t0 = Instant::now();
        let n = match args.lance_read_mode {
            LanceReadMode::Plan => {
                let plan = planner
                    .plan_lookup(&[key_scalar(key, key_type)], None)
                    .await?;
                let batches: Vec<RecordBatch> =
                    plan.execute(0, task_ctx.clone())?.try_collect().await?;
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            }
            LanceReadMode::Fast => fast_lookup(&active, key, key_type)
                .map(|b| b.num_rows())
                .unwrap_or(0),
            LanceReadMode::Api => planner
                .lookup(&[key_scalar(key, key_type)], None)
                .await?
                .map(|b| b.num_rows())
                .unwrap_or(0),
        };
        latencies_us.push(t0.elapsed().as_nanos() as f64 / 1000.0);
        if expect_hit {
            assert_eq!(n, 1, "expected hit for key {key}, got {n}");
            hits += 1;
        } else {
            assert_eq!(n, 0, "expected miss for key {key}, got {n}");
            misses_resolved += 1;
        }
    }
    let read_1t_s = t_read.elapsed().as_secs_f64();
    let read_qps_1t = queries.len() as f64 / read_1t_s.max(1e-9);
    let read_cpu_s = process_cpu_secs() - cpu1;
    let stats = compute_stats(latencies_us);

    // --- read phase: N-thread QPS ---
    let keys: Arc<Vec<i64>> = Arc::new(queries.iter().map(|(k, _)| *k).collect());
    let read_qps_nt = if args.threads <= 1 {
        read_qps_1t
    } else if args.lance_read_mode == LanceReadMode::Fast {
        // Direct path is synchronous; fan out over OS threads like RocksDB.
        let t = Instant::now();
        let mut handles = Vec::with_capacity(args.threads);
        for shard in 0..args.threads {
            let active = active.clone();
            let keys = keys.clone();
            let threads = args.threads;
            handles.push(std::thread::spawn(move || {
                let mut i = shard;
                while i < keys.len() {
                    std::hint::black_box(fast_lookup(&active, keys[i], key_type));
                    i += threads;
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        keys.len() as f64 / t.elapsed().as_secs_f64().max(1e-9)
    } else {
        // Plan and Api are async; fan out over tokio tasks.
        let mode = args.lance_read_mode;
        let t = Instant::now();
        let mut handles = Vec::with_capacity(args.threads);
        for shard in 0..args.threads {
            let planner = planner.clone();
            let keys = keys.clone();
            let threads = args.threads;
            handles.push(tokio::spawn(async move {
                let ctx = SessionContext::new();
                let task_ctx = ctx.task_ctx();
                let mut done = 0usize;
                let mut i = shard;
                while i < keys.len() {
                    match mode {
                        LanceReadMode::Api => {
                            std::hint::black_box(
                                planner
                                    .lookup(&[key_scalar(keys[i], key_type)], None)
                                    .await
                                    .unwrap(),
                            );
                        }
                        _ => {
                            let plan = planner
                                .plan_lookup(&[key_scalar(keys[i], key_type)], None)
                                .await
                                .unwrap();
                            let _b: Vec<RecordBatch> = plan
                                .execute(0, task_ctx.clone())
                                .unwrap()
                                .try_collect()
                                .await
                                .unwrap();
                        }
                    }
                    done += 1;
                    i += threads;
                }
                done
            }));
        }
        let mut total = 0usize;
        for h in handles {
            total += h.await.unwrap();
        }
        let s = t.elapsed().as_secs_f64();
        total as f64 / s.max(1e-9)
    };

    let peak_rss_mb = sampler.stop();
    println!(
        "[lance] read p50={:.2}us p95={:.2}us p99={:.2}us mean={:.2}us qps_1t={:.0} qps_{}t={:.0} (hits={} miss={}) peak_rss={:.0}MB",
        stats.p50_us,
        stats.p95_us,
        stats.p99_us,
        stats.mean_us,
        read_qps_1t,
        args.threads,
        read_qps_nt,
        hits,
        misses_resolved,
        peak_rss_mb
    );

    // Reads are done; release the writer (and, when this function returns, the
    // planner/collector that hold the MemTable Arcs) so Lance memory is freed
    // before any subsequent in-process engine — otherwise `--engine both`
    // would inflate the RocksDB RSS sample. ShardWriter has no blocking Drop.
    drop(writer);

    Ok(EngineResult {
        engine: match args.lance_read_mode {
            LanceReadMode::Plan => "lance",
            LanceReadMode::Fast => "lance-fast",
            LanceReadMode::Api => "lance-api",
        },
        write_rows_per_s,
        write_cpu_s,
        read_p50_us: stats.p50_us,
        read_p95_us: stats.p95_us,
        read_p99_us: stats.p99_us,
        read_mean_us: stats.mean_us,
        read_qps_1t,
        read_qps_nt,
        read_cpu_s,
        hits,
        misses_resolved,
        peak_rss_mb,
        rss_after_load_mb,
    })
}

// ----------------------------------------------------------------------
// Lance SSTable (on-disk) engine
// ----------------------------------------------------------------------

/// Drop the OS page cache so subsequent reads hit storage (cold). Best-effort:
/// needs passwordless sudo (true on the bench box); failures are ignored.
fn drop_page_cache() {
    let _ = std::process::Command::new("sudo")
        .args(["sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"])
        .status();
    std::thread::sleep(Duration::from_millis(300));
}

/// One indexed point lookup via the **DataFusion** path: `scan().filter("id =
/// key")` parses + plans + executes a query per lookup (uses the on-disk BTree
/// index). Returns the matched row count.
async fn sstable_probe(dataset: &Dataset, key: i64) -> Result<usize> {
    use futures::StreamExt;
    let mut scanner = dataset.scan();
    scanner.filter(&format!("{KEY_COL} = {key}"))?;
    let mut stream = scanner.try_into_stream().await?;
    let mut n = 0;
    while let Some(batch) = stream.next().await {
        n += batch?.num_rows();
    }
    Ok(n)
}

/// One point lookup via the **direct** path: search the on-disk BTree scalar
/// index for the row id, then `take` that row — bypassing DataFusion plan
/// construction. Diagnostic for how much of the SSTable read cost is the plan.
async fn sstable_probe_direct(
    dataset: &Dataset,
    scalar_index: &Arc<dyn lance_index::scalar::ScalarIndex>,
    key: i64,
) -> Result<usize> {
    use lance_index::metrics::NoOpMetricsCollector;
    use lance_index::scalar::SargableQuery;
    let query = SargableQuery::Equals(ScalarValue::Int64(Some(key)));
    let result = scalar_index.search(&query, &NoOpMetricsCollector).await?;
    let true_rows = result.row_addrs().true_rows();
    let Some(rid) = true_rows
        .row_addrs()
        .and_then(|mut it| it.next())
        .map(u64::from)
    else {
        return Ok(0);
    };
    let batch = dataset.take_rows(&[rid], dataset.schema().clone()).await?;
    Ok(batch.num_rows())
}

/// Dispatch an SSTable point lookup: `direct` = on-disk BTree index search + take
/// (no DataFusion); otherwise the DataFusion `scan().filter()` path.
async fn sstable_lookup(
    dataset: &Dataset,
    scalar_index: &Arc<dyn lance_index::scalar::ScalarIndex>,
    key: i64,
    direct: bool,
) -> Result<usize> {
    if direct {
        sstable_probe_direct(dataset, scalar_index, key).await
    } else {
        sstable_probe(dataset, key).await
    }
}

/// Batched SSTable lookup over a chunk of keys: `direct` searches the index for
/// each key then issues one `take_rows` for all; otherwise one DataFusion scan
/// with `id IN (...)`. Returns the total matched row count.
async fn sstable_batch(
    dataset: &Dataset,
    scalar_index: &Arc<dyn lance_index::scalar::ScalarIndex>,
    keys: &[i64],
    direct: bool,
) -> Result<usize> {
    if direct {
        use lance_index::metrics::NoOpMetricsCollector;
        use lance_index::scalar::SargableQuery;
        let mut rids = Vec::with_capacity(keys.len());
        for &k in keys {
            let q = SargableQuery::Equals(ScalarValue::Int64(Some(k)));
            let r = scalar_index.search(&q, &NoOpMetricsCollector).await?;
            if let Some(rid) = r
                .row_addrs()
                .true_rows()
                .row_addrs()
                .and_then(|mut it| it.next())
                .map(u64::from)
            {
                rids.push(rid);
            }
        }
        if rids.is_empty() {
            return Ok(0);
        }
        let batch = dataset.take_rows(&rids, dataset.schema().clone()).await?;
        Ok(batch.num_rows())
    } else {
        use futures::StreamExt;
        let list = keys
            .iter()
            .map(|k| k.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let mut scanner = dataset.scan();
        scanner.filter(&format!("{KEY_COL} IN ({list})"))?;
        let mut stream = scanner.try_into_stream().await?;
        let mut n = 0;
        while let Some(b) = stream.next().await {
            n += b?.num_rows();
        }
        Ok(n)
    }
}

/// SSTable Lance: write all rows as one on-disk Lance dataset with a BTree
/// scalar index — the exact artifact a MemTable flush emits (forward-written
/// data file + on-disk BTree index) — then point-lookup through the indexed
/// scan path. Int keys only (the SQL filter literal is the integer).
async fn run_lance_sstable(
    args: &Args,
    insert_order: &[i64],
    queries: &[(i64, bool)],
) -> Result<EngineResult> {
    assert_eq!(
        args.key_type,
        KeyType::Int,
        "sstable mode currently supports --key-type int only"
    );
    let sampler = RssSampler::start();
    let key_type = args.key_type;
    let schema = make_schema(key_type);
    let uri = format!("{}/lance_sstable", args.uri.trim_end_matches('/'));
    let _ = std::fs::remove_dir_all(&uri);

    // --- write + flush: build the on-disk data file + BTree index ---
    let cpu0 = process_cpu_secs();
    let t_write = Instant::now();
    let batches: Vec<RecordBatch> = insert_order
        .chunks(args.batch_rows)
        .map(|c| make_batch(schema.clone(), c, args.value_size, key_type))
        .collect();
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let mut dataset = Dataset::write(reader, &uri, Some(WriteParams::default())).await?;
    dataset
        .create_index(
            &[KEY_COL],
            IndexType::BTree,
            Some(BTREE_INDEX_NAME.to_string()),
            &ScalarIndexParams::default(),
            true,
        )
        .await?;
    let write_s = t_write.elapsed().as_secs_f64();
    let write_cpu_s = process_cpu_secs() - cpu0;
    let write_rows_per_s = args.rows as f64 / write_s.max(1e-9);
    let rss_after_load_mb = sampler.peak_mb();
    println!(
        "[lance] wrote+flushed {} rows + btree index in {:.2}s = {:.0} rows/s (cpu {:.2}s)",
        args.rows, write_s, write_rows_per_s, write_cpu_s
    );
    let dataset = Arc::new(dataset);

    // `plan` = DataFusion scan().filter(); `fast`/`api` = direct BTree index
    // search + take (no DataFusion). Open the on-disk index once for the latter.
    let direct = args.lance_read_mode != LanceReadMode::Plan;
    let scalar_index: Arc<dyn lance_index::scalar::ScalarIndex> = {
        use lance_index::metrics::NoOpMetricsCollector;
        let indices = dataset.load_indices().await?;
        let uuid = indices
            .iter()
            .find(|i| i.name == BTREE_INDEX_NAME)
            .map(|i| i.uuid.to_string())
            .ok_or_else(|| lance_core::Error::internal("sstable: btree index not found"))?;
        dataset
            .open_scalar_index(KEY_COL, &uuid, &NoOpMetricsCollector)
            .await?
    };
    println!(
        "[lance] sstable read path = {}",
        if direct {
            "direct btree-index search + take"
        } else {
            "datafusion scan().filter()"
        }
    );

    // warmup + correctness: a hit resolves to exactly one row via the index.
    if let Some((probe, _)) = queries.iter().find(|(_, h)| *h) {
        let n = sstable_lookup(&dataset, &scalar_index, *probe, direct).await?;
        assert_eq!(n, 1, "sstable warmup lookup for key {probe} returned {n}");
    }

    // Cold mode: drop the OS page cache so reads hit NVMe (data > RAM assumed).
    if args.cold {
        drop_page_cache();
        println!("[lance] dropped page cache (cold reads from NVMe)");
    }

    // --- batch-get path: gather `batch_get` keys per call from the SSTable ---
    if args.batch_get > 0 {
        let bg = args.batch_get;
        let hit_keys: Vec<i64> = queries
            .iter()
            .filter(|(_, h)| *h)
            .map(|(k, _)| *k)
            .collect();
        let cpu1 = process_cpu_secs();
        let mut latencies_us = Vec::with_capacity(hit_keys.len().div_ceil(bg));
        let mut found_total = 0usize;
        let t = Instant::now();
        for chunk in hit_keys.chunks(bg) {
            let t0 = Instant::now();
            found_total += sstable_batch(&dataset, &scalar_index, chunk, direct).await?;
            latencies_us.push(t0.elapsed().as_nanos() as f64 / 1000.0);
        }
        let read_qps_1t = hit_keys.len() as f64 / t.elapsed().as_secs_f64().max(1e-9);
        let read_cpu_s = process_cpu_secs() - cpu1;
        let keys_arc: Arc<Vec<i64>> = Arc::new(hit_keys.clone());
        let read_qps_nt = if args.threads <= 1 {
            read_qps_1t
        } else {
            let t = Instant::now();
            let mut handles = Vec::with_capacity(args.threads);
            for shard in 0..args.threads {
                let dataset = dataset.clone();
                let si = scalar_index.clone();
                let keys = keys_arc.clone();
                let threads = args.threads;
                handles.push(tokio::spawn(async move {
                    let chunks: Vec<&[i64]> = keys.chunks(bg).collect();
                    let mut i = shard;
                    while i < chunks.len() {
                        let _ = sstable_batch(&dataset, &si, chunks[i], direct).await;
                        i += threads;
                    }
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
            hit_keys.len() as f64 / t.elapsed().as_secs_f64().max(1e-9)
        };
        let stats = compute_stats(latencies_us);
        let peak_rss_mb = sampler.stop();
        println!(
            "[lance] batch_get={bg} keys/s_1t={read_qps_1t:.0} keys/s_{}t={read_qps_nt:.0} per_batch p50={:.2}us p99={:.2}us (found={found_total})",
            args.threads, stats.p50_us, stats.p99_us
        );
        return Ok(EngineResult {
            engine: "lance-sstable-batch",
            write_rows_per_s,
            write_cpu_s,
            read_p50_us: stats.p50_us,
            read_p95_us: stats.p95_us,
            read_p99_us: stats.p99_us,
            read_mean_us: stats.mean_us,
            read_qps_1t,
            read_qps_nt,
            read_cpu_s,
            hits: found_total,
            misses_resolved: 0,
            peak_rss_mb,
            rss_after_load_mb,
        });
    }

    // --- single-thread read latency ---
    let cpu1 = process_cpu_secs();
    let mut latencies_us = Vec::with_capacity(queries.len());
    let mut hits = 0usize;
    let mut misses_resolved = 0usize;
    let t_read = Instant::now();
    for &(key, expect_hit) in queries {
        let t0 = Instant::now();
        let n = sstable_lookup(&dataset, &scalar_index, key, direct).await?;
        latencies_us.push(t0.elapsed().as_nanos() as f64 / 1000.0);
        if expect_hit {
            assert_eq!(n, 1, "expected hit for key {key}, got {n}");
            hits += 1;
        } else {
            assert_eq!(n, 0, "expected miss for key {key}, got {n}");
            misses_resolved += 1;
        }
    }
    let read_1t_s = t_read.elapsed().as_secs_f64();
    let read_qps_1t = queries.len() as f64 / read_1t_s.max(1e-9);
    let read_cpu_s = process_cpu_secs() - cpu1;
    let stats = compute_stats(latencies_us);

    // --- N-thread QPS (tokio tasks, shared Arc<Dataset>) ---
    let keys: Arc<Vec<i64>> = Arc::new(queries.iter().map(|(k, _)| *k).collect());
    let read_qps_nt = if args.threads <= 1 {
        read_qps_1t
    } else {
        let t = Instant::now();
        let mut handles = Vec::with_capacity(args.threads);
        for shard in 0..args.threads {
            let dataset = dataset.clone();
            let scalar_index = scalar_index.clone();
            let keys = keys.clone();
            let threads = args.threads;
            handles.push(tokio::spawn(async move {
                let mut i = shard;
                while i < keys.len() {
                    let _ = sstable_lookup(&dataset, &scalar_index, keys[i], direct).await;
                    i += threads;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        queries.len() as f64 / t.elapsed().as_secs_f64().max(1e-9)
    };

    let peak_rss_mb = sampler.stop();
    println!(
        "[lance] read p50={:.2}us p95={:.2}us p99={:.2}us mean={:.2}us qps_1t={:.0} qps_{}t={:.0} (hits={hits} miss={misses_resolved}) peak_rss={:.0}MB",
        stats.p50_us,
        stats.p95_us,
        stats.p99_us,
        stats.mean_us,
        read_qps_1t,
        args.threads,
        read_qps_nt,
        peak_rss_mb
    );

    Ok(EngineResult {
        engine: "lance-sstable",
        write_rows_per_s,
        write_cpu_s,
        read_p50_us: stats.p50_us,
        read_p95_us: stats.p95_us,
        read_p99_us: stats.p99_us,
        read_mean_us: stats.mean_us,
        read_qps_1t,
        read_qps_nt,
        read_cpu_s,
        hits,
        misses_resolved,
        peak_rss_mb,
        rss_after_load_mb,
    })
}

// ----------------------------------------------------------------------
// RocksDB engine (only with --features bench-rocksdb)
// ----------------------------------------------------------------------

#[cfg(feature = "bench-rocksdb")]
fn run_rocksdb(args: &Args, insert_order: &[i64], queries: &[(i64, bool)]) -> Result<EngineResult> {
    use rocksdb::{DB, Options, WriteBatch, WriteOptions};

    let sampler = RssSampler::start();
    let key_type = args.key_type;
    let db_path = format!("{}/rocksdb", args.uri.trim_end_matches('/'));
    let _ = std::fs::remove_dir_all(&db_path);
    // RocksDB's create_if_missing creates the DB dir but not missing parents;
    // for `--engine rocksdb` (no Lance arm to create `--uri`) make it ourselves.
    std::fs::create_dir_all(&db_path)
        .map_err(|e| lance_core::Error::io(format!("mkdir {db_path}: {e}")))?;

    // Write-buffer sizing. Default (warm): above the whole dataset so one
    // memtable holds every row. Cold: cap at 512MB so a larger-than-RAM dataset
    // flushes to SSTs during write instead of OOMing the memtable.
    let write_buf = if args.cold {
        512usize << 20
    } else {
        args.rows * (args.value_size + 200) + (64 << 20)
    };
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(write_buf);
    opts.set_max_write_buffer_number(4);
    opts.set_min_write_buffer_number_to_merge(2);
    opts.set_disable_auto_compactions(true);
    opts.set_db_write_buffer_size(write_buf);
    // Block cache for the `--storage sstable` SST reads. Warm: large enough to
    // hold the SST index/filter + hot data blocks. Cold: small (128MB) so data
    // blocks miss the cache and reads go to NVMe (index/filter stay in memory).
    {
        let mut bbt = rocksdb::BlockBasedOptions::default();
        bbt.set_bloom_filter(10.0, false);
        let cache_bytes = if args.cold {
            128usize << 20
        } else {
            (1usize << 30).max(write_buf / 2)
        };
        let cache = rocksdb::Cache::new_lru_cache(cache_bytes);
        bbt.set_block_cache(&cache);
        opts.set_block_based_table_factory(&bbt);
    }

    let db = Arc::new(
        DB::open(&opts, &db_path)
            .map_err(|e| lance_core::Error::io(format!("rocksdb open: {e}")))?,
    );

    let mut wo = WriteOptions::default();
    // Default: WAL on (durable), matching Lance's durable_write=true path.
    // `--rocksdb-disable-wal` opts into RocksDB's faster no-WAL writes.
    wo.disable_wal(args.rocksdb_disable_wal);

    // --- write phase ---
    // LSM: split into `generations+1` parts; flush each of the first
    // `generations` parts to its own SST (compaction off → they stay separate
    // L0 SSTs); the last part stays in the active memtable.
    let gens = args.generations;
    let part = (insert_order.len() / (gens + 1)).max(1);
    let cpu0 = process_cpu_secs();
    let t_write = Instant::now();
    let mut lo = 0usize;
    for g in 0..=gens {
        let part_end = if g < gens {
            ((g + 1) * part).min(insert_order.len())
        } else {
            insert_order.len()
        };
        while lo < part_end {
            let hi = (lo + args.batch_rows).min(part_end);
            let mut wb = WriteBatch::default();
            for &k in &insert_order[lo..hi] {
                wb.put(rocks_key(k, key_type), make_value(k, args.value_size));
            }
            db.write_opt(wb, &wo)
                .map_err(|e| lance_core::Error::io(format!("rocksdb write: {e}")))?;
            lo = hi;
        }
        if g < gens {
            db.flush()
                .map_err(|e| lance_core::Error::io(format!("rocksdb flush gen: {e}")))?;
        }
    }
    let write_s = t_write.elapsed().as_secs_f64();
    let write_cpu_s = process_cpu_secs() - cpu0;
    let write_rows_per_s = args.rows as f64 / write_s.max(1e-9);
    let rss_after_load_mb = sampler.peak_mb();
    println!(
        "[rocksdb] wrote {} rows in {:.2}s = {:.0} rows/s (cpu {:.2}s, write_buf {}MB)",
        args.rows,
        write_s,
        write_rows_per_s,
        write_cpu_s,
        write_buf >> 20
    );

    // Single-tier `--storage sstable` (no extra generations): flush the active
    // to one SST (compact to one if cold). With generations>0 the per-chunk
    // flushes already produced N separate L0 SSTs + the active memtable.
    if gens == 0 && args.storage == Storage::SsTable {
        db.flush()
            .map_err(|e| lance_core::Error::io(format!("rocksdb flush: {e}")))?;
        if args.cold {
            db.compact_range::<&[u8], &[u8]>(None, None);
        }
    }
    if args.storage == Storage::SsTable || gens > 0 {
        let n_sst = db
            .property_int_value("rocksdb.num-files-at-level0")
            .ok()
            .flatten()
            .unwrap_or(0);
        println!("[rocksdb] {n_sst} L0 SSTs + active memtable");
    }

    // Cold mode: drop the OS page cache so reads hit NVMe.
    if args.cold {
        drop_page_cache();
        println!("[rocksdb] dropped page cache (cold reads from NVMe)");
    }

    // --- batch-get path: RocksDB multi_get of `batch_get` keys per call ---
    if args.batch_get > 0 {
        let bg = args.batch_get;
        let hit_keys: Vec<i64> = queries
            .iter()
            .filter(|(_, h)| *h)
            .map(|(k, _)| *k)
            .collect();
        let multiget = |db: &DB, chunk: &[i64]| -> usize {
            db.multi_get(chunk.iter().map(|k| rocks_key(*k, key_type)))
                .into_iter()
                .filter(|r| matches!(r, Ok(Some(_))))
                .count()
        };
        let cpu1 = process_cpu_secs();
        let mut latencies_us = Vec::with_capacity(hit_keys.len().div_ceil(bg));
        let mut found_total = 0usize;
        let t = Instant::now();
        for chunk in hit_keys.chunks(bg) {
            let t0 = Instant::now();
            found_total += multiget(&db, chunk);
            latencies_us.push(t0.elapsed().as_nanos() as f64 / 1000.0);
        }
        let s1 = t.elapsed().as_secs_f64();
        let read_qps_1t = hit_keys.len() as f64 / s1.max(1e-9);
        let read_cpu_s = process_cpu_secs() - cpu1;
        assert_eq!(
            found_total,
            hit_keys.len(),
            "multi_get must find all hit keys"
        );

        let read_qps_nt = if args.threads > 1 {
            let keys = Arc::new(hit_keys);
            let nchunks = keys.len().div_ceil(bg);
            let t = Instant::now();
            let mut handles = Vec::with_capacity(args.threads);
            for shard in 0..args.threads {
                let keys = keys.clone();
                let db = db.clone();
                let threads = args.threads;
                handles.push(std::thread::spawn(move || {
                    let mut ci = shard;
                    while ci < nchunks {
                        let lo = ci * bg;
                        let hi = (lo + bg).min(keys.len());
                        std::hint::black_box(
                            db.multi_get(keys[lo..hi].iter().map(|k| rocks_key(*k, key_type))),
                        );
                        ci += threads;
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
            keys.len() as f64 / t.elapsed().as_secs_f64().max(1e-9)
        } else {
            read_qps_1t
        };
        let stats = compute_stats(latencies_us);
        let peak_rss_mb = sampler.stop();
        println!(
            "[rocksdb] batch_get={bg} keys/s_1t={read_qps_1t:.0} keys/s_{}t={read_qps_nt:.0} per_batch p50={:.2}us p99={:.2}us (found={found_total})",
            args.threads, stats.p50_us, stats.p99_us
        );
        drop(db);
        let _ = std::fs::remove_dir_all(&db_path);
        return Ok(EngineResult {
            engine: "rocksdb",
            write_rows_per_s,
            write_cpu_s,
            read_p50_us: stats.p50_us,
            read_p95_us: stats.p95_us,
            read_p99_us: stats.p99_us,
            read_mean_us: stats.mean_us,
            read_qps_1t,
            read_qps_nt,
            read_cpu_s,
            hits: found_total,
            misses_resolved: 0,
            peak_rss_mb,
            rss_after_load_mb,
        });
    }

    // --- read phase: single-thread latency ---
    let cpu1 = process_cpu_secs();
    let mut latencies_us = Vec::with_capacity(queries.len());
    let mut hits = 0usize;
    let mut misses_resolved = 0usize;
    let t_read = Instant::now();
    for &(key, expect_hit) in queries {
        let t0 = Instant::now();
        let got = db
            .get(rocks_key(key, key_type))
            .map_err(|e| lance_core::Error::io(format!("rocksdb get: {e}")))?;
        latencies_us.push(t0.elapsed().as_nanos() as f64 / 1000.0);
        if expect_hit {
            assert!(got.is_some(), "expected hit for key {key}");
            hits += 1;
        } else {
            assert!(got.is_none(), "expected miss for key {key}");
            misses_resolved += 1;
        }
    }
    let read_1t_s = t_read.elapsed().as_secs_f64();
    let read_qps_1t = queries.len() as f64 / read_1t_s.max(1e-9);
    let read_cpu_s = process_cpu_secs() - cpu1;
    let stats = compute_stats(latencies_us);

    // --- read phase: N-thread QPS ---
    let read_qps_nt = if args.threads > 1 {
        let keys: Arc<Vec<i64>> = Arc::new(queries.iter().map(|(k, _)| *k).collect());
        let t = Instant::now();
        let mut handles = Vec::with_capacity(args.threads);
        for shard in 0..args.threads {
            let db = db.clone();
            let keys = keys.clone();
            let threads = args.threads;
            handles.push(std::thread::spawn(move || {
                let mut i = shard;
                while i < keys.len() {
                    let _ = db.get(rocks_key(keys[i], key_type)).unwrap();
                    i += threads;
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let s = t.elapsed().as_secs_f64();
        keys.len() as f64 / s.max(1e-9)
    } else {
        read_qps_1t
    };

    let peak_rss_mb = sampler.stop();
    println!(
        "[rocksdb] read p50={:.2}us p95={:.2}us p99={:.2}us mean={:.2}us qps_1t={:.0} qps_{}t={:.0} (hits={} miss={}) peak_rss={:.0}MB",
        stats.p50_us,
        stats.p95_us,
        stats.p99_us,
        stats.mean_us,
        read_qps_1t,
        args.threads,
        read_qps_nt,
        hits,
        misses_resolved,
        peak_rss_mb
    );

    drop(db);
    let _ = std::fs::remove_dir_all(&db_path);

    Ok(EngineResult {
        engine: "rocksdb",
        write_rows_per_s,
        write_cpu_s,
        read_p50_us: stats.p50_us,
        read_p95_us: stats.p95_us,
        read_p99_us: stats.p99_us,
        read_mean_us: stats.mean_us,
        read_qps_1t,
        read_qps_nt,
        read_cpu_s,
        hits,
        misses_resolved,
        peak_rss_mb,
        rss_after_load_mb,
    })
}

#[cfg(not(feature = "bench-rocksdb"))]
fn run_rocksdb(
    _args: &Args,
    _insert_order: &[i64],
    _queries: &[(i64, bool)],
) -> Result<EngineResult> {
    Err(lance_core::Error::invalid_input(
        "RocksDB arm not compiled; rebuild with --features bench-rocksdb",
    ))
}

// ----------------------------------------------------------------------
// Entrypoint
// ----------------------------------------------------------------------

fn print_comparison(results: &[EngineResult]) {
    println!("\n=== comparison ===");
    println!(
        "{:>9} {:>14} {:>12} {:>12} {:>12} {:>12} {:>12} {:>11} {:>11}",
        "engine",
        "write_rows/s",
        "rd_p50_us",
        "rd_p95_us",
        "rd_p99_us",
        "qps_1t",
        "qps_nt",
        "rss_mb",
        "rd_cpu_s"
    );
    for r in results {
        println!(
            "{:>9} {:>14.0} {:>12.2} {:>12.2} {:>12.2} {:>12.0} {:>12.0} {:>11.0} {:>11.3}",
            r.engine,
            r.write_rows_per_s,
            r.read_p50_us,
            r.read_p95_us,
            r.read_p99_us,
            r.read_qps_1t,
            r.read_qps_nt,
            r.peak_rss_mb,
            r.read_cpu_s,
        );
    }
    // Ratios when both ran.
    if let (Some(l), Some(rdb)) = (
        results.iter().find(|r| r.engine.starts_with("lance")),
        results.iter().find(|r| r.engine == "rocksdb"),
    ) {
        let safe = |a: f64, b: f64| if b > 0.0 { a / b } else { f64::NAN };
        println!(
            "\nlance/rocksdb ratios: write={:.2}x  read_p50={:.2}x  qps_1t={:.2}x  qps_nt={:.2}x  rss={:.2}x",
            safe(l.write_rows_per_s, rdb.write_rows_per_s),
            safe(l.read_p50_us, rdb.read_p50_us),
            safe(l.read_qps_1t, rdb.read_qps_1t),
            safe(l.read_qps_nt, rdb.read_qps_nt),
            safe(l.peak_rss_mb, rdb.rss_after_load_mb.max(rdb.peak_rss_mb)),
        );
        println!("(write/qps >1 = lance faster; read_p50/rss <1 = lance better)");
    }
}

async fn run(args: Args) -> Result<()> {
    println!(
        "bench=mem_wal_kv_point_lookup engine={:?} storage={} generations={} prewarm={} key_type={} lance_read_mode={} batch_get={} rows={} value_size={} queries={} miss_ratio={} threads={} batch_rows={} uri={}",
        args.engine,
        args.storage.as_str(),
        args.generations,
        args.prewarm,
        args.key_type.as_str(),
        args.lance_read_mode.as_str(),
        args.batch_get,
        args.rows,
        args.value_size,
        args.queries,
        args.miss_ratio,
        args.threads,
        args.batch_rows,
        args.uri
    );

    let insert_order = shuffled_keys(args.rows, args.seed);
    let queries = build_queries(args.rows, args.queries, args.miss_ratio, args.seed);

    let mut results = Vec::new();
    if matches!(args.engine, Engine::Lance | Engine::Both) {
        let res = match args.storage {
            Storage::Active => run_lance(&args, &insert_order, &queries).await?,
            Storage::SsTable => run_lance_sstable(&args, &insert_order, &queries).await?,
        };
        results.push(res);
    }
    if matches!(args.engine, Engine::Rocksdb | Engine::Both) {
        // RocksDB arm is synchronous; run it on a blocking thread so it does
        // not stall the tokio reactor.
        let a = args.clone();
        let io = insert_order.clone();
        let q = queries.clone();
        let res = tokio::task::spawn_blocking(move || run_rocksdb(&a, &io, &q))
            .await
            .map_err(|e| lance_core::Error::io(format!("rocksdb join: {e}")))??;
        results.push(res);
    }

    print_comparison(&results);

    let out = json!({
        "bench": "mem_wal_kv_point_lookup",
        "rows": args.rows,
        "value_size": args.value_size,
        "queries": args.queries,
        "miss_ratio": args.miss_ratio,
        "threads": args.threads,
        "batch_get": args.batch_get,
        "results": results.iter().map(|r| r.to_json(&args)).collect::<Vec<_>>(),
    });
    let text = serde_json::to_string_pretty(&out)
        .map_err(|e| lance_core::Error::io(format!("serialize: {e}")))?;
    if let Some(path) = &args.output {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(path, text.as_bytes())
            .map_err(|e| lance_core::Error::io(format!("write {}: {e}", path.display())))?;
    }
    println!("\n{text}");
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
