// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#![allow(clippy::print_stdout)]
#![recursion_limit = "256"]

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::hint::black_box;
use std::ops::Range;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use futures::future::BoxFuture;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::{FutureExt, StreamExt, TryStreamExt};
use lance::dataset::ProjectionRequest;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::fragment::{FileFragment, FragReadConfig};
use lance::dataset::scanner::{ExecutionStatsCallback, ExecutionSummaryCounts};
use lance_core::datatypes::Schema;
use lance_encoding::decoder::PageEncoding;
use lance_encoding::format::pb21;
use lance_file::reader::{
    DEFAULT_READ_CHUNK_SIZE, FileReader as LanceFileReader, FileReaderOptions,
};
use lance_io::object_store::ObjectStore as LanceObjectStore;
use lance_io::scheduler::{FileScheduler, ScanScheduler, ScanStats, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use serde_json::{Value, json};
use tracing::field::{Field, Visit};
use tracing::subscriber::Interest;
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

const GIB: u64 = 1024 * 1024 * 1024;
const SCHEDULER_STATE_EVENT_TARGET: &str = "lance_io::scheduler::state";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SchedulerQueueKind {
    Standard,
    Lite,
}

#[derive(Debug, Clone, Copy)]
struct SchedulerDiagnostics {
    kind: SchedulerQueueKind,
    stats: ScanStats,
    io_capacity: u64,
    iops_available: u64,
    active_iops: u64,
    pending_iops: u64,
    pending_bytes: u64,
    bytes_available: i64,
    bytes_reserved: i64,
    io_buffer_size_bytes: u64,
    priorities_in_flight: u64,
    no_backpressure: bool,
    head_task_bytes: Option<u64>,
    head_task_priority_high: Option<u64>,
    head_task_priority_low: Option<u64>,
    min_in_flight_priority_high: Option<u64>,
    min_in_flight_priority_low: Option<u64>,
    head_task_can_deliver: Option<bool>,
    head_task_priority_bypass: Option<bool>,
    head_task_blocked_by_iops: Option<bool>,
    head_task_blocked_by_bytes: Option<bool>,
}

#[derive(Debug, Clone)]
struct Config {
    backend: Backend,
    uri: String,
    dataset_version: u64,
    columns: Option<Vec<String>>,
    limit_rows: u64,
    target_bytes: Option<u64>,
    raw_range_size_bytes: u64,
    raw_range_mode: RawRangeMode,
    raw_column_indices: Option<Vec<u32>>,
    raw_submit_mode: RawSubmitMode,
    raw_completion_mode: RawCompletionMode,
    take_repetitions: u64,
    io_buffer_gib: Vec<Option<u64>>,
    batch_size: u32,
    batch_size_bytes: Option<u64>,
    skip_batch_byte_accounting: bool,
    read_chunk_size: Option<u64>,
    fragment_concurrency: usize,
    batch_concurrency: usize,
    sample_ms: u64,
    out_dir: String,
    case_name: String,
    describe_layout: bool,
    detach_fragment_streams: bool,
    drop_read_tasks: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Backend {
    FileReader,
    Scanner,
    SchedulerRaw,
    DatasetTake,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RawSubmitMode {
    Single,
    SplitNoConcat,
    SplitConcat,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RawRangeMode {
    FileSequential,
    MetadataPages,
    MetadataPagesRoundRobin,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RawCompletionMode {
    Unordered,
    Ordered,
}

impl RawRangeMode {
    fn name(self) -> &'static str {
        match self {
            Self::FileSequential => "file-sequential",
            Self::MetadataPages => "metadata-pages",
            Self::MetadataPagesRoundRobin => "metadata-pages-round-robin",
        }
    }
}

impl RawSubmitMode {
    fn name(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::SplitNoConcat => "split-no-concat",
            Self::SplitConcat => "split-concat",
        }
    }
}

impl RawCompletionMode {
    fn name(self) -> &'static str {
        match self {
            Self::Unordered => "unordered",
            Self::Ordered => "ordered",
        }
    }
}

impl Backend {
    fn name(self) -> &'static str {
        match self {
            Self::FileReader => "lance-file-reader",
            Self::Scanner => "lance-scanner",
            Self::SchedulerRaw => "lance-scheduler-raw",
            Self::DatasetTake => "lance-dataset-take",
        }
    }

    fn layer(self) -> &'static str {
        match self {
            Self::FileReader => "file-reader",
            Self::Scanner => "scanner",
            Self::SchedulerRaw => "scheduler",
            Self::DatasetTake => "dataset-take",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CpuSample {
    idle: u64,
    total: u64,
}

#[derive(Debug, Default)]
struct SharedCounters {
    fragments_started: AtomicU64,
    fragments_completed: AtomicU64,
    batch_futures_emitted: AtomicU64,
    batch_futures_received: AtomicU64,
    batches_completed: AtomicU64,
    rows_completed: AtomicU64,
    arrow_bytes: AtomicU64,
    open_reader_ns: AtomicU64,
    read_stream_create_ns: AtomicU64,
    next_batch_poll_ns: AtomicU64,
    channel_send_wait_ns: AtomicU64,
    decode_ns: AtomicU64,
    raw_reassemble_ns: AtomicU64,
}

#[derive(Debug)]
struct CaseStats {
    rows: u64,
    batches: u64,
    arrow_bytes: u64,
    planned_fragments: usize,
    planned_rows: u64,
    elapsed: Duration,
    producer_finished_at: Option<Duration>,
    peak_decode_in_flight: usize,
    cpu_avg: Option<f64>,
    scheduler_diagnostics: SchedulerDiagnostics,
    counters: Arc<SharedCounters>,
    samples: Vec<Value>,
}

#[derive(Debug)]
struct LastSample {
    elapsed: Duration,
    scheduler_stats: ScanStats,
    rows: u64,
    batches: u64,
    arrow_bytes: u64,
}

fn usage() -> &'static str {
    "usage: s3_file_reader_diagnostics --uri <s3-dataset-uri> \
     [--backend <file-reader|scanner|scheduler-raw>] \
     [--dataset-version <n>] [--columns <all|col[,col...]>] \
     [--limit-rows <n>] [--target-bytes <n>] [--raw-range-size-bytes <n>] \
     [--raw-range-mode <file-sequential|metadata-pages|metadata-pages-round-robin>] \
     [--raw-column-indices <all|n[,n...]>] \
     [--raw-submit-mode <single|split-no-concat|split-concat>] \
     [--raw-completion-mode <unordered|ordered>] \
     [--take-repetitions <n>] \
     [--io-buffer-gib <auto|n[,n...]>] \
     [--batch-size <n>] [--batch-size-bytes <n>] [--read-chunk-size <n>] \
     [--skip-batch-byte-accounting] \
     [--fragment-concurrency <n>] [--batch-concurrency <n>] \
     [--sample-ms <n>] [--out-dir <path>] [--case <name>] \
     [--detach-fragment-streams] [--drop-read-tasks] [--describe-layout]"
}

fn parse_args() -> Result<Config> {
    let mut backend = Backend::FileReader;
    let mut uri = None;
    let mut dataset_version = 1u64;
    let mut columns = Some(vec!["vector".to_string()]);
    let mut limit_rows = 67_108_864u64;
    let mut target_bytes = None;
    let mut raw_range_size_bytes = 16 * 1024 * 1024;
    let mut raw_range_mode = RawRangeMode::FileSequential;
    let mut raw_column_indices = None;
    let mut raw_submit_mode = RawSubmitMode::Single;
    let mut raw_completion_mode = RawCompletionMode::Unordered;
    let mut take_repetitions = 100u64;
    let mut io_buffer_gib = vec![Some(8)];
    let mut batch_size = 8192u32;
    let mut batch_size_bytes = None;
    let mut skip_batch_byte_accounting = false;
    let mut read_chunk_size = None;
    let mut fragment_concurrency = 256usize;
    let mut batch_concurrency = 256usize;
    let mut sample_ms = 1000u64;
    let mut out_dir = "/tmp/lance-s3-bottleneck-results".to_string();
    let mut case_name = "lance-file-reader-diagnostics".to_string();
    let mut describe_layout = false;
    let mut detach_fragment_streams = false;
    let mut drop_read_tasks = false;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--backend" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("missing value for --backend. {}", usage()))?;
                backend = parse_backend(&value)?;
            }
            "--uri" => uri = args.next(),
            "--dataset-version" => {
                dataset_version = parse_required_value(&mut args, "--dataset-version")?;
            }
            "--columns" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("missing value for --columns. {}", usage()))?;
                columns = parse_columns(&value)?;
            }
            "--limit-rows" => {
                limit_rows = parse_required_value(&mut args, "--limit-rows")?;
            }
            "--target-bytes" => {
                target_bytes = Some(parse_required_value(&mut args, "--target-bytes")?);
            }
            "--raw-range-size-bytes" => {
                raw_range_size_bytes = parse_required_value(&mut args, "--raw-range-size-bytes")?;
            }
            "--raw-range-mode" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("missing value for --raw-range-mode. {}", usage()))?;
                raw_range_mode = parse_raw_range_mode(&value)?;
            }
            "--raw-column-indices" => {
                let value = args.next().ok_or_else(|| {
                    format!("missing value for --raw-column-indices. {}", usage())
                })?;
                raw_column_indices = parse_raw_column_indices(&value)?;
            }
            "--raw-submit-mode" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("missing value for --raw-submit-mode. {}", usage()))?;
                raw_submit_mode = parse_raw_submit_mode(&value)?;
            }
            "--raw-completion-mode" => {
                let value = args.next().ok_or_else(|| {
                    format!("missing value for --raw-completion-mode. {}", usage())
                })?;
                raw_completion_mode = parse_raw_completion_mode(&value)?;
            }
            "--take-repetitions" => {
                take_repetitions = parse_required_value(&mut args, "--take-repetitions")?;
            }
            "--io-buffer-gib" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("missing value for --io-buffer-gib. {}", usage()))?;
                io_buffer_gib = parse_io_buffer_gib(&value)?;
            }
            "--batch-size" => {
                batch_size = parse_required_value(&mut args, "--batch-size")?;
            }
            "--batch-size-bytes" => {
                batch_size_bytes = Some(parse_required_value(&mut args, "--batch-size-bytes")?);
            }
            "--skip-batch-byte-accounting" => {
                skip_batch_byte_accounting = true;
            }
            "--read-chunk-size" => {
                read_chunk_size = Some(parse_required_value(&mut args, "--read-chunk-size")?);
            }
            "--fragment-concurrency" => {
                fragment_concurrency = parse_required_value(&mut args, "--fragment-concurrency")?;
            }
            "--batch-concurrency" => {
                batch_concurrency = parse_required_value(&mut args, "--batch-concurrency")?;
            }
            "--sample-ms" => {
                sample_ms = parse_required_value(&mut args, "--sample-ms")?;
            }
            "--out-dir" => {
                out_dir = args
                    .next()
                    .ok_or_else(|| format!("missing value for --out-dir. {}", usage()))?;
            }
            "--case" => {
                case_name = args
                    .next()
                    .ok_or_else(|| format!("missing value for --case. {}", usage()))?;
            }
            "--describe-layout" => {
                describe_layout = true;
            }
            "--detach-fragment-streams" => {
                detach_fragment_streams = true;
            }
            "--drop-read-tasks" => {
                drop_read_tasks = true;
            }
            "--help" | "-h" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            "--bench" => {
                // Cargo appends this flag when running harness-free benches.
            }
            other => {
                return Err(format!("unknown argument {other}. {}", usage()).into());
            }
        }
    }

    let uri = uri.ok_or_else(|| format!("missing required --uri. {}", usage()))?;
    if limit_rows == 0 {
        return Err("--limit-rows must be greater than zero".into());
    }
    if matches!(target_bytes, Some(0)) {
        return Err("--target-bytes must be greater than zero".into());
    }
    if raw_range_size_bytes == 0 {
        return Err("--raw-range-size-bytes must be greater than zero".into());
    }
    if take_repetitions == 0 {
        return Err("--take-repetitions must be greater than zero".into());
    }
    if io_buffer_gib.is_empty() {
        return Err("--io-buffer-gib must not be empty".into());
    }
    if batch_size == 0 {
        return Err("--batch-size must be greater than zero".into());
    }
    if matches!(batch_size_bytes, Some(0)) {
        return Err("--batch-size-bytes must be greater than zero".into());
    }
    if matches!(read_chunk_size, Some(0)) {
        return Err("--read-chunk-size must be greater than zero".into());
    }
    if fragment_concurrency == 0 && !matches!(backend, Backend::Scanner) {
        return Err("--fragment-concurrency must be greater than zero".into());
    }
    if batch_concurrency == 0 && !matches!(backend, Backend::Scanner) {
        return Err("--batch-concurrency must be greater than zero".into());
    }
    if sample_ms == 0 {
        return Err("--sample-ms must be greater than zero".into());
    }

    Ok(Config {
        backend,
        uri,
        dataset_version,
        columns,
        limit_rows,
        target_bytes,
        raw_range_size_bytes,
        raw_range_mode,
        raw_column_indices,
        raw_submit_mode,
        raw_completion_mode,
        take_repetitions,
        io_buffer_gib,
        batch_size,
        batch_size_bytes,
        skip_batch_byte_accounting,
        read_chunk_size,
        fragment_concurrency,
        batch_concurrency,
        sample_ms,
        out_dir,
        case_name,
        describe_layout,
        detach_fragment_streams,
        drop_read_tasks,
    })
}

fn parse_backend(value: &str) -> Result<Backend> {
    match value {
        "file-reader" | "lance-file-reader" => Ok(Backend::FileReader),
        "scanner" | "lance-scanner" => Ok(Backend::Scanner),
        "scheduler-raw" | "lance-scheduler-raw" => Ok(Backend::SchedulerRaw),
        "dataset-take" | "take" | "lance-dataset-take" => Ok(Backend::DatasetTake),
        other => Err(format!(
            "invalid --backend value {other}; expected file-reader, scanner, scheduler-raw, or dataset-take"
        )
        .into()),
    }
}

fn parse_raw_submit_mode(value: &str) -> Result<RawSubmitMode> {
    match value {
        "single" => Ok(RawSubmitMode::Single),
        "split-no-concat" => Ok(RawSubmitMode::SplitNoConcat),
        "split-concat" => Ok(RawSubmitMode::SplitConcat),
        other => Err(format!(
            "invalid --raw-submit-mode value {other}; expected single, split-no-concat, or split-concat"
        )
        .into()),
    }
}

fn parse_raw_range_mode(value: &str) -> Result<RawRangeMode> {
    match value {
        "file-sequential" => Ok(RawRangeMode::FileSequential),
        "metadata-pages" => Ok(RawRangeMode::MetadataPages),
        "metadata-pages-round-robin" => Ok(RawRangeMode::MetadataPagesRoundRobin),
        other => Err(format!(
            "invalid --raw-range-mode value {other}; expected file-sequential, metadata-pages, or metadata-pages-round-robin"
        )
        .into()),
    }
}

fn parse_raw_column_indices(value: &str) -> Result<Option<Vec<u32>>> {
    if value == "all" {
        return Ok(None);
    }

    let indices = value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<u32>()
                .map_err(|err| format!("invalid raw column index {part}: {err}").into())
        })
        .collect::<Result<Vec<_>>>()?;
    if indices.is_empty() {
        return Err("--raw-column-indices must specify at least one column index or all".into());
    }
    Ok(Some(indices))
}

fn parse_raw_completion_mode(value: &str) -> Result<RawCompletionMode> {
    match value {
        "unordered" => Ok(RawCompletionMode::Unordered),
        "ordered" => Ok(RawCompletionMode::Ordered),
        other => Err(format!(
            "invalid --raw-completion-mode value {other}; expected unordered or ordered"
        )
        .into()),
    }
}

fn parse_required_value<T>(args: &mut impl Iterator<Item = String>, name: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display + Send + Sync + 'static,
{
    let value = args
        .next()
        .ok_or_else(|| format!("missing value for {name}. {}", usage()))?;
    value
        .parse()
        .map_err(|err| format!("invalid {name} value {value}: {err}").into())
}

fn parse_columns(value: &str) -> Result<Option<Vec<String>>> {
    match value {
        "all" => Ok(None),
        "empty" => Err("FileReader benchmark requires at least one data column".into()),
        _ => Ok(Some(
            value
                .split(',')
                .map(str::trim)
                .filter(|column| !column.is_empty())
                .map(ToString::to_string)
                .collect(),
        )),
    }
}

fn parse_io_buffer_gib(value: &str) -> Result<Vec<Option<u64>>> {
    value
        .split(',')
        .map(|part| {
            let part = part.trim();
            if part == "auto" {
                Ok(None)
            } else {
                part.parse::<u64>()
                    .map(Some)
                    .map_err(|err| format!("invalid --io-buffer-gib value {part}: {err}").into())
            }
        })
        .collect()
}

fn projection_name(columns: &Option<Vec<String>>) -> String {
    match columns {
        None => "all".to_string(),
        Some(columns) => columns.join(","),
    }
}

fn page_layout_kind(encoding: &PageEncoding) -> &'static str {
    match encoding {
        PageEncoding::Legacy(_) => "legacy",
        PageEncoding::Structural(layout) => match layout.layout.as_ref() {
            Some(pb21::page_layout::Layout::MiniBlockLayout(_)) => "miniblock",
            Some(pb21::page_layout::Layout::ConstantLayout(_)) => "constant",
            Some(pb21::page_layout::Layout::FullZipLayout(_)) => "fullzip",
            Some(pb21::page_layout::Layout::BlobLayout(_)) => "blob",
            Some(pb21::page_layout::Layout::SparseLayout(_)) => "sparse",
            None => "missing",
        },
    }
}

fn summarize_u64(values: &[u64]) -> Value {
    if values.is_empty() {
        return json!({
            "count": 0,
            "min": null,
            "p50": null,
            "p90": null,
            "max": null,
            "sum": 0,
        });
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let percentile = |p: f64| {
        let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
        sorted[idx]
    };
    json!({
        "count": sorted.len(),
        "min": sorted[0],
        "p50": percentile(0.5),
        "p90": percentile(0.9),
        "max": *sorted.last().unwrap(),
        "sum": values.iter().sum::<u64>(),
    })
}

async fn describe_layout(config: &Config) -> Result<()> {
    let dataset = Arc::new(
        DatasetBuilder::from_uri(&config.uri)
            .with_version(config.dataset_version)
            .load()
            .await?,
    );
    let fragment = dataset
        .fragments()
        .first()
        .ok_or("dataset has no fragments")?;
    let data_file = fragment
        .files
        .first()
        .ok_or("first fragment has no data files")?;
    if data_file.base_id.is_some() {
        return Err("layout diagnostics do not support external base data files yet".into());
    }

    let data_path = dataset.data_dir().join(data_file.path.as_str());
    let (object_store, _) = LanceObjectStore::from_uri(&config.uri).await?;
    let scheduler = ScanScheduler::new(object_store, SchedulerConfig::new(8 * GIB));
    let file_scheduler = scheduler
        .open_file(&data_path, &CachedFileSize::unknown())
        .await?;
    let metadata = LanceFileReader::read_all_metadata(&file_scheduler).await?;

    let columns = metadata
        .column_infos
        .iter()
        .map(|column| {
            let mut layout_counts = BTreeMap::new();
            let mut page_rows = Vec::with_capacity(column.page_infos.len());
            let mut page_bytes = Vec::with_capacity(column.page_infos.len());
            for page in column.page_infos.iter() {
                *layout_counts
                    .entry(page_layout_kind(&page.encoding))
                    .or_insert(0usize) += 1;
                page_rows.push(page.num_rows);
                page_bytes.push(
                    page.buffer_offsets_and_sizes
                        .iter()
                        .map(|(_, size)| *size)
                        .sum::<u64>(),
                );
            }
            json!({
                "column_index": column.index,
                "num_pages": column.page_infos.len(),
                "layout_counts": layout_counts,
                "page_rows": summarize_u64(&page_rows),
                "page_bytes": summarize_u64(&page_bytes),
            })
        })
        .collect::<Vec<_>>();

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "dataset_uri": config.uri,
            "dataset_version": config.dataset_version,
            "fragment_id": fragment.id,
            "data_file_path": data_file.path,
            "resolved_data_path": data_path.to_string(),
            "file_version": metadata.version().to_string(),
            "num_rows": metadata.num_rows,
            "num_data_bytes": metadata.num_data_bytes,
            "columns": columns,
        }))?
    );
    Ok(())
}

fn projected_schema(dataset_schema: &Schema, columns: &Option<Vec<String>>) -> Result<Schema> {
    Ok(match columns {
        None => dataset_schema.clone(),
        Some(columns) => dataset_schema.project(columns)?,
    })
}

fn file_reader_options(config: &Config) -> Option<FileReaderOptions> {
    if config.batch_size_bytes.is_none() && config.read_chunk_size.is_none() {
        return None;
    }
    Some(FileReaderOptions {
        batch_size_bytes: config.batch_size_bytes,
        read_chunk_size: config.read_chunk_size.unwrap_or(DEFAULT_READ_CHUNK_SIZE),
        ..Default::default()
    })
}

fn add_duration(counter: &AtomicU64, duration: Duration) {
    let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
    counter.fetch_add(nanos, Ordering::Relaxed);
}

fn ns_to_seconds(ns: u64) -> f64 {
    ns as f64 / 1_000_000_000.0
}

fn diff_u64(current: u64, previous: u64) -> u64 {
    current.saturating_sub(previous)
}

fn scheduler_kind_name(kind: SchedulerQueueKind) -> &'static str {
    match kind {
        SchedulerQueueKind::Standard => "standard",
        SchedulerQueueKind::Lite => "lite",
    }
}

fn diagnostics_json(diagnostics: SchedulerDiagnostics) -> Value {
    json!({
        "queue_kind": scheduler_kind_name(diagnostics.kind),
        "scheduler_iops": diagnostics.stats.iops,
        "scheduler_requests": diagnostics.stats.requests,
        "scheduler_bytes_read": diagnostics.stats.bytes_read,
        "io_capacity": diagnostics.io_capacity,
        "iops_available": diagnostics.iops_available,
        "active_iops": diagnostics.active_iops,
        "pending_iops": diagnostics.pending_iops,
        "pending_bytes": diagnostics.pending_bytes,
        "bytes_available": diagnostics.bytes_available,
        "bytes_reserved": diagnostics.bytes_reserved,
        "io_buffer_size_bytes": diagnostics.io_buffer_size_bytes,
        "priorities_in_flight": diagnostics.priorities_in_flight,
        "no_backpressure": diagnostics.no_backpressure,
        "head_task_bytes": diagnostics.head_task_bytes,
        "head_task_priority_high": diagnostics.head_task_priority_high,
        "head_task_priority_low": diagnostics.head_task_priority_low,
        "min_in_flight_priority_high": diagnostics.min_in_flight_priority_high,
        "min_in_flight_priority_low": diagnostics.min_in_flight_priority_low,
        "head_task_can_deliver": diagnostics.head_task_can_deliver,
        "head_task_priority_bypass": diagnostics.head_task_priority_bypass,
        "head_task_blocked_by_iops": diagnostics.head_task_blocked_by_iops,
        "head_task_blocked_by_bytes": diagnostics.head_task_blocked_by_bytes,
    })
}

#[derive(Debug, Default)]
struct ExecutionStatsHolder {
    collected_stats: Arc<Mutex<Option<ExecutionSummaryCounts>>>,
}

impl ExecutionStatsHolder {
    fn get_setter(&self) -> ExecutionStatsCallback {
        let collected_stats = self.collected_stats.clone();
        Arc::new(move |stats| {
            *collected_stats.lock().unwrap() = Some(stats.clone());
        })
    }

    fn consume(self) -> Option<ExecutionSummaryCounts> {
        self.collected_stats.lock().unwrap().take()
    }
}

#[derive(Debug, Clone, Default)]
struct SchedulerDiagnosticsCollector {
    latest: Arc<Mutex<Option<SchedulerDiagnostics>>>,
}

impl SchedulerDiagnosticsCollector {
    fn clear(&self) {
        *self.latest.lock().unwrap() = None;
    }

    fn observe(&self, diagnostics: SchedulerDiagnostics) {
        *self.latest.lock().unwrap() = Some(diagnostics);
    }

    fn snapshot(&self, io_buffer_gib: Option<u64>) -> SchedulerDiagnostics {
        self.latest
            .lock()
            .unwrap()
            .as_ref()
            .copied()
            .unwrap_or_else(|| diagnostics_from_scan_stats(ScanStats::default(), io_buffer_gib))
    }
}

#[derive(Debug, Clone)]
struct SchedulerDiagnosticsLayer {
    collector: SchedulerDiagnosticsCollector,
}

impl SchedulerDiagnosticsLayer {
    fn new(collector: SchedulerDiagnosticsCollector) -> Self {
        Self { collector }
    }
}

fn is_scheduler_state_metadata(metadata: &Metadata<'_>) -> bool {
    // The scheduler uses `tracing::enabled!` before constructing the event;
    // that guard registers a HINT callsite, not an EVENT callsite.
    metadata.target() == SCHEDULER_STATE_EVENT_TARGET && *metadata.level() == tracing::Level::TRACE
}

impl<S> Layer<S> for SchedulerDiagnosticsLayer
where
    S: Subscriber,
{
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if is_scheduler_state_metadata(metadata) {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        is_scheduler_state_metadata(metadata)
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if !is_scheduler_state_metadata(event.metadata()) {
            return;
        }
        let mut visitor = SchedulerDiagnosticsVisitor::default();
        event.record(&mut visitor);
        if let Some(diagnostics) = visitor.into_diagnostics() {
            self.collector.observe(diagnostics);
        }
    }
}

#[derive(Debug, Default)]
struct SchedulerDiagnosticsVisitor {
    kind: Option<SchedulerQueueKind>,
    scheduler_iops: Option<u64>,
    scheduler_requests: Option<u64>,
    scheduler_bytes_read: Option<u64>,
    io_capacity: Option<u64>,
    iops_available: Option<u64>,
    active_iops: Option<u64>,
    pending_iops: Option<u64>,
    pending_bytes: Option<u64>,
    bytes_available: Option<i64>,
    bytes_reserved: Option<i64>,
    io_buffer_size_bytes: Option<u64>,
    priorities_in_flight: Option<u64>,
    no_backpressure: Option<bool>,
    head_task_bytes_present: bool,
    head_task_bytes: Option<u64>,
    head_task_priority_high_present: bool,
    head_task_priority_high: Option<u64>,
    head_task_priority_low_present: bool,
    head_task_priority_low: Option<u64>,
    min_in_flight_priority_high_present: bool,
    min_in_flight_priority_high: Option<u64>,
    min_in_flight_priority_low_present: bool,
    min_in_flight_priority_low: Option<u64>,
    head_task_can_deliver_present: bool,
    head_task_can_deliver: Option<bool>,
    head_task_priority_bypass_present: bool,
    head_task_priority_bypass: Option<bool>,
    head_task_blocked_by_iops_present: bool,
    head_task_blocked_by_iops: Option<bool>,
    head_task_blocked_by_bytes_present: bool,
    head_task_blocked_by_bytes: Option<bool>,
}

impl SchedulerDiagnosticsVisitor {
    fn into_diagnostics(self) -> Option<SchedulerDiagnostics> {
        Some(SchedulerDiagnostics {
            kind: self.kind?,
            stats: ScanStats {
                iops: self.scheduler_iops.unwrap_or_default(),
                requests: self.scheduler_requests.unwrap_or_default(),
                bytes_read: self.scheduler_bytes_read.unwrap_or_default(),
            },
            io_capacity: self.io_capacity.unwrap_or_default(),
            iops_available: self.iops_available.unwrap_or_default(),
            active_iops: self.active_iops.unwrap_or_default(),
            pending_iops: self.pending_iops.unwrap_or_default(),
            pending_bytes: self.pending_bytes.unwrap_or_default(),
            bytes_available: self.bytes_available.unwrap_or_default(),
            bytes_reserved: self.bytes_reserved.unwrap_or_default(),
            io_buffer_size_bytes: self.io_buffer_size_bytes.unwrap_or_default(),
            priorities_in_flight: self.priorities_in_flight.unwrap_or_default(),
            no_backpressure: self.no_backpressure.unwrap_or(false),
            head_task_bytes: optional_u64(self.head_task_bytes_present, self.head_task_bytes),
            head_task_priority_high: optional_u64(
                self.head_task_priority_high_present,
                self.head_task_priority_high,
            ),
            head_task_priority_low: optional_u64(
                self.head_task_priority_low_present,
                self.head_task_priority_low,
            ),
            min_in_flight_priority_high: optional_u64(
                self.min_in_flight_priority_high_present,
                self.min_in_flight_priority_high,
            ),
            min_in_flight_priority_low: optional_u64(
                self.min_in_flight_priority_low_present,
                self.min_in_flight_priority_low,
            ),
            head_task_can_deliver: optional_bool(
                self.head_task_can_deliver_present,
                self.head_task_can_deliver,
            ),
            head_task_priority_bypass: optional_bool(
                self.head_task_priority_bypass_present,
                self.head_task_priority_bypass,
            ),
            head_task_blocked_by_iops: optional_bool(
                self.head_task_blocked_by_iops_present,
                self.head_task_blocked_by_iops,
            ),
            head_task_blocked_by_bytes: optional_bool(
                self.head_task_blocked_by_bytes_present,
                self.head_task_blocked_by_bytes,
            ),
        })
    }
}

impl Visit for SchedulerDiagnosticsVisitor {
    fn record_bool(&mut self, field: &Field, value: bool) {
        match field.name() {
            "no_backpressure" => self.no_backpressure = Some(value),
            "head_task_bytes_present" => self.head_task_bytes_present = value,
            "head_task_priority_high_present" => self.head_task_priority_high_present = value,
            "head_task_priority_low_present" => self.head_task_priority_low_present = value,
            "min_in_flight_priority_high_present" => {
                self.min_in_flight_priority_high_present = value;
            }
            "min_in_flight_priority_low_present" => {
                self.min_in_flight_priority_low_present = value;
            }
            "head_task_can_deliver_present" => self.head_task_can_deliver_present = value,
            "head_task_can_deliver" => self.head_task_can_deliver = Some(value),
            "head_task_priority_bypass_present" => {
                self.head_task_priority_bypass_present = value;
            }
            "head_task_priority_bypass" => self.head_task_priority_bypass = Some(value),
            "head_task_blocked_by_iops_present" => {
                self.head_task_blocked_by_iops_present = value;
            }
            "head_task_blocked_by_iops" => self.head_task_blocked_by_iops = Some(value),
            "head_task_blocked_by_bytes_present" => {
                self.head_task_blocked_by_bytes_present = value;
            }
            "head_task_blocked_by_bytes" => self.head_task_blocked_by_bytes = Some(value),
            _ => {}
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        match field.name() {
            "bytes_available" => self.bytes_available = Some(value),
            "bytes_reserved" => self.bytes_reserved = Some(value),
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "scheduler_iops" => self.scheduler_iops = Some(value),
            "scheduler_requests" => self.scheduler_requests = Some(value),
            "scheduler_bytes_read" => self.scheduler_bytes_read = Some(value),
            "io_capacity" => self.io_capacity = Some(value),
            "iops_available" => self.iops_available = Some(value),
            "active_iops" => self.active_iops = Some(value),
            "pending_iops" => self.pending_iops = Some(value),
            "pending_bytes" => self.pending_bytes = Some(value),
            "io_buffer_size_bytes" => self.io_buffer_size_bytes = Some(value),
            "priorities_in_flight" => self.priorities_in_flight = Some(value),
            "head_task_bytes" => self.head_task_bytes = Some(value),
            "head_task_priority_high" => self.head_task_priority_high = Some(value),
            "head_task_priority_low" => self.head_task_priority_low = Some(value),
            "min_in_flight_priority_high" => self.min_in_flight_priority_high = Some(value),
            "min_in_flight_priority_low" => self.min_in_flight_priority_low = Some(value),
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "queue_kind" {
            self.kind = match value {
                "standard" => Some(SchedulerQueueKind::Standard),
                "lite" => Some(SchedulerQueueKind::Lite),
                _ => None,
            };
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

fn optional_u64(present: bool, value: Option<u64>) -> Option<u64> {
    present.then(|| value.unwrap_or_default())
}

fn optional_bool(present: bool, value: Option<bool>) -> Option<bool> {
    present.then(|| value.unwrap_or(false))
}

fn diagnostics_from_scan_stats(
    stats: ScanStats,
    io_buffer_gib: Option<u64>,
) -> SchedulerDiagnostics {
    SchedulerDiagnostics {
        kind: SchedulerQueueKind::Standard,
        stats,
        io_capacity: 0,
        iops_available: 0,
        active_iops: 0,
        pending_iops: 0,
        pending_bytes: 0,
        bytes_available: 0,
        bytes_reserved: 0,
        io_buffer_size_bytes: io_buffer_gib.map(|value| value * GIB).unwrap_or_default(),
        priorities_in_flight: 0,
        no_backpressure: false,
        head_task_bytes: None,
        head_task_priority_high: None,
        head_task_priority_low: None,
        min_in_flight_priority_high: None,
        min_in_flight_priority_low: None,
        head_task_can_deliver: None,
        head_task_priority_bypass: None,
        head_task_blocked_by_iops: None,
        head_task_blocked_by_bytes: None,
    }
}

fn scan_stats_from_execution_summary(summary: &ExecutionSummaryCounts) -> ScanStats {
    ScanStats {
        iops: summary.iops as u64,
        requests: summary.requests as u64,
        bytes_read: summary.bytes_read as u64,
    }
}

fn sample_json(
    started: Instant,
    counters: &SharedCounters,
    diagnostics: SchedulerDiagnostics,
    decode_in_flight: usize,
    channel_buffered: usize,
    last: &mut LastSample,
) -> Value {
    let elapsed = started.elapsed();
    let interval = elapsed.saturating_sub(last.elapsed);
    let interval_secs = interval.as_secs_f64();
    let rows = counters.rows_completed.load(Ordering::Relaxed);
    let batches = counters.batches_completed.load(Ordering::Relaxed);
    let arrow_bytes = counters.arrow_bytes.load(Ordering::Relaxed);
    let scheduler_stats = diagnostics.stats;
    let delta_scheduler_bytes =
        diff_u64(scheduler_stats.bytes_read, last.scheduler_stats.bytes_read);
    let delta_rows = diff_u64(rows, last.rows);
    let delta_arrow_bytes = diff_u64(arrow_bytes, last.arrow_bytes);
    let physical_gbps = if interval_secs > 0.0 {
        delta_scheduler_bytes as f64 * 8.0 / interval_secs / 1_000_000_000.0
    } else {
        0.0
    };
    let logical_gbps = if interval_secs > 0.0 {
        delta_arrow_bytes as f64 * 8.0 / interval_secs / 1_000_000_000.0
    } else {
        0.0
    };
    let rows_per_second = if interval_secs > 0.0 {
        delta_rows as f64 / interval_secs
    } else {
        0.0
    };

    last.elapsed = elapsed;
    last.scheduler_stats = scheduler_stats;
    last.rows = rows;
    last.batches = batches;
    last.arrow_bytes = arrow_bytes;

    json!({
        "elapsed_seconds": elapsed.as_secs_f64(),
        "interval_seconds": interval_secs,
        "physical_gbps": physical_gbps,
        "logical_gbps": logical_gbps,
        "rows_per_second": rows_per_second,
        "rows": rows,
        "batches": batches,
        "arrow_bytes": arrow_bytes,
        "delta_rows": delta_rows,
        "delta_arrow_bytes": delta_arrow_bytes,
        "delta_scheduler_bytes": delta_scheduler_bytes,
        "fragments_started": counters.fragments_started.load(Ordering::Relaxed),
        "fragments_completed": counters.fragments_completed.load(Ordering::Relaxed),
        "batch_futures_emitted": counters.batch_futures_emitted.load(Ordering::Relaxed),
        "batch_futures_received": counters.batch_futures_received.load(Ordering::Relaxed),
        "batches_completed": counters.batches_completed.load(Ordering::Relaxed),
        "decode_in_flight": decode_in_flight,
        "channel_buffered": channel_buffered,
        "open_reader_seconds_total": ns_to_seconds(counters.open_reader_ns.load(Ordering::Relaxed)),
        "read_stream_create_seconds_total": ns_to_seconds(counters.read_stream_create_ns.load(Ordering::Relaxed)),
        "next_batch_poll_seconds_total": ns_to_seconds(counters.next_batch_poll_ns.load(Ordering::Relaxed)),
        "channel_send_wait_seconds_total": ns_to_seconds(counters.channel_send_wait_ns.load(Ordering::Relaxed)),
        "decode_seconds_total": ns_to_seconds(counters.decode_ns.load(Ordering::Relaxed)),
        "raw_reassemble_seconds_total": ns_to_seconds(counters.raw_reassemble_ns.load(Ordering::Relaxed)),
        "scheduler": diagnostics_json(diagnostics),
    })
}

async fn run_scanner_case(
    config: &Config,
    io_buffer_gib: Option<u64>,
    scheduler_diagnostics: &SchedulerDiagnosticsCollector,
) -> Result<CaseStats> {
    scheduler_diagnostics.clear();
    let dataset = Arc::new(
        DatasetBuilder::from_uri(&config.uri)
            .with_version(config.dataset_version)
            .load()
            .await?,
    );

    let mut remaining_rows = config.limit_rows;
    let mut planned_fragments = 0usize;
    let mut planned_rows = 0u64;
    for fragment in dataset.fragments().iter() {
        if remaining_rows == 0 {
            break;
        }
        let fragment_rows = fragment
            .num_rows()
            .ok_or_else(|| format!("fragment {} is missing num_rows", fragment.id))?
            as u64;
        let rows = fragment_rows.min(remaining_rows);
        planned_fragments += 1;
        planned_rows += rows;
        remaining_rows -= rows;
    }
    if planned_fragments == 0 {
        return Err("no fragments selected".into());
    }

    let counters = Arc::new(SharedCounters::default());
    let stats_holder = ExecutionStatsHolder::default();
    let cpu_before = read_cpu_sample();
    let started = Instant::now();

    let mut scanner = dataset.scan();
    if let Some(columns) = config.columns.as_ref() {
        scanner.project(columns)?;
    }
    scanner
        .batch_size(config.batch_size as usize)
        .scan_in_order(false)
        .scan_stats_callback(stats_holder.get_setter());
    if config.batch_concurrency > 0 {
        scanner
            .batch_readahead(config.batch_concurrency)
            .target_parallelism(config.batch_concurrency);
    }
    if config.fragment_concurrency > 0 {
        scanner.fragment_readahead(config.fragment_concurrency);
    }
    if let Some(file_reader_options) = file_reader_options(config) {
        scanner.with_file_reader_options(file_reader_options);
    }
    if let Some(batch_size_bytes) = config.batch_size_bytes {
        scanner.batch_size_bytes(batch_size_bytes);
    }
    if let Some(io_buffer_gib) = io_buffer_gib {
        scanner.io_buffer_size(io_buffer_gib * GIB);
    }
    let limit_rows = i64::try_from(config.limit_rows)
        .map_err(|_| "--limit-rows is too large for scanner limit")?;
    scanner.limit(Some(limit_rows), None)?;

    let mut stream = scanner.try_into_stream().await?;
    let mut rows = 0u64;
    let mut batches = 0u64;
    let mut arrow_bytes = 0u64;
    let mut samples = Vec::new();
    let mut sample_interval = tokio::time::interval(Duration::from_millis(config.sample_ms));
    sample_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_sample = LastSample {
        elapsed: Duration::default(),
        scheduler_stats: ScanStats::default(),
        rows: 0,
        batches: 0,
        arrow_bytes: 0,
    };
    loop {
        tokio::select! {
            maybe_batch = stream.next() => {
                let Some(batch) = maybe_batch else {
                    break;
                };
                let batch = batch?;
                let batch_bytes = if config.skip_batch_byte_accounting {
                    0
                } else {
                    batch.get_array_memory_size() as u64
                };
                rows += batch.num_rows() as u64;
                batches += 1;
                arrow_bytes += batch_bytes;
                counters.batches_completed.fetch_add(1, Ordering::Relaxed);
                counters
                    .rows_completed
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                counters.arrow_bytes.fetch_add(batch_bytes, Ordering::Relaxed);
            }
            _ = sample_interval.tick() => {
                samples.push(sample_json(
                    started,
                    counters.as_ref(),
                    scheduler_diagnostics.snapshot(io_buffer_gib),
                    0,
                    0,
                    &mut last_sample,
                ));
            }
        }
    }
    drop(stream);

    let elapsed = started.elapsed();
    let summary = stats_holder
        .consume()
        .ok_or("scanner execution stats callback did not run")?;
    let scheduler_stats = scan_stats_from_execution_summary(&summary);
    let mut final_diagnostics = scheduler_diagnostics.snapshot(io_buffer_gib);
    final_diagnostics.stats = scheduler_stats;
    let cpu_after = read_cpu_sample();
    samples.push(sample_json(
        started,
        counters.as_ref(),
        final_diagnostics,
        0,
        0,
        &mut last_sample,
    ));

    Ok(CaseStats {
        rows,
        batches,
        arrow_bytes,
        planned_fragments,
        planned_rows,
        elapsed,
        producer_finished_at: Some(elapsed),
        peak_decode_in_flight: 0,
        cpu_avg: cpu_before.zip(cpu_after).and_then(|(before, after)| {
            let total = after.total.checked_sub(before.total)?;
            let idle = after.idle.checked_sub(before.idle)?;
            if total == 0 {
                return None;
            }
            Some((total - idle) as f64 / total as f64 * 100.0)
        }),
        scheduler_diagnostics: final_diagnostics,
        counters,
        samples,
    })
}

async fn run_dataset_take_case(
    config: &Config,
    scheduler_diagnostics: &SchedulerDiagnosticsCollector,
) -> Result<CaseStats> {
    scheduler_diagnostics.clear();
    let dataset = DatasetBuilder::from_uri(&config.uri)
        .with_version(config.dataset_version)
        .load()
        .await?;
    let projection = Arc::new(projected_schema(dataset.schema(), &config.columns)?);
    let total_rows = dataset
        .fragments()
        .iter()
        .map(|fragment| {
            fragment
                .num_rows()
                .map(|rows| rows as u64)
                .ok_or_else(|| format!("fragment {} is missing num_rows", fragment.id))
        })
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .sum::<u64>();
    if total_rows == 0 {
        return Err("dataset has no rows".into());
    }

    let counters = Arc::new(SharedCounters::default());
    let cpu_before = read_cpu_sample();
    let started = Instant::now();
    let mut rows = 0u64;
    let mut batches = 0u64;
    let mut arrow_bytes = 0u64;
    const STRIDE: u64 = 104_729;

    for repetition in 0..config.take_repetitions {
        let row_ids = (0..config.limit_rows)
            .map(|offset| {
                repetition
                    .wrapping_mul(STRIDE)
                    .wrapping_add(offset.wrapping_mul(STRIDE))
                    % total_rows
            })
            .collect::<Vec<_>>();
        let batch = dataset
            .take(&row_ids, ProjectionRequest::Schema(projection.clone()))
            .await?;
        if batch.num_rows() as u64 != config.limit_rows {
            return Err(format!(
                "take_rows returned {} rows, expected {}",
                batch.num_rows(),
                config.limit_rows
            )
            .into());
        }
        black_box(&batch);
        rows += batch.num_rows() as u64;
        batches += 1;
        let batch_bytes = if config.skip_batch_byte_accounting {
            0
        } else {
            batch.get_array_memory_size() as u64
        };
        arrow_bytes += batch_bytes;
        counters.batches_completed.fetch_add(1, Ordering::Relaxed);
        counters
            .rows_completed
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        counters
            .arrow_bytes
            .fetch_add(batch_bytes, Ordering::Relaxed);
    }

    let elapsed = started.elapsed();
    let cpu_after = read_cpu_sample();
    Ok(CaseStats {
        rows,
        batches,
        arrow_bytes,
        planned_fragments: dataset.fragments().len(),
        planned_rows: total_rows,
        elapsed,
        producer_finished_at: Some(elapsed),
        peak_decode_in_flight: 0,
        cpu_avg: cpu_before.zip(cpu_after).and_then(|(before, after)| {
            let total = after.total.checked_sub(before.total)?;
            let idle = after.idle.checked_sub(before.idle)?;
            if total == 0 {
                return None;
            }
            Some((total - idle) as f64 / total as f64 * 100.0)
        }),
        scheduler_diagnostics: scheduler_diagnostics.snapshot(None),
        counters,
        samples: Vec::new(),
    })
}

enum RawInFlight {
    Unordered(FuturesUnordered<BoxFuture<'static, lance_core::Result<usize>>>),
    Ordered(FuturesOrdered<BoxFuture<'static, lance_core::Result<usize>>>),
}

impl RawInFlight {
    fn new(mode: RawCompletionMode) -> Self {
        match mode {
            RawCompletionMode::Unordered => Self::Unordered(FuturesUnordered::new()),
            RawCompletionMode::Ordered => Self::Ordered(FuturesOrdered::new()),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Unordered(in_flight) => in_flight.len(),
            Self::Ordered(in_flight) => in_flight.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Unordered(in_flight) => in_flight.is_empty(),
            Self::Ordered(in_flight) => in_flight.is_empty(),
        }
    }

    fn push(&mut self, future: BoxFuture<'static, lance_core::Result<usize>>) {
        match self {
            Self::Unordered(in_flight) => in_flight.push(future),
            Self::Ordered(in_flight) => in_flight.push_back(future),
        }
    }

    async fn next(&mut self) -> Option<lance_core::Result<usize>> {
        match self {
            Self::Unordered(in_flight) => in_flight.next().await,
            Self::Ordered(in_flight) => in_flight.next().await,
        }
    }
}

fn raw_read_future(
    file_scheduler: FileScheduler,
    range: Range<u64>,
    priority: u64,
    raw_submit_mode: RawSubmitMode,
    read_chunk_size: u64,
    counters: Arc<SharedCounters>,
) -> BoxFuture<'static, lance_core::Result<usize>> {
    async move {
        match raw_submit_mode {
            RawSubmitMode::Single => {
                let bytes = file_scheduler.submit_single(range, priority).await?;
                Ok(bytes.len())
            }
            RawSubmitMode::SplitNoConcat => {
                let ranges = split_range_by_size(range, read_chunk_size);
                let bytes = file_scheduler.submit_request(ranges, priority).await?;
                Ok(bytes.iter().map(bytes::Bytes::len).sum())
            }
            RawSubmitMode::SplitConcat => {
                let ranges = split_range_by_size(range, read_chunk_size);
                let bytes = file_scheduler.submit_request(ranges, priority).await?;
                let reassemble_started = Instant::now();
                let total_size = bytes.iter().map(bytes::Bytes::len).sum();
                let mut combined = Vec::with_capacity(total_size);
                for chunk in bytes {
                    combined.extend_from_slice(&chunk);
                }
                add_duration(&counters.raw_reassemble_ns, reassemble_started.elapsed());
                let len = combined.len();
                black_box(&combined);
                Ok(len)
            }
        }
    }
    .boxed()
}

fn split_range_by_size(range: Range<u64>, chunk_size: u64) -> Vec<Range<u64>> {
    let range_size = range.end - range.start;
    if range_size <= chunk_size {
        return vec![range];
    }

    let num_chunks = range_size.div_ceil(chunk_size);
    let per_chunk = range_size / num_chunks;
    let mut ranges = Vec::with_capacity(num_chunks as usize);
    for idx in 0..num_chunks {
        let start = range.start + idx * per_chunk;
        let end = if idx == num_chunks - 1 {
            range.end
        } else {
            start + per_chunk
        };
        ranges.push(start..end);
    }
    ranges
}

fn push_split_planned_ranges(
    planned: &mut Vec<(usize, Range<u64>)>,
    file_idx: usize,
    range: Range<u64>,
    chunk_size: u64,
    remaining: &mut u64,
) {
    let mut start = range.start;
    while start < range.end && *remaining > 0 {
        let bytes_to_read = chunk_size.min(range.end - start).min(*remaining);
        if bytes_to_read == 0 {
            break;
        }
        let end = start + bytes_to_read;
        planned.push((file_idx, start..end));
        *remaining -= bytes_to_read;
        start = end;
    }
}

fn push_split_ranges(ranges: &mut Vec<Range<u64>>, range: Range<u64>, chunk_size: u64) {
    let mut start = range.start;
    while start < range.end {
        let bytes_to_read = chunk_size.min(range.end - start);
        if bytes_to_read == 0 {
            break;
        }
        let end = start + bytes_to_read;
        ranges.push(start..end);
        start = end;
    }
}

async fn run_scheduler_raw_case(
    config: &Config,
    io_buffer_gib: Option<u64>,
    scheduler_diagnostics: &SchedulerDiagnosticsCollector,
) -> Result<CaseStats> {
    scheduler_diagnostics.clear();
    let target_bytes = config
        .target_bytes
        .ok_or("--target-bytes is required for --backend scheduler-raw")?;
    let dataset = Arc::new(
        DatasetBuilder::from_uri(&config.uri)
            .with_version(config.dataset_version)
            .load()
            .await?,
    );
    let (object_store, _) = LanceObjectStore::from_uri(&config.uri).await?;
    let scheduler_config = io_buffer_gib
        .map(|gib| SchedulerConfig::new(gib * GIB))
        .unwrap_or_else(|| SchedulerConfig::max_bandwidth(object_store.as_ref()));
    let scheduler = ScanScheduler::new(object_store, scheduler_config);

    let (selected_files, planned) = match config.raw_range_mode {
        RawRangeMode::FileSequential => {
            let mut selected_files = Vec::new();
            let mut selected_file_bytes = 0u64;
            for fragment in dataset.fragments().iter() {
                for data_file in &fragment.files {
                    if data_file.base_id.is_some() {
                        continue;
                    }
                    let Some(file_size) = data_file.file_size_bytes.get() else {
                        continue;
                    };
                    let path = dataset.data_dir().join(data_file.path.as_str());
                    let file_scheduler = scheduler
                        .open_file_with_priority(&path, 0, &data_file.file_size_bytes)
                        .await?;
                    selected_file_bytes += file_size.get();
                    selected_files.push((file_scheduler, file_size.get()));
                    if selected_file_bytes >= target_bytes {
                        break;
                    }
                }
                if selected_file_bytes >= target_bytes {
                    break;
                }
            }
            if selected_files.is_empty() {
                return Err("scheduler-raw found no data files with known sizes".into());
            }

            let mut offsets = vec![0u64; selected_files.len()];
            let mut planned = Vec::new();
            let mut remaining = target_bytes;
            let mut file_idx = 0usize;
            while remaining > 0 {
                let idx = file_idx % selected_files.len();
                let file_size = selected_files[idx].1;
                if offsets[idx] >= file_size {
                    offsets[idx] = 0;
                }
                let available = file_size - offsets[idx];
                let bytes_to_read = config.raw_range_size_bytes.min(available).min(remaining);
                let start = offsets[idx];
                let end = start + bytes_to_read;
                planned.push((idx, start..end));
                offsets[idx] = end;
                remaining -= bytes_to_read;
                file_idx += 1;
            }
            (selected_files, planned)
        }
        RawRangeMode::MetadataPages | RawRangeMode::MetadataPagesRoundRobin => {
            let mut selected_files = Vec::new();
            let mut per_file_ranges = Vec::<Vec<Range<u64>>>::new();
            let mut candidate_bytes = 0u64;

            'fragments: for fragment in dataset.fragments().iter() {
                for data_file in &fragment.files {
                    if data_file.base_id.is_some() {
                        continue;
                    }
                    if data_file.file_size_bytes.get().is_none() {
                        continue;
                    }
                    let path = dataset.data_dir().join(data_file.path.as_str());
                    let file_scheduler = scheduler
                        .open_file_with_priority(&path, 0, &data_file.file_size_bytes)
                        .await?;
                    let metadata = LanceFileReader::read_all_metadata(&file_scheduler).await?;
                    let mut file_ranges = Vec::new();

                    let raw_column_indices = config
                        .raw_column_indices
                        .clone()
                        .unwrap_or_else(|| (0..metadata.column_infos.len() as u32).collect());
                    for column_index in raw_column_indices {
                        let column_info = metadata
                            .column_infos
                            .get(column_index as usize)
                            .ok_or_else(|| {
                                format!(
                                    "raw metadata-pages requested column index {column_index} but file has {} columns",
                                    metadata.column_infos.len()
                                )
                            })?;
                        for page in column_info.page_infos.iter() {
                            for (offset, size) in page.buffer_offsets_and_sizes.iter() {
                                if *size == 0 {
                                    continue;
                                }
                                push_split_ranges(
                                    &mut file_ranges,
                                    *offset..(*offset + *size),
                                    config.raw_range_size_bytes,
                                );
                            }
                        }
                    }

                    if !file_ranges.is_empty() {
                        candidate_bytes += file_ranges
                            .iter()
                            .map(|range| range.end - range.start)
                            .sum::<u64>();
                        selected_files.push((
                            file_scheduler,
                            data_file.file_size_bytes.get().unwrap().get(),
                        ));
                        per_file_ranges.push(file_ranges);
                        if candidate_bytes >= target_bytes {
                            break 'fragments;
                        }
                    }
                }
            }
            if selected_files.is_empty() || per_file_ranges.is_empty() {
                return Err("scheduler-raw metadata-pages found no readable page buffers".into());
            }

            let mut planned = Vec::new();
            let mut remaining = target_bytes;
            match config.raw_range_mode {
                RawRangeMode::MetadataPages => {
                    'ranges: for (file_idx, ranges) in per_file_ranges.iter().enumerate() {
                        for range in ranges {
                            push_split_planned_ranges(
                                &mut planned,
                                file_idx,
                                range.clone(),
                                config.raw_range_size_bytes,
                                &mut remaining,
                            );
                            if remaining == 0 {
                                break 'ranges;
                            }
                        }
                    }
                }
                RawRangeMode::MetadataPagesRoundRobin => {
                    let mut positions = vec![0usize; per_file_ranges.len()];
                    while remaining > 0 {
                        let mut made_progress = false;
                        for (file_idx, ranges) in per_file_ranges.iter().enumerate() {
                            if positions[file_idx] >= ranges.len() {
                                continue;
                            }
                            let range = ranges[positions[file_idx]].clone();
                            positions[file_idx] += 1;
                            made_progress = true;
                            push_split_planned_ranges(
                                &mut planned,
                                file_idx,
                                range,
                                config.raw_range_size_bytes,
                                &mut remaining,
                            );
                            if remaining == 0 {
                                break;
                            }
                        }
                        if !made_progress {
                            break;
                        }
                    }
                }
                RawRangeMode::FileSequential => unreachable!(),
            }

            if remaining > 0 {
                return Err(format!(
                    "scheduler-raw metadata-pages planned {} bytes but target is {target_bytes}",
                    target_bytes - remaining
                )
                .into());
            }
            (selected_files, planned)
        }
    };
    let planned_bytes = planned
        .iter()
        .map(|(_, range)| range.end - range.start)
        .sum::<u64>();

    let counters = Arc::new(SharedCounters::default());
    let cpu_before = read_cpu_sample();
    let started = Instant::now();
    let mut samples = Vec::new();
    let mut sample_interval = tokio::time::interval(Duration::from_millis(config.sample_ms));
    sample_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_sample = LastSample {
        elapsed: Duration::default(),
        scheduler_stats: ScanStats::default(),
        rows: 0,
        batches: 0,
        arrow_bytes: 0,
    };
    let mut in_flight = RawInFlight::new(config.raw_completion_mode);
    let mut next_range = 0usize;
    let read_chunk_size = config.read_chunk_size.unwrap_or(DEFAULT_READ_CHUNK_SIZE);
    while next_range < planned.len() && in_flight.len() < config.batch_concurrency {
        let (idx, range) = planned[next_range].clone();
        in_flight.push(raw_read_future(
            selected_files[idx].0.clone(),
            range,
            next_range as u64,
            config.raw_submit_mode,
            read_chunk_size,
            counters.clone(),
        ));
        next_range += 1;
    }

    let mut bytes_read = 0u64;
    let mut requests_completed = 0u64;
    while !in_flight.is_empty() {
        tokio::select! {
            maybe_bytes = in_flight.next() => {
                let bytes = maybe_bytes.expect("raw read future disappeared")?;
                let bytes = bytes as u64;
                bytes_read += bytes;
                requests_completed += 1;
                counters.batches_completed.fetch_add(1, Ordering::Relaxed);
                counters.arrow_bytes.fetch_add(bytes, Ordering::Relaxed);
                if next_range < planned.len() {
                    let (idx, range) = planned[next_range].clone();
                    in_flight.push(raw_read_future(
                        selected_files[idx].0.clone(),
                        range,
                        next_range as u64,
                        config.raw_submit_mode,
                        read_chunk_size,
                        counters.clone(),
                    ));
                    next_range += 1;
                }
            }
            _ = sample_interval.tick() => {
                samples.push(sample_json(
                    started,
                    counters.as_ref(),
                    scheduler_diagnostics.snapshot(io_buffer_gib),
                    in_flight.len(),
                    planned.len().saturating_sub(next_range),
                    &mut last_sample,
                ));
            }
        }
    }
    counters.arrow_bytes.store(bytes_read, Ordering::Relaxed);
    let mut final_diagnostics = scheduler_diagnostics.snapshot(io_buffer_gib);
    final_diagnostics.stats = scheduler.stats();
    samples.push(sample_json(
        started,
        counters.as_ref(),
        final_diagnostics,
        in_flight.len(),
        0,
        &mut last_sample,
    ));
    let elapsed = started.elapsed();
    let cpu_after = read_cpu_sample();

    Ok(CaseStats {
        rows: 0,
        batches: requests_completed,
        arrow_bytes: bytes_read,
        planned_fragments: selected_files.len(),
        planned_rows: planned_bytes,
        elapsed,
        producer_finished_at: Some(elapsed),
        peak_decode_in_flight: config.batch_concurrency,
        cpu_avg: cpu_before.zip(cpu_after).and_then(|(before, after)| {
            let total = after.total.checked_sub(before.total)?;
            let idle = after.idle.checked_sub(before.idle)?;
            if total == 0 {
                return None;
            }
            Some((total - idle) as f64 / total as f64 * 100.0)
        }),
        scheduler_diagnostics: final_diagnostics,
        counters,
        samples,
    })
}

async fn run_case(
    config: &Config,
    io_buffer_gib: Option<u64>,
    scheduler_diagnostics: &SchedulerDiagnosticsCollector,
) -> Result<CaseStats> {
    scheduler_diagnostics.clear();
    let dataset = Arc::new(
        DatasetBuilder::from_uri(&config.uri)
            .with_version(config.dataset_version)
            .load()
            .await?,
    );
    let projection = Arc::new(projected_schema(dataset.schema(), &config.columns)?);
    let (object_store, _) = LanceObjectStore::from_uri(&config.uri).await?;
    let scheduler_config = io_buffer_gib
        .map(|gib| SchedulerConfig::new(gib * GIB))
        .unwrap_or_else(|| SchedulerConfig::max_bandwidth(object_store.as_ref()));
    let scheduler = ScanScheduler::new(object_store, scheduler_config);

    let mut planned = Vec::new();
    let mut remaining_rows = config.limit_rows;
    for fragment in dataset.fragments().iter() {
        if remaining_rows == 0 {
            break;
        }
        let fragment_rows = fragment
            .num_rows()
            .ok_or_else(|| format!("fragment {} is missing num_rows", fragment.id))?
            as u64;
        let rows = fragment_rows.min(remaining_rows);
        planned.push((fragment.clone(), rows));
        remaining_rows -= rows;
    }
    if planned.is_empty() {
        return Err("no fragments selected".into());
    }
    let planned_rows: u64 = planned.iter().map(|(_, rows)| *rows).sum();
    let planned_fragments = planned.len();

    let counters = Arc::new(SharedCounters::default());
    let cpu_before = read_cpu_sample();
    let started = Instant::now();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<
        BoxFuture<'static, lance_core::Result<RecordBatch>>,
    >(config.batch_concurrency * 2);
    let producer = if config.detach_fragment_streams {
        tokio::spawn({
            let dataset = dataset.clone();
            let projection = projection.clone();
            let scheduler = scheduler.clone();
            let counters = counters.clone();
            let batch_size = config.batch_size;
            let file_reader_options = file_reader_options(config);
            let fragment_concurrency = config.fragment_concurrency;
            async move {
                let drainers = futures::stream::iter(planned.into_iter().enumerate())
                    .map({
                        move |(priority, (fragment, rows))| {
                            let dataset = dataset.clone();
                            let projection = projection.clone();
                            let scheduler = scheduler.clone();
                            let tx = tx.clone();
                            let counters = counters.clone();
                            let file_reader_options = file_reader_options.clone();
                            async move {
                                counters.fragments_started.fetch_add(1, Ordering::Relaxed);
                                let file_fragment = FileFragment::new(dataset, fragment);
                                let read_config = FragReadConfig::default()
                                    .with_scan_scheduler(scheduler)
                                    .with_reader_priority(priority as u32);
                                let read_config = if let Some(file_reader_options) =
                                    file_reader_options.clone()
                                {
                                    read_config.with_file_reader_options(file_reader_options)
                                } else {
                                    read_config
                                };

                                let open_started = Instant::now();
                                let reader =
                                    file_fragment.open(projection.as_ref(), read_config).await?;
                                add_duration(&counters.open_reader_ns, open_started.elapsed());

                                let create_stream_started = Instant::now();
                                let mut read_stream =
                                    reader.read_ranges(vec![0..rows].into(), batch_size).await?;
                                add_duration(
                                    &counters.read_stream_create_ns,
                                    create_stream_started.elapsed(),
                                );

                                let drainer = tokio::spawn(async move {
                                    loop {
                                        let next_started = Instant::now();
                                        let maybe_batch_fut = read_stream.next().await;
                                        add_duration(
                                            &counters.next_batch_poll_ns,
                                            next_started.elapsed(),
                                        );
                                        let Some(batch_fut) = maybe_batch_fut else {
                                            break;
                                        };
                                        counters
                                            .batch_futures_emitted
                                            .fetch_add(1, Ordering::Relaxed);
                                        let send_started = Instant::now();
                                        tx.send(batch_fut)
                                            .await
                                            .map_err(|_| "batch consumer dropped")?;
                                        add_duration(
                                            &counters.channel_send_wait_ns,
                                            send_started.elapsed(),
                                        );
                                    }
                                    counters.fragments_completed.fetch_add(1, Ordering::Relaxed);
                                    Ok::<_, Error>(())
                                });
                                Ok::<_, Error>(drainer)
                            }
                        }
                    })
                    .buffer_unordered(fragment_concurrency)
                    .try_collect::<Vec<_>>()
                    .await?;

                for drainer in drainers {
                    drainer.await.map_err(Error::from)??;
                }
                Ok::<_, Error>(())
            }
        })
    } else {
        tokio::spawn({
            let dataset = dataset.clone();
            let projection = projection.clone();
            let scheduler = scheduler.clone();
            let counters = counters.clone();
            let batch_size = config.batch_size;
            let file_reader_options = file_reader_options(config);
            let fragment_concurrency = config.fragment_concurrency;
            async move {
                futures::stream::iter(planned.into_iter().enumerate())
                    .map({
                        move |(priority, (fragment, rows))| {
                            let dataset = dataset.clone();
                            let projection = projection.clone();
                            let scheduler = scheduler.clone();
                            let tx = tx.clone();
                            let counters = counters.clone();
                            let file_reader_options = file_reader_options.clone();
                            async move {
                                counters.fragments_started.fetch_add(1, Ordering::Relaxed);
                                let file_fragment = FileFragment::new(dataset, fragment);
                                let read_config = FragReadConfig::default()
                                    .with_scan_scheduler(scheduler)
                                    .with_reader_priority(priority as u32);
                                let read_config = if let Some(file_reader_options) =
                                    file_reader_options.clone()
                                {
                                    read_config.with_file_reader_options(file_reader_options)
                                } else {
                                    read_config
                                };

                                let open_started = Instant::now();
                                let reader =
                                    file_fragment.open(projection.as_ref(), read_config).await?;
                                add_duration(&counters.open_reader_ns, open_started.elapsed());

                                let create_stream_started = Instant::now();
                                let mut read_stream =
                                    reader.read_ranges(vec![0..rows].into(), batch_size).await?;
                                add_duration(
                                    &counters.read_stream_create_ns,
                                    create_stream_started.elapsed(),
                                );

                                loop {
                                    let next_started = Instant::now();
                                    let maybe_batch_fut = read_stream.next().await;
                                    add_duration(
                                        &counters.next_batch_poll_ns,
                                        next_started.elapsed(),
                                    );
                                    let Some(batch_fut) = maybe_batch_fut else {
                                        break;
                                    };
                                    counters
                                        .batch_futures_emitted
                                        .fetch_add(1, Ordering::Relaxed);
                                    let send_started = Instant::now();
                                    tx.send(batch_fut)
                                        .await
                                        .map_err(|_| "batch consumer dropped")?;
                                    add_duration(
                                        &counters.channel_send_wait_ns,
                                        send_started.elapsed(),
                                    );
                                }
                                counters.fragments_completed.fetch_add(1, Ordering::Relaxed);
                                Ok::<_, Error>(())
                            }
                        }
                    })
                    .buffer_unordered(fragment_concurrency)
                    .try_collect::<Vec<_>>()
                    .await?;
                Ok::<_, Error>(())
            }
        })
    };

    let mut in_flight = FuturesUnordered::new();
    let skip_batch_byte_accounting = config.skip_batch_byte_accounting;
    let drop_read_tasks = config.drop_read_tasks;
    let mut producer_done = false;
    let mut producer_finished_at = None;
    let mut rows = 0u64;
    let mut batches = 0u64;
    let mut arrow_bytes = 0u64;
    let mut peak_decode_in_flight = 0usize;
    let mut samples = Vec::new();
    let mut sample_interval = tokio::time::interval(Duration::from_millis(config.sample_ms));
    sample_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_sample = LastSample {
        elapsed: Duration::default(),
        scheduler_stats: ScanStats::default(),
        rows: 0,
        batches: 0,
        arrow_bytes: 0,
    };

    loop {
        if producer_done && in_flight.is_empty() {
            break;
        }
        tokio::select! {
            maybe_batch_fut = rx.recv(), if !producer_done && in_flight.len() < config.batch_concurrency => {
                if let Some(batch_fut) = maybe_batch_fut {
                    counters.batch_futures_received.fetch_add(1, Ordering::Relaxed);
                    if drop_read_tasks {
                        drop(batch_fut);
                        counters.batches_completed.fetch_add(1, Ordering::Relaxed);
                        batches += 1;
                        continue;
                    }
                    let counters_for_task = counters.clone();
                    in_flight.push(async move {
                        let decode_started = Instant::now();
                        let batch = batch_fut.await?;
                        let batch_bytes = if skip_batch_byte_accounting {
                            0
                        } else {
                            batch.get_array_memory_size() as u64
                        };
                        add_duration(&counters_for_task.decode_ns, decode_started.elapsed());
                        counters_for_task.batches_completed.fetch_add(1, Ordering::Relaxed);
                        counters_for_task.rows_completed.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                        counters_for_task.arrow_bytes.fetch_add(batch_bytes, Ordering::Relaxed);
                        Ok::<_, lance_core::Error>((batch, batch_bytes))
                    });
                    peak_decode_in_flight = peak_decode_in_flight.max(in_flight.len());
                } else {
                    producer_done = true;
                    producer_finished_at = Some(started.elapsed());
                }
            }
            maybe_batch = in_flight.next(), if !in_flight.is_empty() => {
                let (batch, batch_bytes) = maybe_batch.expect("in-flight batch future disappeared")?;
                rows += batch.num_rows() as u64;
                batches += 1;
                arrow_bytes += batch_bytes;
            }
            _ = sample_interval.tick() => {
                samples.push(sample_json(
                    started,
                    counters.as_ref(),
                    scheduler_diagnostics.snapshot(io_buffer_gib),
                    in_flight.len(),
                    rx.len(),
                    &mut last_sample,
                ));
            }
        }
    }
    producer.await??;

    let mut final_diagnostics = scheduler_diagnostics.snapshot(io_buffer_gib);
    final_diagnostics.stats = scheduler.stats();
    samples.push(sample_json(
        started,
        counters.as_ref(),
        final_diagnostics,
        in_flight.len(),
        rx.len(),
        &mut last_sample,
    ));

    let elapsed = started.elapsed();
    let cpu_after = read_cpu_sample();

    Ok(CaseStats {
        rows,
        batches,
        arrow_bytes,
        planned_fragments,
        planned_rows,
        elapsed,
        producer_finished_at,
        peak_decode_in_flight,
        cpu_avg: cpu_before.zip(cpu_after).and_then(|(before, after)| {
            let total = after.total.checked_sub(before.total)?;
            let idle = after.idle.checked_sub(before.idle)?;
            if total == 0 {
                return None;
            }
            Some((total - idle) as f64 / total as f64 * 100.0)
        }),
        scheduler_diagnostics: final_diagnostics,
        counters,
        samples,
    })
}

fn read_cpu_sample() -> Option<CpuSample> {
    let contents = fs::read_to_string("/proc/stat").ok()?;
    let line = contents.lines().next()?;
    let values = line
        .split_whitespace()
        .skip(1)
        .map(|value| value.parse::<u64>())
        .collect::<std::result::Result<Vec<_>, _>>()
        .ok()?;
    if values.len() < 5 {
        return None;
    }

    let idle = values[3] + values[4];
    let total = values.iter().sum();
    Some(CpuSample { idle, total })
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn current_commit() -> String {
    option_env!("LANCE_BENCH_COMMIT")
        .or_else(|| option_env!("GIT_COMMIT"))
        .unwrap_or("unknown")
        .to_string()
}

fn env_var(name: &str) -> Option<String> {
    env::var(name).ok()
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = parse_args()?;
    if config.describe_layout {
        return describe_layout(&config).await;
    }
    let scheduler_diagnostics = SchedulerDiagnosticsCollector::default();
    let subscriber = tracing_subscriber::registry().with(SchedulerDiagnosticsLayer::new(
        scheduler_diagnostics.clone(),
    ));
    tracing::subscriber::set_global_default(subscriber).map_err(|error| {
        std::io::Error::other(format!(
            "failed to install scheduler diagnostics subscriber: {error}"
        ))
    })?;

    fs::create_dir_all(&config.out_dir)?;
    let output_path = format!(
        "{}/s3_file_reader_diagnostics_{}.jsonl",
        config.out_dir,
        now_unix_secs()
    );
    let mut jsonl = String::new();
    let commit = current_commit();
    let instance = env_var("EC2_INSTANCE_TYPE").unwrap_or_else(|| "unknown".to_string());
    let region = env_var("AWS_REGION")
        .or_else(|| env_var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|| "unknown".to_string());
    let projection = projection_name(&config.columns);

    for io_buffer_gib in &config.io_buffer_gib {
        println!(
            "running case={} backend={} projection={} limit_rows={} io_buffer_gib={} batch_size={} fragment_concurrency={} batch_concurrency={} sample_ms={}",
            config.case_name,
            config.backend.name(),
            projection,
            config.limit_rows,
            io_buffer_gib
                .map(|value| value.to_string())
                .unwrap_or_else(|| "auto".to_string()),
            config.batch_size,
            config.fragment_concurrency,
            config.batch_concurrency,
            config.sample_ms
        );
        let stats = match config.backend {
            Backend::FileReader => {
                run_case(&config, *io_buffer_gib, &scheduler_diagnostics).await?
            }
            Backend::Scanner => {
                run_scanner_case(&config, *io_buffer_gib, &scheduler_diagnostics).await?
            }
            Backend::SchedulerRaw => {
                run_scheduler_raw_case(&config, *io_buffer_gib, &scheduler_diagnostics).await?
            }
            Backend::DatasetTake => run_dataset_take_case(&config, &scheduler_diagnostics).await?,
        };
        let elapsed_secs = stats.elapsed.as_secs_f64();
        let scheduler_stats = stats.scheduler_diagnostics.stats;
        let logical_gbps = if elapsed_secs > 0.0 {
            stats.arrow_bytes as f64 * 8.0 / elapsed_secs / 1_000_000_000.0
        } else {
            0.0
        };
        let physical_gbps = if elapsed_secs > 0.0 {
            scheduler_stats.bytes_read as f64 * 8.0 / elapsed_secs / 1_000_000_000.0
        } else {
            0.0
        };
        let rows_per_second = if elapsed_secs > 0.0 {
            stats.rows as f64 / elapsed_secs
        } else {
            0.0
        };
        let bytes_per_row = stats.arrow_bytes.checked_div(stats.rows).unwrap_or(0);
        let avg_bytes_per_scheduler_request = scheduler_stats
            .bytes_read
            .checked_div(scheduler_stats.requests)
            .unwrap_or(0);
        let avg_bytes_per_scheduler_iop = scheduler_stats
            .bytes_read
            .checked_div(scheduler_stats.iops)
            .unwrap_or(0);
        let counters = stats.counters.as_ref();
        let record = json!({
            "case": config.case_name,
            "instance": instance,
            "region": region,
            "layer": config.backend.layer(),
            "backend": config.backend.name(),
            "dataset_uri": config.uri,
            "dataset_version": config.dataset_version,
            "lance_commit": commit,
            "projection": projection,
            "limit_rows": config.limit_rows,
            "target_bytes": config.target_bytes,
            "raw_range_size_bytes": config.raw_range_size_bytes,
            "raw_range_mode": config.raw_range_mode.name(),
            "raw_column_indices": config.raw_column_indices.clone(),
            "raw_submit_mode": config.raw_submit_mode.name(),
            "raw_completion_mode": config.raw_completion_mode.name(),
            "take_repetitions": config.take_repetitions,
            "raw_read_chunk_size_bytes": config.read_chunk_size.unwrap_or(DEFAULT_READ_CHUNK_SIZE),
            "planned_rows": stats.planned_rows,
            "planned_fragments": stats.planned_fragments,
            "rows": stats.rows,
            "batches": stats.batches,
            "batch_size": config.batch_size,
            "batch_size_bytes": config.batch_size_bytes,
            "skip_batch_byte_accounting": config.skip_batch_byte_accounting,
            "read_chunk_size": config.read_chunk_size,
            "fragment_concurrency": config.fragment_concurrency,
            "batch_concurrency": config.batch_concurrency,
            "detach_fragment_streams": config.detach_fragment_streams,
            "drop_read_tasks": config.drop_read_tasks,
            "sample_ms": config.sample_ms,
            "io_buffer_bytes": io_buffer_gib.map(|value| value * GIB),
            "io_buffer_mode": if io_buffer_gib.is_some() { "explicit" } else { "auto" },
            "lance_io_threads": env_var("LANCE_IO_THREADS").or_else(|| env_var("IO_THREADS")),
            "lance_default_io_buffer_size": env_var("LANCE_DEFAULT_IO_BUFFER_SIZE"),
            "lance_max_iop_size": env_var("LANCE_MAX_IOP_SIZE"),
            "lance_use_lite_scheduler": env_var("LANCE_USE_LITE_SCHEDULER"),
            "lance_inline_scheduling_threshold": env_var("LANCE_INLINE_SCHEDULING_THRESHOLD"),
            "elapsed_seconds": elapsed_secs,
            "producer_finished_seconds": stats.producer_finished_at.map(|duration| duration.as_secs_f64()),
            "logical_gbps": logical_gbps,
            "physical_gbps": physical_gbps,
            "rows_per_second": rows_per_second,
            "arrow_bytes": stats.arrow_bytes,
            "bytes_per_row": bytes_per_row,
            "avg_bytes_per_scheduler_request": avg_bytes_per_scheduler_request,
            "avg_bytes_per_scheduler_iop": avg_bytes_per_scheduler_iop,
            "scheduler_iops": scheduler_stats.iops,
            "scheduler_requests": scheduler_stats.requests,
            "scheduler_bytes_read": scheduler_stats.bytes_read,
            "scheduler_diagnostics": diagnostics_json(stats.scheduler_diagnostics),
            "fragments_started": counters.fragments_started.load(Ordering::Relaxed),
            "fragments_completed": counters.fragments_completed.load(Ordering::Relaxed),
            "batch_futures_emitted": counters.batch_futures_emitted.load(Ordering::Relaxed),
            "batch_futures_received": counters.batch_futures_received.load(Ordering::Relaxed),
            "batches_completed": counters.batches_completed.load(Ordering::Relaxed),
            "peak_decode_in_flight": stats.peak_decode_in_flight,
            "open_reader_seconds_total": ns_to_seconds(counters.open_reader_ns.load(Ordering::Relaxed)),
            "read_stream_create_seconds_total": ns_to_seconds(counters.read_stream_create_ns.load(Ordering::Relaxed)),
            "next_batch_poll_seconds_total": ns_to_seconds(counters.next_batch_poll_ns.load(Ordering::Relaxed)),
            "channel_send_wait_seconds_total": ns_to_seconds(counters.channel_send_wait_ns.load(Ordering::Relaxed)),
            "decode_seconds_total": ns_to_seconds(counters.decode_ns.load(Ordering::Relaxed)),
            "raw_reassemble_seconds_total": ns_to_seconds(counters.raw_reassemble_ns.load(Ordering::Relaxed)),
            "cpu_avg": stats.cpu_avg,
            "samples": stats.samples,
        });
        println!(
            "case={} backend={} projection={} io_buffer_gib={} elapsed={:.3}s logical_gbps={:.2} physical_gbps={:.2} rows={} batches={} scheduler_bytes_read={} scheduler_iops={} scheduler_requests={} active_iops={} pending_iops={} cpu_avg={}",
            config.case_name,
            config.backend.name(),
            projection,
            io_buffer_gib
                .map(|value| value.to_string())
                .unwrap_or_else(|| "auto".to_string()),
            elapsed_secs,
            logical_gbps,
            physical_gbps,
            stats.rows,
            stats.batches,
            scheduler_stats.bytes_read,
            scheduler_stats.iops,
            scheduler_stats.requests,
            stats.scheduler_diagnostics.active_iops,
            stats.scheduler_diagnostics.pending_iops,
            stats
                .cpu_avg
                .map(|value| format!("{value:.1}%"))
                .unwrap_or_else(|| "unknown".to_string())
        );
        jsonl.push_str(&serde_json::to_string(&record)?);
        jsonl.push('\n');
        fs::write(&output_path, &jsonl)?;
    }

    println!("wrote {output_path}");
    Ok(())
}
