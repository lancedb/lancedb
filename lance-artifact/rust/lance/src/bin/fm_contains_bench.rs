// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, LargeStringArray, RecordBatch, StringArray, UInt64Array};
use clap::Parser;
use futures::future::join_all;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::{Dataset, ReadParams};
use lance::index::DatasetIndexExt;
use lance::{Error, Result};
use lance_io::object_store::{ObjectStoreParams, StorageOptionsAccessor};
use serde::Serialize;
use tokio::sync::Semaphore;
use tokio::time::timeout;

const ROW_ID_COLUMN: &str = "_rowid";

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(
        long,
        default_value = "az://datasets/mmlb/mmlb_100m_fts_en_fm_20260626.lance"
    )]
    uri: String,

    #[arg(long, default_value = "full_content_idx")]
    index_name: String,

    #[arg(long, default_value = "full_content")]
    text_column: String,

    #[arg(long, default_value = "summary_in_image")]
    pattern_source_column: String,

    #[arg(long, default_value = "oailancepub")]
    storage_account: String,

    #[arg(long, value_parser = parse_storage_option)]
    storage_option: Vec<(String, String)>,

    #[arg(long, default_value_t = 1_099_511_627_776)]
    index_cache_bytes: usize,

    #[arg(long, default_value_t = 8_589_934_592)]
    metadata_cache_bytes: usize,

    #[arg(long, default_value_t = 5_000)]
    sample_rows: usize,

    #[arg(long, default_value_t = 5)]
    query_term_count: usize,

    #[arg(long, default_value_t = 100)]
    k: i64,

    #[arg(long, default_value_t = 0)]
    warmup: usize,

    #[arg(long, default_value_t = 4)]
    queries: usize,

    #[arg(long, default_value = "1 2 4 8")]
    threads: String,

    #[arg(long, default_value_t = 60.0)]
    query_timeout_secs: f64,

    #[arg(long, default_value_t = false)]
    skip_prewarm: bool,

    #[arg(long, default_value_t = true)]
    explain: bool,

    #[arg(long)]
    output_json: String,

    #[arg(long)]
    output_csv: String,
}

#[derive(Debug, Clone, Serialize)]
struct QueryPattern {
    query_id: String,
    pattern: String,
    filter_sql: String,
}

#[derive(Debug, Serialize)]
struct QueryResult {
    query_id: String,
    pattern: String,
    filter_sql: String,
    success: bool,
    error: Option<String>,
    elapsed_ms: f64,
    result_count: usize,
    checksum: u64,
}

#[derive(Debug, Serialize)]
struct BatchSummary {
    round_type: String,
    threads: usize,
    queries: usize,
    successes: usize,
    success_rate: f64,
    wall_s: f64,
    qps: f64,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    avg_result_count: f64,
    max_result_count: usize,
    checksum: u64,
    results: Vec<QueryResult>,
}

#[derive(Debug, Serialize)]
struct BenchmarkOutput {
    uri: String,
    index_name: String,
    text_column: String,
    pattern_source_column: String,
    sample_rows: usize,
    query_term_count: usize,
    k: i64,
    warmup: usize,
    queries: usize,
    threads: Vec<usize>,
    query_timeout_secs: f64,
    index_cache_bytes: usize,
    metadata_cache_bytes: usize,
    prewarm_ms: Option<f64>,
    explain_plan: Option<String>,
    summaries: Vec<BatchSummary>,
}

fn parse_threads(value: &str) -> std::result::Result<Vec<usize>, String> {
    let mut threads = Vec::new();
    for part in value.replace(',', " ").split_whitespace() {
        let thread_count = part
            .parse::<usize>()
            .map_err(|err| format!("invalid thread count {part:?}: {err}"))?;
        if thread_count == 0 {
            return Err("thread counts must be greater than zero".to_string());
        }
        threads.push(thread_count);
    }
    if threads.is_empty() {
        return Err("at least one thread count is required".to_string());
    }
    Ok(threads)
}

fn parse_storage_option(value: &str) -> std::result::Result<(String, String), String> {
    let Some((key, val)) = value.split_once('=') else {
        return Err("storage options must be key=value".to_string());
    };
    if key.is_empty() {
        return Err("storage option key cannot be empty".to_string());
    }
    Ok((key.to_string(), val.to_string()))
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn query_terms_from_text(text: &str, term_count: usize) -> Result<String> {
    let terms = text.split_whitespace().take(term_count).collect::<Vec<_>>();
    if terms.len() < term_count {
        return Err(Error::invalid_input(format!(
            "sampled query text has {} terms, but requested {}",
            terms.len(),
            term_count
        )));
    }
    Ok(terms.join(" "))
}

fn string_value(batch: &RecordBatch, column: &str, row: usize) -> Result<Option<String>> {
    let array = batch
        .column_by_name(column)
        .ok_or_else(|| Error::invalid_input(format!("column {column:?} not found")))?;
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(strings.value(row).to_string()));
    }
    if let Some(strings) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Some(strings.value(row).to_string()));
    }
    Err(Error::invalid_input(format!(
        "column {column:?} must be Utf8 or LargeUtf8, got {:?}",
        array.data_type()
    )))
}

async fn open_dataset(args: &Args) -> Result<Dataset> {
    let mut options = HashMap::from([("account_name".to_string(), args.storage_account.clone())]);
    for (key, val) in &args.storage_option {
        options.insert(key.clone(), val.clone());
    }
    let read_params = ReadParams {
        index_cache_size_bytes: args.index_cache_bytes,
        metadata_cache_size_bytes: args.metadata_cache_bytes,
        store_options: Some(ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                options,
            ))),
            ..Default::default()
        }),
        ..Default::default()
    };

    DatasetBuilder::from_uri(&args.uri)
        .with_read_params(read_params)
        .load()
        .await
}

async fn sample_patterns(dataset: &Dataset, args: &Args) -> Result<Vec<QueryPattern>> {
    let total = args.warmup + args.queries;
    let mut scanner = dataset.scan();
    scanner
        .project(&[args.pattern_source_column.as_str()])?
        .limit(Some(args.sample_rows as i64), None)?;
    let batch = scanner.try_into_batch().await?;
    let mut patterns = Vec::with_capacity(total);
    let mut seen = std::collections::HashSet::new();

    for row in 0..batch.num_rows() {
        let Some(text) = string_value(&batch, &args.pattern_source_column, row)? else {
            continue;
        };
        let pattern = query_terms_from_text(&text, args.query_term_count)?;
        if seen.insert(pattern.clone()) {
            let filter_sql = format!(
                "contains({}, {})",
                args.text_column,
                sql_quote(pattern.as_str())
            );
            patterns.push(QueryPattern {
                query_id: format!("Q{:06}", patterns.len() + 1),
                pattern,
                filter_sql,
            });
            if patterns.len() == total {
                return Ok(patterns);
            }
        }
    }

    Err(Error::invalid_input(format!(
        "sampled only {} unique patterns, need {}; increase --sample-rows",
        patterns.len(),
        total
    )))
}

async fn explain_first_query(
    dataset: &Dataset,
    pattern: &QueryPattern,
    args: &Args,
) -> Result<String> {
    let mut scanner = dataset.scan();
    scanner
        .with_row_id()
        .project::<&str>(&[])?
        .filter(&pattern.filter_sql)?
        .limit(Some(args.k), None)?;
    scanner.explain_plan(false).await
}

async fn execute_query(
    dataset: Arc<Dataset>,
    pattern: &QueryPattern,
    args: &Args,
) -> Result<QueryResult> {
    let start = Instant::now();
    let mut scanner = dataset.scan();
    scanner
        .with_row_id()
        .project::<&str>(&[])?
        .filter(&pattern.filter_sql)?
        .limit(Some(args.k), None)?;
    let batch = scanner.try_into_batch().await?;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let row_ids = batch
        .column_by_name(ROW_ID_COLUMN)
        .ok_or_else(|| Error::invalid_input(format!("{ROW_ID_COLUMN} column missing")))?;
    let row_ids = row_ids
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| Error::invalid_input(format!("{ROW_ID_COLUMN} must be UInt64")))?;
    let mut checksum = 0_u64;
    for row in 0..row_ids.len() {
        if !row_ids.is_null(row) {
            checksum ^= row_ids.value(row);
        }
    }
    Ok(QueryResult {
        query_id: pattern.query_id.clone(),
        pattern: pattern.pattern.clone(),
        filter_sql: pattern.filter_sql.clone(),
        success: true,
        error: None,
        elapsed_ms,
        result_count: batch.num_rows(),
        checksum,
    })
}

async fn run_one(dataset: Arc<Dataset>, pattern: QueryPattern, args: Arc<Args>) -> QueryResult {
    let start = Instant::now();
    let result = if args.query_timeout_secs > 0.0 {
        timeout(
            Duration::from_secs_f64(args.query_timeout_secs),
            execute_query(dataset, &pattern, &args),
        )
        .await
        .map_err(|_| {
            Error::io(format!(
                "query timed out after {}s",
                args.query_timeout_secs
            ))
        })
        .and_then(|inner| inner)
    } else {
        execute_query(dataset, &pattern, &args).await
    };

    match result {
        Ok(result) => result,
        Err(err) => QueryResult {
            query_id: pattern.query_id,
            pattern: pattern.pattern,
            filter_sql: pattern.filter_sql,
            success: false,
            error: Some(err.to_string()),
            elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
            result_count: 0,
            checksum: 0,
        },
    }
}

async fn run_batch(
    dataset: Arc<Dataset>,
    args: Arc<Args>,
    patterns: &[QueryPattern],
    thread_count: usize,
    round_type: &str,
) -> BatchSummary {
    let wall_start = Instant::now();
    let semaphore = Arc::new(Semaphore::new(thread_count));
    let tasks = patterns
        .iter()
        .map(|pattern| {
            let pattern = pattern.clone();
            let dataset = Arc::clone(&dataset);
            let args = Arc::clone(&args);
            let semaphore = Arc::clone(&semaphore);
            tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.expect("semaphore closed");
                run_one(dataset, pattern, args).await
            })
        })
        .collect::<Vec<_>>();

    let results = join_all(tasks)
        .await
        .into_iter()
        .map(|result| match result {
            Ok(query_result) => query_result,
            Err(err) => QueryResult {
                query_id: "join_error".to_string(),
                pattern: String::new(),
                filter_sql: String::new(),
                success: false,
                error: Some(err.to_string()),
                elapsed_ms: 0.0,
                result_count: 0,
                checksum: 0,
            },
        })
        .collect::<Vec<_>>();
    summarize(round_type, thread_count, results, wall_start.elapsed())
}

fn percentile(values: &[f64], pct: usize) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut ordered = values.to_vec();
    ordered.sort_by(|left, right| left.total_cmp(right));
    let idx = ((ordered.len() * pct).div_ceil(100)).saturating_sub(1);
    ordered[idx.min(ordered.len() - 1)]
}

fn summarize(
    round_type: &str,
    threads: usize,
    results: Vec<QueryResult>,
    wall: Duration,
) -> BatchSummary {
    let successes = results.iter().filter(|result| result.success).count();
    let latencies = results
        .iter()
        .filter(|result| result.success)
        .map(|result| result.elapsed_ms)
        .collect::<Vec<_>>();
    let result_counts = results
        .iter()
        .filter(|result| result.success)
        .map(|result| result.result_count)
        .collect::<Vec<_>>();
    let wall_s = wall.as_secs_f64();
    BatchSummary {
        round_type: round_type.to_string(),
        threads,
        queries: results.len(),
        successes,
        success_rate: if results.is_empty() {
            0.0
        } else {
            successes as f64 / results.len() as f64
        },
        wall_s,
        qps: if wall_s == 0.0 {
            0.0
        } else {
            successes as f64 / wall_s
        },
        mean_ms: if latencies.is_empty() {
            0.0
        } else {
            latencies.iter().sum::<f64>() / latencies.len() as f64
        },
        p50_ms: percentile(&latencies, 50),
        p95_ms: percentile(&latencies, 95),
        p99_ms: percentile(&latencies, 99),
        avg_result_count: if result_counts.is_empty() {
            0.0
        } else {
            result_counts.iter().sum::<usize>() as f64 / result_counts.len() as f64
        },
        max_result_count: result_counts.into_iter().max().unwrap_or(0),
        checksum: results
            .iter()
            .filter(|result| result.success)
            .map(|result| result.checksum)
            .fold(0, |acc, checksum| acc ^ checksum),
        results,
    }
}

fn write_csv(path: &str, summaries: &[BatchSummary]) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    writeln!(
        writer,
        "round_type,threads,queries,successes,success_rate,wall_s,qps,mean_ms,p50_ms,p95_ms,p99_ms,avg_result_count,max_result_count,checksum"
    )?;
    for summary in summaries {
        writeln!(
            writer,
            "{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{},{}",
            csv_escape(&summary.round_type),
            summary.threads,
            summary.queries,
            summary.successes,
            summary.success_rate,
            summary.wall_s,
            summary.qps,
            summary.mean_ms,
            summary.p50_ms,
            summary.p95_ms,
            summary.p99_ms,
            summary.avg_result_count,
            summary.max_result_count,
            summary.checksum
        )?;
    }
    Ok(())
}

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Arc::new(Args::parse());
    let thread_counts = parse_threads(&args.threads).map_err(Error::invalid_input)?;
    if args.k <= 0 {
        return Err(Error::invalid_input("--k must be greater than zero"));
    }
    if args.query_term_count == 0 {
        return Err(Error::invalid_input(
            "--query-term-count must be greater than zero",
        ));
    }
    if args.query_timeout_secs < 0.0 {
        return Err(Error::invalid_input(
            "--query-timeout-secs must be non-negative",
        ));
    }

    println!("opening dataset {}", args.uri);
    let dataset = Arc::new(open_dataset(&args).await?);
    println!("dataset version {}", dataset.version().version);

    let patterns = sample_patterns(&dataset, &args).await?;
    println!("sampled {} patterns", patterns.len());

    let explain_plan = if args.explain {
        Some(explain_first_query(&dataset, &patterns[0], &args).await?)
    } else {
        None
    };

    let prewarm_ms = if args.skip_prewarm {
        None
    } else {
        println!("prewarming index {}", args.index_name);
        let start = Instant::now();
        dataset.prewarm_index(&args.index_name).await?;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        println!("prewarm finished in {:.3} ms", elapsed_ms);
        Some(elapsed_ms)
    };

    let warmup_patterns = &patterns[..args.warmup];
    let query_patterns = &patterns[args.warmup..];
    let mut summaries = Vec::new();
    for thread_count in thread_counts.iter().copied() {
        if !warmup_patterns.is_empty() {
            summaries.push(
                run_batch(
                    Arc::clone(&dataset),
                    Arc::clone(&args),
                    warmup_patterns,
                    thread_count,
                    "warmup",
                )
                .await,
            );
        }
        let summary = run_batch(
            Arc::clone(&dataset),
            Arc::clone(&args),
            query_patterns,
            thread_count,
            "test",
        )
        .await;
        println!(
            "test threads={} queries={} successes={} wall_s={:.3} qps={:.3} mean_ms={:.3} p95_ms={:.3}",
            summary.threads,
            summary.queries,
            summary.successes,
            summary.wall_s,
            summary.qps,
            summary.mean_ms,
            summary.p95_ms
        );
        summaries.push(summary);
    }

    let output = BenchmarkOutput {
        uri: args.uri.clone(),
        index_name: args.index_name.clone(),
        text_column: args.text_column.clone(),
        pattern_source_column: args.pattern_source_column.clone(),
        sample_rows: args.sample_rows,
        query_term_count: args.query_term_count,
        k: args.k,
        warmup: args.warmup,
        queries: args.queries,
        threads: thread_counts,
        query_timeout_secs: args.query_timeout_secs,
        index_cache_bytes: args.index_cache_bytes,
        metadata_cache_bytes: args.metadata_cache_bytes,
        prewarm_ms,
        explain_plan,
        summaries,
    };

    let mut json = serde_json::to_string_pretty(&output)?;
    json.push('\n');
    std::fs::write(&args.output_json, json)?;
    write_csv(&args.output_csv, &output.summaries)?;
    Ok(())
}
