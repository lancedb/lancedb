// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Standalone CLI benchmark for KNN vector search across LSM levels with
//! recall verification.
//!
//! Uses real embeddings from the `lance-format/fineweb-edu` HuggingFace
//! dataset (384-dim) with an IVF-RQ index on the base table, then ingests
//! additional rows through ShardWriter to populate SSTables and
//! an active memtable.
//!
//! Three phases, selected with `--phase`:
//!
//!   --phase prepare   Load embeddings from HF, write the base dataset, create
//!                     an IVF-RQ index, and initialize MemWAL.
//!   --phase search    Ingest rows across LSM levels via ShardWriter, then run
//!                     KNN queries through LsmVectorSearchPlanner with recall
//!                     verification against brute-force ground truth.
//!   --phase baseline  Run the same KNN queries against the base dataset
//!                     (no memtables) through the standard Scanner API.
//!
//! Example:
//!
//! ```bash
//! cargo bench -p lance --bench mem_wal_vector_bench -- \
//!   --phase prepare --uri /tmp/vec_bench \
//!   --base-rows 100000 --ivf-partitions 64
//!
//! cargo bench -p lance --bench mem_wal_vector_bench -- \
//!   --phase search --uri /tmp/vec_bench \
//!   --base-rows 100000 --max-memtable-rows 10000 \
//!   --queries 100 --k 10 --nprobes 20 --output result.json
//! ```

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::cast::AsArray;
use arrow_array::{
    ArrayRef, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, RecordBatchIterator,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use lance::dataset::mem_wal::scanner::{
    LsmDataSourceCollector, LsmVectorSearchPlanner, ShardSnapshot,
};
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriterConfig};
use lance::dataset::{Dataset, WriteParams};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_core::Result;
use lance_index::IndexType;
use lance_linalg::distance::DistanceType;
use serde_json::json;
use uuid::Uuid;

const VECTOR_DIM: usize = 384;
const VECTOR_COL: &str = "embedding";
const INDEX_NAME: &str = "embedding_ivf_rq";

// ----------------------------------------------------------------------
// Phase / Args
// ----------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Prepare,
    Search,
    Baseline,
}

impl Phase {
    fn parse(value: &str) -> std::result::Result<Self, String> {
        match value {
            "prepare" => Ok(Self::Prepare),
            "search" => Ok(Self::Search),
            "baseline" => Ok(Self::Baseline),
            _ => Err(format!(
                "unknown phase '{value}', expected prepare|search|baseline"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Prepare => "prepare",
            Self::Search => "search",
            Self::Baseline => "baseline",
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
    nprobes: usize,
    ivf_partitions: usize,
    rq_bits: u8,
    refine_factor: u32,
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
            queries: 100,
            k: 10,
            nprobes: 20,
            ivf_partitions: 256,
            rq_bits: 8,
            refine_factor: 0,
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
            "--k" => args.k = parse_val(&flag, &value)?,
            "--nprobes" => args.nprobes = parse_val(&flag, &value)?,
            "--ivf-partitions" => args.ivf_partitions = parse_val(&flag, &value)?,
            "--rq-bits" => args.rq_bits = parse_val(&flag, &value)?,
            "--refine-factor" => args.refine_factor = parse_val(&flag, &value)?,
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
            "--phase is required (prepare|search|baseline)",
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
        Field::new(
            VECTOR_COL,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                VECTOR_DIM as i32,
            ),
            true,
        ),
    ]))
}

/// Prepend a sequential `id` column to an embedding-only batch.
fn prepend_id(batch: &RecordBatch, start_id: i64) -> RecordBatch {
    let n = batch.num_rows();
    let ids: Vec<i64> = (start_id..start_id + n as i64).collect();
    let id_arr: ArrayRef = Arc::new(Int64Array::from(ids));
    let emb_col = batch.column_by_name(VECTOR_COL).unwrap().clone();
    RecordBatch::try_new(make_schema(), vec![id_arr, emb_col]).unwrap()
}

fn wrap_query(values: &[f32], dim: usize) -> FixedSizeListArray {
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dim as i32);
    for v in values {
        builder.values().append_value(*v);
    }
    builder.append(true);
    builder.finish()
}

// ----------------------------------------------------------------------
// Distance / recall helpers
// ----------------------------------------------------------------------

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    1.0 - dot / (norm_a * norm_b + 1e-10)
}

/// Brute-force top-k using cosine distance. Returns the IDs of the k closest.
fn brute_force_topk(
    query: &[f32],
    all_ids: &[i64],
    all_vectors: &[Vec<f32>],
    k: usize,
) -> Vec<i64> {
    let mut scored: Vec<(i64, f32)> = all_ids
        .iter()
        .zip(all_vectors)
        .map(|(&id, vec)| (id, cosine_distance(query, vec)))
        .collect();
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(k);
    scored.into_iter().map(|(id, _)| id).collect()
}

fn compute_recall(result_ids: &[i64], truth_ids: &[i64]) -> f64 {
    let truth_set: HashSet<i64> = truth_ids.iter().copied().collect();
    let hits = result_ids
        .iter()
        .filter(|id| truth_set.contains(id))
        .count();
    hits as f64 / truth_ids.len().max(1) as f64
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

fn compute_latency_stats(mut latencies_us: Vec<f64>) -> LatencyStats {
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
// Extract embeddings from RecordBatches
// ----------------------------------------------------------------------

fn extract_embeddings(batches: &[RecordBatch]) -> (Vec<i64>, Vec<Vec<f32>>) {
    let mut ids = Vec::new();
    let mut vecs = Vec::new();
    for batch in batches {
        let id_col = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .expect("id column");
        let emb_col = batch.column_by_name(VECTOR_COL).expect("embedding column");
        let fsl = emb_col.as_fixed_size_list();
        for i in 0..batch.num_rows() {
            ids.push(id_col.value(i));
            let values = fsl.value(i);
            let f32_arr = values.as_any().downcast_ref::<Float32Array>().unwrap();
            vecs.push(f32_arr.values().to_vec());
        }
    }
    (ids, vecs)
}

fn extract_result_ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut ids = Vec::new();
    for batch in batches {
        let id_col = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        if let Some(arr) = id_col {
            for i in 0..arr.len() {
                ids.push(arr.value(i));
            }
        }
    }
    ids
}

// ----------------------------------------------------------------------
// Synthetic embedding generation
// ----------------------------------------------------------------------

/// Generate `count` synthetic 384-dim embedding vectors starting at
/// logical offset `offset`. Uses a deterministic cluster+noise scheme
/// (same approach as mem_wal_hnsw_bench.rs) so vectors are clustered
/// enough for IVF to be meaningful but noisy enough for recall to be
/// non-trivial.
const NUM_CLUSTERS: usize = 1024;
const NOISE: f32 = 0.05;
const SEED: u64 = 42;

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn unit_f32(key: u64) -> f32 {
    let bits = splitmix64(key) >> 40;
    (bits as f32) * (1.0 / 16_777_216.0)
}

fn generate_vector(row: usize) -> Vec<f32> {
    let cluster = row % NUM_CLUSTERS;
    (0..VECTOR_DIM)
        .map(|col| {
            let base_key = SEED
                ^ (cluster as u64).wrapping_mul(0xbf58_476d_1ce4_e5b9)
                ^ (col as u64).wrapping_mul(0x94d0_49bb_1331_11eb);
            let noise_key = SEED
                ^ (row as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
                ^ (col as u64).wrapping_mul(0xd2b7_4407_b1ce_6e93);
            let base = unit_f32(base_key) * 2.0 - 1.0;
            let noise = (unit_f32(noise_key) * 2.0 - 1.0) * NOISE;
            base + noise
        })
        .collect()
}

fn generate_embedding_batches(offset: usize, count: usize, batch_size: usize) -> Vec<RecordBatch> {
    let field = Arc::new(Field::new("item", DataType::Float32, true));
    let fsl_type = DataType::FixedSizeList(field, VECTOR_DIM as i32);
    let emb_field = Arc::new(Field::new(VECTOR_COL, fsl_type, false));
    let schema = Arc::new(ArrowSchema::new(vec![emb_field]));

    let mut batches = Vec::new();
    let mut cursor = offset;
    let end = offset + count;
    while cursor < end {
        let n = batch_size.min(end - cursor);
        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), VECTOR_DIM as i32);
        for row in cursor..(cursor + n) {
            let vec = generate_vector(row);
            for v in &vec {
                builder.values().append_value(*v);
            }
            builder.append(true);
        }
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.finish()) as ArrayRef])
                .unwrap();
        batches.push(batch);
        cursor += n;
    }
    batches
}

fn is_cloud_uri(uri: &str) -> bool {
    uri.starts_with("s3://") || uri.starts_with("gs://") || uri.starts_with("az://")
}

// ----------------------------------------------------------------------
// Prepare phase
// ----------------------------------------------------------------------

async fn run_prepare(args: &Args) -> Result<()> {
    println!(
        "generating {} synthetic {}-dim embeddings ...",
        args.base_rows, VECTOR_DIM
    );
    let start = Instant::now();
    let raw_batches = generate_embedding_batches(0, args.base_rows, args.batch_rows);
    println!(
        "  generated {} batches in {:.1}s",
        raw_batches.len(),
        start.elapsed().as_secs_f64()
    );

    let schema = make_schema();
    let mut id_cursor: i64 = 0;
    let mut batches_with_id = Vec::with_capacity(raw_batches.len());
    for batch in &raw_batches {
        let with_id = prepend_id(batch, id_cursor);
        id_cursor += batch.num_rows() as i64;
        batches_with_id.push(Ok(with_id));
    }
    println!("  total rows with IDs: {id_cursor}");

    let write_start = Instant::now();
    let reader = RecordBatchIterator::new(batches_with_id.into_iter(), schema);
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
        "  wrote base dataset in {:.1}s",
        write_start.elapsed().as_secs_f64()
    );

    let index_start = Instant::now();
    let params = VectorIndexParams::ivf_rq(args.ivf_partitions, args.rq_bits, DistanceType::Cosine);
    dataset
        .create_index(
            &[VECTOR_COL],
            IndexType::IvfRq,
            Some(INDEX_NAME.to_string()),
            &params,
            true,
        )
        .await?;
    println!(
        "  created IVF-RQ index in {:.1}s (partitions={}, bits={})",
        index_start.elapsed().as_secs_f64(),
        args.ivf_partitions,
        args.rq_bits,
    );

    dataset
        .initialize_mem_wal()
        .maintained_indexes([INDEX_NAME])
        .execute()
        .await?;
    println!("  initialized MemWAL with maintained index: {INDEX_NAME}");
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

async fn run_search(args: &Args) -> Result<serde_json::Value> {
    let dataset = Arc::new(Dataset::open(&args.uri).await?);
    let arrow_schema: Arc<ArrowSchema> = Arc::new(ArrowSchema::from(dataset.schema()));
    let schema = make_schema();

    let shard_id = Uuid::new_v4();
    let row_bytes = VECTOR_DIM * 4 + 8; // embedding + id
    let config = ShardWriterConfig {
        shard_id,
        max_wal_persist_retries: 3,
        wal_persist_retry_base_delay: std::time::Duration::from_millis(50),
        shard_spec_id: 0,
        durable_write: false,
        max_memtable_size: args.max_memtable_rows * row_bytes * 2,
        max_memtable_rows: args.max_memtable_rows,
        max_unflushed_memtable_bytes: args.max_memtable_rows * row_bytes * 6,
        max_wal_flush_interval: Some(Duration::from_secs(60)),
        ..ShardWriterConfig::default()
    };
    let writer = dataset.mem_wal_writer(shard_id, config).await?;

    let flush_wait = if is_cloud_uri(&args.uri) {
        Duration::from_secs(5)
    } else {
        Duration::from_millis(500)
    };

    // Ingest N SSTables + 1 active (50% full)
    let num_sstable_target = args.sstables;
    let active_rows = args.max_memtable_rows / 2;
    let total_memtable_rows = num_sstable_target * args.max_memtable_rows + active_rows;
    let mut gen_sizes: Vec<usize> = (0..num_sstable_target)
        .map(|_| args.max_memtable_rows)
        .collect();
    gen_sizes.push(active_rows);

    println!(
        "generating {} synthetic memtable embeddings ...",
        total_memtable_rows
    );
    let mt_batches =
        generate_embedding_batches(args.base_rows, total_memtable_rows, args.batch_rows);

    // Flatten the memtable embeddings for ground truth computation later
    let (mt_all_ids_raw, mt_all_vecs) = {
        let mut all_ids = Vec::new();
        let mut all_vecs = Vec::new();
        let mut id_cursor = args.base_rows as i64;
        for batch in &mt_batches {
            let emb_col = batch.column_by_name(VECTOR_COL).expect("embedding column");
            let fsl = emb_col.as_fixed_size_list();
            for i in 0..batch.num_rows() {
                all_ids.push(id_cursor);
                let values = fsl.value(i);
                let f32_arr = values.as_any().downcast_ref::<Float32Array>().unwrap();
                all_vecs.push(f32_arr.values().to_vec());
                id_cursor += 1;
            }
        }
        (all_ids, all_vecs)
    };

    // Ingest through ShardWriter
    let mut row_offset = 0usize;
    let mut id_cursor = args.base_rows as i64;
    let ingest_start = Instant::now();
    for (gen_idx, &gen_rows) in gen_sizes.iter().enumerate() {
        let mut rows_written = 0usize;
        // Walk through raw batches, slicing as needed
        while rows_written < gen_rows {
            let remaining = gen_rows - rows_written;
            let chunk_rows = remaining.min(args.batch_rows);
            // Build a batch from the flat vectors
            let end = (row_offset + chunk_rows).min(mt_all_ids_raw.len());
            let actual = end - row_offset;
            if actual == 0 {
                break;
            }
            let ids: Vec<i64> = (id_cursor..id_cursor + actual as i64).collect();
            let mut fsl_builder =
                FixedSizeListBuilder::new(Float32Builder::new(), VECTOR_DIM as i32);
            for vec in &mt_all_vecs[row_offset..end] {
                for &v in vec {
                    fsl_builder.values().append_value(v);
                }
                fsl_builder.append(true);
            }
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids)) as ArrayRef,
                    Arc::new(fsl_builder.finish()) as ArrayRef,
                ],
            )
            .unwrap();

            writer.put(vec![batch]).await?;
            row_offset += actual;
            id_cursor += actual as i64;
            rows_written += actual;
        }

        println!(
            "  gen {}: wrote {} rows in {:.1}s",
            gen_idx + 1,
            gen_rows,
            ingest_start.elapsed().as_secs_f64(),
        );
        if gen_idx < num_sstable_target {
            tokio::time::sleep(flush_wait).await;
        }
    }
    println!(
        "ingested {} total memtable rows ({} SSTables + active) in {:.1}s",
        total_memtable_rows,
        num_sstable_target,
        ingest_start.elapsed().as_secs_f64(),
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
    println!(
        "manifest: {} SSTables, current_generation={}",
        num_sstables,
        manifest.as_ref().map(|m| m.current_generation).unwrap_or(0)
    );

    // Build the LsmVectorSearchPlanner
    let collector = LsmDataSourceCollector::new(dataset.clone(), vec![shard_snapshot])
        .with_in_memory_memtables(shard_id, in_memory_refs);
    let pk_columns = vec!["id".to_string()];
    let planner = LsmVectorSearchPlanner::new(
        collector,
        pk_columns,
        arrow_schema.clone(),
        VECTOR_COL.to_string(),
        DistanceType::Cosine,
    )
    .with_dataset(dataset.clone());

    // Pick query vectors: first `queries` embeddings from the base table
    println!("loading {} query vectors from base table ...", args.queries);
    let mut q_scanner = dataset.scan();
    q_scanner.project(&["id", VECTOR_COL])?;
    q_scanner.limit(Some(args.queries as i64), None)?;
    let q_batches: Vec<RecordBatch> = q_scanner.try_into_stream().await?.try_collect().await?;
    let (_, query_vecs) = extract_embeddings(&q_batches);

    // Ground truth computation (expensive — cap at 20 queries)
    let recall_queries = args.queries.min(20);
    println!("computing brute-force ground truth for {recall_queries} queries ...");
    let gt_start = Instant::now();

    // Scan all base embeddings for ground truth
    let mut base_scanner = dataset.scan();
    base_scanner.project(&["id", VECTOR_COL])?;
    let base_batches: Vec<RecordBatch> =
        base_scanner.try_into_stream().await?.try_collect().await?;
    let (base_ids, base_vecs) = extract_embeddings(&base_batches);

    // Combine base + memtable vectors for full corpus
    let mut all_ids: Vec<i64> = base_ids;
    let mut all_vecs: Vec<Vec<f32>> = base_vecs;
    all_ids.extend_from_slice(&mt_all_ids_raw);
    all_vecs.extend(mt_all_vecs.iter().cloned());

    let mut ground_truth: Vec<Vec<i64>> = Vec::with_capacity(recall_queries);
    for qv in query_vecs.iter().take(recall_queries) {
        let gt = brute_force_topk(qv, &all_ids, &all_vecs, args.k);
        ground_truth.push(gt);
    }
    println!(
        "  ground truth computed in {:.1}s for {} queries over {} total vectors",
        gt_start.elapsed().as_secs_f64(),
        recall_queries,
        all_ids.len(),
    );

    // Run KNN searches
    println!(
        "running {} KNN queries (k={}, nprobes={}) ...",
        args.queries, args.k, args.nprobes
    );
    let ctx = SessionContext::new();
    let mut latencies_us = Vec::with_capacity(args.queries);

    let refine_base_table = args.refine_factor > 0;

    let mut all_result_ids: Vec<Vec<i64>> = Vec::with_capacity(args.queries);
    for (qi, qv) in query_vecs.iter().enumerate().take(args.queries) {
        let fsl = wrap_query(qv, VECTOR_DIM);
        let t0 = Instant::now();
        // overfetch_factor 0.0 leaves stale filtering off (raw search, as before).
        let plan = planner
            .plan_search(&fsl, args.k, args.nprobes, None, refine_base_table, 0.0)
            .await?;
        let stream = plan.execute(0, ctx.task_ctx())?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let elapsed_us = t0.elapsed().as_micros() as f64;
        latencies_us.push(elapsed_us);

        let result_ids = extract_result_ids(&batches);
        if qi < 3 {
            println!(
                "  [q{qi}] {} results in {elapsed_us:.0}us, ids={:?}",
                result_ids.len(),
                &result_ids[..result_ids.len().min(5)]
            );
        }
        all_result_ids.push(result_ids);
    }

    // Compute recall
    let mut recall_sum = 0.0f64;
    for (gt, res) in ground_truth.iter().zip(all_result_ids.iter()) {
        let r = compute_recall(res, gt);
        recall_sum += r;
    }
    let avg_recall = recall_sum / recall_queries.max(1) as f64;

    let stats = compute_latency_stats(latencies_us);
    println!(
        "results: recall@{}={:.4} p50={p50}us p95={p95}us p99={p99}us mean={mean:.0}us qps={qps:.1}",
        args.k,
        avg_recall,
        p50 = stats.p50_us,
        p95 = stats.p95_us,
        p99 = stats.p99_us,
        mean = stats.mean_us,
        qps = stats.qps,
    );

    // Keep writer alive so active memtable stays reachable
    std::mem::forget(writer);

    let active_rows = args.max_memtable_rows / 2;
    Ok(json!({
        "bench": "mem_wal_vector",
        "phase": "search",
        "base_rows": args.base_rows,
        "max_memtable_rows": args.max_memtable_rows,
        "sstables": num_sstables,
        "active_rows": active_rows,
        "vector_dim": VECTOR_DIM,
        "k": args.k,
        "nprobes": args.nprobes,
        "ivf_partitions": args.ivf_partitions,
        "rq_bits": args.rq_bits,
        "refine_factor": args.refine_factor,
        "queries": args.queries,
        "recall_queries": recall_queries,
        "recall_at_k": avg_recall,
        "latency_p50_us": stats.p50_us,
        "latency_p95_us": stats.p95_us,
        "latency_p99_us": stats.p99_us,
        "latency_mean_us": stats.mean_us as u64,
        "qps": stats.qps,
    }))
}

// ----------------------------------------------------------------------
// Baseline phase
// ----------------------------------------------------------------------

async fn run_baseline(args: &Args) -> Result<serde_json::Value> {
    let dataset = Arc::new(Dataset::open(&args.uri).await?);

    // Pick query vectors
    println!("loading {} query vectors from base table ...", args.queries);
    let mut q_scanner = dataset.scan();
    q_scanner.project(&["id", VECTOR_COL])?;
    q_scanner.limit(Some(args.queries as i64), None)?;
    let q_batches: Vec<RecordBatch> = q_scanner.try_into_stream().await?.try_collect().await?;
    let (_, query_vecs) = extract_embeddings(&q_batches);

    // Ground truth
    let recall_queries = args.queries.min(20);
    println!("computing brute-force ground truth for {recall_queries} queries ...");
    let gt_start = Instant::now();
    let mut base_scanner = dataset.scan();
    base_scanner.project(&["id", VECTOR_COL])?;
    let base_batches: Vec<RecordBatch> =
        base_scanner.try_into_stream().await?.try_collect().await?;
    let (base_ids, base_vecs) = extract_embeddings(&base_batches);

    let mut ground_truth: Vec<Vec<i64>> = Vec::with_capacity(recall_queries);
    for qv in query_vecs.iter().take(recall_queries) {
        let gt = brute_force_topk(qv, &base_ids, &base_vecs, args.k);
        ground_truth.push(gt);
    }
    println!(
        "  ground truth computed in {:.1}s",
        gt_start.elapsed().as_secs_f64(),
    );

    // Run baseline KNN through Scanner
    println!(
        "running {} baseline KNN queries (k={}, nprobes={}) ...",
        args.queries, args.k, args.nprobes
    );
    let mut latencies_us = Vec::with_capacity(args.queries);
    let mut all_result_ids: Vec<Vec<i64>> = Vec::with_capacity(args.queries);

    for (qi, qv) in query_vecs.iter().enumerate().take(args.queries) {
        let fsl = wrap_query(qv, VECTOR_DIM);
        let query_arr = fsl.value(0);
        let t0 = Instant::now();
        let mut scanner = dataset.scan();
        scanner.nearest(VECTOR_COL, query_arr.as_ref(), args.k)?;
        scanner.nprobes(args.nprobes);
        scanner.distance_metric(DistanceType::Cosine);
        let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
        let elapsed_us = t0.elapsed().as_micros() as f64;
        latencies_us.push(elapsed_us);

        let result_ids = extract_result_ids(&batches);
        if qi < 3 {
            println!(
                "  [q{qi}] {} results in {elapsed_us:.0}us, ids={:?}",
                result_ids.len(),
                &result_ids[..result_ids.len().min(5)]
            );
        }
        all_result_ids.push(result_ids);
    }

    // Recall
    let mut recall_sum = 0.0f64;
    for (gt, res) in ground_truth.iter().zip(all_result_ids.iter()) {
        let r = compute_recall(res, gt);
        recall_sum += r;
    }
    let avg_recall = recall_sum / recall_queries.max(1) as f64;

    let stats = compute_latency_stats(latencies_us);
    println!(
        "baseline: recall@{}={:.4} p50={p50}us p95={p95}us p99={p99}us mean={mean:.0}us qps={qps:.1}",
        args.k,
        avg_recall,
        p50 = stats.p50_us,
        p95 = stats.p95_us,
        p99 = stats.p99_us,
        mean = stats.mean_us,
        qps = stats.qps,
    );

    Ok(json!({
        "bench": "mem_wal_vector",
        "phase": "baseline",
        "base_rows": args.base_rows,
        "vector_dim": VECTOR_DIM,
        "k": args.k,
        "nprobes": args.nprobes,
        "queries": args.queries,
        "recall_queries": recall_queries,
        "recall_at_k": avg_recall,
        "latency_p50_us": stats.p50_us,
        "latency_p95_us": stats.p95_us,
        "latency_p99_us": stats.p99_us,
        "latency_mean_us": stats.mean_us as u64,
        "qps": stats.qps,
    }))
}

// ----------------------------------------------------------------------
// Entrypoint
// ----------------------------------------------------------------------

async fn run(args: Args) -> Result<()> {
    println!(
        "bench=mem_wal_vector phase={} uri={} base_rows={} max_memtable_rows={} \
         batch_rows={} queries={} k={} nprobes={} ivf_partitions={} rq_bits={} refine_factor={}",
        args.phase.as_str(),
        args.uri,
        args.base_rows,
        args.max_memtable_rows,
        args.batch_rows,
        args.queries,
        args.k,
        args.nprobes,
        args.ivf_partitions,
        args.rq_bits,
        args.refine_factor,
    );

    match args.phase {
        Phase::Prepare => {
            run_prepare(&args).await?;
        }
        Phase::Search | Phase::Baseline => {
            let result = if args.phase == Phase::Search {
                run_search(&args).await?
            } else {
                run_baseline(&args).await?
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
