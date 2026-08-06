// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark comparing read performance between MemTable (with MemTableScanner)
//! and in-memory Lance tables.
//!
//! This benchmark tests different read operations:
//!
//! 1. **Scan**: Full table scan returning all rows
//! 2. **Point Lookup**: Scalar index lookup by primary key (BTree index)
//! 3. **Full-Text Search**: Token-based text search (FTS index)
//! 4. **Vector Search**: IVF-PQ vector similarity search
//!
//! ## Running the benchmark
//!
//! ```bash
//! cargo bench --bench mem_read_benchmark
//! ```
//!
//! ## Configuration
//!
//! - `NUM_ROWS`: Total number of rows (default: 10000)
//! - `BATCH_SIZE`: Number of rows per batch (default: 100)
//! - `VECTOR_DIM`: Vector dimension (default: 128)
//! - `SAMPLE_SIZE`: Number of benchmark iterations (default: 100)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::sync::Arc;

use arrow_array::{
    FixedSizeListArray, Float32Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::TryStreamExt;
use lance::dataset::mem_wal::write::{CacheConfig, IndexStore, MemTable};
use lance::dataset::{Dataset, WriteParams};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_arrow::FixedSizeListArrayExt;
use lance_index::IndexType;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::builder::PQBuildParams;
use lance_linalg::distance::{DistanceType, MetricType};
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};
use rand::Rng;
use uuid::Uuid;

const DEFAULT_NUM_ROWS: usize = 10000;
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_VECTOR_DIM: usize = 128;
const DEFAULT_NUM_LOOKUPS: usize = 100;
const DEFAULT_K: usize = 10;

fn get_num_rows() -> usize {
    std::env::var("NUM_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_NUM_ROWS)
}

fn get_batch_size() -> usize {
    std::env::var("BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BATCH_SIZE)
}

fn get_vector_dim() -> usize {
    std::env::var("VECTOR_DIM")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_VECTOR_DIM)
}

fn get_sample_size() -> usize {
    std::env::var("SAMPLE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
        .max(10)
}

/// Create schema: (id: Int64, text: Utf8, vector: FixedSizeList<Float32>[dim])
fn create_schema(vector_dim: usize) -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, true),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                vector_dim as i32,
            ),
            false,
        ),
    ]))
}

/// Create a test batch with given parameters.
fn create_batch(
    schema: &ArrowSchema,
    start_id: i64,
    num_rows: usize,
    vector_dim: usize,
) -> RecordBatch {
    let mut rng = rand::rng();

    // Create IDs
    let ids: Vec<i64> = (start_id..start_id + num_rows as i64).collect();

    // Create text with some common words for FTS
    let words = [
        "hello",
        "world",
        "search",
        "benchmark",
        "lance",
        "memory",
        "test",
        "data",
    ];
    let texts: Vec<String> = (0..num_rows)
        .map(|i| {
            let w1 = words[i % words.len()];
            let w2 = words[(i + 3) % words.len()];
            let w3 = words[(i + 5) % words.len()];
            format!("{} {} {} row_{}", w1, w2, w3, start_id + i as i64)
        })
        .collect();

    // Create vectors (normalized random)
    let vectors: Vec<f32> = (0..num_rows)
        .flat_map(|_| {
            let v: Vec<f32> = (0..vector_dim).map(|_| rng.random::<f32>() - 0.5).collect();
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            v.into_iter().map(move |x| x / norm)
        })
        .collect();

    let vector_array =
        FixedSizeListArray::try_new_from_values(Float32Array::from(vectors), vector_dim as i32)
            .unwrap();

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(texts)),
            Arc::new(vector_array),
        ],
    )
    .unwrap()
}

/// Create a query vector (normalized random).
fn create_query_vector(vector_dim: usize) -> Vec<f32> {
    let mut rng = rand::rng();
    let v: Vec<f32> = (0..vector_dim).map(|_| rng.random::<f32>() - 0.5).collect();
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    v.into_iter().map(|x| x / norm).collect()
}

/// Generate random IDs for point lookups.
fn generate_random_ids(max_id: i64, count: usize) -> Vec<i64> {
    let mut rng = rand::rng();
    (0..count).map(|_| rng.random_range(0..max_id)).collect()
}

/// Setup MemTable with all indexes (BTree on id, FTS on text, HNSW on vector).
async fn setup_memtable(
    batches: Vec<RecordBatch>,
    _vector_dim: usize,
    _num_partitions: usize,
    _num_sub_vectors: usize,
) -> MemTable {
    let schema = batches[0].schema();
    let num_batches = batches.len();

    // Compute total rows for HNSW capacity sizing.
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Field IDs: id=0, text=1, vector=2
    let mut index_store = IndexStore::new();
    index_store.add_btree("id_idx".to_string(), 0, "id".to_string());
    index_store.add_fts("text_idx".to_string(), 1, "text".to_string());
    index_store.add_hnsw(
        "vector_idx".to_string(),
        2,
        "vector".to_string(),
        DistanceType::L2,
        total_rows.max(1),
        num_batches.max(1),
    );

    let batch_capacity = ((num_batches as f64) * 1.1) as usize;
    let mut memtable =
        MemTable::with_capacity(schema, 1, vec![0], CacheConfig::default(), batch_capacity)
            .unwrap();
    memtable.set_indexes(index_store);

    for batch in batches.into_iter() {
        memtable.insert(batch).await.unwrap();
    }

    memtable
}

/// Lance dataset wrapper.
struct LanceSetup {
    dataset: Arc<Dataset>,
    #[allow(dead_code)]
    total_rows: usize,
}

/// Create Lance dataset with a single fragment (all batches concatenated).
async fn setup_lance(batches: Vec<RecordBatch>) -> LanceSetup {
    let schema = batches[0].schema();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    let uri = format!("memory://lance_bench_{}", Uuid::new_v4());
    let write_params = WriteParams {
        data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
        max_rows_per_file: total_rows + 1,
        ..Default::default()
    };

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    let dataset = Dataset::write(reader, &uri, Some(write_params))
        .await
        .unwrap();

    LanceSetup {
        dataset: Arc::new(dataset),
        total_rows,
    }
}

/// Create Lance dataset with one fragment per batch.
async fn setup_lance_per_batch(batches: Vec<RecordBatch>, batch_size: usize) -> LanceSetup {
    let schema = batches[0].schema();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    let uri = format!("memory://lance_per_batch_{}", Uuid::new_v4());
    let write_params = WriteParams {
        data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
        max_rows_per_file: batch_size,
        ..Default::default()
    };

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    let dataset = Dataset::write(reader, &uri, Some(write_params))
        .await
        .unwrap();

    LanceSetup {
        dataset: Arc::new(dataset),
        total_rows,
    }
}

/// Create Lance dataset with FTS index on text column (single fragment).
async fn setup_lance_with_fts(batches: Vec<RecordBatch>) -> LanceSetup {
    let schema = batches[0].schema();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    let uri = format!("memory://lance_fts_bench_{}", Uuid::new_v4());
    let write_params = WriteParams {
        data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
        max_rows_per_file: total_rows + 1,
        ..Default::default()
    };

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(reader, &uri, Some(write_params))
        .await
        .unwrap();

    // Create FTS (inverted) index on text column
    let params = InvertedIndexParams::default();
    dataset
        .create_index(&["text"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();

    LanceSetup {
        dataset: Arc::new(dataset),
        total_rows,
    }
}

/// Create Lance dataset with FTS index on text column (per-batch fragments).
async fn setup_lance_per_batch_with_fts(
    batches: Vec<RecordBatch>,
    batch_size: usize,
) -> LanceSetup {
    let schema = batches[0].schema();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    let uri = format!("memory://lance_fts_per_batch_{}", Uuid::new_v4());
    let write_params = WriteParams {
        data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
        max_rows_per_file: batch_size,
        ..Default::default()
    };

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(reader, &uri, Some(write_params))
        .await
        .unwrap();

    // Create FTS (inverted) index on text column
    let params = InvertedIndexParams::default();
    dataset
        .create_index(&["text"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();

    LanceSetup {
        dataset: Arc::new(dataset),
        total_rows,
    }
}

/// Create Lance dataset with IVF-PQ vector index (single fragment).
async fn setup_lance_with_vector_index(
    batches: Vec<RecordBatch>,
    num_partitions: usize,
    num_sub_vectors: usize,
) -> LanceSetup {
    let schema = batches[0].schema();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    let uri = format!("memory://lance_vec_bench_{}", Uuid::new_v4());
    let write_params = WriteParams {
        data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
        max_rows_per_file: total_rows + 1,
        ..Default::default()
    };

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(reader, &uri, Some(write_params))
        .await
        .unwrap();

    // Create IVF-PQ index on vector column
    let ivf_params = IvfBuildParams {
        num_partitions: Some(num_partitions),
        ..Default::default()
    };
    let pq_params = PQBuildParams {
        num_sub_vectors,
        num_bits: 8,
        ..Default::default()
    };

    let vector_params =
        VectorIndexParams::with_ivf_pq_params(MetricType::L2, ivf_params, pq_params);

    dataset
        .create_index(&["vector"], IndexType::Vector, None, &vector_params, true)
        .await
        .unwrap();

    LanceSetup {
        dataset: Arc::new(dataset),
        total_rows,
    }
}

/// Create Lance dataset with IVF-PQ vector index (per-batch fragments).
async fn setup_lance_per_batch_with_vector_index(
    batches: Vec<RecordBatch>,
    batch_size: usize,
    num_partitions: usize,
    num_sub_vectors: usize,
) -> LanceSetup {
    let schema = batches[0].schema();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    let uri = format!("memory://lance_vec_per_batch_{}", Uuid::new_v4());
    let write_params = WriteParams {
        data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
        max_rows_per_file: batch_size,
        ..Default::default()
    };

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(reader, &uri, Some(write_params))
        .await
        .unwrap();

    // Create IVF-PQ index on vector column
    let ivf_params = IvfBuildParams {
        num_partitions: Some(num_partitions),
        ..Default::default()
    };
    let pq_params = PQBuildParams {
        num_sub_vectors,
        num_bits: 8,
        ..Default::default()
    };

    let vector_params =
        VectorIndexParams::with_ivf_pq_params(MetricType::L2, ivf_params, pq_params);

    dataset
        .create_index(&["vector"], IndexType::Vector, None, &vector_params, true)
        .await
        .unwrap();

    LanceSetup {
        dataset: Arc::new(dataset),
        total_rows,
    }
}

/// Benchmark scan operations.
fn bench_scan(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let num_rows = get_num_rows();
    let batch_size = get_batch_size();
    let vector_dim = get_vector_dim();
    let sample_size = get_sample_size();

    let num_batches = num_rows.div_ceil(batch_size);
    let schema = create_schema(vector_dim);

    println!("=== Scan Benchmark ===");
    println!("Num rows: {}", num_rows);
    println!("Batch size: {}", batch_size);
    println!("Num batches: {}", num_batches);
    println!();

    // Generate test data
    let batches: Vec<RecordBatch> = (0..num_batches)
        .map(|i| {
            let start_id = (i * batch_size) as i64;
            let rows = batch_size.min(num_rows - i * batch_size);
            create_batch(&schema, start_id, rows, vector_dim)
        })
        .collect();

    // Setup Lance (single fragment)
    let lance_setup = rt.block_on(setup_lance(batches.clone()));
    println!(
        "Lance (single fragment): {} fragments",
        lance_setup.dataset.get_fragments().len()
    );

    // Setup Lance (per-batch fragments)
    let lance_per_batch_setup = rt.block_on(setup_lance_per_batch(batches.clone(), batch_size));
    println!(
        "Lance (per-batch): {} fragments",
        lance_per_batch_setup.dataset.get_fragments().len()
    );

    // Setup MemTable with indexes
    let num_partitions = (num_rows / 100).clamp(4, 256);
    let num_sub_vectors = (vector_dim / 8).clamp(4, 32);
    println!("Creating MemTable with indexes...");
    let memtable = rt.block_on(setup_memtable(
        batches,
        vector_dim,
        num_partitions,
        num_sub_vectors,
    ));
    println!(
        "MemTable created with {} rows",
        memtable.batch_store().total_rows()
    );

    let mut group = c.benchmark_group("Scan");
    group.throughput(Throughput::Elements(num_rows as u64));
    group.sample_size(sample_size);

    let label = format!("{}_rows", num_rows);

    // MemTable scan using MemTableScanner
    group.bench_with_input(BenchmarkId::new("MemTable", &label), &(), |b, _| {
        b.to_async(&rt).iter(|| async {
            let batches: Vec<RecordBatch> = memtable
                .scan()
                .try_into_stream()
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert!(total > 0);
        });
    });

    // Lance scan (single fragment)
    group.bench_with_input(
        BenchmarkId::new("Lance_SingleFragment", &label),
        &(),
        |b, _| {
            let dataset = lance_setup.dataset.clone();
            b.to_async(&rt).iter(|| async {
                let batches: Vec<RecordBatch> = dataset
                    .scan()
                    .try_into_stream()
                    .await
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert!(total > 0);
            });
        },
    );

    // Lance scan (per-batch fragments)
    group.bench_with_input(
        BenchmarkId::new("Lance_PerBatchFragment", &label),
        &(),
        |b, _| {
            let dataset = lance_per_batch_setup.dataset.clone();
            b.to_async(&rt).iter(|| async {
                let batches: Vec<RecordBatch> = dataset
                    .scan()
                    .try_into_stream()
                    .await
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert!(total > 0);
            });
        },
    );

    group.finish();
}

/// Benchmark point lookup operations.
/// Uses individual equality lookups rather than large IN clauses to avoid
/// DataFusion FilterExec issues with large IN expressions.
fn bench_point_lookup(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let num_rows = get_num_rows();
    let batch_size = get_batch_size();
    let vector_dim = get_vector_dim();
    let sample_size = get_sample_size();
    let num_lookups = DEFAULT_NUM_LOOKUPS;

    let num_batches = num_rows.div_ceil(batch_size);
    let schema = create_schema(vector_dim);

    println!("=== Point Lookup Benchmark ===");
    println!("Num rows: {}", num_rows);
    println!("Num lookups: {}", num_lookups);
    println!();

    // Generate test data
    let batches: Vec<RecordBatch> = (0..num_batches)
        .map(|i| {
            let start_id = (i * batch_size) as i64;
            let rows = batch_size.min(num_rows - i * batch_size);
            create_batch(&schema, start_id, rows, vector_dim)
        })
        .collect();

    // Setup Lance (single fragment)
    let lance_setup = rt.block_on(setup_lance(batches.clone()));
    println!(
        "Lance (single fragment): {} fragments",
        lance_setup.dataset.get_fragments().len()
    );

    // Setup Lance (per-batch fragments)
    let lance_per_batch_setup = rt.block_on(setup_lance_per_batch(batches.clone(), batch_size));
    println!(
        "Lance (per-batch): {} fragments",
        lance_per_batch_setup.dataset.get_fragments().len()
    );

    // Setup MemTable with indexes
    let num_partitions = (num_rows / 100).clamp(4, 256);
    let num_sub_vectors = (vector_dim / 8).clamp(4, 32);
    println!("Creating MemTable with indexes...");
    let memtable = rt.block_on(setup_memtable(
        batches,
        vector_dim,
        num_partitions,
        num_sub_vectors,
    ));
    println!("MemTable created.");

    // Generate random lookup IDs
    let lookup_ids = generate_random_ids(num_rows as i64, num_lookups);

    let mut group = c.benchmark_group("PointLookup");
    group.throughput(Throughput::Elements(num_lookups as u64));
    group.sample_size(sample_size);

    let label = format!("{}_lookups", num_lookups);

    // MemTable point lookup using single IN clause (same as Lance)
    group.bench_with_input(
        BenchmarkId::new("MemTable_Filter", &label),
        &lookup_ids,
        |b, ids| {
            let id_list: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
            let filter = format!("id IN ({})", id_list.join(","));

            b.to_async(&rt).iter(|| {
                let filter = filter.clone();
                let mut scanner = memtable.scan();
                async move {
                    let batches: Vec<RecordBatch> = scanner
                        .filter(&filter)
                        .unwrap()
                        .try_into_stream()
                        .await
                        .unwrap()
                        .try_collect()
                        .await
                        .unwrap();
                    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                    assert!(total > 0);
                }
            });
        },
    );

    // Lance filter scan (single fragment) - uses IN clause
    group.bench_with_input(
        BenchmarkId::new("Lance_SingleFragment_Filter", &label),
        &lookup_ids,
        |b, ids| {
            let dataset = lance_setup.dataset.clone();
            let id_list: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
            let filter = format!("id IN ({})", id_list.join(","));

            b.to_async(&rt).iter(|| {
                let dataset = dataset.clone();
                let filter = filter.clone();
                async move {
                    let batches: Vec<RecordBatch> = dataset
                        .scan()
                        .filter(&filter)
                        .unwrap()
                        .try_into_stream()
                        .await
                        .unwrap()
                        .try_collect()
                        .await
                        .unwrap();
                    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                    assert!(total > 0);
                }
            });
        },
    );

    // Lance filter scan (per-batch fragments) - uses IN clause
    group.bench_with_input(
        BenchmarkId::new("Lance_PerBatchFragment_Filter", &label),
        &lookup_ids,
        |b, ids| {
            let dataset = lance_per_batch_setup.dataset.clone();
            let id_list: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
            let filter = format!("id IN ({})", id_list.join(","));

            b.to_async(&rt).iter(|| {
                let dataset = dataset.clone();
                let filter = filter.clone();
                async move {
                    let batches: Vec<RecordBatch> = dataset
                        .scan()
                        .filter(&filter)
                        .unwrap()
                        .try_into_stream()
                        .await
                        .unwrap()
                        .try_collect()
                        .await
                        .unwrap();
                    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                    assert!(total > 0);
                }
            });
        },
    );

    group.finish();
}

/// Benchmark FTS operations.
fn bench_fts(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let num_rows = get_num_rows();
    let batch_size = get_batch_size();
    let vector_dim = get_vector_dim();
    let sample_size = get_sample_size();

    let num_batches = num_rows.div_ceil(batch_size);
    let schema = create_schema(vector_dim);

    println!("=== FTS Benchmark ===");
    println!("Num rows: {}", num_rows);
    println!("Batch size: {}", batch_size);
    println!("Num batches: {}", num_batches);
    println!();

    // Generate test data
    let batches: Vec<RecordBatch> = (0..num_batches)
        .map(|i| {
            let start_id = (i * batch_size) as i64;
            let rows = batch_size.min(num_rows - i * batch_size);
            create_batch(&schema, start_id, rows, vector_dim)
        })
        .collect();

    // Setup Lance with FTS index (single fragment)
    println!("Creating Lance dataset with FTS index (single fragment)...");
    let lance_fts_setup = rt.block_on(setup_lance_with_fts(batches.clone()));
    println!(
        "Lance FTS (single fragment): {} fragments",
        lance_fts_setup.dataset.get_fragments().len()
    );

    // Setup Lance with FTS index (per-batch fragments)
    println!("Creating Lance dataset with FTS index (per-batch fragments)...");
    let lance_fts_per_batch_setup =
        rt.block_on(setup_lance_per_batch_with_fts(batches.clone(), batch_size));
    println!(
        "Lance FTS (per-batch): {} fragments",
        lance_fts_per_batch_setup.dataset.get_fragments().len()
    );

    // Setup MemTable with indexes
    let num_partitions = (num_rows / 100).clamp(4, 256);
    let num_sub_vectors = (vector_dim / 8).clamp(4, 32);
    println!("Creating MemTable with indexes...");
    let memtable = rt.block_on(setup_memtable(
        batches,
        vector_dim,
        num_partitions,
        num_sub_vectors,
    ));
    println!("MemTable created.");

    // Search terms (these are words we know exist in the data)
    let search_terms = ["hello", "world", "search", "benchmark", "lance"];

    let mut group = c.benchmark_group("FTS");
    group.throughput(Throughput::Elements(search_terms.len() as u64));
    group.sample_size(sample_size);

    let label = format!("{}_terms", search_terms.len());

    // MemTable FTS using MemTableScanner
    group.bench_with_input(
        BenchmarkId::new("MemTable_FTS", &label),
        &search_terms,
        |b, terms| {
            b.to_async(&rt).iter(|| {
                let terms = *terms;
                let scanners: Vec<_> = terms.iter().map(|_| memtable.scan()).collect();
                async move {
                    let mut total_found = 0usize;
                    for (mut scanner, term) in scanners.into_iter().zip(terms.iter()) {
                        scanner
                            .full_text_search(
                                FullTextSearchQuery::new(term.to_string())
                                    .with_column("text".to_string())
                                    .unwrap(),
                            )
                            .unwrap();
                        let batches: Vec<RecordBatch> = scanner
                            .try_into_stream()
                            .await
                            .unwrap()
                            .try_collect()
                            .await
                            .unwrap();
                        total_found += batches.iter().map(|b| b.num_rows()).sum::<usize>();
                    }
                    assert!(total_found > 0);
                }
            });
        },
    );

    // Lance FTS (single fragment)
    group.bench_with_input(
        BenchmarkId::new("Lance_SingleFragment_FTS", &label),
        &search_terms,
        |b, terms| {
            let dataset = lance_fts_setup.dataset.clone();
            b.to_async(&rt).iter(|| {
                let dataset = dataset.clone();
                let terms = terms.to_vec();
                async move {
                    let mut total_found = 0usize;
                    for term in terms {
                        let query = FullTextSearchQuery::new(term.to_string());
                        let batches: Vec<RecordBatch> = dataset
                            .scan()
                            .full_text_search(query)
                            .unwrap()
                            .try_into_stream()
                            .await
                            .unwrap()
                            .try_collect()
                            .await
                            .unwrap();
                        total_found += batches.iter().map(|b| b.num_rows()).sum::<usize>();
                    }
                    assert!(total_found > 0);
                }
            });
        },
    );

    // Lance FTS (per-batch fragments)
    group.bench_with_input(
        BenchmarkId::new("Lance_PerBatchFragment_FTS", &label),
        &search_terms,
        |b, terms| {
            let dataset = lance_fts_per_batch_setup.dataset.clone();
            b.to_async(&rt).iter(|| {
                let dataset = dataset.clone();
                let terms = terms.to_vec();
                async move {
                    let mut total_found = 0usize;
                    for term in terms {
                        let query = FullTextSearchQuery::new(term.to_string());
                        let batches: Vec<RecordBatch> = dataset
                            .scan()
                            .full_text_search(query)
                            .unwrap()
                            .try_into_stream()
                            .await
                            .unwrap()
                            .try_collect()
                            .await
                            .unwrap();
                        total_found += batches.iter().map(|b| b.num_rows()).sum::<usize>();
                    }
                    assert!(total_found > 0);
                }
            });
        },
    );

    group.finish();
}

/// Benchmark vector search operations.
fn bench_vector_search(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let num_rows = get_num_rows();
    let batch_size = get_batch_size();
    let vector_dim = get_vector_dim();
    let sample_size = get_sample_size();
    let k = DEFAULT_K;

    let num_batches = num_rows.div_ceil(batch_size);
    let schema = create_schema(vector_dim);

    println!("=== Vector Search Benchmark ===");
    println!("Num rows: {}", num_rows);
    println!("Batch size: {}", batch_size);
    println!("Num batches: {}", num_batches);
    println!("Vector dim: {}", vector_dim);
    println!("K: {}", k);
    println!();

    // Generate test data
    let batches: Vec<RecordBatch> = (0..num_batches)
        .map(|i| {
            let start_id = (i * batch_size) as i64;
            let rows = batch_size.min(num_rows - i * batch_size);
            create_batch(&schema, start_id, rows, vector_dim)
        })
        .collect();

    // Setup Lance with vector index (IVF-PQ) - single fragment
    let num_partitions = (num_rows / 100).clamp(4, 256);
    let num_sub_vectors = (vector_dim / 8).clamp(4, 32);
    println!(
        "Creating Lance dataset with IVF-PQ index (single fragment, partitions={}, sub_vectors={})...",
        num_partitions, num_sub_vectors
    );
    let lance_vec_setup = rt.block_on(setup_lance_with_vector_index(
        batches.clone(),
        num_partitions,
        num_sub_vectors,
    ));
    println!(
        "Lance IVF-PQ (single fragment): {} fragments",
        lance_vec_setup.dataset.get_fragments().len()
    );

    // Setup Lance with vector index (IVF-PQ) - per-batch fragments
    println!(
        "Creating Lance dataset with IVF-PQ index (per-batch fragments, partitions={}, sub_vectors={})...",
        num_partitions, num_sub_vectors
    );
    let lance_vec_per_batch_setup = rt.block_on(setup_lance_per_batch_with_vector_index(
        batches.clone(),
        batch_size,
        num_partitions,
        num_sub_vectors,
    ));
    println!(
        "Lance IVF-PQ (per-batch): {} fragments",
        lance_vec_per_batch_setup.dataset.get_fragments().len()
    );

    // Setup MemTable with IVF-PQ index
    println!(
        "Creating MemTable with IVF-PQ index (partitions={}, sub_vectors={})...",
        num_partitions, num_sub_vectors
    );
    let memtable = rt.block_on(setup_memtable(
        batches,
        vector_dim,
        num_partitions,
        num_sub_vectors,
    ));
    println!("MemTable IVF-PQ index created.");

    // Create query vector
    let query = create_query_vector(vector_dim);

    let mut group = c.benchmark_group("VectorSearch");
    group.throughput(Throughput::Elements(1));
    group.sample_size(sample_size);

    let label = format!("{}_rows_k{}", num_rows, k);

    // MemTable IVF-PQ vector search using MemTableScanner
    group.bench_with_input(
        BenchmarkId::new("MemTable_IVFPQ", &label),
        &query,
        |b, q| {
            let query_array: Arc<dyn arrow_array::Array> = Arc::new(Float32Array::from(q.clone()));
            b.to_async(&rt).iter(|| {
                let query_array = query_array.clone();
                let memtable = &memtable;
                async move {
                    let mut scanner = memtable.scan();
                    scanner
                        .nearest("vector", query_array.as_ref(), k)
                        .unwrap()
                        .nprobes(8);
                    let batches: Vec<RecordBatch> = scanner
                        .try_into_stream()
                        .await
                        .unwrap()
                        .try_collect()
                        .await
                        .unwrap();
                    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                    assert!(total > 0);
                }
            });
        },
    );

    // Lance IVF-PQ vector search (single fragment)
    group.bench_with_input(
        BenchmarkId::new("Lance_SingleFragment_IVFPQ", &label),
        &query,
        |b, q| {
            let dataset = lance_vec_setup.dataset.clone();
            let query_array = Float32Array::from(q.clone());
            b.to_async(&rt).iter(|| {
                let dataset = dataset.clone();
                let query_array = query_array.clone();
                async move {
                    let batches: Vec<RecordBatch> = dataset
                        .scan()
                        .nearest("vector", &query_array, k)
                        .unwrap()
                        .nprobes(8)
                        .try_into_stream()
                        .await
                        .unwrap()
                        .try_collect()
                        .await
                        .unwrap();
                    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                    assert!(total > 0);
                }
            });
        },
    );

    // Lance IVF-PQ vector search (per-batch fragments)
    group.bench_with_input(
        BenchmarkId::new("Lance_PerBatchFragment_IVFPQ", &label),
        &query,
        |b, q| {
            let dataset = lance_vec_per_batch_setup.dataset.clone();
            let query_array = Float32Array::from(q.clone());
            b.to_async(&rt).iter(|| {
                let dataset = dataset.clone();
                let query_array = query_array.clone();
                async move {
                    let batches: Vec<RecordBatch> = dataset
                        .scan()
                        .nearest("vector", &query_array, k)
                        .unwrap()
                        .nprobes(8)
                        .try_into_stream()
                        .await
                        .unwrap()
                        .try_collect()
                        .await
                        .unwrap();
                    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                    assert!(total > 0);
                }
            });
        },
    );

    group.finish();
}

/// Run all benchmarks.
fn all_benchmarks(c: &mut Criterion) {
    bench_scan(c);
    bench_point_lookup(c);
    bench_fts(c);
    bench_vector_search(c);
}

#[cfg(target_os = "linux")]
criterion_group!(
    name = benches;
    config = Criterion::default()
        .significance_level(0.05)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = all_benchmarks
);

#[cfg(not(target_os = "linux"))]
criterion_group!(
    name = benches;
    config = Criterion::default().significance_level(0.05);
    targets = all_benchmarks
);

criterion_main!(benches);
