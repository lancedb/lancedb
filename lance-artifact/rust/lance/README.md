# Rust Implementation of Lance

<div align="center">
<p align="center">

<img width="257" alt="Lance Logo" src="https://user-images.githubusercontent.com/917119/199353423-d3e202f7-0269-411d-8ff2-e747e419e492.png">

**The Open Lakehouse Format for Multimodal AI**
</p></div>

## Installation

Install using cargo:

```shell
cargo install lance
```

## Examples

### Create dataset

Suppose `batches` is an Arrow `Vec<RecordBatch>` and schema is Arrow `SchemaRef`:

```rust
use lance::{dataset::WriteParams, Dataset};

let write_params = WriteParams::default();
let mut reader = RecordBatchIterator::new(
    batches.into_iter().map(Ok),
    schema
);
Dataset::write(reader, &uri, Some(write_params)).await.unwrap();
```

### Read

```rust
let dataset = Dataset::open(path).await.unwrap();
let mut scanner = dataset.scan();
let batches: Vec<RecordBatch> = scanner
    .try_into_stream()
    .await
    .unwrap()
    .map(|b| b.unwrap())
    .collect::<Vec<RecordBatch>>()
    .await;
```

### Take

```rust
let values: Result<RecordBatch> = dataset.take(&[200, 199, 39, 40, 100], &projection).await;
```

### Vector index

Assume "embeddings" is a FixedSizeListArray<f32>
```rust
use ::lance::index::vector::VectorIndexParams;

let params = VectorIndexParams::default();
params.num_partitions = 256;
params.num_sub_vectors = 16;

// this will Err if list_size(embeddings) / num_sub_vectors does not meet simd alignment
dataset.create_index(&["embeddings"], IndexType::Vector, None, &params, true).await;
```

## What is Lance?

Lance is an open lakehouse format for multimodal AI. It contains a file format, table format, and catalog spec that allows you to build a complete lakehouse on top of object storage to power your AI workflows.

The key features of Lance include:

* **Expressive hybrid search:** Combine vector similarity search, full-text search (BM25), and SQL analytics on the same dataset with accelerated secondary indices.

* **Lightning-fast random access:** 100x faster than Parquet or Iceberg for random access without sacrificing scan performance.

* **Native multimodal data support:** Store images, videos, audio, text, and embeddings in a single unified format with efficient blob encoding and lazy loading.

* **Data evolution:** Efficiently add columns with backfilled values without full table rewrites, perfect for ML feature engineering.

* **Zero-copy versioning:** ACID transactions, time travel, and automatic versioning without needing extra infrastructure.

* **Rich ecosystem integrations:** Apache Arrow, Pandas, Polars, DuckDB, Apache Spark, Ray, Trino, Apache Flink, and open catalogs (Apache Polaris, Unity Catalog, Apache Gravitino).

For more details, see the full [Lance format specification](https://lance.org/format).
