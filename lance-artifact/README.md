<div align="center">
<p align="center">

<img width="257" alt="Lance Logo" src="https://user-images.githubusercontent.com/917119/199353423-d3e202f7-0269-411d-8ff2-e747e419e492.png">

**The Open Lakehouse Format for Multimodal AI**<br/>
**High-performance vector search, full-text search, random access, and feature engineering capabilities for the lakehouse.**<br/>
**Compatible with Pandas, DuckDB, Polars, PyArrow, Ray, Spark, and more integrations on the way.**

<a href="https://lance.org">Documentation</a> •
<a href="https://lance.org/community">Community</a> •
<a href="https://discord.gg/lance">Discord</a> •
<a href="https://groups.google.com/a/lance.org/g/dev">Mailing List</a>

[CI]: https://github.com/lance-format/lance/actions/workflows/rust.yml
[CI Badge]: https://github.com/lance-format/lance/actions/workflows/rust.yml/badge.svg
[Docs]: https://lance.org
[Docs Badge]: https://img.shields.io/badge/docs-passing-brightgreen
[crates.io]: https://crates.io/crates/lance
[crates.io badge]: https://img.shields.io/crates/v/lance.svg
[Python versions]: https://pypi.org/project/pylance/
[Python versions badge]: https://img.shields.io/pypi/pyversions/pylance

[![CI Badge]][CI]
[![Docs Badge]][Docs]
[![crates.io badge]][crates.io]
[![Python versions badge]][Python versions]

</p>
</div>

<hr />

Lance is an open lakehouse format for multimodal AI. It contains a file format, table format, and catalog spec that allows you to build a complete lakehouse on top of object storage to power your AI workflows. Lance is perfect for:

1. Building search engines and feature stores with hybrid search capabilities.
2. Large-scale ML training requiring high performance IO and random access.
3. Storing, querying, and managing multimodal data including images, videos, audio, text, and embeddings.

The key features of Lance include:

* **Expressive hybrid search:** Combine vector similarity search, full-text search (BM25), and SQL analytics on the same dataset with accelerated secondary indices.

* **Lightning-fast random access:** 100x faster than Parquet or Iceberg for random access without sacrificing scan performance.

* **Native multimodal data support:** Store images, videos, audio, text, and embeddings in a single unified format with efficient blob encoding and lazy loading.

* **Data evolution:** Efficiently add columns with backfilled values without full table rewrites, perfect for ML feature engineering.

* **Zero-copy versioning:** Automatic versioning with ACID transactions, time travel, tags, and branches—no extra infrastructure needed.

* **Rich ecosystem integrations:** Apache Arrow, Pandas, Polars, DuckDB, Apache Spark, Ray, Trino, Apache Flink, and open catalogs (Apache Polaris, Unity Catalog, Apache Gravitino).

For more details, see the full [Lance format specification](https://lance.org/format).

> [!TIP]
> Lance is in active development and we welcome contributions. Please see our [contributing guide](https://lance.org/community/contributing/) for more information.

## File format stability

Lance releases frequently because the SDKs, integrations, and performance work are moving quickly. This does not mean the Lance file format changes incompatibly in every release. The Lance file format is identified by the `data_storage_version` stored in each dataset, and stable storage versions are a long-term compatibility contract.

* Once a dataset is written with a stable `data_storage_version`, future Lance releases will continue to support reading that storage version.
* SDK and API compatibility is separate from file format compatibility. SDK/API changes follow semantic versioning and are documented in the [migration guide](https://lance.org/guide/migration/).
* Older Lance releases may not understand file format versions introduced later. If you run mixed Lance versions, pin `data_storage_version` for deterministic writes.
* The `next` file format alias is unstable and should only be used for experimentation, never for production data.

For production, write data with a stable `data_storage_version`. See the [format versioning guide](https://lance.org/format/file/versioning/) for the current compatibility matrix.

## Quick Start

**Installation**

```shell
pip install pylance
```

To install a preview release:

```shell
pip install --pre --extra-index-url https://pypi.fury.io/lance-format pylance
```

> [!TIP]
> Preview releases are released more often than full releases and contain the
> latest features and bug fixes. They receive the same level of testing as full releases.
> We guarantee they will remain published and available for download for at
> least 6 months. When you want to pin to a specific version, prefer a stable release.

**Converting to Lance**

```python
import lance

import pandas as pd
import pyarrow as pa
import pyarrow.dataset

df = pd.DataFrame({"a": [5], "b": [10]})
uri = "/tmp/test.parquet"
tbl = pa.Table.from_pandas(df)
pa.dataset.write_dataset(tbl, uri, format='parquet')

parquet = pa.dataset.dataset(uri, format='parquet')
lance.write_dataset(parquet, "/tmp/test.lance")
```

**Reading Lance data**
```python
dataset = lance.dataset("/tmp/test.lance")
assert isinstance(dataset, pa.dataset.Dataset)
```

**Pandas**
```python
df = dataset.to_table().to_pandas()
df
```

**DuckDB**
```python
import duckdb

# If this segfaults, make sure you have duckdb v0.7+ installed
duckdb.query("SELECT * FROM dataset LIMIT 10").to_df()
```

**Vector search**

Download the sift1m subset

```shell
wget ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz
tar -xzf sift.tar.gz
```

Convert it to Lance

```python
import lance
from lance.vector import vec_to_table
import numpy as np
import struct

nvecs = 1000000
ndims = 128
with open("sift/sift_base.fvecs", mode="rb") as fobj:
    buf = fobj.read()
    data = np.array(struct.unpack("<128000000f", buf[4 : 4 + 4 * nvecs * ndims])).reshape((nvecs, ndims))
    dd = dict(zip(range(nvecs), data))

table = vec_to_table(dd)
uri = "vec_data.lance"
sift1m = lance.write_dataset(table, uri, max_rows_per_group=8192, max_rows_per_file=1024*1024)
```

Build the index

```python
sift1m.create_index("vector",
                    index_type="IVF_PQ",
                    num_partitions=256,  # IVF
                    num_sub_vectors=16)  # PQ
```

Search the dataset

```python
# Get top 10 similar vectors
import duckdb

dataset = lance.dataset(uri)

# Sample 100 query vectors. If this segfaults, make sure you have duckdb v0.7+ installed
sample = duckdb.query("SELECT vector FROM dataset USING SAMPLE 100").to_df()
query_vectors = np.array([np.array(x) for x in sample.vector])

# Get nearest neighbors for all of them
rs = [dataset.to_table(nearest={"column": "vector", "k": 10, "q": q})
      for q in query_vectors]
```

## Directory structure

| Directory          | Description              |
|--------------------|--------------------------|
| [rust](./rust)     | Core Rust implementation |
| [python](./python) | Python bindings (PyO3)   |
| [java](./java)     | Java bindings (JNI)      |
| [docs](./docs)     | Documentation source     |

## Benchmarks

### Vector search

We used the SIFT dataset to benchmark our results with 1M vectors of 128D

1. For 100 randomly sampled query vectors, we get <1ms average response time (on a 2023 m2 MacBook Air)

![avg_latency.png](docs/src/images/avg_latency.png)

2. ANNs are always a trade-off between recall and performance

![avg_latency.png](docs/src/images/recall_vs_latency.png)

### Vs. parquet

We create a Lance dataset using the Oxford Pet dataset to do some preliminary performance testing of Lance as compared to Parquet and raw image/XMLs. For analytics queries, Lance is 50-100x better than reading the raw metadata. For batched random access, Lance is 100x better than both parquet and raw files.

![](docs/src/images/lance_perf.png)

## Why Lance for AI/ML workflows?

The machine learning development cycle involves multiple stages:

```mermaid
graph LR
    A[Collection] --> B[Exploration];
    B --> C[Analytics];
    C --> D[Feature Engineer];
    D --> E[Training];
    E --> F[Evaluation];
    F --> C;
    E --> G[Deployment];
    G --> H[Monitoring];
    H --> A;
```

Traditional lakehouse formats were designed for SQL analytics and struggle with AI/ML workloads that require:
- **Vector search** for similarity and semantic retrieval
- **Fast random access** for sampling and interactive exploration
- **Multimodal data** storage (images, videos, audio alongside embeddings)
- **Data evolution** for feature engineering without full table rewrites
- **Hybrid search** combining vectors, full-text, and SQL predicates

While existing formats (Parquet, Iceberg, Delta Lake) excel at SQL analytics, they require additional specialized systems for AI capabilities. Lance brings these AI-first features directly into the lakehouse format.

A comparison of different formats across ML development stages:

|                     | Lance | Parquet & ORC | JSON & XML | TFRecord | Database | Warehouse |
|---------------------|-------|---------------|------------|----------|----------|-----------|
| Analytics           | Fast  | Fast          | Slow       | Slow     | Decent   | Fast      |
| Feature Engineering | Fast  | Fast          | Decent     | Slow     | Decent   | Good      |
| Training            | Fast  | Decent        | Slow       | Fast     | N/A      | N/A       |
| Exploration         | Fast  | Slow          | Fast       | Slow     | Fast     | Decent    |
| Infra Support       | Rich  | Rich          | Decent     | Limited  | Rich     | Rich      |

