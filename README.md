<div align="center">
<p align="center">

<img width="275" alt="LanceDB Logo" src="https://user-images.githubusercontent.com/917119/226205734-6063d87a-1ecc-45fe-85be-1dea6383a3d8.png">

**Serverless, low-latency vector database for AI applications**

<a href="https://lancedb.github.io/lancedb/">Documentation</a> •
<a href="https://blog.eto.ai/">Blog</a> •
<a href="https://discord.gg/zMM32dvNtd">Discord</a> •
<a href="https://twitter.com/etodotai">Twitter</a>

</p>
</div>

<hr />

LanceDB is an open-source database for vector-search built with persistent storage, which greatly simplifies retrevial, filtering and management of embeddings.

The key features of LanceDB include:

* Production-scale vector search with no servers to manage.

* Combine attribute-based information with vectors and store them as a single source-of-truth.

* Zero-copy, automatic versioning, manage versions of your data without needing extra infrastructure.

* Ecosystem integrations: Apache-Arrow, Pandas, Polars, DuckDB and more on the way.

LanceDB's core is written in Rust 🦀 and is built using <a href="https://github.com/eto-ai/lance">Lance</a>, an open-source columnar format designed for performant ML workloads.

## Quick Start

**Installation**

```shell
pip install lancedb
```

**Quickstart**
```python
import lancedb

db = lancedb.connect(uri)
table = db.create_table("my_table",
                         data=[{"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
                               {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}])
result = table.search([100, 100]).limit(2).to_df()
```

## Blogs, Tutorials & Videos
* 🤖 <a href="https://github.com/lancedb/lancedb/blob/main/notebooks/youtube_transcript_search.ipynb">Build a question and answer bot with LanceDB</a>
