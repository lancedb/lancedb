# Welcome to LanceDB's Documentation

LanceDB is an open-source database for vector-search built with persistent storage, which greatly simplifies retrevial, filtering and management of embeddings.

The key features of LanceDB include:

* Production-scale vector search with no servers to manage.

* Combine attribute-based information with vectors and store them as a single source-of-truth.

* Zero-copy, automatic versioning, manage versions of your data without needing extra infrastructure.

* Ecosystem integrations: Apache-Arrow, Pandas, Polars, DuckDB and more on the way.

LanceDB's core is written in Rust 🦀 and is built using Lance, an open-source columnar format designed for performant ML workloads.


## Installation

```shell
pip install lancedb
```

## Quickstart

```python
import lancedb

db = lancedb.connect(".")
table = db.create_table("my_table",
                         data=[{"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
                               {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}])
result = table.search([100, 100]).limit(2).to_df()
```

## Complete Demos

We will be adding completed demo apps built using LanceDB.
- [YouTube Transcript Search](../notebooks/youtube_transcript_search.ipynb)


## Documentation Quick Links
* [`Basic Operations`](basic.md) - basic functionality of LanceDB.
* [`Embedding Functions`](embedding.md) - functions for working with embeddings.
* [`Indexing`](ann_indexes.md) - create vector indexes to speed up queries.
* [`Ecosystem Integrations`](integrations.md) - integrating LanceDB with python data tooling ecosystem.
* [`API Reference`](python.md) - detailed documentation for the LanceDB Python SDK.
