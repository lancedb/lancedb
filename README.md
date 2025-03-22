<a href="https://cloud.lancedb.com" target="_blank">
  <img src="https://github.com/user-attachments/assets/92dad0a2-2a37-4ce1-b783-0d1b4f30a00c" alt="LanceDB Cloud Public Beta" width="100%" style="max-width: 100%;">
</a>

<div align="center">
<p align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/user-attachments/assets/ac270358-333e-4bea-a132-acefaa94040e">
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/user-attachments/assets/b864d814-0d29-4784-8fd9-807297c758c0">
  <img alt="LanceDB Logo" src="https://github.com/user-attachments/assets/b864d814-0d29-4784-8fd9-807297c758c0" width=300>
</picture>

**Search More, Manage Less**

<a href='https://github.com/lancedb/vectordb-recipes/tree/main' target="_blank"><img alt='LanceDB' src='https://img.shields.io/badge/VectorDB_Recipes-100000?style=for-the-badge&logo=LanceDB&logoColor=white&labelColor=645cfb&color=645cfb'/></a>
<a href='https://lancedb.github.io/lancedb/' target="_blank"><img alt='lancdb' src='https://img.shields.io/badge/DOCS-100000?style=for-the-badge&logo=lancdb&logoColor=white&labelColor=645cfb&color=645cfb'/></a>
[![Blog](https://img.shields.io/badge/Blog-12100E?style=for-the-badge&logoColor=white)](https://blog.lancedb.com/)
[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?style=for-the-badge&logo=discord&logoColor=white)](https://discord.gg/zMM32dvNtd)
[![Twitter](https://img.shields.io/badge/Twitter-%231DA1F2.svg?style=for-the-badge&logo=Twitter&logoColor=white)](https://twitter.com/lancedb)
[![Gurubase](https://img.shields.io/badge/Gurubase-Ask%20LanceDB%20Guru-006BFF?style=for-the-badge)](https://gurubase.io/g/lancedb)

</p>

<img max-width="750px" alt="LanceDB Multimodal Search" src="https://github.com/lancedb/lancedb/assets/917119/09c5afc5-7816-4687-bae4-f2ca194426ec">

</p>
</div>

<hr />

LanceDB is an open-source database for vector-search built with persistent storage, which greatly simplifies retrieval, filtering and management of embeddings.

The key features of LanceDB include:

* Production-scale vector search with no servers to manage.

* Store, query and filter vectors, metadata and multi-modal data (text, images, videos, point clouds, and more).

* Support for vector similarity search, full-text search and SQL.

* Native Python and Javascript/Typescript support.

* Zero-copy, automatic versioning, manage versions of your data without needing extra infrastructure.

* GPU support in building vector index(*).

* Ecosystem integrations with [LangChain 🦜️🔗](https://python.langchain.com/docs/integrations/vectorstores/lancedb/), [LlamaIndex 🦙](https://gpt-index.readthedocs.io/en/latest/examples/vector_stores/LanceDBIndexDemo.html), Apache-Arrow, Pandas, Polars, DuckDB and more on the way.

LanceDB's core is written in Rust 🦀 and is built using <a href="https://github.com/lancedb/lance">Lance</a>, an open-source columnar format designed for performant ML workloads.

## Quick Start

**Javascript**
```shell
npm install @lancedb/lancedb
```

```javascript
import * as lancedb from "@lancedb/lancedb";

const db = await lancedb.connect("data/sample-lancedb");
const table = await db.createTable("vectors", [
	{ id: 1, vector: [0.1, 0.2], item: "foo", price: 10 },
	{ id: 2, vector: [1.1, 1.2], item: "bar", price: 50 },
], {mode: 'overwrite'});


const query = table.vectorSearch([0.1, 0.3]).limit(2);
const results = await query.toArray();

// You can also search for rows by specific criteria without involving a vector search.
const rowsByCriteria = await table.query().where("price >= 10").toArray();
```

**Python**
```shell
pip install lancedb
```

```python
import lancedb

uri = "data/sample-lancedb"
db = lancedb.connect(uri)
table = db.create_table("my_table",
                         data=[{"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
                               {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}])
result = table.search([100, 100]).limit(2).to_pandas()
```

## Blogs, Tutorials & Videos
* 📈 <a href="https://blog.lancedb.com/benchmarking-random-access-in-lance/">2000x better performance with Lance over Parquet</a>
* 🤖 <a href="https://github.com/lancedb/vectordb-recipes/tree/main/examples/Youtube-Search-QA-Bot">Build a question and answer bot with LanceDB</a>
