# Welcome to LanceDB's Documentation

LanceDB is an open-source database for vector-search built with persistent storage, which greatly simplifies retrevial, filtering and management of embeddings.

The key features of LanceDB include:

* Production-scale vector search with no servers to manage.

* Store, query and filter vectors, metadata and multi-modal data (text, images, videos, point clouds, and more).

* Support for vector similarity search, full-text search and SQL.

* Native Python and Javascript/Typescript support.

* Zero-copy, automatic versioning, manage versions of your data without needing extra infrastructure.

* Ecosystem integrations with [LangChain 🦜️🔗](https://python.langchain.com/en/latest/modules/indexes/vectorstores/examples/lancedb.html), [LlamaIndex 🦙](https://gpt-index.readthedocs.io/en/latest/examples/vector_stores/LanceDBIndexDemo.html), Apache-Arrow, Pandas, Polars, DuckDB and more on the way.

LanceDB's core is written in Rust 🦀 and is built using <a href="https://github.com/lancedb/lance">Lance</a>, an open-source columnar format designed for performant ML workloads.

## Quick Start

=== "Python"
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
      result = table.search([100, 100]).limit(2).to_df()
      ```

=== "Javascript"
      ```shell
      npm install vectordb
      ```

      ```javascript
      const lancedb = require("vectordb");

      const uri = "data/sample-lancedb";
      const db = await lancedb.connect(uri);
      const table = await db.createTable("my_table", 
            [{ id: 1, vector: [3.1, 4.1], item: "foo", price: 10.0 },
            { id: 2, vector: [5.9, 26.5], item: "bar", price: 20.0 }])
      const results = await table.search([100, 100]).limit(2).execute();
      ```

## Complete Demos (Python)
- [YouTube Transcript Search](notebooks/youtube_transcript_search.ipynb)
- [Documentation QA Bot using LangChain](notebooks/code_qa_bot.ipynb)
- [Multimodal search using CLIP](notebooks/multimodal_search.ipynb)
- [Serverless QA Bot with S3 and Lambda](examples/serverless_lancedb_with_s3_and_lambda.md)
- [Serverless QA Bot with Modal](examples/serverless_qa_bot_with_modal_and_langchain.md)

## Complete Demos (JavaScript)
- [YouTube Transcript Search](examples/youtube_transcript_bot_with_nodejs.md)

## Documentation Quick Links
* [`Basic Operations`](basic.md) - basic functionality of LanceDB.
* [`Embedding Functions`](embedding.md) - functions for working with embeddings.
* [`Indexing`](ann_indexes.md) - create vector indexes to speed up queries.
* [`Full text search`](fts.md) - [EXPERIMENTAL] full-text search API
* [`Ecosystem Integrations`](integrations.md) - integrating LanceDB with python data tooling ecosystem.
* [`Python API Reference`](python/python.md) - detailed documentation for the LanceDB Python SDK.
* [`Node API Reference`](javascript/modules.md) - detailed documentation for the LanceDB Python SDK.
