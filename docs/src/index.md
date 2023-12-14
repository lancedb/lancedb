# LanceDB

LanceDB is an open-source database for AI that's designed to manage and query embeddings for multi-modal data. The core of LanceDB is written in Rust 🦀 and is built on top of [Lance](https://github.com/lancedb/lance), an open-source columnar data format designed for performant ML workloads at huge scale.

## Why use LanceDB?

* Embedded (OSS) and serverless (Cloud), so no need to manage servers

* Store, query and filter vectors, metadata and multi-modal data (text, images, videos, point clouds, and more).

* Support for production-scale vector similarity search, full-text search and SQL querying (via [DataFusion](https://github.com/apache/arrow-datafusion)).

* Native Python and Javascript/Typescript support.

* Tightly integrated with the [Arrow](https://arrow.apache.org/docs/format/Columnar.html) ecosystem, allowing true zero-copy access in shared memory plus SIMD and GPU acceleration.

* Automatic data versioning, manage versions of your data without needing extra infrastructure.

* Fully disk-based data & vector index, persisted on HDD, allowing scalability without breaking the bank.

* Ingest your favorite data formats directly, like pandas DataFrames, Pydantic objects and more.

![Illustration](/lancedb/assets/ecosystem-illustration.png)

---

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
      result = table.search([100, 100]).limit(2).to_list()
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

* [`Basics`](basic.md): Basic functionality of LanceDB.
* [`Embeddings`](embeddings/index.md): Functions for working with embeddings.
* [`Indexing`](ann_indexes.md): Create vector indexes to speed up queries.
* [`Full text search`](fts.md): Full-text search API (Python-only for now)
* [`Ecosystem Integrations`](python/integration.md): Integrating LanceDB with python data tooling ecosystem.
* [`Python API`](python/python.md): Detailed documentation for the LanceDB Python SDK.
* [`Node API`](javascript/modules.md): Detailed documentation for the LanceDB Node TypeScript SDK.
