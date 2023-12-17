# LanceDB

LanceDB is an open-source database for AI that's designed to manage and query embeddings on multi-modal data. The core of LanceDB is written in Rust 🦀 and is built on top of [Lance](https://github.com/lancedb/lance), an open-source columnar data format designed for performant ML workloads at huge scale.

LanceDB is designed from the ground up to be **scalable**, **easy-to-use**, **low cost** and **multi-modal** by nature.

## Why use LanceDB?

* Embedded (OSS) and serverless (Cloud), so no need to manage servers

* *Fast*, production-scale vector similarity search, full-text search and SQL queries (via [DataFusion](https://github.com/apache/arrow-datafusion)).

* Native Python and Javascript/Typescript support.

* Store, query & manage multi-modal data (text, images, videos, point clouds, etc.), not just the embeddings and metadata

* Tight integration with the [Arrow](https://arrow.apache.org/docs/format/Columnar.html) ecosystem, allowing true zero-copy access in shared memory with SIMD and GPU acceleration.

* Automatic data versioning, manage versions of your data without needing extra infrastructure.

* **Fully disk-based** index & storage, allowing scalability without breaking the bank.

* Ingest your favorite data formats directly, like pandas DataFrames, Pydantic objects and Polars (coming soon).

![Illustration](/lancedb/assets/ecosystem-illustration.png)

---

## Solutions

### LanceDB OSS

LanceDB OSS is a fully open-source, batteries-included embedded vector database that you can run on your own infrastructure. LanceDB OSS runs in-process, and it's incredibly simple to get started via its Python or JavaScript API. No servers, no hassle.

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

### LanceDB Cloud

LanceDB Cloud is a SaaS offering that runs serverless LanceDB in the cloud, so you don't have to manage any infrastructure. Storage is clearly separated from compute, and it's designed to be low-cost and highly scalable without breaking the bank.

It's currently in private beta with general availability coming soon, but you can get started with the private beta release by signing up:

[Try out LanceDB Cloud](https://noteforms.com/forms/lancedb-mailing-list-cloud-kty1o5?notionforms=1&utm_source=notionforms){ .md-button .md-button--primary }


## Explore the docs

The following pages go deeper into the internal of LanceDB and how to use it.

* [Basics](basic.md): Basics of LanceDB
* [Working with tables](guides/tables.md): Working with tables
* [Embeddings](embeddings/index.md): Understanding and working with embeddings
* [Indexing](ann_indexes.md): Build vector indexes to perform approximate nearest neighbour search
* [Full text search](fts.md): Build full-text search index, currently Python only
* [Ecosystem Integrations](integrations/index.md): Integrate LanceDB with the PyData ecosystem
* [Python API Reference](python/python.md): Documentation for the LanceDB Python API
* [JavaScript API Reference](javascript/modules.md): Documentation for the LanceDB JavaScript/Node.js API
