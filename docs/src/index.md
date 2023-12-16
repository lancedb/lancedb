# LanceDB

LanceDB is an open-source database for AI that's designed to manage and query embeddings on multi-modal data. The core of LanceDB is written in Rust 🦀 and is built on top of [Lance](https://github.com/lancedb/lance), an open-source columnar data format designed for performant ML workloads at huge scale.

## Why use LanceDB?

* Embedded (OSS) and serverless (Cloud), so no need to manage servers

* Store, query and filter vectors, metadata and multi-modal data (text, images, videos, point clouds, and more).

* Support for production-scale vector similarity search, full-text search and SQL queries (via [DataFusion](https://github.com/apache/arrow-datafusion)).

* Native Python and Javascript/Typescript support.

* Tight integration with the [Arrow](https://arrow.apache.org/docs/format/Columnar.html) ecosystem, allowing true zero-copy access in shared memory with SIMD and GPU acceleration.

* Automatic data versioning, manage versions of your data without needing extra infrastructure.

* **Fully disk-based** index & storage, allowing scalability without breaking the bank.

* Ingest your favorite data formats directly, like pandas DataFrames, Pydantic objects and Polars (coming soon).

![Illustration](/lancedb/assets/ecosystem-illustration.png)

## Solutions


<!-- Add this button once SaaS version is GA: -->

<!-- [Try out LanceDB Cloud](https://noteforms.com/forms/lancedb-mailing-list-cloud-kty1o5?notionforms=1&utm_source=notionforms){ .md-button .md-button--primary } -->


---

## Quick Start

It's incredibly simple to get started with LanceDB. No servers, no hassle.

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

