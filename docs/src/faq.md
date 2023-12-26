# FAQs

### What is the difference between Lance and LanceDB?

[Lance](https://github.com/lancedb/lance) is a modern columnar data format for AI, written in Rust 🦀. It’s perfect for building search engines, feature stores and being the foundation of large-scale ML training jobs requiring high performance IO and shuffles. It also has native support for storing, querying, and inspecting deeply nested data for robotics or large blobs like images, point clouds, and more.

LanceDB is the vector database that’s built on top of Lance, and utilizes the underlying optimized storage format to build efficient disk-based indexes that power semantic search & retrieval applications, from RAGs to QA Bots to recommender systems.

### Why invent another data format instead of using Parquet?

As we mention in our talk titled “[Lance, a modern columnar data format](https://www.youtube.com/watch?v=ixpbVyrsuL8)”, Parquet and other tabular formats that derive from it are rather dated (Parquet is over 10 years old), especially when it comes to random access on vector embeddings. We need a format that’s able to handle the complex trade-offs involved in shuffling, scanning, OLAP and filtering large datasets involving vectors. Our benchmarks show that Lance is up to 1000x faster than Parquet for random access, which we believe justifies our decision to create a new data format for AI.

### Is LanceDB open source?

Yes, LanceDB is an open source vector database available under an Apache 2.0 license. We have another serverless offering, LanceDB Cloud, available under a commercial license.

### Why build in Rust 🦀?

We believe that the Rust ecosystem has attained mainstream maturity and that Rust will form the underpinnings of large parts of the data and ML landscape in a few years. As a result, both Lance (the data format) and LanceDB (the database) are written entirely in Rust. We provide Python and JavaScript client libraries to interact with the database. Our Rust API is a little rough around the edges right now, but is fast coming to be on par with the Python and JS APIs.

### What is the difference between LanceDB OSS and LanceDB Cloud?

LanceDB OSS is an **embedded** (in-process) solution that can be used as the vector store of choice your LLM and RAG applications, and can be interacted with in multiple ways: it can be embedded inside an existing application backend, or used alongside existing ML and data engineering pipelines.

LanceDB Cloud is a **serverless** solution — the database and data sit on the cloud and we manage the scalability of the application side via a remote client, without the need to manage any infrastructure.

Both flavors of LanceDB benefit from the blazing fast Lance data format and are built on the same open source tools.

### What’s different about LanceDB compared to other vendors?

LanceDB is among the few embedded vector DBs out there that we believe can unlock a whole new class of LLM-powered applications in the browser or via edge functions. LanceDB’s multi-modal nature allows you to store the raw data, metadata and the embeddings all at once, unlike other solutions that typically store just the embeddings and metadata.

The Lance format that powers our storage system, also provides zero-copy access to other data formats (like Pandas, Polars, Pydantic) via Apache Arrow, as well as automatic data versioning and data management without needing extra infrastucture.

### How large of a dataset can LanceDB handle?

LanceDB and its underlying data format, Lance, are built to scale to really large amounts of data (hundreds of terabytes). We are currently working with customers who regularly perform operations on 200M+ vectors, and we’re fast approaching billion scale and beyond, which are well-handled by our disk-based indexes.

### Do I need to build an ANN index to run vector search?

No. LanceDB is blazing fast (due to its disk-based index) for even brute force kNN search, up to a limit. In our benchmarks, computing 100K pairs of 1000-dimension vectors takes less than 20ms. For small datasets of ~100K records or applications that can accept ~100ms latency, an ANN index is usually not necessary.

For large-scale (>1M) or higher dimension vectors, it is beneficial to create an ANN index.

### Does LanceDB support full-text search?

Yes, LanceDB supports full-text search (FTS) via [Tantivy](https://github.com/quickwit-oss/tantivy). Our current FTS integration is Python-only, and our goal is to push it down to the Rust level in future versions to enable much more powerful search capabilities available to our Python, JavaScript and Rust clients.