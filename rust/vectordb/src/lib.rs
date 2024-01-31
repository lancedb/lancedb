// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! # VectorDB ([LanceDB](https://github.com/lancedb/lancedb)) -- Developer-friendly, serverless vector database for AI applications
//!
//! [LanceDB](https://github.com/lancedb/lancedb) is an open-source database for vector-search built with persistent storage,
//! which greatly simplifies retrevial, filtering and management of embeddings.
//!
//! The key features of LanceDB include:
//! - Production-scale vector search with no servers to manage.
//! - Store, query and filter vectors, metadata and multi-modal data (text, images, videos, point clouds, and more).
//! - Support for vector similarity search, full-text search and SQL.
//! - Native Rust, Python, Javascript/Typescript support.
//! - Zero-copy, automatic versioning, manage versions of your data without needing extra infrastructure.
//! - GPU support in building vector indices[^note].
//! - Ecosystem integrations with LangChain 🦜️🔗, LlamaIndex 🦙, Apache-Arrow, Pandas, Polars, DuckDB and more on the way.
//!
//! [^note]: Only in Python SDK.
//!
//! ## Getting Started
//!
//! LanceDB runs in process, to use it in your Rust project, put the following in your `Cargo.toml`:
//!
//! ```ignore
//! cargo install vectordb
//! ```
//!
//! ### Quick Start
//!
//! <div class="warning">Rust API is not stable yet, please expect breaking changes.</div>
//!
//! #### Connect to a database.
//!
//! ```rust
//! use vectordb::connect;
//! # use arrow_schema::{Field, Schema};
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let db = connect("data/sample-lancedb").await.unwrap();
//! # });
//! ```
//!
//! LanceDB accepts the different form of database path:
//!
//! - `/path/to/database` - local database on file system.
//! - `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud object store
//! - `db://dbname` - Lance Cloud
//!
//! You can also use [`ConnectOptions`] to configure the connectoin to the database.
//!
//! ```rust
//! use vectordb::{connect_with_options, ConnectOptions};
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let options = ConnectOptions::new("data/sample-lancedb")
//!     .index_cache_size(1024);
//! let db = connect_with_options(&options).await.unwrap();
//! # });
//! ```
//!
//! LanceDB uses [arrow-rs](https://github.com/apache/arrow-rs) to define schema, data types and array itself.
//! It treats [`FixedSizeList<Float16/Float32>`](https://docs.rs/arrow/latest/arrow/array/struct.FixedSizeListArray.html)
//! columns as vector columns.
//!
//! For more details, please refer to [LanceDB documentation](https://lancedb.github.io/lancedb/).
//!
//! #### Create a table
//!
//! To create a Table, you need to provide a [`arrow_schema::Schema`] and a [`arrow_array::RecordBatch`] stream.
//!
//! ```rust
//! # use std::sync::Arc;
//! use arrow_schema::{DataType, Schema, Field};
//! use arrow_array::{RecordBatch, RecordBatchIterator};
//! # use arrow_array::{FixedSizeListArray, Float32Array, Int32Array, types::Float32Type};
//! # use vectordb::connection::{Database, Connection};
//! # use vectordb::connect;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! # let tmpdir = tempfile::tempdir().unwrap();
//! # let db = connect(tmpdir.path().to_str().unwrap()).await.unwrap();
//! let schema = Arc::new(Schema::new(vec![
//!   Field::new("id", DataType::Int32, false),
//!   Field::new("vector", DataType::FixedSizeList(
//!     Arc::new(Field::new("item", DataType::Float32, true)), 128), true),
//! ]));
//! // Create a RecordBatch stream.
//! let batches = RecordBatchIterator::new(vec![
//!     RecordBatch::try_new(schema.clone(),
//!         vec![
//!             Arc::new(Int32Array::from_iter_values(0..1000)),
//!             Arc::new(FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
//!                 (0..1000).map(|_| Some(vec![Some(1.0); 128])), 128)),
//!         ]).unwrap()
//!    ].into_iter().map(Ok),
//!     schema.clone());
//! db.create_table("my_table", Box::new(batches), None).await.unwrap();
//! # });
//! ```
//!
//! #### Create vector index (IVF_PQ)
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use vectordb::connect;
//! # use arrow_array::{FixedSizeListArray, types::Float32Type, RecordBatch,
//! #   RecordBatchIterator, Int32Array};
//! # use arrow_schema::{Schema, Field, DataType};
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! # let tmpdir = tempfile::tempdir().unwrap();
//! # let db = connect(tmpdir.path().to_str().unwrap()).await.unwrap();
//! # let tbl = db.open_table("idx_test").await.unwrap();
//! tbl.create_index(&["vector"])
//!     .ivf_pq()
//!     .num_partitions(256)
//!     .build()
//!     .await
//!     .unwrap();
//! # });
//! ```
//!
//! #### Open table and run search
//!
//! ```rust
//! # use std::sync::Arc;
//! # use futures::TryStreamExt;
//! # use arrow_schema::{DataType, Schema, Field};
//! # use arrow_array::{RecordBatch, RecordBatchIterator};
//! # use arrow_array::{FixedSizeListArray, Float32Array, Int32Array, types::Float32Type};
//! # use vectordb::connection::{Database, Connection};
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! # let tmpdir = tempfile::tempdir().unwrap();
//! # let db = Database::connect(tmpdir.path().to_str().unwrap()).await.unwrap();
//! # let schema = Arc::new(Schema::new(vec![
//! #  Field::new("id", DataType::Int32, false),
//! #  Field::new("vector", DataType::FixedSizeList(
//! #    Arc::new(Field::new("item", DataType::Float32, true)), 128), true),
//! # ]));
//! # let batches = RecordBatchIterator::new(vec![
//! #    RecordBatch::try_new(schema.clone(),
//! #       vec![
//! #           Arc::new(Int32Array::from_iter_values(0..10)),
//! #           Arc::new(FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
//! #               (0..10).map(|_| Some(vec![Some(1.0); 128])), 128)),
//! #       ]).unwrap()
//! #   ].into_iter().map(Ok),
//! #    schema.clone());
//! # db.create_table("my_table", Box::new(batches), None).await.unwrap();
//! # let table = db.open_table("my_table").await.unwrap();
//! let results = table
//!     .search(&[1.0; 128])
//!     .execute_stream()
//!     .await
//!     .unwrap()
//!     .try_collect::<Vec<_>>()
//!     .await
//!     .unwrap();
//! # });
//!
//!
//! ```

pub mod connection;
pub mod data;
pub mod error;
pub mod index;
pub mod io;
pub mod ipc;
pub mod merge_insert;
pub mod query;
pub mod table;
pub mod utils;

pub use connection::{Connection, Database};
pub use error::{Error, Result};
pub use table::{Table, TableRef};

/// Connect to a database
pub use connection::{connect, connect_with_options, ConnectOptions};
pub use lance::dataset::WriteMode;
