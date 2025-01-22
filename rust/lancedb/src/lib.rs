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

//! [LanceDB](https://github.com/lancedb/lancedb) is an open-source database for vector-search built with persistent storage,
//! which greatly simplifies retrieval, filtering and management of embeddings.
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
//! ```shell
//! cargo install lancedb
//! ```
//!
//! ## Crate Features
//!
//! ### Experimental Features
//!
//! These features are not enabled by default.  They are experimental or in-development features that
//! are not yet ready to be released.
//!
//! - `remote` - Enable remote client to connect to LanceDB cloud.  This is not yet fully implemented
//!              and should not be enabled.
//!
//! ### Quick Start
//!
//! #### Connect to a database.
//!
//! ```rust
//! # use arrow_schema::{Field, Schema};
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let db = lancedb::connect("data/sample-lancedb").execute().await.unwrap();
//! # });
//! ```
//!
//! LanceDB accepts the different form of database path:
//!
//! - `/path/to/database` - local database on file system.
//! - `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud object store
//! - `db://dbname` - Lance Cloud
//!
//! You can also use [`ConnectOptions`] to configure the connection to the database.
//!
//! ```rust
//! use object_store::aws::AwsCredential;
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let db = lancedb::connect("data/sample-lancedb")
//!     .aws_creds(AwsCredential {
//!         key_id: "some_key".to_string(),
//!         secret_key: "some_secret".to_string(),
//!         token: None,
//!     })
//!     .execute()
//!     .await
//!     .unwrap();
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
//! use arrow_array::{RecordBatch, RecordBatchIterator};
//! use arrow_schema::{DataType, Field, Schema};
//! # use arrow_array::{FixedSizeListArray, Float32Array, Int32Array, types::Float32Type};
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! # let tmpdir = tempfile::tempdir().unwrap();
//! # let db = lancedb::connect(tmpdir.path().to_str().unwrap()).execute().await.unwrap();
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("id", DataType::Int32, false),
//!     Field::new(
//!         "vector",
//!         DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 128),
//!         true,
//!     ),
//! ]));
//! // Create a RecordBatch stream.
//! let batches = RecordBatchIterator::new(
//!     vec![RecordBatch::try_new(
//!         schema.clone(),
//!         vec![
//!             Arc::new(Int32Array::from_iter_values(0..256)),
//!             Arc::new(
//!                 FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
//!                     (0..256).map(|_| Some(vec![Some(1.0); 128])),
//!                     128,
//!                 ),
//!             ),
//!         ],
//!     )
//!     .unwrap()]
//!     .into_iter()
//!     .map(Ok),
//!     schema.clone(),
//! );
//! db.create_table("my_table", Box::new(batches))
//!     .execute()
//!     .await
//!     .unwrap();
//! # });
//! ```
//!
//! #### Create vector index (IVF_PQ)
//!
//! LanceDB is capable to automatically create appropriate indices based on the data types
//! of the columns. For example,
//!
//! * If a column has a data type of `FixedSizeList<Float16/Float32>`,
//!   LanceDB will create a `IVF-PQ` vector index with default parameters.
//! * Otherwise, it creates a `BTree` index by default.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use arrow_array::{FixedSizeListArray, types::Float32Type, RecordBatch,
//! #   RecordBatchIterator, Int32Array};
//! # use arrow_schema::{Schema, Field, DataType};
//! use lancedb::index::Index;
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! # let tmpdir = tempfile::tempdir().unwrap();
//! # let db = lancedb::connect(tmpdir.path().to_str().unwrap()).execute().await.unwrap();
//! # let tbl = db.open_table("idx_test").execute().await.unwrap();
//! tbl.create_index(&["vector"], Index::Auto)
//!    .execute()
//!    .await
//!    .unwrap();
//! # });
//! ```
//!
//!
//! User can also specify the index type explicitly, see [`Table::create_index`].
//!
//! #### Open table and search
//!
//! ```rust
//! # use std::sync::Arc;
//! # use futures::TryStreamExt;
//! # use arrow_schema::{DataType, Schema, Field};
//! # use arrow_array::{RecordBatch, RecordBatchIterator};
//! # use arrow_array::{FixedSizeListArray, Float32Array, Int32Array, types::Float32Type};
//! # use lancedb::query::{ExecutableQuery, QueryBase};
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! # let tmpdir = tempfile::tempdir().unwrap();
//! # let db = lancedb::connect(tmpdir.path().to_str().unwrap()).execute().await.unwrap();
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
//! # db.create_table("my_table", Box::new(batches)).execute().await.unwrap();
//! # let table = db.open_table("my_table").execute().await.unwrap();
//! let results = table
//!     .query()
//!     .nearest_to(&[1.0; 128])
//!     .unwrap()
//!     .execute()
//!     .await
//!     .unwrap()
//!     .try_collect::<Vec<_>>()
//!     .await
//!     .unwrap();
//! # });
//! ```

pub mod connection;
pub mod data;
pub mod io;
pub mod ipc;
#[cfg(feature = "remote")]
pub mod remote;

// Re-export from internal modules to create a unified public API
pub mod arrow {
    pub use lancedb_core::arrow::*;
}
pub mod embeddings {
    pub use lancedb_core::tabledef::EmbeddingDefinition;
    pub use lancedb_embedding::*;
}
pub mod error {
    pub use lancedb_core::error::*;
}
pub mod index {
    pub use lancedb_table::index::*;
}
#[cfg(feature = "polars")]
pub mod polars_arrow_convertors {
    pub use lancedb_core::polars_arrow_convertors::*;
}
pub mod query {
    pub use lancedb_table::query::*;
}
pub mod rerankers {
    pub use lancedb_reranker::*;
}
pub mod table {
    pub use lancedb_core::tabledef::TableDefinition;
    pub use lancedb_table::table::*;
}
pub mod utils {
    pub use lancedb_table::utils::*;
}

pub use connection::Connection;
pub use error::{Error, Result};
pub use table::Table;

pub use lancedb_core::DistanceType;

/// Connect to a database
pub use connection::connect;
