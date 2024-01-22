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
//! [dependencies]
//! vectordb = "0.4"
//! arrow-schema = "50"
//! arrow-array = "50"
//! ```
//!
//! ### Quick Start
//!
//! #### Connect to a database.
//!
//! ```rust
//! use vectordb::{Database, Table, WriteMode};
//! use arrow_schema::{Field, Schema};
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let db = Database::connect("data/sample-lancedb").await.unwrap();
//! # });
//! ```
//!
//! LanceDB uses [arrow-rs](https://github.com/apache/arrow-rs) to define schema, data types and array itself.
//! It treats [`FixedSizeList<Float16/Float32>`](https://docs.rs/arrow/latest/arrow/array/struct.FixedSizeListArray.html)
//! columns as vectors.
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
//! # use vectordb::Database;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! # let tmpdir = tempfile::tempdir().unwrap();
//! # let db = Database::connect(tmpdir.path().to_str().unwrap()).await.unwrap();
//! let schema = Arc::new(Schema::new(vec![
//!   Field::new("id", DataType::Int32, false),
//!   Field::new("vector", DataType::FixedSizeList(
//!     Arc::new(Field::new("item", DataType::Float32, true)), 128), true),
//! ]));
//! // Create a RecordBatch stream.
//! let batches = RecordBatchIterator::new(vec![
//!     RecordBatch::try_new(schema.clone(),
//!         vec![
//!             Arc::new(Int32Array::from_iter_values(0..10)),
//!             Arc::new(FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
//!                 (0..10).map(|_| Some(vec![Some(1.0); 128])), 128)),
//!         ]).unwrap()
//!    ].into_iter().map(Ok),
//!     schema.clone());
//! db.create_table("my_table", batches, None).await.unwrap();
//! # });
//! ```
//!
//! #### Open table and run search
//!
//! ```rust
//! # use std::sync::Arc;
//! # use arrow_schema::{DataType, Schema, Field};
//! # use arrow_array::{RecordBatch, RecordBatchIterator};
//! # use arrow_array::{FixedSizeListArray, Float32Array, Int32Array, types::Float32Type};
//! # use vectordb::Database;
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
//! # db.create_table("my_table", batches, None).await.unwrap();
//! let table = db.open_table("my_table").await.unwrap();
//! # });
//!
//!
//! ```

pub mod data;
pub mod database;
pub mod error;
pub mod index;
pub mod io;
pub mod query;
pub mod table;
pub mod utils;

pub use database::Database;
pub use table::Table;

pub use lance::dataset::WriteMode;
