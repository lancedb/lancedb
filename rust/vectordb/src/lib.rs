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
//! LanceDB is an open-source database for vector-search built with persistent storage,
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
//! ```rust,no_run
//! [dependencies]
//! vectordb = "0.4"
//! arrow-schema = "50"
//! arrow-array = "50"
//! ```
//!
//! ### Quick Start
//!
//! ```rust
//! use vectordb::{Database, Table, WriteMode};
//! use arrow_schema::{Field, Schema};
//!
//! let db = Database::connect("data/sample-lancedb").await.unwrap();
//! ```
//!
//! LanceDB uses [Apache Arrow] to define schema, data types and data itself.
//! It considers a [`FixedSizeList<float32>`](https://docs.rs/arrow/latest/arrow/array/struct.FixedSizeListArray.html)
//! columns as vectors.

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
