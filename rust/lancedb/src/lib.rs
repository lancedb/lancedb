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
//! ```ignore
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

pub mod arrow;
pub mod connection;
pub mod data;
pub mod embeddings;
pub mod error;
pub mod index;
pub mod io;
pub mod ipc;
#[cfg(feature = "polars")]
mod polars_arrow_convertors;
pub mod query;
#[cfg(feature = "remote")]
pub(crate) mod remote;
pub mod table;
pub mod utils;

use std::fmt::Display;

use serde::{Deserialize, Serialize};

pub use connection::Connection;
pub use error::{Error, Result};
use lance_linalg::distance::DistanceType as LanceDistanceType;
pub use table::Table;

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum DistanceType {
    /// Euclidean distance. This is a very common distance metric that
    /// accounts for both magnitude and direction when determining the distance
    /// between vectors. L2 distance has a range of [0, ∞).
    L2,
    /// Cosine distance.  Cosine distance is a distance metric
    /// calculated from the cosine similarity between two vectors. Cosine
    /// similarity is a measure of similarity between two non-zero vectors of an
    /// inner product space. It is defined to equal the cosine of the angle
    /// between them.  Unlike L2, the cosine distance is not affected by the
    /// magnitude of the vectors.  Cosine distance has a range of [0, 2].
    ///
    /// Note: the cosine distance is undefined when one (or both) of the vectors
    /// are all zeros (there is no direction).  These vectors are invalid and may
    /// never be returned from a vector search.
    Cosine,
    /// Dot product. Dot distance is the dot product of two vectors. Dot
    /// distance has a range of (-∞, ∞). If the vectors are normalized (i.e. their
    /// L2 norm is 1), then dot distance is equivalent to the cosine distance.
    Dot,
    /// Hamming distance. Hamming distance is a distance metric that measures
    /// the number of positions at which the corresponding elements are different.
    Hamming,
}

impl From<DistanceType> for LanceDistanceType {
    fn from(value: DistanceType) -> Self {
        match value {
            DistanceType::L2 => Self::L2,
            DistanceType::Cosine => Self::Cosine,
            DistanceType::Dot => Self::Dot,
            DistanceType::Hamming => Self::Hamming,
        }
    }
}

impl From<LanceDistanceType> for DistanceType {
    fn from(value: LanceDistanceType) -> Self {
        match value {
            LanceDistanceType::L2 => Self::L2,
            LanceDistanceType::Cosine => Self::Cosine,
            LanceDistanceType::Dot => Self::Dot,
            LanceDistanceType::Hamming => Self::Hamming,
        }
    }
}

impl<'a> TryFrom<&'a str> for DistanceType {
    type Error = <LanceDistanceType as TryFrom<&'a str>>::Error;

    fn try_from(value: &str) -> std::prelude::v1::Result<Self, Self::Error> {
        LanceDistanceType::try_from(value).map(Self::from)
    }
}

impl Display for DistanceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        LanceDistanceType::from(*self).fmt(f)
    }
}

/// Connect to a database
pub use connection::connect;
