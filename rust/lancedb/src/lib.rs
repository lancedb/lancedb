// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

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
//! cargo add lancedb
//! ```
//!
//! ## Crate Features
//!
//! - `aws` - Enable AWS S3 object store support.
//! - `dynamodb` - Enable DynamoDB manifest store support.
//! - `azure` - Enable Azure Blob Storage object store support.
//! - `gcs` - Enable Google Cloud Storage object store support.
//! - `oss` - Enable Alibaba Cloud OSS object store support.
//! - `remote` - Enable remote client to connect to LanceDB cloud.
//! - `huggingface` - Enable HuggingFace Hub integration for loading datasets from the Hub.
//! - `fp16kernels` - Enable FP16 kernels for faster vector search on CPU.
//! - `metrics` - Publish LanceDB's internal metrics through the
//!   [`metrics`](https://docs.rs/metrics) crate facade and re-export that crate.
//!   Install any `metrics`-compatible recorder to collect them.
//! - `metrics-otel` - Add a pull-based adapter (the `metrics_otel` module) over
//!   the `metrics` facade for bridging metrics into OpenTelemetry or similar.
//!
//! ### Quick Start
//!
//! #### Connect to a database.
//!
//! ```rust
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
//! You can also use [`ConnectBuilder`] to configure the connection to the database.
//!
//! ```rust
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let db = lancedb::connect("data/sample-lancedb")
//!     .storage_options([
//!         ("aws_access_key_id", "some_key"),
//!         ("aws_secret_access_key", "some_secret"),
//!     ])
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
//! For more details, please refer to the [LanceDB documentation](https://docs.lancedb.com).
//!
//! #### Create a table
//!
//! To create a Table, you need to provide an [`arrow_array::RecordBatch`]. The
//! schema of the `RecordBatch` determines the schema of the table.
//!
//! Vector columns should be represented as `FixedSizeList<Float16/Float32>` data type.
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
//! let ndims = 128;
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("id", DataType::Int32, false),
//!     Field::new(
//!         "vector",
//!         DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), ndims),
//!         true,
//!     ),
//! ]));
//! let data = RecordBatch::try_new(
//!         schema.clone(),
//!         vec![
//!             Arc::new(Int32Array::from_iter_values(0..256)),
//!             Arc::new(
//!                 FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
//!                     (0..256).map(|_| Some(vec![Some(1.0); ndims as usize])),
//!                     ndims,
//!                 ),
//!             ),
//!         ],
//!     )
//!     .unwrap();
//! db.create_table("my_table", data)
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
//! # use futures::TryStreamExt;
//! # use lancedb::query::{ExecutableQuery, QueryBase};
//! # async fn example(table: &lancedb::Table) -> lancedb::Result<()> {
//! let results = table
//!     .query()
//!     .nearest_to(&[1.0; 128])?
//!     .execute()
//!     .await?
//!     .try_collect::<Vec<_>>()
//!     .await?;
//! #   Ok(())
//! # }
//! ```

pub mod arrow;
pub mod blob;
pub mod connection;
pub mod data;
pub mod database;
pub mod dataloader;
pub mod embeddings;
pub mod error;
pub mod expr;
pub mod index;
pub mod io;
pub mod ipc;
#[cfg(feature = "metrics-otel")]
pub mod metrics_otel;
#[cfg(feature = "polars")]
mod polars_arrow_convertors;
pub mod query;
#[cfg(feature = "remote")]
pub mod remote;
pub mod rerankers;
pub mod table;
#[cfg(test)]
pub mod test_utils;
pub mod utils;

use std::{fmt::Display, str::FromStr};

use serde::{Deserialize, Serialize};

pub use blob::{blob, is_blob};
pub use connection::{ConnectNamespaceBuilder, Connection};
pub use error::{Error, Result};
use lance_index::vector::ApproxMode as LanceApproxMode;
use lance_linalg::distance::DistanceType as LanceDistanceType;
/// Re-export of the [`metrics`](https://docs.rs/metrics) crate facade. Enable
/// the `metrics` feature to publish LanceDB's internal metrics; install any
/// `metrics`-compatible recorder to collect them. See also [`metrics_otel`] for
/// a built-in pull-based adapter.
#[cfg(feature = "metrics")]
pub use metrics;
pub use table::{FtsToken, Table};

/// Tokenize a full-text search query using an explicit FTS tokenizer configuration.
///
/// This does not require a table or FTS index. The tokenizer options are the
/// same [`index::scalar::FtsIndexBuilder`] values used when creating an FTS index.
pub fn tokenize(query: &str, params: &index::scalar::FtsIndexBuilder) -> Result<Vec<FtsToken>> {
    table::tokenize(query, params)
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, Default)]
#[non_exhaustive]
#[serde(rename_all = "lowercase")]
pub enum DistanceType {
    /// Euclidean distance. This is a very common distance metric that
    /// accounts for both magnitude and direction when determining the distance
    /// between vectors. l2 distance has a range of [0, ∞).
    #[default]
    L2,
    /// Cosine distance.  Cosine distance is a distance metric
    /// calculated from the cosine similarity between two vectors. Cosine
    /// similarity is a measure of similarity between two non-zero vectors of an
    /// inner product space. It is defined to equal the cosine of the angle
    /// between them.  Unlike l2, the cosine distance is not affected by the
    /// magnitude of the vectors.  Cosine distance has a range of [0, 2].
    ///
    /// Note: the cosine distance is undefined when one (or both) of the vectors
    /// are all zeros (there is no direction).  These vectors are invalid and may
    /// never be returned from a vector search.
    Cosine,
    /// Dot product. Dot distance is the dot product of two vectors. Dot
    /// distance has a range of (-∞, ∞). If the vectors are normalized (i.e. their
    /// l2 norm is 1), then dot distance is equivalent to the cosine distance.
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

/// Controls the speed / accuracy tradeoff for approximate vector search.
///
/// This currently only affects RQ-quantized vector indexes, such as IVF_RQ.
/// Other index types ignore this setting.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[non_exhaustive]
#[serde(rename_all = "lowercase")]
pub enum ApproxMode {
    /// Prefer lower query latency, which can reduce recall.
    Fast,
    /// Use the default balance between query latency and recall.
    #[default]
    Normal,
    /// Prefer higher recall, which can increase query latency.
    Accurate,
}

impl From<ApproxMode> for LanceApproxMode {
    fn from(value: ApproxMode) -> Self {
        match value {
            ApproxMode::Fast => Self::Fast,
            ApproxMode::Normal => Self::Normal,
            ApproxMode::Accurate => Self::Accurate,
        }
    }
}

impl From<LanceApproxMode> for ApproxMode {
    fn from(value: LanceApproxMode) -> Self {
        match value {
            LanceApproxMode::Fast => Self::Fast,
            LanceApproxMode::Normal => Self::Normal,
            LanceApproxMode::Accurate => Self::Accurate,
        }
    }
}

impl TryFrom<&str> for ApproxMode {
    type Error = Error;

    fn try_from(value: &str) -> std::prelude::v1::Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

impl FromStr for ApproxMode {
    type Err = Error;

    fn from_str(value: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "fast" => Ok(Self::Fast),
            "normal" => Ok(Self::Normal),
            "accurate" => Ok(Self::Accurate),
            _ => Err(Error::InvalidInput {
                message: format!(
                    "approx_mode must be one of 'fast', 'normal', or 'accurate', got '{}'",
                    value
                ),
            }),
        }
    }
}

impl Display for ApproxMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fast => write!(f, "fast"),
            Self::Normal => write!(f, "normal"),
            Self::Accurate => write!(f, "accurate"),
        }
    }
}

/// Connect to a database
pub use connection::connect;
/// Connect to a namespace-backed database
pub use connection::connect_namespace;

/// Re-export Lance Session and ObjectStoreRegistry for custom session creation
pub use lance::session::Session;
pub use lance_io::object_store::ObjectStoreRegistry;

/// Re-export DataFusion so consumers can build the `Expr` values that public
/// query/merge APIs (e.g. [`query::QueryBase::only_if_expr`]) accept without
/// declaring their own (potentially mismatched) direct `datafusion` dependency.
/// See <https://github.com/lancedb/lancedb/issues/3575>.
pub use datafusion;
