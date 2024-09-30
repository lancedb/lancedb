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

use std::sync::Arc;

use scalar::FtsIndexBuilder;
use serde::Deserialize;
use serde_with::skip_serializing_none;

use crate::{table::TableInternal, DistanceType, Error, Result};

use self::{
    scalar::{BTreeIndexBuilder, BitmapIndexBuilder, LabelListIndexBuilder},
    vector::{IvfHnswPqIndexBuilder, IvfHnswSqIndexBuilder, IvfPqIndexBuilder},
};

pub mod scalar;
pub mod vector;

/// Supported index types.
pub enum Index {
    Auto,
    /// A `BTree` index is an sorted index on scalar columns.
    /// This index is good for scalar columns with mostly distinct values and does best when
    /// the query is highly selective. It can apply to numeric, temporal, and string columns.
    ///
    /// BTree index is useful to answer queries with
    /// equality (`=`), inequality (`>`, `>=`, `<`, `<=`),and range queries.
    ///
    /// This is the default index type for scalar columns.
    BTree(BTreeIndexBuilder),

    /// A `Bitmap` index stores a bitmap for each distinct value in the column for every row.
    ///
    /// This index works best for low-cardinality columns,
    /// where the number of unique values is small (i.e., less than a few hundreds).
    Bitmap(BitmapIndexBuilder),

    /// [LabelListIndexBuilder] is a scalar index that can be used on `List<T>` columns to
    /// support queries with `array_contains_all` and `array_contains_any`
    /// using an underlying bitmap index.
    LabelList(LabelListIndexBuilder),

    /// Full text search index using bm25.
    FTS(FtsIndexBuilder),

    /// IVF index with Product Quantization
    IvfPq(IvfPqIndexBuilder),

    /// IVF-HNSW index with Product Quantization
    /// It is a variant of the HNSW algorithm that uses product quantization to compress the vectors.
    IvfHnswPq(IvfHnswPqIndexBuilder),

    /// IVF-HNSW index with Scalar Quantization
    /// It is a variant of the HNSW algorithm that uses scalar quantization to compress the vectors.
    IvfHnswSq(IvfHnswSqIndexBuilder),
}

/// Builder for the create_index operation
///
/// The methods on this builder are used to specify options common to all indices.
pub struct IndexBuilder {
    parent: Arc<dyn TableInternal>,
    pub(crate) index: Index,
    pub(crate) columns: Vec<String>,
    pub(crate) replace: bool,
}

impl IndexBuilder {
    pub(crate) fn new(parent: Arc<dyn TableInternal>, columns: Vec<String>, index: Index) -> Self {
        Self {
            parent,
            index,
            columns,
            replace: true,
        }
    }

    /// Whether to replace the existing index, the default is `true`.
    ///
    /// If this is false, and another index already exists on the same columns
    /// and the same name, then an error will be returned.  This is true even if
    /// that index is out of date.
    pub fn replace(mut self, v: bool) -> Self {
        self.replace = v;
        self
    }

    pub async fn execute(self) -> Result<()> {
        self.parent.clone().create_index(self).await
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum IndexType {
    // Vector
    #[serde(alias = "IVF_PQ")]
    IvfPq,
    #[serde(alias = "IVF_HNSW_PQ")]
    IvfHnswPq,
    #[serde(alias = "IVF_HNSW_SQ")]
    IvfHnswSq,
    // Scalar
    #[serde(alias = "BTREE")]
    BTree,
    #[serde(alias = "BITMAP")]
    Bitmap,
    #[serde(alias = "LABEL_LIST")]
    LabelList,
    // FTS
    FTS,
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::IvfPq => write!(f, "IVF_PQ"),
            Self::IvfHnswPq => write!(f, "IVF_HNSW_PQ"),
            Self::IvfHnswSq => write!(f, "IVF_HNSW_SQ"),
            Self::BTree => write!(f, "BTREE"),
            Self::Bitmap => write!(f, "BITMAP"),
            Self::LabelList => write!(f, "LABEL_LIST"),
            Self::FTS => write!(f, "FTS"),
        }
    }
}

impl std::str::FromStr for IndexType {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_uppercase().as_str() {
            "BTREE" => Ok(Self::BTree),
            "BITMAP" => Ok(Self::Bitmap),
            "LABEL_LIST" | "LABELLIST" => Ok(Self::LabelList),
            "FTS" => Ok(Self::FTS),
            "IVF_PQ" => Ok(Self::IvfPq),
            "IVF_HNSW_PQ" => Ok(Self::IvfHnswPq),
            "IVF_HNSW_SQ" => Ok(Self::IvfHnswSq),
            _ => Err(Error::InvalidInput {
                message: format!("the input value {} is not a valid IndexType", value),
            }),
        }
    }
}

/// A description of an index currently configured on a column
#[derive(Debug, PartialEq, Clone)]
pub struct IndexConfig {
    /// The name of the index
    pub name: String,
    /// The type of the index
    pub index_type: IndexType,
    /// The columns in the index
    ///
    /// Currently this is always a Vec of size 1.  In the future there may
    /// be more columns to represent composite indices.
    pub columns: Vec<String>,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize)]
pub(crate) struct IndexMetadata {
    pub metric_type: Option<DistanceType>,
    // Sometimes the index type is provided at this level.
    pub index_type: Option<IndexType>,
}

// This struct is used to deserialize the JSON data returned from the Lance API
// Dataset::index_statistics().
#[skip_serializing_none]
#[derive(Debug, Deserialize)]
pub(crate) struct IndexStatisticsImpl {
    pub num_indexed_rows: usize,
    pub num_unindexed_rows: usize,
    pub indices: Vec<IndexMetadata>,
    // Sometimes, the index type is provided at this level.
    pub index_type: Option<IndexType>,
    pub num_indices: Option<u32>,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, PartialEq)]
pub struct IndexStatistics {
    /// The number of rows in the table that are covered by this index.
    pub num_indexed_rows: usize,
    /// The number of rows in the table that are not covered by this index.
    /// These are rows that haven't yet been added to the index.
    pub num_unindexed_rows: usize,
    /// The type of the index.
    pub index_type: IndexType,
    /// The distance type used by the index.
    ///
    /// This is only present for vector indices.
    pub distance_type: Option<DistanceType>,
    /// The number of parts this index is split into.
    pub num_indices: Option<u32>,
}
