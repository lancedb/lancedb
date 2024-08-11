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

use crate::{table::TableInternal, Result};

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
    IvfHnswPq(IvfHnswPqIndexBuilder),

    /// IVF-HNSW index with Scalar Quantization
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

#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    // Vector
    IvfPq,
    IvfHnswPq,
    IvfHnswSq,
    // Scalar
    BTree,
    Bitmap,
    LabelList,
}

/// A description of an index currently configured on a column
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
pub struct IndexMetadata {
    pub metric_type: Option<String>,
    pub index_type: Option<String>,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize)]
pub struct IndexStatistics {
    pub num_indexed_rows: usize,
    pub num_unindexed_rows: usize,
    pub index_type: Option<String>,
    pub indices: Vec<IndexMetadata>,
}
