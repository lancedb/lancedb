// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use scalar::FtsIndexBuilder;
use serde::Deserialize;
use serde_with::skip_serializing_none;
use std::sync::Arc;
use std::time::Duration;
use vector::IvfFlatIndexBuilder;

use crate::{table::BaseTable, DistanceType, Error, Result};

use self::{
    scalar::{BTreeIndexBuilder, BitmapIndexBuilder, LabelListIndexBuilder},
    vector::{IvfHnswPqIndexBuilder, IvfHnswSqIndexBuilder, IvfPqIndexBuilder},
};

pub mod scalar;
pub mod vector;
pub mod waiter;

/// Supported index types.
#[derive(Debug, Clone)]
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

    /// IVF index
    IvfFlat(IvfFlatIndexBuilder),

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
///
/// # Examples
///
/// Creating a basic vector index:
///
/// ```
/// use lancedb::{connect, index::{Index, vector::IvfPqIndexBuilder}};
///
/// # async fn create_basic_vector_index() -> lancedb::Result<()> {
/// let db = connect("data/sample-lancedb").execute().await?;
/// let table = db.open_table("my_table").execute().await?;
///
/// // Create a vector index with default settings
/// table
///     .create_index(&["vector"], Index::IvfPq(IvfPqIndexBuilder::default()))
///     .execute()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// Creating an index with a custom name:
///
/// ```
/// use lancedb::{connect, index::{Index, vector::IvfPqIndexBuilder}};
///
/// # async fn create_named_index() -> lancedb::Result<()> {
/// let db = connect("data/sample-lancedb").execute().await?;
/// let table = db.open_table("my_table").execute().await?;
///
/// // Create a vector index with a custom name
/// table
///     .create_index(&["embeddings"], Index::IvfPq(IvfPqIndexBuilder::default()))
///     .name("my_embeddings_index".to_string())
///     .execute()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// Creating an untrained index (for scalar indices only):
///
/// ```
/// use lancedb::{connect, index::{Index, scalar::BTreeIndexBuilder}};
///
/// # async fn create_untrained_index() -> lancedb::Result<()> {
/// let db = connect("data/sample-lancedb").execute().await?;
/// let table = db.open_table("my_table").execute().await?;
///
/// // Create a BTree index without training (creates empty index)
/// table
///     .create_index(&["category"], Index::BTree(BTreeIndexBuilder::default()))
///     .train(false)
///     .name("category_index".to_string())
///     .execute()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// Creating a scalar index with all options:
///
/// ```
/// use lancedb::{connect, index::{Index, scalar::BitmapIndexBuilder}};
///
/// # async fn create_full_options_index() -> lancedb::Result<()> {
/// let db = connect("data/sample-lancedb").execute().await?;
/// let table = db.open_table("my_table").execute().await?;
///
/// // Create a bitmap index with full configuration
/// table
///     .create_index(&["status"], Index::Bitmap(BitmapIndexBuilder::default()))
///     .name("status_bitmap_index".to_string())
///     .train(true)  // Train the index with existing data
///     .replace(false)  // Don't replace if index already exists
///     .execute()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct IndexBuilder {
    parent: Arc<dyn BaseTable>,
    pub(crate) index: Index,
    pub(crate) columns: Vec<String>,
    pub(crate) replace: bool,
    pub(crate) wait_timeout: Option<Duration>,
    pub(crate) train: bool,
    pub(crate) name: Option<String>,
}

impl IndexBuilder {
    pub(crate) fn new(parent: Arc<dyn BaseTable>, columns: Vec<String>, index: Index) -> Self {
        Self {
            parent,
            index,
            columns,
            replace: true,
            train: true,
            wait_timeout: None,
            name: None,
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

    /// The name of the index. If not set, a default name will be generated.
    ///
    /// # Examples
    ///
    /// ```
    /// use lancedb::{connect, index::{Index, scalar::BTreeIndexBuilder}};
    ///
    /// # async fn name_example() -> lancedb::Result<()> {
    /// let db = connect("data/sample-lancedb").execute().await?;
    /// let table = db.open_table("my_table").execute().await?;
    ///
    /// // Create an index with a custom name
    /// table
    ///     .create_index(&["user_id"], Index::BTree(BTreeIndexBuilder::default()))
    ///     .name("user_id_btree_index".to_string())
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn name(mut self, v: String) -> Self {
        self.name = Some(v);
        self
    }

    /// Whether to train the index, the default is `true`.
    ///
    /// If this is false, the index will not be trained and just created empty.
    ///
    /// This is not supported for vector indices yet.
    ///
    /// # Examples
    ///
    /// Creating an empty index that will be populated later:
    ///
    /// ```
    /// use lancedb::{connect, index::{Index, scalar::BitmapIndexBuilder}};
    ///
    /// # async fn train_false_example() -> lancedb::Result<()> {
    /// let db = connect("data/sample-lancedb").execute().await?;
    /// let table = db.open_table("my_table").execute().await?;
    ///
    /// // Create an empty bitmap index (not trained with existing data)
    /// table
    ///     .create_index(&["category"], Index::Bitmap(BitmapIndexBuilder::default()))
    ///     .train(false)  // Create empty index
    ///     .name("category_bitmap".to_string())
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Creating a trained index (default behavior):
    ///
    /// ```
    /// use lancedb::{connect, index::{Index, scalar::BTreeIndexBuilder}};
    ///
    /// # async fn train_true_example() -> lancedb::Result<()> {
    /// let db = connect("data/sample-lancedb").execute().await?;
    /// let table = db.open_table("my_table").execute().await?;
    ///
    /// // Create a trained BTree index (includes existing data)
    /// table
    ///     .create_index(&["timestamp"], Index::BTree(BTreeIndexBuilder::default()))
    ///     .train(true)  // Train with existing data (this is the default)
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn train(mut self, v: bool) -> Self {
        self.train = v;
        self
    }

    /// Duration of time to wait for asynchronous indexing to complete. If not set,
    /// `create_index()` will not wait.
    ///
    /// This is not supported for `NativeTable` since indexing is synchronous.
    pub fn wait_timeout(mut self, d: Duration) -> Self {
        self.wait_timeout = Some(d);
        self
    }

    pub async fn execute(self) -> Result<()> {
        self.parent.clone().create_index(self).await
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum IndexType {
    // Vector
    #[serde(alias = "IVF_FLAT")]
    IvfFlat,
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
    #[serde(alias = "INVERTED", alias = "Inverted")]
    FTS,
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::IvfFlat => write!(f, "IVF_FLAT"),
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
            "FTS" | "INVERTED" => Ok(Self::FTS),
            "IVF_FLAT" => Ok(Self::IvfFlat),
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
    pub loss: Option<f64>,
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
    /// The loss value used by the index.
    pub loss: Option<f64>,
}
