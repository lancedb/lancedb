// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use chrono::{DateTime, Utc};
use scalar::{FtsIndexBuilder, FtsStopWordsSource, normalize_stop_words};
use serde::Deserialize;
use serde_with::skip_serializing_none;
use std::sync::Arc;
use std::time::Duration;
use vector::IvfFlatIndexBuilder;

use crate::index::vector::IvfRqIndexBuilder;
use crate::{DistanceType, Error, Result, table::BaseTable};

use self::{
    scalar::{BTreeIndexBuilder, BitmapIndexBuilder, FmIndexBuilder, LabelListIndexBuilder},
    vector::{
        IvfHnswFlatIndexBuilder, IvfHnswPqIndexBuilder, IvfHnswSqIndexBuilder, IvfPqIndexBuilder,
        IvfSqIndexBuilder,
    },
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

    /// An `FM` index is a scalar index on string/binary columns that accelerates
    /// substring search (`contains(col, 'needle')`). It matches arbitrary
    /// substrings of the raw bytes, unlike the tokenized [`Index::FTS`] index.
    Fm(FmIndexBuilder),

    /// Full text search index using BM25.
    ///
    /// The posting block size defaults to 128. Supported values are 128 and 256;
    /// a value of 256 uses the experimental FTS V3 format and may introduce
    /// breaking changes.
    ///
    /// ```
    /// use lancedb::index::{Index, scalar::FtsIndexBuilder};
    ///
    /// # async fn create_fts_index(
    /// #     table: &lancedb::Table,
    /// # ) -> Result<(), Box<dyn std::error::Error>> {
    /// let params = FtsIndexBuilder::default().block_size(256)?;
    /// table
    ///     .create_index(&["text"], Index::FTS(params))
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    FTS(FtsIndexBuilder),

    /// IVF index
    IvfFlat(IvfFlatIndexBuilder),

    /// IVF index with Product Quantization
    IvfPq(IvfPqIndexBuilder),

    /// IVF index with Scalar Quantization
    IvfSq(IvfSqIndexBuilder),

    /// IVF index with RabitQ Quantization
    IvfRq(IvfRqIndexBuilder),

    /// IVF-HNSW index with Product Quantization
    /// It is a variant of the HNSW algorithm that uses product quantization to compress the vectors.
    IvfHnswPq(IvfHnswPqIndexBuilder),

    /// IVF-HNSW index with Scalar Quantization
    /// It is a variant of the HNSW algorithm that uses scalar quantization to compress the vectors.
    IvfHnswSq(IvfHnswSqIndexBuilder),

    /// IVF-HNSW index without quantization.
    /// Stores raw vectors, providing the highest recall at the cost of more memory and disk space.
    IvfHnswFlat(IvfHnswFlatIndexBuilder),
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
    custom_stop_words_source: Option<FtsStopWordsSource>,
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
            custom_stop_words_source: None,
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

    /// Configure a mutually exclusive source of custom FTS stop words.
    ///
    /// The source is resolved on the client when [`Self::execute`] runs. Files
    /// are read as UTF-8 newline-delimited text, and table sources project and
    /// stream only the selected string column. The resulting concrete list is
    /// persisted as the index's tokenizer snapshot and is the only value sent
    /// to a remote server.
    ///
    /// Custom stop words replace the built-in language stop words and are only
    /// used when `remove_stop_words` is enabled in [`FtsIndexBuilder`].
    /// Native FTS queries using an active custom snapshot reject explicit
    /// positive fuzziness because Lance's fuzzy path currently bypasses the
    /// persisted tokenizer. Remote tables reject explicit positive fuzziness
    /// until the server protocol declares snapshot-safe fuzzy support.
    ///
    /// ```
    /// use lancedb::index::{
    ///     Index,
    ///     scalar::{FtsIndexBuilder, FtsStopWordsSource},
    /// };
    ///
    /// # async fn create_index(table: &lancedb::Table) -> lancedb::Result<()> {
    /// table
    ///     .create_index(&["text"], Index::FTS(FtsIndexBuilder::default()))
    ///     .custom_stop_words(FtsStopWordsSource::file("stop-words.txt"))?
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an FTS index, a source was already set,
    /// or the supplied [`FtsIndexBuilder`] already contains a custom stop-word
    /// list.
    pub fn custom_stop_words(mut self, source: impl Into<FtsStopWordsSource>) -> Result<Self> {
        if self.custom_stop_words_source.is_some() {
            return Err(Error::InvalidInput {
                message: "custom stop words source was set more than once".to_string(),
            });
        }
        let Index::FTS(params) = &self.index else {
            return Err(Error::InvalidInput {
                message: "custom stop words can only be configured for an FTS index".to_string(),
            });
        };
        if fts_params_have_custom_stop_words(params)? {
            return Err(Error::InvalidInput {
                message: "custom stop words are already configured directly on FtsIndexBuilder; use either FtsIndexBuilder::custom_stop_words or IndexBuilder::custom_stop_words, not both".to_string(),
            });
        }
        self.custom_stop_words_source = Some(source.into());
        Ok(self)
    }

    pub async fn execute(mut self) -> Result<()> {
        let direct_custom_stop_words = match &self.index {
            Index::FTS(params) => fts_params_custom_stop_words(params)?,
            _ => None,
        };
        if let Some(source) = self.custom_stop_words_source.take() {
            let snapshot = source.resolve().await?;
            let Index::FTS(params) = &mut self.index else {
                return Err(Error::InvalidInput {
                    message: "custom stop words can only be configured for an FTS index"
                        .to_string(),
                });
            };
            if fts_params_have_custom_stop_words(params)? {
                return Err(Error::InvalidInput {
                    message: "custom stop words are already configured directly on FtsIndexBuilder; use either FtsIndexBuilder::custom_stop_words or IndexBuilder::custom_stop_words, not both".to_string(),
                });
            }
            *params = params.clone().custom_stop_words(Some(snapshot));
        } else if direct_custom_stop_words.is_some()
            && let Index::FTS(params) = &mut self.index
        {
            *params = canonicalize_fts_params(params)?;
        }
        self.parent.clone().create_index(self).await
    }
}

fn fts_params_have_custom_stop_words(params: &FtsIndexBuilder) -> Result<bool> {
    Ok(fts_params_custom_stop_words(params)?.is_some())
}

pub(crate) fn fts_params_custom_stop_words(
    params: &FtsIndexBuilder,
) -> Result<Option<Vec<String>>> {
    let value = serde_json::to_value(params).map_err(|e| Error::InvalidInput {
        message: format!("failed to inspect FTS params for custom stop words: {}", e),
    })?;
    match value.get("custom_stop_words") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|e| Error::InvalidInput {
                message: format!("invalid custom stop words in FTS params: {}", e),
            }),
    }
}

pub(crate) fn canonicalize_fts_params(params: &FtsIndexBuilder) -> Result<FtsIndexBuilder> {
    match fts_params_custom_stop_words(params)? {
        Some(stop_words) => Ok(params
            .clone()
            .custom_stop_words(Some(normalize_stop_words(stop_words)))),
        None => Ok(params.clone()),
    }
}

pub(crate) fn fts_params_use_custom_stop_words(params: &FtsIndexBuilder) -> Result<bool> {
    let value = serde_json::to_value(params).map_err(|e| Error::InvalidInput {
        message: format!("failed to inspect FTS params for stop-word removal: {}", e),
    })?;
    let remove_stop_words = value
        .get("remove_stop_words")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| Error::InvalidInput {
            message: "invalid remove_stop_words value in FTS params".to_string(),
        })?;
    Ok(remove_stop_words
        && !matches!(
            value.get("custom_stop_words"),
            None | Some(serde_json::Value::Null)
        ))
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum IndexType {
    // Vector
    #[serde(alias = "IVF_FLAT")]
    IvfFlat,
    #[serde(alias = "IVF_SQ")]
    IvfSq,
    #[serde(alias = "IVF_PQ")]
    IvfPq,
    #[serde(alias = "IVF_RQ")]
    IvfRq,
    #[serde(alias = "IVF_HNSW_PQ")]
    IvfHnswPq,
    #[serde(alias = "IVF_HNSW_SQ")]
    IvfHnswSq,
    #[serde(alias = "IVF_HNSW_FLAT")]
    IvfHnswFlat,
    // Scalar
    #[serde(alias = "BTREE")]
    BTree,
    #[serde(alias = "BITMAP")]
    Bitmap,
    #[serde(alias = "LABEL_LIST")]
    LabelList,
    #[serde(alias = "FM", alias = "FMINDEX", alias = "FMIndex")]
    Fm,
    // FTS
    #[serde(alias = "INVERTED", alias = "Inverted")]
    FTS,
    /// Catch-all for index types not recognized by this version of LanceDB.
    Unknown,
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::IvfFlat => write!(f, "IVF_FLAT"),
            Self::IvfSq => write!(f, "IVF_SQ"),
            Self::IvfPq => write!(f, "IVF_PQ"),
            Self::IvfRq => write!(f, "IVF_RQ"),
            Self::IvfHnswPq => write!(f, "IVF_HNSW_PQ"),
            Self::IvfHnswSq => write!(f, "IVF_HNSW_SQ"),
            Self::IvfHnswFlat => write!(f, "IVF_HNSW_FLAT"),
            Self::BTree => write!(f, "BTREE"),
            Self::Bitmap => write!(f, "BITMAP"),
            Self::LabelList => write!(f, "LABEL_LIST"),
            Self::Fm => write!(f, "FM"),
            Self::FTS => write!(f, "FTS"),
            Self::Unknown => write!(f, "UNKNOWN"),
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
            "FM" | "FMINDEX" => Ok(Self::Fm),
            "FTS" | "INVERTED" => Ok(Self::FTS),
            "IVF_FLAT" => Ok(Self::IvfFlat),
            "IVF_SQ" => Ok(Self::IvfSq),
            "IVF_PQ" => Ok(Self::IvfPq),
            "IVF_RQ" => Ok(Self::IvfRq),
            "IVF_HNSW_PQ" => Ok(Self::IvfHnswPq),
            "IVF_HNSW_SQ" => Ok(Self::IvfHnswSq),
            "IVF_HNSW_FLAT" => Ok(Self::IvfHnswFlat),
            _ => Ok(Self::Unknown),
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
    /// The UUID of the first segment of the index.
    ///
    /// An index may be made up of multiple segments, each with their own UUID.
    /// This is the UUID of the first segment. `None` if it could not be
    /// determined (e.g. for remote tables, which do not yet surface this).
    pub index_uuid: Option<String>,
    /// The protobuf type URL, a precise type identifier for the index.
    ///
    /// `None` if unavailable (e.g. for remote tables).
    pub type_url: Option<String>,
    /// When the index was created, taken as the minimum creation time across
    /// all segments.
    ///
    /// `None` if unavailable, such as for indices created before creation
    /// timestamps were tracked, or for remote tables.
    pub created_at: Option<DateTime<Utc>>,
    /// The number of rows indexed, across all segments.
    ///
    /// This is approximate and may include rows that have since been deleted.
    /// `None` if unavailable (e.g. for remote tables).
    pub num_indexed_rows: Option<u64>,
    /// The number of rows in the table that are not yet covered by this index.
    ///
    /// Computed as the table's total row count minus [`Self::num_indexed_rows`].
    /// Optimizing the index will fold these rows into it. `None` if unavailable
    /// (e.g. for remote tables).
    pub num_unindexed_rows: Option<u64>,
    /// The total size in bytes of all index files across all segments.
    ///
    /// `None` if size information is unavailable, such as for indices created
    /// before file sizes were tracked, or for remote tables.
    pub size_bytes: Option<u64>,
    /// The number of segments that make up the index.
    ///
    /// `None` if unavailable (e.g. for remote tables).
    pub num_segments: Option<u32>,
    /// The on-disk index format version, taken from the first segment.
    ///
    /// `None` if unavailable (e.g. for remote tables).
    pub index_version: Option<i32>,
    /// Index-type-specific details, serialized as JSON.
    ///
    /// The shape of this JSON varies by index type. `None` if the details
    /// could not be produced (e.g. no plugin available) or for remote tables.
    pub index_details: Option<String>,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize)]
pub(crate) struct IndexMetadata {
    pub metric_type: Option<DistanceType>,
}

// Deserializes the JSON returned by Dataset::index_statistics().
#[skip_serializing_none]
#[derive(Debug, Deserialize)]
pub(crate) struct IndexStatisticsImpl {
    pub num_indexed_rows: usize,
    pub num_unindexed_rows: usize,
    pub indices: Vec<IndexMetadata>,
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
