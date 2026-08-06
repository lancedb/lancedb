// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, Result};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

pub mod metrics;
pub mod scalar;

/// Generic methods common across all types of secondary indices
///
#[async_trait]
pub trait Index: Send + Sync + DeepSizeOf {
    /// Cast to [Any].
    fn as_any(&self) -> &dyn Any;

    /// Cast to [Index]
    fn as_index(self: Arc<Self>) -> Arc<dyn Index>;

    /// Retrieve index statistics as a JSON Value
    fn statistics(&self) -> Result<serde_json::Value>;

    /// Prewarm the index.
    ///
    /// This will load the index into memory and cache it.
    async fn prewarm(&self) -> Result<()>;

    /// Get the type of the index
    fn index_type(&self) -> IndexType;

    /// Read through the index and determine which fragment ids are covered by the index
    ///
    /// This is a kind of slow operation.  It's better to use the fragment_bitmap.  This
    /// only exists for cases where the fragment_bitmap has become corrupted or missing.
    async fn calculate_included_frags(&self) -> Result<RoaringBitmap>;
}

/// Index Type
#[derive(Debug, PartialEq, Eq, Copy, Hash, Clone, DeepSizeOf, Serialize, Deserialize)]
pub enum IndexType {
    // Preserve 0-100 for simple indices.
    Scalar = 0, // Legacy scalar index, alias to BTree

    BTree = 1, // BTree

    Bitmap = 2, // Bitmap

    LabelList = 3, // LabelList

    Inverted = 4, // Inverted

    NGram = 5, // NGram

    FragmentReuse = 6,

    MemWal = 7,

    ZoneMap = 8, // ZoneMap

    BloomFilter = 9, // Bloom filter

    RTree = 10, // RTree

    Fm = 11, // FM-Index

    // 100+ and up for vector index.
    /// Flat vector index.
    Vector = 100, // Legacy vector index, alias to IvfPq
    IvfFlat = 101,
    IvfSq = 102,
    IvfPq = 103,
    IvfHnswSq = 104,
    IvfHnswPq = 105,
    IvfHnswFlat = 106,
    IvfRq = 107,
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Scalar | Self::BTree => write!(f, "BTree"),
            Self::Bitmap => write!(f, "Bitmap"),
            Self::LabelList => write!(f, "LabelList"),
            Self::Inverted => write!(f, "Inverted"),
            Self::NGram => write!(f, "NGram"),
            Self::FragmentReuse => write!(f, "FragmentReuse"),
            Self::MemWal => write!(f, "MemWal"),
            Self::ZoneMap => write!(f, "ZoneMap"),
            Self::BloomFilter => write!(f, "BloomFilter"),
            Self::RTree => write!(f, "RTree"),
            Self::Fm => write!(f, "Fm"),
            Self::Vector | Self::IvfPq => write!(f, "IVF_PQ"),
            Self::IvfFlat => write!(f, "IVF_FLAT"),
            Self::IvfSq => write!(f, "IVF_SQ"),
            Self::IvfHnswSq => write!(f, "IVF_HNSW_SQ"),
            Self::IvfHnswPq => write!(f, "IVF_HNSW_PQ"),
            Self::IvfHnswFlat => write!(f, "IVF_HNSW_FLAT"),
            Self::IvfRq => write!(f, "IVF_RQ"),
        }
    }
}

impl TryFrom<i32> for IndexType {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self> {
        match value {
            v if v == Self::Scalar as i32 => Ok(Self::Scalar),
            v if v == Self::BTree as i32 => Ok(Self::BTree),
            v if v == Self::Bitmap as i32 => Ok(Self::Bitmap),
            v if v == Self::LabelList as i32 => Ok(Self::LabelList),
            v if v == Self::NGram as i32 => Ok(Self::NGram),
            v if v == Self::Inverted as i32 => Ok(Self::Inverted),
            v if v == Self::FragmentReuse as i32 => Ok(Self::FragmentReuse),
            v if v == Self::MemWal as i32 => Ok(Self::MemWal),
            v if v == Self::ZoneMap as i32 => Ok(Self::ZoneMap),
            v if v == Self::BloomFilter as i32 => Ok(Self::BloomFilter),
            v if v == Self::RTree as i32 => Ok(Self::RTree),
            v if v == Self::Fm as i32 => Ok(Self::Fm),
            v if v == Self::Vector as i32 => Ok(Self::Vector),
            v if v == Self::IvfFlat as i32 => Ok(Self::IvfFlat),
            v if v == Self::IvfSq as i32 => Ok(Self::IvfSq),
            v if v == Self::IvfPq as i32 => Ok(Self::IvfPq),
            v if v == Self::IvfHnswSq as i32 => Ok(Self::IvfHnswSq),
            v if v == Self::IvfHnswPq as i32 => Ok(Self::IvfHnswPq),
            v if v == Self::IvfHnswFlat as i32 => Ok(Self::IvfHnswFlat),
            v if v == Self::IvfRq as i32 => Ok(Self::IvfRq),
            _ => Err(Error::invalid_input_source(
                format!("the input value {} is not a valid IndexType", value).into(),
            )),
        }
    }
}

impl TryFrom<&str> for IndexType {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            "BTree" | "BTREE" => Ok(Self::BTree),
            "Bitmap" | "BITMAP" => Ok(Self::Bitmap),
            "LabelList" | "LABELLIST" => Ok(Self::LabelList),
            "Inverted" | "INVERTED" => Ok(Self::Inverted),
            "NGram" | "NGRAM" => Ok(Self::NGram),
            "ZoneMap" | "ZONEMAP" => Ok(Self::ZoneMap),
            "BloomFilter" | "BLOOMFILTER" | "BLOOM_FILTER" => Ok(Self::BloomFilter),
            "RTree" | "RTREE" | "R_TREE" => Ok(Self::RTree),
            "Fm" | "FM" => Ok(Self::Fm),
            "Vector" | "VECTOR" => Ok(Self::Vector),
            "IVF_FLAT" => Ok(Self::IvfFlat),
            "IVF_SQ" => Ok(Self::IvfSq),
            "IVF_PQ" => Ok(Self::IvfPq),
            "IVF_RQ" => Ok(Self::IvfRq),
            "IVF_HNSW_FLAT" => Ok(Self::IvfHnswFlat),
            "IVF_HNSW_SQ" => Ok(Self::IvfHnswSq),
            "IVF_HNSW_PQ" => Ok(Self::IvfHnswPq),
            "FragmentReuse" => Ok(Self::FragmentReuse),
            "MemWal" => Ok(Self::MemWal),
            _ => Err(Error::invalid_input(format!(
                "invalid index type: {}",
                value
            ))),
        }
    }
}

impl IndexType {
    pub fn is_scalar(&self) -> bool {
        matches!(
            self,
            Self::Scalar
                | Self::BTree
                | Self::Bitmap
                | Self::LabelList
                | Self::Inverted
                | Self::NGram
                | Self::ZoneMap
                | Self::BloomFilter
                | Self::RTree
                | Self::Fm,
        )
    }

    pub fn is_vector(&self) -> bool {
        matches!(
            self,
            Self::Vector
                | Self::IvfPq
                | Self::IvfHnswSq
                | Self::IvfHnswPq
                | Self::IvfHnswFlat
                | Self::IvfFlat
                | Self::IvfSq
                | Self::IvfRq
        )
    }

    pub fn is_system(&self) -> bool {
        matches!(self, Self::FragmentReuse | Self::MemWal)
    }

    /// Returns the current format version of the index type,
    /// bump this when the index format changes.
    /// Indices which higher version than these will be ignored for compatibility,
    /// This would happen when creating index in a newer version of Lance,
    /// but then opening the index in older version of Lance
    pub fn version(&self) -> i32 {
        match self {
            Self::Scalar => 0,
            Self::BTree => 0,
            Self::Bitmap => 0,
            Self::LabelList => 0,
            Self::Inverted => 0,
            Self::NGram => 0,
            Self::FragmentReuse => 0,
            Self::MemWal => 0,
            Self::ZoneMap => 0,
            Self::BloomFilter => 0,
            Self::RTree => 0,
            Self::Fm => 0,

            // IMPORTANT: if any vector index subtype needs a format bump that is
            // not backward compatible, its new version must be set to
            // (current max vector index version + 1), even if only one subtype
            // changed. Compatibility filtering currently cannot distinguish vector
            // subtypes from details-only metadata, so vector versions effectively
            // share one global monotonic compatibility level.
            Self::Vector
            | Self::IvfFlat
            | Self::IvfSq
            | Self::IvfPq
            | Self::IvfHnswSq
            | Self::IvfHnswPq
            | Self::IvfHnswFlat => 1,
            Self::IvfRq => 2,
        }
    }

    /// Returns the target partition size for the index type.
    ///
    /// This is used to compute the number of partitions for the index.
    /// The partition size is optimized for the best performance of the index.
    ///
    /// This is for vector indices only.
    pub fn target_partition_size(&self) -> usize {
        match self {
            Self::Vector => 8192,
            Self::IvfFlat => 4096,
            Self::IvfSq => 8192,
            Self::IvfPq => 8192,
            Self::IvfRq => 4096,
            Self::IvfHnswFlat => 1 << 20,
            Self::IvfHnswSq => 1 << 20,
            Self::IvfHnswPq => 1 << 20,
            _ => 8192,
        }
    }

    /// Returns the highest supported vector index version in this Lance build.
    pub fn max_vector_version() -> u32 {
        [
            Self::Vector,
            Self::IvfFlat,
            Self::IvfSq,
            Self::IvfPq,
            Self::IvfHnswSq,
            Self::IvfHnswPq,
            Self::IvfHnswFlat,
            Self::IvfRq,
        ]
        .into_iter()
        .map(|index_type| index_type.version() as u32)
        .max()
        .unwrap_or(1)
    }

    pub fn matches_details(&self, details: &prost_types::Any) -> bool {
        let url = &details.type_url;
        match self {
            Self::Scalar | Self::BTree => url.ends_with("BTreeIndexDetails"),
            Self::Bitmap => url.ends_with("BitmapIndexDetails"),
            Self::LabelList => url.ends_with("LabelListIndexDetails"),
            Self::Inverted => url.ends_with("InvertedIndexDetails"),
            Self::NGram => url.ends_with("NGramIndexDetails"),
            Self::ZoneMap => url.ends_with("ZoneMapIndexDetails"),
            Self::BloomFilter => url.ends_with("BloomFilterIndexDetails"),
            Self::RTree => url.ends_with("RTreeIndexDetails"),
            Self::Fm => url.ends_with("FMIndexDetails"),
            Self::FragmentReuse => url.ends_with("FragmentReuseIndexDetails"),
            Self::MemWal => url.ends_with("MemWalIndexDetails"),
            Self::Vector
            | Self::IvfFlat
            | Self::IvfSq
            | Self::IvfPq
            | Self::IvfHnswSq
            | Self::IvfHnswPq
            | Self::IvfHnswFlat
            | Self::IvfRq => url.ends_with("VectorIndexDetails"),
        }
    }
}

pub trait IndexParams: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn index_name(&self) -> &str;
}
