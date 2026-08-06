// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
#![cfg_attr(coverage, feature(coverage_attribute))]

//! Lance secondary index library
//!
//! <section class="warning">
//! This is internal crate used by <a href="https://github.com/lance-format/lance">the lance project</a>.
//! <br/>
//! API stability is not guaranteed.
//! </section>

use crate::frag_reuse::FRAG_REUSE_INDEX_NAME;
use crate::mem_wal::MEM_WAL_INDEX_NAME;
use serde::{Deserialize, Serialize};

pub mod frag_reuse;
pub mod mem_wal;
pub mod metrics;
pub mod optimize;
pub mod prefilter;
pub mod progress;
pub mod registry;
pub mod scalar;
pub mod traits;
pub mod vector;

pub use crate::traits::*;

// Re-export core traits from lance-index-core
pub use lance_index_core::{Index, IndexParams, IndexType};

pub const INDEX_FILE_NAME: &str = "index.idx";
/// The name of the auxiliary index file.
///
/// This file is used to store additional information about the index, to improve performance.
/// - For 'IVF_HNSW' index, it stores the partitioned PQ Storage.
pub const INDEX_AUXILIARY_FILE_NAME: &str = "auxiliary.idx";
pub const INDEX_METADATA_SCHEMA_KEY: &str = "lance:index";

/// Default version for vector index metadata.
///
/// Most vector indices should use this version unless they need to bump for a
/// format change.
pub const VECTOR_INDEX_VERSION: u32 = 1;
/// Version for IVF_RQ indices.
pub const IVF_RQ_INDEX_VERSION: u32 = 2;

/// The factor of threshold to trigger split / join for vector index.
///
/// If the number of rows in the single partition is greater than `MAX_PARTITION_SIZE_FACTOR * target_partition_size`,
/// the partition will be split.
/// If the number of rows in the single partition is less than `MIN_PARTITION_SIZE_PERCENT *target_partition_size / 100`,
/// the partition will be joined.
pub const MAX_PARTITION_SIZE_FACTOR: usize = 4;
pub const MIN_PARTITION_SIZE_PERCENT: usize = 25;

pub mod pb {
    #![allow(clippy::use_self)]
    include!(concat!(env!("OUT_DIR"), "/lance.index.pb.rs"));
}

pub mod pbold {
    #![allow(clippy::use_self)]
    include!(concat!(env!("OUT_DIR"), "/lance.table.rs"));
}

/// Protobuf headers for serialized index cache entries (FTS posting lists,
/// scalar indices, and IVF vector partitions).
pub mod cache_pb {
    #![allow(clippy::use_self)]
    include!(concat!(env!("OUT_DIR"), "/lance.index.cache.rs"));
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexMetadata {
    #[serde(rename = "type")]
    pub index_type: String,
    pub distance_type: String,
}

pub fn is_system_index(index_meta: &lance_table::format::IndexMetadata) -> bool {
    index_meta.name == FRAG_REUSE_INDEX_NAME || index_meta.name == MEM_WAL_INDEX_NAME
}

pub fn infer_system_index_type(
    index_meta: &lance_table::format::IndexMetadata,
) -> Option<IndexType> {
    if index_meta.name == FRAG_REUSE_INDEX_NAME {
        Some(IndexType::FragmentReuse)
    } else if index_meta.name == MEM_WAL_INDEX_NAME {
        Some(IndexType::MemWal)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ivf_rq_has_dedicated_index_version() {
        assert!(IndexType::IvfRq.version() > IndexType::IvfPq.version());
        assert_eq!(IndexType::IvfRq.version() as u32, IVF_RQ_INDEX_VERSION);
    }

    #[test]
    fn test_max_vector_version_tracks_highest_supported() {
        assert_eq!(IndexType::max_vector_version(), IVF_RQ_INDEX_VERSION);
    }

    #[test]
    fn test_ivf_rq_target_partition_size() {
        assert_eq!(IndexType::IvfRq.target_partition_size(), 4096);
    }

    #[test]
    fn test_index_type_try_from_i32_covers_all_variants() {
        let all = [
            IndexType::Scalar,
            IndexType::BTree,
            IndexType::Bitmap,
            IndexType::LabelList,
            IndexType::Inverted,
            IndexType::NGram,
            IndexType::FragmentReuse,
            IndexType::MemWal,
            IndexType::ZoneMap,
            IndexType::BloomFilter,
            IndexType::RTree,
            IndexType::Fm,
            IndexType::Vector,
            IndexType::IvfFlat,
            IndexType::IvfSq,
            IndexType::IvfPq,
            IndexType::IvfHnswSq,
            IndexType::IvfHnswPq,
            IndexType::IvfHnswFlat,
            IndexType::IvfRq,
        ];

        for index_type in all {
            assert_eq!(
                IndexType::try_from(index_type as i32).unwrap(),
                index_type,
                "IndexType::try_from(i32) should support {:?}",
                index_type
            );
        }
    }

    #[test]
    fn test_index_type_try_from_str_covers_all_parseable_variants() {
        let cases = [
            ("BTree", IndexType::BTree),
            ("BTREE", IndexType::BTree),
            ("Bitmap", IndexType::Bitmap),
            ("BITMAP", IndexType::Bitmap),
            ("LabelList", IndexType::LabelList),
            ("LABELLIST", IndexType::LabelList),
            ("Inverted", IndexType::Inverted),
            ("INVERTED", IndexType::Inverted),
            ("NGram", IndexType::NGram),
            ("NGRAM", IndexType::NGram),
            ("ZoneMap", IndexType::ZoneMap),
            ("ZONEMAP", IndexType::ZoneMap),
            ("BloomFilter", IndexType::BloomFilter),
            ("BLOOMFILTER", IndexType::BloomFilter),
            ("BLOOM_FILTER", IndexType::BloomFilter),
            ("RTree", IndexType::RTree),
            ("RTREE", IndexType::RTree),
            ("R_TREE", IndexType::RTree),
            ("Fm", IndexType::Fm),
            ("FM", IndexType::Fm),
            ("Vector", IndexType::Vector),
            ("VECTOR", IndexType::Vector),
            ("IVF_FLAT", IndexType::IvfFlat),
            ("IVF_SQ", IndexType::IvfSq),
            ("IVF_PQ", IndexType::IvfPq),
            ("IVF_RQ", IndexType::IvfRq),
            ("IVF_HNSW_FLAT", IndexType::IvfHnswFlat),
            ("IVF_HNSW_SQ", IndexType::IvfHnswSq),
            ("IVF_HNSW_PQ", IndexType::IvfHnswPq),
            ("FragmentReuse", IndexType::FragmentReuse),
            ("MemWal", IndexType::MemWal),
        ];

        for (text, expected) in cases {
            assert_eq!(
                IndexType::try_from(text).unwrap(),
                expected,
                "IndexType::try_from(&str) should support '{text}'"
            );
        }
    }
}
