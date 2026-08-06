// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! `Index`-trait adapter for the MemWAL system index.
//!
//! The data structures and table-format logic live in
//! [`lance_table::system_index::mem_wal`]; this module re-exports them and
//! provides a newtype wrapper that implements the [`Index`] trait.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use lance_core::Result;
use lance_core::deepsize::DeepSizeOf;
use roaring::RoaringBitmap;
use serde::Serialize;

pub use lance_table::system_index::mem_wal::*;

use crate::{Index, IndexType};

/// Newtype wrapping [`MemWalIndex`] so that `lance-index` can implement
/// the `Index` trait (orphan rules prevent implementing it directly in
/// `lance-table`).
pub struct MemWalIndexHandle(pub Arc<MemWalIndex>);

impl DeepSizeOf for MemWalIndexHandle {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.0.deep_size_of_children(context)
    }
}

#[derive(Serialize)]
struct MemWalStatistics {
    num_shards: u32,
    num_compacted_sstables: usize,
    num_shard_specs: usize,
    num_maintained_indexes: usize,
    num_index_catchup_entries: usize,
}

#[async_trait]
impl Index for MemWalIndexHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    fn statistics(&self) -> Result<serde_json::Value> {
        let stats = MemWalStatistics {
            num_shards: self.0.details.num_shards,
            num_compacted_sstables: self.0.details.compacted_sstables.len(),
            num_shard_specs: self.0.details.sharding_specs.len(),
            num_maintained_indexes: self.0.details.maintained_indexes.len(),
            num_index_catchup_entries: self.0.details.index_catchup.len(),
        };
        serde_json::to_value(stats).map_err(|e| {
            lance_core::Error::internal(format!(
                "failed to serialize MemWAL index statistics: {}",
                e
            ))
        })
    }

    async fn prewarm(&self) -> Result<()> {
        Ok(())
    }

    fn index_type(&self) -> IndexType {
        IndexType::MemWal
    }

    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        Ok(RoaringBitmap::new())
    }
}
