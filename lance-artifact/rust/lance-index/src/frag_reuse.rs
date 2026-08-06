// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! `Index`-trait adapter for the fragment-reuse system index.
//!
//! The data structures and table-format logic live in
//! [`lance_table::system_index::frag_reuse`]; this module re-exports them and
//! provides newtype wrappers that implement the [`Index`] and [`RowIdRemapper`]
//! traits.

use std::any::Any;
use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use lance_core::Result;
use lance_core::deepsize::DeepSizeOf;
use lance_select::RowAddrTreeMap;
use roaring::{RoaringBitmap, RoaringTreemap};
use serde::Serialize;

pub use lance_table::system_index::frag_reuse::*;

use crate::scalar::RowIdRemapper;
use crate::{Index, IndexType};

/// Newtype wrapping [`FragReuseIndex`] so that `lance-index` can implement
/// the `Index` and `RowIdRemapper` traits (orphan rules prevent implementing
/// them directly in `lance-table`).
pub struct FragReuseIndexHandle(pub Arc<FragReuseIndex>);

impl std::fmt::Debug for FragReuseIndexHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("FragReuseIndexHandle")
            .field(&self.0)
            .finish()
    }
}

impl DeepSizeOf for FragReuseIndexHandle {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.0.deep_size_of_children(context)
    }
}

#[derive(Serialize)]
struct FragReuseStatistics {
    num_versions: usize,
}

#[async_trait]
impl Index for FragReuseIndexHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    fn statistics(&self) -> Result<serde_json::Value> {
        let stats = FragReuseStatistics {
            num_versions: self.0.details.versions.len(),
        };
        serde_json::to_value(stats).map_err(|e| {
            lance_core::Error::internal(format!(
                "failed to serialize fragment reuse index statistics: {}",
                e
            ))
        })
    }

    async fn prewarm(&self) -> Result<()> {
        Ok(())
    }

    fn index_type(&self) -> IndexType {
        IndexType::FragmentReuse
    }

    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        unimplemented!()
    }
}

impl RowIdRemapper for FragReuseIndexHandle {
    fn remap_row_id(&self, row_id: u64) -> Option<u64> {
        self.0.remap_row_id(row_id)
    }

    fn remap_row_addrs_tree_map(&self, row_addrs: &RowAddrTreeMap) -> RowAddrTreeMap {
        self.0.remap_row_addrs_tree_map(row_addrs)
    }

    fn remap_row_ids_roaring_tree_map(&self, row_ids: &RoaringTreemap) -> RoaringTreemap {
        self.0.remap_row_ids_roaring_tree_map(row_ids)
    }

    fn remap_row_ids_record_batch(
        &self,
        batch: RecordBatch,
        row_id_idx: usize,
    ) -> Result<RecordBatch> {
        self.0.remap_row_ids_record_batch(batch, row_id_idx)
    }
}
