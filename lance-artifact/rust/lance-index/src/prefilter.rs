// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use async_trait::async_trait;
use lance_core::Result;
use lance_select::RowAddrMask;

/// A trait to be implemented by anything supplying a prefilter row addr mask
///
/// This trait is for internal use only and has no stability guarantees.
#[async_trait]
pub trait FilterLoader: Send + 'static {
    async fn load(self: Box<Self>) -> Result<RowAddrMask>;
}

/// Filter out row ids that we know are not relevant to the query.
///
/// This could be both rows that are deleted or a prefilter
/// that should be applied to the search
///
/// <section class="warning">
/// Internal use only. No API stability guarantees.
/// </section>
#[async_trait]
pub trait PreFilter: Send + Sync {
    /// Waits for the prefilter to be fully loaded
    ///
    /// The prefilter loads in the background while the rest of the index
    /// search is running.  When you are ready to use the prefilter you
    /// must first call this method to ensure it is fully loaded.  This
    /// allows `filter_row_ids` to be a synchronous method.
    async fn wait_for_ready(&self) -> Result<()>;

    /// If the filter is empty.
    fn is_empty(&self) -> bool;

    /// Get the row addr mask for this prefilter
    ///
    /// This method must be called after `wait_for_ready`
    fn mask(&self) -> Arc<RowAddrMask>;

    /// Check whether a slice of row ids should be included in a query.
    ///
    /// Returns a vector of indices into the input slice that should be included,
    /// also known as a selection vector.
    ///
    /// This method must be called after `wait_for_ready`
    fn filter_row_ids<'a>(&self, row_ids: Box<dyn Iterator<Item = &'a u64> + 'a>) -> Vec<u64>;
}

/// A prefilter that does nothing
pub struct NoFilter;

#[async_trait]
impl PreFilter for NoFilter {
    async fn wait_for_ready(&self) -> Result<()> {
        Ok(())
    }

    fn is_empty(&self) -> bool {
        true
    }

    fn mask(&self) -> Arc<RowAddrMask> {
        Arc::new(RowAddrMask::all_rows())
    }

    fn filter_row_ids<'a>(&self, row_ids: Box<dyn Iterator<Item = &'a u64> + 'a>) -> Vec<u64> {
        row_ids.enumerate().map(|(i, _)| i as u64).collect()
    }
}
