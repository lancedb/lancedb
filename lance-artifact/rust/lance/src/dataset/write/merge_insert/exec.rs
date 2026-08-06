// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

mod delete;
mod write;

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
use futures::StreamExt;
use lance_table::format::Fragment;
use roaring::RoaringTreemap;

pub use delete::DeleteOnlyMergeInsertExec;
pub use write::FullSchemaMergeInsertExec;

use super::MergeStats;
use crate::Dataset;

pub(super) struct MergeInsertMetrics {
    pub num_inserted_rows: Count,
    pub num_updated_rows: Count,
    pub num_deleted_rows: Count,
    pub bytes_written: Count,
    pub num_files_written: Count,
    pub num_skipped_duplicates: Count,
}

impl From<&MergeInsertMetrics> for MergeStats {
    fn from(value: &MergeInsertMetrics) -> Self {
        Self {
            num_deleted_rows: value.num_deleted_rows.value() as u64,
            num_inserted_rows: value.num_inserted_rows.value() as u64,
            num_updated_rows: value.num_updated_rows.value() as u64,
            bytes_written: value.bytes_written.value() as u64,
            num_files_written: value.num_files_written.value() as u64,
            num_skipped_duplicates: value.num_skipped_duplicates.value() as u64,
            num_attempts: 1,
        }
    }
}

impl MergeInsertMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let num_inserted_rows = MetricBuilder::new(metrics).counter("num_inserted_rows", partition);
        let num_updated_rows = MetricBuilder::new(metrics).counter("num_updated_rows", partition);
        let num_deleted_rows = MetricBuilder::new(metrics).counter("num_deleted_rows", partition);
        let bytes_written = MetricBuilder::new(metrics).counter("bytes_written", partition);
        let num_files_written = MetricBuilder::new(metrics).counter("num_files_written", partition);
        let num_skipped_duplicates =
            MetricBuilder::new(metrics).counter("num_skipped_duplicates", partition);
        Self {
            num_inserted_rows,
            num_updated_rows,
            num_deleted_rows,
            bytes_written,
            num_files_written,
            num_skipped_duplicates,
        }
    }
}

pub(super) async fn apply_deletions(
    dataset: &Dataset,
    removed_row_addrs: &RoaringTreemap,
) -> crate::Result<(Vec<Fragment>, Vec<u64>)> {
    let bitmaps = Arc::new(removed_row_addrs.bitmaps().collect::<BTreeMap<_, _>>());

    enum FragmentChange {
        Unchanged,
        Modified(Box<Fragment>),
        Removed(u64),
    }

    let mut updated_fragments = Vec::new();
    let mut removed_fragments = Vec::new();

    let mut stream = futures::stream::iter(dataset.get_fragments())
        .map(move |fragment| {
            let bitmaps_ref = bitmaps.clone();
            async move {
                let fragment_id = fragment.id();
                if let Some(bitmap) = bitmaps_ref.get(&(fragment_id as u32)) {
                    match fragment.extend_deletions(*bitmap).await {
                        Ok(Some(new_fragment)) => {
                            Ok(FragmentChange::Modified(Box::new(new_fragment.metadata)))
                        }
                        Ok(None) => Ok(FragmentChange::Removed(fragment_id as u64)),
                        Err(e) => Err(e),
                    }
                } else {
                    Ok(FragmentChange::Unchanged)
                }
            }
        })
        .buffer_unordered(dataset.object_store.io_parallelism());

    while let Some(res) = stream.next().await.transpose()? {
        match res {
            FragmentChange::Unchanged => {}
            FragmentChange::Modified(fragment) => updated_fragments.push(*fragment),
            FragmentChange::Removed(fragment_id) => removed_fragments.push(fragment_id),
        }
    }

    Ok((updated_fragments, removed_fragments))
}
