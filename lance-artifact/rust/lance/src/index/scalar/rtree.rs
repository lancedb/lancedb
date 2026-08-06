// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::rtree::RTreeIndex;
use lance_index::scalar::{OldIndexDataFilter, index_files_to_table};
use lance_select::RowSetOps;
use lance_table::format::IndexMetadata;
use uuid::Uuid;

use crate::{Dataset, Error, Result, dataset::index::LanceIndexStoreExt};

fn filter_keeps_nothing(filter: &Option<OldIndexDataFilter>) -> bool {
    match filter {
        Some(OldIndexDataFilter::Fragments { to_keep, .. }) => to_keep.is_empty(),
        Some(OldIndexDataFilter::RowIds(valid)) => valid.is_empty(),
        None => false,
    }
}

/// Merge one caller-defined group of source RTree segments into a single segment.
pub(in crate::index) async fn merge_segments(
    dataset: &Dataset,
    segments: Vec<IndexMetadata>,
) -> Result<IndexMetadata> {
    if segments.is_empty() {
        return Err(Error::index("No segment metadata was provided".to_string()));
    }

    let field_id = *segments[0].fields.first().ok_or_else(|| {
        Error::invalid_input(format!(
            "CreateIndex: segment {} is missing field ids",
            segments[0].uuid
        ))
    })?;
    let field_path = dataset.schema().field_path(field_id)?;
    let dataset_version = segments
        .iter()
        .map(|segment| segment.dataset_version)
        .min()
        .unwrap_or(dataset.manifest.version);
    let segment_refs = segments.iter().collect::<Vec<_>>();
    let (fragment_bitmap, old_data_filters) =
        crate::index::append::build_per_segment_filters(dataset, &segment_refs).await?;

    let mut source_indices = Vec::with_capacity(segments.len());
    let mut source_filters = Vec::with_capacity(old_data_filters.len());
    let all_keep_nothing = old_data_filters.iter().all(filter_keeps_nothing);
    for (position, (segment, filter)) in segments.iter().zip(&old_data_filters).enumerate() {
        if filter_keeps_nothing(filter) && !(all_keep_nothing && position == 0) {
            continue;
        }
        let scalar_index =
            super::open_scalar_index(dataset, &field_path, segment, &NoOpMetricsCollector).await?;
        let rtree_index = scalar_index
            .as_any()
            .downcast_ref::<RTreeIndex>()
            .ok_or_else(|| {
                Error::index(format!(
                    "merge_existing_index_segments: expected RTree segment {}, got {:?}",
                    segment.uuid,
                    scalar_index.index_type()
                ))
            })?;
        source_indices.push(Arc::new(rtree_index.clone()));
        source_filters.push(filter.clone());
    }

    let new_uuid = Uuid::new_v4();
    let new_store = LanceIndexStore::from_dataset_for_new(dataset, &new_uuid)?;
    let created_index = lance_index::scalar::rtree::merge_rtree_indices(
        &source_indices,
        &new_store,
        &source_filters,
    )
    .await?;

    Ok(IndexMetadata {
        uuid: new_uuid,
        fields: vec![field_id],
        dataset_version,
        fragment_bitmap: Some(fragment_bitmap),
        index_details: Some(Arc::new(created_index.index_details)),
        index_version: created_index.index_version as i32,
        created_at: Some(chrono::Utc::now()),
        base_id: None,
        files: Some(index_files_to_table(created_index.files)),
        ..segments[0].clone()
    })
}
