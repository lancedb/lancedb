// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::IndexStore;
use lance_index::scalar::index_files_to_table;
use lance_index::scalar::label_list::{
    BITMAP_LOOKUP_NAME, LABEL_LIST_NULLS_METADATA_KEY, LABEL_LIST_NULLS_MIN_VERSION, LabelListIndex,
};
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_table::format::IndexMetadata;
use roaring::RoaringBitmap;
use std::sync::Arc;
use uuid::Uuid;

use crate::{Dataset, Error, Result, dataset::index::LanceIndexStoreExt};

async fn validate_nullable_segment_for_merge(
    dataset: &Dataset,
    field_id: i32,
    segment: &IndexMetadata,
) -> Result<()> {
    let field = dataset.schema().field_by_id(field_id).ok_or_else(|| {
        Error::invalid_input(format!(
            "merge_existing_index_segments: field id {} does not exist",
            field_id
        ))
    })?;

    if !field.nullable {
        return Ok(());
    }

    if segment.index_version < LABEL_LIST_NULLS_MIN_VERSION {
        return Err(Error::invalid_input(format!(
            "Cannot merge nullable LabelList segment {} because it was created before list-null metadata was required. Rebuild the segment instead.",
            segment.uuid
        )));
    }

    let index_store = LanceIndexStore::from_dataset_for_existing(dataset, segment).await?;
    let reader = index_store.open_index_file(BITMAP_LOOKUP_NAME).await?;
    if !reader
        .schema()
        .metadata
        .contains_key(LABEL_LIST_NULLS_METADATA_KEY)
    {
        return Err(Error::invalid_input(format!(
            "Cannot merge nullable LabelList segment {} because it is missing required metadata key {}. Rebuild the segment instead.",
            segment.uuid, LABEL_LIST_NULLS_METADATA_KEY
        )));
    }

    Ok(())
}

/// Merge one caller-defined group of source LabelList segments into a single segment.
///
/// A LabelList index is a bitmap over the unnested list values plus a `list_nulls`
/// row set, so segments are recombined by unioning the underlying bitmap states and
/// null sets (see [`lance_index::scalar::label_list::merge_label_list_indices`])
/// rather than rebuilding from source text.
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

    let dataset_fragments = dataset.fragment_bitmap.as_ref();
    let mut effective_old_frags = RoaringBitmap::new();
    let mut deleted_old_frags = RoaringBitmap::new();
    for segment in &segments {
        if segment.fragment_bitmap.is_none() {
            return Err(Error::invalid_input(format!(
                "CreateIndex: segment {} is missing fragment coverage",
                segment.uuid
            )));
        }
        if let Some(effective) = segment.effective_fragment_bitmap(dataset_fragments) {
            effective_old_frags |= effective;
        }
        if let Some(deleted) = segment.deleted_fragment_bitmap(dataset_fragments) {
            deleted_old_frags |= deleted;
        }
    }

    let fragment_bitmap = effective_old_frags.clone();
    let old_data_filter = if deleted_old_frags.is_empty() {
        None
    } else {
        crate::index::append::build_old_data_filter(
            dataset,
            &effective_old_frags,
            &deleted_old_frags,
        )
        .await?
    };

    let mut source_indices = Vec::with_capacity(segments.len());
    for segment in &segments {
        validate_nullable_segment_for_merge(dataset, field_id, segment).await?;
        let scalar_index =
            super::open_scalar_index(dataset, &field_path, segment, &NoOpMetricsCollector).await?;
        let label_list_index = scalar_index
            .as_any()
            .downcast_ref::<LabelListIndex>()
            .ok_or_else(|| {
                Error::index(format!(
                    "merge_existing_index_segments: expected label list segment {}, got {:?}",
                    segment.uuid,
                    scalar_index.index_type()
                ))
            })?;
        source_indices.push(Arc::new(label_list_index.clone()));
    }

    let new_uuid = Uuid::new_v4();
    let new_store = LanceIndexStore::from_dataset_for_new(dataset, &new_uuid)?;
    let created_index = lance_index::scalar::label_list::merge_label_list_indices(
        &source_indices,
        &new_store,
        old_data_filter,
        lance_index::progress::noop_progress(),
    )
    .await?;

    Ok(IndexMetadata {
        uuid: new_uuid,
        fields: vec![field_id],
        dataset_version: dataset.manifest.version,
        fragment_bitmap: Some(fragment_bitmap),
        index_details: Some(Arc::new(created_index.index_details)),
        index_version: created_index.index_version as i32,
        created_at: Some(chrono::Utc::now()),
        base_id: None,
        files: Some(index_files_to_table(created_index.files)),
        ..segments[0].clone()
    })
}
