// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::bloomfilter::BloomFilterIndex;
use lance_index::scalar::index_files_to_table;
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_table::format::IndexMetadata;
use roaring::RoaringBitmap;
use uuid::Uuid;

use crate::{Dataset, Error, Result, dataset::index::LanceIndexStoreExt};

/// Merge one caller-defined group of source BloomFilter segments into a single segment.
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
    let mut fragment_bitmap = RoaringBitmap::new();
    let dataset_fragments = dataset.fragment_bitmap.as_ref();
    let fragment_filters = segments
        .iter()
        .map(|segment| {
            segment
                .effective_fragment_bitmap(dataset_fragments)
                .ok_or_else(|| {
                    Error::invalid_input(format!(
                        "CreateIndex: segment {} is missing fragment coverage",
                        segment.uuid
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    for effective in &fragment_filters {
        fragment_bitmap |= effective;
    }

    let mut scalar_indices = Vec::with_capacity(segments.len());
    for (position, (segment, effective)) in segments.iter().zip(&fragment_filters).enumerate() {
        if effective.is_empty() && !(fragment_bitmap.is_empty() && position == 0) {
            continue;
        }
        let scalar_index =
            super::open_scalar_index(dataset, &field_path, segment, &NoOpMetricsCollector).await?;
        scalar_indices.push((segment.uuid, scalar_index, effective));
    }

    let mut source_indices = Vec::with_capacity(scalar_indices.len());
    for (segment_uuid, scalar_index, fragment_filter) in &scalar_indices {
        let bloomfilter_index = scalar_index
            .as_any()
            .downcast_ref::<BloomFilterIndex>()
            .ok_or_else(|| {
                Error::index(format!(
                    "merge_existing_index_segments: expected bloom filter segment {}, got {:?}",
                    segment_uuid,
                    scalar_index.index_type()
                ))
            })?;
        source_indices.push((bloomfilter_index, *fragment_filter));
    }

    let new_uuid = Uuid::new_v4();
    let new_store = LanceIndexStore::from_dataset_for_new(dataset, &new_uuid)?;
    let created_index =
        lance_index::scalar::bloomfilter::merge_bloomfilter_indices(&source_indices, &new_store)
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
