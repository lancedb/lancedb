// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_index::scalar::index_files_to_table;
use lance_table::format::IndexMetadata;
use roaring::RoaringBitmap;
use std::sync::Arc;
use uuid::Uuid;

use crate::{Dataset, Error, Result};

/// Merge one caller-defined group of source FM-Index segments into a single segment.
///
/// FM-Index merge requires rebuilding from source text — there is no cheap way
/// to combine two BWT structures. This function re-reads text data from the
/// dataset for all fragments covered by the source segments and builds a fresh
/// FM-Index over the combined data.
///
/// As an exception, a single source segment whose fragment coverage is still
/// fully live is returned as-is: rebuilding it would produce equivalent content
/// over the same fragments, which makes distributed builds (one uncommitted
/// segment per worker, merged 1:1 into final segments) pay for every segment
/// twice.
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
    let column = dataset.schema().field_path(field_id)?;

    let mut fragment_bitmap = RoaringBitmap::new();
    for segment in &segments {
        fragment_bitmap |= segment.fragment_bitmap.as_ref().cloned().ok_or_else(|| {
            Error::invalid_input(format!(
                "CreateIndex: segment {} is missing fragment coverage",
                segment.uuid
            ))
        })?;
    }

    // Intersect with the dataset's current live fragments to drop retired/compacted
    // fragments, mirroring the btree merge behavior.
    fragment_bitmap &= dataset.fragment_bitmap.as_ref();

    if fragment_bitmap.is_empty() {
        // All covered fragments have been retired; produce an empty index.
        let new_uuid = Uuid::new_v4();
        let created_index = super::build_scalar_index(
            dataset,
            &column,
            new_uuid,
            &lance_index::scalar::ScalarIndexParams::for_builtin(
                lance_index::scalar::BuiltinIndexType::Fm,
            ),
            false,
            None,
            None,
            Arc::new(lance_index::progress::NoopIndexBuildProgress),
        )
        .await?;

        return Ok(IndexMetadata {
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
        });
    }

    if segments.len() == 1 && segments[0].fragment_bitmap.as_ref() == Some(&fragment_bitmap) {
        return Ok(segments.into_iter().next().unwrap());
    }

    let fragment_ids: Vec<u32> = fragment_bitmap.iter().collect();
    let new_uuid = Uuid::new_v4();

    let created_index = super::build_scalar_index(
        dataset,
        &column,
        new_uuid,
        &lance_index::scalar::ScalarIndexParams::for_builtin(
            lance_index::scalar::BuiltinIndexType::Fm,
        ),
        true,
        Some(fragment_ids),
        None,
        Arc::new(lance_index::progress::NoopIndexBuildProgress),
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
