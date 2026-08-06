// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use datafusion::physical_plan::SendableRecordBatchStream;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::progress::NoopIndexBuildProgress;
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::ngram::NGramIndex;
use lance_index::scalar::{
    BuiltinIndexType, CreatedIndex, IndexStore, OldIndexDataFilter, ScalarIndexParams,
    index_files_to_table,
};
use lance_table::format::IndexMetadata;
use roaring::RoaringBitmap;
use uuid::Uuid;

use crate::{
    Dataset, Error, Result, dataset::index::LanceIndexStoreExt, index::DatasetIndexInternalExt,
};

async fn collect_ngram_segment_stores(
    dataset: &Dataset,
    segments: &[IndexMetadata],
) -> Result<Vec<Arc<dyn IndexStore>>> {
    let mut stores: Vec<Arc<dyn IndexStore>> = Vec::with_capacity(segments.len());
    for segment in segments {
        let store = LanceIndexStore::from_dataset_for_existing(dataset, segment).await?;
        stores.push(Arc::new(store));
    }
    Ok(stores)
}

/// Merge one caller-defined group of source NGram segments into a single segment.
pub(in crate::index) async fn merge_segments(
    dataset: &Dataset,
    segments: Vec<IndexMetadata>,
) -> Result<IndexMetadata> {
    if segments.is_empty() {
        return Err(Error::index("No segment metadata was provided".to_string()));
    }

    // All source segments must belong to the same column.
    let reference_fields = segments[0].fields.as_slice();
    for segment in segments.iter().skip(1) {
        if segment.fields.as_slice() != reference_fields {
            return Err(Error::invalid_input(format!(
                "NGram merge_segments: segment {} has fields {:?}, expected {:?}",
                segment.uuid, segment.fields, reference_fields,
            )));
        }
    }

    let field_id = *segments[0].fields.first().ok_or_else(|| {
        Error::invalid_input(format!(
            "CreateIndex: segment {} is missing field ids",
            segments[0].uuid
        ))
    })?;
    let source_dataset_version = segments
        .iter()
        .map(|segment| segment.dataset_version)
        .min()
        .unwrap_or(dataset.manifest.version);
    let segment_refs = segments.iter().collect::<Vec<_>>();

    let new_uuid = Uuid::new_v4();
    let frag_reuse_index = dataset.open_frag_reuse_index(&NoOpMetricsCollector).await?;
    let has_retired_coverage = segments.iter().any(|segment| {
        segment
            .deleted_fragment_bitmap(&dataset.fragment_bitmap)
            .is_some_and(|retired| !retired.is_empty())
    });
    let requires_rebuild = frag_reuse_index.as_ref().is_some_and(|frag_reuse_index| {
        crate::index::append::fragment_reuse_affects_segments(
            frag_reuse_index,
            segment_refs.iter().copied(),
        )
    });
    if has_retired_coverage && !requires_rebuild {
        return Err(Error::invalid_input(
            "NGram merge_segments: source segments cover retired fragments but no applicable \
             fragment-reuse mapping is available; rebuild the affected segments from the current \
             dataset"
                .to_string(),
        ));
    }
    let (created_index, dataset_version, fragment_bitmap) = if requires_rebuild {
        let mut fragment_bitmap = segments
            .iter()
            .map(|segment| {
                segment.fragment_bitmap.as_ref().cloned().ok_or_else(|| {
                    Error::invalid_input(format!(
                        "CreateIndex: segment {} is missing fragment coverage",
                        segment.uuid
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .fold(RoaringBitmap::new(), |coverage, segment| coverage | segment);
        frag_reuse_index
            .as_ref()
            .expect("requires_rebuild implies a fragment-reuse index")
            .remap_fragment_bitmap(&mut fragment_bitmap)?;
        fragment_bitmap &= dataset.fragment_bitmap.as_ref();
        let field_path = dataset.schema().field_path(field_id)?;
        (
            super::build_scalar_index(
                dataset,
                &field_path,
                new_uuid,
                &ScalarIndexParams::for_builtin(BuiltinIndexType::NGram),
                true,
                Some(fragment_bitmap.iter().collect()),
                None,
                Arc::new(NoopIndexBuildProgress),
            )
            .await?,
            dataset.manifest.version,
            fragment_bitmap,
        )
    } else {
        let (fragment_bitmap, old_data_filters) =
            crate::index::append::build_per_segment_filters(dataset, &segment_refs).await?;
        let new_store = LanceIndexStore::from_dataset_for_new(dataset, &new_uuid)?;
        (
            open_and_merge_segments(dataset, &segment_refs, None, &new_store, &old_data_filters)
                .await?,
            source_dataset_version,
            fragment_bitmap,
        )
    };

    Ok(IndexMetadata {
        uuid: new_uuid,
        name: segments[0].name.clone(),
        fields: vec![field_id],
        dataset_version,
        fragment_bitmap: Some(fragment_bitmap),
        index_details: Some(Arc::new(created_index.index_details)),
        index_version: created_index.index_version as i32,
        created_at: Some(chrono::Utc::now()),
        base_id: None,
        files: Some(index_files_to_table(created_index.files)),
    })
}

/// Merge the given NGram segments with optional newly appended data into a single
/// canonical segment. Pass `None` for `new_data` for a pure consolidation.
pub(in crate::index) async fn open_and_merge_segments(
    dataset: &Dataset,
    segments: &[&IndexMetadata],
    new_data: Option<SendableRecordBatchStream>,
    new_store: &LanceIndexStore,
    old_data_filters: &[Option<OldIndexDataFilter>],
) -> Result<CreatedIndex> {
    let segments = segments.iter().map(|&s| s.clone()).collect::<Vec<_>>();
    let segment_stores = collect_ngram_segment_stores(dataset, &segments).await?;
    let frag_reuse_index = dataset.open_frag_reuse_index(&NoOpMetricsCollector).await?;
    NGramIndex::merge_segments(
        &segment_stores,
        new_data,
        new_store,
        old_data_filters,
        frag_reuse_index,
    )
    .await
}
