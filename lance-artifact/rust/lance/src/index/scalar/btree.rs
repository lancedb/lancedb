// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#![allow(clippy::redundant_pub_crate)]

//! BTree-specific helpers for the segmented index workflow.
use std::sync::Arc;

use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use lance_core::ROW_ID;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::pbold::BTreeIndexDetails;
use lance_index::scalar::btree::BTreeIndex;
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::registry::VALUE_COLUMN_NAME;
use lance_index::scalar::{CreatedIndex, OldIndexDataFilter, index_files_to_table};
use lance_table::format::IndexMetadata;
use uuid::Uuid;

use crate::{Dataset, Error, Result, dataset::index::LanceIndexStoreExt};

/// Build a row-empty `new_data` stream for the BTree merge API.
fn empty_btree_update_stream(
    dataset: &Dataset,
    field_id: i32,
) -> Result<SendableRecordBatchStream> {
    let field = dataset.schema().field_by_id(field_id).ok_or_else(|| {
        Error::invalid_input(format!(
            "merge_existing_index_segments: field id {} does not exist",
            field_id
        ))
    })?;
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new(VALUE_COLUMN_NAME, field.data_type(), true),
        ArrowField::new(ROW_ID, arrow_schema::DataType::UInt64, false),
    ]));
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::empty(),
    )))
}

fn ensure_btree_details(segment: &IndexMetadata) -> Result<()> {
    if let Some(details) = segment.index_details.as_ref()
        && !details.type_url.ends_with("BTreeIndexDetails")
    {
        return Err(Error::invalid_input(format!(
            "Segment '{}' is not a BTree segment (details type_url = '{}')",
            segment.uuid, details.type_url
        )));
    }
    Ok(())
}

/// Open the given BTree `segments` and k-way merge their already-sorted page
/// data, together with `new_data`, into a single canonical BTree written to
/// `new_store`.
pub(crate) async fn open_and_merge_segments(
    dataset: &Dataset,
    field_path: &str,
    segments: &[&IndexMetadata],
    new_data: SendableRecordBatchStream,
    new_store: &LanceIndexStore,
    old_data_filters: &[Option<OldIndexDataFilter>],
) -> Result<CreatedIndex> {
    let mut source_indices = Vec::with_capacity(segments.len());
    for &segment in segments {
        let scalar_index =
            super::open_scalar_index(dataset, field_path, segment, &NoOpMetricsCollector).await?;
        let btree = scalar_index
            .as_any()
            .downcast_ref::<BTreeIndex>()
            .ok_or_else(|| {
                Error::index(format!(
                    "BTree merge: expected BTree segment {}, got {:?}",
                    segment.uuid,
                    scalar_index.index_type()
                ))
            })?;
        source_indices.push(Arc::new(btree.clone()));
    }
    BTreeIndex::merge_segments(&source_indices, new_data, new_store, old_data_filters).await
}

/// Merge one caller-defined group of source BTree segments into a single
/// physical segment.
pub(crate) async fn merge_segments(
    dataset: &Dataset,
    segments: Vec<IndexMetadata>,
) -> Result<IndexMetadata> {
    if segments.is_empty() {
        return Err(Error::index("No segment metadata was provided".to_string()));
    }

    for segment in &segments {
        ensure_btree_details(segment)?;
    }

    // All source segments must belong to the same column.
    let reference_fields = segments[0].fields.as_slice();
    for segment in segments.iter().skip(1) {
        if segment.fields.as_slice() != reference_fields {
            return Err(Error::invalid_input(format!(
                "BTree merge_segments: segment {} has fields {:?}, expected {:?}",
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
    let field_path = dataset.schema().field_path(field_id)?;

    let segment_refs: Vec<&IndexMetadata> = segments.iter().collect();
    let (fragment_bitmap, old_data_filters) =
        crate::index::append::build_per_segment_filters(dataset, &segment_refs).await?;

    let output_uuid = Uuid::new_v4();
    let new_store = LanceIndexStore::from_dataset_for_new(dataset, &output_uuid)?;
    // Pure segment consolidation: no dataset scan, so `new_data` is an empty
    // stream and the merge is driven entirely by the source page data.
    let empty_new_data = empty_btree_update_stream(dataset, field_id)?;
    let created_index = open_and_merge_segments(
        dataset,
        &field_path,
        &segment_refs,
        empty_new_data,
        &new_store,
        &old_data_filters,
    )
    .await?;

    if !created_index
        .index_details
        .type_url
        .ends_with("BTreeIndexDetails")
    {
        return Err(Error::internal(format!(
            "merge_existing_index_segments: BTree merge produced unexpected details type_url '{}'",
            created_index.index_details.type_url
        )));
    }
    debug_assert_eq!(
        created_index.index_details,
        prost_types::Any::from_msg(&BTreeIndexDetails::default()).unwrap(),
    );

    Ok(IndexMetadata {
        uuid: output_uuid,
        name: segments[0].name.clone(),
        fields: vec![field_id],
        dataset_version: dataset.manifest.version,
        fragment_bitmap: Some(fragment_bitmap),
        index_details: Some(Arc::new(created_index.index_details)),
        index_version: created_index.index_version as i32,
        created_at: Some(chrono::Utc::now()),
        base_id: None,
        files: Some(index_files_to_table(created_index.files)),
    })
}
