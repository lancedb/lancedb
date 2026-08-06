// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Query-time logical views over scalar index segments.

use lance_core::utils::row_addr_remap::RowAddrRemap;
use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::try_join_all;
use lance_core::deepsize::{Context, DeepSizeOf};
use lance_core::{Error, Result};
use lance_index::metrics::MetricsCollector;
use lance_index::scalar::{AnyQuery, CreatedIndex, ScalarIndex, SearchResult, UpdateCriteria};
use lance_index::{Index, IndexType};
use lance_select::NullableRowAddrSet;
use lance_table::format::IndexMetadata;
use roaring::RoaringBitmap;
use serde_json::json;

use crate::dataset::Dataset;
use crate::index::scalar::fetch_index_details;
use crate::index::{DatasetIndexExt, DatasetIndexInternalExt};

/// Query-time view that exposes several physical scalar index segments as a single [`ScalarIndex`].
///
/// A named scalar index can be built incrementally, producing multiple physical segments that
/// each cover a disjoint set of fragments. When such an index is opened, the loader bundles
/// the segments into a `LogicalScalarIndex` so the scanner can treat them as one index: queries
/// are fanned out to every segment in parallel and the row-address results are unioned together.
///
/// All segments must share the same [`IndexType`]; mixing types is rejected at construction.
/// Per-segment [`SearchResult`] precision is preserved when combining: a union of `Exact`
/// results stays `Exact`, a union containing `AtMost` results yields `AtMost`, and a union
/// containing `AtLeast` results yields `AtLeast`. Combining `AtMost` and `AtLeast` segments in
/// the same query is not supported.
///
/// This is a read-only wrapper. [`ScalarIndex::remap`] and [`ScalarIndex::update`] both return
/// an error — callers must rebuild the index to consolidate segments before mutating it.
#[derive(Debug)]
pub struct LogicalScalarIndex {
    name: String,
    column: String,
    index_type: IndexType,
    segments: Vec<Arc<dyn ScalarIndex>>,
}

impl LogicalScalarIndex {
    /// Merge several already-opened segments of one scalar index into a single
    /// searchable [`ScalarIndex`].
    ///
    /// Used internally by `open_named_scalar_index`, and exposed so a
    /// distributed query engine can open an explicit subset of a scalar
    /// index's segments and present them as one index.
    pub fn try_new(
        name: String,
        column: String,
        segments: Vec<Arc<dyn ScalarIndex>>,
    ) -> Result<Self> {
        let Some(first) = segments.first() else {
            return Err(Error::invalid_input(format!(
                "LogicalScalarIndex '{}' on column '{}' must contain at least one segment",
                name, column
            )));
        };
        let index_type = first.index_type();
        if segments
            .iter()
            .any(|segment| segment.index_type() != index_type)
        {
            return Err(Error::invalid_input(format!(
                "LogicalScalarIndex '{}' on column '{}' mixes scalar index types",
                name, column
            )));
        }

        Ok(Self {
            name,
            column,
            index_type,
            segments,
        })
    }
}

impl DeepSizeOf for LogicalScalarIndex {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.name.deep_size_of_children(context)
            + self.column.deep_size_of_children(context)
            + self.segments.deep_size_of_children(context)
    }
}

#[async_trait]
impl Index for LogicalScalarIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    fn statistics(&self) -> Result<serde_json::Value> {
        Ok(json!({
            "index_name": self.name,
            "column": self.column,
            "index_type": self.index_type.to_string(),
            "num_segments": self.segments.len(),
        }))
    }

    async fn prewarm(&self) -> Result<()> {
        try_join_all(self.segments.iter().map(|segment| segment.prewarm())).await?;
        Ok(())
    }

    fn index_type(&self) -> IndexType {
        self.index_type
    }

    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        let fragment_sets = try_join_all(
            self.segments
                .iter()
                .map(|segment| segment.calculate_included_frags()),
        )
        .await?;
        let mut combined = RoaringBitmap::new();
        for fragment_set in fragment_sets {
            combined |= fragment_set;
        }
        Ok(combined)
    }
}

#[async_trait]
impl ScalarIndex for LogicalScalarIndex {
    async fn search(
        &self,
        query: &dyn AnyQuery,
        metrics: &dyn MetricsCollector,
    ) -> Result<SearchResult> {
        let results = try_join_all(
            self.segments
                .iter()
                .map(|segment| segment.search(query, metrics)),
        )
        .await?;
        combine_search_results(results)
    }

    fn results_are_row_addresses(&self) -> bool {
        // All segments of a logical index share the same underlying index type,
        // so they agree on the result domain.
        self.segments[0].results_are_row_addresses()
    }

    fn can_remap(&self) -> bool {
        false
    }

    async fn remap(
        &self,
        _mapping: &RowAddrRemap,
        _dest_store: &dyn lance_index::scalar::IndexStore,
    ) -> Result<CreatedIndex> {
        Err(Error::invalid_input(format!(
            "LogicalScalarIndex '{}' is a query-time wrapper and does not support remap; rebuild the index to consolidate segments before remapping",
            self.name
        )))
    }

    async fn update(
        &self,
        _new_data: datafusion::physical_plan::SendableRecordBatchStream,
        _dest_store: &dyn lance_index::scalar::IndexStore,
        _old_data_filter: Option<lance_index::scalar::OldIndexDataFilter>,
    ) -> Result<CreatedIndex> {
        Err(Error::invalid_input(format!(
            "LogicalScalarIndex '{}' is a query-time wrapper and does not support update; rebuild the index to consolidate segments before updating",
            self.name
        )))
    }

    fn update_criteria(&self) -> UpdateCriteria {
        self.segments[0].update_criteria()
    }

    fn derive_index_params(&self) -> Result<lance_index::scalar::ScalarIndexParams> {
        self.segments[0].derive_index_params()
    }
}

fn combine_search_results(results: Vec<SearchResult>) -> Result<SearchResult> {
    let mut saw_at_most = false;
    let mut saw_at_least = false;
    let mut sets = Vec::with_capacity(results.len());

    for result in results {
        match result {
            SearchResult::Exact(set) => sets.push(set),
            SearchResult::AtMost(set) => {
                saw_at_most = true;
                sets.push(set);
            }
            SearchResult::AtLeast(set) => {
                saw_at_least = true;
                sets.push(set);
            }
        }
    }

    if saw_at_most && saw_at_least {
        return Err(Error::not_supported(
            "Logical scalar index cannot combine mixed AtMost and AtLeast segment results",
        ));
    }

    let combined = NullableRowAddrSet::union_all(&sets);
    Ok(if saw_at_most {
        SearchResult::AtMost(combined)
    } else if saw_at_least {
        SearchResult::AtLeast(combined)
    } else {
        SearchResult::Exact(combined)
    })
}

fn index_intersects_dataset(index: &IndexMetadata, dataset: &Dataset) -> bool {
    index
        .fragment_bitmap
        .as_ref()
        .is_some_and(|index_bitmap| index_bitmap.intersection_len(&dataset.fragment_bitmap) > 0)
}

/// List the committed, dataset-intersecting segments of a named scalar index.
///
/// Returns one [`IndexMetadata`] per usable segment. The result length is the
/// segment count: `1` means a single (non-segmented) index, `> 1` means the
/// index is split across multiple segments that a distributed engine may route
/// to different executors. All returned segments are validated to share the
/// same underlying index type.
pub async fn load_named_scalar_segments(
    dataset: &Dataset,
    column: &str,
    index_name: &str,
) -> Result<Vec<IndexMetadata>> {
    let usable_indices = dataset
        .load_indices_by_name(index_name)
        .await?
        .into_iter()
        .filter(|index| index_intersects_dataset(index, dataset))
        .collect::<Vec<_>>();

    let mut index_type_url = None::<String>;
    for index in &usable_indices {
        let segment_type_url = match index.index_details.as_ref() {
            Some(index_details) => index_details.type_url.clone(),
            None => {
                // Legacy manifests may omit embedded details, so fetch only the missing ones.
                fetch_index_details(dataset, column, index)
                    .await?
                    .type_url
                    .clone()
            }
        };
        match &index_type_url {
            Some(expected) if expected != &segment_type_url => {
                return Err(Error::invalid_input(format!(
                    "Scalar index '{}' on column '{}' mixes incompatible segment types",
                    index_name, column
                )));
            }
            None => index_type_url = Some(segment_type_url),
            Some(_) => {}
        }
    }

    Ok(usable_indices)
}

fn union_fragment_bitmaps(indices: &[IndexMetadata], index_name: &str) -> Result<RoaringBitmap> {
    let mut combined = RoaringBitmap::new();
    for index in indices {
        let fragment_bitmap = index.fragment_bitmap.as_ref().ok_or_else(|| {
            Error::invalid_input(format!(
                "Scalar index '{}' segment {} is missing fragment coverage",
                index_name, index.uuid
            ))
        })?;
        combined |= fragment_bitmap.clone();
    }
    Ok(combined)
}

/// Return the union of fragment bitmaps across every usable segment of a named scalar index.
///
/// Only segments whose fragment bitmap intersects the dataset's current fragment set are
/// considered. Returns `Ok(None)` when no such segment exists, `Ok(Some(bitmap))` otherwise.
/// Errors if the segments disagree on their underlying index type.
pub async fn scalar_index_fragment_bitmap(
    dataset: &Dataset,
    column: &str,
    index_name: &str,
) -> Result<Option<RoaringBitmap>> {
    let indices = load_named_scalar_segments(dataset, column, index_name).await?;
    match indices.len() {
        0 => Ok(None),
        1 => Ok(indices
            .into_iter()
            .next()
            .and_then(|index| index.fragment_bitmap)),
        _ => union_fragment_bitmaps(&indices, index_name).map(Some),
    }
}

/// Open a named scalar index, transparently bundling multiple segments when present.
///
/// Loads every segment registered under `index_name` whose fragment bitmap intersects the
/// dataset. If exactly one usable segment exists it is returned directly; if multiple exist
/// they are wrapped in a [`LogicalScalarIndex`] so the caller sees a single [`ScalarIndex`].
/// Errors if no usable segment exists (the scanner planned a query against an index that is
/// not present) or if the segments mix incompatible types.
pub async fn open_named_scalar_index(
    dataset: &Dataset,
    column: &str,
    index_name: &str,
    metrics: &dyn MetricsCollector,
) -> Result<Arc<dyn ScalarIndex>> {
    let indices = load_named_scalar_segments(dataset, column, index_name).await?;
    match indices.len() {
        0 => Err(Error::internal(format!(
            "Scanner created plan for index query on index {} for column {} but no usable index exists with that name",
            index_name, column
        ))),
        1 => {
            dataset
                .open_scalar_index(column, &indices[0].uuid, metrics)
                .await
        }
        _ => {
            let segments = try_join_all(indices.iter().map(|index| async move {
                dataset
                    .open_scalar_index(column, &index.uuid, metrics)
                    .await
            }))
            .await?;

            Ok(Arc::new(LogicalScalarIndex::try_new(
                index_name.to_string(),
                column.to_string(),
                segments,
            )?) as Arc<dyn ScalarIndex>)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::ops::Bound;

    use arrow::datatypes::Int32Type;
    use datafusion::scalar::ScalarValue;
    use lance_core::utils::address::RowAddress;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_datagen::array;
    use lance_index::IndexType;
    use lance_index::metrics::NoOpMetricsCollector;
    use lance_index::scalar::bitmap::BITMAP_LOOKUP_NAME;
    use lance_index::scalar::{BuiltinIndexType, SargableQuery, ScalarIndexParams};

    use crate::Dataset;
    use crate::dataset::WriteParams;
    use crate::dataset::optimize::{CompactionOptions, compact_files};
    use crate::dataset::write::WriteMode;
    use crate::index::create::CreateIndexBuilder;
    use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount};

    use super::*;

    #[tokio::test]
    async fn test_open_named_scalar_index_uses_all_zonemap_segments() {
        let dataset = lance_datagen::gen_batch()
            .col("value", array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(4), FragmentRowCount::from(16))
            .await
            .unwrap();
        let mut dataset = dataset;
        let fragments = dataset.get_fragments();
        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap);
        let mut segments = Vec::new();

        for fragment in &fragments {
            let segment =
                CreateIndexBuilder::new(&mut dataset, &["value"], IndexType::ZoneMap, &params)
                    .name("value_zonemap".to_string())
                    .fragments(vec![fragment.id() as u32])
                    .execute_uncommitted()
                    .await
                    .unwrap();
            segments.push(segment);
        }

        dataset
            .commit_existing_index_segments("value_zonemap", "value", segments)
            .await
            .unwrap();

        let committed = dataset.load_indices_by_name("value_zonemap").await.unwrap();
        assert_eq!(committed.len(), fragments.len());

        let logical =
            open_named_scalar_index(&dataset, "value", "value_zonemap", &NoOpMetricsCollector)
                .await
                .unwrap();
        assert_eq!(
            logical.calculate_included_frags().await.unwrap(),
            dataset.fragment_bitmap.as_ref().clone()
        );

        let combined_bitmap = scalar_index_fragment_bitmap(&dataset, "value", "value_zonemap")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(combined_bitmap, dataset.fragment_bitmap.as_ref().clone());
    }

    #[tokio::test]
    async fn test_open_named_scalar_index_uses_all_btree_segments() {
        let test_dir = TempStrDir::default();
        let dataset = lance_datagen::gen_batch()
            .col("value", array::step::<Int32Type>())
            .into_dataset(
                test_dir.as_str(),
                FragmentCount::from(4),
                FragmentRowCount::from(16),
            )
            .await
            .unwrap();
        let mut dataset = dataset;
        let fragments = dataset.get_fragments();
        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::BTree);
        let mut segments = Vec::new();

        for fragment in &fragments {
            let segment =
                CreateIndexBuilder::new(&mut dataset, &["value"], IndexType::BTree, &params)
                    .name("value_btree".to_string())
                    .fragments(vec![fragment.id() as u32])
                    .execute_uncommitted()
                    .await
                    .unwrap();
            segments.push(segment);
        }

        dataset
            .commit_existing_index_segments("value_btree", "value", segments)
            .await
            .unwrap();

        let committed = dataset.load_indices_by_name("value_btree").await.unwrap();
        assert_eq!(committed.len(), fragments.len());

        let logical =
            open_named_scalar_index(&dataset, "value", "value_btree", &NoOpMetricsCollector)
                .await
                .unwrap();
        assert_eq!(logical.index_type(), IndexType::BTree);
        assert_eq!(
            logical.calculate_included_frags().await.unwrap(),
            dataset.fragment_bitmap.as_ref().clone()
        );

        let combined_bitmap = scalar_index_fragment_bitmap(&dataset, "value", "value_btree")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(combined_bitmap, dataset.fragment_bitmap.as_ref().clone());
    }

    #[tokio::test]
    async fn test_btree_segment_search_is_exact_across_fragments() {
        let test_dir = TempStrDir::default();
        let dataset = lance_datagen::gen_batch()
            .col("value", array::step::<Int32Type>())
            .into_dataset(
                test_dir.as_str(),
                FragmentCount::from(4),
                FragmentRowCount::from(16),
            )
            .await
            .unwrap();
        let mut dataset = dataset;
        let fragments = dataset.get_fragments();
        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::BTree);
        let mut segments = Vec::new();

        for fragment in &fragments {
            segments.push(
                CreateIndexBuilder::new(&mut dataset, &["value"], IndexType::BTree, &params)
                    .name("value_btree_search".to_string())
                    .fragments(vec![fragment.id() as u32])
                    .execute_uncommitted()
                    .await
                    .unwrap(),
            );
        }

        dataset
            .commit_existing_index_segments("value_btree_search", "value", segments)
            .await
            .unwrap();

        let logical = open_named_scalar_index(
            &dataset,
            "value",
            "value_btree_search",
            &NoOpMetricsCollector,
        )
        .await
        .unwrap();

        let query = SargableQuery::Range(
            Bound::Included(ScalarValue::Int32(Some(20))),
            Bound::Included(ScalarValue::Int32(Some(43))),
        );
        let result = logical.search(&query, &NoOpMetricsCollector).await.unwrap();
        let row_addrs = match result {
            SearchResult::Exact(row_addrs) => row_addrs,
            other => panic!(
                "expected exact result from segmented btree, got {:?}",
                other
            ),
        };

        let searched_fragments = row_addrs
            .true_rows()
            .row_addrs()
            .unwrap()
            .map(|row_addr| RowAddress::from(u64::from(row_addr)).fragment_id())
            .collect::<Vec<_>>();
        assert_eq!(searched_fragments.len(), 24);
        assert_eq!(
            searched_fragments.into_iter().collect::<BTreeSet<_>>(),
            BTreeSet::from([1, 2])
        );
    }

    #[tokio::test]
    async fn test_bitmap_segments_commit_and_query_as_logical_index() {
        let test_dir = TempStrDir::default();
        let dataset = lance_datagen::gen_batch()
            .col("value", array::step::<Int32Type>())
            .into_dataset(
                test_dir.as_str(),
                FragmentCount::from(4),
                FragmentRowCount::from(16),
            )
            .await
            .unwrap();
        let mut dataset = dataset;
        let fragments = dataset.get_fragments();
        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::Bitmap);
        let mut staged = Vec::new();

        for fragment_group in fragments.chunks(2) {
            let fragment_ids = fragment_group
                .iter()
                .map(|fragment| fragment.id() as u32)
                .collect::<Vec<_>>();
            let segment =
                CreateIndexBuilder::new(&mut dataset, &["value"], IndexType::Bitmap, &params)
                    .name("value_bitmap".to_string())
                    .fragments(fragment_ids.clone())
                    .execute_uncommitted()
                    .await
                    .unwrap();
            assert_eq!(
                segment
                    .fragment_bitmap
                    .as_ref()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                fragment_ids
            );
            let files = segment.files.as_ref().unwrap();
            assert!(files.iter().any(|file| file.path == BITMAP_LOOKUP_NAME));
            assert!(files.iter().all(|file| !file.path.starts_with("part_")));
            staged.push(segment);
        }

        let staged_uuids = staged
            .iter()
            .map(|segment| segment.uuid)
            .collect::<Vec<_>>();
        let merged = dataset.merge_existing_index_segments(staged).await.unwrap();
        assert!(!staged_uuids.contains(&merged.uuid));
        assert_eq!(
            merged
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            fragments
                .iter()
                .map(|fragment| fragment.id() as u32)
                .collect::<Vec<_>>()
        );
        let files = merged.files.as_ref().unwrap();
        assert!(files.iter().any(|file| file.path == BITMAP_LOOKUP_NAME));
        assert!(files.iter().all(|file| !file.path.starts_with("part_")));

        dataset
            .commit_existing_index_segments("value_bitmap", "value", vec![merged])
            .await
            .unwrap();

        let committed = dataset.load_indices_by_name("value_bitmap").await.unwrap();
        assert_eq!(committed.len(), 1);
        assert_eq!(
            scalar_index_fragment_bitmap(&dataset, "value", "value_bitmap")
                .await
                .unwrap()
                .unwrap(),
            dataset.fragment_bitmap.as_ref().clone()
        );

        let logical =
            open_named_scalar_index(&dataset, "value", "value_bitmap", &NoOpMetricsCollector)
                .await
                .unwrap();
        assert_eq!(logical.index_type(), IndexType::Bitmap);

        let query = SargableQuery::Equals(ScalarValue::Int32(Some(20)));
        let result = logical.search(&query, &NoOpMetricsCollector).await.unwrap();
        let row_addrs = match result {
            SearchResult::Exact(row_addrs) => row_addrs,
            other => panic!(
                "expected exact result from segmented bitmap, got {:?}",
                other
            ),
        };

        let searched_fragments = row_addrs
            .true_rows()
            .row_addrs()
            .unwrap()
            .map(|row_addr| RowAddress::from(u64::from(row_addr)).fragment_id())
            .collect::<Vec<_>>();
        assert_eq!(searched_fragments, vec![1]);
    }

    #[tokio::test]
    async fn test_zonemap_segment_search_keeps_fragment_ids() {
        let dataset = lance_datagen::gen_batch()
            .col("value", array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(4), FragmentRowCount::from(16))
            .await
            .unwrap();
        let mut dataset = dataset;
        let target_fragment = dataset.get_fragments()[2].id() as u32;
        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap);

        let segment =
            CreateIndexBuilder::new(&mut dataset, &["value"], IndexType::ZoneMap, &params)
                .name("value_zonemap_single_fragment".to_string())
                .fragments(vec![target_fragment])
                .execute_uncommitted()
                .await
                .unwrap();

        dataset
            .commit_existing_index_segments("value_zonemap_single_fragment", "value", vec![segment])
            .await
            .unwrap();

        let logical = open_named_scalar_index(
            &dataset,
            "value",
            "value_zonemap_single_fragment",
            &NoOpMetricsCollector,
        )
        .await
        .unwrap();

        assert_eq!(
            logical
                .calculate_included_frags()
                .await
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![target_fragment]
        );

        let query = SargableQuery::Range(
            Bound::Included(ScalarValue::Int32(Some(0))),
            Bound::Included(ScalarValue::Int32(Some(10_000))),
        );
        let result = logical.search(&query, &NoOpMetricsCollector).await.unwrap();
        let searched_fragments = result
            .row_addrs()
            .true_rows()
            .row_addrs()
            .unwrap()
            .map(|row_addr| RowAddress::from(u64::from(row_addr)).fragment_id())
            .collect::<Vec<_>>();
        assert!(!searched_fragments.is_empty());
        assert!(
            searched_fragments
                .iter()
                .all(|fragment_id| *fragment_id == target_fragment)
        );
    }

    #[tokio::test]
    async fn test_merge_existing_index_segments_supports_zonemap_segments() {
        let dataset = lance_datagen::gen_batch()
            .col("value", array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(4), FragmentRowCount::from(16))
            .await
            .unwrap();
        let mut dataset = dataset;
        let fragments = dataset.get_fragments();
        let zonemap_params = lance_index::scalar::zonemap::ZoneMapIndexBuilderParams::new(8);
        let params_json = serde_json::to_value(&zonemap_params).unwrap();
        let params =
            ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap).with_params(&params_json);
        let mut staged = Vec::new();

        for fragment in &fragments {
            let segment =
                CreateIndexBuilder::new(&mut dataset, &["value"], IndexType::ZoneMap, &params)
                    .name("value_zonemap_merged".to_string())
                    .fragments(vec![fragment.id() as u32])
                    .execute_uncommitted()
                    .await
                    .unwrap();
            staged.push(segment);
        }

        let staged_uuids = staged
            .iter()
            .map(|segment| segment.uuid)
            .collect::<Vec<_>>();
        let merged = dataset.merge_existing_index_segments(staged).await.unwrap();
        assert!(!staged_uuids.contains(&merged.uuid));
        assert_eq!(
            merged
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            fragments
                .iter()
                .map(|fragment| fragment.id() as u32)
                .collect::<Vec<_>>()
        );
        assert!(
            merged
                .files
                .as_ref()
                .unwrap()
                .iter()
                .any(|file| file.path == "zonemap.lance")
        );

        dataset
            .commit_existing_index_segments("value_zonemap_merged", "value", vec![merged])
            .await
            .unwrap();

        let committed = dataset
            .load_indices_by_name("value_zonemap_merged")
            .await
            .unwrap();
        assert_eq!(committed.len(), 1);

        let logical = open_named_scalar_index(
            &dataset,
            "value",
            "value_zonemap_merged",
            &NoOpMetricsCollector,
        )
        .await
        .unwrap();
        assert_eq!(logical.index_type(), IndexType::ZoneMap);
        assert_eq!(
            logical.statistics().unwrap()["rows_per_zone"],
            serde_json::json!(8)
        );
        assert_eq!(
            logical.calculate_included_frags().await.unwrap(),
            dataset.fragment_bitmap.as_ref().clone()
        );

        let query = SargableQuery::Range(
            Bound::Included(ScalarValue::Int32(Some(0))),
            Bound::Included(ScalarValue::Int32(Some(10_000))),
        );
        let result = logical.search(&query, &NoOpMetricsCollector).await.unwrap();
        let searched_fragments = result
            .row_addrs()
            .true_rows()
            .row_addrs()
            .unwrap()
            .map(|row_addr| RowAddress::from(u64::from(row_addr)).fragment_id())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            searched_fragments,
            fragments
                .iter()
                .map(|fragment| fragment.id() as u32)
                .collect::<BTreeSet<_>>()
        );

        let selective_query = SargableQuery::Range(
            Bound::Included(ScalarValue::Int32(Some(20))),
            Bound::Included(ScalarValue::Int32(Some(43))),
        );
        let selective_result = logical
            .search(&selective_query, &NoOpMetricsCollector)
            .await
            .unwrap();
        let selective_fragments = selective_result
            .row_addrs()
            .true_rows()
            .row_addrs()
            .unwrap()
            .map(|row_addr| RowAddress::from(u64::from(row_addr)).fragment_id())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            selective_fragments,
            fragments[1..=2]
                .iter()
                .map(|fragment| fragment.id() as u32)
                .collect::<BTreeSet<_>>()
        );
    }

    #[tokio::test]
    async fn test_merge_existing_zonemap_segments_drops_retired_fragments() {
        let tmpdir = TempStrDir::default();
        let dataset_uri = format!("file://{}", tmpdir.as_str());
        let reader = lance_datagen::gen_batch()
            .col("value", array::step::<Int32Type>())
            .into_reader_rows(
                lance_datagen::RowCount::from(64),
                lance_datagen::BatchCount::from(2),
            );
        let mut dataset = Dataset::write(
            reader,
            &dataset_uri,
            Some(WriteParams {
                max_rows_per_file: 64,
                mode: WriteMode::Overwrite,
                enable_stable_row_ids: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap);
        let mut staged = Vec::new();
        for fragment in dataset.get_fragments() {
            staged.push(
                CreateIndexBuilder::new(&mut dataset, &["value"], IndexType::ZoneMap, &params)
                    .name("value_zonemap_retired".to_string())
                    .fragments(vec![fragment.id() as u32])
                    .execute_uncommitted()
                    .await
                    .unwrap(),
            );
        }
        dataset
            .commit_existing_index_segments("value_zonemap_retired", "value", staged)
            .await
            .unwrap();

        dataset.delete("value < 16").await.unwrap();
        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 64,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        let live_frags = dataset.fragment_bitmap.as_ref().clone();
        assert!(!live_frags.contains(0), "compaction should retire frag 0");

        let merged = dataset
            .merge_existing_index_segments(
                dataset
                    .load_indices_by_name("value_zonemap_retired")
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        let coverage = merged.fragment_bitmap.as_ref().unwrap();
        assert!(!coverage.contains(0), "must drop retired frag 0");
        assert!(coverage.contains(1), "must keep live indexed frag 1");

        let field_path = dataset.schema().field_path(merged.fields[0]).unwrap();
        let index = crate::index::scalar::open_scalar_index(
            &dataset,
            &field_path,
            &merged,
            &NoOpMetricsCollector,
        )
        .await
        .unwrap();
        let query = SargableQuery::Range(
            Bound::Included(ScalarValue::Int32(Some(0))),
            Bound::Excluded(ScalarValue::Int32(Some(16))),
        );
        let searched_fragments = index
            .search(&query, &NoOpMetricsCollector)
            .await
            .unwrap()
            .row_addrs()
            .true_rows()
            .row_addrs()
            .unwrap()
            .map(|row_addr| RowAddress::from(u64::from(row_addr)).fragment_id())
            .collect::<BTreeSet<_>>();
        assert!(
            searched_fragments.is_empty(),
            "must filter retired-fragment zones"
        );
    }

    #[tokio::test]
    async fn test_merge_then_commit_zonemap_segment_ignores_retired_fragment_coverage() {
        let tmpdir = TempStrDir::default();
        let dataset_uri = format!("file://{}", tmpdir.as_str());
        let reader = lance_datagen::gen_batch()
            .col("value", array::step::<Int32Type>())
            .into_reader_rows(
                lance_datagen::RowCount::from(64),
                lance_datagen::BatchCount::from(2),
            );
        let mut dataset = Dataset::write(
            reader,
            &dataset_uri,
            Some(WriteParams {
                max_rows_per_file: 64,
                mode: WriteMode::Overwrite,
                enable_stable_row_ids: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap);
        let segment =
            CreateIndexBuilder::new(&mut dataset, &["value"], IndexType::ZoneMap, &params)
                .name("value_zonemap_replace_retired".to_string())
                .execute_uncommitted()
                .await
                .unwrap();
        let original_coverage = segment.fragment_bitmap.as_ref().unwrap().clone();
        assert!(original_coverage.contains(0));
        assert!(original_coverage.contains(1));

        dataset
            .commit_existing_index_segments("value_zonemap_replace_retired", "value", vec![segment])
            .await
            .unwrap();

        dataset.delete("value < 16").await.unwrap();
        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 64,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        let live_frags = dataset.fragment_bitmap.as_ref().clone();
        assert!(!live_frags.contains(0), "compaction should retire frag 0");

        let merged = dataset
            .merge_existing_index_segments(
                dataset
                    .load_indices_by_name("value_zonemap_replace_retired")
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        let merged_coverage = merged.fragment_bitmap.as_ref().unwrap().clone();
        let merged_uuid = merged.uuid;

        dataset
            .commit_existing_index_segments("value_zonemap_replace_retired", "value", vec![merged])
            .await
            .unwrap();

        let committed = dataset
            .load_indices_by_name("value_zonemap_replace_retired")
            .await
            .unwrap();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].uuid, merged_uuid);

        let combined_bitmap =
            scalar_index_fragment_bitmap(&dataset, "value", "value_zonemap_replace_retired")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(combined_bitmap, merged_coverage);
    }

    #[tokio::test]
    async fn test_merge_existing_index_segments_rejects_mismatched_zonemap_params() {
        let dataset = lance_datagen::gen_batch()
            .col("value", array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(16))
            .await
            .unwrap();
        let mut dataset = dataset;
        let fragments = dataset.get_fragments();
        let mut staged = Vec::new();

        for (fragment, rows_per_zone) in fragments.iter().zip([8, 16]) {
            let zonemap_params =
                lance_index::scalar::zonemap::ZoneMapIndexBuilderParams::new(rows_per_zone);
            let params_json = serde_json::to_value(&zonemap_params).unwrap();
            let params =
                ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap).with_params(&params_json);
            let segment =
                CreateIndexBuilder::new(&mut dataset, &["value"], IndexType::ZoneMap, &params)
                    .name("value_zonemap_mismatched".to_string())
                    .fragments(vec![fragment.id() as u32])
                    .execute_uncommitted()
                    .await
                    .unwrap();
            staged.push(segment);
        }

        let err = dataset
            .merge_existing_index_segments(staged)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("different rows_per_zone values"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_commit_existing_zonemap_segments_replaces_overlapping_segments() {
        let dataset = lance_datagen::gen_batch()
            .col("value", array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(16))
            .await
            .unwrap();
        let mut dataset = dataset;
        let fragments = dataset.get_fragments();
        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap);

        let mut first_segments = Vec::new();
        for fragment in &fragments {
            first_segments.push(
                CreateIndexBuilder::new(&mut dataset, &["value"], IndexType::ZoneMap, &params)
                    .name("value_zonemap_replace".to_string())
                    .fragments(vec![fragment.id() as u32])
                    .execute_uncommitted()
                    .await
                    .unwrap(),
            );
        }

        dataset
            .commit_existing_index_segments("value_zonemap_replace", "value", first_segments)
            .await
            .unwrap();

        let mut replacement_segments = Vec::new();
        for fragment in &fragments {
            replacement_segments.push(
                CreateIndexBuilder::new(&mut dataset, &["value"], IndexType::ZoneMap, &params)
                    .name("value_zonemap_replace".to_string())
                    .replace(true)
                    .fragments(vec![fragment.id() as u32])
                    .execute_uncommitted()
                    .await
                    .unwrap(),
            );
        }
        let replacement_uuids = replacement_segments
            .iter()
            .map(|segment| segment.uuid)
            .collect::<Vec<_>>();

        dataset
            .commit_existing_index_segments("value_zonemap_replace", "value", replacement_segments)
            .await
            .unwrap();

        let committed = dataset
            .load_indices_by_name("value_zonemap_replace")
            .await
            .unwrap();
        assert_eq!(committed.len(), fragments.len());
        assert_eq!(
            committed
                .iter()
                .map(|segment| segment.uuid)
                .collect::<Vec<_>>(),
            replacement_uuids
        );
        assert_eq!(
            scalar_index_fragment_bitmap(&dataset, "value", "value_zonemap_replace")
                .await
                .unwrap()
                .unwrap(),
            dataset.fragment_bitmap.as_ref().clone()
        );
    }

    #[tokio::test]
    async fn test_fmindex_segments_commit_and_query_as_logical_index() {
        let test_dir = TempStrDir::default();

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "text",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let write_params = crate::dataset::write::WriteParams {
            max_rows_per_file: 4,
            ..Default::default()
        };
        let batches = vec![
            arrow_array::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow_array::StringArray::from(vec![
                    "the quick brown fox",
                    "jumps over the lazy dog",
                    "hello world from rust",
                    "pack my box with five dozen liquor jugs",
                    "how vexingly quick daft zebras jump",
                    "the five boxing wizards jump quickly",
                    "sphinx of black quartz judge my vow",
                    "two driven jocks help fax my big quiz",
                    "waltz bad nymph for quick jigs vex",
                    "glib jocks quiz nymph to vex dwarf",
                    "quick brown fox jumps again here",
                    "lazy dog sleeps under the tree",
                ]))],
            )
            .unwrap(),
        ];
        let reader =
            arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(reader, test_dir.as_str(), Some(write_params))
            .await
            .unwrap();

        let fragments = dataset.get_fragments();
        assert_eq!(fragments.len(), 3);

        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::Fm);
        let mut segments = Vec::new();
        for fragment in &fragments {
            let segment = CreateIndexBuilder::new(&mut dataset, &["text"], IndexType::Fm, &params)
                .name("text_fmindex".to_string())
                .fragments(vec![fragment.id() as u32])
                .execute_uncommitted()
                .await
                .unwrap();

            assert_eq!(
                segment
                    .fragment_bitmap
                    .as_ref()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![fragment.id() as u32]
            );
            segments.push(segment);
        }

        dataset
            .commit_existing_index_segments("text_fmindex", "text", segments)
            .await
            .unwrap();

        let committed = dataset.load_indices_by_name("text_fmindex").await.unwrap();
        assert_eq!(committed.len(), fragments.len());

        let logical =
            open_named_scalar_index(&dataset, "text", "text_fmindex", &NoOpMetricsCollector)
                .await
                .unwrap();
        assert_eq!(logical.index_type(), IndexType::Fm);

        let query = lance_index::scalar::TextQuery::StringContains("quick".to_string());
        let result = logical.search(&query, &NoOpMetricsCollector).await.unwrap();
        let row_addrs = match result {
            SearchResult::Exact(row_addrs) => row_addrs,
            other => panic!(
                "expected exact result from segmented fmindex, got {:?}",
                other
            ),
        };
        let match_count = row_addrs.true_rows().row_addrs().unwrap().count();
        assert_eq!(
            match_count, 5,
            "expected exactly 5 matches for 'quick', got {match_count}"
        );

        // Verify fragment coverage via manifest metadata (not calculate_included_frags,
        // which derives from row addresses and may not encode fragment IDs for all layouts)
        assert_eq!(
            scalar_index_fragment_bitmap(&dataset, "text", "text_fmindex")
                .await
                .unwrap()
                .unwrap(),
            dataset.fragment_bitmap.as_ref().clone()
        );
    }

    #[tokio::test]
    async fn test_fmindex_segments_merge_and_query() {
        let test_dir = TempStrDir::default();

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "text",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let write_params = crate::dataset::write::WriteParams {
            max_rows_per_file: 4,
            ..Default::default()
        };
        let batches = vec![
            arrow_array::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow_array::StringArray::from(vec![
                    "alpha beta gamma delta",
                    "beta gamma delta epsilon",
                    "gamma delta epsilon zeta",
                    "delta epsilon zeta eta",
                    "epsilon zeta eta theta",
                    "zeta eta theta iota",
                    "eta theta iota kappa",
                    "theta iota kappa lambda",
                ]))],
            )
            .unwrap(),
        ];
        let reader =
            arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(reader, test_dir.as_str(), Some(write_params))
            .await
            .unwrap();

        let fragments = dataset.get_fragments();
        assert_eq!(fragments.len(), 2);

        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::Fm);
        let mut staged = Vec::new();
        for fragment in &fragments {
            let segment = CreateIndexBuilder::new(&mut dataset, &["text"], IndexType::Fm, &params)
                .name("text_fmindex_merge".to_string())
                .fragments(vec![fragment.id() as u32])
                .execute_uncommitted()
                .await
                .unwrap();
            staged.push(segment);
        }
        assert_eq!(staged.len(), 2);

        let staged_uuids = staged.iter().map(|s| s.uuid).collect::<Vec<_>>();
        let merged = dataset.merge_existing_index_segments(staged).await.unwrap();

        assert!(!staged_uuids.contains(&merged.uuid));
        assert_eq!(
            merged
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            fragments.iter().map(|f| f.id() as u32).collect::<Vec<_>>()
        );

        dataset
            .commit_existing_index_segments("text_fmindex_merge", "text", vec![merged])
            .await
            .unwrap();

        let committed = dataset
            .load_indices_by_name("text_fmindex_merge")
            .await
            .unwrap();
        assert_eq!(committed.len(), 1);

        let logical = open_named_scalar_index(
            &dataset,
            "text",
            "text_fmindex_merge",
            &NoOpMetricsCollector,
        )
        .await
        .unwrap();
        assert_eq!(logical.index_type(), IndexType::Fm);

        let query = lance_index::scalar::TextQuery::StringContains("delta".to_string());
        let result = logical.search(&query, &NoOpMetricsCollector).await.unwrap();
        let row_addrs = match result {
            SearchResult::Exact(row_addrs) => row_addrs,
            other => panic!("expected exact result from merged fmindex, got {:?}", other),
        };
        assert_eq!(row_addrs.true_rows().row_addrs().unwrap().count(), 4);

        let query = lance_index::scalar::TextQuery::StringContains("nonexistent".to_string());
        let result = logical.search(&query, &NoOpMetricsCollector).await.unwrap();
        let row_addrs = match result {
            SearchResult::Exact(row_addrs) => row_addrs,
            other => panic!("expected exact result from merged fmindex, got {:?}", other),
        };
        assert_eq!(row_addrs.true_rows().row_addrs().unwrap().count(), 0);
    }

    #[tokio::test]
    async fn test_fmindex_merge_after_compaction_drops_retired_fragments() {
        use crate::dataset::write::WriteParams;

        let test_dir = TempStrDir::default();

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "text",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        // Create two fragments with 4 rows each so compaction can retire one
        let write_params = WriteParams {
            max_rows_per_file: 4,
            enable_stable_row_ids: true,
            ..Default::default()
        };
        let batches = vec![
            arrow_array::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow_array::StringArray::from(vec![
                    "alpha beta gamma",
                    "beta gamma delta",
                    "gamma delta epsilon",
                    "delta epsilon zeta",
                    "epsilon zeta eta",
                    "zeta eta theta",
                    "eta theta iota",
                    "theta iota kappa",
                ]))],
            )
            .unwrap(),
        ];
        let reader =
            arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(reader, test_dir.as_str(), Some(write_params))
            .await
            .unwrap();

        let fragments = dataset.get_fragments();
        assert_eq!(fragments.len(), 2);

        // Build per-fragment FM-Index segments and commit
        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::Fm);
        let mut staged = Vec::new();
        for fragment in &fragments {
            let segment = CreateIndexBuilder::new(&mut dataset, &["text"], IndexType::Fm, &params)
                .name("text_fmindex_compact".to_string())
                .fragments(vec![fragment.id() as u32])
                .execute_uncommitted()
                .await
                .unwrap();
            staged.push(segment);
        }
        dataset
            .commit_existing_index_segments("text_fmindex_compact", "text", staged)
            .await
            .unwrap();

        // Verify initial state: 2 segments, both fragments live
        let committed = dataset
            .load_indices_by_name("text_fmindex_compact")
            .await
            .unwrap();
        assert_eq!(committed.len(), 2);

        // Delete rows from fragment 0 to trigger compaction retirement
        dataset.delete("text = 'alpha beta gamma'").await.unwrap();
        dataset.delete("text = 'beta gamma delta'").await.unwrap();
        crate::dataset::optimize::compact_files(
            &mut dataset,
            crate::dataset::optimize::CompactionOptions {
                target_rows_per_fragment: 4,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        let live_frags: RoaringBitmap = dataset
            .get_fragments()
            .iter()
            .map(|f| f.id() as u32)
            .collect();
        assert!(
            !live_frags.contains(0),
            "compaction should retire fragment 0"
        );

        // Merge: the retired fragment should be dropped from coverage
        let segments = dataset
            .load_indices_by_name("text_fmindex_compact")
            .await
            .unwrap();
        let merged = dataset
            .merge_existing_index_segments(segments)
            .await
            .unwrap();

        let coverage = merged.fragment_bitmap.as_ref().unwrap();
        assert!(
            !coverage.contains(0),
            "merged coverage must drop retired fragment 0"
        );
        assert!(
            coverage.contains(1),
            "merged coverage must keep live fragment 1"
        );

        // Commit the merged segment and verify search works
        dataset
            .commit_existing_index_segments("text_fmindex_compact", "text", vec![merged])
            .await
            .unwrap();

        let committed = dataset
            .load_indices_by_name("text_fmindex_compact")
            .await
            .unwrap();
        assert_eq!(committed.len(), 1);

        let logical = open_named_scalar_index(
            &dataset,
            "text",
            "text_fmindex_compact",
            &NoOpMetricsCollector,
        )
        .await
        .unwrap();

        // "alpha" only existed in the deleted/retired rows
        let query = lance_index::scalar::TextQuery::StringContains("alpha".to_string());
        let result = logical.search(&query, &NoOpMetricsCollector).await.unwrap();
        let row_addrs = match result {
            SearchResult::Exact(row_addrs) => row_addrs,
            other => panic!("expected exact result from merged fmindex, got {:?}", other),
        };
        assert_eq!(
            row_addrs.true_rows().row_addrs().unwrap().count(),
            0,
            "deleted rows from retired fragment should not appear in merged index"
        );

        // "theta" exists in fragment 1 rows only
        let query = lance_index::scalar::TextQuery::StringContains("theta".to_string());
        let result = logical.search(&query, &NoOpMetricsCollector).await.unwrap();
        let row_addrs = match result {
            SearchResult::Exact(row_addrs) => row_addrs,
            other => panic!("expected exact result from merged fmindex, got {:?}", other),
        };
        assert!(
            row_addrs.true_rows().row_addrs().unwrap().count() > 0,
            "rows from live fragment should still be searchable"
        );
    }

    #[tokio::test]
    async fn test_ngram_segment_merge_rebuilds_after_deferred_compaction() {
        let test_dir = TempStrDir::default();
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "text",
            arrow_schema::DataType::Utf8,
            true,
        )]));
        let make_batch = |values| {
            arrow_array::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow_array::StringArray::from(values))],
            )
            .unwrap()
        };
        let reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(make_batch(vec![Some("alpha needle"), None]))],
            schema.clone(),
        );
        Dataset::write(
            reader,
            test_dir.as_str(),
            Some(WriteParams {
                max_rows_per_file: 2,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let appended = arrow_array::RecordBatchIterator::new(
            vec![Ok(make_batch(vec![
                Some("beta needle"),
                Some("gamma stack"),
            ]))],
            schema.clone(),
        );
        let mut dataset = Dataset::write(
            appended,
            test_dir.as_str(),
            Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::NGram);
        let mut segments = Vec::new();
        for fragment in dataset.get_fragments() {
            segments.push(
                CreateIndexBuilder::new(&mut dataset, &["text"], IndexType::NGram, &params)
                    .name("text_ngram".to_string())
                    .fragments(vec![fragment.id() as u32])
                    .execute_uncommitted()
                    .await
                    .unwrap(),
            );
        }
        let source_version = segments[0].dataset_version;
        dataset
            .create_index(
                &["text"],
                IndexType::NGram,
                Some("compaction_guard".to_string()),
                &params,
                false,
            )
            .await
            .unwrap();

        let metrics = compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 10,
                defer_index_remap: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        assert!(metrics.fragments_removed > 0 && metrics.fragments_added > 0);
        assert!(dataset.version().version > source_version);

        let direct_commit_err = dataset
            .commit_existing_index_segments("text_ngram_direct", "text", segments.clone())
            .await
            .unwrap_err();
        assert!(
            direct_commit_err
                .to_string()
                .contains("must be rebuilt or merged")
        );

        let rebuild_version = dataset.version().version;
        let merged = dataset
            .merge_existing_index_segments(segments)
            .await
            .unwrap();
        assert_eq!(merged.dataset_version, rebuild_version);
        dataset
            .commit_existing_index_segments("text_ngram", "text", vec![merged])
            .await
            .unwrap();

        let committed = dataset.load_indices_by_name("text_ngram").await.unwrap();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].dataset_version, rebuild_version);
        assert_eq!(
            committed[0].fragment_bitmap.as_ref().unwrap(),
            dataset.fragment_bitmap.as_ref()
        );

        let logical =
            open_named_scalar_index(&dataset, "text", "text_ngram", &NoOpMetricsCollector)
                .await
                .unwrap();
        let result = logical
            .search(
                &lance_index::scalar::TextQuery::StringContains("needle".to_string()),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();
        let row_addrs = match result {
            SearchResult::AtMost(row_addrs) => row_addrs,
            other => panic!("expected AtMost result from ngram, got {other:?}"),
        };
        assert_eq!(row_addrs.true_rows().row_addrs().unwrap().count(), 2);
    }

    #[tokio::test]
    async fn test_ngram_segment_merge_rejects_retired_coverage_without_remap() {
        let test_dir = TempStrDir::default();
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "text",
            arrow_schema::DataType::Utf8,
            true,
        )]));
        let make_batch = |values| {
            arrow_array::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow_array::StringArray::from(values))],
            )
            .unwrap()
        };
        let reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(make_batch(vec!["alpha", "beta"]))],
            schema.clone(),
        );
        Dataset::write(
            reader,
            test_dir.as_str(),
            Some(WriteParams {
                max_rows_per_file: 2,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let appended = arrow_array::RecordBatchIterator::new(
            vec![Ok(make_batch(vec!["gamma", "delta"]))],
            schema,
        );
        let mut dataset = Dataset::write(
            appended,
            test_dir.as_str(),
            Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::NGram);
        let mut segments = Vec::new();
        for fragment in dataset.get_fragments() {
            segments.push(
                CreateIndexBuilder::new(&mut dataset, &["text"], IndexType::NGram, &params)
                    .name("text_ngram".to_string())
                    .fragments(vec![fragment.id() as u32])
                    .execute_uncommitted()
                    .await
                    .unwrap(),
            );
        }

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 10,
                defer_index_remap: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        let direct_err = dataset
            .commit_existing_index_segments("text_ngram", "text", segments.clone())
            .await
            .unwrap_err();
        assert!(direct_err.to_string().contains("must be rebuilt or merged"));

        let merge_err = dataset
            .merge_existing_index_segments(segments)
            .await
            .unwrap_err();
        assert!(
            merge_err
                .to_string()
                .contains("no applicable fragment-reuse mapping is available")
        );
    }

    #[tokio::test]
    async fn test_fmindex_merge_single_segment_passthrough() {
        let test_dir = TempStrDir::default();

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "text",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let write_params = crate::dataset::write::WriteParams {
            max_rows_per_file: 4,
            ..Default::default()
        };
        let batches = vec![
            arrow_array::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow_array::StringArray::from(vec![
                    "alpha beta gamma delta",
                    "beta gamma delta epsilon",
                    "gamma delta epsilon zeta",
                    "delta epsilon zeta eta",
                    "epsilon zeta eta theta",
                    "zeta eta theta iota",
                    "eta theta iota kappa",
                    "theta iota kappa lambda",
                ]))],
            )
            .unwrap(),
        ];
        let reader =
            arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(reader, test_dir.as_str(), Some(write_params))
            .await
            .unwrap();

        let fragments = dataset.get_fragments();
        assert_eq!(fragments.len(), 2);
        let fragment_ids: Vec<u32> = fragments.iter().map(|f| f.id() as u32).collect();

        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::Fm);
        let segment = CreateIndexBuilder::new(&mut dataset, &["text"], IndexType::Fm, &params)
            .name("text_fmindex_single".to_string())
            .fragments(fragment_ids.clone())
            .execute_uncommitted()
            .await
            .unwrap();
        let source_uuid = segment.uuid;

        // A single segment whose coverage is fully live is reused, not rebuilt.
        let merged = dataset
            .merge_existing_index_segments(vec![segment])
            .await
            .unwrap();
        assert_eq!(
            merged.uuid, source_uuid,
            "single-segment merge with unchanged coverage should reuse the segment"
        );
        assert_eq!(
            merged
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            fragment_ids
        );

        dataset
            .commit_existing_index_segments("text_fmindex_single", "text", vec![merged])
            .await
            .unwrap();

        let logical = open_named_scalar_index(
            &dataset,
            "text",
            "text_fmindex_single",
            &NoOpMetricsCollector,
        )
        .await
        .unwrap();
        assert_eq!(logical.index_type(), IndexType::Fm);

        let query = lance_index::scalar::TextQuery::StringContains("delta".to_string());
        let result = logical.search(&query, &NoOpMetricsCollector).await.unwrap();
        let row_addrs = match result {
            SearchResult::Exact(row_addrs) => row_addrs,
            other => panic!("expected exact result from fmindex, got {:?}", other),
        };
        assert_eq!(row_addrs.true_rows().row_addrs().unwrap().count(), 4);
    }

    #[tokio::test]
    async fn test_fmindex_merge_single_segment_rebuilds_when_coverage_shrinks() {
        let test_dir = TempStrDir::default();

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "text",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let write_params = crate::dataset::write::WriteParams {
            max_rows_per_file: 4,
            enable_stable_row_ids: true,
            ..Default::default()
        };
        let batches = vec![
            arrow_array::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow_array::StringArray::from(vec![
                    "alpha beta gamma",
                    "beta gamma delta",
                    "gamma delta epsilon",
                    "delta epsilon zeta",
                    "epsilon zeta eta",
                    "zeta eta theta",
                    "eta theta iota",
                    "theta iota kappa",
                ]))],
            )
            .unwrap(),
        ];
        let reader =
            arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(reader, test_dir.as_str(), Some(write_params))
            .await
            .unwrap();

        let fragments = dataset.get_fragments();
        assert_eq!(fragments.len(), 2);

        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::Fm);
        let segment = CreateIndexBuilder::new(&mut dataset, &["text"], IndexType::Fm, &params)
            .name("text_fmindex_shrink".to_string())
            .fragments(fragments.iter().map(|f| f.id() as u32).collect())
            .execute_uncommitted()
            .await
            .unwrap();
        let source_uuid = segment.uuid;

        // Retire fragment 0: delete its rows and compact it away.
        dataset.delete("text = 'alpha beta gamma'").await.unwrap();
        dataset.delete("text = 'beta gamma delta'").await.unwrap();
        crate::dataset::optimize::compact_files(
            &mut dataset,
            crate::dataset::optimize::CompactionOptions {
                target_rows_per_fragment: 4,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        let live_frags: RoaringBitmap = dataset
            .get_fragments()
            .iter()
            .map(|f| f.id() as u32)
            .collect();
        assert!(
            !live_frags.contains(0),
            "compaction should retire fragment 0"
        );
        assert!(live_frags.contains(1), "fragment 1 should stay live");

        // Coverage shrank, so even a single segment must be rebuilt.
        let merged = dataset
            .merge_existing_index_segments(vec![segment])
            .await
            .unwrap();
        assert_ne!(
            merged.uuid, source_uuid,
            "shrunk coverage must trigger a rebuild"
        );
        assert_eq!(
            merged
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![1]
        );
    }
}
