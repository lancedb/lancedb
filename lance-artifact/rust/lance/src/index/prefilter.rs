// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Secondary Index pre-filter
//!
//! Based on the query, we might have information about which fragment ids and
//! row ids can be excluded from the search.

use std::borrow::Cow;
use std::cell::OnceCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use futures::stream;
use lance_core::utils::deletion::DeletionVector;
use lance_core::utils::tokio::spawn_cpu;
use lance_select::{RowAddrMask, RowAddrTreeMap};
use lance_table::format::Fragment;
use lance_table::format::IndexMetadata;
use lance_table::rowids::RowIdSequence;
use roaring::RoaringBitmap;
use tokio::join;
use tracing::Instrument;
use tracing::instrument;

use crate::Dataset;
use crate::Result;
use crate::dataset::fragment::FileFragment;
use crate::dataset::rowids::load_row_id_sequence;
use crate::utils::future::SharedPrerequisite;

pub use lance_index::prefilter::{FilterLoader, PreFilter};

/// Filter out row ids that we know are not relevant to the query.
///
/// This could be both rows that are deleted or a prefilter
/// that should be applied to the search
///
/// This struct is for internal use only and has no stability guarantees.
pub struct DatasetPreFilter {
    // Expressing these as tasks allows us to start calculating the block list
    // and allow list at the same time we start searching the query.  We will await
    // these tasks only when we've done as much work as we can without them.
    pub(super) deleted_ids: Option<Arc<SharedPrerequisite<Arc<RowAddrMask>>>>,
    pub(super) filtered_ids: Option<Arc<SharedPrerequisite<RowAddrMask>>>,
    // Fragment IDs whose data is still in the index but has been removed from the dataset.
    // Used by FTS merge-on-read to prune stale fragments at search time.
    pub(super) deleted_fragments: Option<RoaringBitmap>,
    // Row addresses whose index entries are stale due to a newer data overlay committed after
    // the index was built. Computed synchronously at plan time and ANDead into the final mask
    // so the index never returns those rows.
    pub(super) overlay_block: Option<RowAddrMask>,
    // When the tasks are finished this is the combined filter
    pub(super) final_mask: Mutex<OnceCell<Arc<RowAddrMask>>>,
}

impl DatasetPreFilter {
    pub fn new(
        dataset: Arc<Dataset>,
        indices: &[IndexMetadata],
        filter: Option<Box<dyn FilterLoader>>,
    ) -> Self {
        let mut fragments = RoaringBitmap::new();
        let all_have_bitmaps = indices.iter().all(|idx| idx.fragment_bitmap.is_some());
        if !all_have_bitmaps {
            if let Some(max_fragment_id) = dataset.manifest.max_fragment_id() {
                fragments.insert_range(0..=max_fragment_id as u32);
            }
        } else {
            indices.iter().for_each(|idx| {
                fragments |= idx.fragment_bitmap.as_ref().unwrap();
            });
        }
        let deleted_ids = if all_have_bitmaps {
            Self::create_restricted_deletion_mask(dataset, fragments)
        } else {
            Self::create_deletion_mask(dataset, fragments)
        }
        .map(SharedPrerequisite::spawn);
        let filtered_ids = filter
            .map(|filtered_ids| SharedPrerequisite::spawn(filtered_ids.load().in_current_span()));
        Self {
            deleted_ids,
            filtered_ids,
            deleted_fragments: None,
            overlay_block: None,
            final_mask: Mutex::new(OnceCell::new()),
        }
    }

    #[instrument(level = "debug", skip_all)]
    async fn do_create_deletion_mask(
        dataset: Arc<Dataset>,
        missing_frags: Vec<u32>,
        frags_with_deletion_files: Vec<u32>,
    ) -> Result<Arc<RowAddrMask>> {
        let fragments = dataset.get_fragments();
        let frag_map: Arc<HashMap<u32, &FileFragment>> = Arc::new(HashMap::from_iter(
            fragments.iter().map(|frag| (frag.id() as u32, frag)),
        ));
        let frag_id_deletion_vectors = stream::iter(
            frags_with_deletion_files
                .iter()
                .map(|frag_id| (frag_id, frag_map.clone())),
        )
        .map(|(frag_id, frag_map)| async move {
            let frag = frag_map.get(frag_id).unwrap();
            frag.get_deletion_vector()
                .await
                .transpose()
                .unwrap()
                .map(|deletion_vector| (*frag_id, RoaringBitmap::from(deletion_vector.as_ref())))
        })
        .collect::<Vec<_>>()
        .await;
        let mut frag_id_deletion_vectors = stream::iter(frag_id_deletion_vectors)
            .buffer_unordered(dataset.object_store.io_parallelism());

        let mut deleted_ids = RowAddrTreeMap::new();
        while let Some((id, deletion_vector)) = frag_id_deletion_vectors.try_next().await? {
            deleted_ids.insert_bitmap(id, deletion_vector);
        }

        for frag_id in missing_frags.into_iter() {
            deleted_ids.insert_fragment(frag_id);
        }
        Ok(Arc::new(RowAddrMask::from_block(deleted_ids)))
    }

    #[instrument(level = "debug", skip_all)]
    async fn do_create_deletion_mask_row_id(
        dataset: Arc<Dataset>,
        restrict_to: Option<RoaringBitmap>,
    ) -> Result<Arc<RowAddrMask>> {
        // The mask is an allow-list of stable row ids. When `restrict_to` is
        // set the iteration is limited to the listed fragments, so the
        // resulting list excludes stable row ids whose *current* physical home
        // is outside the restriction. This is the missing piece for the
        // stable-row-id branch of #6563: without it, the merge_insert UNION
        // (indexed scan ∪ unindexed-fragments scan) sees the same logical row
        // twice — once via the BTREE (which holds the row's stable_row_id) and
        // once via the unindexed scan (which holds the fragment the row now
        // lives in). See issue #6877.
        async fn load_row_ids_and_deletions(
            dataset: &Dataset,
            restrict_to: Option<&RoaringBitmap>,
        ) -> Result<Vec<(Arc<RowIdSequence>, Option<Arc<DeletionVector>>)>> {
            let frags: Vec<_> = dataset
                .get_fragments()
                .into_iter()
                .filter(|f| {
                    restrict_to
                        .map(|allow| allow.contains(f.id() as u32))
                        .unwrap_or(true)
                })
                .collect();
            stream::iter(frags)
                .map(|frag| async move {
                    let row_ids = load_row_id_sequence(dataset, frag.metadata());
                    let deletion_vector = frag.get_deletion_vector();
                    let (row_ids, deletion_vector) = join!(row_ids, deletion_vector);
                    Ok::<_, crate::Error>((row_ids?, deletion_vector?))
                })
                .buffer_unordered(dataset.object_store.as_ref().io_parallelism())
                .try_collect::<Vec<_>>()
                .await
        }

        let restrict_hash = restrict_to.as_ref().map(|b| {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut h = DefaultHasher::new();
            // RoaringBitmap doesn't implement Hash; serialize the sorted u32s.
            for v in b.iter() {
                v.hash(&mut h);
            }
            h.finish()
        });

        let dataset_clone = dataset.clone();
        let restrict_for_load = restrict_to.clone();
        let key = crate::session::caches::RowAddrMaskKey {
            version: dataset.manifest().version,
            restrict_hash,
        };
        dataset
            .metadata_cache
            .as_ref()
            .get_or_insert_with_key(key, move || {
                async move {
                    let row_ids_and_deletions =
                        load_row_ids_and_deletions(&dataset_clone, restrict_for_load.as_ref())
                            .await?;

                    // The process of computing the final mask is CPU-bound, so we spawn it
                    // on a blocking thread.
                    let allow_list = spawn_cpu(move || {
                        Result::Ok(row_ids_and_deletions.into_iter().fold(
                            RowAddrTreeMap::new(),
                            |mut allow_list, (row_ids, deletion_vector)| {
                                let seq = if let Some(deletion_vector) = deletion_vector {
                                    let mut row_ids = row_ids.as_ref().clone();
                                    row_ids.mask(deletion_vector.to_sorted_iter()).unwrap();
                                    Cow::<RowIdSequence>::Owned(row_ids)
                                } else {
                                    Cow::<RowIdSequence>::Borrowed(row_ids.as_ref())
                                };
                                let treemap = RowAddrTreeMap::from(seq.as_ref());
                                allow_list |= treemap;
                                allow_list
                            },
                        ))
                    })
                    .await?;

                    Ok(RowAddrMask::from_allowed(allow_list))
                }
            })
            .await
    }

    /// Sets the deleted fragment IDs to block during search.
    ///
    /// Used by FTS indices which track fragments that have been removed from the
    /// dataset but whose data is still present in the index (merge-on-read).
    pub fn set_deleted_fragments(&mut self, fragments: RoaringBitmap) {
        self.deleted_fragments = Some(fragments);
    }

    /// Block specific row addresses from index results because their index entries are stale
    /// due to a data overlay committed after the index was built.
    pub fn with_overlay_block(mut self, block: RowAddrMask) -> Self {
        self.overlay_block = Some(block);
        self
    }

    /// Creates a task to load a mask that filters out deleted rows and,
    /// when `restrict_to_fragments` is true, also restricts results to only
    /// the given `fragments`.
    ///
    /// The fragment restriction blocks stale index entries from fragments
    /// whose data changed but whose index was not rewritten. It should be
    /// enabled when `fragments` represents a real index fragment bitmap. It
    /// should be disabled when `fragments` is a conservative fallback (e.g.
    /// when the index has no fragment bitmap).
    ///
    /// The deletion mask is built as a block list (from deletion files) or
    /// an allow list (when stable row ids are in use and fragments have
    /// been removed).
    ///
    /// Returns `None` if it can be synchronously determined that no
    /// filtering is needed.
    pub fn create_deletion_mask(
        dataset: Arc<Dataset>,
        fragments: RoaringBitmap,
    ) -> Option<BoxFuture<'static, Result<Arc<RowAddrMask>>>> {
        Self::create_deletion_mask_impl(dataset, fragments, false)
    }

    /// Like [`Self::create_deletion_mask`] but also restricts results to the given
    /// `fragments`, blocking any row from a fragment not in the set.
    pub fn create_restricted_deletion_mask(
        dataset: Arc<Dataset>,
        fragments: RoaringBitmap,
    ) -> Option<BoxFuture<'static, Result<Arc<RowAddrMask>>>> {
        Self::create_deletion_mask_impl(dataset, fragments, true)
    }

    fn create_deletion_mask_impl(
        dataset: Arc<Dataset>,
        fragments: RoaringBitmap,
        restrict_to_fragments: bool,
    ) -> Option<BoxFuture<'static, Result<Arc<RowAddrMask>>>> {
        let mut missing_frags = Vec::new();
        let mut frags_with_deletion_files = Vec::new();
        let frag_map: HashMap<u32, &Fragment> = HashMap::from_iter(
            dataset
                .manifest
                .fragments
                .iter()
                .map(|frag| (frag.id as u32, frag)),
        );
        // When restrict_to_fragments is set, check if the dataset has fragments
        // outside the index bitmap. This can happen when a fragment's data was
        // modified but the index was not rewritten (e.g. after DataReplacement
        // or partial merge_insert).
        //
        // We materialize the set of non-index fragments here so the slow path
        // below can fold them into the deletion mask as Full blocks instead of
        // computing AllowList(Full) - BlockList(Partial), which forces
        // RoaringBitmap::full() per fragment in RowAddrTreeMap::sub_assign and
        // is the dominant cost on every merge_insert call.
        let non_index_frags: Option<RoaringBitmap> = if restrict_to_fragments {
            let dataset_frag_ids: RoaringBitmap = frag_map.keys().copied().collect();
            let outside = &dataset_frag_ids - &fragments;
            (!outside.is_empty()).then_some(outside)
        } else {
            None
        };
        let needs_allow_list = non_index_frags.is_some();
        for frag_id in fragments.iter() {
            let frag = frag_map.get(&frag_id);
            if let Some(frag) = frag {
                if frag.deletion_file.is_some() {
                    frags_with_deletion_files.push(frag_id);
                }
            } else {
                missing_frags.push(frag_id);
            }
        }
        if missing_frags.is_empty() && frags_with_deletion_files.is_empty() && !needs_allow_list {
            None
        } else if dataset.manifest.uses_stable_row_ids() {
            let restrict_to = if restrict_to_fragments {
                Some(fragments)
            } else {
                None
            };
            Some(Self::do_create_deletion_mask_row_id(dataset.clone(), restrict_to).boxed())
        } else if missing_frags.is_empty() && frags_with_deletion_files.is_empty() {
            // No deletions to load, but the dataset has fragments outside the
            // index bitmap. Return a synchronous allow-list mask.
            let mut allow_list = RowAddrTreeMap::new();
            for frag_id in fragments.iter() {
                allow_list.insert_fragment(frag_id);
            }
            Some(async move { Ok(Arc::new(RowAddrMask::from_allowed(allow_list))) }.boxed())
        } else {
            // There are deletions/missing frags. Build the deletion mask and,
            // if needed, fold the non-index fragments into it as Full blocks.
            // Equivalent to BlockList(deletions) | BlockList(non_index) — but
            // expressed via insert_fragment so we never materialize a
            // RoaringBitmap::full() per fragment.
            let fut =
                Self::do_create_deletion_mask(dataset, missing_frags, frags_with_deletion_files);
            if let Some(non_index_frags) = non_index_frags {
                Some(
                    async move {
                        let deletion_mask = fut.await?;
                        let mut combined = match &*deletion_mask {
                            RowAddrMask::BlockList(b) => b.clone(),
                            RowAddrMask::AllowList(_) => {
                                // do_create_deletion_mask only returns BlockList; this is
                                // defensive and should be unreachable.
                                return Ok(deletion_mask);
                            }
                        };
                        for frag_id in non_index_frags.iter() {
                            combined.insert_fragment(frag_id);
                        }
                        Ok(Arc::new(RowAddrMask::from_block(combined)))
                    }
                    .boxed(),
                )
            } else {
                Some(fut.boxed())
            }
        }
    }
}

#[async_trait]
impl PreFilter for DatasetPreFilter {
    /// Waits for the prefilter to be fully loaded
    ///
    /// The prefilter loads in the background while the rest of the index
    /// search is running.  When you are ready to use the prefilter you
    /// must first call this method to ensure it is fully loaded.  This
    /// allows `filter_row_ids` to be a synchronous method.
    #[instrument(level = "debug", skip(self))]
    async fn wait_for_ready(&self) -> Result<()> {
        if let Some(filtered_ids) = &self.filtered_ids {
            filtered_ids.wait_ready().await?;
        }
        if let Some(deleted_ids) = &self.deleted_ids {
            deleted_ids.wait_ready().await?;
        }
        let final_mask = self.final_mask.lock().unwrap();
        final_mask.get_or_init(|| {
            let mut combined = RowAddrMask::default();
            if let Some(filtered_ids) = &self.filtered_ids {
                combined = combined & filtered_ids.get_ready();
            }
            if let Some(deleted_ids) = &self.deleted_ids {
                combined = combined & (*deleted_ids.get_ready()).clone();
            }
            if let Some(deleted) = &self.deleted_fragments {
                let mut block_list = RowAddrTreeMap::new();
                for frag_id in deleted.iter() {
                    block_list.insert_fragment(frag_id);
                }
                combined = combined & RowAddrMask::from_block(block_list);
            }
            if let Some(overlay_block) = &self.overlay_block {
                combined = combined & overlay_block.clone();
            }
            Arc::new(combined)
        });

        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.deleted_ids.is_none()
            && self.filtered_ids.is_none()
            && self.deleted_fragments.is_none()
            && self.overlay_block.is_none()
    }

    /// Get the row id mask for this prefilter
    fn mask(&self) -> Arc<RowAddrMask> {
        self.final_mask
            .lock()
            .unwrap()
            .get()
            .expect("mask called without call to wait_for_ready")
            .clone()
    }

    /// Check whether a slice of row ids should be included in a query.
    ///
    /// Returns a vector of indices into the input slice that should be included,
    /// also known as a selection vector.
    ///
    /// This method must be called after `wait_for_ready`
    #[instrument(level = "debug", skip_all)]
    fn filter_row_ids<'a>(&self, row_ids: Box<dyn Iterator<Item = &'a u64> + 'a>) -> Vec<u64> {
        self.mask().selected_indices(row_ids)
    }
}

#[cfg(test)]
mod test {
    use lance_select::RowSetOps;
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32};
    use rstest::rstest;

    use crate::dataset::WriteParams;

    use super::*;

    struct TestDatasets {
        no_deletions: Arc<Dataset>,
        deletions_no_missing_frags: Arc<Dataset>,
        deletions_missing_frags: Arc<Dataset>,
        only_missing_frags: Arc<Dataset>,
    }

    async fn test_datasets(use_stable_row_id: bool) -> TestDatasets {
        let test_data = BatchGenerator::new()
            .col(Box::new(IncrementingInt32::new().named("x")))
            .batch(9);
        let mut dataset = Dataset::write(
            test_data,
            "memory://test",
            Some(WriteParams {
                max_rows_per_file: 3,
                enable_stable_row_ids: use_stable_row_id,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let no_deletions = Arc::new(dataset.clone());

        // This will add a deletion file.
        dataset.delete("x = 8").await.unwrap();
        let deletions_no_missing_frags = Arc::new(dataset.clone());

        dataset.delete("x >= 3 and x <= 5").await.unwrap();
        assert_eq!(dataset.get_fragments().len(), 2);
        let deletions_missing_frags = Arc::new(dataset.clone());

        dataset.delete("x >= 3").await.unwrap();
        assert_eq!(dataset.get_fragments().len(), 1);
        assert!(
            dataset.get_fragments()[0]
                .metadata()
                .deletion_file
                .is_none()
        );
        let only_missing_frags = Arc::new(dataset.clone());

        TestDatasets {
            no_deletions,
            deletions_no_missing_frags,
            deletions_missing_frags,
            only_missing_frags,
        }
    }

    #[tokio::test]
    async fn test_deletion_mask() {
        let datasets = test_datasets(false).await;

        // If there are no deletions, we should get None
        let mask = DatasetPreFilter::create_deletion_mask(
            datasets.no_deletions.clone(),
            RoaringBitmap::from_iter(0..3),
        );
        assert!(mask.is_none());

        // If there are deletions, we should get a mask
        let mask = DatasetPreFilter::create_deletion_mask(
            datasets.deletions_no_missing_frags.clone(),
            RoaringBitmap::from_iter(0..3),
        );
        assert!(mask.is_some());
        let mask = mask.unwrap().await.unwrap();
        assert_eq!(mask.block_list().and_then(|x| x.len()), Some(1)); // There was just one row deleted.

        // If there are deletions and missing fragments, we should get a mask
        let mask = DatasetPreFilter::create_deletion_mask(
            datasets.deletions_missing_frags.clone(),
            RoaringBitmap::from_iter(0..3),
        );
        assert!(mask.is_some());
        let mask = mask.unwrap().await.unwrap();
        let mut expected = RowAddrTreeMap::from_iter(vec![(2 << 32) + 2]);
        expected.insert_fragment(1);
        assert_eq!(mask.block_list(), Some(&expected));

        // If we don't pass the missing fragment id, we should get a smaller mask.
        let mask = DatasetPreFilter::create_deletion_mask(
            datasets.deletions_missing_frags.clone(),
            RoaringBitmap::from_iter(2..3),
        );
        assert!(mask.is_some());
        let mask = mask.unwrap().await.unwrap();
        assert_eq!(mask.block_list().and_then(|x| x.len()), Some(1));

        // If there are only missing fragments, we should still get a mask
        let mask = DatasetPreFilter::create_deletion_mask(
            datasets.only_missing_frags.clone(),
            RoaringBitmap::from_iter(0..3),
        );
        assert!(mask.is_some());
        let mask = mask.unwrap().await.unwrap();
        let mut expected = RowAddrTreeMap::new();
        expected.insert_fragment(1);
        expected.insert_fragment(2);
        assert_eq!(mask.block_list(), Some(&expected));
    }

    #[rstest]
    #[case::stored_high_water_mark(false)]
    #[case::computed_high_water_mark(true)]
    #[tokio::test]
    async fn test_legacy_index_mask_includes_max_fragment(#[case] unset_max_fragment_id: bool) {
        let datasets = test_datasets(false).await;
        let mut dataset = (*datasets.deletions_missing_frags).clone();
        if unset_max_fragment_id {
            Arc::make_mut(&mut dataset.manifest).max_fragment_id = None;
        }
        let index = IndexMetadata {
            uuid: uuid::Uuid::new_v4(),
            fields: Vec::new(),
            name: "legacy".to_string(),
            dataset_version: dataset.manifest.version,
            fragment_bitmap: None,
            index_details: None,
            index_version: 0,
            created_at: None,
            base_id: None,
            files: None,
        };
        let prefilter = DatasetPreFilter::new(Arc::new(dataset), &[index], None);

        prefilter.wait_for_ready().await.unwrap();

        let mut expected = RowAddrTreeMap::from_iter(vec![(2 << 32) + 2]);
        expected.insert_fragment(1);
        assert_eq!(prefilter.mask().block_list(), Some(&expected));
    }

    #[tokio::test]
    async fn test_deletion_mask_stable_row_id() {
        // Here, behavior is different.
        let datasets = test_datasets(true).await;

        // If there are no deletions, we should get None
        let mask = DatasetPreFilter::create_deletion_mask(
            datasets.no_deletions.clone(),
            RoaringBitmap::from_iter(0..3),
        );
        assert!(mask.is_none());

        // If there are deletions but no missing files, we should get a block list
        let mask = DatasetPreFilter::create_deletion_mask(
            datasets.deletions_no_missing_frags.clone(),
            RoaringBitmap::from_iter(0..3),
        );
        assert!(mask.is_some());
        let mask = mask.unwrap().await.unwrap();
        let expected = RowAddrTreeMap::from_iter(0..8);
        assert_eq!(mask.allow_list(), Some(&expected)); // There was just one row deleted.

        // If there are deletions and missing fragments, we should get an allow list
        let mask = DatasetPreFilter::create_deletion_mask(
            datasets.deletions_missing_frags.clone(),
            RoaringBitmap::from_iter(0..2),
        );
        assert!(mask.is_some());
        let mask = mask.unwrap().await.unwrap();
        assert_eq!(mask.allow_list().and_then(|x| x.len()), Some(5)); // There were five rows left over;

        // If there are only missing fragments, we should get an allow list
        let mask = DatasetPreFilter::create_deletion_mask(
            datasets.only_missing_frags.clone(),
            RoaringBitmap::from_iter(0..3),
        );
        assert!(mask.is_some());
        let mask = mask.unwrap().await.unwrap();
        assert_eq!(mask.allow_list().and_then(|x| x.len()), Some(3)); // There were three rows left over;
    }

    // Regression test for issue #6877.
    //
    // `create_restricted_deletion_mask` on a stable-row-id dataset must honor
    // the bitmap restriction by excluding stable row ids whose *current*
    // physical home is outside the bitmap. Without this, the merge_insert
    // UNION (indexed-scan ∪ unindexed-fragments scan) emits the same logical
    // row twice — once via the BTREE (which holds the row's stable_row_id)
    // and once via the unindexed scan.
    #[tokio::test]
    async fn test_restricted_deletion_mask_stable_row_id_honors_bitmap() {
        // Dataset with three fragments, 3 rows each, stable_row_ids = {0..9}.
        // Row x=8 is deleted, so the live stable_row_ids are {0..7}.
        let datasets = test_datasets(true).await;
        let ds = datasets.deletions_no_missing_frags.clone();

        // Full bitmap: allow-list covers all currently-live stable row ids.
        let mask = DatasetPreFilter::create_restricted_deletion_mask(
            ds.clone(),
            RoaringBitmap::from_iter(0..3),
        )
        .expect("full-bitmap mask present on stable-row-id dataset with deletions")
        .await
        .unwrap();
        let expected_all = RowAddrTreeMap::from_iter(0..8);
        assert_eq!(mask.allow_list(), Some(&expected_all));

        // Restricted to fragments {0, 1}: allow-list must exclude stable row
        // ids whose current home is in fragment 2 (rows 6, 7 — 8 was deleted).
        let mask = DatasetPreFilter::create_restricted_deletion_mask(
            ds.clone(),
            RoaringBitmap::from_iter(0..2),
        )
        .expect("restricted mask present")
        .await
        .unwrap();
        let expected_restricted = RowAddrTreeMap::from_iter(0..6);
        assert_eq!(mask.allow_list(), Some(&expected_restricted));

        // Restricted to empty bitmap: every BTREE-returned address is filtered
        // out. Empty allow-list is the correct semantic ("no row's current home
        // is in the restriction").
        let mask =
            DatasetPreFilter::create_restricted_deletion_mask(ds.clone(), RoaringBitmap::new())
                .expect("empty-restriction mask present")
                .await
                .unwrap();
        assert_eq!(mask.allow_list().and_then(|x| x.len()), Some(0));
    }
}
