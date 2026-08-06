// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Secondary Index
//!

use lance_core::utils::row_addr_remap::RowAddrRemap;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use arrow_schema::DataType;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use futures::{FutureExt, StreamExt, TryStreamExt};
use itertools::Itertools;
use lance_core::cache::{CacheKey, CacheKeySchema, KeyBuilder};
use lance_core::datatypes::Field;
use lance_core::datatypes::Schema as LanceSchema;
use lance_core::utils::parse::parse_env_as_bool;
use lance_core::utils::tracing::{
    IO_TYPE_OPEN_FRAG_REUSE, IO_TYPE_OPEN_MEM_WAL, IO_TYPE_OPEN_VECTOR, TRACE_IO_EVENTS,
};
use lance_file::reader::FileReaderOptions;
use lance_file::versions::v1::reader::FileReader as V1FileReader;
use lance_index::INDEX_METADATA_SCHEMA_KEY;
pub use lance_index::IndexParams;
use lance_index::frag_reuse::{FRAG_REUSE_INDEX_NAME, FragReuseIndex, FragReuseIndexHandle};
use lance_index::mem_wal::{MEM_WAL_INDEX_NAME, MemWalIndex, MemWalIndexHandle};
use lance_index::optimize::OptimizeOptions;
use lance_index::pb::index::Implementation;
pub use lance_index::progress::{IndexBuildProgress, NoopIndexBuildProgress};
use lance_index::scalar::expression::{IndexInformationProvider, MultiQueryParser};
use lance_index::scalar::inverted::{InvertedIndex, InvertedIndexPlugin};
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::registry::{TrainingCriteria, TrainingOrdering};
use lance_index::scalar::{CreatedIndex, ScalarIndex, index_files_to_table, table_files_to_index};
use lance_index::vector::bq::builder::RabitQuantizer;
use lance_index::vector::flat::index::{FlatBinQuantizer, FlatIndex, FlatQuantizer};
use lance_index::vector::hnsw::HNSW;
use lance_index::vector::pq::ProductQuantizer;
use lance_index::vector::quantizer::Quantization;
use lance_index::vector::sq::ScalarQuantizer;
use lance_index::vector::v3::subindex::IvfSubIndex;
use lance_index::{INDEX_FILE_NAME, Index, IndexType, PrewarmOptions, pb, vector::VectorIndex};
use lance_index::{
    IndexCriteria, is_system_index,
    metrics::{MetricsCollector, NoOpMetricsCollector},
    registry::display_type_from_url,
    scalar::btree::BTREE_LOOKUP_NAME,
};
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::traits::Reader;
use lance_io::utils::{
    CachedFileSize, read_last_block, read_message, read_message_from_buf, read_metadata_offset,
    read_version,
};
use lance_table::format::{Fragment, SelfDescribingFileReader};
use lance_table::format::{IndexFile, IndexMetadata, list_index_files_with_sizes};
use lance_table::io::manifest::read_manifest_indexes;
use roaring::RoaringBitmap;
use scalar::index_matches_criteria;
use serde_json::json;
use tracing::{info, instrument, warn};
use uuid::Uuid;
use vector::details::{
    derive_vector_index_type, infer_missing_vector_details, needs_vector_details_inference,
    vector_details_as_json,
};
pub(crate) use vector::details::{vector_index_details, vector_index_details_default};
use vector::ivf::v2::{IVFIndex, IvfStateEntryBox};
use vector::utils::get_vector_type;

mod api;
pub(crate) mod append;
mod create;
pub mod frag_reuse;
pub mod mem_wal;
pub mod prefilter;
pub mod scalar;
pub(crate) mod scalar_logical;
pub mod vector;

use self::append::merge_indices;
use self::vector::remap_vector_index;
use crate::dataset::index::LanceIndexStoreExt;
use crate::dataset::optimize::RemappedIndex;
use crate::dataset::optimize::remapping::RemapResult;
use crate::dataset::transaction::{Operation, Transaction, TransactionBuilder};
pub use crate::index::api::{DatasetIndexExt, IndexSegment, IntoIndexSegment};
use crate::index::frag_reuse::{load_frag_reuse_index_details, open_frag_reuse_index};
use crate::index::mem_wal::open_mem_wal_index;
pub use crate::index::prefilter::{FilterLoader, PreFilter};
use crate::index::scalar::{IndexDetails, fetch_index_details, load_training_data};
pub use crate::index::vector::{LogicalIvfView, LogicalVectorIndex};
use crate::session::index_caches::{FragReuseIndexKey, IndexMetadataKey, write_index_identity};
use crate::{Error, Result, dataset::Dataset};
pub use create::CreateIndexBuilder;
pub use lance_index::IndexDescription;

fn validate_segment_metadata(index_name: &str, segments: &[IndexMetadata]) -> Result<()> {
    if segments.is_empty() {
        return Err(Error::invalid_input(
            "CreateIndex: at least one index segment is required".to_string(),
        ));
    }

    let mut seen_segment_ids = HashSet::with_capacity(segments.len());
    let mut covered_fragments = RoaringBitmap::new();
    for segment in segments {
        if !seen_segment_ids.insert(segment.uuid) {
            return Err(Error::invalid_input(format!(
                "CreateIndex: duplicate segment uuid {} for index '{}'",
                segment.uuid, index_name
            )));
        }
        let fragment_bitmap = segment.fragment_bitmap.as_ref().ok_or_else(|| {
            Error::invalid_input(format!(
                "CreateIndex: segment {} is missing fragment coverage",
                segment.uuid
            ))
        })?;
        if !covered_fragments.is_disjoint(fragment_bitmap) {
            return Err(Error::invalid_input(format!(
                "CreateIndex: overlapping fragment coverage in segment set for index '{}'",
                index_name
            )));
        }
        covered_fragments |= fragment_bitmap.clone();
    }

    Ok(())
}

fn collect_subtree_field_ids(field: &Field, field_ids: &mut HashSet<i32>) {
    field_ids.insert(field.id);
    for child in &field.children {
        collect_subtree_field_ids(child, field_ids);
    }
}

fn fragment_field_paths<'a>(
    fragment: &'a Fragment,
    indexed_field_ids: &HashSet<i32>,
) -> HashMap<i32, &'a str> {
    fragment
        .files
        .iter()
        .flat_map(|file| {
            file.fields
                .iter()
                .filter(|field_id| indexed_field_ids.contains(field_id))
                .map(|field_id| (*field_id, file.path.as_str()))
        })
        .collect()
}

async fn prune_stale_segment_coverage(
    dataset: &Dataset,
    field_id: i32,
    segments: &mut [IndexSegment],
    prune_historically_missing: bool,
    prune_newer_overlays: bool,
) -> Result<()> {
    let field = dataset.schema().field_by_id(field_id).ok_or_else(|| {
        Error::invalid_input(format!(
            "CreateIndex: field id {field_id} does not exist in the current schema"
        ))
    })?;
    let mut indexed_field_ids = HashSet::new();
    collect_subtree_field_ids(field, &mut indexed_field_ids);

    let current_fragments = dataset
        .fragments()
        .iter()
        .map(|fragment| (fragment.id as u32, fragment))
        .collect::<HashMap<_, _>>();
    let historical_versions = segments
        .iter()
        .map(IndexSegment::dataset_version)
        .filter(|version| *version < dataset.manifest.version)
        .collect::<HashSet<_>>();

    for version in historical_versions {
        let historical = dataset.checkout_version(version).await.map_err(|error| {
            Error::invalid_input(format!(
                "CreateIndex: cannot validate segment coverage built at dataset version {version}: {error}"
            ))
        })?;
        let historical_fragments = historical
            .fragments()
            .iter()
            .map(|fragment| (fragment.id as u32, fragment))
            .collect::<HashMap<_, _>>();

        for segment in segments
            .iter_mut()
            .filter(|segment| segment.dataset_version() == version)
        {
            let stale_fragments = segment
                .fragment_bitmap()
                .iter()
                .filter(|fragment_id| {
                    let Some(historical_fragment) = historical_fragments.get(fragment_id) else {
                        return prune_historically_missing;
                    };
                    let Some(current_fragment) = current_fragments.get(fragment_id) else {
                        return true;
                    };
                    let changed_files =
                        fragment_field_paths(historical_fragment, &indexed_field_ids)
                            != fragment_field_paths(current_fragment, &indexed_field_ids);
                    let changed_overlays = prune_newer_overlays
                        && current_fragment.overlays.iter().any(|overlay| {
                            overlay.committed_version > version
                                && overlay
                                    .data_file
                                    .fields
                                    .iter()
                                    .any(|field_id| indexed_field_ids.contains(field_id))
                        });
                    changed_files || changed_overlays
                })
                .collect::<Vec<_>>();
            for fragment_id in stale_fragments {
                segment.fragment_bitmap_mut().remove(fragment_id);
            }
        }
    }
    Ok(())
}

pub(crate) async fn build_index_metadata_from_segments(
    dataset: &Dataset,
    index_name: &str,
    field_id: i32,
    mut segments: Vec<IndexSegment>,
) -> Result<Vec<IndexMetadata>> {
    if segments.is_empty() {
        return Err(Error::invalid_input(
            "CreateIndex: at least one index segment is required".to_string(),
        ));
    }

    let mut seen_segment_ids = HashSet::with_capacity(segments.len());
    let mut covered_fragments = RoaringBitmap::new();
    for segment in &segments {
        if segment.dataset_version() > dataset.manifest.version {
            return Err(Error::invalid_input(format!(
                "CreateIndex: segment {} was built at future dataset version {} (current version {})",
                segment.uuid(),
                segment.dataset_version(),
                dataset.manifest.version
            )));
        }
        if segment.fields() != [field_id] {
            return Err(Error::invalid_input(format!(
                "CreateIndex: segment {} was built for fields {:?}, expected [{}]",
                segment.uuid(),
                segment.fields(),
                field_id
            )));
        }
        if !seen_segment_ids.insert(segment.uuid()) {
            return Err(Error::invalid_input(format!(
                "CreateIndex: duplicate segment uuid {} for index '{}'",
                segment.uuid(),
                index_name
            )));
        }
        if !covered_fragments.is_disjoint(segment.fragment_bitmap()) {
            return Err(Error::invalid_input(format!(
                "CreateIndex: overlapping fragment coverage in segment set for index '{}'",
                index_name
            )));
        }
        covered_fragments |= segment.fragment_bitmap().clone();
    }

    prune_stale_segment_coverage(dataset, field_id, &mut segments, false, false).await?;

    let new_indices = futures::stream::iter(segments.into_iter().map(|segment| async move {
        let (uuid, fragment_bitmap, fields, index_details, index_version, dataset_version) =
            segment.into_parts();
        let is_inverted_index = index_details.type_url.ends_with("InvertedIndexDetails");
        if is_inverted_index {
            let metadata = IndexMetadata {
                uuid,
                name: index_name.to_string(),
                fields: fields.clone(),
                dataset_version,
                fragment_bitmap: Some(fragment_bitmap.clone()),
                index_details: Some(index_details.clone()),
                index_version,
                created_at: Some(chrono::Utc::now()),
                base_id: None,
                files: None,
            };
            crate::index::scalar::inverted::finalize_segment_files_if_needed(dataset, &metadata)
                .await?;
        }
        let index_dir = dataset.indices_dir().join(uuid.to_string());
        let mut files = list_index_files_with_sizes(&dataset.object_store, &index_dir).await?;
        if is_inverted_index {
            retain_committed_inverted_files(&mut files);
        }
        Ok::<_, Error>(IndexMetadata {
            uuid,
            name: index_name.to_string(),
            fields,
            dataset_version,
            fragment_bitmap: Some(fragment_bitmap),
            index_details: Some(index_details),
            index_version,
            created_at: Some(chrono::Utc::now()),
            base_id: None,
            files: Some(files),
        })
    }))
    .buffered(dataset.object_store.io_parallelism())
    .try_collect::<Vec<_>>()
    .await?;

    Ok(new_indices)
}

fn retain_committed_inverted_files(files: &mut Vec<IndexFile>) {
    files.retain(|file| !file.path.starts_with("staging/"));
}

async fn prewarm_opened_index(
    index: Arc<dyn Index>,
    options: Option<&PrewarmOptions>,
) -> Result<()> {
    match options {
        None => index.prewarm().await,
        Some(PrewarmOptions::Fts(fts_options)) => {
            let inverted = index
                .as_any()
                .downcast_ref::<InvertedIndex>()
                .ok_or_else(|| {
                    Error::invalid_input(format!(
                        "FTS prewarm options are only supported for inverted indices, got {:?}",
                        index.index_type()
                    ))
                })?;
            inverted.prewarm_with_options(fts_options).await
        }
        Some(_) => Err(Error::not_supported(
            "unsupported prewarm options for this lance version".to_owned(),
        )),
    }
}

fn total_index_segment_size_bytes(indices: &[IndexMetadata]) -> Option<u64> {
    let mut total = 0u64;
    for index_meta in indices {
        total += index_meta.total_size_bytes()?;
    }
    Some(total)
}

fn cache_size_delta(after: usize, before: usize) -> i64 {
    after.saturating_sub(before) as i64 - before.saturating_sub(after) as i64
}

fn prewarm_options_fields(options: Option<&PrewarmOptions>) -> (&'static str, bool) {
    match options {
        None => ("default", false),
        Some(PrewarmOptions::Fts(fts_options)) => ("fts", fts_options.with_position),
        Some(_) => ("unsupported", false),
    }
}

async fn prewarm_index_segments_by_metadata(
    dataset: &Dataset,
    name: &str,
    indices: Vec<IndexMetadata>,
    options: Option<&PrewarmOptions>,
    available_segment_count: usize,
    requested_segment_count: Option<usize>,
) -> Result<()> {
    let request_started = Instant::now();
    let selected_segment_count = indices.len();
    let selected_size_bytes = total_index_segment_size_bytes(&indices);
    let (prewarm_options, fts_with_position) = prewarm_options_fields(options);
    let cache_stats_before = dataset.session.index_cache_stats().await;
    info!(
        index_name = name,
        selected_segment_count,
        available_segment_count,
        requested_segment_count = requested_segment_count.unwrap_or(0),
        segment_filter = requested_segment_count.is_some(),
        selected_size_bytes = selected_size_bytes.unwrap_or(0),
        selected_size_bytes_known = selected_size_bytes.is_some(),
        index_cache_entries_before = cache_stats_before.num_entries,
        index_cache_size_bytes_before = cache_stats_before.size_bytes,
        prewarm_options,
        fts_with_position,
        "prewarm index segments started"
    );

    let result = futures::future::try_join_all(indices.into_iter().map(|index_meta| async move {
        let index_uuid = index_meta.uuid;
        let size_bytes = index_meta.total_size_bytes();
        let fragment_count = index_meta
            .fragment_bitmap
            .as_ref()
            .map(|bitmap| bitmap.len());
        let segment_started = Instant::now();
        info!(
            index_name = name,
            %index_uuid,
            index_version = index_meta.index_version,
            dataset_version = index_meta.dataset_version,
            fragment_count = fragment_count.unwrap_or(0),
            fragment_count_known = fragment_count.is_some(),
            size_bytes = size_bytes.unwrap_or(0),
            size_bytes_known = size_bytes.is_some(),
            "prewarm index segment started"
        );

        let index = match dataset
            .open_generic_index(name, &index_uuid, &NoOpMetricsCollector)
            .await
        {
            Ok(index) => {
                info!(
                    index_name = name,
                    %index_uuid,
                    index_type = ?index.index_type(),
                    elapsed_ms = segment_started.elapsed().as_millis() as u64,
                    "opened index segment for prewarm"
                );
                index
            }
            Err(err) => {
                warn!(
                    index_name = name,
                    %index_uuid,
                    error = %err,
                    elapsed_ms = segment_started.elapsed().as_millis() as u64,
                    "failed to open index segment for prewarm"
                );
                return Err(err);
            }
        };

        if let Err(err) = prewarm_opened_index(index, options).await {
            warn!(
                index_name = name,
                %index_uuid,
                error = %err,
                elapsed_ms = segment_started.elapsed().as_millis() as u64,
                "prewarm index segment failed"
            );
            return Err(err);
        }

        info!(
            index_name = name,
            %index_uuid,
            elapsed_ms = segment_started.elapsed().as_millis() as u64,
            "prewarm index segment finished"
        );
        Ok(())
    }))
    .await;

    match result {
        Ok(_) => {
            let cache_stats_after = dataset.session.index_cache_stats().await;
            info!(
                index_name = name,
                selected_segment_count,
                index_cache_entries_after = cache_stats_after.num_entries,
                index_cache_entries_delta = cache_size_delta(
                    cache_stats_after.num_entries,
                    cache_stats_before.num_entries,
                ),
                index_cache_size_bytes_after = cache_stats_after.size_bytes,
                index_cache_size_bytes_delta =
                    cache_size_delta(cache_stats_after.size_bytes, cache_stats_before.size_bytes,),
                elapsed_ms = request_started.elapsed().as_millis() as u64,
                "prewarm index segments finished"
            );
            Ok(())
        }
        Err(err) => {
            let cache_stats_after = dataset.session.index_cache_stats().await;
            warn!(
                index_name = name,
                selected_segment_count,
                index_cache_entries_after = cache_stats_after.num_entries,
                index_cache_entries_delta = cache_size_delta(
                    cache_stats_after.num_entries,
                    cache_stats_before.num_entries,
                ),
                index_cache_size_bytes_after = cache_stats_after.size_bytes,
                index_cache_size_bytes_delta = cache_size_delta(
                    cache_stats_after.size_bytes,
                    cache_stats_before.size_bytes,
                ),
                error = %err,
                elapsed_ms = request_started.elapsed().as_millis() as u64,
                "prewarm index segments failed"
            );
            Err(err)
        }
    }
}

fn filter_index_segments_by_ids(
    name: &str,
    indices: Vec<IndexMetadata>,
    segment_ids: &[Uuid],
) -> Result<Vec<IndexMetadata>> {
    if segment_ids.is_empty() {
        return Ok(Vec::new());
    }

    let requested = segment_ids.iter().copied().collect::<HashSet<_>>();
    let mut matched = HashSet::new();
    let filtered = indices
        .into_iter()
        .filter(|index_meta| {
            if requested.contains(&index_meta.uuid) {
                matched.insert(index_meta.uuid);
                true
            } else {
                false
            }
        })
        .collect::<Vec<_>>();

    if matched.len() != requested.len() {
        let mut missing = requested
            .difference(&matched)
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        missing.sort();
        return Err(Error::index_not_found(format!(
            "name={}, segment_ids=[{}]",
            name,
            missing.join(", ")
        )));
    }

    Ok(filtered)
}

fn validate_segment_index_details(index_name: &str, segments: &[IndexMetadata]) -> Result<()> {
    let mut type_url = None::<&str>;
    for segment in segments {
        let segment_type_url = segment.index_details.as_ref().ok_or_else(|| {
            Error::invalid_input(format!(
                "CreateIndex: segment {} is missing index details",
                segment.uuid
            ))
        })?;
        match type_url {
            Some(expected) if expected != segment_type_url.type_url => {
                return Err(Error::invalid_input(format!(
                    "CreateIndex: segment set for index '{}' mixes incompatible index detail types",
                    index_name
                )));
            }
            None => type_url = Some(segment_type_url.type_url.as_str()),
            Some(_) => {}
        }
    }

    Ok(())
}

/// Detect vector segments while preserving the legacy pre-details fallback.
///
/// Older vector segments may not have `VectorIndexDetails` in the manifest, so
/// we also recognize them by the legacy monolithic index file name.
fn segment_has_vector_details(segment: &IndexMetadata) -> bool {
    segment.index_details.as_ref().map_or_else(
        || {
            segment
                .files
                .as_ref()
                .is_some_and(|files| files.iter().any(|file| file.path == INDEX_FILE_NAME))
        },
        |details| details.type_url.ends_with("VectorIndexDetails"),
    )
}

/// Detect FTS / inverted segments from manifest details.
///
/// Unlike vector, inverted segment support was added after index details were
/// part of the segment contract, so there is no legacy file-name fallback here.
fn segment_has_inverted_details(segment: &IndexMetadata) -> bool {
    segment
        .index_details
        .as_ref()
        .is_some_and(|details| details.type_url.ends_with("InvertedIndexDetails"))
}

fn segment_has_bitmap_details(segment: &IndexMetadata) -> bool {
    segment
        .index_details
        .as_ref()
        .is_some_and(|details| details.type_url.ends_with("BitmapIndexDetails"))
}

fn segment_has_bloomfilter_details(segment: &IndexMetadata) -> bool {
    segment
        .index_details
        .as_ref()
        .is_some_and(|details| details.type_url.ends_with("BloomFilterIndexDetails"))
}

/// Detect BTree segments, preserving a legacy pre-details fallback.
fn segment_has_btree_details(segment: &IndexMetadata) -> bool {
    segment.index_details.as_ref().map_or_else(
        || {
            segment
                .files
                .as_ref()
                .is_some_and(|files| files.iter().any(|file| file.path == BTREE_LOOKUP_NAME))
        },
        |details| details.type_url.ends_with("BTreeIndexDetails"),
    )
}

fn segment_has_zonemap_details(segment: &IndexMetadata) -> bool {
    segment
        .index_details
        .as_ref()
        .is_some_and(|details| details.type_url.ends_with("ZoneMapIndexDetails"))
}

/// True when the index reports matches as physical row addresses rather than row ids
/// (`ScalarIndex::results_are_row_addresses`).
///
/// Such an index cannot follow its data through a rewrite: the addresses it stores
/// name fragments and offsets, and neither kind supports remap.
pub(crate) fn index_results_are_row_addrs(index: &IndexMetadata) -> bool {
    segment_has_zonemap_details(index) || segment_has_bloomfilter_details(index)
}

fn segment_has_fmindex_details(segment: &IndexMetadata) -> bool {
    segment
        .index_details
        .as_ref()
        .is_some_and(|details| details.type_url.ends_with("FMIndexDetails"))
}

fn segment_has_label_list_details(segment: &IndexMetadata) -> bool {
    segment
        .index_details
        .as_ref()
        .is_some_and(|details| details.type_url.ends_with("LabelListIndexDetails"))
}

fn segment_has_rtree_details(segment: &IndexMetadata) -> bool {
    segment
        .index_details
        .as_ref()
        .is_some_and(|details| details.type_url.ends_with("RTreeIndexDetails"))
}

fn segment_has_ngram_details(segment: &IndexMetadata) -> bool {
    segment
        .index_details
        .as_ref()
        .is_some_and(|details| details.type_url.ends_with("NGramIndexDetails"))
}

// Cache keys for different index types
#[derive(Debug, Clone)]
pub(crate) struct LegacyVectorIndexCacheKey<'a> {
    uuid: &'a Uuid,
    fri_uuid: Option<&'a Uuid>,
}

impl<'a> LegacyVectorIndexCacheKey<'a> {
    fn new(uuid: &'a Uuid, fri_uuid: Option<&'a Uuid>) -> Self {
        Self { uuid, fri_uuid }
    }
}

impl CacheKey for LegacyVectorIndexCacheKey<'_> {
    type ValueType = CachedLegacyVectorIndex;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        if let Some(fri_uuid) = self.fri_uuid {
            format!("{}-{}", self.uuid, fri_uuid).into()
        } else {
            self.uuid.to_string().into()
        }
    }

    fn type_name() -> &'static str {
        "LegacyVectorIndex"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.index.legacy-vector-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        write_index_identity(builder, self.uuid, self.fri_uuid);
    }
}

/// Sized cache key for `IvfIndexState`.
///
/// Used for v0.3+ indices that support serialization. This key has a codec,
/// so custom cache backends can serialize the state to disk/Redis/etc.
/// Legacy indices use `LegacyVectorIndexCacheKey` instead (in-memory only).
///
/// Note: legacy entries hold live readers bound to the object store that
/// opened them, so they are only valid for credential setups that refresh
/// internally (e.g. a credentials provider). Deployments that pass fresh
/// static credentials per dataset open should use v0.3+ index formats.
#[derive(Debug, Clone)]
pub(crate) struct IvfIndexStateCacheKey<'a> {
    uuid: &'a Uuid,
    fri_uuid: Option<&'a Uuid>,
}

impl<'a> IvfIndexStateCacheKey<'a> {
    fn new(uuid: &'a Uuid, fri_uuid: Option<&'a Uuid>) -> Self {
        Self { uuid, fri_uuid }
    }
}

impl CacheKey for IvfIndexStateCacheKey<'_> {
    type ValueType = IvfStateEntryBox;

    fn type_name() -> &'static str {
        "IvfIndexState"
    }

    fn key(&self) -> std::borrow::Cow<'_, str> {
        if let Some(fri_uuid) = self.fri_uuid {
            format!("{}-{}", self.uuid, fri_uuid).into()
        } else {
            self.uuid.to_string().into()
        }
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.index.ivf-state-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        write_index_identity(builder, self.uuid, self.fri_uuid);
    }

    fn codec() -> Option<lance_core::cache::CacheCodec> {
        Some(lance_core::cache::CacheCodec::from_impl::<IvfStateEntryBox>())
    }
}

/// Wrapper that stores a live VectorIndex in the cache.
/// Used for v0.1/v0.2 indices that don't support serializable caching.
#[derive(Debug, lance_core::deepsize::DeepSizeOf)]
pub(crate) struct CachedLegacyVectorIndex(Arc<dyn VectorIndex>);

#[derive(Debug, Clone)]
pub struct FragReuseIndexCacheKey<'a> {
    pub uuid: &'a Uuid,
    pub fri_uuid: Option<&'a Uuid>,
}

impl<'a> FragReuseIndexCacheKey<'a> {
    pub fn new(uuid: &'a Uuid, fri_uuid: Option<&'a Uuid>) -> Self {
        Self { uuid, fri_uuid }
    }
}

impl CacheKey for FragReuseIndexCacheKey<'_> {
    type ValueType = FragReuseIndex;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        if let Some(fri_uuid) = self.fri_uuid {
            format!("{}-{}", self.uuid, fri_uuid).into()
        } else {
            self.uuid.to_string().into()
        }
    }

    fn type_name() -> &'static str {
        "FragReuseIndex"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.index.fragment-reuse-cache-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        write_index_identity(builder, self.uuid, self.fri_uuid);
    }
}

#[derive(Debug, Clone)]
pub struct MemWalCacheKey<'a> {
    pub uuid: &'a Uuid,
    pub fri_uuid: Option<&'a Uuid>,
}

impl<'a> MemWalCacheKey<'a> {
    pub fn new(uuid: &'a Uuid, fri_uuid: Option<&'a Uuid>) -> Self {
        Self { uuid, fri_uuid }
    }
}

impl CacheKey for MemWalCacheKey<'_> {
    type ValueType = MemWalIndex;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        if let Some(fri_uuid) = self.fri_uuid {
            format!("{}-{}", self.uuid, fri_uuid).into()
        } else {
            self.uuid.to_string().into()
        }
    }

    fn type_name() -> &'static str {
        "MemWalIndex"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.index.mem-wal-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        write_index_identity(builder, self.uuid, self.fri_uuid);
    }
}

// Whether to auto-migrate a dataset when we encounter corruption.
fn auto_migrate_corruption() -> bool {
    static LANCE_AUTO_MIGRATION: OnceLock<bool> = OnceLock::new();
    *LANCE_AUTO_MIGRATION.get_or_init(|| parse_env_as_bool("LANCE_AUTO_MIGRATION", true))
}

/// Derive a friendly (but not necessarily unique) type name from a type URL.
/// Extract a human-friendly type name from a type URL.
///
/// Strips prefixes like `type.googleapis.com/` and package names, then removes
/// trailing `IndexDetails` / `Index` so callers get a concise display name.
fn type_name_from_uri(index_uri: &str) -> String {
    let type_name = index_uri.rsplit('/').next().unwrap_or(index_uri);
    let type_name = type_name.rsplit('.').next().unwrap_or(type_name);
    type_name.trim_end_matches("IndexDetails").to_string()
}

/// Legacy mapping from type URL to the old IndexType string for backwards compatibility.
/// Legacy mapping from type URL to the old IndexType string for backwards compatibility.
///
/// If `index_type_hint` is provided (e.g. parsed from the index statistics of a concrete
/// index instance), it takes precedence so callers can surface the exact index type even
/// when the type URL alone is too generic (such as VectorIndexDetails).
fn legacy_type_name(index_uri: &str, index_type_hint: Option<&str>) -> String {
    if let Some(hint) = index_type_hint {
        return hint.to_string();
    }

    let base = type_name_from_uri(index_uri);

    match base.as_str() {
        "BTree" => IndexType::BTree.to_string(),
        "Bitmap" => IndexType::Bitmap.to_string(),
        "LabelList" => IndexType::LabelList.to_string(),
        "NGram" => IndexType::NGram.to_string(),
        "ZoneMap" => IndexType::ZoneMap.to_string(),
        "BloomFilter" => IndexType::BloomFilter.to_string(),
        "RTree" => IndexType::RTree.to_string(),
        "Inverted" => IndexType::Inverted.to_string(),
        "FMIndex" | "FM" => IndexType::Fm.to_string(),
        "Json" => IndexType::Scalar.to_string(),
        "Flat" | "Vector" => IndexType::Vector.to_string(),
        other if other.contains("Vector") => IndexType::Vector.to_string(),
        _ => "N/A".to_string(),
    }
}

/// Builds index.
#[async_trait]
pub trait IndexBuilder {
    fn index_type() -> IndexType;

    async fn build(&self) -> Result<()>;
}

pub(crate) async fn remap_index(
    dataset: &Dataset,
    index_id: &Uuid,
    row_id_map: &RowAddrRemap,
) -> Result<RemapResult> {
    // Load indices from the disk.
    let indices = dataset.load_indices().await?;
    let matched = indices
        .iter()
        .find(|i| i.uuid == *index_id)
        .ok_or_else(|| Error::index(format!("Index with id {} does not exist", index_id)))?;

    if matched.fields.len() > 1 {
        return Err(Error::index(
            "Remapping indices with multiple fields is not supported".to_string(),
        ));
    }

    if let Some(deleted_bitmap) = row_id_map.fully_deleted_fragments()
        && Some(deleted_bitmap) == matched.fragment_bitmap
    {
        // If remap deleted all rows, we can just return the same index ID.
        // This can happen if there is a bug where the index is covering empty
        // fragment that haven't been cleaned up. They should be cleaned up
        // outside of this function.
        return Ok(RemapResult::Keep(*index_id));
    }

    let field_id = matched
        .fields
        .first()
        .expect("An index existed with no fields");
    let field_path = dataset.schema().field_path(*field_id)?;

    let new_id = Uuid::new_v4();

    let generic = match dataset
        .open_generic_index(&field_path, index_id, &NoOpMetricsCollector)
        .await
    {
        Ok(g) => g,
        Err(e) => {
            log::warn!(
                "Cannot open index '{}' on '{}': {}. \
                 Index will be dropped during compaction.",
                index_id,
                field_path,
                e
            );
            return Ok(RemapResult::Drop);
        }
    };

    let created_index = match generic.index_type() {
        it if it.is_scalar() => {
            let new_store = LanceIndexStore::from_dataset_for_new(dataset, &new_id)?;

            let scalar_index = dataset
                .open_scalar_index(&field_path, index_id, &NoOpMetricsCollector)
                .await?;
            if !scalar_index.can_remap() {
                return Ok(RemapResult::Drop);
            }

            match scalar_index.index_type() {
                IndexType::Inverted => {
                    let inverted_index = scalar_index
                        .as_any()
                        .downcast_ref::<lance_index::scalar::inverted::InvertedIndex>()
                        .ok_or(Error::index("expected inverted index".to_string()))?;
                    if inverted_index.is_legacy() {
                        log::warn!(
                            "reindex because of legacy format, index_type: {}, index_id: {}, field: {}",
                            scalar_index.index_type(),
                            index_id,
                            field_path
                        );
                        let training_data = load_training_data(
                            dataset,
                            &field_path,
                            &TrainingCriteria::new(TrainingOrdering::None),
                            None,
                            true, // Legacy reindexing should always train
                            None,
                        )
                        .await?;
                        InvertedIndexPlugin::train_inverted_index(
                            training_data,
                            &new_store,
                            inverted_index.params().clone(),
                            None,
                            Arc::new(NoopIndexBuildProgress),
                        )
                        .await?
                    } else {
                        scalar_index.remap(row_id_map, &new_store).await?
                    }
                }
                _ => scalar_index.remap(row_id_map, &new_store).await?,
            }
        }
        it if it.is_vector() => {
            let index_version = u32::try_from(matched.index_version).map_err(|_| {
                Error::index(format!(
                    "Invalid vector index version {} on index {}",
                    matched.index_version, matched.name
                ))
            })?;
            let files = remap_vector_index(
                Arc::new(dataset.clone()),
                &field_path,
                index_id,
                &new_id,
                matched,
                row_id_map,
            )
            .await?;

            CreatedIndex {
                index_details: prost_types::Any::from_msg(
                    &lance_index::pb::VectorIndexDetails::default(),
                )
                .unwrap(),
                index_version,
                files: table_files_to_index(files),
            }
        }
        _ => {
            return Err(Error::index(format!(
                "Index type {} is not supported",
                generic.index_type()
            )));
        }
    };

    Ok(RemapResult::Remapped(RemappedIndex {
        old_id: *index_id,
        new_id,
        index_details: created_index.index_details,
        index_version: created_index.index_version,
        files: Some(index_files_to_table(created_index.files)),
    }))
}

/// Snapshot of every scalar index on a dataset, captured at planning time
/// and consumed by the scalar/aggregate pushdown machinery.
///
/// Built once per planner invocation by walking the manifest's `IndexMetadata`
/// entries; thereafter all lookups are synchronous, so optimizer rules and the
/// filter parser can interrogate it without needing an async context.
#[derive(Debug)]
pub struct ScalarIndexInfo {
    /// Per-column dispatch table for [`apply_scalar_indices`]: keyed by the
    /// full dotted field path (e.g. `"x"`, `"metadata.status.code"`), the same
    /// string callers use when referring to columns in filter expressions.
    ///
    /// The value pairs the column's data type with a [`MultiQueryParser`]
    /// that fans out to every per-index parser registered for that column.
    /// When a column carries more than one index (e.g. BTree + bitmap), the
    /// `MultiQueryParser` tries each in order and the first match wins; the
    /// resulting [`crate::scalar::expression::ScalarIndexSearch`] records
    /// which specific index was chosen. So *which* index served the query is
    /// an output of parsing, not an input — that's why this map is keyed only
    /// by column.
    ///
    /// `fragment_bitmaps`, by contrast, *is* keyed by `(column, index_name)`,
    /// because by the time the optimizer needs the bitmap the index name is
    /// already pinned in the parsed leaf.
    indexed_columns: HashMap<String, (DataType, Box<MultiQueryParser>)>,
    /// `(column, index_name) → fragment_bitmap` taken straight off each
    /// [`IndexMetadata`] at construction time. Used by the optimizer rule for
    /// aggregate pushdown to reason about index coverage synchronously.
    /// Indices that omit `fragment_bitmap` (legacy or unsupported) simply
    /// don't appear here and so report coverage as unknown.
    fragment_bitmaps: HashMap<(String, String), RoaringBitmap>,
}

impl IndexInformationProvider for ScalarIndexInfo {
    fn get_index(&self, col: &str) -> Option<(&DataType, &MultiQueryParser)> {
        self.indexed_columns
            .get(col)
            .map(|(ty, parser)| (ty, parser.as_ref()))
    }

    fn fragment_bitmap(&self, column: &str, index_name: &str) -> Option<RoaringBitmap> {
        self.fragment_bitmaps
            .get(&(column.to_string(), index_name.to_string()))
            .cloned()
    }
}

async fn open_index_proto(reader: &dyn Reader) -> Result<pb::Index> {
    let file_size = reader.size().await?;
    let tail_bytes = read_last_block(reader).await?;
    let metadata_pos = read_metadata_offset(&tail_bytes)?;
    let proto: pb::Index = if metadata_pos < file_size - tail_bytes.len() {
        // We have not read the metadata bytes yet.
        read_message(reader, metadata_pos).await?
    } else {
        let offset = tail_bytes.len() - (file_size - metadata_pos);
        read_message_from_buf(&tail_bytes.slice(offset..))?
    };
    Ok(proto)
}

struct IndexDescriptionImpl {
    name: String,
    field_ids: Vec<u32>,
    segments: Vec<IndexMetadata>,
    index_type: String,
    /// Index details, or `None` for indices created before details were
    /// persisted in the manifest. Such indices are still described on a
    /// best-effort basis rather than rejected.
    details: Option<IndexDetails>,
    rows_indexed: u64,
}

impl IndexDescriptionImpl {
    async fn try_new(segments: Vec<IndexMetadata>, dataset: &Dataset) -> Result<Self> {
        if segments.is_empty() {
            return Err(Error::index("Index metadata is empty".to_string()));
        }

        // We assume the type URL and details are the same for all segments
        let example_metadata = &segments[0];

        let name = example_metadata.name.clone();
        if !segments.iter().all(|shard| shard.name == name) {
            return Err(Error::index(
                "Index name should be identical across all segments".to_string(),
            ));
        }

        let field_ids = &example_metadata.fields;
        if !segments.iter().all(|shard| shard.fields == *field_ids) {
            return Err(Error::index(
                "Index fields should be identical across all segments".to_string(),
            ));
        }
        let field_ids_vec: Vec<u32> = field_ids.iter().map(|id| *id as u32).collect();

        // Index details may be absent on indices created before details were
        // persisted in the manifest. We describe such indices on a best-effort
        // basis rather than erroring, so callers can still see they exist.
        let details = example_metadata.index_details.clone().map(IndexDetails);
        if let Some(details) = details.as_ref() {
            let type_url = &details.0.type_url;
            if !segments.iter().all(|shard| {
                shard
                    .index_details
                    .as_ref()
                    .map(|d| d.type_url == *type_url)
                    .unwrap_or(false)
            }) {
                return Err(Error::index(
                    "Index type URL should be present and identical across all segments"
                        .to_string(),
                ));
            }
        }

        let index_type =
            if let Some(system_type) = lance_index::infer_system_index_type(example_metadata) {
                // System indices (frag-reuse, mem-wal) are identified by name, not
                // by index details, so this must be checked before the plugin lookup.
                system_type.to_string()
            } else if let Some(details) = details.as_ref() {
                if details.is_vector() {
                    derive_vector_index_type(&details.0)
                } else {
                    // Fall back to a name derived from the type URL when no plugin
                    // is registered, so a known type URL is never reported as the
                    // opaque "Unknown".
                    details
                        .get_plugin()
                        .map(|p| p.name().to_string())
                        .unwrap_or_else(|_| {
                            display_type_from_url(details.0.type_url.as_str()).to_string()
                        })
                }
            } else if segment_has_vector_details(example_metadata) {
                // Legacy vector indices predate VectorIndexDetails and are
                // recognized by their monolithic index file name.
                "Vector".to_string()
            } else {
                "Unknown".to_string()
            };

        let mut fragment_rows = HashMap::with_capacity(dataset.manifest.fragments.len());
        for fragment in dataset.iter_fragments() {
            fragment_rows.insert(
                fragment.id as u32,
                fragment_logical_rows_from_metadata(fragment)?,
            );
        }
        let mut rows_indexed = 0;
        let mut indexed_fragment_refs = 0u64;
        let mut missing_fragment_refs = 0u64;

        for shard in &segments {
            let Some(fragment_bitmap) = shard.fragment_bitmap.as_ref() else {
                // A system index (e.g. __mem_wal) indexes no fragments, so a
                // missing bitmap means zero indexed rows. For a data index it
                // means unknown coverage — reject rather than fabricate a count.
                if is_system_index(shard) {
                    continue;
                }
                return Err(Error::index("Fragment bitmap is required for index description.  This index must be retrained to support this method.".to_string()));
            };

            indexed_fragment_refs += fragment_bitmap.len();
            for fragment_id in fragment_bitmap.iter() {
                if let Some(fragment_rows) = fragment_rows.get(&fragment_id) {
                    rows_indexed += *fragment_rows;
                } else {
                    missing_fragment_refs += 1;
                }
            }
        }
        tracing::debug!(
            index_name = name.as_str(),
            index_type = index_type.as_str(),
            segment_count = segments.len(),
            dataset_fragment_count = fragment_rows.len(),
            indexed_fragment_refs,
            missing_fragment_refs,
            rows_indexed,
            "described index row coverage from fragment metadata"
        );

        Ok(Self {
            name,
            field_ids: field_ids_vec,
            index_type,
            segments,
            details,
            rows_indexed,
        })
    }
}

fn fragment_logical_rows_from_metadata(fragment: &Fragment) -> Result<u64> {
    fragment.num_rows().map(|rows| rows as u64).ok_or_else(|| {
        Error::internal(format!(
            "Index description requires physical row count and deletion count in fragment metadata. To add this, make a write to this dataset using this library version. Fragment id: {}",
            fragment.id
        ))
    })
}

impl IndexDescription for IndexDescriptionImpl {
    fn name(&self) -> &str {
        &self.name
    }

    fn field_ids(&self) -> &[u32] {
        &self.field_ids
    }

    fn index_type(&self) -> &str {
        &self.index_type
    }

    fn metadata(&self) -> &[IndexMetadata] {
        &self.segments
    }

    fn type_url(&self) -> &str {
        self.details
            .as_ref()
            .map(|d| d.0.type_url.as_str())
            .unwrap_or("")
    }

    fn rows_indexed(&self) -> u64 {
        self.rows_indexed
    }

    fn details(&self) -> Result<String> {
        let Some(details) = self.details.as_ref() else {
            return Ok("{}".to_string());
        };
        if details.is_vector() {
            vector_details_as_json(&details.0)
        } else {
            let plugin = details.get_plugin()?;
            plugin.details_as_json(&details.0).map(|v| v.to_string())
        }
    }

    fn total_size_bytes(&self) -> Option<u64> {
        let mut total = 0u64;
        for segment in &self.segments {
            // If any segment is missing file info, return None for backward compatibility
            let files = segment.files.as_ref()?;
            for file in files {
                total += file.size_bytes;
            }
        }
        Some(total)
    }
}

#[async_trait]
impl DatasetIndexExt for Dataset {
    type IndexBuilder<'a> = CreateIndexBuilder<'a>;
    /// Create a builder for creating an index on columns.
    ///
    /// This returns a builder that can be configured with additional options
    /// before awaiting to execute.
    ///
    /// # Examples
    ///
    /// Create a scalar BTREE index:
    /// ```
    /// # use lance::{Dataset, Result};
    /// # use lance::index::DatasetIndexExt;
    /// # use lance_index::{IndexType, scalar::ScalarIndexParams};
    /// # async fn example(dataset: &mut Dataset) -> Result<()> {
    /// let params = ScalarIndexParams::default();
    /// dataset
    ///     .create_index_builder(&["id"], IndexType::BTree, &params)
    ///     .name("id_index".to_string())
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Create an empty index that will be populated later:
    /// ```
    /// # use lance::{Dataset, Result};
    /// # use lance::index::DatasetIndexExt;
    /// # use lance_index::{IndexType, scalar::ScalarIndexParams};
    /// # async fn example(dataset: &mut Dataset) -> Result<()> {
    /// let params = ScalarIndexParams::default();
    /// dataset
    ///     .create_index_builder(&["category"], IndexType::Bitmap, &params)
    ///     .train(false)  // Create empty index
    ///     .replace(true)  // Replace if exists
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    fn create_index_builder<'a>(
        &'a mut self,
        columns: &[&str],
        index_type: IndexType,
        params: &'a dyn IndexParams,
    ) -> CreateIndexBuilder<'a> {
        CreateIndexBuilder::new(self, columns, index_type, params)
    }

    #[instrument(skip_all)]
    async fn create_index(
        &mut self,
        columns: &[&str],
        index_type: IndexType,
        name: Option<String>,
        params: &dyn IndexParams,
        replace: bool,
    ) -> Result<IndexMetadata> {
        // Use the builder pattern with default train=true for backward compatibility
        let mut builder = self.create_index_builder(columns, index_type, params);

        if let Some(name) = name {
            builder = builder.name(name);
        }

        builder.replace(replace).await
    }

    async fn drop_index(&mut self, name: &str) -> Result<()> {
        let indices = self.load_indices_by_name(name).await?;
        if indices.is_empty() {
            return Err(Error::index_not_found(format!("name={}", name)));
        }

        let transaction = Transaction::new(
            self.manifest.version,
            Operation::CreateIndex {
                new_indices: vec![],
                removed_indices: indices.clone(),
            },
            None,
        );

        self.apply_commit(transaction, &Default::default(), &Default::default())
            .await?;

        Ok(())
    }

    async fn prewarm_index(&self, name: &str) -> Result<()> {
        let indices = self.load_indices_by_name(name).await?;
        if indices.is_empty() {
            return Err(Error::index_not_found(format!("name={}", name)));
        }

        let available_segment_count = indices.len();
        prewarm_index_segments_by_metadata(self, name, indices, None, available_segment_count, None)
            .await
    }

    async fn prewarm_index_with_options(&self, name: &str, options: &PrewarmOptions) -> Result<()> {
        let indices = self.load_indices_by_name(name).await?;
        if indices.is_empty() {
            return Err(Error::index_not_found(format!("name={}", name)));
        }

        let available_segment_count = indices.len();
        prewarm_index_segments_by_metadata(
            self,
            name,
            indices,
            Some(options),
            available_segment_count,
            None,
        )
        .await
    }

    async fn prewarm_index_segments(&self, name: &str, segment_ids: &[Uuid]) -> Result<()> {
        let indices = self.load_indices_by_name(name).await?;
        if indices.is_empty() {
            return Err(Error::index_not_found(format!("name={}", name)));
        }
        let available_segment_count = indices.len();
        let indices = filter_index_segments_by_ids(name, indices, segment_ids)?;

        prewarm_index_segments_by_metadata(
            self,
            name,
            indices,
            None,
            available_segment_count,
            Some(segment_ids.len()),
        )
        .await
    }

    async fn prewarm_index_segments_with_options(
        &self,
        name: &str,
        segment_ids: &[Uuid],
        options: &PrewarmOptions,
    ) -> Result<()> {
        let indices = self.load_indices_by_name(name).await?;
        if indices.is_empty() {
            return Err(Error::index_not_found(format!("name={}", name)));
        }
        let available_segment_count = indices.len();
        let indices = filter_index_segments_by_ids(name, indices, segment_ids)?;

        prewarm_index_segments_by_metadata(
            self,
            name,
            indices,
            Some(options),
            available_segment_count,
            Some(segment_ids.len()),
        )
        .await
    }

    async fn describe_indices<'a, 'b>(
        &'a self,
        criteria: Option<IndexCriteria<'b>>,
    ) -> Result<Vec<Arc<dyn IndexDescription>>> {
        let indices = self.load_indices().await?;
        let mut indices = if let Some(criteria) = criteria {
            indices.iter().filter(|idx| {
                if idx.index_details.is_none() {
                    log::warn!("The method describe_indices does not support indexes without index details.  Please retrain the index {}", idx.name);
                    return false;
                }
                let fields = idx
                    .fields
                    .iter()
                    .filter_map(|id| self.schema().field_by_id(*id))
                    .collect::<Vec<_>>();
                match index_matches_criteria(idx, &criteria, &fields, false, self.schema()) {
                    Ok(matched) => matched,
                    Err(err) => {
                        log::warn!("Could not describe index {}: {}", idx.name, err);
                        false
                    }
                }
            }).collect::<Vec<_>>()
        } else {
            indices.iter().collect::<Vec<_>>()
        };
        indices.sort_by_key(|idx| &idx.name);

        let grouped: Vec<Vec<IndexMetadata>> = indices
            .into_iter()
            .chunk_by(|idx| idx.name.clone())
            .into_iter()
            .map(|(_, segments)| segments.cloned().collect::<Vec<_>>())
            .collect();

        let mut results = Vec::with_capacity(grouped.len());
        for segments in grouped {
            let desc = IndexDescriptionImpl::try_new(segments, self).await?;
            results.push(Arc::new(desc) as Arc<dyn IndexDescription>);
        }
        Ok(results)
    }

    async fn load_indices(&self) -> Result<Arc<Vec<IndexMetadata>>> {
        let metadata_key = IndexMetadataKey {
            version: self.version().version,
            store_identity: &self.object_store.store_prefix,
        };
        let mut indices = self
            .index_cache
            .get_or_insert_with_key(metadata_key, || async {
                let mut loaded_indices = read_manifest_indexes(
                    &self.object_store,
                    &self.manifest_location,
                    &self.manifest,
                )
                .await?;
                retain_supported_indices(&mut loaded_indices);
                Ok(loaded_indices)
            })
            .await?;

        // Infer details for legacy vector indices (once per index name, concurrently).
        // This may run on indices that were opportunistically cached during Dataset::open
        // before the full Dataset was available for inference.
        {
            let schema = self.schema();
            if indices
                .iter()
                .any(|idx| needs_vector_details_inference(idx, schema))
            {
                let mut updated = indices.as_ref().clone();
                infer_missing_vector_details(self, &mut updated).await;
                if updated != *indices {
                    indices = Arc::new(updated);
                    self.index_cache
                        .insert_with_key(&metadata_key, indices.clone())
                        .await;
                }
            }
        }

        if let Some(frag_reuse_index_meta) =
            indices.iter().find(|idx| idx.name == FRAG_REUSE_INDEX_NAME)
        {
            let fri_key = FragReuseIndexKey {
                uuid: &frag_reuse_index_meta.uuid,
            };
            let frag_reuse_index = self
                .index_cache
                .get_or_insert_with_key(fri_key, || async move {
                    let index_details =
                        load_frag_reuse_index_details(self, frag_reuse_index_meta).await?;
                    open_frag_reuse_index(frag_reuse_index_meta.uuid, index_details.as_ref()).await
                })
                .await?;
            let mut indices = indices.as_ref().clone();
            for idx in indices.iter_mut() {
                if let Some(bitmap) = idx.fragment_bitmap.as_mut() {
                    frag_reuse_index.remap_fragment_bitmap(bitmap)?;
                }
            }
            Ok(Arc::new(indices))
        } else {
            Ok(indices)
        }
    }

    async fn merge_existing_index_segments(
        &self,
        mut source_segments: Vec<IndexMetadata>,
    ) -> Result<IndexMetadata> {
        validate_segment_metadata("uncommitted", &source_segments)?;
        if let Some(segment) = source_segments
            .iter()
            .find(|segment| segment.dataset_version > self.manifest.version)
        {
            return Err(Error::invalid_input(format!(
                "merge_existing_index_segments: segment {} was built at future dataset version {} (current version {})",
                segment.uuid, segment.dataset_version, self.manifest.version
            )));
        }
        let source_dataset_version = source_segments
            .iter()
            .map(|segment| segment.dataset_version)
            .min()
            .unwrap_or(self.manifest.version);
        let field_id = *source_segments[0].fields.first().ok_or_else(|| {
            Error::invalid_input(format!(
                "CreateIndex: segment {} is missing field ids",
                source_segments[0].uuid
            ))
        })?;
        if source_segments
            .iter()
            .any(|segment| segment.fields != [field_id])
        {
            return Err(Error::invalid_input(
                "merge_existing_index_segments requires segments with identical fields".to_string(),
            ));
        }
        let all_vector = source_segments.iter().all(segment_has_vector_details);
        let all_inverted = source_segments.iter().all(segment_has_inverted_details);
        let all_bitmap = source_segments.iter().all(segment_has_bitmap_details);
        let all_bloomfilter = source_segments.iter().all(segment_has_bloomfilter_details);
        let all_btree = source_segments.iter().all(segment_has_btree_details);
        let all_fmindex = source_segments.iter().all(segment_has_fmindex_details);
        let all_zonemap = source_segments.iter().all(segment_has_zonemap_details);
        let all_label_list = source_segments.iter().all(segment_has_label_list_details);
        let all_rtree = source_segments.iter().all(segment_has_rtree_details);
        let all_ngram = source_segments.iter().all(segment_has_ngram_details);
        if !all_vector
            && !all_inverted
            && !all_bitmap
            && !all_bloomfilter
            && !all_btree
            && !all_fmindex
            && !all_zonemap
            && !all_label_list
            && !all_rtree
            && !all_ngram
        {
            return Err(Error::invalid_input(
                "merge_existing_index_segments requires all segments to have the same supported index type"
                    .to_string(),
            ));
        }

        let merged_dataset_version = if all_rtree {
            let mut source_coverage = source_segments
                .iter()
                .cloned()
                .map(IntoIndexSegment::into_index_segment)
                .collect::<Result<Vec<_>>>()?;
            prune_stale_segment_coverage(self, field_id, &mut source_coverage, true, true).await?;
            for (source, coverage) in source_segments.iter_mut().zip(source_coverage) {
                source.fragment_bitmap = Some(coverage.fragment_bitmap().clone());
            }
            self.manifest.version
        } else {
            source_dataset_version
        };

        let mut merged_segment = if all_vector {
            crate::index::vector::ivf::merge_segments(
                self.object_store.as_ref(),
                &self.indices_dir(),
                source_segments,
            )
            .await?
        } else if all_inverted {
            crate::index::scalar::inverted::merge_segments(self, source_segments).await?
        } else if all_fmindex {
            crate::index::scalar::fmindex::merge_segments(self, source_segments).await?
        } else if all_bitmap {
            crate::index::scalar::bitmap::merge_segments(self, source_segments).await?
        } else if all_bloomfilter {
            crate::index::scalar::bloomfilter::merge_segments(self, source_segments).await?
        } else if all_label_list {
            crate::index::scalar::label_list::merge_segments(self, source_segments).await?
        } else if all_zonemap {
            crate::index::scalar::zonemap::merge_segments(self, source_segments).await?
        } else if all_ngram {
            crate::index::scalar::ngram::merge_segments(self, source_segments).await?
        } else if all_rtree {
            #[cfg(feature = "geo")]
            {
                crate::index::scalar::rtree::merge_segments(self, source_segments).await?
            }
            #[cfg(not(feature = "geo"))]
            return Err(Error::not_supported(
                "RTree segment merge requires the `geo` feature".to_string(),
            ));
        } else {
            crate::index::scalar::btree::merge_segments(self, source_segments).await?
        };
        if !all_ngram && !all_fmindex {
            merged_segment.dataset_version = merged_dataset_version;
        }
        merged_segment.fields = vec![field_id];
        Ok(merged_segment)
    }

    async fn commit_existing_index_segments(
        &mut self,
        index_name: &str,
        column: &str,
        segments: Vec<impl IntoIndexSegment + Send>,
    ) -> Result<()> {
        let Some(field) = self.schema().field(column) else {
            return Err(Error::index(format!(
                "CreateIndex: column '{column}' does not exist"
            )));
        };

        let segments = segments
            .into_iter()
            .map(IntoIndexSegment::into_index_segment)
            .collect::<Result<Vec<_>>>()?;
        let dataset_fragments = self.fragment_bitmap.as_ref().clone();
        if segments.first().is_some_and(|segment| {
            segment
                .index_details()
                .type_url
                .ends_with("NGramIndexDetails")
        }) {
            let has_retired_coverage = segments
                .iter()
                .any(|segment| !(segment.fragment_bitmap() - &dataset_fragments).is_empty());
            let frag_reuse_index = self.open_frag_reuse_index(&NoOpMetricsCollector).await?;
            let requires_rebuild = frag_reuse_index.as_ref().is_some_and(|frag_reuse_index| {
                segments.iter().any(|segment| {
                    append::fragment_reuse_affects_segment(
                        frag_reuse_index,
                        segment.fragment_bitmap(),
                        segment.dataset_version(),
                    )
                })
            });
            if has_retired_coverage || requires_rebuild {
                return Err(Error::invalid_input(
                    "CreateIndex: NGram segments built before compaction must be rebuilt or merged \
                     with merge_existing_index_segments before commit"
                        .to_string(),
                ));
            }
        }
        let new_indices =
            build_index_metadata_from_segments(self, index_name, field.id, segments).await?;
        validate_segment_metadata(index_name, &new_indices)?;
        validate_segment_index_details(index_name, &new_indices)?;

        let incoming_type_url = new_indices[0]
            .index_details
            .as_ref()
            .map(|details| details.type_url.clone());
        let mut incoming_fragments = RoaringBitmap::new();
        for segment in &new_indices {
            if segment.fields != [field.id] {
                return Err(Error::invalid_input(format!(
                    "CreateIndex: segment {} was built for fields {:?}, expected [{}]",
                    segment.uuid, segment.fields, field.id
                )));
            }
            if let Some(fragment_bitmap) = &segment.fragment_bitmap {
                incoming_fragments |= fragment_bitmap.clone();
            }
        }

        let existing_named_indices = self.load_indices_by_name(index_name).await?;
        if existing_named_indices
            .iter()
            .any(|idx| idx.fields != [field.id])
        {
            return Err(Error::index(format!(
                "Index name '{index_name}' already exists with different fields, \
                please specify a different name"
            )));
        }
        let existing_different_type_url = existing_named_indices.iter().find_map(|idx| {
            // Legacy metadata may omit index details. Its type cannot be proven
            // compatible, so only a full replacement is safe.
            let Some(existing_details) = idx.index_details.as_ref() else {
                return Some("<unknown>".to_owned());
            };
            let existing_type_url = existing_details.type_url.as_str();
            (Some(existing_type_url) != incoming_type_url.as_deref())
                .then(|| existing_type_url.to_owned())
        });
        let missing_fragments = &dataset_fragments - &incoming_fragments;
        if let Some(existing_type_url) = &existing_different_type_url
            && !missing_fragments.is_empty()
        {
            return Err(Error::invalid_input(format!(
                "CreateIndex: cannot change index '{}' from type '{}' to type '{}' with partial fragment coverage; incoming segments are missing current fragments {:?}",
                index_name,
                existing_type_url,
                incoming_type_url.as_deref().unwrap_or("<missing>"),
                missing_fragments.iter().collect::<Vec<_>>()
            )));
        }

        let is_index_type_change = existing_different_type_url.is_some();
        let removed_indices = existing_named_indices
            .into_iter()
            .map(|idx| -> Result<Option<IndexMetadata>> {
                // A logical index cannot combine segment types. Full current-fragment
                // coverage was verified above, so a type change replaces every segment.
                if is_index_type_change {
                    return Ok(Some(idx));
                }

                let Some(existing_fragments) = idx.effective_fragment_bitmap(&dataset_fragments)
                else {
                    if incoming_fragments != dataset_fragments {
                        return Err(Error::invalid_input(format!(
                            "CreateIndex: cannot replace legacy index segment {} for '{}' with partial fragment coverage; rebuild all fragments in one commit",
                            idx.uuid, index_name
                        )));
                    }
                    return Ok(Some(idx));
                };

                // A zero-fragment segment can be used to create an index while
                // deferring the actual build. Such a segment is disjoint from every
                // other segment but should still be removed.
                if existing_fragments.is_empty() {
                    return Ok(Some(idx));
                }

                if existing_fragments.is_disjoint(&incoming_fragments) {
                    return Ok(None);
                }

                let uncovered = existing_fragments - &incoming_fragments;
                if !uncovered.is_empty() {
                    return Err(Error::invalid_input(format!(
                        "CreateIndex: incoming segments for '{}' would orphan fragments {:?} from existing segment {}",
                        index_name,
                        uncovered.iter().collect::<Vec<_>>(),
                        idx.uuid
                    )));
                }

                Ok(Some(idx))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let transaction = Transaction::new(
            self.manifest.version,
            Operation::CreateIndex {
                new_indices,
                removed_indices,
            },
            None,
        );

        self.apply_commit(transaction, &Default::default(), &Default::default())
            .await?;

        Ok(())
    }

    async fn load_scalar_index<'a, 'b>(
        &'a self,
        criteria: IndexCriteria<'b>,
    ) -> Result<Option<IndexMetadata>> {
        let indices = self.load_indices().await?;

        let mut indices = indices
            .iter()
            .filter(|idx| {
                // We shouldn't have any indices with empty fields, but just in case, log an error
                // but don't fail the operation (we might not be using that index)
                if idx.fields.is_empty() {
                    if idx.name != FRAG_REUSE_INDEX_NAME {
                        log::error!("Index {} has no fields", idx.name);
                    }
                    false
                } else {
                    true
                }
            })
            .collect::<Vec<_>>();
        // This sorting & chunking is only needed to calculate if there are multiple indexes on the same
        // field.  This fact is only needed for backwards compatibility behavior for indexes that don't have
        // index details.  At some point we should deprecate indexes without index details.
        //
        // TODO: At some point we should just fail if the index details are missing and ask the user to
        // retrain the index.
        indices.sort_by_key(|idx| idx.fields[0]);
        let indice_by_field = indices.into_iter().chunk_by(|idx| idx.fields[0]);
        for (field_id, indices) in &indice_by_field {
            let indices = indices.collect::<Vec<_>>();
            let has_multiple = indices.len() > 1;
            for idx in indices {
                let field = self.schema().field_by_id(field_id);
                if let Some(field) = field
                    && index_matches_criteria(
                        idx,
                        &criteria,
                        &[field],
                        has_multiple,
                        self.schema(),
                    )?
                {
                    let non_empty = idx.fragment_bitmap.as_ref().is_some_and(|bitmap| {
                        bitmap.intersection_len(self.fragment_bitmap.as_ref()) > 0
                    });
                    let is_fts_index = if let Some(details) = &idx.index_details {
                        IndexDetails(details.clone()).supports_fts()
                    } else {
                        false
                    };
                    // FTS indices must always be returned even if empty, because FTS queries
                    // require an index to exist. The query execution will handle the empty
                    // bitmap appropriately and fall back to scanning unindexed data.
                    // Other index types can be skipped if empty since they're optional optimizations.
                    if non_empty || is_fts_index {
                        return Ok(Some(idx.clone()));
                    }
                }
            }
        }
        return Ok(None);
    }

    #[instrument(skip_all)]
    async fn optimize_indices(&mut self, options: &OptimizeOptions) -> Result<()> {
        let dataset = Arc::new(self.clone());
        let indices = self.load_indices().await?;

        let indices_to_optimize = options
            .index_names
            .as_ref()
            .map(|names| names.iter().collect::<HashSet<_>>());
        let name_to_indices = indices
            .iter()
            .filter(|idx| {
                indices_to_optimize
                    .as_ref()
                    .is_none_or(|names| names.contains(&idx.name))
                    && !is_system_index(idx)
            })
            .map(|idx| (idx.name.clone(), idx))
            .into_group_map();

        let mut new_indices = vec![];
        let mut removed_indices = vec![];
        for deltas in name_to_indices.values() {
            // Scalar indices have no rebalance concept, so skip them entirely
            // when every fragment is already covered and the caller hasn't
            // asked for retrain or an explicit delta merge. Vector indices
            // fall through and use a rebalance-aware no-op check inside
            // merge_indices_with_unindexed_frags.
            if !options.retrain
                && options.num_indices_to_merge.is_none_or(|n| n == 0)
                && index_group_is_scalar(self, deltas)
                && index_group_has_no_unindexed(self, deltas)
            {
                continue;
            }

            let Some(res) = merge_indices(dataset.clone(), deltas.as_slice(), options).await?
            else {
                continue;
            };

            let last_idx = deltas.last().expect("Delta indices should not be empty");
            let new_idx = IndexMetadata {
                uuid: res.new_uuid,
                name: last_idx.name.clone(), // Keep the same name
                fields: last_idx.fields.clone(),
                dataset_version: res.new_dataset_version,
                fragment_bitmap: Some(res.new_fragment_bitmap),
                index_details: Some(Arc::new(res.new_index_details)),
                index_version: res.new_index_version,
                created_at: Some(chrono::Utc::now()),
                base_id: None, // New merged index file locates in the cloned dataset.
                files: Some(res.files),
            };
            removed_indices.extend(res.removed_indices.iter().map(|&idx| idx.clone()));
            new_indices.push(new_idx);
        }

        if new_indices.is_empty() {
            return Ok(());
        }

        let transaction = TransactionBuilder::new(
            self.manifest.version,
            Operation::CreateIndex {
                new_indices,
                removed_indices,
            },
        )
        .transaction_properties(options.transaction_properties.clone())
        .build();

        self.apply_commit(transaction, &Default::default(), &Default::default())
            .await?;

        Ok(())
    }

    async fn index_statistics(&self, index_name: &str) -> Result<String> {
        let metadatas = self.load_indices_by_name(index_name).await?;
        if metadatas.is_empty() {
            return Err(Error::index_not_found(format!("name={}", index_name)));
        }

        if index_name == FRAG_REUSE_INDEX_NAME {
            return index_statistics_frag_reuse(self).boxed().await;
        }

        if index_name == MEM_WAL_INDEX_NAME {
            return index_statistics_mem_wal(self).boxed().await;
        }

        index_statistics_scalar(self, index_name, metadatas)
            .boxed()
            .await
    }

    async fn read_index_partition(
        &self,
        index_name: &str,
        partition_id: usize,
        with_vector: bool,
    ) -> Result<SendableRecordBatchStream> {
        let indices = self.load_indices_by_name(index_name).await?;
        if indices.is_empty() {
            return Err(Error::index_not_found(format!("name={}", index_name)));
        }
        let field_id = indices[0].fields[0];
        let field_path = self.schema().field_path(field_id)?;
        let logical_index = self
            .open_logical_vector_index(&field_path, index_name)
            .await?;
        logical_index
            .as_ivf()?
            .read_partition(partition_id, with_vector)
            .await
    }
}

fn index_group_is_scalar(dataset: &Dataset, deltas: &[&IndexMetadata]) -> bool {
    let Some(field_id) = deltas.first().and_then(|d| d.fields.first()) else {
        return false;
    };
    match dataset.schema().field_by_id(*field_id) {
        Some(field) => !is_vector_field(field.data_type()),
        None => false,
    }
}

fn index_group_has_no_unindexed(dataset: &Dataset, deltas: &[&IndexMetadata]) -> bool {
    let mut indexed = RoaringBitmap::new();
    for idx in deltas {
        if let Some(bitmap) = idx.fragment_bitmap.as_ref() {
            indexed |= bitmap;
        } else {
            // Pre-0.8 indices have no fragment bitmap; treat as needing optimize.
            return false;
        }
    }
    dataset
        .fragments()
        .iter()
        .all(|frag| indexed.contains(frag.id as u32))
}

fn sum_indexed_rows_per_delta(indexed_fragments_per_delta: &[Vec<Fragment>]) -> Result<Vec<usize>> {
    let mut rows_per_delta = Vec::with_capacity(indexed_fragments_per_delta.len());
    for frags in indexed_fragments_per_delta {
        let mut sum = 0usize;
        for frag in frags {
            sum += frag.num_rows().ok_or_else(|| {
                Error::internal(
                    "Fragment should have row counts, please upgrade lance and \
                                  trigger a single write to fix this"
                        .to_string(),
                )
            })?;
        }
        rows_per_delta.push(sum);
    }
    Ok(rows_per_delta)
}

fn unique_indexed_fragment_count(indexed_fragments_per_delta: &[Vec<Fragment>]) -> Option<usize> {
    let mut fragment_ids = HashSet::new();
    for frags in indexed_fragments_per_delta {
        for frag in frags {
            if !fragment_ids.insert(frag.id) {
                return None;
            }
        }
    }
    Some(fragment_ids.len())
}

fn serialize_index_statistics(stats: &serde_json::Value) -> Result<String> {
    serde_json::to_string(stats)
        .map_err(|e| Error::index(format!("Failed to serialize index statistics: {}", e)))
}

async fn migrate_and_recompute_index_statistics(ds: &Dataset, index_name: &str) -> Result<String> {
    let mut ds = ds.clone();
    log::warn!(
        "Detecting out-dated fragment metadata, migrating dataset. \
                        To disable migration, set LANCE_AUTO_MIGRATION=false"
    );
    ds.delete("false").await.map(|_| ()).map_err(|err| {
        Error::execution(format!(
            "Failed to migrate dataset while calculating index statistics. \
                        To disable migration, set LANCE_AUTO_MIGRATION=false. Original error: {}",
            err
        ))
    })?;
    ds.index_statistics(index_name).await
}

async fn index_statistics_frag_reuse(ds: &Dataset) -> Result<String> {
    let index = ds
        .open_frag_reuse_index(&NoOpMetricsCollector)
        .await?
        .expect("FragmentReuse index does not exist");
    serialize_index_statistics(&FragReuseIndexHandle(index).statistics()?)
}

async fn index_statistics_mem_wal(ds: &Dataset) -> Result<String> {
    let index = ds
        .open_mem_wal_index(&NoOpMetricsCollector)
        .await?
        .expect("MemWal index does not exist");
    serialize_index_statistics(&MemWalIndexHandle(index).statistics()?)
}

async fn index_statistics_scalar(
    ds: &Dataset,
    index_name: &str,
    metadatas: Vec<IndexMetadata>,
) -> Result<String> {
    let field_id = metadatas[0].fields[0];
    let field_path = ds.schema().field_path(field_id)?;

    let (indices_stats, index_uri, num_indices, updated_at) =
        collect_regular_indices_statistics(ds, metadatas, &field_path).await?;

    let index_type_hint = indices_stats
        .first()
        .and_then(|stats| stats.get("index_type"))
        .and_then(|v| v.as_str());
    let index_type = legacy_type_name(&index_uri, index_type_hint);

    let Some((
        num_indexed_rows_per_delta,
        num_indexed_fragments,
        num_unindexed_fragments,
        num_indexed_rows,
        num_unindexed_rows,
    )) = gather_fragment_statistics(ds, index_name).await?
    else {
        return migrate_and_recompute_index_statistics(ds, index_name).await;
    };

    let stats = json!({
        "index_type": index_type,
        "name": index_name,
        "num_indices": num_indices,
        "num_segments": num_indices,
        "indices": indices_stats.clone(),
        "segments": indices_stats,
        "num_indexed_fragments": num_indexed_fragments,
        "num_indexed_rows": num_indexed_rows,
        "num_unindexed_fragments": num_unindexed_fragments,
        "num_unindexed_rows": num_unindexed_rows,
        "num_indexed_rows_per_delta": num_indexed_rows_per_delta,
        "updated_at_timestamp_ms": updated_at,
    });

    serialize_index_statistics(&stats)
}

async fn collect_regular_indices_statistics(
    ds: &Dataset,
    metadatas: Vec<IndexMetadata>,
    field_path: &str,
) -> Result<(Vec<serde_json::Value>, String, usize, Option<u64>)> {
    let num_indices = metadatas.len();
    let updated_at = metadatas
        .iter()
        .filter_map(|m| m.created_at)
        .max()
        .map(|dt| dt.timestamp_millis() as u64);

    let mut indices_stats = Vec::with_capacity(num_indices);
    let mut index_uri: Option<String> = None;

    for meta in metadatas.iter() {
        let index_store = Arc::new(LanceIndexStore::from_dataset_for_existing(ds, meta).await?);
        let index_details = scalar::fetch_index_details(ds, field_path, meta).await?;
        if index_uri.is_none() {
            index_uri = Some(index_details.type_url.clone());
        }

        let index_details_wrapper = scalar::IndexDetails(index_details.clone());
        if let Ok(plugin) = index_details_wrapper.get_plugin()
            && let Some(stats) = plugin
                .load_statistics(index_store.clone(), index_details.as_ref())
                .await?
        {
            indices_stats.push(stats);
            continue;
        }

        let index = ds
            .open_generic_index(field_path, &meta.uuid, &NoOpMetricsCollector)
            .await?;

        indices_stats.push(index.statistics()?);
    }

    Ok((
        indices_stats,
        index_uri.unwrap_or_else(|| "unknown".to_string()),
        num_indices,
        updated_at,
    ))
}

async fn gather_fragment_statistics(
    ds: &Dataset,
    index_name: &str,
) -> Result<Option<(Vec<usize>, usize, usize, usize, usize)>> {
    let indexed_fragments_per_delta = ds.indexed_fragments(index_name).await?;

    let num_indexed_rows_per_delta = match sum_indexed_rows_per_delta(&indexed_fragments_per_delta)
    {
        Ok(rows) => rows,
        Err(Error::Internal { message, .. })
            if auto_migrate_corruption() && message.contains("trigger a single write") =>
        {
            return Ok(None);
        }
        Err(e) => return Err(e),
    };

    let Some(num_indexed_fragments) = unique_indexed_fragment_count(&indexed_fragments_per_delta)
    else {
        if auto_migrate_corruption() {
            return Ok(None);
        }
        return Err(Error::internal(
            "Overlap in indexed fragments. Please upgrade to lance >= 0.23.0 \
                              and trigger a single write to fix this"
                .to_string(),
        ));
    };

    let num_unindexed_fragments = ds.fragments().len() - num_indexed_fragments;
    let num_indexed_rows: usize = num_indexed_rows_per_delta.iter().sum();

    drop(indexed_fragments_per_delta);
    let total_rows = ds.count_rows(None).await?;
    let num_unindexed_rows = total_rows - num_indexed_rows;

    Ok(Some((
        num_indexed_rows_per_delta,
        num_indexed_fragments,
        num_unindexed_fragments,
        num_indexed_rows,
        num_unindexed_rows,
    )))
}

pub(crate) fn retain_supported_indices(indices: &mut Vec<IndexMetadata>) {
    indices.retain(|idx| {
        let max_supported_version = idx
            .index_details
            .as_ref()
            .map(|details| {
                IndexDetails(details.clone())
                    .index_version()
                    // If we don't know how to read the index, it isn't supported
                    .unwrap_or(i32::MAX as u32)
            })
            .unwrap_or_default();
        let is_valid = idx.index_version <= max_supported_version as i32;
        if !is_valid {
            log::warn!(
                "Index {} has version {}, which is not supported (<={}), ignoring it",
                idx.name,
                idx.index_version,
                max_supported_version,
            );
        }
        is_valid
    })
}

/// A trait for internal dataset utilities
///
/// Internal use only. No API stability guarantees.
#[async_trait]
pub trait DatasetIndexInternalExt: DatasetIndexExt {
    /// Opens an index (scalar or vector) as a generic index
    async fn open_generic_index(
        &self,
        column: &str,
        uuid: &Uuid,
        metrics: &dyn MetricsCollector,
    ) -> Result<Arc<dyn Index>>;
    /// Opens the requested scalar index
    async fn open_scalar_index(
        &self,
        column: &str,
        uuid: &Uuid,
        metrics: &dyn MetricsCollector,
    ) -> Result<Arc<dyn ScalarIndex>>;
    /// Opens the requested vector index
    async fn open_vector_index(
        &self,
        column: &str,
        uuid: &Uuid,
        metrics: &dyn MetricsCollector,
    ) -> Result<Arc<dyn VectorIndex>>;
    /// Opens all segments for one logical vector index and returns a materialized snapshot.
    async fn open_logical_vector_index(
        &self,
        column: &str,
        name: &str,
    ) -> Result<LogicalVectorIndex>;

    /// Opens the fragment reuse index
    async fn open_frag_reuse_index(
        &self,
        metrics: &dyn MetricsCollector,
    ) -> Result<Option<Arc<FragReuseIndex>>>;

    /// Opens the MemWAL index
    async fn open_mem_wal_index(
        &self,
        metrics: &dyn MetricsCollector,
    ) -> Result<Option<Arc<MemWalIndex>>>;

    /// Gets the fragment reuse index UUID from the current manifest, if it exists
    async fn frag_reuse_index_uuid(&self) -> Option<Uuid>;

    /// Loads information about all the available scalar indices on the dataset
    async fn scalar_index_info(&self) -> Result<ScalarIndexInfo>;

    /// Return the fragments that are not covered by any of the deltas of the index.
    async fn unindexed_fragments(&self, idx_name: &str) -> Result<Vec<Fragment>>;

    /// Return the fragments that are covered by each of the deltas of the index.
    async fn indexed_fragments(&self, idx_name: &str) -> Result<Vec<Vec<Fragment>>>;

    /// Initialize a specific index on this dataset based on an index from a source dataset.
    async fn initialize_index(&mut self, source_dataset: &Dataset, index_name: &str) -> Result<()>;

    /// Initialize all indices on this dataset based on indices from a source dataset.
    /// This will call `initialize_index` for each non-system index in the source dataset.
    async fn initialize_indices(&mut self, source_dataset: &Dataset) -> Result<()>;
}

#[async_trait]
impl DatasetIndexInternalExt for Dataset {
    async fn open_generic_index(
        &self,
        column: &str,
        uuid: &Uuid,
        metrics: &dyn MetricsCollector,
    ) -> Result<Arc<dyn Index>> {
        // Checking for cache existence is cheap so we just check the vector caches.
        // Scalar indices cache themselves inside `open_scalar_index` (the cache
        // key is a plugin detail), so there is no cheap scalar check here.
        let frag_reuse_uuid = self.frag_reuse_index_uuid().await;

        // Check sized cache for IvfIndexState (v2+ indices).
        let state_key = IvfIndexStateCacheKey::new(uuid, frag_reuse_uuid.as_ref());
        if self.index_cache.get_with_key(&state_key).await.is_some() {
            // Reconstruct via open_vector_index which will hit the same sized key.
            let index = self.open_vector_index(column, uuid, metrics).await?;
            return Ok(index.as_index());
        }

        // Fallback: in-memory cache for legacy indices.
        let vector_cache_key = LegacyVectorIndexCacheKey::new(uuid, frag_reuse_uuid.as_ref());
        if let Some(cached) = self.index_cache.get_with_key(&vector_cache_key).await {
            return Ok(cached.0.clone().as_index());
        }

        let frag_reuse_cache_key = FragReuseIndexCacheKey::new(uuid, frag_reuse_uuid.as_ref());
        if let Some(index) = self.index_cache.get_with_key(&frag_reuse_cache_key).await {
            return Ok(Arc::new(FragReuseIndexHandle(index)).as_index());
        }

        // Sometimes we want to open an index and we don't care if it is a scalar or vector index.
        // For example, we might want to get statistics for an index, regardless of type.
        //
        // We determine if this is a vector index by checking if INDEX_FILE_NAME exists in the
        // file list (available since file sizes tracking was added). If the file list is not
        // available (older indices), we fall back to checking file existence via HEAD request.
        let index_meta = self
            .load_index(uuid)
            .await?
            .ok_or_else(|| Error::index(format!("Index with id {} does not exist", uuid)))?;

        // Check if this is a vector index by looking at the files list
        let is_vector_index = if let Some(files) = &index_meta.files {
            // If we have file metadata, check if INDEX_FILE_NAME is in the list
            files.iter().any(|f| f.path == INDEX_FILE_NAME)
        } else {
            // Fall back to file existence check for older indices without file metadata
            let index_dir = self.indice_files_dir(&index_meta)?;
            let index_file = index_dir
                .clone()
                .join(uuid.to_string())
                .join(INDEX_FILE_NAME);
            let object_store = self.object_store_for_index(&index_meta).await?;
            object_store.exists(&index_file).await?
        };

        if is_vector_index {
            let index = self.open_vector_index(column, uuid, metrics).await?;
            Ok(index.as_index())
        } else {
            let index = self.open_scalar_index(column, uuid, metrics).await?;
            Ok(index.as_index())
        }
    }

    #[instrument(level = "debug", skip_all)]
    async fn open_scalar_index(
        &self,
        column: &str,
        uuid: &Uuid,
        metrics: &dyn MetricsCollector,
    ) -> Result<Arc<dyn ScalarIndex>> {
        // Caching (including the choice of in-memory vs. serializable state) is
        // a plugin implementation detail handled inside `scalar::open_scalar_index`.
        let index_meta = self
            .load_index(uuid)
            .await?
            .ok_or_else(|| Error::index(format!("Index with id {} does not exist", uuid)))?;

        scalar::open_scalar_index(self, column, &index_meta, metrics).await
    }

    async fn open_vector_index(
        &self,
        column: &str,
        uuid: &Uuid,
        metrics: &dyn MetricsCollector,
    ) -> Result<Arc<dyn VectorIndex>> {
        let frag_reuse_uuid = self.frag_reuse_index_uuid().await;
        let index_meta = self
            .load_index(uuid)
            .await?
            .ok_or_else(|| Error::index(format!("Index with id {} does not exist", uuid)))?;
        let object_store = self.object_store_for_index(&index_meta).await?;

        // Check sized cache first (v2+ indices with serializable state).
        let state_key = IvfIndexStateCacheKey::new(uuid, frag_reuse_uuid.as_ref());
        if let Some(entry) = self.index_cache.get_with_key(&state_key).await {
            log::debug!("Found IvfIndexState in cache uuid: {}", uuid);
            let partition_cache = self.index_cache.for_index(uuid, frag_reuse_uuid.as_ref());
            let frag_reuse_index = self.open_frag_reuse_index(metrics).await?;
            return entry
                .0
                .reconstruct(
                    object_store,
                    self.metadata_cache.as_ref(),
                    partition_cache,
                    frag_reuse_index,
                )
                .await;
        }

        // Fallback: in-memory cache for legacy indices.
        let cache_key = LegacyVectorIndexCacheKey::new(uuid, frag_reuse_uuid.as_ref());
        if let Some(cached) = self.index_cache.get_with_key(&cache_key).await {
            return Ok(cached.0.clone());
        }

        let frag_reuse_index = self.open_frag_reuse_index(metrics).await?;
        let index_dir = self.indice_files_dir(&index_meta)?;
        let index_file = index_dir
            .clone()
            .join(uuid.to_string())
            .join(INDEX_FILE_NAME);
        let file_sizes = index_meta.file_size_map();
        let reader: Arc<dyn Reader> = vector::open_index_file(
            object_store.as_ref(),
            &index_file,
            INDEX_FILE_NAME,
            &file_sizes,
        )
        .await?
        .into();

        let tailing_bytes = read_last_block(reader.as_ref()).await?;
        let (major_version, minor_version) = read_version(&tailing_bytes)?;

        // Namespace the index cache by the UUID of the index. v2+ partition
        // entries are store-free and remain reusable across object-store
        // generations alongside their serializable state.
        let index_cache = self.index_cache.for_index(uuid, frag_reuse_uuid.as_ref());

        // Extract the cacheable state before type-erasing to Arc<dyn VectorIndex>.
        fn wrap_ivf<S: IvfSubIndex + 'static, Q: Quantization + 'static>(
            ivf: IVFIndex<S, Q>,
        ) -> (Arc<dyn VectorIndex>, Option<IvfStateEntryBox>) {
            let entry = ivf.to_state_entry();
            (Arc::new(ivf), Some(entry))
        }

        // the index file is in lance format since version (0,2)
        // TODO: we need to change the legacy IVF_PQ to be in lance format
        let result: Result<(Arc<dyn VectorIndex>, Option<IvfStateEntryBox>)> = match (
            major_version,
            minor_version,
        ) {
            (0, 1) | (0, 0) => {
                info!(target: TRACE_IO_EVENTS, index_uuid=%uuid, r#type=IO_TYPE_OPEN_VECTOR, version="0.1", index_type="IVF_PQ");
                let proto = open_index_proto(reader.as_ref()).await?;
                match &proto.implementation {
                    Some(Implementation::VectorIndex(vector_index)) => {
                        let dataset = Arc::new(self.clone());
                        let idx = vector::open_vector_index(
                            dataset,
                            uuid,
                            vector_index,
                            reader,
                            frag_reuse_index,
                        )
                        .await?;
                        Ok((idx, None::<IvfStateEntryBox>))
                    }
                    None => Err(Error::internal(
                        "Index proto was missing implementation field",
                    )),
                }
            }

            (0, 2) => {
                info!(target: TRACE_IO_EVENTS, index_uuid=%uuid, r#type=IO_TYPE_OPEN_VECTOR, version="0.2", index_type="IVF_PQ");
                let reader = V1FileReader::try_new_self_described_from_reader(
                    reader.clone(),
                    Some(&self.metadata_cache.file_metadata_cache(&index_file)),
                )
                .await?;
                let idx = vector::open_vector_index_v2(
                    Arc::new(self.clone()),
                    column,
                    uuid,
                    reader,
                    frag_reuse_index,
                )
                .await?;
                Ok((idx, None::<IvfStateEntryBox>))
            }

            (0, 3) | (2, _) => {
                let scheduler = ScanScheduler::new(
                    object_store.clone(),
                    SchedulerConfig::max_bandwidth(&object_store),
                );
                let cached_size = file_sizes
                    .get(INDEX_FILE_NAME)
                    .map(|&size| CachedFileSize::new(size))
                    .unwrap_or_else(CachedFileSize::unknown);
                let file = scheduler.open_file(&index_file, &cached_size).await?;
                let reader = lance_file::reader::FileReader::try_open(
                    file,
                    None,
                    Default::default(),
                    &self.metadata_cache.file_metadata_cache(&index_file),
                    FileReaderOptions::default(),
                )
                .await?;
                let index_metadata = reader
                    .schema()
                    .metadata
                    .get(INDEX_METADATA_SCHEMA_KEY)
                    .ok_or(Error::index("Index Metadata not found".to_owned()))?;
                let index_metadata: lance_index::IndexMetadata =
                    serde_json::from_str(index_metadata)?;

                // Resolve the column name and field
                let (field_path, field) = resolve_index_column(self.schema(), &index_meta, column)?;

                let (_, element_type) = get_vector_type(self.schema(), &field_path)?;

                info!(target: TRACE_IO_EVENTS, index_uuid=%uuid, r#type=IO_TYPE_OPEN_VECTOR, version="0.3", index_type=index_metadata.index_type);

                match index_metadata.index_type.as_str() {
                    "IVF_FLAT" => match element_type {
                        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                            let ivf = IVFIndex::<FlatIndex, FlatQuantizer>::try_new(
                                object_store.clone(),
                                index_dir,
                                uuid.to_owned(),
                                frag_reuse_index,
                                self.metadata_cache.as_ref(),
                                index_cache,
                                file_sizes,
                            )
                            .await?;
                            Ok(wrap_ivf(ivf))
                        }
                        DataType::UInt8 => {
                            let ivf = IVFIndex::<FlatIndex, FlatBinQuantizer>::try_new(
                                object_store.clone(),
                                index_dir,
                                uuid.to_owned(),
                                frag_reuse_index,
                                self.metadata_cache.as_ref(),
                                index_cache,
                                file_sizes,
                            )
                            .await?;
                            Ok(wrap_ivf(ivf))
                        }
                        _ => Err(Error::index(format!(
                            "the field type {} is not supported for FLAT index",
                            field.data_type()
                        ))),
                    },

                    "IVF_PQ" => {
                        let ivf = IVFIndex::<FlatIndex, ProductQuantizer>::try_new(
                            object_store.clone(),
                            index_dir,
                            uuid.to_owned(),
                            frag_reuse_index,
                            self.metadata_cache.as_ref(),
                            index_cache,
                            file_sizes,
                        )
                        .await?;
                        Ok(wrap_ivf(ivf))
                    }

                    "IVF_SQ" => {
                        let ivf = IVFIndex::<FlatIndex, ScalarQuantizer>::try_new(
                            object_store.clone(),
                            index_dir,
                            uuid.to_owned(),
                            frag_reuse_index,
                            self.metadata_cache.as_ref(),
                            index_cache,
                            file_sizes,
                        )
                        .await?;
                        Ok(wrap_ivf(ivf))
                    }

                    "IVF_RQ" => {
                        let ivf = IVFIndex::<FlatIndex, RabitQuantizer>::try_new(
                            object_store.clone(),
                            index_dir,
                            uuid.to_owned(),
                            frag_reuse_index,
                            self.metadata_cache.as_ref(),
                            index_cache,
                            file_sizes,
                        )
                        .await?;
                        Ok(wrap_ivf(ivf))
                    }

                    "IVF_HNSW_FLAT" => match element_type {
                        DataType::UInt8 => {
                            let ivf = IVFIndex::<HNSW, FlatBinQuantizer>::try_new(
                                object_store.clone(),
                                index_dir,
                                uuid.to_owned(),
                                frag_reuse_index,
                                self.metadata_cache.as_ref(),
                                index_cache,
                                file_sizes,
                            )
                            .await?;
                            Ok(wrap_ivf(ivf))
                        }
                        _ => {
                            let ivf = IVFIndex::<HNSW, FlatQuantizer>::try_new(
                                object_store.clone(),
                                index_dir,
                                uuid.to_owned(),
                                frag_reuse_index,
                                self.metadata_cache.as_ref(),
                                index_cache,
                                file_sizes,
                            )
                            .await?;
                            Ok(wrap_ivf(ivf))
                        }
                    },

                    "IVF_HNSW_SQ" => {
                        let ivf = IVFIndex::<HNSW, ScalarQuantizer>::try_new(
                            object_store.clone(),
                            index_dir,
                            uuid.to_owned(),
                            frag_reuse_index,
                            self.metadata_cache.as_ref(),
                            index_cache,
                            file_sizes,
                        )
                        .await?;
                        Ok(wrap_ivf(ivf))
                    }

                    "IVF_HNSW_PQ" => {
                        let ivf = IVFIndex::<HNSW, ProductQuantizer>::try_new(
                            object_store.clone(),
                            index_dir,
                            uuid.to_owned(),
                            frag_reuse_index,
                            self.metadata_cache.as_ref(),
                            index_cache,
                            file_sizes,
                        )
                        .await?;
                        Ok(wrap_ivf(ivf))
                    }

                    _ => Err(Error::index(format!(
                        "Unsupported index type: {}",
                        index_metadata.index_type
                    ))),
                }
            }

            _ => Err(Error::index(
                "unsupported index version (maybe need to upgrade your lance version)".to_owned(),
            )),
        };
        let (index, ivf_entry) = result?;
        metrics.record_index_load();
        // Attribute the one-time index-open I/O (file footers, IVF centroids,
        // quantization metadata) to this query's metrics.  This runs only on a
        // real open; cache hits return earlier, so a warm query reports zero
        // index-open I/O.
        if let Some(io_stats) = metrics.io_stats()
            && let Some(open_stats) = index.open_io_stats()
        {
            io_stats.add_scan_stats(&open_stats);
        }
        if let Some(ivf_entry) = ivf_entry {
            self.index_cache
                .insert_with_key(&state_key, Arc::new(ivf_entry))
                .await;
        } else {
            self.index_cache
                .insert_with_key(&cache_key, Arc::new(CachedLegacyVectorIndex(index.clone())))
                .await;
        }
        Ok(index)
    }

    async fn open_logical_vector_index(
        &self,
        column: &str,
        name: &str,
    ) -> Result<LogicalVectorIndex> {
        let metadatas = self.load_indices_by_name(name).await?;
        if metadatas.is_empty() {
            return Err(Error::index_not_found(format!("name={name}")));
        }

        let field_id = self.schema().field_id(column)?;
        if let Some(invalid_metadata) = metadatas
            .iter()
            .find(|metadata| !metadata.fields.contains(&field_id))
        {
            return Err(Error::invalid_input(format!(
                "Logical vector index '{}' contains segment {} that does not belong to column '{}'",
                name, invalid_metadata.uuid, column
            )));
        }

        let mut segments = Vec::with_capacity(metadatas.len());
        for metadata in metadatas {
            let index = self
                .open_vector_index(column, &metadata.uuid, &NoOpMetricsCollector)
                .await?;
            segments.push((metadata, index));
        }

        LogicalVectorIndex::try_new(name.to_string(), column.to_string(), segments)
    }

    async fn open_frag_reuse_index(
        &self,
        metrics: &dyn MetricsCollector,
    ) -> Result<Option<Arc<FragReuseIndex>>> {
        if let Some(frag_reuse_index_meta) = self.load_index_by_name(FRAG_REUSE_INDEX_NAME).await? {
            let frag_reuse_uuid = frag_reuse_index_meta.uuid;
            let frag_reuse_key = FragReuseIndexKey {
                uuid: &frag_reuse_uuid,
            };

            let index = self
                .index_cache
                .get_or_insert_with_key(frag_reuse_key, || async move {
                    let index_meta =
                        self.load_index(&frag_reuse_uuid).await?.ok_or_else(|| {
                            Error::index(format!(
                                "Index with id {} does not exist",
                                frag_reuse_uuid
                            ))
                        })?;
                    let index_details = load_frag_reuse_index_details(self, &index_meta).await?;
                    let index =
                        open_frag_reuse_index(frag_reuse_index_meta.uuid, index_details.as_ref())
                            .await?;

                    info!(target: TRACE_IO_EVENTS, index_uuid=%frag_reuse_uuid, r#type=IO_TYPE_OPEN_FRAG_REUSE);
                    metrics.record_index_load();

                    Ok(index)
                })
                .await?;

            Ok(Some(index))
        } else {
            Ok(None)
        }
    }

    async fn open_mem_wal_index(
        &self,
        metrics: &dyn MetricsCollector,
    ) -> Result<Option<Arc<MemWalIndex>>> {
        let Some(mem_wal_meta) = self.load_index_by_name(MEM_WAL_INDEX_NAME).await? else {
            return Ok(None);
        };

        let frag_reuse_uuid = self.frag_reuse_index_uuid().await;
        let cache_key = MemWalCacheKey::new(&mem_wal_meta.uuid, frag_reuse_uuid.as_ref());
        if let Some(index) = self.index_cache.get_with_key(&cache_key).await {
            log::debug!("Found MemWAL index in cache uuid: {}", mem_wal_meta.uuid);
            return Ok(Some(index));
        }

        let uuid = mem_wal_meta.uuid;

        let index_meta = self
            .load_index(&uuid)
            .await?
            .ok_or_else(|| Error::index(format!("Index with id {} does not exist", uuid)))?;
        let index = open_mem_wal_index(index_meta)?;

        info!(target: TRACE_IO_EVENTS, index_uuid=%uuid, r#type=IO_TYPE_OPEN_MEM_WAL);
        metrics.record_index_load();

        self.index_cache
            .insert_with_key(&cache_key, index.clone())
            .await;
        Ok(Some(index))
    }

    async fn frag_reuse_index_uuid(&self) -> Option<Uuid> {
        if let Ok(indices) = self.load_indices().await {
            indices
                .iter()
                .find(|idx| idx.name == FRAG_REUSE_INDEX_NAME)
                .map(|idx| idx.uuid)
        } else {
            None
        }
    }

    #[instrument(level = "trace", skip_all)]
    async fn scalar_index_info(&self) -> Result<ScalarIndexInfo> {
        let indices = self.load_indices().await?;
        let schema = self.schema();
        let mut indexed_fields = Vec::new();
        // (column, index_name) → union of every contributing IndexMetadata's
        // fragment_bitmap. Multiple entries can land here for delta-merged
        // indices that share a name. We only insert when every contributing
        // entry has a bitmap; if any are missing, we leave the entry absent
        // so the optimizer treats coverage as unknown.
        let mut fragment_bitmaps: HashMap<(String, String), Option<RoaringBitmap>> = HashMap::new();
        for index in indices.iter().filter(|idx| {
            // Check if this is an FTS index by looking at index details
            let is_fts_index = if let Some(details) = &idx.index_details {
                IndexDetails(details.clone()).supports_fts()
            } else {
                false
            };

            // Only include indices with non-empty fragment bitmaps, except for FTS indices
            // which need to be discoverable even when empty
            let has_non_empty_bitmap = idx.fragment_bitmap.as_ref().is_some_and(|bitmap| {
                !bitmap.is_empty() && !(bitmap & self.fragment_bitmap.as_ref()).is_empty()
            });

            idx.fields.len() == 1 && (has_non_empty_bitmap || is_fts_index)
        }) {
            let field = index.fields[0];
            let field = schema.field_by_id(field).ok_or_else(|| {
                Error::internal(format!(
                    "Index referenced a field with id {field} which did not exist in the schema"
                ))
            })?;

            // Build the full field path for nested fields
            let field_path = if let Some(ancestors) = schema.field_ancestry_by_id(field.id) {
                let field_refs: Vec<&str> = ancestors.iter().map(|f| f.name.as_str()).collect();
                lance_core::datatypes::format_field_path(&field_refs)
            } else {
                field.name.clone()
            };

            let index_details = IndexDetails(fetch_index_details(self, &field_path, index).await?);
            if index_details.is_vector() {
                continue;
            }

            let plugin = match index_details.get_plugin() {
                Ok(plugin) => plugin,
                Err(e) => {
                    log::warn!(
                        "Skipping index '{}' on column '{}': {}. \
                         Queries on this column will fall back to a full scan.",
                        index.name,
                        field_path,
                        e
                    );
                    continue;
                }
            };
            let query_parser = plugin.new_query_parser(index.name.clone(), &index_details.0);

            if let Some(query_parser) = query_parser {
                // Union the per-segment fragment bitmap into this
                // (column, index_name) entry. If any segment is missing a
                // bitmap, downgrade the entry to None so callers know
                // coverage is partial/unknown.
                let key = (field_path.clone(), index.name.clone());
                fragment_bitmaps
                    .entry(key)
                    .and_modify(|entry| {
                        if let (Some(acc), Some(seg)) =
                            (entry.as_mut(), index.fragment_bitmap.as_ref())
                        {
                            *acc |= seg;
                        } else {
                            *entry = None;
                        }
                    })
                    .or_insert_with(|| index.fragment_bitmap.clone());
                indexed_fields.push((field_path, (field.data_type(), query_parser)));
            }
        }
        let mut index_info_map = HashMap::with_capacity(indexed_fields.len());
        for indexed_field in indexed_fields {
            // Need to wrap in an option here because we know that only one of and_modify and or_insert will be called
            // but the rust compiler does not.
            let mut parser = Some(indexed_field.1.1);
            let parser = &mut parser;
            index_info_map
                .entry(indexed_field.0)
                .and_modify(|existing: &mut (DataType, Box<MultiQueryParser>)| {
                    // If there are two indices on the same column, they must have the same type
                    debug_assert_eq!(existing.0, indexed_field.1.0);

                    existing.1.add(parser.take().unwrap());
                })
                .or_insert_with(|| {
                    (
                        indexed_field.1.0,
                        Box::new(MultiQueryParser::single(parser.take().unwrap())),
                    )
                });
        }
        // Drop entries we couldn't pin to a known bitmap.
        let fragment_bitmaps = fragment_bitmaps
            .into_iter()
            .filter_map(|(k, v)| v.map(|bm| (k, bm)))
            .collect();
        Ok(ScalarIndexInfo {
            indexed_columns: index_info_map,
            fragment_bitmaps,
        })
    }

    async fn unindexed_fragments(&self, name: &str) -> Result<Vec<Fragment>> {
        let indices = self.load_indices_by_name(name).await?;
        let mut total_fragment_bitmap = RoaringBitmap::new();
        for idx in indices.iter() {
            total_fragment_bitmap |= idx.fragment_bitmap.as_ref().ok_or(Error::index(
                "Please upgrade lance to 0.8+ to use this function".to_string(),
            ))?;
        }
        Ok(self
            .fragments()
            .iter()
            .filter(|f| !total_fragment_bitmap.contains(f.id as u32))
            .cloned()
            .collect())
    }

    async fn indexed_fragments(&self, name: &str) -> Result<Vec<Vec<Fragment>>> {
        let indices = self.load_indices_by_name(name).await?;
        indices
            .iter()
            .map(|index| {
                let fragment_bitmap = index.fragment_bitmap.as_ref().ok_or(Error::index(
                    "Please upgrade lance to 0.8+ to use this function".to_string(),
                ))?;
                let mut indexed_frags = Vec::with_capacity(fragment_bitmap.len() as usize);
                for frag in self.fragments().iter() {
                    if fragment_bitmap.contains(frag.id as u32) {
                        indexed_frags.push(frag.clone());
                    }
                }
                Ok(indexed_frags)
            })
            .collect()
    }

    async fn initialize_index(&mut self, source_dataset: &Dataset, index_name: &str) -> Result<()> {
        let source_indices = source_dataset.load_indices_by_name(index_name).await?;

        if source_indices.is_empty() {
            return Err(Error::index(format!(
                "Index '{}' not found in source dataset",
                index_name
            )));
        }

        let source_index = source_indices
            .iter()
            .min_by_key(|idx| idx.created_at)
            .ok_or_else(|| {
                Error::index(format!(
                    "Could not determine oldest index for '{}'",
                    index_name
                ))
            })?;

        let mut field_paths = Vec::with_capacity(source_index.fields.len());
        for field_id in source_index.fields.iter() {
            let source_field = source_dataset
                .schema()
                .field_by_id(*field_id)
                .ok_or_else(|| {
                    Error::index(format!(
                        "Field with id {} not found in source dataset",
                        field_id
                    ))
                })?;
            let source_field_path = source_dataset.schema().field_path(*field_id)?;

            let target_field = self.schema().field(&source_field_path).ok_or_else(|| {
                Error::index(format!(
                    "Field '{}' required by index '{}' not found in target dataset",
                    source_field_path, index_name
                ))
            })?;

            if source_field.data_type() != target_field.data_type() {
                return Err(Error::index(format!(
                    "Field '{}' has different types in source ({:?}) and target ({:?}) datasets",
                    source_field_path,
                    source_field.data_type(),
                    target_field.data_type()
                )));
            }

            field_paths.push(source_field_path);
        }

        if field_paths.is_empty() {
            return Err(Error::index(format!(
                "Index '{}' has no fields",
                index_name
            )));
        }

        let field_names = field_paths.iter().map(String::as_str).collect::<Vec<_>>();
        if let Some(index_details) = &source_index.index_details {
            let index_details_wrapper = IndexDetails(index_details.clone());

            if index_details_wrapper.is_vector() {
                vector::initialize_vector_index(self, source_dataset, source_index, &field_names)
                    .await?;
            } else {
                scalar::initialize_scalar_index(self, source_dataset, source_index, &field_names)
                    .await?;
            }
        } else {
            log::warn!(
                "Index '{}' has no index_details, skipping",
                source_index.name
            );
        }

        Ok(())
    }

    async fn initialize_indices(&mut self, source_dataset: &Dataset) -> Result<()> {
        let source_indices = source_dataset.load_indices().await?;
        let non_system_indices: Vec<_> = source_indices
            .iter()
            .filter(|idx| !lance_index::is_system_index(idx))
            .collect();

        if non_system_indices.is_empty() {
            return Ok(());
        }

        let mut unique_index_names = HashSet::new();
        for index in non_system_indices.iter() {
            unique_index_names.insert(index.name.clone());
        }

        for index_name in unique_index_names {
            self.initialize_index(source_dataset, &index_name).await?;
        }

        Ok(())
    }
}

/// Resolves the column name and field for an index operation.
///
/// This function handles the case where the caller passes an index name instead of a column name.
/// It returns the full field path and the field reference.
fn resolve_index_column(
    schema: &LanceSchema,
    index_meta: &IndexMetadata,
    column_arg: &str,
) -> Result<(String, Arc<Field>)> {
    // First, try to find the column directly in the schema
    if let Some(field) = schema.field(column_arg) {
        // Column exists in schema, use it
        return Ok((column_arg.to_string(), Arc::new(field.clone())));
    }

    // Column doesn't exist in schema, check if it's the index name
    if column_arg == index_meta.name {
        // Get the actual column from index metadata
        if let Some(field_id) = index_meta.fields.first() {
            let field = schema.field_by_id(*field_id).ok_or_else(|| {
                Error::index(format!(
                    "Index '{}' references field with id {} which does not exist in schema",
                    index_meta.name, field_id
                ))
            })?;
            let field_path = schema.field_path(*field_id)?;
            return Ok((field_path, Arc::new(field.clone())));
        } else {
            return Err(Error::index(format!(
                "Index '{}' has no fields",
                index_meta.name
            )));
        }
    }

    // Column doesn't exist and is not the index name
    Err(Error::index(format!(
        "Column '{}' does not exist in the schema",
        column_arg
    )))
}

fn is_vector_field(data_type: DataType) -> bool {
    match data_type {
        DataType::FixedSizeList(_, _) => true,
        DataType::List(inner) => {
            // If the inner type is a fixed size list, then it is a multivector field
            matches!(inner.data_type(), DataType::FixedSizeList(_, _))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::builder::DatasetBuilder;
    use crate::dataset::optimize::{CompactionOptions, compact_files};
    use crate::dataset::{WriteMode, WriteParams};
    use crate::index::vector::VectorIndexParams;
    use crate::session::Session;
    use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount, copy_test_data_to_tmp};
    use arrow::array::AsArray;
    use arrow::datatypes::{Float32Type, Int32Type, Int64Type};
    use arrow_array::Int32Array;
    use arrow_array::{
        FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator, StringArray,
    };
    use arrow_schema::{DataType, Field, Schema};
    use futures::stream::TryStreamExt;
    use lance_arrow::*;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_datagen::gen_batch;
    use lance_datagen::{BatchCount, ByteCount, Dimension, RowCount, array};
    use lance_index::pbold::{BTreeIndexDetails, InvertedIndexDetails};
    use lance_index::scalar::bitmap::BITMAP_LOOKUP_NAME;
    use lance_index::scalar::inverted::query::{FtsQuery, PhraseQuery};
    use lance_index::scalar::inverted::{
        INVERTED_INDEX_VERSION_V1, INVERTED_INDEX_VERSION_V2, INVERTED_INDEX_VERSION_V3,
    };
    use lance_index::scalar::{
        BuiltinIndexType, FullTextSearchQuery, InvertedIndexParams, ScalarIndexParams,
    };
    use lance_index::vector::{
        hnsw::builder::HnswBuildParams,
        ivf::IvfBuildParams,
        kmeans::{KMeansParams, train_kmeans},
        sq::builder::SQBuildParams,
    };
    use lance_io::{assert_io_eq, assert_io_lt, utils::tracking_store::IoStats};
    use lance_linalg::distance::{DistanceType, MetricType};
    use lance_testing::datagen::generate_random_array;
    use object_store::ObjectStoreExt;
    use rstest::rstest;
    use std::collections::{HashMap, HashSet};

    async fn write_vector_segment_metadata(
        dataset: &Dataset,
        index_name: &str,
        field_id: i32,
        uuid: Uuid,
        fragment_bitmap: impl IntoIterator<Item = u32>,
        payload: &[u8],
    ) -> IndexMetadata {
        let index_path = dataset
            .indices_dir()
            .join(uuid.to_string())
            .join(INDEX_FILE_NAME);
        dataset
            .object_store
            .as_ref()
            .put(&index_path, payload)
            .await
            .unwrap();
        IndexMetadata {
            uuid,
            name: index_name.to_string(),
            fields: vec![field_id],
            dataset_version: dataset.manifest.version,
            fragment_bitmap: Some(fragment_bitmap.into_iter().collect()),
            index_details: Some(Arc::new(vector_index_details_default())),
            index_version: IndexType::Vector.version(),
            created_at: Some(chrono::Utc::now()),
            base_id: None,
            files: Some(vec![lance_table::format::IndexFile {
                path: INDEX_FILE_NAME.to_string(),
                size_bytes: payload.len() as u64,
            }]),
        }
    }

    fn list_io_stats(stats: &IoStats) -> IoStats {
        let requests = stats
            .requests
            .iter()
            .filter(|request| request.method == "list")
            .cloned()
            .collect::<Vec<_>>();
        IoStats {
            read_iops: requests.len() as u64,
            requests,
            ..Default::default()
        }
    }

    fn segment_from_metadata(metadata: &IndexMetadata) -> IndexSegment {
        metadata
            .clone()
            .into_index_segment()
            .expect("test segment metadata should convert to an index segment")
    }

    async fn write_fragmented_vector_dataset(uri: &str, dimension: i32) -> Dataset {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dimension,
                ),
                false,
            ),
        ]));
        let batches = (0..5)
            .map(|i| {
                let vector_values: Float32Array = (0..dimension * 80)
                    .map(|value| value as f32 + (i * 1000) as f32)
                    .collect();
                let vectors =
                    FixedSizeListArray::try_new_from_values(vector_values, dimension).unwrap();
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int32Array::from_iter_values(i * 80..(i + 1) * 80)),
                        Arc::new(vectors),
                    ],
                )
            })
            .collect::<std::result::Result<Vec<_>, arrow_schema::ArrowError>>()
            .unwrap();
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        Dataset::write(
            reader,
            uri,
            Some(WriteParams {
                max_rows_per_group: 10,
                max_rows_per_file: 80,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
    }

    async fn write_skewed_fragmented_vector_dataset(uri: &str, dimension: i32) -> Dataset {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dimension,
                ),
                false,
            ),
        ]));
        let first_fragment_rows = 20_000;
        let second_fragment_rows = 100;
        let first_values = vec![0.0f32; first_fragment_rows * dimension as usize];
        let second_values = vec![100.0f32; second_fragment_rows * dimension as usize];
        let first_vectors =
            FixedSizeListArray::try_new_from_values(Float32Array::from(first_values), dimension)
                .unwrap();
        let second_vectors =
            FixedSizeListArray::try_new_from_values(Float32Array::from(second_values), dimension)
                .unwrap();
        let batches = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..first_fragment_rows as i32)),
                    Arc::new(first_vectors),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(
                        first_fragment_rows as i32
                            ..(first_fragment_rows + second_fragment_rows) as i32,
                    )),
                    Arc::new(second_vectors),
                ],
            )
            .unwrap(),
        ];
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        Dataset::write(
            reader,
            uri,
            Some(WriteParams {
                max_rows_per_group: 1024,
                max_rows_per_file: first_fragment_rows,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
    }

    async fn create_segmented_vector_index(
        dataset: &mut Dataset,
        index_name: &str,
        column: &str,
        dimension: i32,
    ) -> Vec<Uuid> {
        let batch = dataset
            .scan()
            .project(&[column])
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let vectors = batch
            .column_by_name(column)
            .expect("vector column should exist")
            .as_fixed_size_list();
        let values = vectors.values().as_primitive::<Float32Type>();
        let centroids = train_kmeans::<Float32Type>(
            values,
            KMeansParams::new(None, 10, 1, DistanceType::L2),
            dimension as usize,
            2,
            2,
        )
        .unwrap()
        .centroids
        .as_primitive::<Float32Type>()
        .clone();
        let centroids =
            Arc::new(FixedSizeListArray::try_new_from_values(centroids, dimension).unwrap());
        let params = VectorIndexParams::with_ivf_flat_params(
            DistanceType::L2,
            IvfBuildParams::try_with_centroids(2, centroids).unwrap(),
        );
        let fragment_ids = dataset
            .get_fragments()
            .iter()
            .map(|fragment| fragment.id() as u32)
            .collect::<Vec<_>>();
        let columns = [column];

        let mut segments = Vec::with_capacity(fragment_ids.len());
        for fragment_id in fragment_ids {
            let mut builder = dataset.create_index_builder(&columns, IndexType::Vector, &params);
            builder = builder
                .name(index_name.to_string())
                .fragments(vec![fragment_id]);
            segments.push(builder.execute_uncommitted().await.unwrap());
        }

        let segment_ids = segments
            .iter()
            .map(|segment| segment.uuid)
            .collect::<Vec<_>>();
        dataset
            .commit_existing_index_segments(index_name, column, segments)
            .await
            .unwrap();
        segment_ids
    }

    #[tokio::test]
    async fn test_open_logical_vector_index_single_segment_quality_apis() {
        const DIMENSION: i32 = 8;

        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();
        let mut dataset = write_fragmented_vector_dataset(test_uri, DIMENSION).await;
        let params =
            VectorIndexParams::with_ivf_flat_params(DistanceType::L2, IvfBuildParams::new(2));

        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("vector_idx".to_string()),
                &params,
                true,
            )
            .await
            .unwrap();

        let logical_index = dataset
            .open_logical_vector_index("vector", "vector_idx")
            .await
            .unwrap();

        assert_eq!(logical_index.name(), "vector_idx");
        assert_eq!(logical_index.column(), "vector");
        assert_eq!(logical_index.num_segments(), 1);
        assert_eq!(logical_index.metadatas().len(), 1);

        let rows_per_segment = logical_index.num_rows_per_segment();
        assert_eq!(rows_per_segment.len(), 1);
        assert_eq!(rows_per_segment[0].1, 400);

        let ivf_view = logical_index.as_ivf().unwrap();
        let partitions_per_segment = ivf_view.num_partitions_per_segment();
        assert_eq!(partitions_per_segment, vec![(rows_per_segment[0].0, 2)]);

        let partition_sizes = ivf_view.partition_sizes();
        assert_eq!(partition_sizes.len(), 1);
        assert_eq!(partition_sizes[0].1.len(), 2);
        assert_eq!(
            partition_sizes[0].1.iter().sum::<usize>(),
            rows_per_segment[0].1 as usize
        );
    }

    #[tokio::test]
    async fn test_open_logical_vector_index_segmented_quality_apis() {
        const DIMENSION: i32 = 8;

        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();
        let mut dataset = write_fragmented_vector_dataset(test_uri, DIMENSION).await;
        let segment_ids =
            create_segmented_vector_index(&mut dataset, "vector_idx", "vector", DIMENSION).await;

        let logical_index = dataset
            .open_logical_vector_index("vector", "vector_idx")
            .await
            .unwrap();

        assert_eq!(logical_index.name(), "vector_idx");
        assert_eq!(logical_index.column(), "vector");
        assert_eq!(logical_index.num_segments(), segment_ids.len());

        let metadata_ids = logical_index
            .metadatas()
            .map(|metadata| metadata.uuid)
            .collect::<HashSet<_>>();
        assert_eq!(
            metadata_ids,
            segment_ids.into_iter().collect::<HashSet<_>>()
        );

        let rows_per_segment = logical_index.num_rows_per_segment();
        assert_eq!(rows_per_segment.len(), logical_index.num_segments());
        assert_eq!(
            rows_per_segment
                .iter()
                .map(|(_, num_rows)| *num_rows)
                .sum::<u64>(),
            400
        );
        assert!(
            rows_per_segment.iter().all(|(_, num_rows)| *num_rows > 0),
            "each segment should contain indexed rows"
        );

        let ivf_view = logical_index.as_ivf().unwrap();
        let partitions_per_segment = ivf_view.num_partitions_per_segment();
        assert!(
            partitions_per_segment
                .iter()
                .all(|(_, num_partitions)| *num_partitions == 2)
        );

        let row_count_by_segment = rows_per_segment.into_iter().collect::<HashMap<_, _>>();
        let partition_sizes = ivf_view.partition_sizes();
        assert_eq!(partition_sizes.len(), logical_index.num_segments());
        for (segment_id, sizes) in partition_sizes {
            assert_eq!(sizes.len(), 2);
            assert_eq!(
                sizes.iter().sum::<usize>(),
                row_count_by_segment[&segment_id] as usize
            );
        }
    }

    #[tokio::test]
    async fn test_open_logical_vector_index_rejects_wrong_column() {
        const DIMENSION: i32 = 8;

        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();
        let mut dataset = write_fragmented_vector_dataset(test_uri, DIMENSION).await;
        let params =
            VectorIndexParams::with_ivf_flat_params(DistanceType::L2, IvfBuildParams::new(2));

        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("vector_idx".to_string()),
                &params,
                true,
            )
            .await
            .unwrap();

        let err = dataset
            .open_logical_vector_index("id", "vector_idx")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("does not belong to column 'id'"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_segmented_optimize_rebalances_only_one_segment() {
        const DIMENSION: i32 = 8;

        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();
        let mut dataset = write_skewed_fragmented_vector_dataset(test_uri, DIMENSION).await;
        create_segmented_vector_index(&mut dataset, "vector_idx", "vector", DIMENSION).await;

        let before_segments = dataset.load_indices_by_name("vector_idx").await.unwrap();
        assert_eq!(before_segments.len(), 2);
        let before_by_fragment = before_segments
            .iter()
            .map(|metadata| {
                let fragments = metadata
                    .fragment_bitmap
                    .as_ref()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>();
                (fragments, metadata.uuid)
            })
            .collect::<HashMap<_, _>>();
        assert_eq!(before_by_fragment.len(), 2);

        dataset
            .optimize_indices(&OptimizeOptions::default())
            .await
            .unwrap();

        let after_segments = dataset.load_indices_by_name("vector_idx").await.unwrap();
        assert_eq!(after_segments.len(), 2);
        let after_by_fragment = after_segments
            .iter()
            .map(|metadata| {
                let fragments = metadata
                    .fragment_bitmap
                    .as_ref()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>();
                (fragments, metadata.uuid)
            })
            .collect::<HashMap<_, _>>();
        assert_eq!(after_by_fragment.len(), 2);

        assert_ne!(
            before_by_fragment[&vec![0]],
            after_by_fragment[&vec![0]],
            "expected optimize to replace the oversized segment"
        );
        assert_eq!(
            before_by_fragment[&vec![1]],
            after_by_fragment[&vec![1]],
            "expected optimize to leave the smaller segment untouched"
        );

        let logical_index = dataset
            .open_logical_vector_index("vector", "vector_idx")
            .await
            .unwrap();
        let partitions_per_segment = logical_index
            .as_ivf()
            .unwrap()
            .num_partitions_per_segment()
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(partitions_per_segment[&after_by_fragment[&vec![0]]], 3);
        assert_eq!(partitions_per_segment[&after_by_fragment[&vec![1]]], 2);
    }

    #[tokio::test]
    async fn test_recreate_index() {
        const DIM: i32 = 8;
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "v",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), DIM),
                true,
            ),
            Field::new(
                "o",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), DIM),
                true,
            ),
        ]));
        let data = generate_random_array(2048 * DIM as usize);
        let batches: Vec<RecordBatch> = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(FixedSizeListArray::try_new_from_values(data.clone(), DIM).unwrap()),
                    Arc::new(FixedSizeListArray::try_new_from_values(data, DIM).unwrap()),
                ],
            )
            .unwrap(),
        ];

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        let params = VectorIndexParams::ivf_pq(2, 8, 2, MetricType::L2, 2);
        dataset
            .create_index(&["v"], IndexType::Vector, None, &params, true)
            .await
            .unwrap();
        dataset
            .create_index(&["o"], IndexType::Vector, None, &params, true)
            .await
            .unwrap();

        // Create index again
        dataset
            .create_index(&["v"], IndexType::Vector, None, &params, true)
            .await
            .unwrap();

        // Can not overwrite an index on different columns.
        assert!(
            dataset
                .create_index(
                    &["v"],
                    IndexType::Vector,
                    Some("o_idx".to_string()),
                    &params,
                    true,
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_bitmap_index_statistics_minimal_io_via_dataset() {
        const NUM_ROWS: usize = 500_000;
        let test_dir = TempStrDir::default();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Int32,
            false,
        )]));
        let values: Vec<i32> = (0..NUM_ROWS as i32).collect();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))]).unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let mut dataset = Dataset::write(reader, &test_dir, None).await.unwrap();
        let io_tracker = dataset.object_store.as_ref().io_tracker().clone();

        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::Bitmap);
        dataset
            .create_index(
                &["status"],
                IndexType::Bitmap,
                Some("status_idx".to_string()),
                &params,
                true,
            )
            .await
            .unwrap();

        let indices = dataset.load_indices().await.unwrap();
        let index_meta = indices
            .iter()
            .find(|idx| idx.name == "status_idx")
            .expect("status_idx should exist");
        let lookup_path = dataset
            .indice_files_dir(index_meta)
            .unwrap()
            .join(index_meta.uuid.to_string())
            .join(BITMAP_LOOKUP_NAME);
        let meta = dataset.object_store.inner.head(&lookup_path).await.unwrap();
        assert!(
            meta.size >= 1_000_000,
            "bitmap index should be large enough to fail without metadata path, size={} bytes",
            meta.size
        );

        // Reset stats collected during index creation
        io_tracker.incremental_stats();

        dataset.index_statistics("status_idx").await.unwrap();

        let stats = io_tracker.incremental_stats();
        assert_io_eq!(
            stats,
            read_bytes,
            4096,
            "index_statistics should only read the index footer; got {} bytes",
            stats.read_bytes
        );
        assert_io_lt!(
            stats,
            read_iops,
            3,
            "index_statistics should only require a head plus one range read; got {} ops",
            stats.read_iops
        );
        assert_io_eq!(
            stats,
            written_bytes,
            0,
            "index_statistics should not perform writes"
        );
    }

    fn sample_vector_field() -> Field {
        let dimensions = 16;
        let column_name = "vec";
        Field::new(
            column_name,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimensions,
            ),
            false,
        )
    }

    #[tokio::test]
    async fn test_drop_index() {
        let test_dir = TempStrDir::default();
        let schema = Schema::new(vec![
            sample_vector_field(),
            Field::new("ints", DataType::Int32, false),
        ]);
        let mut dataset = lance_datagen::rand(&schema)
            .into_dataset(
                &test_dir,
                FragmentCount::from(1),
                FragmentRowCount::from(256),
            )
            .await
            .unwrap();

        let idx_name = "name".to_string();
        dataset
            .create_index(
                &["vec"],
                IndexType::Vector,
                Some(idx_name.clone()),
                &VectorIndexParams::ivf_pq(2, 8, 2, MetricType::L2, 10),
                true,
            )
            .await
            .unwrap();
        dataset
            .create_index(
                &["ints"],
                IndexType::BTree,
                None,
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();

        assert_eq!(dataset.load_indices().await.unwrap().len(), 2);

        dataset.drop_index(&idx_name).await.unwrap();

        assert_eq!(dataset.load_indices().await.unwrap().len(), 1);

        // Even though we didn't give the scalar index a name it still has an auto-generated one we can use
        let scalar_idx_name = &dataset.load_indices().await.unwrap()[0].name;
        dataset.drop_index(scalar_idx_name).await.unwrap();

        assert_eq!(dataset.load_indices().await.unwrap().len(), 0);

        // Make sure it returns an error if the index doesn't exist
        assert!(dataset.drop_index(scalar_idx_name).await.is_err());
    }

    #[tokio::test]
    async fn test_count_index_rows() {
        let test_dir = TempStrDir::default();
        let dimensions = 16;
        let column_name = "vec";
        let field = sample_vector_field();
        let schema = Arc::new(Schema::new(vec![field]));

        let float_arr = generate_random_array(512 * dimensions as usize);

        let vectors =
            arrow_array::FixedSizeListArray::try_new_from_values(float_arr, dimensions).unwrap();

        let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vectors)]).unwrap();

        let reader =
            RecordBatchIterator::new(vec![record_batch].into_iter().map(Ok), schema.clone());
        let test_uri = &test_dir;
        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();
        dataset.validate().await.unwrap();

        // Make sure it returns None if there's no index with the passed identifier
        assert!(dataset.index_statistics("bad_id").await.is_err());
        // Create an index
        let params = VectorIndexParams::ivf_pq(10, 8, 2, MetricType::L2, 10);
        dataset
            .create_index(
                &[column_name],
                IndexType::Vector,
                Some("vec_idx".into()),
                &params,
                true,
            )
            .await
            .unwrap();

        let stats: serde_json::Value =
            serde_json::from_str(&dataset.index_statistics("vec_idx").await.unwrap()).unwrap();
        assert_eq!(stats["num_unindexed_rows"], 0);
        assert_eq!(stats["num_indexed_rows"], 512);

        // Now we'll append some rows which shouldn't be indexed and see the
        // count change
        let float_arr = generate_random_array(512 * dimensions as usize);
        let vectors =
            arrow_array::FixedSizeListArray::try_new_from_values(float_arr, dimensions).unwrap();

        let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vectors)]).unwrap();

        let reader = RecordBatchIterator::new(vec![record_batch].into_iter().map(Ok), schema);
        dataset.append(reader, None).await.unwrap();

        let stats: serde_json::Value =
            serde_json::from_str(&dataset.index_statistics("vec_idx").await.unwrap()).unwrap();
        assert_eq!(stats["num_unindexed_rows"], 512);
        assert_eq!(stats["num_indexed_rows"], 512);
    }

    #[tokio::test]
    async fn test_optimize_delta_indices() {
        let dimensions = 16;
        let column_name = "vec";
        let vec_field = Field::new(
            column_name,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimensions,
            ),
            false,
        );
        let other_column_name = "other_vec";
        let other_vec_field = Field::new(
            other_column_name,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimensions,
            ),
            false,
        );
        let schema = Arc::new(Schema::new(vec![vec_field, other_vec_field]));

        let float_arr = generate_random_array(512 * dimensions as usize);

        let vectors = Arc::new(
            arrow_array::FixedSizeListArray::try_new_from_values(float_arr, dimensions).unwrap(),
        );

        let record_batch =
            RecordBatch::try_new(schema.clone(), vec![vectors.clone(), vectors.clone()]).unwrap();

        let reader = RecordBatchIterator::new(
            vec![record_batch.clone()].into_iter().map(Ok),
            schema.clone(),
        );

        let mut dataset = Dataset::write(reader, "memory://", None).await.unwrap();
        let params = VectorIndexParams::ivf_pq(10, 8, 2, MetricType::L2, 10);
        dataset
            .create_index(
                &[column_name],
                IndexType::Vector,
                Some("vec_idx".into()),
                &params,
                true,
            )
            .await
            .unwrap();
        dataset
            .create_index(
                &[other_column_name],
                IndexType::Vector,
                Some("other_vec_idx".into()),
                &params,
                true,
            )
            .await
            .unwrap();

        async fn get_stats(dataset: &Dataset, name: &str) -> serde_json::Value {
            serde_json::from_str(&dataset.index_statistics(name).await.unwrap()).unwrap()
        }
        async fn get_meta(dataset: &Dataset, name: &str) -> Vec<IndexMetadata> {
            dataset
                .load_indices()
                .await
                .unwrap()
                .iter()
                .filter(|m| m.name == name)
                .cloned()
                .collect()
        }
        fn get_bitmap(meta: &IndexMetadata) -> Vec<u32> {
            meta.fragment_bitmap.as_ref().unwrap().iter().collect()
        }
        fn assert_segment_aliases(stats: &serde_json::Value) {
            assert_eq!(stats["num_segments"], stats["num_indices"]);
            assert_eq!(stats["segments"], stats["indices"]);
        }

        let stats = get_stats(&dataset, "vec_idx").await;
        assert_segment_aliases(&stats);
        assert_eq!(stats["num_unindexed_rows"], 0);
        assert_eq!(stats["num_indexed_rows"], 512);
        assert_eq!(stats["num_indexed_fragments"], 1);
        assert_eq!(stats["num_indices"], 1);
        let meta = get_meta(&dataset, "vec_idx").await;
        assert_eq!(meta.len(), 1);
        assert_eq!(get_bitmap(&meta[0]), vec![0]);

        let reader =
            RecordBatchIterator::new(vec![record_batch].into_iter().map(Ok), schema.clone());
        dataset.append(reader, None).await.unwrap();
        let stats = get_stats(&dataset, "vec_idx").await;
        assert_segment_aliases(&stats);
        assert_eq!(stats["num_unindexed_rows"], 512);
        assert_eq!(stats["num_indexed_rows"], 512);
        assert_eq!(stats["num_indexed_fragments"], 1);
        assert_eq!(stats["num_unindexed_fragments"], 1);
        assert_eq!(stats["num_indices"], 1);
        let meta = get_meta(&dataset, "vec_idx").await;
        assert_eq!(meta.len(), 1);
        assert_eq!(get_bitmap(&meta[0]), vec![0]);

        dataset
            .optimize_indices(&OptimizeOptions::append().index_names(vec![])) // Does nothing because no index name is passed
            .await
            .unwrap();
        let stats = get_stats(&dataset, "vec_idx").await;
        assert_segment_aliases(&stats);
        assert_eq!(stats["num_unindexed_rows"], 512);
        assert_eq!(stats["num_indexed_rows"], 512);
        assert_eq!(stats["num_indexed_fragments"], 1);
        assert_eq!(stats["num_unindexed_fragments"], 1);
        assert_eq!(stats["num_indices"], 1);
        let meta = get_meta(&dataset, "vec_idx").await;
        assert_eq!(meta.len(), 1);
        assert_eq!(get_bitmap(&meta[0]), vec![0]);

        // optimize the other index
        dataset
            .optimize_indices(
                &OptimizeOptions::append().index_names(vec!["other_vec_idx".to_owned()]),
            )
            .await
            .unwrap();
        let stats = get_stats(&dataset, "vec_idx").await;
        assert_segment_aliases(&stats);
        assert_eq!(stats["num_unindexed_rows"], 512);
        assert_eq!(stats["num_indexed_rows"], 512);
        assert_eq!(stats["num_indexed_fragments"], 1);
        assert_eq!(stats["num_unindexed_fragments"], 1);
        assert_eq!(stats["num_indices"], 1);
        let meta = get_meta(&dataset, "vec_idx").await;
        assert_eq!(meta.len(), 1);
        assert_eq!(get_bitmap(&meta[0]), vec![0]);

        let stats = get_stats(&dataset, "other_vec_idx").await;
        assert_segment_aliases(&stats);
        assert_eq!(stats["num_unindexed_rows"], 0);
        assert_eq!(stats["num_indexed_rows"], 1024);
        assert_eq!(stats["num_indexed_fragments"], 2);
        assert_eq!(stats["num_unindexed_fragments"], 0);
        assert_eq!(stats["num_indices"], 2);
        let meta = get_meta(&dataset, "other_vec_idx").await;
        assert_eq!(meta.len(), 2);
        assert_eq!(get_bitmap(&meta[0]), vec![0]);
        assert_eq!(get_bitmap(&meta[1]), vec![1]);

        dataset
            .optimize_indices(&OptimizeOptions::retrain())
            .await
            .unwrap();

        let stats = get_stats(&dataset, "vec_idx").await;
        assert_segment_aliases(&stats);
        assert_eq!(stats["num_unindexed_rows"], 0);
        assert_eq!(stats["num_indexed_rows"], 1024);
        assert_eq!(stats["num_indexed_fragments"], 2);
        assert_eq!(stats["num_unindexed_fragments"], 0);
        assert_eq!(stats["num_indices"], 1);
        let meta = get_meta(&dataset, "vec_idx").await;
        assert_eq!(meta.len(), 1);
        assert_eq!(get_bitmap(&meta[0]), vec![0, 1]);

        dataset
            .optimize_indices(&OptimizeOptions::retrain())
            .await
            .unwrap();
        let stats = get_stats(&dataset, "other_vec_idx").await;
        assert_segment_aliases(&stats);
        assert_eq!(stats["num_unindexed_rows"], 0);
        assert_eq!(stats["num_indexed_rows"], 1024);
        assert_eq!(stats["num_indexed_fragments"], 2);
        assert_eq!(stats["num_unindexed_fragments"], 0);
        assert_eq!(stats["num_indices"], 1);
        let meta = get_meta(&dataset, "other_vec_idx").await;
        assert_eq!(meta.len(), 1);
        assert_eq!(get_bitmap(&meta[0]), vec![0, 1]);
    }

    #[tokio::test]
    async fn test_optimize_ivf_hnsw_sq_delta_indices() {
        let test_dir = TempStrDir::default();
        let dimensions = 16;
        let column_name = "vec";
        let field = Field::new(
            column_name,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimensions,
            ),
            false,
        );
        let schema = Arc::new(Schema::new(vec![field]));

        let float_arr = generate_random_array(512 * dimensions as usize);

        let vectors =
            arrow_array::FixedSizeListArray::try_new_from_values(float_arr, dimensions).unwrap();

        let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vectors)]).unwrap();

        let reader = RecordBatchIterator::new(
            vec![record_batch.clone()].into_iter().map(Ok),
            schema.clone(),
        );

        let test_uri = &test_dir;
        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        let ivf_params = IvfBuildParams::default();
        let hnsw_params = HnswBuildParams::default();
        let sq_params = SQBuildParams::default();
        let params = VectorIndexParams::with_ivf_hnsw_sq_params(
            MetricType::L2,
            ivf_params,
            hnsw_params,
            sq_params,
        );
        dataset
            .create_index(
                &[column_name],
                IndexType::Vector,
                Some("vec_idx".into()),
                &params,
                true,
            )
            .await
            .unwrap();

        let stats: serde_json::Value =
            serde_json::from_str(&dataset.index_statistics("vec_idx").await.unwrap()).unwrap();
        assert_eq!(stats["num_unindexed_rows"], 0);
        assert_eq!(stats["num_indexed_rows"], 512);
        assert_eq!(stats["num_indexed_fragments"], 1);
        assert_eq!(stats["num_indices"], 1);

        let reader =
            RecordBatchIterator::new(vec![record_batch].into_iter().map(Ok), schema.clone());
        dataset.append(reader, None).await.unwrap();
        let mut dataset = DatasetBuilder::from_uri(test_uri).load().await.unwrap();
        let stats: serde_json::Value =
            serde_json::from_str(&dataset.index_statistics("vec_idx").await.unwrap()).unwrap();
        assert_eq!(stats["num_unindexed_rows"], 512);
        assert_eq!(stats["num_indexed_rows"], 512);
        assert_eq!(stats["num_indexed_fragments"], 1);
        assert_eq!(stats["num_unindexed_fragments"], 1);
        assert_eq!(stats["num_indices"], 1);

        dataset
            .optimize_indices(&OptimizeOptions::append())
            .await
            .unwrap();

        let stats: serde_json::Value =
            serde_json::from_str(&dataset.index_statistics("vec_idx").await.unwrap()).unwrap();
        assert_eq!(stats["num_unindexed_rows"], 0);
        assert_eq!(stats["num_indexed_rows"], 1024);
        assert_eq!(stats["num_indexed_fragments"], 2);
        assert_eq!(stats["num_unindexed_fragments"], 0);
        assert_eq!(stats["num_indices"], 2);

        dataset
            .optimize_indices(&OptimizeOptions::retrain())
            .await
            .unwrap();
        let stats: serde_json::Value =
            serde_json::from_str(&dataset.index_statistics("vec_idx").await.unwrap()).unwrap();
        assert_eq!(stats["num_unindexed_rows"], 0);
        assert_eq!(stats["num_indexed_rows"], 1024);
        assert_eq!(stats["num_indexed_fragments"], 2);
        assert_eq!(stats["num_unindexed_fragments"], 0);
        assert_eq!(stats["num_indices"], 1);
    }

    #[rstest]
    #[case::v1("v3.0.1/fts_v1", INVERTED_INDEX_VERSION_V1)]
    #[case::v2("v4.0.1/fts_v2", INVERTED_INDEX_VERSION_V2)]
    #[tokio::test]
    async fn test_read_fts_format_fixture(
        #[case] fixture_path: &str,
        #[case] expected_version: u32,
    ) {
        async fn search_ids(dataset: &Dataset, query: FullTextSearchQuery) -> Vec<i64> {
            let result = dataset
                .scan()
                .project(&["id"])
                .unwrap()
                .full_text_search(query)
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();
            let mut ids = result["id"].as_primitive::<Int64Type>().values().to_vec();
            ids.sort_unstable();
            ids
        }

        let test_dir = copy_test_data_to_tmp(fixture_path).unwrap();
        let dataset = Dataset::open(&test_dir.path_str()).await.unwrap();

        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0].index_version, expected_version as i32);

        let match_ids = search_ids(
            &dataset,
            FullTextSearchQuery::new("compatibility".to_string()),
        )
        .await;
        assert_eq!(match_ids, (0..300).collect::<Vec<_>>());

        let phrase =
            PhraseQuery::new("lance database".to_string()).with_column(Some("text".to_string()));
        let phrase_ids = search_ids(
            &dataset,
            FullTextSearchQuery::new_query(FtsQuery::Phrase(phrase)),
        )
        .await;
        assert_eq!(phrase_ids, (0..300).step_by(3).collect::<Vec<_>>());
    }

    #[rstest]
    #[tokio::test]
    async fn test_optimize_fts(#[values(false, true)] with_position: bool) {
        let words = ["apple", "banana", "cherry", "date"];

        let dir = TempStrDir::default();
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
        let data = StringArray::from_iter_values(words.iter().map(|s| s.to_string()));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(data)]).unwrap();
        let batch_iterator = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let mut dataset = Dataset::write(batch_iterator, &dir, None).await.unwrap();

        let params = InvertedIndexParams::default()
            .lower_case(false)
            .with_position(with_position);
        dataset
            .create_index(&["text"], IndexType::Inverted, None, &params, true)
            .await
            .unwrap();

        async fn assert_indexed_rows(dataset: &Dataset, expected_indexed_rows: usize) {
            let stats = dataset.index_statistics("text_idx").await.unwrap();
            let stats: serde_json::Value = serde_json::from_str(&stats).unwrap();
            let indexed_rows = stats["num_indexed_rows"].as_u64().unwrap() as usize;
            let unindexed_rows = stats["num_unindexed_rows"].as_u64().unwrap() as usize;
            let num_rows = dataset.count_all_rows().await.unwrap();
            assert_eq!(indexed_rows, expected_indexed_rows);
            assert_eq!(unindexed_rows, num_rows - expected_indexed_rows);
        }

        let num_rows = dataset.count_all_rows().await.unwrap();
        assert_indexed_rows(&dataset, num_rows).await;

        let new_words = ["elephant", "fig", "grape", "honeydew"];
        let new_data = StringArray::from_iter_values(new_words.iter().map(|s| s.to_string()));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(new_data)]).unwrap();
        let batch_iter = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        dataset.append(batch_iter, None).await.unwrap();
        assert_indexed_rows(&dataset, num_rows).await;

        dataset
            .optimize_indices(&OptimizeOptions::append())
            .await
            .unwrap();
        let num_rows = dataset.count_all_rows().await.unwrap();
        assert_indexed_rows(&dataset, num_rows).await;

        for &word in words.iter().chain(new_words.iter()) {
            let query_result = dataset
                .scan()
                .project(&["text"])
                .unwrap()
                .full_text_search(FullTextSearchQuery::new(word.to_string()))
                .unwrap()
                .limit(Some(10), None)
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();

            let texts = query_result["text"]
                .as_string::<i32>()
                .iter()
                .map(|v| match v {
                    None => "".to_string(),
                    Some(v) => v.to_string(),
                })
                .collect::<Vec<String>>();

            assert_eq!(texts.len(), 1);
            assert_eq!(texts[0], word);
        }

        let uppercase_words = ["Apple", "Banana", "Cherry", "Date"];
        for &word in uppercase_words.iter() {
            let query_result = dataset
                .scan()
                .project(&["text"])
                .unwrap()
                .full_text_search(FullTextSearchQuery::new(word.to_string()))
                .unwrap()
                .limit(Some(10), None)
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();

            let texts = query_result["text"]
                .as_string::<i32>()
                .iter()
                .map(|v| match v {
                    None => "".to_string(),
                    Some(v) => v.to_string(),
                })
                .collect::<Vec<String>>();

            assert_eq!(texts.len(), 0);
        }
        let new_data = StringArray::from_iter_values(uppercase_words.iter().map(|s| s.to_string()));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(new_data)]).unwrap();
        let batch_iter = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        dataset.append(batch_iter, None).await.unwrap();
        assert_indexed_rows(&dataset, num_rows).await;

        // we should be able to query the new words
        for &word in uppercase_words.iter() {
            let query_result = dataset
                .scan()
                .project(&["text"])
                .unwrap()
                .full_text_search(FullTextSearchQuery::new(word.to_string()))
                .unwrap()
                .limit(Some(10), None)
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();

            let texts = query_result["text"]
                .as_string::<i32>()
                .iter()
                .map(|v| match v {
                    None => "".to_string(),
                    Some(v) => v.to_string(),
                })
                .collect::<Vec<String>>();

            assert_eq!(texts.len(), 1, "query: {}, texts: {:?}", word, texts);
            assert_eq!(texts[0], word, "query: {}, texts: {:?}", word, texts);
        }

        dataset
            .optimize_indices(&OptimizeOptions::append())
            .await
            .unwrap();
        let num_rows = dataset.count_all_rows().await.unwrap();
        assert_indexed_rows(&dataset, num_rows).await;

        // we should be able to query the new words after optimization
        for &word in uppercase_words.iter() {
            let query_result = dataset
                .scan()
                .project(&["text"])
                .unwrap()
                .full_text_search(FullTextSearchQuery::new(word.to_string()))
                .unwrap()
                .limit(Some(10), None)
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();

            let texts = query_result["text"]
                .as_string::<i32>()
                .iter()
                .map(|v| match v {
                    None => "".to_string(),
                    Some(v) => v.to_string(),
                })
                .collect::<Vec<String>>();

            assert_eq!(texts.len(), 1, "query: {}, texts: {:?}", word, texts);
            assert_eq!(texts[0], word, "query: {}, texts: {:?}", word, texts);

            // we should be able to query the new words after compaction
            compact_files(&mut dataset, CompactionOptions::default(), None)
                .await
                .unwrap();
            for &word in uppercase_words.iter() {
                let query_result = dataset
                    .scan()
                    .project(&["text"])
                    .unwrap()
                    .full_text_search(FullTextSearchQuery::new(word.to_string()))
                    .unwrap()
                    .try_into_batch()
                    .await
                    .unwrap();
                let texts = query_result["text"]
                    .as_string::<i32>()
                    .iter()
                    .map(|v| match v {
                        None => "".to_string(),
                        Some(v) => v.to_string(),
                    })
                    .collect::<Vec<String>>();
                assert_eq!(texts.len(), 1, "query: {}, texts: {:?}", word, texts);
                assert_eq!(texts[0], word, "query: {}, texts: {:?}", word, texts);
            }
            assert_indexed_rows(&dataset, num_rows).await;
        }
    }

    #[tokio::test]
    async fn test_optimize_fts_respects_num_indices_to_merge() {
        use lance_index::scalar::inverted::query::PhraseQuery;

        async fn append_texts(dataset: &mut Dataset, schema: Arc<Schema>, rows: &[(i32, &str)]) {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(rows.iter().map(|(id, _)| *id))),
                    Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|(_, text)| *text),
                    )),
                ],
            )
            .unwrap();
            let batch_iter = RecordBatchIterator::new(vec![Ok(batch)], schema);
            dataset.append(batch_iter, None).await.unwrap();
        }

        async fn num_fts_segments(dataset: &Dataset) -> usize {
            let stats = dataset.index_statistics("text_idx").await.unwrap();
            let stats: serde_json::Value = serde_json::from_str(&stats).unwrap();
            stats["num_indices"].as_u64().unwrap() as usize
        }

        async fn assert_fts_ids(
            dataset: &Dataset,
            query: FullTextSearchQuery,
            expected_ids: &[i32],
        ) {
            let result = dataset
                .scan()
                .project(&["id"])
                .unwrap()
                .full_text_search(query)
                .unwrap()
                .limit(Some(20), None)
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();
            let mut ids = result["id"].as_primitive::<Int32Type>().values().to_vec();
            ids.sort_unstable();
            assert_eq!(ids, expected_ids);
        }

        let dir = TempStrDir::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values([0, 1])),
                Arc::new(StringArray::from_iter_values([
                    "alpha base phrase",
                    "beta base phrase",
                ])),
            ],
        )
        .unwrap();
        let batch_iterator = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let mut dataset = Dataset::write(batch_iterator, &dir, None).await.unwrap();
        let params = InvertedIndexParams::default().with_position(true);
        dataset
            .create_index(&["text"], IndexType::Inverted, None, &params, true)
            .await
            .unwrap();
        assert_eq!(num_fts_segments(&dataset).await, 1);

        append_texts(&mut dataset, schema.clone(), &[(2, "gamma delta phrase")]).await;
        dataset
            .optimize_indices(&OptimizeOptions::append())
            .await
            .unwrap();
        assert_eq!(num_fts_segments(&dataset).await, 2);

        append_texts(&mut dataset, schema.clone(), &[(3, "epsilon zeta phrase")]).await;
        dataset
            .optimize_indices(&OptimizeOptions::merge(1))
            .await
            .unwrap();
        assert_eq!(num_fts_segments(&dataset).await, 2);
        assert_fts_ids(
            &dataset,
            FullTextSearchQuery::new("epsilon".to_owned()),
            &[3],
        )
        .await;

        append_texts(&mut dataset, schema.clone(), &[(4, "eta theta phrase")]).await;
        dataset
            .optimize_indices(&OptimizeOptions::append())
            .await
            .unwrap();
        assert_eq!(num_fts_segments(&dataset).await, 3);

        dataset
            .optimize_indices(&OptimizeOptions::merge(2))
            .await
            .unwrap();
        assert_eq!(num_fts_segments(&dataset).await, 2);
        assert_fts_ids(
            &dataset,
            FullTextSearchQuery::new_query(PhraseQuery::new("eta theta".to_owned()).into()),
            &[4],
        )
        .await;

        append_texts(&mut dataset, schema, &[(5, "iota kappa phrase")]).await;
        dataset
            .optimize_indices(&OptimizeOptions::append())
            .await
            .unwrap();
        assert_eq!(num_fts_segments(&dataset).await, 3);

        dataset
            .optimize_indices(&OptimizeOptions::merge(10))
            .await
            .unwrap();
        assert_eq!(num_fts_segments(&dataset).await, 1);
        assert_fts_ids(&dataset, FullTextSearchQuery::new("alpha".to_owned()), &[0]).await;
        assert_fts_ids(&dataset, FullTextSearchQuery::new("iota".to_owned()), &[5]).await;
    }

    #[tokio::test]
    async fn test_create_index_too_small_for_pq() {
        let test_dir = TempStrDir::default();
        let dimensions = 1536;

        let field = Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimensions,
            ),
            false,
        );

        let schema = Arc::new(Schema::new(vec![field]));
        let float_arr = generate_random_array(100 * dimensions as usize);

        let vectors =
            arrow_array::FixedSizeListArray::try_new_from_values(float_arr, dimensions).unwrap();
        let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vectors)]).unwrap();
        let reader = RecordBatchIterator::new(
            vec![record_batch.clone()].into_iter().map(Ok),
            schema.clone(),
        );

        let test_uri = &test_dir;
        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        let params = VectorIndexParams::ivf_pq(1, 8, 96, DistanceType::L2, 1);
        let result = dataset
            .create_index(&["vector"], IndexType::Vector, None, &params, false)
            .await;

        assert!(matches!(result, Err(Error::Unprocessable { .. })));
        if let Error::Unprocessable { message, .. } = result.unwrap_err() {
            assert_eq!(
                message,
                "Not enough rows to train PQ. Requires 256 rows but only 100 available",
            )
        }
    }

    #[tokio::test]
    async fn test_create_bitmap_index() {
        let test_dir = TempStrDir::default();
        let field = Field::new("tag", DataType::Utf8, false);
        let schema = Arc::new(Schema::new(vec![field]));
        let array = StringArray::from_iter_values((0..128).map(|i| ["a", "b", "c"][i % 3]));
        let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
        let reader = RecordBatchIterator::new(
            vec![record_batch.clone()].into_iter().map(Ok),
            schema.clone(),
        );

        let test_uri = &test_dir;
        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();
        dataset
            .create_index(
                &["tag"],
                IndexType::Bitmap,
                None,
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let indices = dataset.load_indices().await.unwrap();
        let index = dataset
            .open_generic_index("tag", &indices[0].uuid, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(index.index_type(), IndexType::Bitmap);
    }

    // #[tokio::test]
    #[lance_test_macros::test(tokio::test)]
    async fn test_load_indices() {
        let session = Arc::new(Session::default());
        let write_params = WriteParams {
            session: Some(session.clone()),
            ..Default::default()
        };

        let test_dir = TempStrDir::default();
        let field = Field::new("tag", DataType::Utf8, false);
        let schema = Arc::new(Schema::new(vec![field]));
        let array = StringArray::from_iter_values((0..128).map(|i| ["a", "b", "c"][i % 3]));
        let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
        let reader = RecordBatchIterator::new(
            vec![record_batch.clone()].into_iter().map(Ok),
            schema.clone(),
        );

        let test_uri = &test_dir;
        let mut dataset = Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();
        dataset
            .create_index(
                &["tag"],
                IndexType::Bitmap,
                None,
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        dataset.object_store.as_ref().io_stats_incremental(); // Reset

        let indices = dataset.load_indices().await.unwrap();
        let stats = dataset.object_store.as_ref().io_stats_incremental();
        // We should already have this cached since we just wrote it.
        assert_io_eq!(stats, read_iops, 0);
        assert_io_eq!(stats, read_bytes, 0);
        assert_eq!(indices.len(), 1);

        // Clear the cache
        session.index_cache.clear().await;
        session.file_metadata_cache().clear().await;

        let dataset2 = DatasetBuilder::from_uri(test_uri)
            .with_session(session.clone())
            .load()
            .await
            .unwrap();
        let stats = dataset2.object_store.as_ref().io_stats_incremental(); // Reset
        assert_io_lt!(stats, read_bytes, 64 * 1024);

        // Because the manifest is so small, we should have opportunistically
        // cached the indices in memory already.
        let indices2 = dataset2.load_indices().await.unwrap();
        let stats = dataset2.object_store.as_ref().io_stats_incremental();
        assert_io_eq!(stats, read_iops, 0);
        assert_io_eq!(stats, read_bytes, 0);
        assert_eq!(indices2.len(), 1);
    }

    #[tokio::test]
    async fn test_load_indices_singleflights_concurrent_cache_misses() {
        let session = Arc::new(Session::default());
        let write_params = WriteParams {
            session: Some(session.clone()),
            ..Default::default()
        };
        let test_dir = TempStrDir::default();
        let schema = Arc::new(Schema::new(vec![Field::new("tag", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["a", "b", "c"]))],
        )
        .unwrap();
        let mut dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            &test_dir,
            Some(write_params),
        )
        .await
        .unwrap();
        dataset
            .create_index(
                &["tag"],
                IndexType::Bitmap,
                None,
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        session.index_cache.clear().await;
        let before = session.index_cache_stats().await;
        let results = futures::future::join_all((0..32).map(|_| dataset.load_indices())).await;
        assert!(results.iter().all(|result| result.is_ok()));
        let after = session.index_cache_stats().await;

        assert_eq!(after.misses - before.misses, 1);
        assert_eq!(after.hits - before.hits, 31);
    }

    #[tokio::test]
    async fn test_remap_empty() {
        let data = gen_batch()
            .col("int", array::step::<Int32Type>())
            .col(
                "vector",
                array::rand_vec::<Float32Type>(Dimension::from(16)),
            )
            .into_reader_rows(RowCount::from(256), BatchCount::from(1));
        let mut dataset = Dataset::write(data, "memory://", None).await.unwrap();

        let params = VectorIndexParams::ivf_pq(1, 8, 1, DistanceType::L2, 1);
        dataset
            .create_index(&["vector"], IndexType::Vector, None, &params, false)
            .await
            .unwrap();

        let index_uuid = dataset.load_indices().await.unwrap()[0].uuid;
        let remap_to_empty = (0..dataset.count_all_rows().await.unwrap())
            .map(|i| (i as u64, None))
            .collect::<HashMap<_, _>>();
        let new_uuid = remap_index(&dataset, &index_uuid, &RowAddrRemap::direct(remap_to_empty))
            .await
            .unwrap();
        assert_eq!(new_uuid, RemapResult::Keep(index_uuid));
    }

    #[tokio::test]
    async fn test_optimize_ivf_pq_up_to_date() {
        // https://github.com/lance-format/lance/issues/4016
        let nrows = 256;
        let dimensions = 16;
        let column_name = "vector";
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                column_name,
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dimensions,
                ),
                false,
            ),
        ]));

        let float_arr = generate_random_array(nrows * dimensions as usize);
        let vectors =
            arrow_array::FixedSizeListArray::try_new_from_values(float_arr, dimensions).unwrap();
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::Int32Array::from_iter_values(0..nrows as i32)),
                Arc::new(vectors),
            ],
        )
        .unwrap();

        let reader = RecordBatchIterator::new(
            vec![record_batch.clone()].into_iter().map(Ok),
            schema.clone(),
        );
        let mut dataset = Dataset::write(reader, "memory://", None).await.unwrap();

        let params = VectorIndexParams::ivf_pq(1, 8, 2, MetricType::L2, 2);
        dataset
            .create_index(&[column_name], IndexType::Vector, None, &params, true)
            .await
            .unwrap();

        let query_vector = generate_random_array(dimensions as usize);

        let nearest = dataset
            .scan()
            .nearest(column_name, &query_vector, 5)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        let ids = nearest["id"].as_primitive::<Int32Type>();
        let mut seen = HashSet::new();
        for id in ids.values() {
            assert!(seen.insert(*id), "Duplicate id found: {}", id);
        }

        dataset
            .optimize_indices(&OptimizeOptions::default())
            .await
            .unwrap();

        dataset.validate().await.unwrap();

        let nearest_after = dataset
            .scan()
            .nearest(column_name, &query_vector, 5)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        let ids = nearest_after["id"].as_primitive::<Int32Type>();
        let mut seen = HashSet::new();
        for id in ids.values() {
            assert!(seen.insert(*id), "Duplicate id found: {}", id);
        }
    }

    #[tokio::test]
    async fn test_index_created_at_timestamp() {
        // Test that created_at is set when creating an index
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("values", DataType::Utf8, false),
        ]));

        let values = StringArray::from_iter_values(["hello", "world", "foo", "bar"]);
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..4)),
                Arc::new(values),
            ],
        )
        .unwrap();

        let reader =
            RecordBatchIterator::new(vec![record_batch].into_iter().map(Ok), schema.clone());

        let mut dataset = Dataset::write(reader, "memory://", None).await.unwrap();

        // Record time before creating index
        let before_index = chrono::Utc::now();

        // Create a scalar index
        dataset
            .create_index(
                &["values"],
                IndexType::Scalar,
                Some("test_idx".to_string()),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        // Record time after creating index
        let after_index = chrono::Utc::now();

        // Get index metadata
        let indices = dataset.load_indices().await.unwrap();
        let test_index = indices.iter().find(|idx| idx.name == "test_idx").unwrap();

        // Verify created_at is set and within reasonable bounds
        assert!(test_index.created_at.is_some());
        let created_at = test_index.created_at.unwrap();
        assert!(created_at >= before_index);
        assert!(created_at <= after_index);
    }

    #[tokio::test]
    async fn test_index_statistics_updated_at() {
        // Test that updated_at appears in index statistics
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("values", DataType::Utf8, false),
        ]));

        let values = StringArray::from_iter_values(["hello", "world", "foo", "bar"]);
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..4)),
                Arc::new(values),
            ],
        )
        .unwrap();

        let reader =
            RecordBatchIterator::new(vec![record_batch].into_iter().map(Ok), schema.clone());

        let mut dataset = Dataset::write(reader, "memory://", None).await.unwrap();

        // Create a scalar index
        dataset
            .create_index(
                &["values"],
                IndexType::Scalar,
                Some("test_idx".to_string()),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        // Get index statistics
        let stats_str = dataset.index_statistics("test_idx").await.unwrap();
        let stats: serde_json::Value = serde_json::from_str(&stats_str).unwrap();

        // Verify updated_at_timestamp_ms field exists in statistics
        assert!(stats["updated_at_timestamp_ms"].is_number());
        let updated_at = stats["updated_at_timestamp_ms"].as_u64().unwrap();

        // Get the index metadata to compare with created_at
        let indices = dataset.load_indices().await.unwrap();
        let test_index = indices.iter().find(|idx| idx.name == "test_idx").unwrap();
        let created_at = test_index.created_at.unwrap().timestamp_millis() as u64;

        // For a single index, updated_at should equal created_at
        assert_eq!(updated_at, created_at);
    }

    #[tokio::test]
    async fn test_index_statistics_updated_at_multiple_deltas() {
        // Test that updated_at reflects max(created_at) across multiple delta indices
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                false,
            ),
        ]));

        // Create initial dataset (need more rows for PQ training)
        let num_rows = 300;
        let float_arr = generate_random_array(4 * num_rows);
        let vectors = FixedSizeListArray::try_new_from_values(float_arr, 4).unwrap();
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..num_rows as i32)),
                Arc::new(vectors),
            ],
        )
        .unwrap();

        let reader =
            RecordBatchIterator::new(vec![record_batch].into_iter().map(Ok), schema.clone());

        let mut dataset = Dataset::write(reader, "memory://", None).await.unwrap();

        // Create vector index
        let params = VectorIndexParams::ivf_pq(1, 8, 2, MetricType::L2, 2);
        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("test_vec_idx".to_string()),
                &params,
                false,
            )
            .await
            .unwrap();

        // Get initial statistics
        let stats_str_1 = dataset.index_statistics("test_vec_idx").await.unwrap();
        let stats_1: serde_json::Value = serde_json::from_str(&stats_str_1).unwrap();
        let initial_updated_at = stats_1["updated_at_timestamp_ms"].as_u64().unwrap();

        // Add more data to create additional delta indices
        std::thread::sleep(std::time::Duration::from_millis(10)); // Ensure different timestamp

        let num_rows_2 = 50;
        let float_arr_2 = generate_random_array(4 * num_rows_2);
        let vectors_2 = FixedSizeListArray::try_new_from_values(float_arr_2, 4).unwrap();
        let record_batch_2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(
                    num_rows as i32..(num_rows + num_rows_2) as i32,
                )),
                Arc::new(vectors_2),
            ],
        )
        .unwrap();

        let reader_2 =
            RecordBatchIterator::new(vec![record_batch_2].into_iter().map(Ok), schema.clone());

        dataset.append(reader_2, None).await.unwrap();

        // Update the index
        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("test_vec_idx".to_string()),
                &params,
                true,
            )
            .await
            .unwrap();

        // Get updated statistics
        let stats_str_2 = dataset.index_statistics("test_vec_idx").await.unwrap();
        let stats_2: serde_json::Value = serde_json::from_str(&stats_str_2).unwrap();
        let final_updated_at = stats_2["updated_at_timestamp_ms"].as_u64().unwrap();

        // The updated_at should be newer than the initial one
        assert!(final_updated_at >= initial_updated_at);
    }

    #[tokio::test]
    async fn test_index_statistics_updated_at_none_when_no_created_at() {
        // Test backward compatibility: when indices were created before the created_at field,
        // updated_at_timestamp_ms should be null

        // Use test data created with Lance 0.29.0 (before created_at field was added)
        let test_dir =
            copy_test_data_to_tmp("v0.30.0_pre_created_at/index_without_created_at").unwrap();
        let test_uri = test_dir.path_str();
        let test_uri = &test_uri;

        let dataset = Dataset::open(test_uri).await.unwrap();

        // Get the index metadata to verify created_at is None
        let indices = dataset.load_indices().await.unwrap();
        assert!(!indices.is_empty(), "Test dataset should have indices");

        // Verify that the index created with old version has no created_at
        for index in indices.iter() {
            assert!(
                index.created_at.is_none(),
                "Index from old version should have created_at = None"
            );
        }

        // Get index statistics - should work even with old indices
        let index_name = &indices[0].name;
        let stats_str = dataset.index_statistics(index_name).await.unwrap();
        let stats: serde_json::Value = serde_json::from_str(&stats_str).unwrap();

        // Verify updated_at_timestamp_ms field is null when no indices have created_at
        assert!(
            stats["updated_at_timestamp_ms"].is_null(),
            "updated_at_timestamp_ms should be null when no indices have created_at timestamps"
        );
    }

    #[tokio::test]
    async fn test_legacy_vector_index_details_inferred_on_load_and_migration() {
        use lance_linalg::distance::DistanceType;

        // Create a fresh dataset with IVF_HNSW_SQ so inference produces non-default
        // details (HNSW config + SQ compression) that survive proto serialization.
        let test_dir = lance_core::utils::tempfile::TempDir::default();
        let test_uri = test_dir.path_str();
        let data = gen_batch()
            .col("i", array::step::<Int32Type>())
            .col("vec", array::rand_vec::<Float32Type>(16.into()))
            .into_reader_rows(RowCount::from(1024), BatchCount::from(1));
        let mut dataset = Dataset::write(data, &test_uri, None).await.unwrap();

        let params = VectorIndexParams::with_ivf_hnsw_sq_params(
            DistanceType::Cosine,
            IvfBuildParams {
                num_partitions: Some(2),
                ..Default::default()
            },
            HnswBuildParams::default(),
            SQBuildParams::default(),
        );
        dataset
            .create_index(&["vec"], IndexType::Vector, None, &params, true)
            .await
            .unwrap();

        // Verify the index has populated details.
        let descriptions = dataset.describe_indices(None).await.unwrap();
        assert_eq!(descriptions.len(), 1);
        assert_eq!(descriptions[0].index_type(), "IVF_HNSW_SQ");

        // Simulate a legacy dataset by clearing details from the manifest.
        // Write a new manifest with empty VectorIndexDetails value bytes.
        let mut indices = dataset.load_indices().await.unwrap().as_ref().clone();
        for idx in &mut indices {
            if let Some(details) = idx.index_details.as_ref()
                && details.type_url.ends_with("VectorIndexDetails")
            {
                idx.index_details = Some(Arc::new(vector_index_details_default()));
            }
        }
        // Write back via a no-op commit that carries the cleared indices.
        // We commit by doing a delete("false") after replacing the cached indices.
        let metadata_key = crate::session::index_caches::IndexMetadataKey {
            version: dataset.version().version,
            store_identity: &dataset.object_store.store_prefix,
        };
        dataset
            .index_cache
            .insert_with_key(&metadata_key, Arc::new(indices))
            .await;
        dataset.delete("false").await.unwrap();

        // -- Part 1: Inference on load --
        // Open with a fresh session so nothing is cached.
        let dataset = DatasetBuilder::from_uri(&test_uri)
            .with_session(Arc::new(Session::default()))
            .load()
            .await
            .unwrap();

        // load_indices should detect empty details and infer from index files.
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        let details = indices[0].index_details.as_ref().unwrap();
        assert!(
            !details.value.is_empty(),
            "Details should have been inferred from index files"
        );

        // describe_indices should return a real type (not generic "Vector").
        let descriptions = dataset.describe_indices(None).await.unwrap();
        assert_eq!(descriptions.len(), 1);
        assert_ne!(
            descriptions[0].index_type(),
            "Vector",
            "Should have inferred a specific index type"
        );
        assert_eq!(
            descriptions[0].index_type(),
            "IVF_HNSW_SQ",
            "Inferred type should match the originally-built index"
        );
        let inferred_type = descriptions[0].index_type().to_string();
        let details_json: serde_json::Value =
            serde_json::from_str(&descriptions[0].details().unwrap()).unwrap();
        assert_eq!(details_json["metric_type"], "COSINE");
        assert_eq!(details_json["compression"]["type"], "sq");
        assert!(
            details_json["hnsw"]["max_connections"].is_number(),
            "Inferred HNSW config should have max_connections; got {details_json}"
        );
        assert!(details_json["hnsw"]["construction_ef"].is_number());
        assert!(details_json["hnsw"]["max_level"].is_number());

        // -- Part 2: Migration persists inferred details --
        let mut dataset = dataset;
        dataset.delete("false").await.unwrap();

        // Open with yet another fresh session.
        let dataset = DatasetBuilder::from_uri(&test_uri)
            .with_session(Arc::new(Session::default()))
            .load()
            .await
            .unwrap();

        // The migrated manifest should have non-empty details without
        // needing to read index files again.
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        assert!(
            !indices[0].index_details.as_ref().unwrap().value.is_empty(),
            "Migrated manifest should have non-empty details"
        );
        let descriptions = dataset.describe_indices(None).await.unwrap();
        assert_eq!(descriptions[0].index_type(), inferred_type);
    }

    #[tokio::test]
    async fn test_describe_indices_tolerates_missing_index_details() {
        // An index whose manifest entry has no index details (e.g. created
        // before details were persisted) is still described on a best-effort
        // basis rather than causing describe_indices to error.
        use lance_datagen::{BatchCount, RowCount, array};

        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();
        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let dataset = Dataset::write(reader, test_uri, None).await.unwrap();
        let field_id = dataset.schema().field("id").unwrap().id;

        let metadata = IndexMetadata {
            uuid: Uuid::new_v4(),
            name: "mystery_idx".to_string(),
            fields: vec![field_id],
            dataset_version: dataset.manifest.version,
            fragment_bitmap: Some(std::iter::once(0_u32).collect()),
            index_details: None,
            index_version: 0,
            created_at: None,
            base_id: None,
            files: None,
        };

        let desc = IndexDescriptionImpl::try_new(vec![metadata], &dataset)
            .await
            .unwrap();
        assert_eq!(desc.index_type(), "Unknown");
        assert_eq!(desc.type_url(), "");
        assert_eq!(desc.details().unwrap(), "{}");
        assert_eq!(desc.rows_indexed(), 10);
    }

    #[tokio::test]
    async fn test_describe_indices_derives_type_from_url_without_plugin() {
        // When index details exist but no plugin is registered for the type
        // URL, the index type is derived from the type URL rather than being
        // reported as the opaque "Unknown".
        use lance_datagen::{BatchCount, RowCount, array};

        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();
        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let dataset = Dataset::write(reader, test_uri, None).await.unwrap();
        let field_id = dataset.schema().field("id").unwrap().id;

        let metadata = IndexMetadata {
            uuid: Uuid::new_v4(),
            name: "mystery_idx".to_string(),
            fields: vec![field_id],
            dataset_version: dataset.manifest.version,
            fragment_bitmap: Some(std::iter::once(0_u32).collect()),
            index_details: Some(Arc::new(prost_types::Any {
                type_url: "/lance.index.pb.MysteryIndexDetails".to_string(),
                value: Vec::new(),
            })),
            index_version: 0,
            created_at: None,
            base_id: None,
            files: None,
        };

        let desc = IndexDescriptionImpl::try_new(vec![metadata], &dataset)
            .await
            .unwrap();
        assert_eq!(desc.index_type(), "Mystery");
        assert_eq!(desc.type_url(), "/lance.index.pb.MysteryIndexDetails");
    }

    #[rstest]
    #[case::btree("i", IndexType::BTree, Box::new(ScalarIndexParams::default()))]
    #[case::bitmap("i", IndexType::Bitmap, Box::new(ScalarIndexParams::default()))]
    #[case::inverted("text", IndexType::Inverted, Box::new(InvertedIndexParams::default()))]
    #[tokio::test]
    async fn test_create_empty_scalar_index(
        #[case] column_name: &str,
        #[case] index_type: IndexType,
        #[case] params: Box<dyn IndexParams>,
    ) {
        use lance_datagen::{BatchCount, ByteCount, RowCount, array};

        // Create dataset with scalar and text columns (no vector column needed)
        let reader = lance_datagen::gen_batch()
            .col("i", array::step::<Int32Type>())
            .col("text", array::rand_utf8(ByteCount::from(10), false))
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));
        let mut dataset = Dataset::write(reader, "memory://test", None).await.unwrap();

        // Create an empty index with train=false
        // Test using IntoFuture - can await directly without calling .execute()
        dataset
            .create_index_builder(&[column_name], index_type, params.as_ref())
            .name("index".to_string())
            .train(false)
            .await
            .unwrap();

        // Verify we can get index statistics
        let stats = dataset.index_statistics("index").await.unwrap();
        let stats: serde_json::Value = serde_json::from_str(&stats).unwrap();
        assert_eq!(
            stats["num_indexed_rows"], 0,
            "Empty index should have zero indexed rows"
        );

        // Append new data using lance_datagen
        let append_reader = lance_datagen::gen_batch()
            .col("i", array::step::<Int32Type>())
            .col("text", array::rand_utf8(ByteCount::from(10), false))
            .into_reader_rows(RowCount::from(50), BatchCount::from(1));

        dataset.append(append_reader, None).await.unwrap();

        // Critical test: Verify the empty index is still present after append
        let indices_after_append = dataset.load_indices().await.unwrap();
        assert_eq!(
            indices_after_append.len(),
            1,
            "Index should be retained after append for index type {:?}",
            index_type
        );

        let stats = dataset.index_statistics("index").await.unwrap();
        let stats: serde_json::Value = serde_json::from_str(&stats).unwrap();
        assert_eq!(
            stats["num_indexed_rows"], 0,
            "Empty index should still have zero indexed rows after append"
        );

        // Test optimize_indices with empty index
        dataset.optimize_indices(&Default::default()).await.unwrap();

        // Verify the index still exists after optimization
        let indices_after_optimize = dataset.load_indices().await.unwrap();
        assert_eq!(
            indices_after_optimize.len(),
            1,
            "Index should still exist after optimization"
        );

        // Check index statistics after optimization
        let stats = dataset.index_statistics("index").await.unwrap();
        let stats: serde_json::Value = serde_json::from_str(&stats).unwrap();
        assert_eq!(
            stats["num_unindexed_rows"], 0,
            "Empty index should indexed all rows"
        );
    }

    #[rstest]
    #[case::block_size_128(128)]
    #[case::block_size_256(256)]
    #[tokio::test]
    async fn test_optimize_empty_code_fts_index_preserves_params(#[case] block_size: usize) {
        let dir = TempStrDir::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("code", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values([0, 1])),
                Arc::new(StringArray::from_iter_values([
                    "fn GetUserEmail() {}",
                    "fn ParseConfig() {}",
                ])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let mut dataset = Dataset::write(reader, &dir, None).await.unwrap();

        let params = InvertedIndexParams::default()
            .analyzer("code")
            .unwrap()
            .block_size(block_size)
            .unwrap()
            .split_identifiers(true)
            .preserve_original(false);
        dataset
            .create_index_builder(&["code"], IndexType::Inverted, &params)
            .name("code_idx".to_string())
            .train(false)
            .await
            .unwrap();

        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0].index_version, INVERTED_INDEX_VERSION_V3 as i32);

        dataset.optimize_indices(&Default::default()).await.unwrap();

        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0].index_version, INVERTED_INDEX_VERSION_V3 as i32);
        let details = indices[0]
            .index_details
            .as_ref()
            .expect("optimized FTS index should retain index details")
            .to_msg::<InvertedIndexDetails>()
            .unwrap();
        assert_eq!(details.base_tokenizer.as_deref(), Some("code"));
        assert_eq!(details.block_size, Some(block_size as u32));
        let code_config = details
            .code_config
            .expect("optimized code FTS index should retain code configuration");
        assert!(code_config.split_identifiers);
        assert_eq!(code_config.preserve_original, Some(false));

        let stats: serde_json::Value =
            serde_json::from_str(&dataset.index_statistics("code_idx").await.unwrap()).unwrap();
        assert_eq!(stats["num_unindexed_rows"], 0);

        let result = dataset
            .scan()
            .project(&["id"])
            .unwrap()
            .full_text_search(FullTextSearchQuery::new("email".to_string()))
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(result["id"].as_primitive::<Int32Type>().values(), &[0]);
    }

    /// Helper function to check if an index is being used in a query plan
    fn assert_index_usage(plan: &str, column_name: &str, should_use_index: bool, context: &str) {
        let index_used = if column_name == "text" {
            // For inverted index, look for MatchQuery which indicates FTS index usage
            plan.contains("MatchQuery")
        } else {
            // For btree/bitmap, look for MaterializeIndex which indicates scalar index usage
            plan.contains("ScalarIndexQuery")
        };

        if should_use_index {
            assert!(
                index_used,
                "Query plan should use index {}: {}",
                context, plan
            );
        } else {
            assert!(
                !index_used,
                "Query plan should NOT use index {}: {}",
                context, plan
            );
        }
    }

    /// Test that scalar indices are retained after deleting all data from a table.
    ///
    /// This test verifies that when we:
    /// 1. Create a table with data
    /// 2. Add a scalar index with train=true
    /// 3. Delete all data in the table
    /// The index remains available on the table.
    #[rstest]
    #[case::btree("i", IndexType::BTree, Box::new(ScalarIndexParams::default()))]
    #[case::bitmap("i", IndexType::Bitmap, Box::new(ScalarIndexParams::default()))]
    #[case::inverted("text", IndexType::Inverted, Box::new(InvertedIndexParams::default()))]
    #[tokio::test]
    async fn test_scalar_index_retained_after_delete_all(
        #[case] column_name: &str,
        #[case] index_type: IndexType,
        #[case] params: Box<dyn IndexParams>,
    ) {
        use lance_datagen::{BatchCount, ByteCount, RowCount, array};

        // Create dataset with initial data
        let reader = lance_datagen::gen_batch()
            .col("i", array::step::<Int32Type>())
            .col("text", array::rand_utf8(ByteCount::from(10), false))
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));
        let mut dataset = Dataset::write(reader, "memory://test", None).await.unwrap();

        // Create index with train=true (normal index with data)
        dataset
            .create_index_builder(&[column_name], index_type, params.as_ref())
            .name("index".to_string())
            .train(true)
            .await
            .unwrap();

        // Verify index was created and has indexed rows
        let stats = dataset.index_statistics("index").await.unwrap();
        let stats: serde_json::Value = serde_json::from_str(&stats).unwrap();
        assert_eq!(
            stats["num_indexed_rows"], 100,
            "Index should have indexed all 100 rows"
        );

        // Verify index is being used in queries before delete
        let plan = if column_name == "text" {
            // Use full-text search for inverted index
            dataset
                .scan()
                .full_text_search(FullTextSearchQuery::new("test".to_string()))
                .unwrap()
                .explain_plan(false)
                .await
                .unwrap()
        } else {
            // Use equality filter for btree/bitmap indices
            dataset
                .scan()
                .filter(format!("{} = 50", column_name).as_str())
                .unwrap()
                .explain_plan(false)
                .await
                .unwrap()
        };
        // Verify index is being used before delete
        assert_index_usage(&plan, column_name, true, "before delete");

        let indexes = dataset.load_indices().await.unwrap();
        let original_index = indexes[0].clone();

        // Delete all rows from the table
        dataset.delete("true").await.unwrap();

        // Verify table is empty
        let row_count = dataset.count_rows(None).await.unwrap();
        assert_eq!(row_count, 0, "Table should be empty after delete all");

        // Critical test: Verify the index still exists after deleting all data
        let indices_after_delete = dataset.load_indices().await.unwrap();
        assert_eq!(
            indices_after_delete.len(),
            1,
            "Index should be retained after deleting all data"
        );
        assert_eq!(
            indices_after_delete[0].name, "index",
            "Index name should remain the same after delete"
        );

        // Critical test: Verify the fragment bitmap is empty after delete
        let index_after_delete = &indices_after_delete[0];
        let effective_bitmap = index_after_delete
            .effective_fragment_bitmap(&dataset.fragment_bitmap)
            .unwrap();
        assert!(
            effective_bitmap.is_empty(),
            "Effective bitmap should be empty after deleting all data"
        );
        assert_eq!(
            index_after_delete.fragment_bitmap, original_index.fragment_bitmap,
            "Fragment bitmap should remain the same after delete"
        );

        // Verify we can still get index statistics
        let stats = dataset.index_statistics("index").await.unwrap();
        let stats: serde_json::Value = serde_json::from_str(&stats).unwrap();
        assert_eq!(
            stats["num_indexed_rows"], 0,
            "Index should now report zero indexed rows after delete all"
        );
        assert_eq!(
            stats["num_unindexed_rows"], 0,
            "Index should report zero unindexed rows after delete all"
        );
        assert_eq!(
            stats["num_indexed_fragments"], 0,
            "Index should report zero indexed fragments after delete all"
        );
        assert_eq!(
            stats["num_unindexed_fragments"], 0,
            "Index should report zero unindexed fragments after delete all"
        );

        // Verify index is NOT being used in queries after delete (empty bitmap)
        if column_name == "text" {
            // Inverted indexes will still appear to be used in FTS queries.
            // TODO: once metrics are working on FTS queries, we can check the
            // analyze plan output instead for index usage.
            let _plan_after_delete = dataset
                .scan()
                .project(&[column_name])
                .unwrap()
                .full_text_search(FullTextSearchQuery::new("test".to_string()))
                .unwrap()
                .explain_plan(false)
                .await
                .unwrap();
            assert_index_usage(
                &_plan_after_delete,
                column_name,
                true,
                "after delete (empty bitmap)",
            );
        } else {
            // Use equality filter for btree/bitmap indices
            let _plan_after_delete = dataset
                .scan()
                .filter(format!("{} = 50", column_name).as_str())
                .unwrap()
                .explain_plan(false)
                .await
                .unwrap();
            // Verify index is NOT being used after delete (empty bitmap)
            assert_index_usage(
                &_plan_after_delete,
                column_name,
                false,
                "after delete (empty bitmap)",
            );
        }

        // Test that we can append new data and the index is still there
        let append_reader = lance_datagen::gen_batch()
            .col("i", array::step::<Int32Type>())
            .col("text", array::rand_utf8(ByteCount::from(10), false))
            .into_reader_rows(RowCount::from(50), BatchCount::from(1));

        dataset.append(append_reader, None).await.unwrap();

        // Verify index still exists after append
        let indices_after_append = dataset.load_indices().await.unwrap();
        assert_eq!(
            indices_after_append.len(),
            1,
            "Index should still exist after appending to empty table"
        );
        let stats = dataset.index_statistics("index").await.unwrap();
        let stats: serde_json::Value = serde_json::from_str(&stats).unwrap();
        assert_eq!(
            stats["num_indexed_rows"], 0,
            "Index should now report zero indexed rows after data is added"
        );

        // Test optimize_indices after delete all
        dataset.optimize_indices(&Default::default()).await.unwrap();

        // Verify index still exists after optimization
        let indices_after_optimize = dataset.load_indices().await.unwrap();
        assert_eq!(
            indices_after_optimize.len(),
            1,
            "Index should still exist after optimization following delete all"
        );

        // Verify we can still get index statistics after optimization
        let stats = dataset.index_statistics("index").await.unwrap();
        let stats: serde_json::Value = serde_json::from_str(&stats).unwrap();
        assert_eq!(
            stats["num_indexed_rows"],
            dataset.count_rows(None).await.unwrap(),
            "Index should now cover all newly added rows after optimization"
        );
    }

    /// Test that scalar indices are retained after updating rows in a table.
    ///
    /// This test verifies that when we:
    /// 1. Create a table with data
    /// 2. Add a scalar index with train=true
    /// 3. Update rows in the table
    /// The index remains available on the table.
    #[rstest]
    #[case::btree("i", IndexType::BTree, Box::new(ScalarIndexParams::default()))]
    #[case::bitmap("i", IndexType::Bitmap, Box::new(ScalarIndexParams::default()))]
    #[case::inverted("text", IndexType::Inverted, Box::new(InvertedIndexParams::default()))]
    #[tokio::test]
    async fn test_scalar_index_retained_after_update(
        #[case] column_name: &str,
        #[case] index_type: IndexType,
        #[case] params: Box<dyn IndexParams>,
    ) {
        use crate::dataset::UpdateBuilder;
        use lance_datagen::{BatchCount, ByteCount, RowCount, array};

        // Create dataset with initial data
        let reader = lance_datagen::gen_batch()
            .col("i", array::step::<Int32Type>())
            .col("text", array::rand_utf8(ByteCount::from(10), false))
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));
        let mut dataset = Dataset::write(reader, "memory://test", None).await.unwrap();

        // Create index with train=true (normal index with data)
        dataset
            .create_index_builder(&[column_name], index_type, params.as_ref())
            .name("index".to_string())
            .train(true)
            .await
            .unwrap();

        // Verify index was created and has indexed rows
        let stats = dataset.index_statistics("index").await.unwrap();
        let stats: serde_json::Value = serde_json::from_str(&stats).unwrap();
        assert_eq!(
            stats["num_indexed_rows"], 100,
            "Index should have indexed all 100 rows"
        );

        // Verify index is being used in queries before update
        let plan = if column_name == "text" {
            // Use full-text search for inverted index
            dataset
                .scan()
                .project(&[column_name])
                .unwrap()
                .full_text_search(FullTextSearchQuery::new("test".to_string()))
                .unwrap()
                .explain_plan(false)
                .await
                .unwrap()
        } else {
            // Use equality filter for btree/bitmap indices
            dataset
                .scan()
                .filter(format!("{} = 50", column_name).as_str())
                .unwrap()
                .explain_plan(false)
                .await
                .unwrap()
        };
        // Verify index is being used before update
        assert_index_usage(&plan, column_name, true, "before update");

        // Update some rows - update first 50 rows
        let update_result = UpdateBuilder::new(Arc::new(dataset))
            .set("i", "i + 1000")
            .unwrap()
            .set("text", "'updated_' || text")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();

        let mut dataset = update_result.new_dataset.as_ref().clone();

        // Verify row count remains the same
        let row_count = dataset.count_rows(None).await.unwrap();
        assert_eq!(row_count, 100, "Row count should remain 100 after update");

        // Critical test: Verify the index still exists after updating data
        let indices_after_update = dataset.load_indices().await.unwrap();
        assert_eq!(
            indices_after_update.len(),
            1,
            "Index should be retained after updating rows"
        );

        // Critical test: Verify the effective fragment bitmap is empty after update
        let indices = dataset.load_indices().await.unwrap();
        let index = &indices[0];
        let effective_bitmap = index
            .effective_fragment_bitmap(&dataset.fragment_bitmap)
            .unwrap();
        assert!(
            effective_bitmap.is_empty(),
            "Effective fragment bitmap should be empty after updating all data"
        );

        // Verify we can still get index statistics
        let stats_after_update = dataset.index_statistics("index").await.unwrap();
        let stats_after_update: serde_json::Value =
            serde_json::from_str(&stats_after_update).unwrap();

        // The index should still be available
        assert_eq!(
            stats_after_update["num_indexed_rows"], 0,
            "Index statistics should be zero after update, as it is not re-trained"
        );

        // Verify index behavior in queries after update (empty bitmap)
        if column_name == "text" {
            // Inverted indexes will still appear to be used in FTS queries even with empty bitmaps.
            // This is because FTS queries require an index to exist, and the query execution
            // will handle the empty bitmap appropriately by falling back to scanning unindexed data.
            // TODO: once metrics are working on FTS queries, we can check the
            // analyze plan output instead for actual index usage statistics.
            let _plan_after_update = dataset
                .scan()
                .project(&[column_name])
                .unwrap()
                .full_text_search(FullTextSearchQuery::new("test".to_string()))
                .unwrap()
                .explain_plan(false)
                .await
                .unwrap();
            assert_index_usage(
                &_plan_after_update,
                column_name,
                true, // FTS indices always appear in the plan, even with empty bitmaps
                "after update (empty bitmap)",
            );
        } else {
            // Use equality filter for btree/bitmap indices
            let _plan_after_update = dataset
                .scan()
                .filter(format!("{} = 50", column_name).as_str())
                .unwrap()
                .explain_plan(false)
                .await
                .unwrap();
            // With immutable bitmaps, index is still used even with empty effective bitmap
            // The prefilter will handle non-existent fragments
            assert_index_usage(
                &_plan_after_update,
                column_name,
                false,
                "after update (empty effective bitmap)",
            );
        }

        // Test that we can optimize indices after update
        dataset.optimize_indices(&Default::default()).await.unwrap();

        // Verify index still exists after optimization
        let indices_after_optimize = dataset.load_indices().await.unwrap();
        assert_eq!(
            indices_after_optimize.len(),
            1,
            "Index should still exist after optimization following update"
        );

        let stats_after_optimization = dataset.index_statistics("index").await.unwrap();
        let stats_after_optimization: serde_json::Value =
            serde_json::from_str(&stats_after_optimization).unwrap();

        // The index should still be available
        assert_eq!(
            stats_after_optimization["num_unindexed_rows"], 0,
            "Index should have zero unindexed rows after optimization"
        );
    }

    // Helper function to validate indices after each clone iteration
    async fn validate_indices_after_clone(
        dataset: &Dataset,
        round: usize,
        expected_scalar_rows: usize,
        dimensions: u32,
    ) {
        // Verify cloned dataset has indices
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(
            indices.len(),
            2,
            "Round {}: Cloned dataset should have 2 indices",
            round
        );
        let index_names: HashSet<String> = indices.iter().map(|idx| idx.name.clone()).collect();
        assert!(
            index_names.contains("vector_idx"),
            "Round {}: Should contain vector_idx",
            round
        );
        assert!(
            index_names.contains("category_idx"),
            "Round {}: Should contain category_idx",
            round
        );

        // Test basic data access without using indices for now
        // This ensures the dataset is accessible and data is intact
        // In chain cloning, each round adds 50 rows to the previous dataset
        // Round 1: 300 (original), Round 2: 350 (300 + 50 from round 1), Round 3: 400 (350 + 50 from round 2)
        let expected_total_rows = 300 + (round - 1) * 50;
        let total_rows = dataset.count_rows(None).await.unwrap();
        assert_eq!(
            total_rows, expected_total_rows,
            "Round {}: Should have {} rows after clone (chain cloning accumulates data)",
            round, expected_total_rows
        );

        // Verify vector search
        let query_vector = generate_random_array(dimensions as usize);
        let search_results = dataset
            .scan()
            .nearest("vector", &query_vector, 5)
            .unwrap()
            .limit(Some(5), None)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert!(
            search_results.num_rows() > 0,
            "Round {}: Vector search should return results immediately after clone",
            round
        );

        // Test basic scalar query to verify data integrity
        let scalar_results = dataset
            .scan()
            .filter("category = 'category_0'")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        assert_eq!(
            expected_scalar_rows,
            scalar_results.num_rows(),
            "Round {}: Scalar query should return {} results",
            round,
            expected_scalar_rows
        );
    }

    #[tokio::test]
    async fn test_shallow_clone_with_index() {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        // Create a schema with both vector and scalar columns
        let dimensions = 16u32;
        // Generate test data using lance_datagen (300 rows to satisfy PQ training requirements)
        let data = gen_batch()
            .col("id", array::step::<Int32Type>())
            .col("category", array::fill_utf8("category_0".to_string()))
            .col(
                "vector",
                array::rand_vec::<Float32Type>(Dimension::from(dimensions)),
            )
            .into_reader_rows(RowCount::from(300), BatchCount::from(1));

        // Create initial dataset
        let mut dataset = Dataset::write(data, test_uri, None).await.unwrap();
        // Create vector index (IVF_PQ)
        let vector_params = VectorIndexParams::ivf_pq(2, 8, 2, MetricType::L2, 10);
        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("vector_idx".to_string()),
                &vector_params,
                true,
            )
            .await
            .unwrap();

        // Create scalar index (BTree)
        dataset
            .create_index(
                &["category"],
                IndexType::BTree,
                Some("category_idx".to_string()),
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();

        // Verify indices were created
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 2, "Should have 2 indices");
        let index_names: HashSet<String> = indices.iter().map(|idx| idx.name.clone()).collect();
        assert!(index_names.contains("vector_idx"));
        assert!(index_names.contains("category_idx"));

        // Test scalar query on source dataset
        let scalar_results = dataset
            .scan()
            .filter("category = 'category_0'")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        let source_scalar_query_rows = scalar_results.num_rows();
        assert!(
            scalar_results.num_rows() > 0,
            "Scalar query should return results"
        );

        // Multiple shallow clone iterations test with chain cloning
        let clone_rounds = 3;
        let mut current_dataset = dataset;

        for round in 1..=clone_rounds {
            let round_clone_dir = format!("{}/clone_round_{}", test_dir, round);
            let round_cloned_uri = &round_clone_dir;
            let tag_name = format!("shallow_clone_test_{}", round);

            // Create tag for this round (use current dataset for chain cloning)
            let current_version = current_dataset.version().version;
            current_dataset
                .tags()
                .create(&tag_name, current_version)
                .await
                .unwrap();

            // Perform shallow clone for this round (chain cloning from current dataset)
            let mut round_cloned_dataset = current_dataset
                .shallow_clone(round_cloned_uri, tag_name.as_str(), None)
                .await
                .unwrap();

            // Immediately validate indices after each clone
            validate_indices_after_clone(
                &round_cloned_dataset,
                round,
                source_scalar_query_rows,
                dimensions,
            )
            .await;

            // Complete validation cycle after each clone: append data, optimize index, validate
            // Append new data to the cloned dataset
            let new_data = gen_batch()
                .col(
                    "id",
                    array::step_custom::<Int32Type>(300 + (round * 50) as i32, 1),
                )
                .col("category", array::fill_utf8(format!("category_{}", round)))
                .col(
                    "vector",
                    array::rand_vec::<Float32Type>(Dimension::from(dimensions)),
                )
                .into_reader_rows(RowCount::from(50), BatchCount::from(1));

            round_cloned_dataset = Dataset::write(
                new_data,
                round_cloned_uri,
                Some(WriteParams {
                    mode: WriteMode::Append,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

            // Verify row count increased
            let expected_rows = 300 + round * 50;
            let total_rows = round_cloned_dataset.count_rows(None).await.unwrap();
            assert_eq!(
                total_rows, expected_rows,
                "Round {}: Should have {} rows after append",
                round, expected_rows
            );

            let indices_before_optimize = round_cloned_dataset.load_indices().await.unwrap();
            let vector_idx_before = indices_before_optimize
                .iter()
                .find(|idx| idx.name == "vector_idx")
                .unwrap();
            let category_idx_before = indices_before_optimize
                .iter()
                .find(|idx| idx.name == "category_idx")
                .unwrap();

            // Optimize indices
            round_cloned_dataset
                .optimize_indices(&OptimizeOptions::merge(indices_before_optimize.len()))
                .await
                .unwrap();

            // Verify index UUID has changed
            let optimized_indices = round_cloned_dataset.load_indices().await.unwrap();
            let new_vector_idx = optimized_indices
                .iter()
                .find(|idx| idx.name == "vector_idx")
                .unwrap();
            let new_category_idx = optimized_indices
                .iter()
                .find(|idx| idx.name == "category_idx")
                .unwrap();

            assert_ne!(
                new_vector_idx.uuid, vector_idx_before.uuid,
                "Round {}: Vector index should have a new UUID after optimization",
                round
            );
            assert_ne!(
                new_category_idx.uuid, category_idx_before.uuid,
                "Round {}: Category index should have a new UUID after optimization",
                round
            );

            // Verify the location of index files
            use std::path::PathBuf;
            let clone_indices_dir = PathBuf::from(round_cloned_uri).join("_indices");
            let vector_index_dir = clone_indices_dir.join(new_vector_idx.uuid.to_string());
            let category_index_dir = clone_indices_dir.join(new_category_idx.uuid.to_string());

            assert!(
                vector_index_dir.exists(),
                "Round {}: New vector index directory should exist in cloned dataset location: {:?}",
                round,
                vector_index_dir
            );
            assert!(
                category_index_dir.exists(),
                "Round {}: New category index directory should exist in cloned dataset location: {:?}",
                round,
                category_index_dir
            );

            // Verify base id
            assert!(
                new_vector_idx.base_id.is_none(),
                "Round {}: New vector index should not have base_id after optimization in cloned dataset",
                round
            );
            assert!(
                new_category_idx.base_id.is_none(),
                "Round {}: New category index should not have base_id after optimization in cloned dataset",
                round
            );

            // Verify the source location does NOT contain new data
            let original_indices_dir = PathBuf::from(current_dataset.uri()).join("_indices");
            let wrong_vector_dir = original_indices_dir.join(new_vector_idx.uuid.to_string());
            let wrong_category_dir = original_indices_dir.join(new_category_idx.uuid.to_string());

            assert!(
                !wrong_vector_dir.exists(),
                "Round {}: New vector index should NOT be in original dataset location: {:?}",
                round,
                wrong_vector_dir
            );
            assert!(
                !wrong_category_dir.exists(),
                "Round {}: New category index should NOT be in original dataset location: {:?}",
                round,
                wrong_category_dir
            );

            // Validate data integrity and index functionality after optimization
            let old_category_results = round_cloned_dataset
                .scan()
                .filter("category = 'category_0'")
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();

            let new_category_results = round_cloned_dataset
                .scan()
                .filter(&format!("category = 'category_{}'", round))
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();

            assert_eq!(
                source_scalar_query_rows,
                old_category_results.num_rows(),
                "Round {}: Should find old category data with {} rows",
                round,
                source_scalar_query_rows
            );
            assert!(
                new_category_results.num_rows() > 0,
                "Round {}: Should find new category data",
                round
            );

            // Test vector search functionality
            let query_vector = generate_random_array(dimensions as usize);
            let search_results = round_cloned_dataset
                .scan()
                .nearest("vector", &query_vector, 10)
                .unwrap()
                .limit(Some(10), None)
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();

            assert!(
                search_results.num_rows() > 0,
                "Round {}: Vector search should return results after optimization",
                round
            );

            // Verify statistic of indexes
            let vector_stats: serde_json::Value = serde_json::from_str(
                &round_cloned_dataset
                    .index_statistics("vector_idx")
                    .await
                    .unwrap(),
            )
            .unwrap();
            let category_stats: serde_json::Value = serde_json::from_str(
                &round_cloned_dataset
                    .index_statistics("category_idx")
                    .await
                    .unwrap(),
            )
            .unwrap();

            assert_eq!(
                vector_stats["num_indexed_rows"].as_u64().unwrap(),
                expected_rows as u64,
                "Round {}: Vector index should have {} indexed rows",
                round,
                expected_rows
            );
            assert_eq!(
                category_stats["num_indexed_rows"].as_u64().unwrap(),
                expected_rows as u64,
                "Round {}: Category index should have {} indexed rows",
                round,
                expected_rows
            );

            // Prepare for next round: use the cloned dataset as the source for next clone
            current_dataset = round_cloned_dataset;
        }

        // Use the final cloned dataset for any remaining tests
        let final_cloned_dataset = current_dataset;

        // Verify cloned dataset has indices
        let cloned_indices = final_cloned_dataset.load_indices().await.unwrap();
        assert_eq!(
            cloned_indices.len(),
            2,
            "Final cloned dataset should have 2 indices"
        );
        let cloned_index_names: HashSet<String> =
            cloned_indices.iter().map(|idx| idx.name.clone()).collect();
        assert!(cloned_index_names.contains("vector_idx"));
        assert!(cloned_index_names.contains("category_idx"));

        // Test vector search on final cloned dataset
        let query_vector = generate_random_array(dimensions as usize);
        let search_results = final_cloned_dataset
            .scan()
            .nearest("vector", &query_vector, 5)
            .unwrap()
            .limit(Some(5), None)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        assert!(
            search_results.num_rows() > 0,
            "Vector search should return results on final dataset"
        );

        // Test scalar query on final cloned dataset
        let scalar_results = final_cloned_dataset
            .scan()
            .filter("category = 'category_0'")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        assert_eq!(
            source_scalar_query_rows,
            scalar_results.num_rows(),
            "Scalar query should return results on final dataset"
        );
    }

    #[tokio::test]
    async fn test_initialize_indices() {
        use crate::dataset::Dataset;
        use arrow_array::types::Float32Type;
        use lance_core::utils::tempfile::TempStrDir;
        use lance_datagen::{BatchCount, RowCount, array};
        use lance_index::scalar::{InvertedIndexParams, ScalarIndexParams};
        use lance_linalg::distance::MetricType;
        use std::collections::HashSet;

        // Create source dataset with various index types
        let test_dir = TempStrDir::default();
        let source_uri = format!("{}/{}", test_dir, "source");
        let target_uri = format!("{}/{}", test_dir, "target");

        // Generate test data using lance_datagen (need at least 256 rows for PQ training)
        let source_reader = lance_datagen::gen_batch()
            .col("vector", array::rand_vec::<Float32Type>(8.into()))
            .col(
                "text",
                array::cycle_utf8_literals(&["hello world", "foo bar", "test data"]),
            )
            .col("id", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(300), BatchCount::from(1));

        // Create source dataset
        let mut source_dataset = Dataset::write(source_reader, &source_uri, None)
            .await
            .unwrap();

        // Create indices on source dataset
        // 1. Vector index
        let vector_params = VectorIndexParams::ivf_pq(4, 8, 2, MetricType::L2, 10);
        source_dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("vec_idx".to_string()),
                &vector_params,
                false,
            )
            .await
            .unwrap();

        // 2. FTS index
        let fts_params = InvertedIndexParams::default();
        source_dataset
            .create_index(
                &["text"],
                IndexType::Inverted,
                Some("text_idx".to_string()),
                &fts_params,
                false,
            )
            .await
            .unwrap();

        // 3. Scalar index
        let scalar_params = ScalarIndexParams::default();
        source_dataset
            .create_index(
                &["id"],
                IndexType::BTree,
                Some("id_idx".to_string()),
                &scalar_params,
                false,
            )
            .await
            .unwrap();

        // Reload source dataset to get updated index metadata
        let source_dataset = Dataset::open(&source_uri).await.unwrap();

        // Verify source has 3 indices
        let source_indices = source_dataset.load_indices().await.unwrap();
        assert_eq!(
            source_indices.len(),
            3,
            "Source dataset should have 3 indices"
        );

        // Create target dataset with same schema but different data (need at least 256 rows for PQ)
        let target_reader = lance_datagen::gen_batch()
            .col("vector", array::rand_vec::<Float32Type>(8.into()))
            .col(
                "text",
                array::cycle_utf8_literals(&["foo bar", "test data", "hello world"]),
            )
            .col("id", array::step_custom::<Int32Type>(100, 1))
            .into_reader_rows(RowCount::from(300), BatchCount::from(1));
        let mut target_dataset = Dataset::write(target_reader, &target_uri, None)
            .await
            .unwrap();

        // Initialize indices from source dataset
        target_dataset
            .initialize_indices(&source_dataset)
            .await
            .unwrap();

        // Verify target has same indices
        let target_indices = target_dataset.load_indices().await.unwrap();
        assert_eq!(
            target_indices.len(),
            3,
            "Target dataset should have 3 indices after initialization"
        );

        // Check index names match
        let source_names: HashSet<String> =
            source_indices.iter().map(|idx| idx.name.clone()).collect();
        let target_names: HashSet<String> =
            target_indices.iter().map(|idx| idx.name.clone()).collect();
        assert_eq!(
            source_names, target_names,
            "Index names should match between source and target"
        );

        // Verify indices are functional by running queries
        // 1. Test vector index
        let query_vector = generate_random_array(8);
        let search_results = target_dataset
            .scan()
            .nearest("vector", &query_vector, 5)
            .unwrap()
            .limit(Some(5), None)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert!(
            search_results.num_rows() > 0,
            "Vector index should be functional"
        );

        // 2. Test scalar index
        let scalar_results = target_dataset
            .scan()
            .filter("id = 125")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(
            scalar_results.num_rows(),
            1,
            "Scalar index should find exact match"
        );
    }

    #[tokio::test]
    async fn test_initialize_indices_with_missing_field() {
        use crate::dataset::Dataset;
        use arrow_array::types::Int32Type;
        use lance_core::utils::tempfile::TempStrDir;
        use lance_datagen::{BatchCount, RowCount, array};
        use lance_index::scalar::ScalarIndexParams;

        // Test that initialize_indices handles missing fields gracefully
        let test_dir = TempStrDir::default();
        let source_uri = format!("{}/{}", test_dir, "source");
        let target_uri = format!("{}/{}", test_dir, "target");

        // Create source dataset with extra field
        let source_reader = lance_datagen::gen_batch()
            .col("id", array::step::<Int32Type>())
            .col("extra", array::cycle_utf8_literals(&["test"]))
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut source_dataset = Dataset::write(source_reader, &source_uri, None)
            .await
            .unwrap();

        // Create index on extra field in source
        source_dataset
            .create_index(
                &["extra"],
                IndexType::BTree,
                None,
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        // Create target dataset without extra field
        let target_reader = lance_datagen::gen_batch()
            .col("id", array::step_custom::<Int32Type>(10, 1))
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut target_dataset = Dataset::write(target_reader, &target_uri, None)
            .await
            .unwrap();

        // Initialize indices should skip the index on missing field with an error
        let result = target_dataset.initialize_indices(&source_dataset).await;

        // Should fail when field is missing
        assert!(result.is_err(), "Should error when field is missing");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not found in target dataset")
        );
    }

    #[tokio::test]
    async fn test_initialize_single_index() {
        use crate::dataset::Dataset;
        use crate::index::vector::VectorIndexParams;
        use arrow_array::types::{Float32Type, Int32Type};
        use lance_core::utils::tempfile::TempStrDir;
        use lance_datagen::{BatchCount, RowCount, array};
        use lance_index::scalar::ScalarIndexParams;
        use lance_linalg::distance::MetricType;

        let test_dir = TempStrDir::default();
        let source_uri = format!("{}/{}", test_dir, "source");
        let target_uri = format!("{}/{}", test_dir, "target");

        // Create source dataset (need at least 256 rows for PQ training)
        let source_reader = lance_datagen::gen_batch()
            .col("id", array::step::<Int32Type>())
            .col("name", array::rand_utf8(4.into(), false))
            .col("vector", array::rand_vec::<Float32Type>(8.into()))
            .into_reader_rows(RowCount::from(300), BatchCount::from(1));
        let mut source_dataset = Dataset::write(source_reader, &source_uri, None)
            .await
            .unwrap();

        // Create multiple indices on source
        let scalar_params = ScalarIndexParams::default();
        source_dataset
            .create_index(
                &["id"],
                IndexType::BTree,
                Some("id_index".to_string()),
                &scalar_params,
                false,
            )
            .await
            .unwrap();

        let vector_params = VectorIndexParams::ivf_pq(16, 8, 4, MetricType::L2, 50);
        source_dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("vector_index".to_string()),
                &vector_params,
                false,
            )
            .await
            .unwrap();

        // Reload source dataset to get updated index metadata
        let source_dataset = Dataset::open(&source_uri).await.unwrap();

        // Create target dataset with same schema
        let target_reader = lance_datagen::gen_batch()
            .col("id", array::step::<Int32Type>())
            .col("name", array::rand_utf8(4.into(), false))
            .col("vector", array::rand_vec::<Float32Type>(8.into()))
            .into_reader_rows(RowCount::from(300), BatchCount::from(1));
        let mut target_dataset = Dataset::write(target_reader, &target_uri, None)
            .await
            .unwrap();

        // Initialize only the vector index
        target_dataset
            .initialize_index(&source_dataset, "vector_index")
            .await
            .unwrap();

        // Verify only vector index was created
        let target_indices = target_dataset.load_indices().await.unwrap();
        assert_eq!(target_indices.len(), 1, "Should have only 1 index");
        assert_eq!(
            target_indices[0].name, "vector_index",
            "Should have the vector index"
        );

        // Initialize the scalar index
        target_dataset
            .initialize_index(&source_dataset, "id_index")
            .await
            .unwrap();

        // Verify both indices now exist
        let target_indices = target_dataset.load_indices().await.unwrap();
        assert_eq!(target_indices.len(), 2, "Should have 2 indices");

        let index_names: HashSet<String> =
            target_indices.iter().map(|idx| idx.name.clone()).collect();
        assert!(
            index_names.contains("vector_index"),
            "Should have vector index"
        );
        assert!(index_names.contains("id_index"), "Should have id index");

        // Test error case - non-existent index
        let result = target_dataset
            .initialize_index(&source_dataset, "non_existent")
            .await;
        assert!(result.is_err(), "Should error for non-existent index");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not found in source dataset")
        );
    }

    #[rstest]
    #[case::simple("value", "data.value")]
    #[case::quoted("value.with.dot", "data.`value.with.dot`")]
    #[tokio::test]
    async fn test_initialize_index_on_nested_field(
        #[case] nested_field_name: &str,
        #[case] nested_field_path: &str,
    ) {
        let nested_field = Arc::new(Field::new(nested_field_name, DataType::Int32, false));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::Struct(vec![nested_field.clone()].into()),
            false,
        )]));
        let nested_values = arrow_array::StructArray::from(vec![(
            nested_field,
            Arc::new(Int32Array::from_iter_values(0..10)) as Arc<dyn arrow_array::Array>,
        )]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(nested_values)]).unwrap();

        let test_dir = TempStrDir::default();
        let source_uri = format!("{}/source", test_dir);
        let target_uri = format!("{}/target", test_dir);

        let source_reader =
            RecordBatchIterator::new(vec![batch.clone()].into_iter().map(Ok), schema.clone());
        let mut source_dataset = Dataset::write(source_reader, &source_uri, None)
            .await
            .unwrap();
        source_dataset
            .create_index(
                &[nested_field_path],
                IndexType::BTree,
                Some("nested_idx".to_string()),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let source_dataset = Dataset::open(&source_uri).await.unwrap();

        let target_reader =
            RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let mut target_dataset = Dataset::write(target_reader, &target_uri, None)
            .await
            .unwrap();
        target_dataset
            .initialize_index(&source_dataset, "nested_idx")
            .await
            .unwrap();

        let target_indices = target_dataset.load_indices().await.unwrap();
        assert_eq!(target_indices.len(), 1);
        assert_eq!(
            target_dataset
                .schema()
                .field_path(target_indices[0].fields[0])
                .unwrap(),
            nested_field_path
        );
    }

    #[tokio::test]
    async fn test_vector_index_on_nested_field_with_dots() {
        let dimensions = 16;
        let num_rows = 256;

        // Create schema with nested field containing dots in the name
        let struct_field = Field::new(
            "embedding_data",
            DataType::Struct(
                vec![
                    Field::new(
                        "vector.v1", // Field name with dot
                        DataType::FixedSizeList(
                            Arc::new(Field::new("item", DataType::Float32, true)),
                            dimensions,
                        ),
                        false,
                    ),
                    Field::new(
                        "vector.v2", // Another field name with dot
                        DataType::FixedSizeList(
                            Arc::new(Field::new("item", DataType::Float32, true)),
                            dimensions,
                        ),
                        false,
                    ),
                    Field::new("metadata", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            struct_field,
        ]));

        // Generate test data
        let float_arr_v1 = generate_random_array(num_rows * dimensions as usize);
        let vectors_v1 = FixedSizeListArray::try_new_from_values(float_arr_v1, dimensions).unwrap();

        let float_arr_v2 = generate_random_array(num_rows * dimensions as usize);
        let vectors_v2 = FixedSizeListArray::try_new_from_values(float_arr_v2, dimensions).unwrap();

        let ids = Int32Array::from_iter_values(0..num_rows as i32);
        let metadata = StringArray::from_iter_values((0..num_rows).map(|i| format!("meta_{}", i)));

        let struct_array = arrow_array::StructArray::from(vec![
            (
                Arc::new(Field::new(
                    "vector.v1",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        dimensions,
                    ),
                    false,
                )),
                Arc::new(vectors_v1) as Arc<dyn arrow_array::Array>,
            ),
            (
                Arc::new(Field::new(
                    "vector.v2",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        dimensions,
                    ),
                    false,
                )),
                Arc::new(vectors_v2) as Arc<dyn arrow_array::Array>,
            ),
            (
                Arc::new(Field::new("metadata", DataType::Utf8, false)),
                Arc::new(metadata) as Arc<dyn arrow_array::Array>,
            ),
        ]);

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(struct_array)])
                .unwrap();

        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        // Test creating index on nested field with dots using quoted syntax
        let nested_column_path_v1 = "embedding_data.`vector.v1`";
        let params = VectorIndexParams::ivf_pq(10, 8, 2, MetricType::L2, 10);

        dataset
            .create_index(
                &[nested_column_path_v1],
                IndexType::Vector,
                Some("vec_v1_idx".to_string()),
                &params,
                true,
            )
            .await
            .unwrap();

        // Verify index was created
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0].name, "vec_v1_idx");

        // Verify the correct field was indexed
        let field_id = indices[0].fields[0];
        let field_path = dataset.schema().field_path(field_id).unwrap();
        assert_eq!(field_path, "embedding_data.`vector.v1`");

        // Test creating index on the second vector field with dots
        let nested_column_path_v2 = "embedding_data.`vector.v2`";
        dataset
            .create_index(
                &[nested_column_path_v2],
                IndexType::Vector,
                Some("vec_v2_idx".to_string()),
                &params,
                true,
            )
            .await
            .unwrap();

        // Verify both indices exist
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 2);

        // Verify we can query using the indexed fields and check the plan
        let query_vector = generate_random_array(dimensions as usize);

        // Check the query plan for the first vector field
        let plan_v1 = dataset
            .scan()
            .nearest(nested_column_path_v1, &query_vector, 5)
            .unwrap()
            .explain_plan(false)
            .await
            .unwrap();

        // Verify the vector index is being used (should show ANNSubIndex or ANNIvfPartition)
        assert!(
            plan_v1.contains("ANNSubIndex") || plan_v1.contains("ANNIvfPartition"),
            "Query plan should use vector index for nested field with dots. Plan: {}",
            plan_v1
        );

        let search_results_v1 = dataset
            .scan()
            .nearest(nested_column_path_v1, &query_vector, 5)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        assert_eq!(search_results_v1.num_rows(), 5);

        // Check the query plan for the second vector field
        let plan_v2 = dataset
            .scan()
            .nearest(nested_column_path_v2, &query_vector, 5)
            .unwrap()
            .explain_plan(false)
            .await
            .unwrap();

        // Verify the vector index is being used
        assert!(
            plan_v2.contains("ANNSubIndex") || plan_v2.contains("ANNIvfPartition"),
            "Query plan should use vector index for second nested field with dots. Plan: {}",
            plan_v2
        );

        let search_results_v2 = dataset
            .scan()
            .nearest(nested_column_path_v2, &query_vector, 5)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        assert_eq!(search_results_v2.num_rows(), 5);
    }

    #[tokio::test]
    async fn test_vector_index_on_simple_nested_field() {
        // This test reproduces the Python test scenario from test_nested_field_vector_index
        // where the nested field path is simple (data.embedding) without dots in field names
        let dimensions = 16;
        let num_rows = 256;

        // Create schema with simple nested field (no dots in field names)
        let struct_field = Field::new(
            "data",
            DataType::Struct(
                vec![
                    Field::new(
                        "embedding",
                        DataType::FixedSizeList(
                            Arc::new(Field::new("item", DataType::Float32, true)),
                            dimensions,
                        ),
                        false,
                    ),
                    Field::new("label", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            struct_field,
        ]));

        // Generate test data
        let float_arr = generate_random_array(num_rows * dimensions as usize);
        let vectors = FixedSizeListArray::try_new_from_values(float_arr, dimensions).unwrap();

        let ids = Int32Array::from_iter_values(0..num_rows as i32);
        let labels = StringArray::from_iter_values((0..num_rows).map(|i| format!("label_{}", i)));

        let struct_array = arrow_array::StructArray::from(vec![
            (
                Arc::new(Field::new(
                    "embedding",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        dimensions,
                    ),
                    false,
                )),
                Arc::new(vectors) as Arc<dyn arrow_array::Array>,
            ),
            (
                Arc::new(Field::new("label", DataType::Utf8, false)),
                Arc::new(labels) as Arc<dyn arrow_array::Array>,
            ),
        ]);

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(struct_array)])
                .unwrap();

        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        // Test creating index on nested field
        let nested_column_path = "data.embedding";
        let params = VectorIndexParams::ivf_pq(2, 8, 2, MetricType::L2, 10);

        dataset
            .create_index(
                &[nested_column_path],
                IndexType::Vector,
                Some("vec_idx".to_string()),
                &params,
                true,
            )
            .await
            .unwrap();

        // Verify index was created
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0].name, "vec_idx");

        // Verify the correct field was indexed
        let field_id = indices[0].fields[0];
        let field_path = dataset.schema().field_path(field_id).unwrap();
        assert_eq!(field_path, "data.embedding");

        // Test querying with the index
        let query_vector = generate_random_array(dimensions as usize);

        let plan = dataset
            .scan()
            .nearest(nested_column_path, &query_vector, 5)
            .unwrap()
            .explain_plan(false)
            .await
            .unwrap();

        // Verify the vector index is being used
        assert!(
            plan.contains("ANNSubIndex") || plan.contains("ANNIvfPartition"),
            "Query plan should use vector index for nested field. Plan: {}",
            plan
        );

        let search_results = dataset
            .scan()
            .nearest(nested_column_path, &query_vector, 5)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        assert_eq!(search_results.num_rows(), 5);
    }

    #[tokio::test]
    async fn test_btree_index_on_nested_field_with_dots() {
        // Test creating BTree index on nested field with dots in the name
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        // Create schema with nested field containing dots
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "data",
                DataType::Struct(
                    vec![
                        Field::new("value.v1", DataType::Int32, false),
                        Field::new("value.v2", DataType::Float32, false),
                        Field::new("text", DataType::Utf8, false),
                    ]
                    .into(),
                ),
                false,
            ),
        ]));

        // Generate test data
        let num_rows = 1000;
        let ids = Int32Array::from_iter_values(0..num_rows);
        let values_v1 = Int32Array::from_iter_values((0..num_rows).map(|i| i % 100));
        let values_v2 = Float32Array::from_iter_values((0..num_rows).map(|i| (i as f32) * 0.1));
        let texts = StringArray::from_iter_values((0..num_rows).map(|i| format!("text_{}", i)));

        let struct_array = arrow_array::StructArray::from(vec![
            (
                Arc::new(Field::new("value.v1", DataType::Int32, false)),
                Arc::new(values_v1) as Arc<dyn arrow_array::Array>,
            ),
            (
                Arc::new(Field::new("value.v2", DataType::Float32, false)),
                Arc::new(values_v2) as Arc<dyn arrow_array::Array>,
            ),
            (
                Arc::new(Field::new("text", DataType::Utf8, false)),
                Arc::new(texts) as Arc<dyn arrow_array::Array>,
            ),
        ]);

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(struct_array)])
                .unwrap();

        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        // Create BTree index on nested field with dots
        let nested_column_path = "data.`value.v1`";
        let params = ScalarIndexParams::default();

        dataset
            .create_index(
                &[nested_column_path],
                IndexType::BTree,
                Some("btree_v1_idx".to_string()),
                &params,
                true,
            )
            .await
            .unwrap();

        // Reload dataset to ensure index is loaded
        dataset = Dataset::open(test_uri).await.unwrap();

        // Verify index was created
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0].name, "btree_v1_idx");

        // Verify the correct field was indexed
        let field_id = indices[0].fields[0];
        let field_path = dataset.schema().field_path(field_id).unwrap();
        assert_eq!(field_path, "data.`value.v1`");

        // Test querying with the index and verify it's being used
        let plan = dataset
            .scan()
            .filter("data.`value.v1` = 42")
            .unwrap()
            .prefilter(true)
            .explain_plan(false)
            .await
            .unwrap();

        // Verify the query plan (scalar indices on nested fields may use optimized filters)
        // The index may be used internally even if not shown as ScalarIndexQuery
        assert!(
            plan.contains("ScalarIndexQuery"),
            "Query plan should show optimized read. Plan: {}",
            plan
        );

        // Also test that the query returns results
        let results = dataset
            .scan()
            .filter("data.`value.v1` = 42")
            .unwrap()
            .prefilter(true)
            .try_into_batch()
            .await
            .unwrap();

        assert!(results.num_rows() > 0);
    }

    #[tokio::test]
    async fn test_bitmap_index_on_nested_field_with_dots() {
        // Test creating Bitmap index on nested field with dots in the name
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        // Create schema with nested field containing dots - using low cardinality for bitmap
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "metadata",
                DataType::Struct(
                    vec![
                        Field::new("status.code", DataType::Int32, false),
                        Field::new("category.name", DataType::Utf8, false),
                    ]
                    .into(),
                ),
                false,
            ),
        ]));

        // Generate test data with low cardinality (good for bitmap index)
        let num_rows = 1000;
        let ids = Int32Array::from_iter_values(0..num_rows);
        // Only 10 unique status codes
        let status_codes = Int32Array::from_iter_values((0..num_rows).map(|i| i % 10));
        // Only 5 unique categories
        let categories =
            StringArray::from_iter_values((0..num_rows).map(|i| format!("category_{}", i % 5)));

        let struct_array = arrow_array::StructArray::from(vec![
            (
                Arc::new(Field::new("status.code", DataType::Int32, false)),
                Arc::new(status_codes) as Arc<dyn arrow_array::Array>,
            ),
            (
                Arc::new(Field::new("category.name", DataType::Utf8, false)),
                Arc::new(categories) as Arc<dyn arrow_array::Array>,
            ),
        ]);

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(struct_array)])
                .unwrap();

        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        // Create Bitmap index on nested field with dots
        let nested_column_path = "metadata.`status.code`";
        let params = ScalarIndexParams::default();

        dataset
            .create_index(
                &[nested_column_path],
                IndexType::Bitmap,
                Some("bitmap_status_idx".to_string()),
                &params,
                true,
            )
            .await
            .unwrap();

        // Reload dataset to ensure index is loaded
        dataset = Dataset::open(test_uri).await.unwrap();

        // Verify index was created
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0].name, "bitmap_status_idx");

        // Verify the correct field was indexed
        let field_id = indices[0].fields[0];
        let field_path = dataset.schema().field_path(field_id).unwrap();
        assert_eq!(field_path, "metadata.`status.code`");

        // Test querying with the index and verify it's being used
        let plan = dataset
            .scan()
            .filter("metadata.`status.code` = 5")
            .unwrap()
            .explain_plan(false)
            .await
            .unwrap();

        // Verify the query plan (scalar indices on nested fields may use optimized filters)
        // The index may be used internally even if not shown as ScalarIndexQuery
        assert!(
            plan.contains("ScalarIndexQuery"),
            "Query plan should show optimized read. Plan: {}",
            plan
        );

        // Also test that the query returns results
        let results = dataset
            .scan()
            .filter("metadata.`status.code` = 5")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        // Should have ~100 rows with status code 5
        assert!(results.num_rows() > 0);
        assert_eq!(results.num_rows(), 100);
    }

    #[tokio::test]
    async fn test_inverted_index_on_nested_field_with_dots() {
        use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;

        // Test creating Inverted index on nested text field with dots in the name
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        // Create schema with nested text field containing dots
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "document",
                DataType::Struct(
                    vec![
                        Field::new("content.text", DataType::Utf8, false),
                        Field::new("content.summary", DataType::Utf8, false),
                    ]
                    .into(),
                ),
                false,
            ),
        ]));

        // Generate test data with text content
        let num_rows = 100;
        let ids = Int32Array::from_iter_values(0..num_rows as i32);
        let content_texts = StringArray::from_iter_values((0..num_rows).map(|i| match i % 3 {
            0 => format!("The quick brown fox jumps over the lazy dog {}", i),
            1 => format!(
                "Machine learning and artificial intelligence document {}",
                i
            ),
            _ => format!("Data science and analytics content piece {}", i),
        }));
        let summaries = StringArray::from_iter_values(
            (0..num_rows).map(|i| format!("Summary of document {}", i)),
        );

        let struct_array = arrow_array::StructArray::from(vec![
            (
                Arc::new(Field::new("content.text", DataType::Utf8, false)),
                Arc::new(content_texts) as Arc<dyn arrow_array::Array>,
            ),
            (
                Arc::new(Field::new("content.summary", DataType::Utf8, false)),
                Arc::new(summaries) as Arc<dyn arrow_array::Array>,
            ),
        ]);

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(struct_array)])
                .unwrap();

        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        // Create Inverted index on nested text field with dots
        let nested_column_path = "document.`content.text`";
        let params = InvertedIndexParams::default();

        dataset
            .create_index(
                &[nested_column_path],
                IndexType::Inverted,
                Some("inverted_content_idx".to_string()),
                &params,
                true,
            )
            .await
            .unwrap();

        // Reload the dataset to ensure the index is loaded
        dataset = Dataset::open(test_uri).await.unwrap();

        // Verify index was created
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0].name, "inverted_content_idx");

        // Verify the correct field was indexed
        let field_id = indices[0].fields[0];
        let field_path = dataset.schema().field_path(field_id).unwrap();
        assert_eq!(field_path, "document.`content.text`");

        // Test full-text search on the nested field with dots
        // Use the field_path that the index reports
        let query = FullTextSearchQuery::new("machine learning".to_string())
            .with_column(field_path.clone())
            .unwrap();

        // Check the query plan uses the inverted index
        let plan = dataset
            .scan()
            .full_text_search(query.clone())
            .unwrap()
            .explain_plan(false)
            .await
            .unwrap();

        // Verify the inverted index is being used
        assert!(
            plan.contains("MatchQuery") || plan.contains("PhraseQuery"),
            "Query plan should use inverted index for nested field with dots. Plan: {}",
            plan
        );

        let results = dataset
            .scan()
            .full_text_search(query)
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        // Verify we get results from the full-text search
        assert!(
            !results.is_empty(),
            "Full-text search should return results"
        );

        // Check that we found documents containing "machine learning"
        let mut found_count = 0;
        for batch in results {
            found_count += batch.num_rows();
        }
        // We expect to find approximately 1/3 of documents (those with i % 3 == 1)
        assert!(
            found_count > 0,
            "Should find at least some documents with 'machine learning'"
        );
        assert!(found_count < num_rows, "Should not match all documents");
    }

    #[tokio::test]
    async fn test_resolve_index_column() {
        use lance_datagen::{BatchCount, RowCount, array};

        // Create a test dataset with a vector column
        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();

        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .col(
                "vector",
                array::rand_vec::<arrow_array::types::Float32Type>(32.into()),
            )
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));

        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        // Create an index with a custom name
        let params = crate::index::vector::VectorIndexParams::ivf_flat(
            4,
            lance_linalg::distance::MetricType::L2,
        );
        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("my_vector_index".to_string()),
                &params,
                false,
            )
            .await
            .unwrap();

        // Reload dataset to get the index metadata
        let dataset = Dataset::open(test_uri).await.unwrap();
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        let index_meta = &indices[0];

        // Test 1: Pass the actual column name
        let (field_path, field) =
            resolve_index_column(dataset.schema(), index_meta, "vector").unwrap();
        assert_eq!(field_path, "vector");
        assert_eq!(field.name, "vector");

        // Test 2: Pass the index name (should resolve to the actual column)
        let (field_path2, field2) =
            resolve_index_column(dataset.schema(), index_meta, "my_vector_index").unwrap();
        assert_eq!(field_path2, "vector");
        assert_eq!(field2.name, "vector");

        // Test 3: Pass a non-existent column name (should fail)
        let result = resolve_index_column(dataset.schema(), index_meta, "nonexistent");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("does not exist in the schema")
        );
    }

    #[tokio::test]
    async fn test_commit_existing_index_segments_commits_multiple_segments() {
        use lance_datagen::{BatchCount, RowCount, array};

        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();

        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .col(
                "vector",
                array::rand_vec::<arrow_array::types::Float32Type>(8.into()),
            )
            .into_reader_rows(RowCount::from(20), BatchCount::from(2));

        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 10,
                max_rows_per_group: 10,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let field_id = dataset.schema().field("vector").unwrap().id;
        let seg0 = write_vector_segment_metadata(
            &dataset,
            "vector_idx",
            field_id,
            Uuid::new_v4(),
            [0_u32],
            b"seg0",
        )
        .await;
        let seg1 = write_vector_segment_metadata(
            &dataset,
            "vector_idx",
            field_id,
            Uuid::new_v4(),
            [1_u32],
            b"seg1",
        )
        .await;

        dataset
            .commit_existing_index_segments(
                "vector_idx",
                "vector",
                vec![segment_from_metadata(&seg0), segment_from_metadata(&seg1)],
            )
            .await
            .unwrap();

        let committed = dataset.load_indices_by_name("vector_idx").await.unwrap();
        assert_eq!(committed.len(), 2);
        let committed_uuids = committed.iter().map(|idx| idx.uuid).collect::<HashSet<_>>();
        assert_eq!(
            committed_uuids,
            HashSet::from([seg0.uuid, seg1.uuid]),
            "all committed segment uuids should be preserved"
        );
        assert_eq!(
            committed
                .iter()
                .map(|idx| idx
                    .fragment_bitmap
                    .as_ref()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>())
                .collect::<HashSet<_>>(),
            HashSet::from([vec![0], vec![1]]),
            "each committed segment should preserve its fragment coverage"
        );
        assert!(
            committed
                .iter()
                .all(|idx| idx.files.as_ref().is_some_and(|files| !files.is_empty())),
            "committed segment metadata should capture on-disk file info"
        );
    }

    #[tokio::test]
    async fn test_commit_existing_index_segments_rejects_duplicate_segment_ids() {
        use lance_datagen::{BatchCount, RowCount, array};

        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();

        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .col(
                "vector",
                array::rand_vec::<arrow_array::types::Float32Type>(8.into()),
            )
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));

        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        let field_id = dataset.schema().field("vector").unwrap().id;
        let base = write_vector_segment_metadata(
            &dataset,
            "vector_idx",
            field_id,
            Uuid::new_v4(),
            [0_u32],
            b"base",
        )
        .await;

        let err = dataset
            .commit_existing_index_segments(
                "vector_idx",
                "vector",
                vec![
                    segment_from_metadata(&base),
                    segment_from_metadata(&IndexMetadata {
                        fragment_bitmap: Some(std::iter::once(1_u32).collect()),
                        ..base
                    }),
                ],
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("duplicate segment uuid"));
    }

    #[tokio::test]
    async fn test_commit_existing_index_segments_rejects_empty_segments() {
        use lance_datagen::{BatchCount, RowCount, array};

        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();

        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .col(
                "vector",
                array::rand_vec::<arrow_array::types::Float32Type>(8.into()),
            )
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));

        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        let err = dataset
            .commit_existing_index_segments("vector_idx", "vector", Vec::<IndexSegment>::new())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("at least one index segment"));
    }

    #[tokio::test]
    async fn test_commit_existing_index_segments_rejects_wrong_field_provenance() {
        use lance_datagen::{BatchCount, RowCount, array};

        let test_dir = tempfile::tempdir().unwrap();
        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .col(
                "vector",
                array::rand_vec::<arrow_array::types::Float32Type>(8.into()),
            )
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut dataset = Dataset::write(reader, test_dir.path().to_str().unwrap(), None)
            .await
            .unwrap();

        let vector_field_id = dataset.schema().field("vector").unwrap().id;
        let mut metadata = write_vector_segment_metadata(
            &dataset,
            "vector_idx",
            vector_field_id,
            Uuid::new_v4(),
            [0_u32],
            b"segment",
        )
        .await;
        metadata.fields = vec![dataset.schema().field("id").unwrap().id];

        let error = dataset
            .commit_existing_index_segments(
                "vector_idx",
                "vector",
                vec![segment_from_metadata(&metadata)],
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("was built for fields"));
    }

    #[tokio::test]
    async fn test_commit_existing_index_segments_rejects_future_dataset_version() {
        use lance_datagen::{BatchCount, RowCount, array};

        let test_dir = tempfile::tempdir().unwrap();
        let reader = lance_datagen::gen_batch()
            .col(
                "vector",
                array::rand_vec::<arrow_array::types::Float32Type>(8.into()),
            )
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut dataset = Dataset::write(reader, test_dir.path().to_str().unwrap(), None)
            .await
            .unwrap();

        let field_id = dataset.schema().field("vector").unwrap().id;
        let mut metadata = write_vector_segment_metadata(
            &dataset,
            "vector_idx",
            field_id,
            Uuid::new_v4(),
            [0_u32],
            b"segment",
        )
        .await;
        metadata.dataset_version = dataset.manifest.version + 1;

        let error = dataset
            .commit_existing_index_segments(
                "vector_idx",
                "vector",
                vec![segment_from_metadata(&metadata)],
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("future dataset version"));
    }

    #[tokio::test]
    async fn test_commit_existing_index_segments_rejects_overlapping_fragment_coverage() {
        use lance_datagen::{BatchCount, RowCount, array};

        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();

        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .col(
                "vector",
                array::rand_vec::<arrow_array::types::Float32Type>(8.into()),
            )
            .into_reader_rows(RowCount::from(20), BatchCount::from(2));

        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        let field_id = dataset.schema().field("vector").unwrap().id;
        let seg0 = write_vector_segment_metadata(
            &dataset,
            "vector_idx",
            field_id,
            Uuid::new_v4(),
            [0_u32, 1_u32],
            b"seg0",
        )
        .await;
        let seg1 = write_vector_segment_metadata(
            &dataset,
            "vector_idx",
            field_id,
            Uuid::new_v4(),
            [1_u32],
            b"seg1",
        )
        .await;

        let err = dataset
            .commit_existing_index_segments(
                "vector_idx",
                "vector",
                vec![segment_from_metadata(&seg0), segment_from_metadata(&seg1)],
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("overlapping fragment coverage"));
    }

    #[tokio::test]
    async fn test_commit_existing_index_segments_rejects_mixed_index_detail_types() {
        use lance_datagen::{BatchCount, RowCount, array};

        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();

        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .col(
                "vector",
                array::rand_vec::<arrow_array::types::Float32Type>(8.into()),
            )
            .into_reader_rows(RowCount::from(20), BatchCount::from(2));

        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        let field_id = dataset.schema().field("vector").unwrap().id;
        let seg0 = write_vector_segment_metadata(
            &dataset,
            "vector_idx",
            field_id,
            Uuid::new_v4(),
            [0_u32],
            b"seg0",
        )
        .await;
        let seg1 = IndexMetadata {
            uuid: Uuid::new_v4(),
            name: "vector_idx".to_string(),
            fields: vec![field_id],
            dataset_version: dataset.manifest.version,
            fragment_bitmap: Some(std::iter::once(1_u32).collect()),
            index_details: Some(Arc::new(
                prost_types::Any::from_msg(&BTreeIndexDetails::default()).unwrap(),
            )),
            index_version: IndexType::BTree.version(),
            created_at: Some(chrono::Utc::now()),
            base_id: None,
            files: seg0.files.clone(),
        };

        let err = dataset
            .commit_existing_index_segments("vector_idx", "vector", vec![seg0, seg1])
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("mixes incompatible index detail types")
        );
    }

    #[tokio::test]
    async fn test_commit_existing_index_segments_rejects_partial_replacement_of_wider_segment() {
        use lance_datagen::{BatchCount, RowCount, array};

        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();

        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .col(
                "vector",
                array::rand_vec::<arrow_array::types::Float32Type>(8.into()),
            )
            .into_reader_rows(RowCount::from(20), BatchCount::from(2));

        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 20,
                max_rows_per_group: 20,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(dataset.get_fragments().len(), 2);

        let field_id = dataset.schema().field("vector").unwrap().id;
        let original = write_vector_segment_metadata(
            &dataset,
            "vector_idx",
            field_id,
            Uuid::new_v4(),
            [0_u32, 1_u32],
            b"original",
        )
        .await;
        dataset
            .commit_existing_index_segments("vector_idx", "vector", vec![original])
            .await
            .unwrap();

        let replacement = write_vector_segment_metadata(
            &dataset,
            "vector_idx",
            field_id,
            Uuid::new_v4(),
            [0_u32],
            b"replacement",
        )
        .await;

        let err = dataset
            .commit_existing_index_segments("vector_idx", "vector", vec![replacement])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("would orphan fragments"));
    }

    #[tokio::test]
    async fn test_commit_existing_index_segments_replaces_different_index_type() {
        use lance_datagen::{BatchCount, RowCount, array};

        let test_dir = tempfile::tempdir().unwrap();
        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut dataset = Dataset::write(reader, test_dir.path().to_str().unwrap(), None)
            .await
            .unwrap();

        let index_name = "shared_name";
        let btree_params = ScalarIndexParams::for_builtin(BuiltinIndexType::BTree);
        let original = dataset
            .create_index_builder(&["id"], IndexType::BTree, &btree_params)
            .name("staged_btree".to_string())
            .execute_uncommitted()
            .await
            .unwrap();
        dataset
            .commit_existing_index_segments(index_name, "id", vec![original])
            .await
            .unwrap();

        let bitmap_params = ScalarIndexParams::for_builtin(BuiltinIndexType::Bitmap);
        let replacement = dataset
            .create_index_builder(&["id"], IndexType::Bitmap, &bitmap_params)
            .name("staged_bitmap".to_string())
            .execute_uncommitted()
            .await
            .unwrap();
        let replacement_uuid = replacement.uuid;
        let replacement_type_url = replacement.index_details.as_ref().unwrap().type_url.clone();

        dataset
            .commit_existing_index_segments(index_name, "id", vec![replacement])
            .await
            .unwrap();

        let committed = dataset
            .load_index_by_name(index_name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(committed.uuid, replacement_uuid);
        assert_eq!(
            committed.index_details.unwrap().type_url,
            replacement_type_url
        );
    }

    #[tokio::test]
    async fn test_commit_existing_index_segments_rejects_partial_index_type_change() {
        use lance_datagen::{BatchCount, RowCount, array};

        let test_dir = tempfile::tempdir().unwrap();
        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .into_reader_rows(RowCount::from(20), BatchCount::from(2));
        let mut dataset = Dataset::write(
            reader,
            test_dir.path().to_str().unwrap(),
            Some(WriteParams {
                max_rows_per_file: 20,
                max_rows_per_group: 20,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let index_name = "shared_name";
        let fragments = dataset.get_fragments();
        assert_eq!(fragments.len(), 2);
        let btree_params = ScalarIndexParams::for_builtin(BuiltinIndexType::BTree);
        let mut original_segments = Vec::with_capacity(fragments.len());
        for fragment in &fragments {
            original_segments.push(
                dataset
                    .create_index_builder(&["id"], IndexType::BTree, &btree_params)
                    .fragments(vec![fragment.id() as u32])
                    .execute_uncommitted()
                    .await
                    .unwrap(),
            );
        }
        dataset
            .commit_existing_index_segments(index_name, "id", original_segments)
            .await
            .unwrap();
        let committed_version = dataset.manifest.version;

        let bitmap_params = ScalarIndexParams::for_builtin(BuiltinIndexType::Bitmap);
        let replacement = dataset
            .create_index_builder(&["id"], IndexType::Bitmap, &bitmap_params)
            .fragments(vec![fragments[0].id() as u32])
            .execute_uncommitted()
            .await
            .unwrap();
        let error = dataset
            .commit_existing_index_segments(index_name, "id", vec![replacement])
            .await
            .unwrap_err();

        assert!(
            matches!(error, Error::InvalidInput { .. }),
            "expected invalid input error, got: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("cannot change index 'shared_name'")
        );
        assert!(error.to_string().contains("missing current fragments [1]"));
        assert_eq!(dataset.manifest.version, committed_version);

        let committed =
            crate::index::scalar_logical::load_named_scalar_segments(&dataset, "id", index_name)
                .await
                .unwrap();
        assert_eq!(committed.len(), 2);
        assert!(committed.iter().all(|segment| {
            segment
                .index_details
                .as_ref()
                .is_some_and(|details| details.type_url.ends_with("BTreeIndexDetails"))
        }));
    }

    #[tokio::test]
    async fn test_partial_type_change_with_legacy_missing_details_is_rejected() {
        use lance_datagen::{BatchCount, RowCount, array};

        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();
        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        let index_name = "shared_name";
        let btree_params = ScalarIndexParams::for_builtin(BuiltinIndexType::BTree);
        let original = dataset
            .create_index_builder(&["id"], IndexType::BTree, &btree_params)
            .execute_uncommitted()
            .await
            .unwrap();
        dataset
            .commit_existing_index_segments(index_name, "id", vec![original])
            .await
            .unwrap();

        let current = dataset.load_indices_by_name(index_name).await.unwrap();
        assert_eq!(current.len(), 1);
        let original_uuid = current[0].uuid;
        let mut legacy = current.clone();
        legacy[0].index_details = None;
        legacy[0].index_version = 0;
        let transaction = Transaction::new(
            dataset.manifest.version,
            Operation::CreateIndex {
                new_indices: legacy,
                removed_indices: current,
            },
            None,
        );
        dataset
            .apply_commit(transaction, &Default::default(), &Default::default())
            .await
            .unwrap();

        let append_reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut dataset = Dataset::write(
            append_reader,
            test_uri,
            Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let fragments = dataset.get_fragments();
        assert_eq!(fragments.len(), 2);

        let bitmap_params = ScalarIndexParams::for_builtin(BuiltinIndexType::Bitmap);
        let replacement = dataset
            .create_index_builder(&["id"], IndexType::Bitmap, &bitmap_params)
            .fragments(vec![fragments[1].id() as u32])
            .execute_uncommitted()
            .await
            .unwrap();
        let version_before = dataset.manifest.version;
        let error = dataset
            .commit_existing_index_segments(index_name, "id", vec![replacement])
            .await
            .unwrap_err();

        assert!(
            matches!(error, Error::InvalidInput { .. }),
            "expected invalid input error, got: {error}"
        );
        assert!(error.to_string().contains("from type '<unknown>'"));
        assert!(error.to_string().contains("partial fragment coverage"));
        assert!(error.to_string().contains("missing current fragments [0]"));
        assert_eq!(dataset.manifest.version, version_before);

        let committed = dataset.load_indices_by_name(index_name).await.unwrap();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].uuid, original_uuid);
        assert!(committed[0].index_details.is_none());
        assert_eq!(committed[0].index_version, 0);

        let field_id = dataset.schema().field("id").unwrap().id;
        let sentinel_segment = IndexSegment::new(
            Uuid::new_v4(),
            [fragments[1].id() as u32],
            [field_id],
            Arc::new(prost_types::Any {
                type_url: "<unknown>".to_string(),
                value: Vec::new(),
            }),
            0,
            dataset.manifest.version,
        );
        let error = dataset
            .commit_existing_index_segments(index_name, "id", vec![sentinel_segment])
            .await
            .unwrap_err();

        assert!(
            matches!(error, Error::InvalidInput { .. }),
            "expected invalid input error, got: {error}"
        );
        assert!(error.to_string().contains("from type '<unknown>'"));
        assert!(error.to_string().contains("to type '<unknown>'"));
        assert!(error.to_string().contains("missing current fragments [0]"));
        assert_eq!(dataset.manifest.version, version_before);

        let committed = dataset.load_indices_by_name(index_name).await.unwrap();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].uuid, original_uuid);
        assert!(committed[0].index_details.is_none());
        assert_eq!(committed[0].index_version, 0);
    }

    #[tokio::test]
    async fn test_commit_existing_index_segments_removes_empty_segment() {
        use lance_datagen::{BatchCount, RowCount, array};

        let test_dir = tempfile::tempdir().unwrap();
        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .col(
                "vector",
                array::rand_vec::<arrow_array::types::Float32Type>(8.into()),
            )
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut dataset = Dataset::write(reader, test_dir.path().to_str().unwrap(), None)
            .await
            .unwrap();
        let field_id = dataset.schema().field("vector").unwrap().id;
        let uuid = Uuid::new_v4();

        // Commit a 0-fragment segment, then a real segment covering the dataset.
        let empty = write_vector_segment_metadata(
            &dataset,
            "vector_idx",
            field_id,
            uuid,
            std::iter::empty::<u32>(),
            b"empty",
        )
        .await;
        dataset
            .commit_existing_index_segments(
                "vector_idx",
                "vector",
                vec![segment_from_metadata(&empty)],
            )
            .await
            .unwrap();
        let seg = write_vector_segment_metadata(
            &dataset,
            "vector_idx",
            field_id,
            Uuid::new_v4(),
            [0_u32],
            b"seg",
        )
        .await;
        dataset
            .commit_existing_index_segments(
                "vector_idx",
                "vector",
                vec![segment_from_metadata(&seg)],
            )
            .await
            .unwrap();

        // The real segment covers the dataset, so the redundant empty one is removed.
        let committed = dataset.load_indices_by_name("vector_idx").await.unwrap();
        assert_eq!(
            committed.iter().map(|i| i.uuid).collect::<HashSet<_>>(),
            HashSet::from([seg.uuid]),
            "empty segment should be removed once a real segment covers the dataset",
        );
    }

    #[tokio::test]
    async fn test_resolve_index_column_error_cases() {
        use lance_datagen::{BatchCount, RowCount, array};

        // Create a test dataset
        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();

        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .col(
                "vector",
                array::rand_vec::<arrow_array::types::Float32Type>(32.into()),
            )
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));

        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        // Create an index
        let params = crate::index::vector::VectorIndexParams::ivf_flat(
            4,
            lance_linalg::distance::MetricType::L2,
        );
        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("my_index".to_string()),
                &params,
                false,
            )
            .await
            .unwrap();

        // Reload dataset
        let dataset = Dataset::open(test_uri).await.unwrap();
        let indices = dataset.load_indices().await.unwrap();
        let index_meta = &indices[0];

        // Test: Pass a column that doesn't exist and is not the index name
        let result = resolve_index_column(dataset.schema(), index_meta, "nonexistent_column");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("does not exist in the schema"),
            "Error message should mention column doesn't exist, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_resolve_index_column_nested_field() {
        use arrow_array::{RecordBatch, StructArray};
        use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};

        // Create a test dataset with nested struct manually
        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();

        // Create schema with nested structure: data.vector
        let vector_field = ArrowField::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(ArrowField::new("item", DataType::Float32, true)),
                8,
            ),
            false,
        );
        let struct_field = ArrowField::new(
            "data",
            DataType::Struct(vec![vector_field.clone()].into()),
            false,
        );
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, false),
            struct_field,
        ]));

        // Create data
        let id_array = arrow_array::Int32Array::from(vec![1, 2, 3, 4, 5]);

        // Create nested vector data
        let mut vector_values = Vec::new();
        for _ in 0..5 {
            for _ in 0..8 {
                vector_values.push(rand::random::<f32>());
            }
        }
        let vector_array = arrow_array::FixedSizeListArray::try_new_from_values(
            arrow_array::Float32Array::from(vector_values),
            8,
        )
        .unwrap();

        let struct_array = StructArray::from(vec![(
            Arc::new(vector_field),
            Arc::new(vector_array) as arrow_array::ArrayRef,
        )]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(struct_array)],
        )
        .unwrap();

        let reader = Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(batch)],
            schema,
        ));

        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();

        // Create an index on the nested field
        let params = crate::index::vector::VectorIndexParams::ivf_flat(
            2,
            lance_linalg::distance::MetricType::L2,
        );
        dataset
            .create_index(
                &["data.vector"],
                IndexType::Vector,
                Some("nested_vector_index".to_string()),
                &params,
                false,
            )
            .await
            .unwrap();

        // Reload dataset to get the index metadata
        let dataset = Dataset::open(test_uri).await.unwrap();
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        let index_meta = &indices[0];

        // Test 1: Pass the nested field path directly
        let (field_path, field) =
            resolve_index_column(dataset.schema(), index_meta, "data.vector").unwrap();
        assert_eq!(field_path, "data.vector");
        assert_eq!(field.name, "vector");

        // Test 2: Pass the index name, should resolve to the nested field path
        let (field_path2, field2) =
            resolve_index_column(dataset.schema(), index_meta, "nested_vector_index").unwrap();
        assert_eq!(field_path2, "data.vector");
        assert_eq!(field2.name, "vector");

        // Verify the field path is correct for nested access
        assert!(
            field_path2.contains('.'),
            "Field path should contain '.' for nested field"
        );
    }

    #[tokio::test]
    async fn test_scalar_index_file_sizes_captured() {
        // Test that file sizes are captured when creating a scalar index
        let reader = gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .col("values", array::rand_utf8(ByteCount::from(10), false))
            .into_reader_rows(RowCount::from(4), BatchCount::from(1));

        let mut dataset = Dataset::write(reader, "memory://", None).await.unwrap();

        // Create a scalar index
        dataset
            .create_index(
                &["values"],
                IndexType::Scalar,
                Some("test_idx".to_string()),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        // Get index metadata and verify files are populated
        let indices = dataset.load_indices().await.unwrap();
        let test_index = indices.iter().find(|idx| idx.name == "test_idx").unwrap();

        assert!(
            test_index.files.is_some(),
            "Index should have files populated"
        );
        let files = test_index.files.as_ref().unwrap();
        assert!(!files.is_empty(), "Index should have at least one file");

        // Verify each file has a positive size
        for file in files {
            assert!(
                file.size_bytes > 0,
                "File {} should have positive size",
                file.path
            );
        }

        // Verify total_size_bytes works
        let total_size = test_index.total_size_bytes();
        assert!(total_size.is_some(), "total_size_bytes should return Some");
        assert!(total_size.unwrap() > 0, "Total size should be positive");
    }

    #[tokio::test]
    async fn test_vector_index_file_sizes_captured() {
        // Test that file sizes are captured when creating a vector index
        let reader = gen_batch()
            .col("id", array::step::<arrow_array::types::Int32Type>())
            .col(
                "vector",
                array::rand_vec::<arrow_array::types::Float32Type>(4.into()),
            )
            .into_reader_rows(RowCount::from(300), BatchCount::from(1));

        let mut dataset = Dataset::write(reader, "memory://", None).await.unwrap();

        // Create vector index
        let params = VectorIndexParams::ivf_pq(1, 8, 2, MetricType::L2, 2);
        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("test_vec_idx".to_string()),
                &params,
                false,
            )
            .await
            .unwrap();

        // Get index metadata and verify files are populated
        let indices = dataset.load_indices().await.unwrap();
        let test_index = indices
            .iter()
            .find(|idx| idx.name == "test_vec_idx")
            .unwrap();

        assert!(
            test_index.files.is_some(),
            "Index should have files populated"
        );
        let files = test_index.files.as_ref().unwrap();
        assert!(!files.is_empty(), "Index should have at least one file");

        // Verify each file has a positive size
        for file in files {
            assert!(
                file.size_bytes > 0,
                "File {} should have positive size",
                file.path
            );
        }

        // Verify total_size_bytes works
        let total_size = test_index.total_size_bytes();
        assert!(total_size.is_some(), "total_size_bytes should return Some");
        assert!(total_size.unwrap() > 0, "Total size should be positive");
    }

    #[tokio::test]
    async fn test_describe_indices_total_size() {
        // Test that describe_indices returns total_size_bytes
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("values", DataType::Utf8, false),
        ]));

        let values = StringArray::from_iter_values(["hello", "world", "foo", "bar"]);
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..4)),
                Arc::new(values),
            ],
        )
        .unwrap();

        let reader =
            RecordBatchIterator::new(vec![record_batch].into_iter().map(Ok), schema.clone());

        let mut dataset = Dataset::write(reader, "memory://", None).await.unwrap();

        // Create a scalar index
        dataset
            .create_index(
                &["values"],
                IndexType::Scalar,
                Some("test_idx".to_string()),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        // Use describe_indices to get index info
        let descriptions = dataset.describe_indices(None).await.unwrap();
        assert_eq!(descriptions.len(), 1);

        let desc = &descriptions[0];
        assert_eq!(desc.name(), "test_idx");
        assert_eq!(desc.rows_indexed(), 4);

        // Verify total_size_bytes is available
        let total_size = desc.total_size_bytes();
        assert!(total_size.is_some(), "total_size_bytes should be Some");
        assert!(total_size.unwrap() > 0, "Total size should be positive");
    }

    #[tokio::test]
    async fn test_describe_indices_rows_indexed_multi_fragment() {
        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();

        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<Int32Type>())
            .col("values", array::rand_utf8(ByteCount::from(8), false))
            .into_reader_rows(RowCount::from(10), BatchCount::from(3));

        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 10,
                max_rows_per_group: 10,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(dataset.fragments().len(), 3);

        dataset
            .create_index(
                &["values"],
                IndexType::Scalar,
                Some("multi_frag_idx".to_string()),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        let descriptions = dataset.describe_indices(None).await.unwrap();
        assert_eq!(descriptions.len(), 1);
        assert_eq!(descriptions[0].name(), "multi_frag_idx");
        assert_eq!(
            descriptions[0].rows_indexed(),
            30,
            "rows_indexed should sum logical rows across all indexed fragments"
        );
    }

    #[tokio::test]
    async fn test_describe_indices_rows_indexed_with_deletions() {
        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();

        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<Int32Type>())
            .col("values", array::rand_utf8(ByteCount::from(8), false))
            .into_reader_rows(RowCount::from(20), BatchCount::from(1));

        let mut dataset = Dataset::write(reader, test_uri, None).await.unwrap();
        dataset.delete("id >= 15").await.unwrap();
        assert_eq!(dataset.count_rows(None).await.unwrap(), 15);

        dataset
            .create_index(
                &["values"],
                IndexType::Scalar,
                Some("deleted_rows_idx".to_string()),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        let descriptions = dataset.describe_indices(None).await.unwrap();
        assert_eq!(descriptions.len(), 1);
        assert_eq!(descriptions[0].name(), "deleted_rows_idx");
        assert_eq!(
            descriptions[0].rows_indexed(),
            15,
            "rows_indexed should use logical rows (physical_rows - num_deleted_rows)"
        );
    }

    #[tokio::test]
    async fn test_describe_indices_rows_indexed_stale_bitmap_fragment() {
        let test_dir = tempfile::tempdir().unwrap();
        let test_uri = test_dir.path().to_str().unwrap();

        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<Int32Type>())
            .col("vector", array::rand_vec::<Float32Type>(8.into()))
            .into_reader_rows(RowCount::from(10), BatchCount::from(2));

        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 10,
                max_rows_per_group: 10,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(dataset.fragments().len(), 2);

        let field_id = dataset.schema().field("vector").unwrap().id;
        let stale_segment = write_vector_segment_metadata(
            &dataset,
            "vector_idx",
            field_id,
            Uuid::new_v4(),
            [0_u32, 1_u32, 999_u32],
            b"stale-bitmap-segment",
        )
        .await;

        dataset
            .commit_existing_index_segments(
                "vector_idx",
                "vector",
                vec![segment_from_metadata(&stale_segment)],
            )
            .await
            .unwrap();

        let descriptions = dataset.describe_indices(None).await.unwrap();
        assert_eq!(descriptions.len(), 1);
        assert_eq!(descriptions[0].name(), "vector_idx");
        assert_eq!(
            descriptions[0].rows_indexed(),
            20,
            "stale bitmap entries for missing fragments should be skipped without failing"
        );
    }

    /// Helper to assert that all indices have file sizes populated
    async fn assert_all_indices_have_files(dataset: &Dataset, context: &str) {
        let indices = dataset.load_indices().await.unwrap();
        for index in indices.iter() {
            // Skip system indices (mem_wal, frag_reuse) which don't have files
            if index.name == lance_index::mem_wal::MEM_WAL_INDEX_NAME
                || index.name == lance_index::frag_reuse::FRAG_REUSE_INDEX_NAME
            {
                continue;
            }
            assert!(
                index.files.is_some(),
                "{}: Index '{}' should have files field populated",
                context,
                index.name
            );
            let files = index.files.as_ref().unwrap();
            assert!(
                !files.is_empty(),
                "{}: Index '{}' should have at least one file",
                context,
                index.name
            );
            for file in files {
                assert!(
                    file.size_bytes > 0,
                    "{}: Index '{}' file '{}' should have positive size",
                    context,
                    index.name,
                    file.path
                );
            }
        }
    }

    #[tokio::test]
    async fn test_scalar_index_create_does_not_list_files() {
        let test_dir = TempStrDir::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Int32, false),
        ]));
        let ids = Int32Array::from_iter_values(0..128);
        let categories = Int32Array::from_iter_values((0..128).map(|value| value % 8));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(categories)])
            .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let mut dataset = Dataset::write(reader, test_dir.as_str(), None)
            .await
            .unwrap();
        let io_tracker = dataset.object_store.as_ref().io_tracker().clone();

        io_tracker.incremental_stats();
        dataset
            .create_index(
                &["category"],
                IndexType::Bitmap,
                Some("category_bitmap".to_string()),
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();

        let stats = io_tracker.incremental_stats();
        let list_stats = list_io_stats(&stats);
        assert_io_eq!(
            list_stats,
            read_iops,
            0,
            "new scalar index files should be reported by writer return values"
        );
    }

    #[tokio::test]
    async fn test_vector_index_create_does_not_list_files() {
        let test_dir = TempStrDir::default();
        let dimension = 8;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dimension,
                ),
                false,
            ),
        ]));
        let ids = Int32Array::from_iter_values(0..256);
        let vectors = (0..256)
            .map(|row| {
                Some(
                    (0..dimension)
                        .map(|dim| Some((row * dimension + dim) as f32))
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let vector_array =
            FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(vectors, dimension);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(vector_array)])
                .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let mut dataset = Dataset::write(reader, test_dir.as_str(), None)
            .await
            .unwrap();
        let io_tracker = dataset.object_store.as_ref().io_tracker().clone();

        io_tracker.incremental_stats();
        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("vector_ivf_flat".to_string()),
                &VectorIndexParams::ivf_flat(4, MetricType::L2),
                true,
            )
            .await
            .unwrap();

        let stats = io_tracker.incremental_stats();
        let list_stats = list_io_stats(&stats);
        assert_io_eq!(
            list_stats,
            read_iops,
            0,
            "new V3 vector index files should be reported by builder return values"
        );
    }

    #[tokio::test]
    async fn test_index_file_sizes_through_lifecycle() {
        use crate::dataset::WriteDestination;
        use crate::dataset::optimize::{CompactionOptions, compact_files, remapping};
        use lance_index::frag_reuse::FRAG_REUSE_INDEX_NAME;

        // Create initial dataset with columns for different index types
        let data = gen_batch()
            .col("int_col", array::step::<Int32Type>())
            .col("str_col", array::rand_utf8(8.into(), false))
            .col(
                "vec_col",
                array::rand_vec::<Float32Type>(Dimension::from(32)),
            )
            .into_reader_rows(RowCount::from(1000), BatchCount::from(1));

        let test_dir = TempStrDir::default();
        let mut dataset = Dataset::write(
            data,
            test_dir.as_str(),
            Some(WriteParams {
                max_rows_per_file: 200, // Multiple fragments for compaction
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Create BTree index
        dataset
            .create_index(
                &["int_col"],
                IndexType::BTree,
                Some("btree_idx".to_string()),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        // Create Bitmap index
        dataset
            .create_index(
                &["int_col"],
                IndexType::Bitmap,
                Some("bitmap_idx".to_string()),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        // Create Inverted index for text search
        dataset
            .create_index(
                &["str_col"],
                IndexType::Inverted,
                Some("inverted_idx".to_string()),
                &InvertedIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        // Validate files are populated after creation
        assert_all_indices_have_files(&dataset, "after initial creation").await;

        // Append more data
        let more_data = gen_batch()
            .col("int_col", array::step::<Int32Type>())
            .col("str_col", array::rand_utf8(8.into(), false))
            .col(
                "vec_col",
                array::rand_vec::<Float32Type>(Dimension::from(32)),
            )
            .into_reader_rows(RowCount::from(500), BatchCount::from(1));

        Dataset::write(
            more_data,
            WriteDestination::Dataset(Arc::new(dataset.clone())),
            Some(WriteParams {
                max_rows_per_file: 200,
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        dataset = DatasetBuilder::from_uri(test_dir.as_str())
            .load()
            .await
            .unwrap();

        // Optimize indices (triggers update/merge)
        dataset
            .optimize_indices(&OptimizeOptions::default())
            .await
            .unwrap();

        // Validate files are still populated after optimize
        assert_all_indices_have_files(&dataset, "after optimize_indices").await;

        // Run compaction with deferred remap
        let options = CompactionOptions {
            target_rows_per_fragment: 500,
            defer_index_remap: true,
            ..Default::default()
        };

        compact_files(&mut dataset, options.clone(), None)
            .await
            .unwrap();

        // Check if frag reuse index exists (indicates remap is needed)
        if dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
            .is_some()
        {
            // Remap each index
            remapping::remap_column_index(
                &mut dataset,
                &["int_col"],
                Some("btree_idx".to_string()),
            )
            .await
            .unwrap();

            remapping::remap_column_index(
                &mut dataset,
                &["int_col"],
                Some("bitmap_idx".to_string()),
            )
            .await
            .unwrap();

            remapping::remap_column_index(
                &mut dataset,
                &["str_col"],
                Some("inverted_idx".to_string()),
            )
            .await
            .unwrap();

            // Validate files are populated after remap
            assert_all_indices_have_files(&dataset, "after remap").await;
        }
    }

    #[tokio::test]
    async fn test_btree_index_iops() {
        // Test that querying a BTree index uses minimal IOPs (no HEAD requests)
        let test_dir = TempStrDir::default();

        // Create dataset with a column suitable for BTree index
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let num_rows = 1000;
        let ids = Int32Array::from_iter_values(0..num_rows);
        let values = Int32Array::from_iter_values((0..num_rows).map(|i| i % 100));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(values)]).unwrap();

        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(reader, test_dir.as_str(), None)
            .await
            .unwrap();

        // Create BTree index
        dataset
            .create_index(
                &["value"],
                IndexType::BTree,
                Some("btree_idx".to_string()),
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();

        // Re-open dataset fresh to avoid cached state
        let dataset = DatasetBuilder::from_uri(test_dir.as_str())
            .load()
            .await
            .unwrap();

        // Reset IO stats before query
        let _ = dataset.object_store.as_ref().io_stats_incremental();

        // Query using the BTree index
        let results = dataset
            .scan()
            .filter("value = 50")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert!(results.num_rows() > 0);

        // Verify IOPs - should be minimal (no HEAD requests)
        let stats = dataset.object_store.as_ref().io_stats_incremental();
        // We expect reads for: index metadata + index pages + data files
        // The key assertion is that we don't have extra HEAD requests
        assert_io_lt!(
            stats,
            read_iops,
            10,
            "BTree index query should use minimal IOPs"
        );
    }

    #[tokio::test]
    async fn test_bitmap_index_iops() {
        // Test that querying a Bitmap index uses minimal IOPs (no HEAD requests)
        let test_dir = TempStrDir::default();

        // Create dataset with low-cardinality column for Bitmap index
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Int32, false),
        ]));

        let num_rows = 1000;
        let ids = Int32Array::from_iter_values(0..num_rows);
        // Low cardinality - only 10 unique values
        let categories = Int32Array::from_iter_values((0..num_rows).map(|i| i % 10));

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(categories)])
            .unwrap();

        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(reader, test_dir.as_str(), None)
            .await
            .unwrap();

        // Create Bitmap index
        dataset
            .create_index(
                &["category"],
                IndexType::Bitmap,
                Some("bitmap_idx".to_string()),
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();

        // Re-open dataset fresh
        let dataset = DatasetBuilder::from_uri(test_dir.as_str())
            .load()
            .await
            .unwrap();

        // Reset IO stats before query
        let _ = dataset.object_store.as_ref().io_stats_incremental();

        // Query using the Bitmap index
        let results = dataset
            .scan()
            .filter("category = 5")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert!(results.num_rows() > 0);

        // Verify IOPs
        let stats = dataset.object_store.as_ref().io_stats_incremental();
        assert_io_lt!(
            stats,
            read_iops,
            10,
            "Bitmap index query should use minimal IOPs"
        );
    }

    #[tokio::test]
    async fn test_inverted_index_iops() {
        // Test that querying an Inverted (FTS) index uses minimal IOPs
        let test_dir = TempStrDir::default();

        // Create dataset with text column for Inverted index
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, false),
        ]));

        let num_rows = 100;
        let ids = Int32Array::from_iter_values(0..num_rows);
        let texts = StringArray::from_iter_values((0..num_rows).map(|i| {
            if i % 3 == 0 {
                format!("hello world document {}", i)
            } else if i % 3 == 1 {
                format!("goodbye universe text {}", i)
            } else {
                format!("random content item {}", i)
            }
        }));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(texts)]).unwrap();

        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(reader, test_dir.as_str(), None)
            .await
            .unwrap();

        // Create Inverted index
        let params = InvertedIndexParams::default();
        dataset
            .create_index(
                &["text"],
                IndexType::Inverted,
                Some("inverted_idx".to_string()),
                &params,
                true,
            )
            .await
            .unwrap();

        // Re-open dataset fresh
        let dataset = DatasetBuilder::from_uri(test_dir.as_str())
            .load()
            .await
            .unwrap();

        // Reset IO stats before query
        let _ = dataset.object_store.as_ref().io_stats_incremental();

        // Query using the Inverted index (full-text search)
        let results = dataset
            .scan()
            .full_text_search(FullTextSearchQuery::new("hello".to_string()))
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert!(results.num_rows() > 0);

        // Verify IOPs. The deferred DocSet loads per-doc num_tokens/row_ids on
        // first use rather than eagerly at index open, so a cold (un-prewarmed)
        // query opens the docs file on demand — a couple more IOPs than the
        // eager path, but constant and only on the first query (prewarm or a
        // warm cache serve it with zero IO).
        let stats = dataset.object_store.as_ref().io_stats_incremental();
        assert_io_lt!(
            stats,
            read_iops,
            18,
            "Inverted index query should use minimal IOPs"
        );
    }

    #[tokio::test]
    async fn test_ivf_pq_index_iops() {
        // Test that querying an IVF_PQ vector index uses minimal IOPs
        let test_dir = TempStrDir::default();

        // Create dataset with vector column
        let dimension = 32;
        let num_rows = 1000;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dimension,
                ),
                false,
            ),
        ]));

        let ids = Int32Array::from_iter_values(0..num_rows);
        let vectors: Vec<Option<Vec<Option<f32>>>> = (0..num_rows)
            .map(|i| {
                Some(
                    (0..dimension)
                        .map(|j| Some((i * dimension + j) as f32 / 1000.0))
                        .collect(),
                )
            })
            .collect();
        let vector_array =
            FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(vectors, dimension);

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(vector_array)])
                .unwrap();

        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(reader, test_dir.as_str(), None)
            .await
            .unwrap();

        // Create IVF_PQ index
        let params = VectorIndexParams::ivf_pq(4, 8, 4, MetricType::L2, 50);
        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("ivf_pq_idx".to_string()),
                &params,
                true,
            )
            .await
            .unwrap();

        // Re-open dataset fresh
        let dataset = DatasetBuilder::from_uri(test_dir.as_str())
            .load()
            .await
            .unwrap();

        // Do a full scan to warm up data file metadata
        let _ = dataset.scan().try_into_batch().await.unwrap();

        // Reset IO stats before query
        let _ = dataset.object_store.as_ref().io_stats_incremental();

        // Query using the IVF_PQ index (KNN search)
        let query_vector: Vec<f32> = (0..dimension).map(|i| i as f32 / 1000.0).collect();
        let results = dataset
            .scan()
            .nearest("vector", &Float32Array::from(query_vector), 10)
            .unwrap()
            .nprobes(2)
            .try_into_batch()
            .await
            .unwrap();
        assert!(results.num_rows() > 0);

        // Verify IOPs
        let stats = dataset.object_store.as_ref().io_stats_incremental();
        assert_io_lt!(
            stats,
            read_iops,
            17,
            "IVF_PQ index query should use minimal IOPs"
        );
    }

    #[tokio::test]
    async fn test_describe_indices_returns_correct_vector_index_type() {
        const DIM: i32 = 8;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), DIM),
                true,
            ),
        ]));

        let data = generate_random_array(256 * DIM as usize);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..256)),
                Arc::new(FixedSizeListArray::try_new_from_values(data, DIM).unwrap()),
            ],
        )
        .unwrap();

        let test_dir = TempStrDir::default();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let mut dataset = Dataset::write(reader, &test_dir, None).await.unwrap();

        // Create IVF_FLAT index
        let params = VectorIndexParams::ivf_flat(2, MetricType::L2);
        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("vector_idx".to_string()),
                &params,
                true,
            )
            .await
            .unwrap();

        // Reload dataset and call describe_indices
        let dataset = Dataset::open(&test_dir).await.unwrap();
        let descriptions = dataset.describe_indices(None).await.unwrap();

        assert_eq!(descriptions.len(), 1);
        let desc = &descriptions[0];
        assert_eq!(desc.name(), "vector_idx");
        // This should be "IVF_FLAT", not "Unknown"
        assert_eq!(desc.index_type(), "IVF_FLAT");
        assert!(!desc.field_ids().is_empty());
    }

    /// FRI-straddle corruption (PR #6610) used to panic in `load_indices`.
    /// The fixture is a pre-#6610 dataset where a user index's
    /// `fragment_bitmap` only partially covers a rewrite group. After the
    /// tolerant-load fix `load_indices` returns Ok; affected old-frag IDs
    /// are dropped, no new-frag IDs are inserted, and `validate()` succeeds.
    #[tokio::test]
    async fn test_load_indices_tolerates_fri_straddle() {
        let tmp = copy_test_data_to_tmp("fri_straddle_pre_6610/fri_straddle_dataset").unwrap();
        let uri = format!("file://{}", tmp.std_path().display());
        let dataset = Dataset::open(&uri).await.unwrap();

        let indices = dataset.load_indices().await.unwrap();
        assert!(!indices.is_empty());
        dataset.validate().await.unwrap();
    }

    /// Any commit reseeds indices via `load_indices` → `build_manifest`,
    /// so a single no-op write persists the cleaned bitmap to disk.
    #[tokio::test]
    async fn test_auto_heal_persists_cleaned_bitmap() {
        use lance_table::io::manifest::read_manifest_indexes;

        let tmp = copy_test_data_to_tmp("fri_straddle_pre_6610/fri_straddle_dataset").unwrap();
        let uri = format!("file://{}", tmp.std_path().display());

        // Sanity: the on-disk fixture has at least one straddling segment.
        let pre = Dataset::open(&uri).await.unwrap();
        let raw_pre =
            read_manifest_indexes(&pre.object_store, &pre.manifest_location, &pre.manifest)
                .await
                .unwrap();
        let cleaned = pre.load_indices().await.unwrap();
        let any_changed = raw_pre
            .iter()
            .zip(cleaned.iter())
            .any(|(r, c)| r.fragment_bitmap != c.fragment_bitmap);
        assert!(
            any_changed,
            "fixture should have at least one segment whose bitmap is cleaned at load"
        );
        drop(pre);

        // No-op delete commits a fresh manifest seeded from cleaned indices.
        let mut dataset = Dataset::open(&uri).await.unwrap();
        dataset.delete("false").await.unwrap();

        // Reopen and read raw manifest indices: cleaned bitmaps now persisted.
        let healed = Dataset::open(&uri).await.unwrap();
        let raw_post = read_manifest_indexes(
            &healed.object_store,
            &healed.manifest_location,
            &healed.manifest,
        )
        .await
        .unwrap();
        let cleaned_post = healed.load_indices().await.unwrap();
        for (r, c) in raw_post.iter().zip(cleaned_post.iter()) {
            assert_eq!(
                r.fragment_bitmap, c.fragment_bitmap,
                "after auto-heal, raw on-disk bitmap should match cleaned bitmap"
            );
        }
    }

    #[tokio::test]
    async fn test_optimize_rebuilds_dormant_vector_index_instead_of_merging_stale_rows() {
        use crate::dataset::UpdateBuilder;

        let mut dataset = gen_batch()
            .col("vec", array::rand_vec::<Float32Type>(Dimension::from(4)))
            .into_ram_dataset_with_params(
                FragmentCount::from(2),
                FragmentRowCount::from(3),
                Some(WriteParams {
                    max_rows_per_file: 3,
                    enable_stable_row_ids: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        let original = dataset
            .scan()
            .project(&["vec"])
            .unwrap()
            .limit(Some(1), None)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let query = original["vec"].as_fixed_size_list().value(0);

        dataset
            .create_index(
                &["vec"],
                IndexType::Vector,
                Some("vec_idx".to_string()),
                &VectorIndexParams::ivf_flat(1, MetricType::L2),
                true,
            )
            .await
            .unwrap();

        // A full rewrite replaces every covered fragment; the definition is
        // retained as a dormant segment with empty effective coverage.
        let mut updated = UpdateBuilder::new(Arc::new(dataset))
            .set("vec", "array[100.0, 100.0, 100.0, 100.0]")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap()
            .new_dataset
            .as_ref()
            .clone();
        let retained = updated.load_indices_by_name("vec_idx").await.unwrap();
        assert_eq!(retained.len(), 1);
        assert!(
            retained[0]
                .effective_fragment_bitmap(&updated.fragment_bitmap)
                .unwrap()
                .is_empty()
        );

        // Optimize must rebuild from live data and replace the dormant
        // segment, not merge its stale postings back in.
        updated.optimize_indices(&Default::default()).await.unwrap();
        let rebuilt = updated.load_indices_by_name("vec_idx").await.unwrap();
        assert_eq!(rebuilt.len(), 1);
        assert!(
            !rebuilt[0]
                .effective_fragment_bitmap(&updated.fragment_bitmap)
                .unwrap()
                .is_empty()
        );

        let result = updated
            .scan()
            .nearest("vec", query.as_ref(), 1)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let distance = result["_distance"].as_primitive::<Float32Type>().value(0);
        assert!(
            distance > 1_000.0,
            "nearest distance {distance} should reflect the rewritten vectors, not stale postings"
        );
    }
}
