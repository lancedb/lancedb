// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Transaction definitions for updating datasets
//!
//! Prior to creating a new manifest, a transaction must be created representing
//! the changes being made to the dataset. By representing them as incremental
//! changes, we can detect whether concurrent operations are compatible with
//! one another. We can also rebuild manifests when retrying committing a
//! manifest.
//!
//! For more details please refer to the
//! [Transaction Specification](https://lance.org/format/table/transaction/#transaction-types).

use super::ManifestWriteConfig;
use super::write::merge_insert::inserted_rows::KeyExistenceFilter;
use crate::dataset::overlay::collect_overlay_stale_frags;
use crate::dataset::transaction::UpdateMode::{RewriteColumns, RewriteRows};
use crate::index::index_results_are_row_addrs;
use crate::index::mem_wal::update_mem_wal_index_compacted_sstables;
use crate::utils::temporal::timestamp_to_nanos;
use lance_core::datatypes::{
    LANCE_UNENFORCED_CLUSTERING_KEY_POSITION, LANCE_UNENFORCED_PRIMARY_KEY,
    LANCE_UNENFORCED_PRIMARY_KEY_POSITION,
};
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, Result, datatypes::Schema};
use lance_file::{
    datatypes::Fields,
    version::{ConcreteFileVersion, LanceFileVersion},
};
use lance_index::mem_wal::CompactedSsTable;
use lance_index::{frag_reuse::FRAG_REUSE_INDEX_NAME, is_system_index};
use lance_io::object_store::ObjectStore;
use lance_table::feature_flags::{FLAG_STABLE_ROW_IDS, apply_feature_flags};
use lance_table::rowids::read_row_ids;
use lance_table::{
    format::{
        BasePath, DataFile, DataStorageFormat, Fragment, IndexFile, IndexMetadata, Manifest,
        RowDatasetVersionMeta, RowDatasetVersionRun, RowDatasetVersionSequence, RowIdMeta,
        overlay::DataOverlayFile, pb,
    },
    io::{
        commit::CommitHandler,
        deletion::relative_deletion_file_path,
        manifest::{read_manifest, read_manifest_indexes},
    },
    rowids::{RowIdSequence, segment::U64Segment, version::build_version_meta, write_row_ids},
};
use object_store::path::Path;
use roaring::RoaringBitmap;
use std::cmp::Ordering;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use uuid::Uuid;

/// Fallback version for rows whose original creation version cannot be determined.
/// Version 1 is the initial dataset version in the Lance format.
const UNKNOWN_CREATED_AT_VERSION: u64 = 1;

/// Look up the `created_at` version for a single UPDATE-branch row ID.
///
/// Callers must only call this for row IDs that are confirmed to be present in
/// `row_id_to_source` (i.e. UPDATE branch rows whose source exists in an existing
/// fragment).  INSERT branch rows (no source) must use `new_version` directly and
/// must not call this function.
///
/// Uses `row_id_to_source` to find the originating fragment and row offset, then
/// performs a O(K) random-access lookup via [`RowDatasetVersionSequence::version_at`]
/// on the pre-decoded sequence in `version_cache` (keyed by fragment ID).
///
/// Returns [`UNKNOWN_CREATED_AT_VERSION`] if the source fragment has no
/// `created_at_version_meta` (missing or failed to decode) or the offset is
/// out of range.
fn resolve_created_at_version(
    row_id: u64,
    row_id_to_source: &HashMap<u64, (&Fragment, usize)>,
    version_cache: &HashMap<u64, RowDatasetVersionSequence>,
) -> u64 {
    let Some((orig_frag, row_offset)) = row_id_to_source.get(&row_id) else {
        return UNKNOWN_CREATED_AT_VERSION;
    };
    let Some(seq) = version_cache.get(&orig_frag.id) else {
        return UNKNOWN_CREATED_AT_VERSION;
    };
    seq.version_at(*row_offset)
        .unwrap_or(UNKNOWN_CREATED_AT_VERSION)
}

/// For each new fragment produced by an update, set `created_at_version_meta`
/// (preserved from the original rows) and `last_updated_at_version_meta`.
fn resolve_update_version_metadata(
    existing_fragments: &[Fragment],
    new_fragments: &mut [Fragment],
    new_version: u64,
) -> Result<()> {
    // Collect only the row IDs we actually need to resolve, those appearing in new_fragments
    // with inline metadata. This bounds the lookup map to O(updated rows) instead of O(all dataset rows)
    let needed_row_ids: HashSet<u64> = new_fragments
        .iter()
        .filter_map(|f| match &f.row_id_meta {
            Some(RowIdMeta::Inline(data)) => read_row_ids(data).ok(),
            _ => None,
        })
        .flat_map(|seq| seq.iter().collect::<Vec<_>>())
        .collect();

    let mut row_id_to_source: HashMap<u64, (&Fragment, usize)> = HashMap::new();

    if !needed_row_ids.is_empty() {
        // Compute the bounding range of the needed set once.  Any fragment whose
        // entire row-id range lies outside [needed_min, needed_max] cannot contain
        // any needed ID and can be skipped before the inner per-row loop.
        let needed_min = *needed_row_ids.iter().min().unwrap();
        let needed_max = *needed_row_ids.iter().max().unwrap();

        // Stable row IDs must be globally unique among *live* rows, but after a rewrite-style
        // update the same stable ID can appear twice in `existing_fragments`: once in an older
        // fragment's inline `row_id_meta` at the original row offset (rows may be soft-deleted
        // via a deletion vector) and again in a newer fragment holding rewritten data. For
        // `created_at` we need the mapping from the original fragment/offset; that is always the
        // first occurrence when fragments are processed in ascending `id` order.
        let mut sorted_frags: Vec<&Fragment> = existing_fragments.iter().collect();
        sorted_frags.sort_by_key(|f| f.id);
        for frag in sorted_frags {
            if let Some(RowIdMeta::Inline(data)) = &frag.row_id_meta
                && let Ok(seq) = read_row_ids(data)
            {
                // Range pre-filter: skip the per-row inner loop when the fragment's
                // bounding row-id range has no overlap with [needed_min, needed_max].
                // row_id_range() returns None for empty sequences, which are also skipped.
                // This is a conservative check (may produce false positives for sparse
                // segments) but never skips a fragment that actually contains a needed ID.
                if seq
                    .row_id_range()
                    .is_none_or(|r| *r.end() < needed_min || *r.start() > needed_max)
                {
                    continue;
                }

                for (offset, rid) in seq.iter().enumerate() {
                    if needed_row_ids.contains(&rid) {
                        row_id_to_source.entry(rid).or_insert((frag, offset));
                    }
                }
            }
        }
    }

    // Pre-decode the `created_at` version sequence for each source fragment exactly
    // once.  Without this cache, resolve_created_at_version would call load_sequence()
    // (a protobuf decode) for every single updated row, even when many rows originate
    // from the same fragment.
    let source_frag_ids: HashSet<u64> = row_id_to_source.values().map(|(f, _)| f.id).collect();
    let version_cache: HashMap<u64, RowDatasetVersionSequence> = existing_fragments
        .iter()
        .filter(|f| source_frag_ids.contains(&f.id))
        .filter_map(|frag| {
            let seq = frag
                .created_at_version_meta
                .as_ref()?
                .load_sequence()
                .ok()?;
            Some((frag.id, seq))
        })
        .collect();

    for fragment in new_fragments.iter_mut() {
        let row_ids = match &fragment.row_id_meta {
            Some(RowIdMeta::Inline(data)) => read_row_ids(data).ok(),
            Some(RowIdMeta::External(_)) => {
                log::warn!(
                    "Fragment {} has external row ID metadata; \
                     version tracking will use defaults",
                    fragment.id,
                );
                None
            }
            None => None,
        };

        if let Some(row_ids) = row_ids {
            let physical_rows = fragment.physical_rows.unwrap_or(0);
            let created_at_versions: Vec<u64> = row_ids
                .iter()
                .map(|rid| {
                    if row_id_to_source.contains_key(&rid) {
                        // UPDATE branch: stable row ID resolves to a source row in an
                        // existing fragment.  Copy created_at from the original row so
                        // the row's first-appearance version is preserved across rewrites.
                        resolve_created_at_version(rid, &row_id_to_source, &version_cache)
                    } else {
                        // INSERT branch: stable row ID has no source in existing fragments
                        // (e.g. NOT MATCHED arm of MERGE INTO).  The row first appears in
                        // this commit, so created_at equals the new commit version.
                        new_version
                    }
                })
                .collect();
            debug_assert_eq!(created_at_versions.len(), physical_rows);

            let runs = encode_version_runs(&created_at_versions);
            let created_at_seq = RowDatasetVersionSequence { runs };
            fragment.created_at_version_meta = Some(
                RowDatasetVersionMeta::from_sequence(&created_at_seq).map_err(|e| {
                    Error::internal(format!(
                        "Failed to create created_at version metadata: {}",
                        e
                    ))
                })?,
            );

            fragment.last_updated_at_version_meta = build_version_meta(fragment, new_version);
        } else {
            let version_meta = build_version_meta(fragment, new_version);
            fragment.last_updated_at_version_meta = version_meta.clone();
            fragment.created_at_version_meta = version_meta;
        }
    }
    Ok(())
}

/// Run-length encode a sequence of per-row versions into [`RowDatasetVersionRun`]s.
fn encode_version_runs(versions: &[u64]) -> Vec<RowDatasetVersionRun> {
    if versions.is_empty() {
        return Vec::new();
    }
    let mut runs = Vec::new();
    let mut current_version = versions[0];
    let mut run_start = 0u64;
    for (i, &version) in versions.iter().enumerate().skip(1) {
        if version != current_version {
            runs.push(RowDatasetVersionRun {
                span: U64Segment::Range(run_start..i as u64),
                version: current_version,
            });
            current_version = version;
            run_start = i as u64;
        }
    }
    runs.push(RowDatasetVersionRun {
        span: U64Segment::Range(run_start..versions.len() as u64),
        version: current_version,
    });
    runs
}

/// A change to a dataset that can be retried
///
/// This contains enough information to be able to build the next manifest,
/// given the current manifest.
#[derive(Debug, Clone, DeepSizeOf, PartialEq)]
pub struct Transaction {
    /// The version of the table this transaction is based off of. If this is
    /// the first transaction, this should be 0.
    pub read_version: u64,
    pub uuid: String,
    pub operation: Operation,
    pub tag: Option<String>,
    pub transaction_properties: Option<Arc<HashMap<String, String>>>,
}

#[derive(Debug, Clone, DeepSizeOf, PartialEq)]
pub struct DataReplacementGroup(pub u64, pub DataFile);

/// Overlay files to append to a single fragment, in order (the last entry is
/// newest). The overlays are appended to the fragment's existing `overlays`
/// list rather than replacing it, so overlays written by concurrent commits are
/// preserved. Each overlay's `committed_version` is stamped to the new dataset
/// version at commit time (re-stamped on retry).
#[derive(Debug, Clone, DeepSizeOf, PartialEq)]
pub struct DataOverlayGroup {
    pub fragment_id: u64,
    pub overlays: Vec<DataOverlayFile>,
}

/// An entry for a map update. If value is None, the key will be removed from the map.
#[derive(Debug, Clone, DeepSizeOf, PartialEq)]
pub struct UpdateMapEntry {
    /// The key of the map entry to update.
    pub key: String,
    /// The value to set for the key.
    pub value: Option<String>,
}

impl From<(String, Option<String>)> for UpdateMapEntry {
    fn from((key, value): (String, Option<String>)) -> Self {
        Self { key, value }
    }
}

impl From<(String, String)> for UpdateMapEntry {
    fn from((key, value): (String, String)) -> Self {
        Self::from((key, Some(value)))
    }
}

impl From<(&str, Option<&str>)> for UpdateMapEntry {
    fn from((key, value): (&str, Option<&str>)) -> Self {
        Self {
            key: key.to_string(),
            value: value.map(str::to_owned),
        }
    }
}

impl From<(&str, &str)> for UpdateMapEntry {
    fn from((key, value): (&str, &str)) -> Self {
        Self::from((key, Some(value)))
    }
}

/// Represents updates to a map (either incremental or replacement)
#[derive(Debug, Clone, DeepSizeOf, PartialEq)]
pub struct UpdateMap {
    pub update_entries: Vec<UpdateMapEntry>,
    /// If true, the map will be replaced entirely with the new entries.
    /// If false, the new entries will be merged with the existing map.
    pub replace: bool,
}

/// An operation on a dataset.
#[derive(Debug, Clone, DeepSizeOf)]
pub enum Operation {
    /// Adding new fragments to the dataset. The fragments contained within
    /// haven't yet been assigned a final ID.
    Append { fragments: Vec<Fragment> },
    /// Updated fragments contain those that have been modified with new deletion
    /// files. The deleted fragment IDs are those that should be removed from
    /// the manifest.
    Delete {
        updated_fragments: Vec<Fragment>,
        deleted_fragment_ids: Vec<u64>,
        predicate: String,
    },
    /// Overwrite the entire dataset with the given fragments. This is also
    /// used when initially creating a table.
    ///
    /// The fragments are newly written ones and are assigned fresh ids at commit
    /// time, continuing from the dataset's highest id ever used; the ids they
    /// arrive with are ignored.
    ///
    /// A fragment carrying a deletion file is rejected. A deletion file's path
    /// embeds the fragment id, so it cannot follow its fragment to the new id:
    /// minting a fragment and giving it a deletion file are mutually exclusive in
    /// one transaction. Use [`Self::Delete`] to commit deletions against existing
    /// fragments, or [`Self::Merge`] to change their schema.
    Overwrite {
        fragments: Vec<Fragment>,
        schema: Schema,
        config_upsert_values: Option<HashMap<String, String>>,
        initial_bases: Option<Vec<BasePath>>,
    },
    /// A new index has been created.
    CreateIndex {
        /// The new secondary indices,
        /// any existing indices with the same name will be replaced.
        new_indices: Vec<IndexMetadata>,
        /// The indices that have been modified.
        removed_indices: Vec<IndexMetadata>,
    },
    /// Data is rewritten but *not* modified. This is used for things like
    /// compaction or re-ordering. Contains the old fragments and the new
    /// ones that have been replaced.
    ///
    /// This operation will modify the row addresses of existing rows and
    /// so any existing index covering a rewritten fragment will need to be
    /// remapped.
    Rewrite {
        /// Groups of fragments that have been modified
        groups: Vec<RewriteGroup>,
        /// Indices that have been updated with the new row addresses
        rewritten_indices: Vec<RewrittenIndex>,
        /// The fragment reuse index to be created or updated to
        frag_reuse_index: Option<IndexMetadata>,
    },
    /// Replace data in a column in the dataset with new data. This is used for
    /// null column population where we replace an entirely null column with a
    /// new column that has data.
    ///
    /// This operation will only allow replacing files that contain the same schema
    /// e.g. if the original files contain columns A, B, C and the new files contain
    /// only columns A, B then the operation is not allowed. As we would need to split
    /// the original files into two files, one with column A, B and the other with column C.
    ///
    /// Corollary to the above: the operation will also not allow replacing files unless the
    /// affected columns all have the same datafile layout across the fragments being replaced.
    ///
    /// e.g. if fragments being replaced contain files with different schema layouts on
    /// the column being replaced, the operation is not allowed.
    /// say `frag_1: [A] [B, C]` and `frag_2: [A, B] [C]` and we are trying to replace column A
    /// with a new column A, the operation is not allowed.
    DataReplacement {
        replacements: Vec<DataReplacementGroup>,
    },
    /// Attach overlay files to fragments, supplying new values for a subset of
    /// `(physical offset, field)` cells without rewriting the fragments' base
    /// data files. See [`DataOverlayFile`] and the Data Overlay Files
    /// specification for resolution, coverage, and versioning rules.
    DataOverlay { groups: Vec<DataOverlayGroup> },
    /// Merge a new column in
    /// 'fragments' is the final fragments include all data files, the new fragments must align with old ones at rows.
    /// 'schema' is not forced to include existed columns, which means we could use Merge to drop column data
    Merge {
        fragments: Vec<Fragment>,
        schema: Schema,
    },
    /// Restore an old version of the database
    Restore { version: u64 },
    /// Reserves fragment ids for future use
    /// This can be used when row ids need to be known before a transaction
    /// has been committed.  It is used during a rewrite operation to allow
    /// indices to be remapped to the new row ids as part of the operation.
    ReserveFragments { num_fragments: u32 },

    /// Update values in the dataset.
    ///
    /// Updates are generally vertical or horizontal.
    ///
    /// A vertical update adds new rows.  In this case, the updated_fragments
    /// will only have existing rows deleted and will not have any new fields added.
    /// All new data will be contained in new_fragments.
    /// This is what is used by a merge_insert that matches the whole schema and what
    /// is used by the dataset updater.
    ///
    /// A horizontal update adds new columns.  In this case, the updated fragments
    /// may have fields removed or added.  It is even possible for a field to be tombstoned
    /// and then added back in the same update. (which is a field modification).  If any
    /// fields are modified in this way then they need to be added to the fields_modified list.
    /// This way we can correctly update the indices.
    /// This is what is used by a merge insert that does not match the whole schema.
    Update {
        /// Ids of fragments that have been moved
        removed_fragment_ids: Vec<u64>,
        /// Fragments that have been updated
        updated_fragments: Vec<Fragment>,
        /// Fragments that have been added
        new_fragments: Vec<Fragment>,
        /// The fields that have been modified
        fields_modified: Vec<u32>,
        /// MemWAL SSTables to mark as compacted after this transaction.
        compacted_sstables: Vec<CompactedSsTable>,
        /// The fields that used to judge whether to preserve the new frag's id into
        /// the frag bitmap of the specified indices.
        fields_for_preserving_frag_bitmap: Vec<u32>,
        /// The mode of update
        update_mode: Option<UpdateMode>,
        /// Optional filter for detecting conflicts on inserted row keys.
        /// Only tracks keys from INSERT operations during merge insert, not updates.
        inserted_rows_filter: Option<KeyExistenceFilter>,
        /// Physical row offsets (per fragment) that matched `update_columns` for RewriteColumns.
        /// `None` means callers did not supply offsets; `build_manifest` skips partial refresh then.
        updated_fragment_offsets: Option<UpdatedFragmentOffsets>,
    },

    /// Project to a new schema. This only changes the schema, not the data.
    Project { schema: Schema },

    /// Update the dataset configuration.
    UpdateConfig {
        config_updates: Option<UpdateMap>,
        table_metadata_updates: Option<UpdateMap>,
        schema_metadata_updates: Option<UpdateMap>,
        field_metadata_updates: HashMap<i32, UpdateMap>,
    },
    /// Update SSTable compaction progress in the MemWAL index.
    ///
    /// This is used during merge-insert to atomically record which
    /// SSTables have been compacted into the base table.
    UpdateMemWalState {
        compacted_sstables: Vec<CompactedSsTable>,
    },

    /// Clone a dataset.
    Clone {
        is_shallow: bool,
        ref_name: Option<String>,
        ref_version: u64,
        ref_path: String,
        branch_name: Option<String>,
    },

    // Update base paths in the dataset (currently only supports adding new bases).
    UpdateBases {
        /// The new base paths to add to the manifest.
        new_bases: Vec<BasePath>,
    },
}

#[derive(Debug, Clone, PartialEq, DeepSizeOf)]
pub enum UpdateMode {
    /// rows are deleted in current fragments and rewritten in new fragments.
    /// This is most optimal when the majority of columns are being rewritten
    /// or only a few rows are being updated.
    RewriteRows,

    /// within each fragment, columns are fully rewritten and inserted as new data files.
    /// Old versions of columns are tombstoned. This is most optimal when most rows are affected
    /// but a small subset of columns are affected.
    RewriteColumns,
}

/// Matched physical row offsets per fragment for a partial [`UpdateMode::RewriteColumns`] update.
///
/// Used with stable row IDs so `build_manifest` can refresh row-level version
/// metadata only for rows that were rewritten.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UpdatedFragmentOffsets(pub HashMap<u64, RoaringBitmap>);

impl DeepSizeOf for UpdatedFragmentOffsets {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.0.iter().fold(0_usize, |acc, (frag_id, bitmap)| {
            acc + frag_id.deep_size_of_children(context)
                + (bitmap.len() as usize).saturating_mul(std::mem::size_of::<u32>())
        })
    }
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Append { .. } => write!(f, "Append"),
            Self::Delete { .. } => write!(f, "Delete"),
            Self::Overwrite { .. } => write!(f, "Overwrite"),
            Self::CreateIndex { .. } => write!(f, "CreateIndex"),
            Self::Rewrite { .. } => write!(f, "Rewrite"),
            Self::Merge { .. } => write!(f, "Merge"),
            Self::Restore { .. } => write!(f, "Restore"),
            Self::ReserveFragments { .. } => write!(f, "ReserveFragments"),
            Self::Update { .. } => write!(f, "Update"),
            Self::Project { .. } => write!(f, "Project"),
            Self::UpdateConfig { .. } => write!(f, "UpdateConfig"),
            Self::DataReplacement { .. } => write!(f, "DataReplacement"),
            Self::DataOverlay { .. } => write!(f, "DataOverlay"),
            Self::Clone { .. } => write!(f, "Clone"),
            Self::UpdateMemWalState { .. } => write!(f, "UpdateMemWalState"),
            Self::UpdateBases { .. } => write!(f, "UpdateBases"),
        }
    }
}

impl From<&Transaction> for lance_table::format::Transaction {
    fn from(value: &Transaction) -> Self {
        let pb_transaction: pb::Transaction = value.into();
        Self {
            inner: pb_transaction,
        }
    }
}

impl PartialEq for Operation {
    fn eq(&self, other: &Self) -> bool {
        // Many of the operations contain `Vec<T>` where the order of the
        // elements don't matter. So we need to compare them in a way that
        // ignores the order of the elements.
        // TODO: we can make it so the vecs are always constructed in order.
        // Then we can use `==` instead of `compare_vec`.
        fn compare_vec<T: PartialEq>(a: &[T], b: &[T]) -> bool {
            a.len() == b.len() && a.iter().all(|f| b.contains(f))
        }
        match (self, other) {
            (Self::Append { fragments: a }, Self::Append { fragments: b }) => compare_vec(a, b),
            (
                Self::Clone {
                    is_shallow: a_is_shallow,
                    ref_name: a_ref_name,
                    ref_version: a_ref_version,
                    ref_path: a_source_path,
                    branch_name: a_branch_name,
                },
                Self::Clone {
                    is_shallow: b_is_shallow,
                    ref_name: b_ref_name,
                    ref_version: b_ref_version,
                    ref_path: b_source_path,
                    branch_name: b_branch_name,
                },
            ) => {
                a_is_shallow == b_is_shallow
                    && a_ref_name == b_ref_name
                    && a_ref_version == b_ref_version
                    && a_source_path == b_source_path
                    && a_branch_name == b_branch_name
            }
            (
                Self::Delete {
                    updated_fragments: a_updated,
                    deleted_fragment_ids: a_deleted,
                    predicate: a_predicate,
                },
                Self::Delete {
                    updated_fragments: b_updated,
                    deleted_fragment_ids: b_deleted,
                    predicate: b_predicate,
                },
            ) => {
                compare_vec(a_updated, b_updated)
                    && compare_vec(a_deleted, b_deleted)
                    && a_predicate == b_predicate
            }
            (
                Self::Overwrite {
                    fragments: a_fragments,
                    schema: a_schema,
                    config_upsert_values: a_config,
                    initial_bases: a_initial,
                },
                Self::Overwrite {
                    fragments: b_fragments,
                    schema: b_schema,
                    config_upsert_values: b_config,
                    initial_bases: b_initial,
                },
            ) => {
                compare_vec(a_fragments, b_fragments)
                    && a_schema == b_schema
                    && a_config == b_config
                    && a_initial == b_initial
            }
            (
                Self::CreateIndex {
                    new_indices: a_new,
                    removed_indices: a_removed,
                },
                Self::CreateIndex {
                    new_indices: b_new,
                    removed_indices: b_removed,
                },
            ) => compare_vec(a_new, b_new) && compare_vec(a_removed, b_removed),
            (
                Self::Rewrite {
                    groups: a_groups,
                    rewritten_indices: a_indices,
                    frag_reuse_index: a_frag_reuse_index,
                },
                Self::Rewrite {
                    groups: b_groups,
                    rewritten_indices: b_indices,
                    frag_reuse_index: b_frag_reuse_index,
                },
            ) => {
                compare_vec(a_groups, b_groups)
                    && compare_vec(a_indices, b_indices)
                    && a_frag_reuse_index == b_frag_reuse_index
            }
            (
                Self::Merge {
                    fragments: a_fragments,
                    schema: a_schema,
                },
                Self::Merge {
                    fragments: b_fragments,
                    schema: b_schema,
                },
            ) => compare_vec(a_fragments, b_fragments) && a_schema == b_schema,
            (Self::Restore { version: a }, Self::Restore { version: b }) => a == b,
            (
                Self::ReserveFragments { num_fragments: a },
                Self::ReserveFragments { num_fragments: b },
            ) => a == b,
            (
                Self::Update {
                    removed_fragment_ids: a_removed,
                    updated_fragments: a_updated,
                    new_fragments: a_new,
                    fields_modified: a_fields,
                    compacted_sstables: a_compacted_sstables,
                    fields_for_preserving_frag_bitmap: a_fields_for_preserving_frag_bitmap,
                    update_mode: a_update_mode,
                    inserted_rows_filter: a_inserted_rows_filter,
                    updated_fragment_offsets: a_updated_fragment_offsets,
                },
                Self::Update {
                    removed_fragment_ids: b_removed,
                    updated_fragments: b_updated,
                    new_fragments: b_new,
                    fields_modified: b_fields,
                    compacted_sstables: b_compacted_sstables,
                    fields_for_preserving_frag_bitmap: b_fields_for_preserving_frag_bitmap,
                    update_mode: b_update_mode,
                    inserted_rows_filter: b_inserted_rows_filter,
                    updated_fragment_offsets: b_updated_fragment_offsets,
                },
            ) => {
                compare_vec(a_removed, b_removed)
                    && compare_vec(a_updated, b_updated)
                    && compare_vec(a_new, b_new)
                    && compare_vec(a_fields, b_fields)
                    && compare_vec(a_compacted_sstables, b_compacted_sstables)
                    && compare_vec(
                        a_fields_for_preserving_frag_bitmap,
                        b_fields_for_preserving_frag_bitmap,
                    )
                    && a_update_mode == b_update_mode
                    && a_inserted_rows_filter == b_inserted_rows_filter
                    && a_updated_fragment_offsets == b_updated_fragment_offsets
            }
            (Self::Project { schema: a }, Self::Project { schema: b }) => a == b,
            (
                Self::UpdateConfig {
                    config_updates: a_config,
                    table_metadata_updates: a_table_metadata,
                    schema_metadata_updates: a_schema,
                    field_metadata_updates: a_field,
                },
                Self::UpdateConfig {
                    config_updates: b_config,
                    table_metadata_updates: b_table_metadata,
                    schema_metadata_updates: b_schema,
                    field_metadata_updates: b_field,
                },
            ) => {
                a_config == b_config
                    && a_table_metadata == b_table_metadata
                    && a_schema == b_schema
                    && a_field == b_field
            }
            (
                Self::DataReplacement { replacements: a },
                Self::DataReplacement { replacements: b },
            ) => a.len() == b.len() && a.iter().all(|r| b.contains(r)),
            // Handle all remaining combinations.
            // We spell out all combinations explicitly to prevent
            // us accidentally handling a new case in the wrong way.
            (Self::Append { .. }, Self::Delete { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Append { .. }, Self::Overwrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Append { .. }, Self::CreateIndex { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Append { .. }, Self::Rewrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Append { .. }, Self::Merge { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Append { .. }, Self::Restore { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Append { .. }, Self::ReserveFragments { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Append { .. }, Self::Update { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Append { .. }, Self::Project { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Append { .. }, Self::UpdateConfig { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Append { .. }, Self::DataReplacement { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Append { .. }, Self::UpdateMemWalState { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Append { .. }, Self::Clone { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }

            (Self::Delete { .. }, Self::Append { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Delete { .. }, Self::Overwrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Delete { .. }, Self::CreateIndex { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Delete { .. }, Self::Rewrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Delete { .. }, Self::Merge { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Delete { .. }, Self::Restore { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Delete { .. }, Self::ReserveFragments { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Delete { .. }, Self::Update { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Delete { .. }, Self::Project { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Delete { .. }, Self::UpdateConfig { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Delete { .. }, Self::DataReplacement { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Delete { .. }, Self::UpdateMemWalState { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Delete { .. }, Self::Clone { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }

            (Self::Overwrite { .. }, Self::Append { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Overwrite { .. }, Self::Delete { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Overwrite { .. }, Self::CreateIndex { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Overwrite { .. }, Self::Rewrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Overwrite { .. }, Self::Merge { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Overwrite { .. }, Self::Restore { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Overwrite { .. }, Self::ReserveFragments { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Overwrite { .. }, Self::Update { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Overwrite { .. }, Self::Project { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Overwrite { .. }, Self::UpdateConfig { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Overwrite { .. }, Self::DataReplacement { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Overwrite { .. }, Self::UpdateMemWalState { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Overwrite { .. }, Self::Clone { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }

            (Self::CreateIndex { .. }, Self::Append { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::CreateIndex { .. }, Self::Delete { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::CreateIndex { .. }, Self::Overwrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::CreateIndex { .. }, Self::Rewrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::CreateIndex { .. }, Self::Merge { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::CreateIndex { .. }, Self::Restore { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::CreateIndex { .. }, Self::ReserveFragments { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::CreateIndex { .. }, Self::Update { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::CreateIndex { .. }, Self::Project { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::CreateIndex { .. }, Self::UpdateConfig { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::CreateIndex { .. }, Self::DataReplacement { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::CreateIndex { .. }, Self::UpdateMemWalState { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::CreateIndex { .. }, Self::Clone { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }

            (Self::Rewrite { .. }, Self::Append { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Rewrite { .. }, Self::Delete { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Rewrite { .. }, Self::Overwrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Rewrite { .. }, Self::CreateIndex { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Rewrite { .. }, Self::Merge { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Rewrite { .. }, Self::Restore { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Rewrite { .. }, Self::ReserveFragments { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Rewrite { .. }, Self::Update { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Rewrite { .. }, Self::Project { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Rewrite { .. }, Self::UpdateConfig { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Rewrite { .. }, Self::DataReplacement { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Rewrite { .. }, Self::UpdateMemWalState { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Rewrite { .. }, Self::Clone { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }

            (Self::Merge { .. }, Self::Append { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Merge { .. }, Self::Delete { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Merge { .. }, Self::Overwrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Merge { .. }, Self::CreateIndex { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Merge { .. }, Self::Rewrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Merge { .. }, Self::Restore { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Merge { .. }, Self::ReserveFragments { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Merge { .. }, Self::Update { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Merge { .. }, Self::Project { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Merge { .. }, Self::UpdateConfig { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Merge { .. }, Self::DataReplacement { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Merge { .. }, Self::UpdateMemWalState { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Merge { .. }, Self::Clone { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }

            (Self::Restore { .. }, Self::Append { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Restore { .. }, Self::Delete { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Restore { .. }, Self::Overwrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Restore { .. }, Self::CreateIndex { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Restore { .. }, Self::Rewrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Restore { .. }, Self::Merge { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Restore { .. }, Self::ReserveFragments { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Restore { .. }, Self::Update { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Restore { .. }, Self::Project { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Restore { .. }, Self::UpdateConfig { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Restore { .. }, Self::DataReplacement { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Restore { .. }, Self::UpdateMemWalState { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Restore { .. }, Self::Clone { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }

            (Self::ReserveFragments { .. }, Self::Append { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::ReserveFragments { .. }, Self::Delete { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::ReserveFragments { .. }, Self::Overwrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::ReserveFragments { .. }, Self::CreateIndex { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::ReserveFragments { .. }, Self::Rewrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::ReserveFragments { .. }, Self::Merge { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::ReserveFragments { .. }, Self::Restore { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::ReserveFragments { .. }, Self::Update { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::ReserveFragments { .. }, Self::Project { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::ReserveFragments { .. }, Self::UpdateConfig { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::ReserveFragments { .. }, Self::DataReplacement { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::ReserveFragments { .. }, Self::UpdateMemWalState { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::ReserveFragments { .. }, Self::Clone { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }

            (Self::Update { .. }, Self::Append { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Update { .. }, Self::Delete { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Update { .. }, Self::Overwrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Update { .. }, Self::CreateIndex { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Update { .. }, Self::Rewrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Update { .. }, Self::Merge { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Update { .. }, Self::Restore { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Update { .. }, Self::ReserveFragments { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Update { .. }, Self::Project { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Update { .. }, Self::UpdateConfig { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Update { .. }, Self::DataReplacement { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Update { .. }, Self::UpdateMemWalState { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Update { .. }, Self::Clone { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }

            (Self::Project { .. }, Self::Append { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Project { .. }, Self::Delete { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Project { .. }, Self::Overwrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Project { .. }, Self::CreateIndex { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Project { .. }, Self::Rewrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Project { .. }, Self::Merge { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Project { .. }, Self::Restore { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Project { .. }, Self::ReserveFragments { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Project { .. }, Self::Update { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Project { .. }, Self::UpdateConfig { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Project { .. }, Self::DataReplacement { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Project { .. }, Self::UpdateMemWalState { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Project { .. }, Self::Clone { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }

            (Self::UpdateConfig { .. }, Self::Append { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateConfig { .. }, Self::Delete { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateConfig { .. }, Self::Overwrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateConfig { .. }, Self::CreateIndex { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateConfig { .. }, Self::Rewrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateConfig { .. }, Self::Merge { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateConfig { .. }, Self::Restore { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateConfig { .. }, Self::ReserveFragments { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateConfig { .. }, Self::Update { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateConfig { .. }, Self::Project { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateConfig { .. }, Self::DataReplacement { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateConfig { .. }, Self::UpdateMemWalState { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateConfig { .. }, Self::Clone { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }

            (Self::DataReplacement { .. }, Self::Append { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::DataReplacement { .. }, Self::Delete { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::DataReplacement { .. }, Self::Overwrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::DataReplacement { .. }, Self::CreateIndex { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::DataReplacement { .. }, Self::Rewrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::DataReplacement { .. }, Self::Merge { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::DataReplacement { .. }, Self::Restore { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::DataReplacement { .. }, Self::ReserveFragments { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::DataReplacement { .. }, Self::Update { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::DataReplacement { .. }, Self::Project { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::DataReplacement { .. }, Self::UpdateConfig { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::DataReplacement { .. }, Self::UpdateMemWalState { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::DataReplacement { .. }, Self::Clone { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }

            (Self::UpdateMemWalState { .. }, Self::Append { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateMemWalState { .. }, Self::Delete { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateMemWalState { .. }, Self::Overwrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateMemWalState { .. }, Self::CreateIndex { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateMemWalState { .. }, Self::Rewrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateMemWalState { .. }, Self::Merge { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateMemWalState { .. }, Self::Restore { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateMemWalState { .. }, Self::ReserveFragments { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateMemWalState { .. }, Self::Update { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateMemWalState { .. }, Self::Project { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateMemWalState { .. }, Self::UpdateConfig { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateMemWalState { .. }, Self::DataReplacement { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateMemWalState { .. }, Self::Clone { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (
                Self::UpdateMemWalState {
                    compacted_sstables: a_compacted,
                },
                Self::UpdateMemWalState {
                    compacted_sstables: b_compacted,
                },
            ) => compare_vec(a_compacted, b_compacted),
            (Self::Clone { .. }, Self::Append { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Clone { .. }, Self::Delete { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Clone { .. }, Self::Overwrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Clone { .. }, Self::CreateIndex { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Clone { .. }, Self::Rewrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Clone { .. }, Self::Merge { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Clone { .. }, Self::Restore { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Clone { .. }, Self::ReserveFragments { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Clone { .. }, Self::Update { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Clone { .. }, Self::Project { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Clone { .. }, Self::UpdateConfig { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Clone { .. }, Self::DataReplacement { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Clone { .. }, Self::UpdateMemWalState { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }

            (Self::UpdateBases { new_bases: a }, Self::UpdateBases { new_bases: b }) => {
                compare_vec(a, b)
            }

            (Self::UpdateBases { .. }, Self::Append { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateBases { .. }, Self::Delete { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateBases { .. }, Self::Overwrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateBases { .. }, Self::CreateIndex { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateBases { .. }, Self::Rewrite { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateBases { .. }, Self::Merge { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateBases { .. }, Self::Restore { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateBases { .. }, Self::ReserveFragments { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateBases { .. }, Self::Update { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateBases { .. }, Self::Project { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateBases { .. }, Self::UpdateConfig { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateBases { .. }, Self::DataReplacement { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateBases { .. }, Self::UpdateMemWalState { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateBases { .. }, Self::Clone { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }

            (Self::Append { .. }, Self::UpdateBases { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Delete { .. }, Self::UpdateBases { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Overwrite { .. }, Self::UpdateBases { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::CreateIndex { .. }, Self::UpdateBases { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Rewrite { .. }, Self::UpdateBases { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Merge { .. }, Self::UpdateBases { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Restore { .. }, Self::UpdateBases { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::ReserveFragments { .. }, Self::UpdateBases { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Update { .. }, Self::UpdateBases { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Project { .. }, Self::UpdateBases { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateConfig { .. }, Self::UpdateBases { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::DataReplacement { .. }, Self::UpdateBases { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::UpdateMemWalState { .. }, Self::UpdateBases { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::Clone { .. }, Self::UpdateBases { .. }) => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
            }
            (Self::DataOverlay { groups: a }, Self::DataOverlay { groups: b }) => compare_vec(a, b),
            (Self::DataOverlay { .. }, _) | (_, Self::DataOverlay { .. }) => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RewrittenIndex {
    pub old_id: Uuid,
    pub new_id: Uuid,
    pub new_index_details: prost_types::Any,
    pub new_index_version: u32,
    /// Files in the new index with their sizes.
    /// Empty list from older writers that didn't persist this field.
    pub new_index_files: Option<Vec<IndexFile>>,
}

impl DeepSizeOf for RewrittenIndex {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.new_index_details
            .type_url
            .deep_size_of_children(context)
            + self.new_index_details.value.deep_size_of_children(context)
    }
}

#[derive(Debug, Clone, DeepSizeOf)]
pub struct RewriteGroup {
    pub old_fragments: Vec<Fragment>,
    pub new_fragments: Vec<Fragment>,
}

impl PartialEq for RewriteGroup {
    fn eq(&self, other: &Self) -> bool {
        fn compare_vec<T: PartialEq>(a: &[T], b: &[T]) -> bool {
            a.len() == b.len() && a.iter().all(|f| b.contains(f))
        }
        compare_vec(&self.old_fragments, &other.old_fragments)
            && compare_vec(&self.new_fragments, &other.new_fragments)
    }
}

impl Operation {
    /// Returns the config keys that have been upserted by this operation.
    fn get_upsert_config_keys(&self) -> Vec<String> {
        match self {
            Self::Overwrite {
                config_upsert_values: Some(upsert_values),
                ..
            } => {
                let vec: Vec<String> = upsert_values.keys().cloned().collect();
                vec
            }
            Self::UpdateConfig {
                config_updates: Some(config_updates),
                ..
            } => config_updates
                .update_entries
                .iter()
                .filter_map(|entry| {
                    if entry.value.is_some() {
                        Some(entry.key.clone())
                    } else {
                        None
                    }
                })
                .collect(),
            _ => Vec::<String>::new(),
        }
    }

    /// Returns the config keys that have been deleted by this operation.
    fn get_delete_config_keys(&self) -> Vec<String> {
        match self {
            Self::UpdateConfig {
                config_updates: Some(config_updates),
                ..
            } => config_updates
                .update_entries
                .iter()
                .filter_map(|entry| {
                    if entry.value.is_none() {
                        Some(entry.key.clone())
                    } else {
                        None
                    }
                })
                .collect(),
            _ => Vec::<String>::new(),
        }
    }

    pub(crate) fn modifies_same_metadata(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::UpdateConfig {
                    table_metadata_updates,
                    schema_metadata_updates,
                    field_metadata_updates,
                    ..
                },
                Self::UpdateConfig {
                    table_metadata_updates: other_table_metadata,
                    schema_metadata_updates: other_schema_metadata,
                    field_metadata_updates: other_field_metadata,
                    ..
                },
            ) => {
                if Self::update_maps_conflict(
                    table_metadata_updates.as_ref(),
                    other_table_metadata.as_ref(),
                ) {
                    return true;
                }
                if schema_metadata_updates.is_some() && other_schema_metadata.is_some() {
                    return true;
                }
                if !field_metadata_updates.is_empty() && !other_field_metadata.is_empty() {
                    for field in field_metadata_updates.keys() {
                        if other_field_metadata.contains_key(field) {
                            return true;
                        }
                    }
                }
                false
            }
            _ => false,
        }
    }

    fn update_maps_conflict(left: Option<&UpdateMap>, right: Option<&UpdateMap>) -> bool {
        let (Some(left), Some(right)) = (left, right) else {
            return false;
        };
        if left.replace || right.replace {
            return true;
        }
        let left_keys = left
            .update_entries
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<HashSet<_>>();
        right
            .update_entries
            .iter()
            .any(|entry| left_keys.contains(entry.key.as_str()))
    }

    /// Check whether another operation upserts a key that is referenced by another operation
    pub(crate) fn upsert_key_conflict(&self, other: &Self) -> bool {
        let self_upsert_keys = self.get_upsert_config_keys();
        let other_upsert_keys = other.get_upsert_config_keys();

        let self_delete_keys = self.get_delete_config_keys();
        let other_delete_keys = other.get_delete_config_keys();

        self_upsert_keys
            .iter()
            .any(|x| other_upsert_keys.contains(x) || other_delete_keys.contains(x))
            || other_upsert_keys
                .iter()
                .any(|x| self_upsert_keys.contains(x) || self_delete_keys.contains(x))
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Append { .. } => "Append",
            Self::Delete { .. } => "Delete",
            Self::Overwrite { .. } => "Overwrite",
            Self::CreateIndex { .. } => "CreateIndex",
            Self::Rewrite { .. } => "Rewrite",
            Self::Merge { .. } => "Merge",
            Self::ReserveFragments { .. } => "ReserveFragments",
            Self::Restore { .. } => "Restore",
            Self::Update { .. } => "Update",
            Self::Project { .. } => "Project",
            Self::UpdateConfig { .. } => "UpdateConfig",
            Self::DataReplacement { .. } => "DataReplacement",
            Self::DataOverlay { .. } => "DataOverlay",
            Self::UpdateMemWalState { .. } => "UpdateMemWalState",
            Self::Clone { .. } => "Clone",
            Self::UpdateBases { .. } => "UpdateBases",
        }
    }
}

/// Helper function to apply UpdateMap changes to a HashMap<String, String>
fn apply_update_map(
    target: &mut std::collections::HashMap<String, String>,
    update_map: &UpdateMap,
) {
    if update_map.replace {
        // Full replacement - clear existing and replace with new entries that have values
        target.clear();
        for entry in &update_map.update_entries {
            if let Some(value) = &entry.value {
                target.insert(entry.key.clone(), value.clone());
            }
        }
    } else {
        // Incremental update - merge entries
        for entry in &update_map.update_entries {
            if let Some(value) = &entry.value {
                target.insert(entry.key.clone(), value.clone());
            } else {
                target.remove(&entry.key);
            }
        }
    }
}

/// Helper function to translate old-style config updates to new UpdateMap format
pub fn translate_config_updates(
    upsert_values: &std::collections::HashMap<String, String>,
    delete_keys: &[String],
) -> UpdateMap {
    let mut update_entries = Vec::new();

    // Add upsert entries (with values)
    for (key, value) in upsert_values {
        update_entries.push(UpdateMapEntry {
            key: key.clone(),
            value: Some(value.clone()),
        });
    }

    // Add delete entries (without values)
    for key in delete_keys {
        update_entries.push(UpdateMapEntry {
            key: key.clone(),
            value: None,
        });
    }

    UpdateMap {
        update_entries,
        replace: false, // Old style was always incremental
    }
}

/// Helper function to translate old-style schema metadata to new UpdateMap format
pub fn translate_schema_metadata_updates(
    schema_metadata: &std::collections::HashMap<String, String>,
) -> UpdateMap {
    let update_entries = schema_metadata
        .iter()
        .map(|(key, value)| UpdateMapEntry {
            key: key.clone(),
            value: Some(value.clone()),
        })
        .collect();

    UpdateMap {
        update_entries,
        replace: true, // Old style schema metadata was full replacement
    }
}

impl From<&UpdateMap> for pb::transaction::UpdateMap {
    fn from(update_map: &UpdateMap) -> Self {
        Self {
            update_entries: update_map
                .update_entries
                .iter()
                .map(|entry| pb::transaction::UpdateMapEntry {
                    key: entry.key.clone(),
                    value: entry.value.clone(),
                })
                .collect(),
            replace: update_map.replace,
        }
    }
}

impl From<&pb::transaction::UpdateMap> for UpdateMap {
    fn from(pb_update_map: &pb::transaction::UpdateMap) -> Self {
        Self {
            update_entries: pb_update_map
                .update_entries
                .iter()
                .map(|entry| UpdateMapEntry {
                    key: entry.key.clone(),
                    value: entry.value.clone(),
                })
                .collect(),
            replace: pb_update_map.replace,
        }
    }
}

/// Add TransactionBuilder for flexibly setting option without using `mut`
pub struct TransactionBuilder {
    read_version: u64,
    // uuid is optional for builder since it can autogenerate
    uuid: Option<String>,
    operation: Operation,
    tag: Option<String>,
    transaction_properties: Option<Arc<HashMap<String, String>>>,
}

impl TransactionBuilder {
    pub fn new(read_version: u64, operation: Operation) -> Self {
        Self {
            read_version,
            uuid: None,
            operation,
            tag: None,
            transaction_properties: None,
        }
    }

    pub fn uuid(mut self, uuid: String) -> Self {
        self.uuid = Some(uuid);
        self
    }

    pub fn tag(mut self, tag: Option<String>) -> Self {
        self.tag = tag;
        self
    }

    pub fn transaction_properties(
        mut self,
        transaction_properties: Option<Arc<HashMap<String, String>>>,
    ) -> Self {
        self.transaction_properties = transaction_properties;
        self
    }

    pub fn build(self) -> Transaction {
        let uuid = self
            .uuid
            .unwrap_or_else(|| Uuid::new_v4().hyphenated().to_string());
        Transaction {
            read_version: self.read_version,
            uuid,
            operation: self.operation,
            tag: self.tag,
            transaction_properties: self.transaction_properties,
        }
    }
}

impl Transaction {
    pub fn new_from_version(read_version: u64, operation: Operation) -> Self {
        TransactionBuilder::new(read_version, operation).build()
    }

    pub fn new(read_version: u64, operation: Operation, tag: Option<String>) -> Self {
        TransactionBuilder::new(read_version, operation)
            .tag(tag)
            .build()
    }

    fn fragments_with_ids<'a, T>(
        new_fragments: T,
        fragment_id: &'a mut u64,
    ) -> impl Iterator<Item = Fragment> + 'a
    where
        T: IntoIterator<Item = Fragment> + 'a,
    {
        new_fragments.into_iter().map(move |mut f| {
            if f.id == 0 {
                f.id = *fragment_id;
                *fragment_id += 1;
            }
            f
        })
    }

    fn data_storage_format_from_files(
        fragments: &[Fragment],
        user_requested: Option<LanceFileVersion>,
    ) -> Result<DataStorageFormat> {
        if let Some(file_version) = Fragment::try_infer_version(fragments)? {
            // Ensure user-requested matches data files
            if let Some(user_requested) = user_requested
                && ConcreteFileVersion::from(user_requested) != file_version
            {
                return Err(Error::invalid_input(format!(
                    "User requested data storage version ({}) does not match version in data files ({})",
                    user_requested, file_version
                )));
            }
            Ok(DataStorageFormat::new(file_version))
        } else {
            // If no files use user-requested or default
            Ok(user_requested
                .map(ConcreteFileVersion::from)
                .map(DataStorageFormat::new)
                .unwrap_or_default())
        }
    }

    pub(crate) async fn restore_old_manifest(
        object_store: &ObjectStore,
        commit_handler: &dyn CommitHandler,
        base_path: &Path,
        version: u64,
        config: &ManifestWriteConfig,
        tx_path: &str,
        current_manifest: &Manifest,
    ) -> Result<(Manifest, Vec<IndexMetadata>)> {
        let location = commit_handler
            .resolve_version_location(base_path, version, &object_store.inner)
            .await?;
        let mut manifest = read_manifest(object_store, &location.path, location.size).await?;
        manifest.set_timestamp(timestamp_to_nanos(config.timestamp));
        manifest.transaction_file = Some(tx_path.to_string());
        let indices = read_manifest_indexes(object_store, &location, &manifest).await?;
        manifest.max_fragment_id = manifest
            .max_fragment_id
            .max(current_manifest.max_fragment_id);
        Ok((manifest, indices))
    }

    /// Create a new manifest from the current manifest and the transaction.
    ///
    /// `current_manifest` should only be None if the dataset does not yet exist.
    pub(crate) fn build_manifest(
        &self,
        current_manifest: Option<&Manifest>,
        current_indices: Vec<IndexMetadata>,
        transaction_file_path: &str,
        config: &ManifestWriteConfig,
    ) -> Result<(Manifest, Vec<IndexMetadata>)> {
        if config.use_stable_row_ids
            && current_manifest
                .map(|m| !m.uses_stable_row_ids())
                .unwrap_or_default()
        {
            return Err(Error::not_supported_source(
                "Cannot enable stable row ids on existing dataset".into(),
            ));
        }
        let mut reference_paths = match current_manifest {
            Some(m) => m.base_paths.clone(),
            None => HashMap::new(),
        };

        if let Operation::Overwrite {
            initial_bases: Some(initial_bases),
            ..
        } = &self.operation
        {
            if current_manifest.is_none() {
                // CREATE mode: registering base paths
                // Base IDs should have been assigned during write operation
                // Validate uniqueness and insert them into the manifest
                for base_path in initial_bases.iter() {
                    if reference_paths.contains_key(&base_path.id) {
                        return Err(Error::invalid_input(format!(
                            "Duplicate base path ID {} detected. Base path IDs must be unique.",
                            base_path.id
                        )));
                    }
                    reference_paths.insert(base_path.id, base_path.clone());
                }
            } else {
                // OVERWRITE mode with initial_bases should have been rejected by validation
                // This branch should never be reached
                return Err(Error::invalid_input(
                    "OVERWRITE mode cannot register new bases. This should have been caught by validation.",
                ));
            }
        }

        // Get the schema and the final fragment list
        let schema = match self.operation {
            Operation::Overwrite { ref schema, .. } => schema.clone(),
            Operation::Merge { ref schema, .. } => schema.clone(),
            Operation::Project { ref schema, .. } => schema.clone(),
            _ => {
                if let Some(current_manifest) = current_manifest {
                    current_manifest.schema.clone()
                } else {
                    return Err(Error::internal(
                        "Cannot create a new dataset without a schema".to_string(),
                    ));
                }
            }
        };

        // Fragment ids are a high water mark for the whole dataset history: an id
        // must never name two different sets of rows, or per-fragment state keyed
        // by id (caches, deletion files, row addresses) can be attributed to the
        // wrong rows.
        let mut fragment_id = current_manifest
            .and_then(|m| m.max_fragment_id())
            .map(|id| id + 1)
            .unwrap_or(0);
        let mut final_fragments = Vec::new();
        let mut final_indices = current_indices;

        let mut next_row_id = {
            // Only use row ids if the feature flag is set already or
            match (current_manifest, config.use_stable_row_ids) {
                (Some(manifest), _) if manifest.reader_feature_flags & FLAG_STABLE_ROW_IDS != 0 => {
                    Some(manifest.next_row_id)
                }
                (None, true) => Some(0),
                (_, false) => None,
                (Some(_), true) => {
                    return Err(Error::not_supported_source(
                        "Cannot enable stable row ids on existing dataset".into(),
                    ));
                }
            }
        };

        let maybe_existing_fragments =
            current_manifest
                .map(|m| m.fragments.as_ref())
                .ok_or_else(|| {
                    Error::internal(format!(
                        "No current manifest was provided while building manifest for operation {}",
                        self.operation.name()
                    ))
                });

        let new_version = current_manifest.map_or(1, |m| m.version + 1);

        match &self.operation {
            Operation::Clone { .. } => {
                return Err(Error::internal(
                    "Clone operation should not enter build_manifest.".to_string(),
                ));
            }
            Operation::Append { fragments } => {
                final_fragments.extend(maybe_existing_fragments?.clone());
                let mut new_fragments =
                    Self::fragments_with_ids(fragments.clone(), &mut fragment_id)
                        .collect::<Vec<_>>();
                if let Some(next_row_id) = &mut next_row_id {
                    Self::assign_row_ids(next_row_id, new_fragments.as_mut_slice())?;
                    // Add version metadata for all new fragments
                    for fragment in new_fragments.iter_mut() {
                        let version_meta = build_version_meta(fragment, new_version);
                        fragment.last_updated_at_version_meta = version_meta.clone();
                        fragment.created_at_version_meta = version_meta;
                    }
                }
                final_fragments.extend(new_fragments);
            }
            Operation::Delete {
                updated_fragments,
                deleted_fragment_ids,
                ..
            } => {
                // Remove the deleted fragments
                // Hash lookups keep this linear on tables with many fragments.
                let deleted_ids: HashSet<u64> = deleted_fragment_ids.iter().copied().collect();
                let updated_by_id: HashMap<u64, &Fragment> =
                    updated_fragments.iter().map(|f| (f.id, f)).collect();
                final_fragments.extend(maybe_existing_fragments?.clone());
                final_fragments.retain(|f| !deleted_ids.contains(&f.id));
                final_fragments.iter_mut().for_each(|f| {
                    if let Some(updated) = updated_by_id.get(&f.id) {
                        *f = (*updated).clone();
                    }
                });
                Self::retain_relevant_indices(&mut final_indices, &schema, &final_fragments)
            }
            Operation::Update {
                removed_fragment_ids,
                updated_fragments,
                new_fragments,
                fields_modified,
                compacted_sstables,
                fields_for_preserving_frag_bitmap,
                update_mode,
                updated_fragment_offsets,
                ..
            } => {
                // Extract existing fragments once for reuse
                let existing_fragments = maybe_existing_fragments?;

                // Apply updates to existing fragments
                // Hash lookups keep this linear on tables with many fragments.
                let removed_ids: HashSet<u64> = removed_fragment_ids.iter().copied().collect();
                let mut updated_by_id: HashMap<u64, &Fragment> =
                    HashMap::with_capacity(updated_fragments.len());
                for fragment in updated_fragments {
                    updated_by_id.entry(fragment.id).or_insert(fragment);
                }
                let updated_frags: Vec<Fragment> = existing_fragments
                    .iter()
                    .filter_map(|f| {
                        if removed_ids.contains(&f.id) {
                            return None;
                        }
                        if let Some(&updated) = updated_by_id.get(&f.id) {
                            let mut updated = updated.clone();
                            // Carry forward the fragment's current overlays (which
                            // may include ones added by a concurrent commit). An
                            // in-place column rewrite then tombstones the overlaid
                            // fields it rewrote, since the fresh base values
                            // supersede them.
                            updated.overlays = f.overlays.clone();
                            if matches!(update_mode, Some(RewriteColumns)) {
                                lance_table::format::overlay::tombstone_overlay_fields(
                                    &mut updated.overlays,
                                    fields_modified,
                                );
                            }
                            Some(updated)
                        } else {
                            Some(f.clone())
                        }
                    })
                    .collect();

                // Update version metadata for updated fragments if stable row IDs are enabled
                // Note: We don't update version metadata for fragments with deletion vectors
                // because the version sequences are indexed by physical row position, not logical position.
                // Version metadata for deleted rows will be filtered out during scan using the deletion vector.
                if next_row_id.is_some() {
                    // Version metadata will be properly set during compaction when deletions are materialized
                }

                final_fragments.extend(updated_frags);

                if next_row_id.is_some()
                    && matches!(update_mode, Some(RewriteColumns))
                    && let Some(UpdatedFragmentOffsets(off_map)) = updated_fragment_offsets
                    && !off_map.is_empty()
                {
                    let prev_version = current_manifest.map(|m| m.version).unwrap_or(0);
                    for fragment in final_fragments.iter_mut() {
                        let Some(bitmap) = off_map.get(&fragment.id) else {
                            continue;
                        };
                        if bitmap.is_empty() {
                            continue;
                        }
                        // Skip fragments with no existing version metadata: the helper
                        // would fill unmatched rows with prev_version, fabricating a
                        // last_updated stamp for rows that never had one.
                        if fragment.last_updated_at_version_meta.is_none() {
                            continue;
                        }
                        let offsets: Vec<usize> = bitmap.iter().map(|o| o as usize).collect();
                        lance_table::rowids::version::refresh_row_latest_update_meta_for_partial_frag_rewrite_cols(
                            fragment,
                            &offsets,
                            new_version,
                            prev_version,
                        )?;
                    }
                }

                // If we updated any fields, remove those fragments from indices covering those fields
                Self::prune_updated_fields_from_indices(
                    &mut final_indices,
                    updated_fragments,
                    fields_modified,
                );

                let mut new_fragments =
                    Self::fragments_with_ids(new_fragments.clone(), &mut fragment_id)
                        .collect::<Vec<_>>();

                // Assign row IDs to any fragments that don't have them yet
                // (e.g., inserted rows from merge_insert operations)
                if let Some(next_row_id) = &mut next_row_id {
                    Self::assign_row_ids(next_row_id, new_fragments.as_mut_slice())?;
                }

                if next_row_id.is_some() {
                    resolve_update_version_metadata(
                        existing_fragments,
                        new_fragments.as_mut_slice(),
                        new_version,
                    )?;
                }

                if config.use_stable_row_ids
                    && update_mode.is_some()
                    && *update_mode == Some(RewriteRows)
                {
                    let pure_updated_frag_ids =
                        Self::collect_pure_rewrite_row_update_frags_ids(&new_fragments)?;

                    // collect all the original frag ids that contains the updated rows
                    let original_fragment_ids: Vec<u64> = removed_fragment_ids
                        .iter()
                        .chain(updated_fragments.iter().map(|f| &f.id))
                        .copied()
                        .collect();

                    // The original fragments that carried an overlay: their moved rows may have a
                    // stale index entry (see `register_pure_rewrite_rows_update_frags_in_indices`).
                    // Reuse the hash lookups built above instead of scanning
                    // `original_fragment_ids` per fragment.
                    let original_overlaid_frags: HashMap<u32, &Fragment> = existing_fragments
                        .iter()
                        .filter(|f| {
                            (removed_ids.contains(&f.id) || updated_by_id.contains_key(&f.id))
                                && !f.overlays.is_empty()
                        })
                        .map(|f| (f.id as u32, f))
                        .collect();

                    Self::register_pure_rewrite_rows_update_frags_in_indices(
                        &mut final_indices,
                        &pure_updated_frag_ids,
                        &original_fragment_ids,
                        fields_for_preserving_frag_bitmap,
                        &original_overlaid_frags,
                        &schema,
                    )?;
                }

                if let Some(next_row_id) = &mut next_row_id {
                    Self::assign_row_ids(next_row_id, new_fragments.as_mut_slice())?;
                    // Note: Version metadata is already set above (lines 1627-1755)
                    // for Update operations, preserving created_at from original fragments.
                    // Don't overwrite it here.
                }
                // Identify fragments that were updated or newly created in this update
                let mut target_ids: HashSet<u64> = HashSet::new();
                target_ids.extend(new_fragments.iter().map(|f| f.id));
                final_fragments.extend(new_fragments);
                Self::retain_relevant_indices(&mut final_indices, &schema, &final_fragments);

                if !compacted_sstables.is_empty() {
                    update_mem_wal_index_compacted_sstables(
                        &mut final_indices,
                        new_version,
                        compacted_sstables.clone(),
                    )?;
                }
            }
            Operation::Overwrite { fragments, .. } => {
                // Every fragment in an overwrite is newly written, so all of them
                // take fresh ids regardless of the id they arrive with. Fragments
                // carried over from the dataset being replaced are rejected by
                // `validate_operation`, which is what makes ignoring the incoming
                // id safe here.
                let mut new_fragments = fragments.clone();
                for fragment in new_fragments.iter_mut() {
                    fragment.id = fragment_id;
                    fragment_id += 1;
                }
                if let Some(next_row_id) = &mut next_row_id {
                    Self::assign_row_ids(next_row_id, new_fragments.as_mut_slice())?;
                    // Add version metadata for all new fragments
                    for fragment in new_fragments.iter_mut() {
                        let version_meta = build_version_meta(fragment, new_version);
                        fragment.last_updated_at_version_meta = version_meta.clone();
                        fragment.created_at_version_meta = version_meta;
                    }
                }
                final_fragments.extend(new_fragments);
                final_indices = Vec::new();
            }
            Operation::Rewrite {
                groups,
                rewritten_indices,
                frag_reuse_index,
            } => {
                final_fragments.extend(maybe_existing_fragments?.clone());
                let current_version = current_manifest.map(|m| m.version).unwrap_or_default();
                Self::handle_rewrite_fragments(
                    &mut final_fragments,
                    groups,
                    &mut fragment_id,
                    current_version,
                    next_row_id.as_ref(),
                )?;

                if next_row_id.is_some() {
                    // We can re-use indices, but need to rewrite the fragment bitmaps
                    debug_assert!(rewritten_indices.is_empty());
                    for index in final_indices.iter_mut() {
                        let results_are_row_addrs = index_results_are_row_addrs(index);
                        if let Some(fragment_bitmap) = &mut index.fragment_bitmap {
                            *fragment_bitmap = if results_are_row_addrs {
                                // Stable row ids survive a rewrite, so a row-id-domain index
                                // can simply follow its data to the new fragments. An
                                // address-domain index cannot: its stored addresses point into
                                // the fragments the rewrite dropped. Claiming coverage of the
                                // new fragments would make it answer queries with addresses
                                // that no longer resolve, so drop the rewritten fragments from
                                // its coverage instead and let the scanner fall back to a full
                                // scan for them.
                                Self::drop_rewritten_fragments(fragment_bitmap, groups)
                            } else {
                                Self::recalculate_fragment_bitmap(fragment_bitmap, groups)?
                            };
                        }
                    }
                } else {
                    Self::handle_rewrite_indices(&mut final_indices, rewritten_indices, groups)?;
                }

                // A full compaction materializes a fragment's overlays into fresh
                // base data. Any index older than one of those overlays was built on
                // the pre-overlay values, so drop the rewritten fragment from its
                // coverage to keep it from serving stale values.
                Self::prune_overlay_stale_fields_from_indices(&mut final_indices, groups);

                if let Some(frag_reuse_index) = frag_reuse_index {
                    final_indices.retain(|idx| idx.name != frag_reuse_index.name);
                    final_indices.push(frag_reuse_index.clone());
                }
            }
            Operation::CreateIndex {
                new_indices,
                removed_indices,
            } => {
                final_fragments.extend(maybe_existing_fragments?.clone());
                let removed_uuids = removed_indices
                    .iter()
                    .map(|old_index| old_index.uuid)
                    .collect::<HashSet<_>>();
                let new_uuids = new_indices
                    .iter()
                    .map(|new_index| new_index.uuid)
                    .collect::<HashSet<_>>();
                final_indices.retain(|existing_index| {
                    !removed_uuids.contains(&existing_index.uuid)
                        && !new_uuids.contains(&existing_index.uuid)
                });
                final_indices.extend(new_indices.clone());
            }
            Operation::ReserveFragments { .. } | Operation::UpdateConfig { .. } => {
                final_fragments.extend(maybe_existing_fragments?.clone());
            }
            Operation::Merge { fragments, .. } => {
                let existing_fragments = maybe_existing_fragments?;
                let mut merged_fragments = fragments.clone();
                if next_row_id.is_some() {
                    let prev_by_id: HashMap<u64, &Fragment> =
                        existing_fragments.iter().map(|f| (f.id, f)).collect();
                    for fragment in merged_fragments.iter_mut() {
                        match prev_by_id.get(&fragment.id) {
                            Some(prev) => {
                                if merge_fragment_physically_rewritten(prev, fragment) {
                                    lance_table::rowids::version::refresh_row_latest_update_meta_for_full_frag_rewrite_cols(
                                        fragment,
                                        new_version,
                                    )?;
                                }
                            }
                            None => {
                                // Brand-new fragment ID not present in the previous manifest.
                                // Set both last_updated and created version meta, consistent
                                // with Append/Overwrite for genuinely new fragments.
                                lance_table::rowids::version::refresh_row_latest_update_meta_for_full_frag_rewrite_cols(
                                    fragment,
                                    new_version,
                                )?;
                                fragment.created_at_version_meta =
                                    fragment.last_updated_at_version_meta.clone();
                            }
                        }
                    }
                }
                final_fragments.extend(merged_fragments);

                // A Merge can rewrite a column's data file in place; the field stays
                // in the schema, so the index is retained -- prune its now-stale
                // entries for the rewritten fragments.
                Self::prune_merge_rewritten_fields_from_indices(
                    &mut final_indices,
                    existing_fragments,
                    fragments,
                );

                // Some fields that have indices may have been removed, so we should
                // remove those indices as well.
                Self::retain_relevant_indices(&mut final_indices, &schema, &final_fragments)
            }
            Operation::Project { .. } => {
                final_fragments.extend(maybe_existing_fragments?.clone());

                // We might have removed all fields for certain data files, so
                // we should remove the data files that are no longer relevant.
                let remaining_field_ids = schema
                    .fields_pre_order()
                    .map(|f| f.id)
                    .collect::<HashSet<_>>();
                for fragment in final_fragments.iter_mut() {
                    fragment.files.retain(|file| {
                        file.fields
                            .iter()
                            .any(|field_id| remaining_field_ids.contains(field_id))
                    });
                }

                // Some fields that have indices may have been removed, so we should
                // remove those indices as well.
                Self::retain_relevant_indices(&mut final_indices, &schema, &final_fragments)
            }
            Operation::Restore { .. } => {
                unreachable!()
            }
            Operation::DataReplacement { replacements } => {
                log::warn!(
                    "Building manifest with DataReplacement operation. This operation is not stable yet, please use with caution."
                );

                let (old_fragment_ids, new_datafiles): (Vec<&u64>, Vec<&DataFile>) = replacements
                    .iter()
                    .map(|DataReplacementGroup(fragment_id, new_file)| (fragment_id, new_file))
                    .unzip();

                // 1. make sure the new files all have the same fields / or empty
                // NOTE: arguably this requirement could be relaxed in the future
                // for the sake of simplicity, we require the new files to have the same fields
                if new_datafiles
                    .iter()
                    .map(|f| f.fields.clone())
                    .collect::<HashSet<_>>()
                    .len()
                    > 1
                {
                    let field_info = new_datafiles
                        .iter()
                        .enumerate()
                        .map(|(id, f)| (id, f.fields.clone()))
                        .fold("".to_string(), |acc, (id, fields)| {
                            format!("{}File {}: {:?}\n", acc, id, fields)
                        });

                    return Err(Error::invalid_input(format!(
                        "All new data files must have the same fields, but found different fields:\n{field_info}"
                    )));
                }

                let existing_fragments = maybe_existing_fragments?;

                // Collect replaced field IDs before consuming new_datafiles
                let replaced_fields: Vec<u32> = new_datafiles
                    .first()
                    .map(|f| {
                        f.fields
                            .iter()
                            .filter(|&&id| id >= 0)
                            .map(|&id| id as u32)
                            .collect()
                    })
                    .unwrap_or_default();

                // 2. check that the fragments being modified have isomorphic layouts along the columns being replaced
                // 3. add modified fragments to final_fragments
                for (frag_id, new_file) in old_fragment_ids.iter().zip(new_datafiles) {
                    let frag = existing_fragments
                        .iter()
                        .find(|f| f.id == **frag_id)
                        .ok_or_else(|| {
                            Error::invalid_input(
                                "Fragment being replaced not found in existing fragments",
                            )
                        })?;
                    let mut new_frag = frag.clone();

                    // TODO(rmeng): check new file and fragment are the same length

                    let mut columns_covered = HashSet::new();
                    for file in &mut new_frag.files {
                        if file.fields == new_file.fields
                            && file.file_major_version == new_file.file_major_version
                            && file.file_minor_version == new_file.file_minor_version
                        {
                            // assign the new file path / size / base to the fragment
                            file.path = new_file.path.clone();
                            file.file_size_bytes = new_file.file_size_bytes.clone();
                            file.base_id = new_file.base_id;
                        }
                        columns_covered.extend(file.fields.iter());
                    }
                    // SPECIAL CASE: if the column(s) being replaced are not covered by the fragment
                    // Then it means it's a all-NULL column that is being replaced with real data
                    // just add it to the final fragments. Push the DataFile as
                    // given so every field (including base_id) is preserved.
                    if columns_covered.is_disjoint(&new_file.fields.iter().collect()) {
                        new_file
                            .file_version()
                            .expect("Expected valid file version");
                        new_frag.files.push(new_file.clone());
                    }

                    // Nothing changed in the current fragment, which is not expected -- error out
                    if &new_frag == frag {
                        return Err(Error::invalid_input(
                            "Expected to modify the fragment but no changes were made. This means the new data files does not align with any exiting datafiles. Please check if the schema of the new data files matches the schema of the old data files including the file major and minor versions",
                        ));
                    }

                    // New base values for these fields supersede any overlay
                    // still shadowing them; tombstone the overlaid fields so the
                    // replacement is not silently masked.
                    lance_table::format::overlay::tombstone_overlay_fields(
                        &mut new_frag.overlays,
                        &replaced_fields,
                    );

                    final_fragments.push(new_frag);
                }

                let fragments_changed = old_fragment_ids
                    .iter()
                    .cloned()
                    .cloned()
                    .collect::<HashSet<_>>();

                // 4. push fragments that didn't change back to final_fragments
                let unmodified_fragments = existing_fragments
                    .iter()
                    .filter(|f| !fragments_changed.contains(&f.id))
                    .cloned()
                    .collect::<Vec<_>>();

                final_fragments.extend(unmodified_fragments);

                // 5. Invalidate index bitmaps for replaced fields
                let modified_fragments: Vec<Fragment> = final_fragments
                    .iter()
                    .filter(|f| fragments_changed.contains(&f.id))
                    .cloned()
                    .collect();

                // A replacement changes what its rows read as, so stamp them
                // updated. Without this, get_updated_rows never reports them and
                // an incremental consumer skips them for good.
                if next_row_id.is_some() {
                    let new_version = current_manifest.map_or(1, |m| m.version + 1);
                    for fragment in final_fragments
                        .iter_mut()
                        .filter(|f| fragments_changed.contains(&f.id))
                    {
                        lance_table::rowids::version::refresh_row_latest_update_meta_for_full_frag_rewrite_cols(
                            fragment,
                            new_version,
                        )?;
                    }
                }

                Self::prune_updated_fields_from_indices(
                    &mut final_indices,
                    &modified_fragments,
                    &replaced_fields,
                );
            }
            Operation::DataOverlay { groups } => {
                // Stamp each overlay with the version this commit is producing.
                // build_manifest re-runs on every retry with an updated
                // current_manifest, so this is naturally re-stamped on retry.
                let new_version = current_manifest.map_or(1, |m| m.version + 1);

                let existing_fragments = maybe_existing_fragments?;
                // Multiple groups may target the same fragment; merge them in
                // order rather than letting a HashMap collapse drop all but the
                // last group's overlays.
                let mut overlays_by_fragment: HashMap<u64, Vec<&DataOverlayFile>> = HashMap::new();
                for group in groups {
                    overlays_by_fragment
                        .entry(group.fragment_id)
                        .or_default()
                        .extend(group.overlays.iter());
                }

                // Every group must target an existing fragment. Build a set of
                // existing ids once so this is O(groups + fragments) rather than
                // O(groups * fragments).
                let existing_fragment_ids: HashSet<u64> =
                    existing_fragments.iter().map(|f| f.id).collect();
                for fragment_id in overlays_by_fragment.keys() {
                    if !existing_fragment_ids.contains(fragment_id) {
                        return Err(Error::invalid_input(format!(
                            "DataOverlay targets fragment {fragment_id}, which does not exist"
                        )));
                    }
                }

                for fragment in existing_fragments {
                    let mut fragment = fragment.clone();
                    if let Some(new_overlays) = overlays_by_fragment.get(&fragment.id) {
                        // Appended (not replaced) so concurrently-written overlays
                        // survive; later entries are newer.
                        fragment
                            .overlays
                            .extend(new_overlays.iter().map(|&overlay| {
                                let mut overlay = overlay.clone();
                                overlay.committed_version = new_version;
                                overlay
                            }));
                    }
                    final_fragments.push(fragment);
                }
            }
            Operation::UpdateMemWalState { compacted_sstables } => {
                update_mem_wal_index_compacted_sstables(
                    &mut final_indices,
                    new_version,
                    compacted_sstables.clone(),
                )?;
            }
            Operation::UpdateBases { .. } => {
                // UpdateBases operation doesn't modify fragments or indices
                // Base paths are handled in the manifest creation section below
                final_fragments.extend(maybe_existing_fragments?.clone());
            }
        };

        // If a fragment was reserved then it may not belong at the end of the fragments list.
        final_fragments.sort_by_key(|frag| frag.id);

        // Clean up data files that only contain tombstoned fields
        Self::remove_tombstoned_data_files(&mut final_fragments);

        // Enforce the newest-last overlay ordering invariant at the write
        // boundary. Load normalizes with a sort; this rejects any commit path
        // that assembled a fragment's overlays out of order.
        for fragment in &final_fragments {
            if !fragment.overlays.is_empty() {
                lance_table::format::overlay::verify_overlays_newest_last(&fragment.overlays)?;
            }
        }

        let user_requested_version = match (&config.storage_format, config.use_legacy_format) {
            (Some(storage_format), _) => Some(storage_format.lance_file_version()?),
            (None, Some(true)) => Some(LanceFileVersion::Legacy),
            (None, Some(false)) => Some(LanceFileVersion::V2_0),
            (None, None) => None,
        };

        let mut manifest = if let Some(current_manifest) = current_manifest {
            // OVERWRITE with initial_bases on existing dataset is not allowed (caught by validation)
            // So we always use new_from_previous which preserves base_paths
            let mut prev_manifest =
                Manifest::new_from_previous(current_manifest, schema, Arc::new(final_fragments));

            if let (Some(user_requested_version), Operation::Overwrite { .. }) =
                (user_requested_version, &self.operation)
            {
                // If this is an overwrite operation and the user has requested a specific version
                // then overwrite with that version.  Otherwise, if the user didn't request a specific
                // version, then overwrite with whatever version we had before.
                prev_manifest.data_storage_format =
                    DataStorageFormat::new(ConcreteFileVersion::from(user_requested_version));
            }

            prev_manifest
        } else {
            let data_storage_format =
                Self::data_storage_format_from_files(&final_fragments, user_requested_version)?;
            Manifest::new(
                schema,
                Arc::new(final_fragments),
                data_storage_format,
                reference_paths,
            )
        };

        manifest.tag.clone_from(&self.tag);

        if config.auto_set_feature_flags {
            // Internal operations (e.g. CreateIndex) use ManifestWriteConfig::default()
            // which has use_stable_row_ids = false. Without inheriting from the previous
            // manifest, apply_feature_flags would clear FLAG_STABLE_ROW_IDS.
            let inherited = current_manifest
                .map(|m| m.uses_stable_row_ids())
                .unwrap_or(false);
            let use_stable_row_ids = config.use_stable_row_ids || inherited;
            apply_feature_flags(
                &mut manifest,
                use_stable_row_ids,
                config.disable_transaction_file,
            )?;
        }
        manifest.set_timestamp(timestamp_to_nanos(config.timestamp));

        manifest.update_max_fragment_id();

        match &self.operation {
            Operation::Overwrite {
                config_upsert_values: Some(tm),
                ..
            } => {
                manifest.config_mut().extend(tm.clone());
            }
            Operation::UpdateConfig {
                config_updates,
                table_metadata_updates,
                schema_metadata_updates,
                field_metadata_updates,
            } => {
                if let Some(config_updates) = config_updates {
                    let mut config = manifest.config.clone();
                    apply_update_map(&mut config, config_updates);
                    manifest.config = config;
                }
                if let Some(table_metadata_updates) = table_metadata_updates {
                    let mut table_metadata = manifest.table_metadata.clone();
                    apply_update_map(&mut table_metadata, table_metadata_updates);
                    manifest.table_metadata = table_metadata;
                }
                if let Some(schema_metadata_updates) = schema_metadata_updates {
                    let mut schema_metadata = manifest.schema.metadata.clone();
                    apply_update_map(&mut schema_metadata, schema_metadata_updates);
                    manifest.schema.metadata = schema_metadata;
                }
                // The unenforced primary and clustering keys are reserved
                // schema properties: each is immutable once set, and its
                // reserved metadata keys cannot be written with an invalid
                // value. Capture the prior keys, and whether this transaction
                // writes a reserved key, before applying the updates so
                // violations can be rejected below. This runs on every apply,
                // including conflict-rebase, so it also rejects the
                // concurrent-writer race.
                let primary_key_before: Vec<i32> = manifest
                    .schema
                    .unenforced_primary_key()
                    .iter()
                    .map(|field| field.id)
                    .collect();
                let writes_primary_key = field_metadata_updates.values().any(|update| {
                    update.update_entries.iter().any(|entry| {
                        entry.key == LANCE_UNENFORCED_PRIMARY_KEY
                            || entry.key == LANCE_UNENFORCED_PRIMARY_KEY_POSITION
                    })
                });
                let clustering_key_before: Vec<i32> = manifest
                    .schema
                    .unenforced_clustering_key()
                    .iter()
                    .map(|field| field.id)
                    .collect();
                let writes_clustering_key = field_metadata_updates.values().any(|update| {
                    update
                        .update_entries
                        .iter()
                        .any(|entry| entry.key == LANCE_UNENFORCED_CLUSTERING_KEY_POSITION)
                });
                for (field_id, field_metadata_update) in field_metadata_updates {
                    if let Some(field) = manifest.schema.field_by_id_mut(*field_id) {
                        apply_update_map(&mut field.metadata, field_metadata_update);
                        // Also set unenforced primary key based on updated field metadata.
                        field.unenforced_primary_key_position = field
                            .metadata
                            .get(LANCE_UNENFORCED_PRIMARY_KEY_POSITION)
                            .and_then(|s| s.parse::<u32>().ok())
                            .or_else(|| {
                                field
                                    .metadata
                                    .get(LANCE_UNENFORCED_PRIMARY_KEY)
                                    .filter(|s| {
                                        matches!(s.to_lowercase().as_str(), "true" | "1" | "yes")
                                    })
                                    .map(|_| 0)
                            });
                        // Also set unenforced clustering key based on updated
                        // field metadata.
                        field.unenforced_clustering_key_position = field
                            .metadata
                            .get(LANCE_UNENFORCED_CLUSTERING_KEY_POSITION)
                            .and_then(|s| s.parse::<u32>().ok());
                    } else {
                        return Err(Error::invalid_input_source(
                            format!("Field with id {} does not exist", field_id).into(),
                        ));
                    }
                }
                let primary_key_after: Vec<i32> = manifest
                    .schema
                    .unenforced_primary_key()
                    .iter()
                    .map(|field| field.id)
                    .collect();
                if !primary_key_before.is_empty() {
                    // The primary key is already set: reject any change to it,
                    // and any write that touches a reserved primary key.
                    if writes_primary_key || primary_key_after != primary_key_before {
                        return Err(Error::invalid_input(
                            "the unenforced primary key is a reserved key and cannot be changed once set",
                        ));
                    }
                } else if writes_primary_key && primary_key_after.is_empty() {
                    // A reserved primary key was written but did not install a
                    // valid primary key (e.g. a non-marker flag value or a
                    // non-numeric position).
                    return Err(Error::invalid_input(
                        "the unenforced primary key is a reserved key and cannot be set to an invalid value",
                    ));
                }
                let clustering_key_after: Vec<i32> = manifest
                    .schema
                    .unenforced_clustering_key()
                    .iter()
                    .map(|field| field.id)
                    .collect();
                if !clustering_key_before.is_empty() {
                    // The clustering key is already set: reject any change to
                    // it, and any write that touches the reserved key.
                    if writes_clustering_key || clustering_key_after != clustering_key_before {
                        return Err(Error::invalid_input(
                            "the unenforced clustering key is a reserved key and cannot be changed once set",
                        ));
                    }
                } else if writes_clustering_key && clustering_key_after.is_empty() {
                    // The reserved clustering key was written but did not
                    // install a valid clustering key (e.g. a non-numeric
                    // position value).
                    return Err(Error::invalid_input(
                        "the unenforced clustering key is a reserved key and cannot be set to an invalid value",
                    ));
                }
            }
            _ => {}
        }

        // Handle UpdateBases operation to update manifest base_paths
        if let Operation::UpdateBases { new_bases } = &self.operation {
            // Validate and add new base paths to the manifest
            for new_base in new_bases {
                // Check for conflicts with existing base paths
                if let Some(existing_base) = manifest
                    .base_paths
                    .values()
                    .find(|bp| bp.name == new_base.name || bp.path == new_base.path)
                {
                    return Err(Error::invalid_input(format!(
                        "Conflict detected: Base path with name '{:?}' or path '{}' already exists. Existing: name='{:?}', path='{}'",
                        new_base.name, new_base.path, existing_base.name, existing_base.path
                    )));
                }

                // Assign a new ID if not already assigned
                let mut base_to_add = new_base.clone();
                if base_to_add.id == 0 {
                    let next_id = manifest
                        .base_paths
                        .keys()
                        .max()
                        .map(|&id| id + 1)
                        .unwrap_or(1);
                    base_to_add.id = next_id;
                }

                manifest.base_paths.insert(base_to_add.id, base_to_add);
            }
        }

        if let Operation::ReserveFragments { num_fragments } = self.operation {
            manifest.max_fragment_id = Some(manifest.max_fragment_id.unwrap_or(0) + num_fragments);
        }

        manifest.transaction_file = Some(transaction_file_path.to_string());

        if let Some(next_row_id) = next_row_id {
            manifest.next_row_id = next_row_id;
        }

        Ok((manifest, final_indices))
    }

    fn register_pure_rewrite_rows_update_frags_in_indices(
        indices: &mut [IndexMetadata],
        pure_update_frag_ids: &[u64],
        original_fragment_ids: &[u64],
        fields_for_preserving_frag_bitmap: &[u32],
        original_overlaid_frags: &HashMap<u32, &Fragment>,
        schema: &Schema,
    ) -> Result<()> {
        if pure_update_frag_ids.is_empty() {
            return Ok(());
        }

        let value_updated_field_set = fields_for_preserving_frag_bitmap
            .iter()
            .collect::<HashSet<_>>();

        for index in indices.iter_mut() {
            // Physical row addresses cannot follow moved rows into a new fragment.
            // Leave that fragment uncovered so the scanner reads it directly.
            if index_results_are_row_addrs(index) {
                continue;
            }
            let index_covers_modified_field = index.fields.iter().any(|field_id| {
                value_updated_field_set.contains(&u32::try_from(*field_id).unwrap())
            });
            if index_covers_modified_field {
                continue;
            }
            let Some(fragment_bitmap) = index.fragment_bitmap.as_ref() else {
                continue;
            };

            // Check that all the original fragments containing the updated rows are covered by
            // the index. If not, some updated rows were not indexed, so we cannot index them.
            let index_covers_all_original_fragments = original_fragment_ids
                .iter()
                .all(|&fragment_id| fragment_bitmap.contains(fragment_id as u32));
            if !index_covers_all_original_fragments {
                continue;
            }

            // A rewrite materializes overlays.  If any of those overlays touched the
            // column being indexed then the rewrite will modify that column.  As a
            // result, that index will no longer cover the fragment and it does not
            // count as a pure rewrite and we must exclude it from the index's fragment
            // bitmap.
            let mut overlay_stale = RoaringBitmap::new();
            collect_overlay_stale_frags(
                index,
                original_overlaid_frags,
                &mut overlay_stale,
                schema,
            )?;
            if !overlay_stale.is_empty() {
                continue;
            }

            if let Some(fragment_bitmap) = index.fragment_bitmap.as_mut() {
                for fragment_id in pure_update_frag_ids.iter().map(|f| *f as u32) {
                    fragment_bitmap.insert(fragment_id);
                }
            }
        }
        Ok(())
    }

    /// If an operation modifies one or more fields in a fragment then we need to remove
    /// that fragment from any indices that cover one of the modified fields.
    pub(crate) fn prune_updated_fields_from_indices(
        indices: &mut [IndexMetadata],
        updated_fragments: &[Fragment],
        fields_modified: &[u32],
    ) {
        if fields_modified.is_empty() {
            return;
        }

        // If we modified any fields in the fragments then we need to remove those fragments
        // from the index if the index covers one of those modified fields.
        let fields_modified_set = fields_modified.iter().collect::<HashSet<_>>();
        for index in indices.iter_mut() {
            if index
                .fields
                .iter()
                .any(|field_id| fields_modified_set.contains(&u32::try_from(*field_id).unwrap()))
                && let Some(fragment_bitmap) = &mut index.fragment_bitmap
            {
                for fragment_id in updated_fragments.iter().map(|f| f.id as u32) {
                    fragment_bitmap.remove(fragment_id);
                }
            }
        }
    }

    /// Map each (non-tombstoned) field id in a fragment to the path of the data
    /// file that backs it.
    fn fragment_field_paths(frag: &Fragment) -> HashMap<i32, &str> {
        let mut map = HashMap::new();
        for file in &frag.files {
            for &field_id in file.fields.iter() {
                if field_id >= 0 {
                    map.insert(field_id, file.path.as_str());
                }
            }
        }
        map
    }

    /// A `Merge` can rewrite a column's data *in place* -- the field stays in the
    /// schema but its backing data file changes (the overlay fragment carries a new
    /// file for the field and tombstones its old field id). `retain_relevant_indices`
    /// only drops indices for *removed* fields, so without this the index keeps
    /// covering the rewritten fragments with stale entries. Remove each such fragment
    /// from any index covering a field whose backing data file changed.
    fn prune_merge_rewritten_fields_from_indices(
        indices: &mut [IndexMetadata],
        prev_fragments: &[Fragment],
        new_fragments: &[Fragment],
    ) {
        let prev_by_id: HashMap<u64, &Fragment> =
            prev_fragments.iter().map(|f| (f.id, f)).collect();
        for new_frag in new_fragments {
            let Some(prev) = prev_by_id.get(&new_frag.id) else {
                continue; // brand-new fragment: nothing stale to prune
            };
            let prev_paths = Self::fragment_field_paths(prev);
            let new_paths = Self::fragment_field_paths(new_frag);
            // Fields still present whose backing file path changed == rewritten data.
            let changed: Vec<u32> = prev_paths
                .iter()
                .filter(|(field_id, prev_path)| {
                    new_paths
                        .get(*field_id)
                        .is_some_and(|new_path| new_path != *prev_path)
                })
                .map(|(field_id, _)| *field_id as u32)
                .collect();
            if changed.is_empty() {
                continue;
            }
            Self::prune_updated_fields_from_indices(
                indices,
                std::slice::from_ref(new_frag),
                &changed,
            );
        }
    }

    /// After a `Rewrite` fully compacts a fragment, its data overlays are baked
    /// into the new fragment's base data. An index built *before* one of those
    /// overlays (`overlay.committed_version > index.dataset_version`) indexed the
    /// stale pre-overlay values -- and unlike a live overlay, the compacted
    /// fragment no longer signals that staleness to the query path. Drop each
    /// rewritten (new) fragment from the coverage of any index covering a field
    /// such an overlay supplied, so those rows fall back to a flat scan.
    fn prune_overlay_stale_fields_from_indices(
        indices: &mut [IndexMetadata],
        groups: &[RewriteGroup],
    ) {
        for group in groups {
            // field id -> newest overlay committed_version supplying that field
            let mut overlaid_field_versions: HashMap<i32, u64> = HashMap::new();
            for old_frag in &group.old_fragments {
                for overlay in &old_frag.overlays {
                    for &field_id in overlay.data_file.fields.iter() {
                        if field_id < 0 {
                            // Tombstoned (obsolete) overlay field: supplies nothing.
                            continue;
                        }
                        let entry = overlaid_field_versions.entry(field_id).or_insert(0);
                        *entry = (*entry).max(overlay.committed_version);
                    }
                }
            }
            if overlaid_field_versions.is_empty() {
                continue;
            }

            let new_fragment_ids = group
                .new_fragments
                .iter()
                .map(|f| f.id as u32)
                .collect::<Vec<_>>();
            for index in indices.iter_mut() {
                let is_stale = index.fields.iter().any(|field_id| {
                    overlaid_field_versions
                        .get(field_id)
                        .is_some_and(|&overlay_version| overlay_version > index.dataset_version)
                });
                if is_stale && let Some(fragment_bitmap) = &mut index.fragment_bitmap {
                    for new_id in &new_fragment_ids {
                        fragment_bitmap.remove(*new_id);
                    }
                }
            }
        }
    }

    /// Remove data files that only contain tombstoned fields (-2)
    /// These files no longer contain any live data and can be safely dropped
    fn remove_tombstoned_data_files(fragments: &mut [Fragment]) {
        for fragment in fragments {
            fragment.files.retain(|file| {
                // Keep file if it has at least one non-tombstoned field
                file.fields.iter().any(|&field_id| field_id != -2)
            });
        }
    }

    fn retain_relevant_indices(
        indices: &mut Vec<IndexMetadata>,
        schema: &Schema,
        fragments: &[Fragment],
    ) {
        let field_ids = schema
            .fields_pre_order()
            .map(|f| f.id)
            .collect::<HashSet<_>>();

        // Remove indices for fields no longer in schema
        indices.retain(|existing_index| {
            existing_index
                .fields
                .iter()
                .all(|field_id| field_ids.contains(field_id))
                || is_system_index(existing_index)
        });

        // Fragment bitmaps record which fragments the index was originally built for.
        // Operations like updates and data replacement prune these bitmaps, and
        // effective_fragment_bitmap intersects with existing fragments at query time.

        // Apply retention logic for indices with empty bitmaps per index name
        // (except for fragment reuse indices which are always kept)
        let mut indices_by_name: std::collections::HashMap<String, Vec<&IndexMetadata>> =
            std::collections::HashMap::new();

        // Group indices by name
        for index in indices.iter() {
            if index.name != FRAG_REUSE_INDEX_NAME {
                indices_by_name
                    .entry(index.name.clone())
                    .or_default()
                    .push(index);
            }
        }

        // Build a set of UUIDs to keep based on retention rules
        let mut uuids_to_keep = std::collections::HashSet::new();

        let existing_fragments = fragments
            .iter()
            .map(|f| f.id as u32)
            .collect::<RoaringBitmap>();

        // For each group of indices with the same name
        for (_, same_name_indices) in indices_by_name {
            if same_name_indices.len() > 1 {
                // Separate empty and non-empty indices
                let (empty_indices, non_empty_indices): (Vec<_>, Vec<_>) =
                    same_name_indices.iter().partition(|index| {
                        index
                            .effective_fragment_bitmap(&existing_fragments)
                            .as_ref()
                            .is_none_or(|bitmap| bitmap.is_empty())
                    });

                if non_empty_indices.is_empty() {
                    // All indices are empty -- keep only the oldest definition.
                    //
                    // An empty index definition is still correct: the scanner
                    // falls back to scanning unindexed fragments, and normal
                    // index maintenance rebuilds coverage once rows accrue.
                    // Dropping the definition instead would silently lose the
                    // index whenever an operation replaces every fragment it
                    // covered (e.g. a full table rewrite), leaving the dataset
                    // without its declared index.
                    let mut sorted_indices = empty_indices;
                    sorted_indices.sort_by_key(|index: &&IndexMetadata| index.dataset_version); // Sort by ascending dataset_version

                    if let Some(oldest) = sorted_indices.first() {
                        uuids_to_keep.insert(oldest.uuid);
                    }
                } else {
                    // At least one index has non-empty bitmap - keep all non-empty indices
                    for index in non_empty_indices {
                        uuids_to_keep.insert(index.uuid);
                    }
                }
            } else {
                // Single index whose column is still in schema: keep it, even
                // when its coverage is empty (see the all-empty note above).
                if let Some(index) = same_name_indices.first() {
                    uuids_to_keep.insert(index.uuid);
                }
            }
        }

        // Use Vec::retain to safely remove indices
        indices.retain(|index| {
            index.name == FRAG_REUSE_INDEX_NAME || uuids_to_keep.contains(&index.uuid)
        });
    }

    fn recalculate_fragment_bitmap(
        old: &RoaringBitmap,
        groups: &[RewriteGroup],
    ) -> Result<RoaringBitmap> {
        let mut new_bitmap = old.clone();
        for group in groups {
            let any_in_index = group
                .old_fragments
                .iter()
                .any(|frag| old.contains(frag.id as u32));
            let all_in_index = group
                .old_fragments
                .iter()
                .all(|frag| old.contains(frag.id as u32));
            // Any rewrite group may or may not be covered by the index.  However, if any fragment
            // in a rewrite group was previously covered by the index then all fragments in the rewrite
            // group must have been previously covered by the index.  plan_compaction takes care of
            // this for us so this should be safe to assume.
            if any_in_index {
                if all_in_index {
                    for frag_id in group.old_fragments.iter().map(|frag| frag.id as u32) {
                        new_bitmap.remove(frag_id);
                    }
                    new_bitmap.extend(group.new_fragments.iter().map(|frag| frag.id as u32));
                } else {
                    return Err(Error::invalid_input(
                        "The compaction plan included a rewrite group that was a split of indexed and non-indexed data",
                    ));
                }
            }
        }
        Ok(new_bitmap)
    }

    /// Coverage of an index that a rewrite invalidates: the rewritten fragments are
    /// removed and the fragments they became are *not* added.
    fn drop_rewritten_fragments(old: &RoaringBitmap, groups: &[RewriteGroup]) -> RoaringBitmap {
        let mut new_bitmap = old.clone();
        for group in groups {
            for old_fragment in &group.old_fragments {
                new_bitmap.remove(old_fragment.id as u32);
            }
        }
        new_bitmap
    }

    fn handle_rewrite_indices(
        indices: &mut [IndexMetadata],
        rewritten_indices: &[RewrittenIndex],
        groups: &[RewriteGroup],
    ) -> Result<()> {
        let mut modified_indices = HashSet::new();

        for rewritten_index in rewritten_indices {
            if !modified_indices.insert(rewritten_index.old_id) {
                return Err(Error::invalid_input(format!(
                    "An invalid compaction plan must have been generated because multiple tasks modified the same index: {}",
                    rewritten_index.old_id
                )));
            }

            // Skip indices that no longer exist (may have been removed by concurrent operation)
            let Some(index) = indices
                .iter_mut()
                .find(|idx| idx.uuid == rewritten_index.old_id)
            else {
                continue;
            };

            index.fragment_bitmap = Some(Self::recalculate_fragment_bitmap(
                index.fragment_bitmap.as_ref().ok_or_else(|| {
                    Error::invalid_input(format!(
                        "Cannot rewrite index {} which did not store fragment bitmap",
                        index.uuid
                    ))
                })?,
                groups,
            )?);
            index.uuid = rewritten_index.new_id;
            // Update file sizes to match the new index files. When not available
            // (e.g., from older writers), clear the old file sizes to avoid
            // using stale sizes from the pre-remap index.
            index.files = rewritten_index.new_index_files.clone();
        }
        Ok(())
    }

    fn handle_rewrite_fragments(
        final_fragments: &mut Vec<Fragment>,
        groups: &[RewriteGroup],
        fragment_id: &mut u64,
        version: u64,
        _next_row_id: Option<&u64>,
    ) -> Result<()> {
        for group in groups {
            // If the old fragments are contiguous, find the range
            let replace_range = {
                let start = final_fragments
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.id == group.old_fragments[0].id)
                    .ok_or_else(|| {
                        Error::commit_conflict_source(
                            version,
                            format!(
                                "dataset does not contain a fragment a rewrite operation wants to replace: id={}",
                                group.old_fragments[0].id
                            )
                            .into(),
                        )
                    })?
                    .0;

                // Verify old_fragments matches contiguous range
                let mut i = 1;
                loop {
                    if i == group.old_fragments.len() {
                        break Some(start..start + i);
                    }
                    if final_fragments[start + i].id != group.old_fragments[i].id {
                        break None;
                    }
                    i += 1;
                }
            };

            let new_fragments = Self::fragments_with_ids(group.new_fragments.clone(), fragment_id)
                .collect::<Vec<_>>();

            // Version metadata for rewritten fragments is handled by the compaction code
            // (recalc_versions_for_rewritten_fragments) which preserves version information
            // from the original fragments. We don't modify it here.

            if let Some(replace_range) = replace_range {
                // Efficiently path using slice
                final_fragments.splice(replace_range, new_fragments);
            } else {
                // Slower path for non-contiguous ranges
                for fragment in group.old_fragments.iter() {
                    final_fragments.retain(|f| f.id != fragment.id);
                }
                final_fragments.extend(new_fragments);
            }
        }
        Ok(())
    }

    /// collect the pure(the num of row IDs are equal to the physical rows) "rewrite rows" updated fragment ids
    fn collect_pure_rewrite_row_update_frags_ids(fragments: &[Fragment]) -> Result<Vec<u64>> {
        let mut pure_update_frag_ids = Vec::new();

        for fragment in fragments {
            let physical_rows = fragment
                .physical_rows
                .ok_or_else(|| Error::internal("Fragment does not have physical rows"))?
                as u64;

            if let Some(row_id_meta) = &fragment.row_id_meta {
                let existing_row_count = match row_id_meta {
                    RowIdMeta::Inline(data) => {
                        let sequence = read_row_ids(data)?;
                        sequence.len() as u64
                    }
                    _ => 0,
                };

                // only filter the fragments that match: all the rows have row id,
                // which means it does not contain inserted rows in this fragment
                if existing_row_count == physical_rows {
                    pure_update_frag_ids.push(fragment.id);
                }
            }
        }

        Ok(pure_update_frag_ids)
    }

    fn assign_row_ids(next_row_id: &mut u64, fragments: &mut [Fragment]) -> Result<()> {
        for fragment in fragments {
            let physical_rows = fragment
                .physical_rows
                .ok_or_else(|| Error::internal("Fragment does not have physical rows"))?
                as u64;

            if fragment.row_id_meta.is_some() {
                // we may meet merge insert case, it only has partial row ids.
                // so here, we need to check if the row ids match the physical rows
                // if yes, continue
                // if not, fill the remaining row ids to the physical rows, then update row_id_meta

                // Check if existing row IDs match the physical rows count
                let existing_row_count = match &fragment.row_id_meta {
                    Some(RowIdMeta::Inline(data)) => {
                        // Parse the serialized row ID sequence to get the count
                        let sequence = read_row_ids(data)?;
                        sequence.len() as u64
                    }
                    _ => 0,
                };

                match existing_row_count.cmp(&physical_rows) {
                    Ordering::Equal => {
                        // Row IDs already match physical rows, continue to next fragment
                        continue;
                    }
                    Ordering::Less => {
                        // Partial row IDs - need to fill the remaining ones
                        let remaining_rows = physical_rows - existing_row_count;
                        let new_row_ids = *next_row_id..(*next_row_id + remaining_rows);

                        // Merge existing and new row IDs
                        let combined_sequence = match &fragment.row_id_meta {
                            Some(RowIdMeta::Inline(data)) => read_row_ids(data)?,
                            _ => {
                                return Err(Error::internal(
                                    "Failed to deserialize existing row ID sequence",
                                ));
                            }
                        };

                        let mut row_ids: Vec<u64> = combined_sequence.iter().collect();
                        for row_id in new_row_ids {
                            row_ids.push(row_id);
                        }
                        let combined_sequence = RowIdSequence::from(row_ids.as_slice());

                        let serialized = write_row_ids(&combined_sequence);
                        fragment.row_id_meta = Some(RowIdMeta::Inline(serialized));
                        *next_row_id += remaining_rows;
                    }
                    Ordering::Greater => {
                        // More row IDs than physical rows - this shouldn't happen
                        return Err(Error::internal(format!(
                            "Fragment has more row IDs ({}) than physical rows ({})",
                            existing_row_count, physical_rows
                        )));
                    }
                }
            } else {
                let row_ids = *next_row_id..(*next_row_id + physical_rows);
                let sequence = RowIdSequence::from(row_ids);
                // TODO: write to a separate file if large. Possibly share a file with other fragments.
                let serialized = write_row_ids(&sequence);
                fragment.row_id_meta = Some(RowIdMeta::Inline(serialized));
                *next_row_id += physical_rows;
            }
        }
        Ok(())
    }
}

impl From<&DataReplacementGroup> for pb::transaction::DataReplacementGroup {
    fn from(DataReplacementGroup(fragment_id, new_file): &DataReplacementGroup) -> Self {
        Self {
            fragment_id: *fragment_id,
            new_file: Some(new_file.into()),
        }
    }
}

/// Convert a protobug DataReplacementGroup to a rust native DataReplacementGroup
/// this is unfortunately TryFrom instead of From because of the Option in the pb::DataReplacementGroup
impl TryFrom<pb::transaction::DataReplacementGroup> for DataReplacementGroup {
    type Error = Error;

    fn try_from(message: pb::transaction::DataReplacementGroup) -> Result<Self> {
        Ok(Self(
            message.fragment_id,
            message
                .new_file
                .ok_or(Error::invalid_input(
                    "DataReplacementGroup must have a new_file",
                ))?
                .try_into()?,
        ))
    }
}

impl From<&DataOverlayGroup> for pb::transaction::DataOverlayGroup {
    fn from(group: &DataOverlayGroup) -> Self {
        Self {
            fragment_id: group.fragment_id,
            overlays: group
                .overlays
                .iter()
                .map(pb::DataOverlayFile::from)
                .collect(),
        }
    }
}

impl TryFrom<pb::transaction::DataOverlayGroup> for DataOverlayGroup {
    type Error = Error;

    fn try_from(message: pb::transaction::DataOverlayGroup) -> Result<Self> {
        Ok(Self {
            fragment_id: message.fragment_id,
            overlays: message
                .overlays
                .into_iter()
                .map(DataOverlayFile::try_from)
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl TryFrom<pb::Transaction> for Transaction {
    type Error = Error;

    fn try_from(message: pb::Transaction) -> Result<Self> {
        let operation = match message.operation {
            Some(pb::transaction::Operation::Append(pb::transaction::Append { fragments })) => {
                Operation::Append {
                    fragments: fragments
                        .into_iter()
                        .map(Fragment::try_from)
                        .collect::<Result<Vec<_>>>()?,
                }
            }
            Some(pb::transaction::Operation::Clone(pb::transaction::Clone {
                is_shallow,
                ref_name,
                ref_version,
                ref_path,
                branch_name,
            })) => Operation::Clone {
                is_shallow,
                ref_name,
                ref_version,
                ref_path,
                branch_name,
            },
            Some(pb::transaction::Operation::Delete(pb::transaction::Delete {
                updated_fragments,
                deleted_fragment_ids,
                predicate,
            })) => Operation::Delete {
                updated_fragments: updated_fragments
                    .into_iter()
                    .map(Fragment::try_from)
                    .collect::<Result<Vec<_>>>()?,
                deleted_fragment_ids,
                predicate,
            },
            Some(pb::transaction::Operation::Overwrite(pb::transaction::Overwrite {
                fragments,
                schema,
                schema_metadata: _schema_metadata, // TODO: handle metadata
                config_upsert_values,
                initial_bases,
            })) => {
                let config_upsert_option = if config_upsert_values.is_empty() {
                    None
                } else {
                    Some(config_upsert_values)
                };

                Operation::Overwrite {
                    fragments: fragments
                        .into_iter()
                        .map(Fragment::try_from)
                        .collect::<Result<Vec<_>>>()?,
                    schema: Schema::try_from(&Fields(schema))?,
                    config_upsert_values: config_upsert_option,
                    initial_bases: if initial_bases.is_empty() {
                        None
                    } else {
                        Some(initial_bases.into_iter().map(BasePath::from).collect())
                    },
                }
            }
            Some(pb::transaction::Operation::ReserveFragments(
                pb::transaction::ReserveFragments { num_fragments },
            )) => Operation::ReserveFragments { num_fragments },
            Some(pb::transaction::Operation::Rewrite(pb::transaction::Rewrite {
                old_fragments,
                new_fragments,
                groups,
                rewritten_indices,
            })) => {
                let groups = if !groups.is_empty() {
                    groups
                        .into_iter()
                        .map(RewriteGroup::try_from)
                        .collect::<Result<_>>()?
                } else {
                    vec![RewriteGroup {
                        old_fragments: old_fragments
                            .into_iter()
                            .map(Fragment::try_from)
                            .collect::<Result<Vec<_>>>()?,
                        new_fragments: new_fragments
                            .into_iter()
                            .map(Fragment::try_from)
                            .collect::<Result<Vec<_>>>()?,
                    }]
                };
                let rewritten_indices = rewritten_indices
                    .iter()
                    .map(RewrittenIndex::try_from)
                    .collect::<Result<_>>()?;

                Operation::Rewrite {
                    groups,
                    rewritten_indices,
                    frag_reuse_index: None,
                }
            }
            Some(pb::transaction::Operation::CreateIndex(pb::transaction::CreateIndex {
                new_indices,
                removed_indices,
            })) => Operation::CreateIndex {
                new_indices: new_indices
                    .into_iter()
                    .map(IndexMetadata::try_from)
                    .collect::<Result<_>>()?,
                removed_indices: removed_indices
                    .into_iter()
                    .map(IndexMetadata::try_from)
                    .collect::<Result<_>>()?,
            },
            Some(pb::transaction::Operation::Merge(pb::transaction::Merge {
                fragments,
                schema,
                schema_metadata: _schema_metadata, // TODO: handle metadata
            })) => Operation::Merge {
                fragments: fragments
                    .into_iter()
                    .map(Fragment::try_from)
                    .collect::<Result<Vec<_>>>()?,
                schema: Schema::try_from(&Fields(schema))?,
            },
            Some(pb::transaction::Operation::Restore(pb::transaction::Restore { version })) => {
                Operation::Restore { version }
            }
            Some(pb::transaction::Operation::Update(pb::transaction::Update {
                removed_fragment_ids,
                updated_fragments,
                new_fragments,
                fields_modified,
                compacted_sstables,
                fields_for_preserving_frag_bitmap,
                update_mode,
                inserted_rows,
                updated_fragment_offsets,
            })) => Operation::Update {
                removed_fragment_ids,
                updated_fragments: updated_fragments
                    .into_iter()
                    .map(Fragment::try_from)
                    .collect::<Result<Vec<_>>>()?,
                new_fragments: new_fragments
                    .into_iter()
                    .map(Fragment::try_from)
                    .collect::<Result<Vec<_>>>()?,
                fields_modified,
                compacted_sstables: compacted_sstables
                    .into_iter()
                    .map(|m| CompactedSsTable::try_from(m).unwrap())
                    .collect(),
                fields_for_preserving_frag_bitmap,
                update_mode: match update_mode {
                    0 => Some(UpdateMode::RewriteRows),
                    1 => Some(UpdateMode::RewriteColumns),
                    _ => Some(UpdateMode::RewriteRows),
                },
                inserted_rows_filter: inserted_rows
                    .map(|ik| KeyExistenceFilter::try_from(&ik))
                    .transpose()?,
                updated_fragment_offsets: {
                    let m: HashMap<u64, RoaringBitmap> = updated_fragment_offsets
                        .into_iter()
                        .filter(|(_, list)| !list.values.is_empty())
                        .map(|(id, list)| (id, RoaringBitmap::from_iter(list.values)))
                        .collect();
                    if m.is_empty() {
                        None
                    } else {
                        Some(UpdatedFragmentOffsets(m))
                    }
                },
            },
            Some(pb::transaction::Operation::Project(pb::transaction::Project { schema })) => {
                Operation::Project {
                    schema: Schema::try_from(&Fields(schema))?,
                }
            }
            Some(pb::transaction::Operation::UpdateConfig(update_config)) => {
                // Check if new-style fields are present
                let has_new_fields = update_config.config_updates.is_some()
                    || update_config.table_metadata_updates.is_some()
                    || update_config.schema_metadata_updates.is_some()
                    || !update_config.field_metadata_updates.is_empty();

                // Check if old-style fields are present
                let has_old_fields = !update_config.upsert_values.is_empty()
                    || !update_config.delete_keys.is_empty()
                    || !update_config.schema_metadata.is_empty()
                    || !update_config.field_metadata.is_empty();

                // Error if both are present
                if has_new_fields && has_old_fields {
                    return Err(Error::invalid_input_source(
                        "Cannot mix old and new style UpdateConfig fields".into(),
                    ));
                }

                if has_old_fields {
                    // Translate old-style to new-style
                    let config_updates = if !update_config.upsert_values.is_empty()
                        || !update_config.delete_keys.is_empty()
                    {
                        Some(translate_config_updates(
                            &update_config.upsert_values,
                            &update_config.delete_keys,
                        ))
                    } else {
                        None
                    };

                    let schema_metadata_updates = if !update_config.schema_metadata.is_empty() {
                        Some(translate_schema_metadata_updates(
                            &update_config.schema_metadata,
                        ))
                    } else {
                        None
                    };

                    let field_metadata_updates = update_config
                        .field_metadata
                        .into_iter()
                        .map(|(field_id, field_meta_update)| {
                            (
                                field_id as i32,
                                translate_schema_metadata_updates(&field_meta_update.metadata),
                            )
                        })
                        .collect();

                    Operation::UpdateConfig {
                        config_updates,
                        table_metadata_updates: None,
                        schema_metadata_updates,
                        field_metadata_updates,
                    }
                } else {
                    // Use new-style fields directly (convert from protobuf)
                    Operation::UpdateConfig {
                        config_updates: update_config.config_updates.as_ref().map(UpdateMap::from),
                        table_metadata_updates: update_config
                            .table_metadata_updates
                            .as_ref()
                            .map(UpdateMap::from),
                        schema_metadata_updates: update_config
                            .schema_metadata_updates
                            .as_ref()
                            .map(UpdateMap::from),
                        field_metadata_updates: update_config
                            .field_metadata_updates
                            .iter()
                            .map(|(field_id, pb_update_map)| {
                                (*field_id, UpdateMap::from(pb_update_map))
                            })
                            .collect(),
                    }
                }
            }
            Some(pb::transaction::Operation::DataReplacement(
                pb::transaction::DataReplacement { replacements },
            )) => Operation::DataReplacement {
                replacements: replacements
                    .into_iter()
                    .map(DataReplacementGroup::try_from)
                    .collect::<Result<Vec<_>>>()?,
            },
            Some(pb::transaction::Operation::UpdateMemWalState(
                pb::transaction::UpdateMemWalState { compacted_sstables },
            )) => Operation::UpdateMemWalState {
                compacted_sstables: compacted_sstables
                    .into_iter()
                    .map(|m| CompactedSsTable::try_from(m).unwrap())
                    .collect(),
            },
            Some(pb::transaction::Operation::UpdateBases(pb::transaction::UpdateBases {
                new_bases,
            })) => Operation::UpdateBases {
                new_bases: new_bases.into_iter().map(BasePath::from).collect(),
            },
            Some(pb::transaction::Operation::DataOverlay(pb::transaction::DataOverlay {
                groups,
            })) => Operation::DataOverlay {
                groups: groups
                    .into_iter()
                    .map(DataOverlayGroup::try_from)
                    .collect::<Result<Vec<_>>>()?,
            },
            None => {
                return Err(Error::internal(
                    "Transaction message did not contain an operation".to_string(),
                ));
            }
        };
        Ok(Self {
            read_version: message.read_version,
            uuid: message.uuid.clone(),
            operation,
            tag: if message.tag.is_empty() {
                None
            } else {
                Some(message.tag.clone())
            },
            transaction_properties: if message.transaction_properties.is_empty() {
                None
            } else {
                Some(Arc::new(message.transaction_properties))
            },
        })
    }
}

impl TryFrom<&pb::transaction::rewrite::RewrittenIndex> for RewrittenIndex {
    type Error = Error;

    fn try_from(message: &pb::transaction::rewrite::RewrittenIndex) -> Result<Self> {
        Ok(Self {
            old_id: message
                .old_id
                .as_ref()
                .map(Uuid::try_from)
                .ok_or_else(|| {
                    Error::invalid_input("required field (old_id) missing from message".to_string())
                })??,
            new_id: message
                .new_id
                .as_ref()
                .map(Uuid::try_from)
                .ok_or_else(|| {
                    Error::invalid_input("required field (new_id) missing from message".to_string())
                })??,
            new_index_details: message
                .new_index_details
                .as_ref()
                .ok_or_else(|| {
                    Error::invalid_input("new_index_details is a required field".to_string())
                })?
                .clone(),
            new_index_version: message.new_index_version,
            new_index_files: if message.new_index_files.is_empty() {
                None
            } else {
                Some(
                    message
                        .new_index_files
                        .iter()
                        .map(|f| IndexFile {
                            path: f.path.clone(),
                            size_bytes: f.size_bytes,
                        })
                        .collect(),
                )
            },
        })
    }
}

impl TryFrom<pb::transaction::rewrite::RewriteGroup> for RewriteGroup {
    type Error = Error;

    fn try_from(message: pb::transaction::rewrite::RewriteGroup) -> Result<Self> {
        Ok(Self {
            old_fragments: message
                .old_fragments
                .into_iter()
                .map(Fragment::try_from)
                .collect::<Result<Vec<_>>>()?,
            new_fragments: message
                .new_fragments
                .into_iter()
                .map(Fragment::try_from)
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl From<&Transaction> for pb::Transaction {
    fn from(value: &Transaction) -> Self {
        let operation = match &value.operation {
            Operation::Append { fragments } => {
                pb::transaction::Operation::Append(pb::transaction::Append {
                    fragments: fragments.iter().map(pb::DataFragment::from).collect(),
                })
            }
            Operation::Clone {
                is_shallow,
                ref_name,
                ref_version,
                ref_path,
                branch_name,
            } => pb::transaction::Operation::Clone(pb::transaction::Clone {
                is_shallow: *is_shallow,
                ref_name: ref_name.clone(),
                ref_version: *ref_version,
                ref_path: ref_path.clone(),
                branch_name: branch_name.clone(),
            }),
            Operation::Delete {
                updated_fragments,
                deleted_fragment_ids,
                predicate,
            } => pb::transaction::Operation::Delete(pb::transaction::Delete {
                updated_fragments: updated_fragments
                    .iter()
                    .map(pb::DataFragment::from)
                    .collect(),
                deleted_fragment_ids: deleted_fragment_ids.clone(),
                predicate: predicate.clone(),
            }),
            Operation::Overwrite {
                fragments,
                schema,
                config_upsert_values,
                initial_bases,
            } => {
                pb::transaction::Operation::Overwrite(pb::transaction::Overwrite {
                    fragments: fragments.iter().map(pb::DataFragment::from).collect(),
                    schema: Fields::from(schema).0,
                    schema_metadata: Default::default(), // TODO: handle metadata
                    config_upsert_values: config_upsert_values
                        .clone()
                        .unwrap_or(Default::default()),
                    initial_bases: initial_bases
                        .as_ref()
                        .map(|paths| {
                            paths
                                .iter()
                                .cloned()
                                .map(|bp: BasePath| -> pb::BasePath { bp.into() })
                                .collect::<Vec<pb::BasePath>>()
                        })
                        .unwrap_or_default(),
                })
            }
            Operation::ReserveFragments { num_fragments } => {
                pb::transaction::Operation::ReserveFragments(pb::transaction::ReserveFragments {
                    num_fragments: *num_fragments,
                })
            }
            Operation::Rewrite {
                groups,
                rewritten_indices,
                frag_reuse_index: _,
            } => pb::transaction::Operation::Rewrite(pb::transaction::Rewrite {
                groups: groups
                    .iter()
                    .map(pb::transaction::rewrite::RewriteGroup::from)
                    .collect(),
                rewritten_indices: rewritten_indices
                    .iter()
                    .map(|rewritten| rewritten.into())
                    .collect(),
                ..Default::default()
            }),
            Operation::CreateIndex {
                new_indices,
                removed_indices,
            } => pb::transaction::Operation::CreateIndex(pb::transaction::CreateIndex {
                new_indices: new_indices.iter().map(pb::IndexMetadata::from).collect(),
                removed_indices: removed_indices
                    .iter()
                    .map(pb::IndexMetadata::from)
                    .collect(),
            }),
            Operation::Merge { fragments, schema } => {
                pb::transaction::Operation::Merge(pb::transaction::Merge {
                    fragments: fragments.iter().map(pb::DataFragment::from).collect(),
                    schema: Fields::from(schema).0,
                    schema_metadata: Default::default(), // TODO: handle metadata
                })
            }
            Operation::Restore { version } => {
                pb::transaction::Operation::Restore(pb::transaction::Restore { version: *version })
            }
            Operation::Update {
                removed_fragment_ids,
                updated_fragments,
                new_fragments,
                fields_modified,
                compacted_sstables,
                fields_for_preserving_frag_bitmap,
                update_mode,
                inserted_rows_filter,
                updated_fragment_offsets,
            } => pb::transaction::Operation::Update(pb::transaction::Update {
                removed_fragment_ids: removed_fragment_ids.clone(),
                updated_fragments: updated_fragments
                    .iter()
                    .map(pb::DataFragment::from)
                    .collect(),
                new_fragments: new_fragments.iter().map(pb::DataFragment::from).collect(),
                fields_modified: fields_modified.clone(),
                compacted_sstables: compacted_sstables
                    .iter()
                    .map(pb::CompactedSsTable::from)
                    .collect(),
                fields_for_preserving_frag_bitmap: fields_for_preserving_frag_bitmap.clone(),
                update_mode: update_mode
                    .as_ref()
                    .map(|mode| match mode {
                        UpdateMode::RewriteRows => 0,
                        UpdateMode::RewriteColumns => 1,
                    })
                    .unwrap_or(0),
                inserted_rows: inserted_rows_filter.as_ref().map(|ik| ik.into()),
                updated_fragment_offsets: updated_fragment_offsets
                    .as_ref()
                    .map(|UpdatedFragmentOffsets(m)| {
                        m.iter()
                            .filter(|(_, b)| !b.is_empty())
                            .map(|(frag_id, b)| {
                                let values: Vec<u32> = b.iter().collect();
                                (*frag_id, pb::transaction::UInt32List { values })
                            })
                            .collect::<HashMap<_, _>>()
                    })
                    .unwrap_or_default(),
            }),
            Operation::Project { schema } => {
                pb::transaction::Operation::Project(pb::transaction::Project {
                    schema: Fields::from(schema).0,
                })
            }
            Operation::UpdateConfig {
                config_updates,
                table_metadata_updates,
                schema_metadata_updates,
                field_metadata_updates,
            } => pb::transaction::Operation::UpdateConfig(pb::transaction::UpdateConfig {
                config_updates: config_updates
                    .as_ref()
                    .map(pb::transaction::UpdateMap::from),
                table_metadata_updates: table_metadata_updates
                    .as_ref()
                    .map(pb::transaction::UpdateMap::from),
                schema_metadata_updates: schema_metadata_updates
                    .as_ref()
                    .map(pb::transaction::UpdateMap::from),
                field_metadata_updates: field_metadata_updates
                    .iter()
                    .map(|(field_id, update_map)| {
                        (*field_id, pb::transaction::UpdateMap::from(update_map))
                    })
                    .collect(),
                // Leave old fields empty - we only write new-style fields
                upsert_values: Default::default(),
                delete_keys: Default::default(),
                schema_metadata: Default::default(),
                field_metadata: Default::default(),
            }),
            Operation::DataReplacement { replacements } => {
                pb::transaction::Operation::DataReplacement(pb::transaction::DataReplacement {
                    replacements: replacements
                        .iter()
                        .map(pb::transaction::DataReplacementGroup::from)
                        .collect(),
                })
            }
            Operation::DataOverlay { groups } => {
                pb::transaction::Operation::DataOverlay(pb::transaction::DataOverlay {
                    groups: groups
                        .iter()
                        .map(pb::transaction::DataOverlayGroup::from)
                        .collect(),
                })
            }
            Operation::UpdateMemWalState { compacted_sstables } => {
                pb::transaction::Operation::UpdateMemWalState(pb::transaction::UpdateMemWalState {
                    compacted_sstables: compacted_sstables
                        .iter()
                        .map(pb::CompactedSsTable::from)
                        .collect::<Vec<_>>(),
                })
            }
            Operation::UpdateBases { new_bases } => {
                pb::transaction::Operation::UpdateBases(pb::transaction::UpdateBases {
                    new_bases: new_bases
                        .iter()
                        .cloned()
                        .map(|bp: BasePath| -> pb::BasePath { bp.into() })
                        .collect::<Vec<pb::BasePath>>(),
                })
            }
        };

        let transaction_properties = value
            .transaction_properties
            .as_ref()
            .map(|arc| arc.as_ref().clone())
            .unwrap_or_default();
        Self {
            read_version: value.read_version,
            uuid: value.uuid.clone(),
            operation: Some(operation),
            tag: value.tag.clone().unwrap_or("".to_string()),
            transaction_properties,
        }
    }
}

impl From<&RewrittenIndex> for pb::transaction::rewrite::RewrittenIndex {
    fn from(value: &RewrittenIndex) -> Self {
        Self {
            old_id: Some((&value.old_id).into()),
            new_id: Some((&value.new_id).into()),
            new_index_details: Some(value.new_index_details.clone()),
            new_index_version: value.new_index_version,
            new_index_files: value
                .new_index_files
                .as_ref()
                .map(|files| {
                    files
                        .iter()
                        .map(|f| pb::IndexFile {
                            path: f.path.clone(),
                            size_bytes: f.size_bytes,
                        })
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}

impl From<&RewriteGroup> for pb::transaction::rewrite::RewriteGroup {
    fn from(value: &RewriteGroup) -> Self {
        Self {
            old_fragments: value
                .old_fragments
                .iter()
                .map(pb::DataFragment::from)
                .collect(),
            new_fragments: value
                .new_fragments
                .iter()
                .map(pb::DataFragment::from)
                .collect(),
        }
    }
}

/// Validate the operation is valid for the given manifest.
pub fn validate_operation(manifest: Option<&Manifest>, operation: &Operation) -> Result<()> {
    let manifest = match (manifest, operation) {
        (
            None,
            Operation::Overwrite {
                fragments, schema, ..
            },
        ) => {
            // Validate here because we are going to return early.
            overwrite_fragments_valid(fragments)?;
            schema_fragments_valid(None, schema, fragments)?;

            return Ok(());
        }
        (None, Operation::Clone { .. }) => return Ok(()),
        (Some(manifest), _) => manifest,
        (None, _) => {
            return Err(Error::invalid_input(format!(
                "Cannot apply operation {} to non-existent dataset",
                operation.name()
            )));
        }
    };

    match operation {
        Operation::Append { fragments } => {
            // Fragments must contain all fields in the schema
            schema_fragments_valid(Some(manifest), &manifest.schema, fragments)
        }
        Operation::Project { schema } => {
            schema_fragments_valid(Some(manifest), schema, manifest.fragments.as_ref())
        }
        Operation::Merge { fragments, schema } => {
            merge_fragments_valid(manifest, fragments)?;
            schema_fragments_valid(Some(manifest), schema, fragments)
        }
        Operation::Overwrite {
            fragments, schema, ..
        } => {
            overwrite_fragments_valid(fragments)?;
            // Pass None for manifest because Overwrite replaces all fragments.
            // The old manifest's storage format is irrelevant for validating
            // the new fragments (e.g., LEGACY→STABLE transitions).
            schema_fragments_valid(None, schema, fragments)
        }
        Operation::Update {
            updated_fragments,
            new_fragments,
            ..
        } => {
            schema_fragments_valid(Some(manifest), &manifest.schema, updated_fragments)?;
            schema_fragments_valid(Some(manifest), &manifest.schema, new_fragments)
        }
        _ => Ok(()),
    }
}

// An overwrite's fragments are newly written, so they are given fresh ids at
// commit time. A deletion file cannot come along for that ride: its path embeds
// the fragment id, so renumbering the fragment would orphan the deletion vector
// and silently resurrect deleted rows.
fn overwrite_fragments_valid(fragments: &[Fragment]) -> Result<()> {
    for fragment in fragments {
        if let Some(deletion_file) = &fragment.deletion_file {
            return Err(Error::invalid_input(format!(
                "Overwrite fragments must be newly written, but fragment {} carries \
                 deletion file {}. Use Delete to commit deletions against existing \
                 fragments, or Merge to change their schema.",
                fragment.id,
                relative_deletion_file_path(fragment.id, deletion_file)
            )));
        }
    }
    Ok(())
}

fn schema_fragments_valid(
    manifest: Option<&Manifest>,
    schema: &Schema,
    fragments: &[Fragment],
) -> Result<()> {
    if let Some(manifest) = manifest
        && manifest.data_storage_format.lance_file_version()? == LanceFileVersion::Legacy
    {
        return schema_fragments_legacy_valid(schema, fragments);
    }
    // validate that each data file at least contains one field.
    for fragment in fragments {
        for data_file in &fragment.files {
            if data_file.fields.iter().len() == 0 {
                return Err(Error::invalid_input(format!(
                    "Datafile {} does not contain any fields",
                    data_file.path
                )));
            }
        }
    }
    Ok(())
}

/// Check that each fragment contains all fields in the schema.
/// It is not required that the schema contains all fields in the fragment.
/// There may be masked fields.
fn schema_fragments_legacy_valid(schema: &Schema, fragments: &[Fragment]) -> Result<()> {
    // TODO: add additional validation. Consider consolidating with various
    // validate() methods in the codebase.
    for fragment in fragments {
        for field in schema.fields_pre_order() {
            if !fragment
                .files
                .iter()
                .flat_map(|f| f.fields.iter())
                .any(|f_id| f_id == &field.id)
            {
                return Err(Error::invalid_input(format!(
                    "Fragment {} does not contain field {:?}",
                    fragment.id, field
                )));
            }
        }
    }
    Ok(())
}

/// Returns true if Operation::Merge rewrote this fragment's column data files (Fragment::files
/// changed versus the previous manifest). Used to bump last_updated_at_version_meta only when
/// new column values were materialized to disk.
///
/// Deletion file changes alone are not treated as rewrites: tombstones remove rows but
/// survivors did not receive new column bytes; stamping last_updated for those rows would be
/// incorrect for CDF.
#[inline]
fn merge_fragment_physically_rewritten(prev: &Fragment, merged: &Fragment) -> bool {
    debug_assert_eq!(prev.id, merged.id);
    if prev.files.len() != merged.files.len() {
        return true;
    }
    // Compare identity fields only. file_size_bytes is an AtomicU64 cache that
    // concurrent scans can populate in place on the manifest's DataFile, so it
    // must not be part of the rewrite check.
    prev.files.iter().zip(merged.files.iter()).any(|(p, m)| {
        p.path != m.path
            || p.fields != m.fields
            || p.column_indices != m.column_indices
            || p.file_major_version != m.file_major_version
            || p.file_minor_version != m.file_minor_version
            || p.base_id != m.base_id
    })
}

/// Validate that Merge operations preserve all original fragments.
/// Merge operations should only add columns or rows, not reduce fragments.
/// This ensures fragments correspond at one-to-one with the original fragment list.
fn merge_fragments_valid(manifest: &Manifest, new_fragments: &[Fragment]) -> Result<()> {
    let original_fragments = manifest.fragments.as_ref();

    // Additional validation: ensure we're not accidentally reducing the fragment count
    if new_fragments.len() < original_fragments.len() {
        return Err(Error::invalid_input(format!(
            "Merge operation reduced fragment count from {} to {}. \
             Merge operations should only add columns, not reduce fragments.",
            original_fragments.len(),
            new_fragments.len()
        )));
    }

    // Collect new fragment IDs
    let new_fragment_map: HashMap<u64, &Fragment> =
        new_fragments.iter().map(|f| (f.id, f)).collect();

    // Check that all original fragments are preserved in the new fragments list
    // Validate that each original fragment's metadata is preserved
    let mut missing_fragments: Vec<u64> = Vec::new();
    for original_fragment in original_fragments {
        if let Some(new_fragment) = new_fragment_map.get(&original_fragment.id) {
            // Validate physical_rows (row count) hasn't changed
            if original_fragment.physical_rows != new_fragment.physical_rows {
                return Err(Error::invalid_input(format!(
                    "Merge operation changed row count for fragment {}. \
                     Original: {:?}, New: {:?}. \
                     Merge operations should preserve fragment row counts and only add new columns.",
                    original_fragment.id,
                    original_fragment.physical_rows,
                    new_fragment.physical_rows
                )));
            }
        } else {
            missing_fragments.push(original_fragment.id);
        }
    }

    if !missing_fragments.is_empty() {
        return Err(Error::invalid_input(format!(
            "Merge operation is missing original fragments: {:?}. \
             Merge operations should preserve all original fragments and only add new columns. \
             Expected fragments: {:?}, but got: {:?}",
            missing_fragments,
            original_fragments.iter().map(|f| f.id).collect::<Vec<_>>(),
            new_fragment_map.keys().copied().collect::<Vec<_>>()
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::UInt64Type;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use chrono::Utc;
    use futures::TryStreamExt;
    use lance_core::datatypes::Schema as LanceSchema;
    use lance_core::utils::address::RowAddress;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_core::{ROW_ADDR, ROW_CREATED_AT_VERSION, ROW_LAST_UPDATED_AT_VERSION};
    use lance_file::version::{ConcreteFileVersion, LanceFileVersion};
    use lance_io::utils::CachedFileSize;
    use lance_table::format::overlay::OverlayCoverage;
    use lance_table::format::{
        RowDatasetVersionMeta, RowDatasetVersionRun, RowDatasetVersionSequence, RowIdMeta,
    };
    use lance_table::rowids::segment::U64Segment;
    use lance_table::rowids::write_row_ids;
    use std::collections::HashMap;
    use std::sync::Arc;
    use uuid::Uuid;

    use crate::Dataset;
    use crate::dataset::write::WriteParams;
    use crate::session::Session;

    fn sample_manifest() -> Manifest {
        sample_manifest_with_fragments(0..1)
    }

    fn sample_manifest_with_fragments(ids: std::ops::Range<u64>) -> Manifest {
        let schema = ArrowSchema::new(vec![ArrowField::new("id", DataType::Int32, false)]);
        Manifest::new(
            LanceSchema::try_from(&schema).unwrap(),
            Arc::new(ids.map(Fragment::new).collect()),
            DataStorageFormat::new(ConcreteFileVersion::V2_0),
            HashMap::new(),
        )
    }

    fn sample_index_metadata(name: &str) -> IndexMetadata {
        IndexMetadata {
            uuid: Uuid::new_v4(),
            fields: vec![0],
            name: name.to_string(),
            dataset_version: 0,
            fragment_bitmap: Some([0].into_iter().collect()),
            index_details: None,
            index_version: 1,
            created_at: Some(Utc::now()),
            base_id: None,
            files: None,
        }
    }

    #[test]
    fn test_rewrite_fragments() {
        let existing_fragments: Vec<Fragment> = (0..10).map(Fragment::new).collect();

        let mut final_fragments = existing_fragments;
        let rewrite_groups = vec![
            // Since these are contiguous, they will be put in the same location
            // as 1 and 2.
            RewriteGroup {
                old_fragments: vec![Fragment::new(1), Fragment::new(2)],
                // These two fragments were previously reserved
                new_fragments: vec![Fragment::new(15), Fragment::new(16)],
            },
            // These are not contiguous, so they will be inserted at the end.
            RewriteGroup {
                old_fragments: vec![Fragment::new(5), Fragment::new(8)],
                // We pretend this id was not reserved.  Does not happen in practice today
                // but we want to leave the door open.
                new_fragments: vec![Fragment::new(0)],
            },
        ];

        let mut fragment_id = 20;
        let version = 0;

        Transaction::handle_rewrite_fragments(
            &mut final_fragments,
            &rewrite_groups,
            &mut fragment_id,
            version,
            None,
        )
        .unwrap();

        assert_eq!(fragment_id, 21);

        let expected_fragments: Vec<Fragment> = vec![
            Fragment::new(0),
            Fragment::new(15),
            Fragment::new(16),
            Fragment::new(3),
            Fragment::new(4),
            Fragment::new(6),
            Fragment::new(7),
            Fragment::new(9),
            Fragment::new(20),
        ];

        assert_eq!(final_fragments, expected_fragments);
    }

    #[test]
    fn test_merge_fragments_valid() {
        // Create a simple schema for testing
        let schema = ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, false),
            ArrowField::new("name", DataType::Utf8, false),
        ]);

        // Create original fragments
        let original_fragments = vec![Fragment::new(1), Fragment::new(2), Fragment::new(3)];

        // Create a manifest with original fragments
        let manifest = Manifest::new(
            LanceSchema::try_from(&schema).unwrap(),
            Arc::new(original_fragments),
            DataStorageFormat::new(ConcreteFileVersion::V2_0),
            HashMap::new(),
        );

        // Test 1: Empty fragments should fail
        let empty_fragments = vec![];
        let result = merge_fragments_valid(&manifest, &empty_fragments);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("reduced fragment count")
        );

        // Test 2: Missing original fragments should fail
        let missing_fragments = vec![
            Fragment::new(1),
            Fragment::new(2),
            // Fragment 3 is missing
            Fragment::new(4), // New fragment
        ];
        let result = merge_fragments_valid(&manifest, &missing_fragments);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("missing original fragments")
        );

        // Test 3: Reduced fragment count should fail
        let reduced_fragments = vec![
            Fragment::new(1),
            Fragment::new(2),
            // Fragment 3 is missing, no new fragments added
        ];
        let result = merge_fragments_valid(&manifest, &reduced_fragments);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("reduced fragment count")
        );

        // Test 4: Valid merge with all original fragments plus new ones should succeed
        let valid_fragments = vec![
            Fragment::new(1),
            Fragment::new(2),
            Fragment::new(3),
            Fragment::new(4), // New fragment
            Fragment::new(5), // Another new fragment
        ];
        let result = merge_fragments_valid(&manifest, &valid_fragments);
        assert!(result.is_ok());

        // Test 5: Same fragments (no new ones) should succeed
        let same_fragments = vec![Fragment::new(1), Fragment::new(2), Fragment::new(3)];
        let result = merge_fragments_valid(&manifest, &same_fragments);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_index_build_manifest_keeps_unremoved_same_name_indices() {
        let manifest = sample_manifest();
        let first_index = sample_index_metadata("vector_idx");
        let second_index = sample_index_metadata("vector_idx");
        let third_index = sample_index_metadata("vector_idx");

        let transaction = Transaction::new(
            manifest.version,
            Operation::CreateIndex {
                new_indices: vec![third_index.clone()],
                removed_indices: vec![second_index.clone()],
            },
            None,
        );

        let (_, final_indices) = transaction
            .build_manifest(
                Some(&manifest),
                vec![first_index.clone(), second_index.clone()],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        assert_eq!(final_indices.len(), 2);
        assert!(final_indices.iter().any(|idx| idx.uuid == first_index.uuid));
        assert!(final_indices.iter().any(|idx| idx.uuid == third_index.uuid));
        assert!(
            !final_indices
                .iter()
                .any(|idx| idx.uuid == second_index.uuid)
        );
    }

    #[test]
    fn test_create_index_build_manifest_deduplicates_relisted_indices_by_uuid() {
        let manifest = sample_manifest();
        let first_index = sample_index_metadata("vector_idx");
        let second_index = sample_index_metadata("vector_idx");
        let third_index = sample_index_metadata("vector_idx");

        let transaction = Transaction::new(
            manifest.version,
            Operation::CreateIndex {
                new_indices: vec![first_index.clone(), third_index.clone()],
                removed_indices: vec![second_index.clone()],
            },
            None,
        );

        let (_, final_indices) = transaction
            .build_manifest(
                Some(&manifest),
                vec![first_index.clone(), second_index.clone()],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        assert_eq!(final_indices.len(), 2);
        assert_eq!(
            final_indices
                .iter()
                .filter(|idx| idx.uuid == first_index.uuid)
                .count(),
            1
        );
        assert!(final_indices.iter().any(|idx| idx.uuid == third_index.uuid));
        assert!(
            !final_indices
                .iter()
                .any(|idx| idx.uuid == second_index.uuid)
        );
    }

    #[test]
    fn test_update_build_manifest_replaces_and_removes_fragments() {
        let manifest = sample_manifest_with_fragments(0..5);

        let mut updated2 = Fragment::new(2);
        updated2.physical_rows = Some(42);
        let mut updated4 = Fragment::new(4);
        updated4.physical_rows = Some(43);

        let transaction = Transaction::new(
            manifest.version,
            Operation::Update {
                removed_fragment_ids: vec![1],
                // Fragment 99 does not exist in the dataset; it must be ignored,
                // not appended.
                updated_fragments: vec![updated2, updated4, Fragment::new(99)],
                new_fragments: vec![],
                fields_modified: vec![],
                compacted_sstables: vec![],
                fields_for_preserving_frag_bitmap: vec![],
                update_mode: None,
                inserted_rows_filter: None,
                updated_fragment_offsets: None,
            },
            None,
        );

        let (new_manifest, _) = transaction
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        let ids: Vec<u64> = new_manifest.fragments.iter().map(|f| f.id).collect();
        assert_eq!(ids, vec![0, 2, 3, 4]);
        let rows: Vec<Option<usize>> = new_manifest
            .fragments
            .iter()
            .map(|f| f.physical_rows)
            .collect();
        assert_eq!(rows, vec![None, Some(42), None, Some(43)]);
    }

    #[test]
    fn test_delete_build_manifest_replaces_and_removes_fragments() {
        let manifest = sample_manifest_with_fragments(0..5);

        let mut updated2 = Fragment::new(2);
        updated2.physical_rows = Some(42);

        let transaction = Transaction::new(
            manifest.version,
            Operation::Delete {
                updated_fragments: vec![updated2],
                deleted_fragment_ids: vec![1, 3],
                predicate: "id > 0".to_string(),
            },
            None,
        );

        let (new_manifest, _) = transaction
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        let ids: Vec<u64> = new_manifest.fragments.iter().map(|f| f.id).collect();
        assert_eq!(ids, vec![0, 2, 4]);
        let rows: Vec<Option<usize>> = new_manifest
            .fragments
            .iter()
            .map(|f| f.physical_rows)
            .collect();
        assert_eq!(rows, vec![None, Some(42), None]);
    }

    #[test]
    fn test_remove_tombstoned_data_files() {
        // Create a fragment with mixed data files: some normal, some fully tombstoned
        let mut fragment = Fragment::new(1);

        // Add a normal data file with valid field IDs
        fragment.files.push(DataFile {
            path: "normal.lance".to_string(),
            fields: Arc::from([1, 2, 3]),
            column_indices: Arc::from([]),
            file_major_version: 2,
            file_minor_version: 0,
            file_size_bytes: CachedFileSize::new(1000),
            base_id: None,
        });

        // Add a data file with all fields tombstoned
        fragment.files.push(DataFile {
            path: "all_tombstoned.lance".to_string(),
            fields: Arc::from([-2, -2, -2]),
            column_indices: Arc::from([]),
            file_major_version: 2,
            file_minor_version: 0,
            file_size_bytes: CachedFileSize::new(500),
            base_id: None,
        });

        // Add a data file with mixed tombstoned and valid fields
        fragment.files.push(DataFile {
            path: "mixed.lance".to_string(),
            fields: Arc::from([4, -2, 5]),
            column_indices: Arc::from([]),
            file_major_version: 2,
            file_minor_version: 0,
            file_size_bytes: CachedFileSize::new(750),
            base_id: None,
        });

        // Add another fully tombstoned file
        fragment.files.push(DataFile {
            path: "another_tombstoned.lance".to_string(),
            fields: Arc::from([-2_i32]),
            column_indices: Arc::from([]),
            file_major_version: 2,
            file_minor_version: 0,
            file_size_bytes: CachedFileSize::new(250),
            base_id: None,
        });

        let mut fragments = vec![fragment];

        // Apply the cleanup
        Transaction::remove_tombstoned_data_files(&mut fragments);

        // Should have removed the two fully tombstoned files
        assert_eq!(fragments[0].files.len(), 2);
        assert_eq!(fragments[0].files[0].path, "normal.lance");
        assert_eq!(fragments[0].files[1].path, "mixed.lance");
    }

    #[test]
    fn test_assign_row_ids_new_fragment() {
        // Test assigning row IDs to a fragment without existing row IDs
        let mut fragments = vec![Fragment {
            id: 1,
            physical_rows: Some(100),
            row_id_meta: None,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        }];
        let mut next_row_id = 0;

        Transaction::assign_row_ids(&mut next_row_id, &mut fragments).unwrap();

        assert_eq!(next_row_id, 100);
        assert!(fragments[0].row_id_meta.is_some());

        if let Some(RowIdMeta::Inline(data)) = &fragments[0].row_id_meta {
            let sequence = read_row_ids(data).unwrap();
            assert_eq!(sequence.len(), 100);
            let row_ids: Vec<u64> = sequence.iter().collect();
            assert_eq!(row_ids, (0..100).collect::<Vec<u64>>());
        } else {
            panic!("Expected inline row ID metadata");
        }
    }

    #[test]
    fn test_assign_row_ids_existing_complete() {
        // Test with fragment that already has complete row IDs
        let existing_sequence = RowIdSequence::from(0..50);
        let serialized = write_row_ids(&existing_sequence);

        let mut fragments = vec![Fragment {
            id: 1,
            physical_rows: Some(50),
            row_id_meta: Some(RowIdMeta::Inline(serialized)),
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        }];
        let mut next_row_id = 100;

        Transaction::assign_row_ids(&mut next_row_id, &mut fragments).unwrap();

        // next_row_id should not change
        assert_eq!(next_row_id, 100);

        if let Some(RowIdMeta::Inline(data)) = &fragments[0].row_id_meta {
            let sequence = read_row_ids(data).unwrap();
            assert_eq!(sequence.len(), 50);
            let row_ids: Vec<u64> = sequence.iter().collect();
            assert_eq!(row_ids, (0..50).collect::<Vec<u64>>());
        } else {
            panic!("Expected inline row ID metadata");
        }
    }

    #[test]
    fn test_assign_row_ids_partial_existing() {
        // Test with fragment that has partial row IDs (merge insert case)
        let existing_sequence = RowIdSequence::from(0..30);
        let serialized = write_row_ids(&existing_sequence);

        let mut fragments = vec![Fragment {
            id: 1,
            physical_rows: Some(50), // More physical rows than existing row IDs
            row_id_meta: Some(RowIdMeta::Inline(serialized)),
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        }];
        let mut next_row_id = 100;

        Transaction::assign_row_ids(&mut next_row_id, &mut fragments).unwrap();

        // next_row_id should advance by 20 (50 - 30)
        assert_eq!(next_row_id, 120);

        if let Some(RowIdMeta::Inline(data)) = &fragments[0].row_id_meta {
            let sequence = read_row_ids(data).unwrap();
            assert_eq!(sequence.len(), 50);
            let row_ids: Vec<u64> = sequence.iter().collect();
            // Should contain original 0-29 plus new 100-119
            let mut expected = (0..30).collect::<Vec<u64>>();
            expected.extend(100..120);
            assert_eq!(row_ids, expected);
        } else {
            panic!("Expected inline row ID metadata");
        }
    }

    #[test]
    fn test_assign_row_ids_excess_row_ids() {
        // Test error case where fragment has more row IDs than physical rows
        let existing_sequence = RowIdSequence::from(0..60);
        let serialized = write_row_ids(&existing_sequence);

        let mut fragments = vec![Fragment {
            id: 1,
            physical_rows: Some(50), // Less physical rows than existing row IDs
            row_id_meta: Some(RowIdMeta::Inline(serialized)),
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        }];
        let mut next_row_id = 100;

        let result = Transaction::assign_row_ids(&mut next_row_id, &mut fragments);

        assert!(result.is_err());
        if let Err(Error::Internal { message, .. }) = result {
            assert!(message.contains("more row IDs (60) than physical rows (50)"));
        } else {
            panic!("Expected Internal error about excess row IDs");
        }
    }

    #[test]
    fn test_assign_row_ids_multiple_fragments() {
        // Test with multiple fragments, some with existing row IDs, some without
        let existing_sequence = RowIdSequence::from(500..520);
        let serialized = write_row_ids(&existing_sequence);

        let mut fragments = vec![
            Fragment {
                id: 1,
                physical_rows: Some(30), // No existing row IDs
                row_id_meta: None,
                files: vec![],
                overlays: vec![],
                deletion_file: None,
                last_updated_at_version_meta: None,
                created_at_version_meta: None,
            },
            Fragment {
                id: 2,
                physical_rows: Some(25), // Partial existing row IDs
                row_id_meta: Some(RowIdMeta::Inline(serialized)),
                files: vec![],
                overlays: vec![],
                deletion_file: None,
                last_updated_at_version_meta: None,
                created_at_version_meta: None,
            },
        ];
        let mut next_row_id = 1000;

        Transaction::assign_row_ids(&mut next_row_id, &mut fragments).unwrap();

        // Should advance by 30 (first fragment) + 5 (second fragment partial)
        assert_eq!(next_row_id, 1035);

        // Check first fragment
        if let Some(RowIdMeta::Inline(data)) = &fragments[0].row_id_meta {
            let sequence = read_row_ids(data).unwrap();
            assert_eq!(sequence.len(), 30);
            let row_ids: Vec<u64> = sequence.iter().collect();
            assert_eq!(row_ids, (1000..1030).collect::<Vec<u64>>());
        } else {
            panic!("Expected inline row ID metadata for first fragment");
        }

        // Check second fragment
        if let Some(RowIdMeta::Inline(data)) = &fragments[1].row_id_meta {
            let sequence = read_row_ids(data).unwrap();
            assert_eq!(sequence.len(), 25);
            let row_ids: Vec<u64> = sequence.iter().collect();
            // Should contain original 500-519 plus new 1030-1034
            let mut expected = (500..520).collect::<Vec<u64>>();
            expected.extend(1030..1035);
            assert_eq!(row_ids, expected);
        } else {
            panic!("Expected inline row ID metadata for second fragment");
        }
    }

    #[test]
    fn test_assign_row_ids_missing_physical_rows() {
        // Test error case where fragment doesn't have physical_rows set
        let mut fragments = vec![Fragment {
            id: 1,
            physical_rows: None,
            row_id_meta: None,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        }];
        let mut next_row_id = 0;

        let result = Transaction::assign_row_ids(&mut next_row_id, &mut fragments);

        assert!(result.is_err());
        if let Err(Error::Internal { message, .. }) = result {
            assert!(message.contains("Fragment does not have physical rows"));
        } else {
            panic!("Expected Internal error about missing physical rows");
        }
    }

    // Helper functions for retain_relevant_indices tests
    fn create_test_index(
        name: &str,
        field_id: i32,
        dataset_version: u64,
        fragment_bitmap: Option<RoaringBitmap>,
        is_vector: bool,
    ) -> IndexMetadata {
        use prost_types::Any;
        use std::sync::Arc;
        use uuid::Uuid;

        let index_details = if is_vector {
            Some(Arc::new(Any {
                type_url: "type.googleapis.com/lance.index.VectorIndexDetails".to_string(),
                value: vec![],
            }))
        } else {
            Some(Arc::new(Any {
                type_url: "type.googleapis.com/lance.index.ScalarIndexDetails".to_string(),
                value: vec![],
            }))
        };

        IndexMetadata {
            uuid: Uuid::new_v4(),
            fields: vec![field_id],
            name: name.to_string(),
            dataset_version,
            fragment_bitmap,
            index_details,
            index_version: 1,
            created_at: None,
            base_id: None,
            files: None,
        }
    }

    fn create_system_index(name: &str, field_id: i32) -> IndexMetadata {
        use prost_types::Any;
        use std::sync::Arc;
        use uuid::Uuid;

        IndexMetadata {
            uuid: Uuid::new_v4(),
            fields: vec![field_id],
            name: name.to_string(),
            dataset_version: 1,
            fragment_bitmap: Some(RoaringBitmap::from_iter([1, 2])),
            index_details: Some(Arc::new(Any {
                type_url: "type.googleapis.com/lance.index.SystemIndexDetails".to_string(),
                value: vec![],
            })),
            index_version: 1,
            created_at: None,
            base_id: None,
            files: None,
        }
    }

    fn create_test_schema(field_ids: &[i32]) -> Schema {
        use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
        use lance_core::datatypes::Schema as LanceSchema;

        let fields: Vec<ArrowField> = field_ids
            .iter()
            .map(|id| ArrowField::new(format!("field_{}", id), DataType::Int32, false))
            .collect();

        let arrow_schema = ArrowSchema::new(fields);
        let mut lance_schema = LanceSchema::try_from(&arrow_schema).unwrap();

        // Assign field IDs
        for (i, field_id) in field_ids.iter().enumerate() {
            lance_schema.mut_field_by_id(i as i32).unwrap().id = *field_id;
        }

        lance_schema
    }

    #[test]
    fn test_retain_indices_removes_missing_fields() {
        let schema = create_test_schema(&[1, 2]);
        let fragments = vec![Fragment::new(1), Fragment::new(2)];

        let mut indices = vec![
            create_test_index("idx1", 1, 1, Some(RoaringBitmap::from_iter([1])), false),
            create_test_index("idx2", 2, 1, Some(RoaringBitmap::from_iter([1])), false),
            create_test_index("idx3", 99, 1, Some(RoaringBitmap::from_iter([1])), false), // Field doesn't exist
        ];

        Transaction::retain_relevant_indices(&mut indices, &schema, &fragments);

        assert_eq!(indices.len(), 2);
        assert!(indices.iter().all(|idx| idx.fields[0] != 99));
    }

    #[test]
    fn test_retain_indices_keeps_system_indices() {
        use lance_index::mem_wal::MEM_WAL_INDEX_NAME;

        let schema = create_test_schema(&[1, 2]);
        let fragments = vec![Fragment::new(1)];

        let mut indices = vec![
            create_system_index(FRAG_REUSE_INDEX_NAME, 99), // Field doesn't exist but should be kept
            create_system_index(MEM_WAL_INDEX_NAME, 99), // Field doesn't exist but should be kept
            create_test_index("regular_idx", 99, 1, Some(RoaringBitmap::new()), false), // Should be removed
        ];

        Transaction::retain_relevant_indices(&mut indices, &schema, &fragments);

        assert_eq!(indices.len(), 2);
        assert!(indices.iter().any(|idx| idx.name == FRAG_REUSE_INDEX_NAME));
        assert!(indices.iter().any(|idx| idx.name == MEM_WAL_INDEX_NAME));
    }

    #[test]
    fn test_retain_indices_keeps_fragment_reuse_index() {
        let schema = create_test_schema(&[1]);
        let fragments = vec![Fragment::new(1)];

        let mut indices = vec![
            create_system_index(FRAG_REUSE_INDEX_NAME, 1),
            create_test_index("other_idx", 1, 1, Some(RoaringBitmap::new()), false),
        ];

        Transaction::retain_relevant_indices(&mut indices, &schema, &fragments);

        // Fragment reuse index should always be kept
        assert!(indices.iter().any(|idx| idx.name == FRAG_REUSE_INDEX_NAME));
    }

    #[test]
    fn test_retain_single_empty_scalar_index() {
        let schema = create_test_schema(&[1]);
        let fragments = vec![Fragment::new(1)];

        let mut indices = vec![create_test_index(
            "scalar_idx",
            1,
            1,
            Some(RoaringBitmap::new()), // Empty bitmap
            false,
        )];

        Transaction::retain_relevant_indices(&mut indices, &schema, &fragments);

        // Single empty scalar index should be kept
        assert_eq!(indices.len(), 1);
    }

    #[test]
    fn test_retain_single_empty_vector_index_is_kept() {
        let schema = create_test_schema(&[1]);
        let fragments = vec![Fragment::new(1)];

        let mut indices = vec![create_test_index(
            "vector_idx",
            1,
            1,
            Some(RoaringBitmap::new()), // Empty bitmap
            true,
        )];

        Transaction::retain_relevant_indices(&mut indices, &schema, &fragments);

        // The empty definition is retained: coverage is empty but the index
        // declaration must survive operations that replace every fragment.
        assert_eq!(indices.len(), 1);
    }

    #[test]
    fn test_retain_single_nonempty_index() {
        let schema = create_test_schema(&[1]);
        let fragments = vec![Fragment::new(1)];

        let mut scalar_indices = vec![create_test_index(
            "scalar_idx",
            1,
            1,
            Some(RoaringBitmap::from_iter([1])),
            false,
        )];

        let mut vector_indices = vec![create_test_index(
            "vector_idx",
            1,
            1,
            Some(RoaringBitmap::from_iter([1])),
            true,
        )];

        Transaction::retain_relevant_indices(&mut scalar_indices, &schema, &fragments);
        Transaction::retain_relevant_indices(&mut vector_indices, &schema, &fragments);

        // Both should be kept
        assert_eq!(scalar_indices.len(), 1);
        assert_eq!(vector_indices.len(), 1);
    }

    #[test]
    fn test_retain_single_index_with_none_bitmap() {
        let schema = create_test_schema(&[1]);
        let fragments = vec![Fragment::new(1)];

        let mut scalar_indices = vec![create_test_index("scalar_idx", 1, 1, None, false)];
        let mut vector_indices = vec![create_test_index("vector_idx", 1, 1, None, true)];

        Transaction::retain_relevant_indices(&mut scalar_indices, &schema, &fragments);
        Transaction::retain_relevant_indices(&mut vector_indices, &schema, &fragments);

        // Both kept: a None bitmap counts as empty coverage, and empty
        // definitions are retained regardless of index type.
        assert_eq!(scalar_indices.len(), 1);
        assert_eq!(vector_indices.len(), 1);
    }

    #[test]
    fn test_retain_multiple_empty_scalar_indices_keeps_oldest() {
        let schema = create_test_schema(&[1]);
        let fragments = vec![Fragment::new(1)];

        let mut indices = vec![
            create_test_index("idx", 1, 3, Some(RoaringBitmap::new()), false),
            create_test_index("idx", 1, 1, Some(RoaringBitmap::new()), false), // Oldest
            create_test_index("idx", 1, 2, Some(RoaringBitmap::new()), false),
        ];

        Transaction::retain_relevant_indices(&mut indices, &schema, &fragments);

        // Should keep only the oldest (dataset_version = 1)
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0].dataset_version, 1);
    }

    #[test]
    fn test_retain_multiple_empty_vector_indices_keeps_oldest() {
        let schema = create_test_schema(&[1]);
        let fragments = vec![Fragment::new(1)];

        let mut indices = vec![
            create_test_index("vec_idx", 1, 1, Some(RoaringBitmap::new()), true),
            create_test_index("vec_idx", 1, 2, Some(RoaringBitmap::new()), true),
            create_test_index("vec_idx", 1, 3, Some(RoaringBitmap::new()), true),
        ];

        Transaction::retain_relevant_indices(&mut indices, &schema, &fragments);

        // Same as the scalar case: all deltas are empty, so only the oldest
        // definition survives.
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0].dataset_version, 1);
    }

    #[test]
    fn test_retain_mixed_empty_nonempty_keeps_nonempty() {
        let schema = create_test_schema(&[1]);
        let fragments = vec![Fragment::new(1)];

        let mut indices = vec![
            create_test_index("idx", 1, 1, Some(RoaringBitmap::new()), false), // Empty
            create_test_index("idx", 1, 2, Some(RoaringBitmap::from_iter([1])), false), // Non-empty
            create_test_index("idx", 1, 3, Some(RoaringBitmap::new()), false), // Empty
            create_test_index("idx", 1, 4, Some(RoaringBitmap::from_iter([1])), false), // Non-empty
        ];

        Transaction::retain_relevant_indices(&mut indices, &schema, &fragments);

        // Should keep only non-empty indices
        assert_eq!(indices.len(), 2);
        assert!(
            indices
                .iter()
                .all(|idx| idx.dataset_version == 2 || idx.dataset_version == 4)
        );
    }

    #[test]
    fn test_retain_mixed_empty_nonempty_vector_keeps_nonempty() {
        let schema = create_test_schema(&[1]);
        let fragments = vec![Fragment::new(1)];

        let mut indices = vec![
            create_test_index("vec_idx", 1, 1, Some(RoaringBitmap::new()), true), // Empty
            create_test_index("vec_idx", 1, 2, Some(RoaringBitmap::from_iter([1])), true), // Non-empty
            create_test_index("vec_idx", 1, 3, Some(RoaringBitmap::new()), true),          // Empty
        ];

        Transaction::retain_relevant_indices(&mut indices, &schema, &fragments);

        // Should keep only non-empty index
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0].dataset_version, 2);
    }

    #[test]
    fn test_retain_fragment_bitmap_with_nonexistent_fragments() {
        let schema = create_test_schema(&[1]);
        let fragments = vec![Fragment::new(1), Fragment::new(2)]; // Only fragments 1 and 2 exist

        let mut indices = vec![create_test_index(
            "idx",
            1,
            1,
            Some(RoaringBitmap::from_iter([1, 2, 3, 4])), // References non-existent fragments 3, 4
            false,
        )];

        Transaction::retain_relevant_indices(&mut indices, &schema, &fragments);

        // Should still keep the index (effective bitmap will be intersection with existing)
        assert_eq!(indices.len(), 1);
        // Original bitmap should be unchanged
        assert_eq!(
            indices[0].fragment_bitmap.as_ref().unwrap(),
            &RoaringBitmap::from_iter([1, 2, 3, 4])
        );
    }

    #[test]
    fn test_retain_effective_empty_bitmap_single_index() {
        let schema = create_test_schema(&[1]);
        let fragments = vec![Fragment::new(5), Fragment::new(6)];

        // Bitmap references fragments that don't exist, so effective bitmap is empty
        let mut scalar_indices = vec![create_test_index(
            "scalar_idx",
            1,
            1,
            Some(RoaringBitmap::from_iter([1, 2, 3])),
            false,
        )];

        let mut vector_indices = vec![create_test_index(
            "vector_idx",
            1,
            1,
            Some(RoaringBitmap::from_iter([1, 2, 3])),
            true,
        )];

        Transaction::retain_relevant_indices(&mut scalar_indices, &schema, &fragments);
        Transaction::retain_relevant_indices(&mut vector_indices, &schema, &fragments);

        // Both kept: a single index whose column is still in schema is
        // retained even when its effective coverage is empty.
        assert_eq!(scalar_indices.len(), 1);
        assert_eq!(vector_indices.len(), 1);
    }

    #[test]
    fn test_retain_different_index_names() {
        let schema = create_test_schema(&[1]);
        let fragments = vec![Fragment::new(1)];

        let mut indices = vec![
            create_test_index("idx_a", 1, 1, Some(RoaringBitmap::new()), false),
            create_test_index("idx_b", 1, 1, Some(RoaringBitmap::new()), true),
            create_test_index("idx_c", 1, 1, Some(RoaringBitmap::from_iter([1])), false),
        ];

        Transaction::retain_relevant_indices(&mut indices, &schema, &fragments);

        // All three kept: empty definitions are retained for scalar and
        // vector indexes alike.
        assert_eq!(indices.len(), 3);
        assert!(indices.iter().any(|idx| idx.name == "idx_a"));
        assert!(indices.iter().any(|idx| idx.name == "idx_b"));
        assert!(indices.iter().any(|idx| idx.name == "idx_c"));
    }

    #[test]
    fn test_retain_empty_indices_vec() {
        let schema = create_test_schema(&[1]);
        let fragments = vec![Fragment::new(1)];

        let mut indices: Vec<IndexMetadata> = vec![];

        Transaction::retain_relevant_indices(&mut indices, &schema, &fragments);

        assert_eq!(indices.len(), 0);
    }

    #[test]
    fn test_retain_all_indices_removed() {
        let schema = create_test_schema(&[1]);
        let fragments = vec![Fragment::new(1)];

        let mut indices = vec![
            create_test_index("vec1", 1, 1, Some(RoaringBitmap::new()), true),
            create_test_index("vec2", 1, 1, Some(RoaringBitmap::new()), true),
            create_test_index("idx3", 99, 1, Some(RoaringBitmap::from_iter([1])), false), // Bad field
        ];

        Transaction::retain_relevant_indices(&mut indices, &schema, &fragments);

        // Only the bad-field index is dropped; the empty vector definitions
        // are retained.
        assert_eq!(indices.len(), 2);
        assert!(!indices.iter().any(|idx| idx.name == "idx3"));
    }

    #[test]
    fn test_retain_complex_scenario() {
        let schema = create_test_schema(&[1, 2]);
        let fragments = vec![Fragment::new(1), Fragment::new(2)];

        let mut indices = vec![
            // System index - should always be kept
            create_system_index(FRAG_REUSE_INDEX_NAME, 1),
            // Group "idx_a" - all empty scalars, keep oldest
            create_test_index("idx_a", 1, 3, Some(RoaringBitmap::new()), false),
            create_test_index("idx_a", 1, 1, Some(RoaringBitmap::new()), false), // Oldest
            create_test_index("idx_a", 1, 2, Some(RoaringBitmap::new()), false),
            // Group "vec_b" - all empty vectors, keep oldest definition
            create_test_index("vec_b", 1, 1, Some(RoaringBitmap::new()), true),
            create_test_index("vec_b", 1, 2, Some(RoaringBitmap::new()), true),
            // Group "idx_c" - mixed empty/non-empty, keep non-empty
            create_test_index("idx_c", 2, 1, Some(RoaringBitmap::new()), false),
            create_test_index("idx_c", 2, 2, Some(RoaringBitmap::from_iter([1])), false), // Keep
            create_test_index("idx_c", 2, 3, Some(RoaringBitmap::from_iter([2])), false), // Keep
            // Single non-empty - keep
            create_test_index("idx_d", 1, 1, Some(RoaringBitmap::from_iter([1, 2])), false),
            // Index with bad field - remove
            create_test_index("idx_e", 99, 1, Some(RoaringBitmap::from_iter([1])), false),
        ];

        Transaction::retain_relevant_indices(&mut indices, &schema, &fragments);

        // Expected: frag_reuse, idx_a (oldest), vec_b (oldest), idx_c (2
        // non-empty), idx_d = 6 total
        assert_eq!(indices.len(), 6);

        // Verify system index kept
        assert!(indices.iter().any(|idx| idx.name == FRAG_REUSE_INDEX_NAME));

        // Verify idx_a kept oldest only
        let idx_a_indices: Vec<_> = indices.iter().filter(|idx| idx.name == "idx_a").collect();
        assert_eq!(idx_a_indices.len(), 1);
        assert_eq!(idx_a_indices[0].dataset_version, 1);

        // Verify vec_b kept oldest definition only
        let vec_b_indices: Vec<_> = indices.iter().filter(|idx| idx.name == "vec_b").collect();
        assert_eq!(vec_b_indices.len(), 1);
        assert_eq!(vec_b_indices[0].dataset_version, 1);

        // Verify idx_c kept non-empty only
        let idx_c_indices: Vec<_> = indices.iter().filter(|idx| idx.name == "idx_c").collect();
        assert_eq!(idx_c_indices.len(), 2);
        assert!(
            idx_c_indices
                .iter()
                .all(|idx| idx.dataset_version == 2 || idx.dataset_version == 3)
        );

        // Verify idx_d kept
        assert!(indices.iter().any(|idx| idx.name == "idx_d"));

        // Verify idx_e removed (bad field)
        assert!(!indices.iter().any(|idx| idx.name == "idx_e"));
    }

    #[test]
    fn test_handle_rewrite_indices_skips_missing_index() {
        use uuid::Uuid;

        // Create an empty indices list
        let mut indices = vec![];

        // Create rewritten_indices referring to a non-existent index
        let rewritten_indices = vec![RewrittenIndex {
            old_id: Uuid::new_v4(),
            new_id: Uuid::new_v4(),
            new_index_details: prost_types::Any {
                type_url: String::new(),
                value: vec![],
            },
            new_index_version: 1,
            new_index_files: None,
        }];

        // Should succeed (skip missing index) instead of error
        let result = Transaction::handle_rewrite_indices(&mut indices, &rewritten_indices, &[]);
        assert!(result.is_ok());
        assert!(indices.is_empty());
    }

    /// When a fragment has no existing last_updated_at_version_meta (None), a
    /// partial RewriteColumns refresh must leave it as None rather than fabricating
    /// prev_version for unmatched rows.
    #[test]
    fn test_partial_rewrite_skips_fragment_with_no_version_meta() {
        let row_ids = RowIdSequence::from([10u64, 11, 12, 13, 14].as_slice());
        let row_id_meta = Some(RowIdMeta::Inline(write_row_ids(&row_ids)));

        let data_file = DataFile::new(
            "data.lance",
            vec![0],
            vec![0],
            ConcreteFileVersion::from(LanceFileVersion::Stable),
            None,
            None,
        );

        let fragment = Fragment {
            id: 1,
            files: vec![data_file],
            overlays: vec![],
            deletion_file: None,
            row_id_meta,
            physical_rows: Some(5),
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        };

        let manifest = make_stable_row_id_manifest(vec![fragment.clone()]);

        // Simulate a RewriteColumns update that matched offsets 1 and 3
        let off_map = HashMap::from([(1u64, RoaringBitmap::from_iter([1u32, 3]))]);
        let tx = Transaction::new(
            manifest.version,
            Operation::Update {
                removed_fragment_ids: vec![],
                updated_fragments: vec![fragment],
                new_fragments: vec![],
                fields_modified: vec![],
                compacted_sstables: vec![],
                fields_for_preserving_frag_bitmap: vec![],
                update_mode: Some(UpdateMode::RewriteColumns),
                inserted_rows_filter: None,
                updated_fragment_offsets: Some(UpdatedFragmentOffsets(off_map)),
            },
            None,
        );

        let (out, _) = tx
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        assert!(
            out.fragments[0].last_updated_at_version_meta.is_none(),
            "fragment with no prior version metadata must not have fabricated prev_version stamped on unmatched rows"
        );
    }

    /// Partial RewriteColumns refresh in `build_manifest`: only matched physical
    /// rows get `last_updated_at_version` bumped; same-fragment unmatched rows and
    /// untouched fragments keep both version sequences.
    #[tokio::test]
    async fn test_build_manifest_partial_last_updated_rewrite_columns_stable_row_ids() {
        let dir = TempStrDir::default();
        let uri = dir.as_str();

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", DataType::Int32, false),
            ArrowField::new("x", DataType::Int32, false),
        ]));
        let batch0 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..8)),
                Arc::new(Int32Array::from(vec![0_i32; 8])),
            ],
        )
        .unwrap();
        let reader0 = RecordBatchIterator::new(vec![Ok(batch0)], schema.clone());
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::Stable),
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader0, uri, Some(write_params))
            .await
            .unwrap();

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(100..108)),
                Arc::new(Int32Array::from(vec![0_i32; 8])),
            ],
        )
        .unwrap();
        let reader1 = RecordBatchIterator::new(vec![Ok(batch1)], schema.clone());
        dataset.append(reader1, None).await.unwrap();

        let frags = dataset.get_fragments();
        assert_eq!(
            frags.len(),
            2,
            "expected two fragments (append creates a new fragment)"
        );

        async fn scan_row_versions(ds: &Dataset) -> HashMap<(u32, u32), (u64, u64)> {
            let mut scanner = ds.scan();
            scanner
                .project(&[
                    ROW_ADDR,
                    ROW_LAST_UPDATED_AT_VERSION,
                    ROW_CREATED_AT_VERSION,
                ])
                .unwrap();
            let batches = scanner
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let mut out = HashMap::new();
            for batch in batches {
                let addrs = batch
                    .column_by_name(ROW_ADDR)
                    .unwrap()
                    .as_primitive::<UInt64Type>();
                let last = batch
                    .column_by_name(ROW_LAST_UPDATED_AT_VERSION)
                    .unwrap()
                    .as_primitive::<UInt64Type>();
                let created = batch
                    .column_by_name(ROW_CREATED_AT_VERSION)
                    .unwrap()
                    .as_primitive::<UInt64Type>();
                for row in 0..batch.num_rows() {
                    let addr = RowAddress::from(addrs.value(row));
                    out.insert(
                        (addr.fragment_id(), addr.row_offset()),
                        (last.value(row), created.value(row)),
                    );
                }
            }
            out
        }

        let before = scan_row_versions(&dataset).await;
        assert_eq!(before.len(), 16);

        // Update only rows i in {2, 4, 6} within fragment 0 (physical offsets 2, 4, 6).
        let update_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", DataType::Int32, false),
            ArrowField::new("x", DataType::Int32, false),
        ]));
        let update_batch = RecordBatch::try_new(
            update_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 4, 6])),
                Arc::new(Int32Array::from(vec![99, 99, 99])),
            ],
        )
        .unwrap();
        let right: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
            RecordBatchIterator::new(vec![Ok(update_batch)].into_iter(), update_schema),
        );

        let mut frag0 = dataset.get_fragment(0).unwrap();
        let u = frag0
            .update_columns_with_offsets(right, "i", "i")
            .await
            .unwrap();
        assert_eq!(u.matched_offsets.iter().count(), 3);
        for off in [2_u32, 4, 6] {
            assert!(u.matched_offsets.contains(off));
        }

        let updated_fragment_offsets = Some(UpdatedFragmentOffsets(HashMap::from([(
            u.fragment.id,
            u.matched_offsets,
        )])));

        let op = Operation::Update {
            removed_fragment_ids: vec![],
            updated_fragments: vec![u.fragment],
            new_fragments: vec![],
            fields_modified: u.fields_modified,
            compacted_sstables: Vec::new(),
            fields_for_preserving_frag_bitmap: vec![],
            update_mode: Some(UpdateMode::RewriteColumns),
            inserted_rows_filter: None,
            updated_fragment_offsets,
        };

        let read_v = dataset.version().version;
        let dataset = Dataset::commit(
            uri,
            op,
            Some(read_v),
            None,
            None,
            Arc::new(Session::default()),
            true,
        )
        .await
        .unwrap();

        let new_v = dataset.version().version;
        assert_eq!(new_v, read_v + 1);

        let after = scan_row_versions(&dataset).await;
        for off in 0..8_u32 {
            let key = (0, off);
            let (last_before, created_before) = before[&key];
            let (last_after, created_after) = after[&key];
            assert_eq!(created_after, created_before);
            if off == 2 || off == 4 || off == 6 {
                assert_eq!(
                    last_after, new_v,
                    "matched row offset {off} should advance last_updated to new version"
                );
            } else {
                assert_eq!(
                    last_after, last_before,
                    "unmatched row offset {off} in fragment 0 should keep last_updated"
                );
            }
        }

        for off in 0..8_u32 {
            let key = (1, off);
            assert_eq!(
                after[&key], before[&key],
                "fragment 1 row offset {off}: both version columns unchanged"
            );
        }
    }

    /// Regression test for https://github.com/lance-format/lance/issues/6417
    ///
    /// When overwriting a LEGACY dataset with STABLE-format fragments, the
    /// validation should not use the old manifest's format. STABLE fragments
    /// omit struct parent fields, which the strict legacy check rejects.
    #[test]
    fn test_overwrite_legacy_to_stable_with_struct_fields() {
        use arrow_schema::Fields;

        // Schema: id (field 0), name (field 1), address (field 2, struct parent),
        //   city (field 3), country (field 4)
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, false),
            ArrowField::new("name", DataType::Utf8, false),
            ArrowField::new(
                "address",
                DataType::Struct(Fields::from(vec![
                    ArrowField::new("city", DataType::Utf8, false),
                    ArrowField::new("country", DataType::Utf8, false),
                ])),
                false,
            ),
        ]);
        let schema = LanceSchema::try_from(&arrow_schema).unwrap();

        // Old manifest is LEGACY format
        let legacy_manifest = Manifest::new(
            schema.clone(),
            Arc::new(vec![Fragment::new(0)]),
            DataStorageFormat::new(ConcreteFileVersion::V1),
            HashMap::new(),
        );

        // New fragments in STABLE format omit struct parent field (id=2),
        // only including leaf fields: id=0, name=1, city=3, country=4
        let stable_fragment = Fragment {
            id: 0,
            files: vec![DataFile::new(
                "data.lance",
                vec![0, 1, 3, 4], // no field 2 (struct parent)
                vec![0, 1, 2, 3],
                ConcreteFileVersion::V1,
                None,
                None,
            )],
            physical_rows: Some(10),
            overlays: vec![],
            deletion_file: None,
            row_id_meta: None,
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        };

        let operation = Operation::Overwrite {
            fragments: vec![stable_fragment],
            schema,
            config_upsert_values: None,
            initial_bases: None,
        };

        // This should succeed — the old manifest's LEGACY format should not
        // cause strict validation of the new STABLE fragments.
        validate_operation(Some(&legacy_manifest), &operation).unwrap();
    }

    /// Existing fragments use id >= 1 to avoid collision with `Fragment::new(0)`
    /// used by `sample_manifest`. New (updated) fragments use id = 10.
    fn make_stable_row_id_manifest(fragments: Vec<Fragment>) -> Manifest {
        let schema = ArrowSchema::new(vec![ArrowField::new("id", DataType::Int32, false)]);
        let mut manifest = Manifest::new(
            LanceSchema::try_from(&schema).unwrap(),
            Arc::new(fragments),
            DataStorageFormat::new(ConcreteFileVersion::V2_0),
            HashMap::new(),
        );
        manifest.reader_feature_flags = FLAG_STABLE_ROW_IDS;
        manifest.next_row_id = 1000;
        manifest.version = 4;
        manifest
    }

    fn update_txn(new_fragments: Vec<Fragment>) -> Transaction {
        Transaction::new(
            4,
            Operation::Update {
                removed_fragment_ids: vec![],
                updated_fragments: vec![],
                new_fragments,
                fields_modified: vec![],
                compacted_sstables: vec![],
                fields_for_preserving_frag_bitmap: vec![],
                update_mode: None,
                inserted_rows_filter: None,
                updated_fragment_offsets: None,
            },
            None,
        )
    }

    fn created_at_versions(manifest: &Manifest, frag_id: u64) -> Vec<u64> {
        let frag = manifest.fragments.iter().find(|f| f.id == frag_id).unwrap();
        let seq = frag
            .created_at_version_meta
            .as_ref()
            .unwrap()
            .load_sequence()
            .unwrap();
        seq.versions().collect()
    }

    fn last_updated_at_versions(manifest: &Manifest, frag_id: u64) -> Vec<u64> {
        let frag = manifest.fragments.iter().find(|f| f.id == frag_id).unwrap();
        let seq = frag
            .last_updated_at_version_meta
            .as_ref()
            .unwrap()
            .load_sequence()
            .unwrap();
        seq.versions().collect()
    }

    #[test]
    fn merge_build_manifest_refreshes_last_updated_when_data_files_change_stable_row_ids() {
        use lance_file::version::LanceFileVersion;
        use lance_table::feature_flags::FLAG_STABLE_ROW_IDS;

        let mk_file = |path: &str| {
            DataFile::new(
                path,
                vec![0],
                vec![0],
                ConcreteFileVersion::from(LanceFileVersion::Stable),
                None,
                None,
            )
        };

        let arrow_schema = ArrowSchema::new(vec![ArrowField::new("id", DataType::Int32, false)]);
        let lance_schema = LanceSchema::try_from(&arrow_schema).unwrap();

        let row_ids = RowIdSequence::from([100u64, 101, 102, 103, 104].as_slice());
        let row_id_meta = Some(RowIdMeta::Inline(write_row_ids(&row_ids)));

        let prev_fragment = Fragment {
            id: 0,
            files: vec![mk_file("before.lance")],
            overlays: vec![],
            deletion_file: None,
            row_id_meta,
            physical_rows: Some(5),
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        };

        let mut manifest = Manifest::new(
            lance_schema.clone(),
            Arc::new(vec![prev_fragment.clone()]),
            DataStorageFormat::new(ConcreteFileVersion::V2_0),
            HashMap::new(),
        );
        manifest.reader_feature_flags |= FLAG_STABLE_ROW_IDS;
        manifest.next_row_id = 100;

        let merged_fragment = Fragment {
            files: vec![mk_file("after.lance")],
            ..prev_fragment
        };

        let tx = Transaction::new(
            manifest.version,
            Operation::Merge {
                fragments: vec![merged_fragment],
                schema: lance_schema,
            },
            None,
        );

        let (out, _) = tx
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        assert_eq!(out.version, 2);
        let frag = &out.fragments[0];
        let seq = frag
            .last_updated_at_version_meta
            .as_ref()
            .unwrap()
            .load_sequence()
            .unwrap();
        assert_eq!(seq.version_at(0).unwrap(), 2);
        assert_eq!(seq.version_at(4).unwrap(), 2);
    }

    #[test]
    fn merge_build_manifest_skips_refresh_when_carry_forward_stable_row_ids() {
        use lance_file::version::LanceFileVersion;
        use lance_table::feature_flags::FLAG_STABLE_ROW_IDS;
        use lance_table::rowids::version::{RowDatasetVersionMeta, RowDatasetVersionSequence};

        let data_file = DataFile::new(
            "same.lance",
            vec![0],
            vec![0],
            ConcreteFileVersion::from(LanceFileVersion::Stable),
            None,
            None,
        );

        let arrow_schema = ArrowSchema::new(vec![ArrowField::new("id", DataType::Int32, false)]);
        let lance_schema = LanceSchema::try_from(&arrow_schema).unwrap();

        let row_ids = RowIdSequence::from([200u64, 201, 202, 203, 204].as_slice());
        let row_id_meta = Some(RowIdMeta::Inline(write_row_ids(&row_ids)));

        let uniform_v1 = RowDatasetVersionSequence::from_uniform_row_count(5, 1);
        let meta_v1 = RowDatasetVersionMeta::from_sequence(&uniform_v1).unwrap();

        let prev_fragment = Fragment {
            id: 0,
            files: vec![data_file.clone()],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: row_id_meta.clone(),
            physical_rows: Some(5),
            last_updated_at_version_meta: Some(meta_v1.clone()),
            created_at_version_meta: None,
        };

        let mut manifest = Manifest::new(
            lance_schema.clone(),
            Arc::new(vec![prev_fragment]),
            DataStorageFormat::new(ConcreteFileVersion::V2_0),
            HashMap::new(),
        );
        manifest.reader_feature_flags |= FLAG_STABLE_ROW_IDS;
        manifest.next_row_id = 100;

        let merged_fragment = Fragment {
            id: 0,
            files: vec![data_file],
            overlays: vec![],
            deletion_file: None,
            row_id_meta,
            physical_rows: Some(5),
            last_updated_at_version_meta: Some(meta_v1),
            created_at_version_meta: None,
        };

        let tx = Transaction::new(
            manifest.version,
            Operation::Merge {
                fragments: vec![merged_fragment],
                schema: lance_schema,
            },
            None,
        );

        let (out, _) = tx
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        let seq = out.fragments[0]
            .last_updated_at_version_meta
            .as_ref()
            .unwrap()
            .load_sequence()
            .unwrap();
        assert_eq!(seq.version_at(0).unwrap(), 1);
        assert_eq!(seq.version_at(4).unwrap(), 1);
    }

    #[test]
    fn merge_build_manifest_no_last_updated_refresh_without_stable_row_ids() {
        use lance_file::version::LanceFileVersion;
        use lance_table::feature_flags::FLAG_STABLE_ROW_IDS;

        let mk_file = |path: &str| {
            DataFile::new(
                path,
                vec![0],
                vec![0],
                ConcreteFileVersion::from(LanceFileVersion::Stable),
                None,
                None,
            )
        };

        let arrow_schema = ArrowSchema::new(vec![ArrowField::new("id", DataType::Int32, false)]);
        let lance_schema = LanceSchema::try_from(&arrow_schema).unwrap();

        let prev_fragment = Fragment {
            id: 0,
            files: vec![mk_file("before.lance")],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: None,
            physical_rows: Some(5),
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        };

        let manifest = Manifest::new(
            lance_schema.clone(),
            Arc::new(vec![prev_fragment.clone()]),
            DataStorageFormat::new(ConcreteFileVersion::V2_0),
            HashMap::new(),
        );
        assert_eq!(
            manifest.reader_feature_flags & FLAG_STABLE_ROW_IDS,
            0,
            "manifest must not use stable row IDs for this guard test"
        );

        let merged_fragment = Fragment {
            files: vec![mk_file("after.lance")],
            ..prev_fragment
        };

        let tx = Transaction::new(
            manifest.version,
            Operation::Merge {
                fragments: vec![merged_fragment],
                schema: lance_schema,
            },
            None,
        );

        let (out, _) = tx
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        assert!(
            out.fragments[0].last_updated_at_version_meta.is_none(),
            "without stable row IDs, Merge must not populate per-row last_updated metadata"
        );
    }

    #[test]
    fn merge_build_manifest_sets_both_version_meta_for_new_fragment_id_stable_row_ids() {
        use lance_file::version::LanceFileVersion;
        use lance_table::feature_flags::FLAG_STABLE_ROW_IDS;

        let mk_file = |path: &str| {
            DataFile::new(
                path,
                vec![0],
                vec![0],
                ConcreteFileVersion::from(LanceFileVersion::Stable),
                None,
                None,
            )
        };

        let arrow_schema = ArrowSchema::new(vec![ArrowField::new("id", DataType::Int32, false)]);
        let lance_schema = LanceSchema::try_from(&arrow_schema).unwrap();

        // Existing fragment (id=0) with stable row IDs
        let row_ids_0 = RowIdSequence::from([10u64, 11, 12].as_slice());
        let existing_fragment = Fragment {
            id: 0,
            files: vec![mk_file("existing.lance")],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&row_ids_0))),
            physical_rows: Some(3),
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        };

        let mut manifest = Manifest::new(
            lance_schema.clone(),
            Arc::new(vec![existing_fragment.clone()]),
            DataStorageFormat::new(ConcreteFileVersion::V2_0),
            HashMap::new(),
        );
        manifest.reader_feature_flags |= FLAG_STABLE_ROW_IDS;
        manifest.next_row_id = 100;
        manifest.version = 1;

        // New fragment (id=1) not present in prev manifest — exercises the None branch
        let row_ids_1 = RowIdSequence::from([20u64, 21, 22, 23].as_slice());
        let new_fragment = Fragment {
            id: 1,
            files: vec![mk_file("new.lance")],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&row_ids_1))),
            physical_rows: Some(4),
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        };

        let tx = Transaction::new(
            manifest.version,
            Operation::Merge {
                fragments: vec![existing_fragment, new_fragment],
                schema: lance_schema,
            },
            None,
        );

        let (out, _) = tx
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        assert_eq!(out.version, 2);

        let new_frag = out.fragments.iter().find(|f| f.id == 1).unwrap();

        // last_updated_at_version must be set to the commit version
        let last_updated_seq = new_frag
            .last_updated_at_version_meta
            .as_ref()
            .expect("new fragment must have last_updated_at_version_meta")
            .load_sequence()
            .unwrap();
        assert_eq!(last_updated_seq.version_at(0).unwrap(), 2);
        assert_eq!(last_updated_seq.version_at(3).unwrap(), 2);

        // created_at_version must also be set — must not be None
        let created_seq = new_frag
            .created_at_version_meta
            .as_ref()
            .expect("new fragment must have created_at_version_meta")
            .load_sequence()
            .unwrap();
        assert_eq!(created_seq.version_at(0).unwrap(), 2);
        assert_eq!(created_seq.version_at(3).unwrap(), 2);
    }

    #[test]
    fn test_update_version_tracking_preserves_created_at() {
        let existing_seq = RowIdSequence::from([100u64, 101, 102].as_slice());
        let created_at_seq = RowDatasetVersionSequence {
            runs: vec![RowDatasetVersionRun {
                span: U64Segment::Range(0..3),
                version: 5,
            }],
        };
        let existing_fragment = Fragment {
            id: 1,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&existing_seq))),
            physical_rows: Some(3),
            created_at_version_meta: Some(
                RowDatasetVersionMeta::from_sequence(&created_at_seq).unwrap(),
            ),
            last_updated_at_version_meta: None,
        };

        let new_seq = RowIdSequence::from([100u64, 102].as_slice());
        let new_fragment = Fragment {
            id: 10,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&new_seq))),
            physical_rows: Some(2),
            created_at_version_meta: None,
            last_updated_at_version_meta: None,
        };

        let manifest = make_stable_row_id_manifest(vec![existing_fragment]);
        let (result, _) = update_txn(vec![new_fragment])
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        assert_eq!(created_at_versions(&result, 10), vec![5, 5]);
        assert_eq!(last_updated_at_versions(&result, 10), vec![5, 5]);
    }

    #[test]
    fn test_update_version_tracking_mixed_origins() {
        let frag_a_seq = RowIdSequence::from([10u64, 11].as_slice());
        let frag_a_created = RowDatasetVersionSequence {
            runs: vec![RowDatasetVersionRun {
                span: U64Segment::Range(0..2),
                version: 2,
            }],
        };
        let frag_b_seq = RowIdSequence::from([20u64, 21, 22].as_slice());
        let frag_b_created = RowDatasetVersionSequence {
            runs: vec![RowDatasetVersionRun {
                span: U64Segment::Range(0..3),
                version: 3,
            }],
        };

        let manifest = make_stable_row_id_manifest(vec![
            Fragment {
                id: 1,
                files: vec![],
                overlays: vec![],
                deletion_file: None,
                row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&frag_a_seq))),
                physical_rows: Some(2),
                created_at_version_meta: Some(
                    RowDatasetVersionMeta::from_sequence(&frag_a_created).unwrap(),
                ),
                last_updated_at_version_meta: None,
            },
            Fragment {
                id: 2,
                files: vec![],
                overlays: vec![],
                deletion_file: None,
                row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&frag_b_seq))),
                physical_rows: Some(3),
                created_at_version_meta: Some(
                    RowDatasetVersionMeta::from_sequence(&frag_b_created).unwrap(),
                ),
                last_updated_at_version_meta: None,
            },
        ]);

        // New fragment has rows from both original fragments: row 11 from frag_a, row 20 from frag_b
        let new_seq = RowIdSequence::from([11u64, 20].as_slice());
        let new_fragment = Fragment {
            id: 10,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&new_seq))),
            physical_rows: Some(2),
            created_at_version_meta: None,
            last_updated_at_version_meta: None,
        };

        let (result, _) = update_txn(vec![new_fragment])
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        // Row 11 came from frag_a (offset 1, version 2), row 20 came from frag_b (offset 0, version 3)
        assert_eq!(created_at_versions(&result, 10), vec![2, 3]);
        assert_eq!(last_updated_at_versions(&result, 10), vec![5, 5]);
    }

    #[test]
    fn test_update_version_tracking_insert_branch_gets_new_version() {
        // Simulates the INSERT branch (NOT MATCHED) of a MERGE INTO commit:
        // the new fragment contains a mix of rewritten rows (UPDATE branch, row ID
        // present in existing fragments) and freshly inserted rows (INSERT branch,
        // row ID not present in any existing fragment).
        //
        // UPDATE branch row (10): created_at must be copied from the source fragment.
        // INSERT branch row (999): created_at must equal new_version (the merge commit
        // version), because the row first appeared in this commit.
        let existing_seq = RowIdSequence::from([10u64, 11].as_slice());
        let existing_created = RowDatasetVersionSequence {
            runs: vec![RowDatasetVersionRun {
                span: U64Segment::Range(0..2),
                version: 5,
            }],
        };
        let existing_fragment = Fragment {
            id: 1,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&existing_seq))),
            physical_rows: Some(2),
            created_at_version_meta: Some(
                RowDatasetVersionMeta::from_sequence(&existing_created).unwrap(),
            ),
            last_updated_at_version_meta: None,
        };

        // New fragment has row 10 (UPDATE branch) and row 999 (INSERT branch)
        let new_seq = RowIdSequence::from([10u64, 999].as_slice());
        let new_fragment = Fragment {
            id: 10,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&new_seq))),
            physical_rows: Some(2),
            created_at_version_meta: None,
            last_updated_at_version_meta: None,
        };

        // update_txn uses read_version 4 → new_version is 5
        let manifest = make_stable_row_id_manifest(vec![existing_fragment]);
        let (result, _) = update_txn(vec![new_fragment])
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        // Row 10 (UPDATE branch): created_at copied from source (version 5).
        // Row 999 (INSERT branch): created_at == new_version (5).
        assert_eq!(created_at_versions(&result, 10), vec![5, 5]);
        assert_eq!(last_updated_at_versions(&result, 10), vec![5, 5]);
    }

    #[test]
    fn test_update_version_tracking_merge_into_distinguishes_insert_and_update_branch() {
        // Verifies the MERGE INTO correctness contract when UPDATE branch rows and INSERT
        // branch rows have *different* source created_at values, so we can distinguish
        // which row got which value.
        //
        // Existing fragment (id=1): row IDs [10, 11], created_at = version 3.
        // New fragment (id=20): row IDs [10, 500, 11, 501].
        //   - Rows 10 and 11: UPDATE branch (present in existing fragment) → created_at = 3.
        //   - Rows 500 and 501: INSERT branch (no source) → created_at = new_version = 5.
        let existing_seq = RowIdSequence::from([10u64, 11].as_slice());
        let existing_created = RowDatasetVersionSequence {
            runs: vec![RowDatasetVersionRun {
                span: U64Segment::Range(0..2),
                version: 3,
            }],
        };
        let existing_fragment = Fragment {
            id: 1,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&existing_seq))),
            physical_rows: Some(2),
            created_at_version_meta: Some(
                RowDatasetVersionMeta::from_sequence(&existing_created).unwrap(),
            ),
            last_updated_at_version_meta: None,
        };

        let new_seq = RowIdSequence::from([10u64, 500, 11, 501].as_slice());
        let new_fragment = Fragment {
            id: 20,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&new_seq))),
            physical_rows: Some(4),
            created_at_version_meta: None,
            last_updated_at_version_meta: None,
        };

        // update_txn uses read_version 4 → new_version is 5
        let manifest = make_stable_row_id_manifest(vec![existing_fragment]);
        let (result, _) = update_txn(vec![new_fragment])
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        // UPDATE branch rows (10, 11): created_at preserved from source (version 3).
        // INSERT branch rows (500, 501): created_at == new_version (5).
        assert_eq!(created_at_versions(&result, 20), vec![3, 5, 3, 5]);
        // All rows in the new fragment get last_updated == new_version.
        assert_eq!(last_updated_at_versions(&result, 20), vec![5, 5, 5, 5]);
    }

    #[test]
    fn test_update_version_tracking_source_fragment_no_created_at_defaults_to_1() {
        // Source fragment has row_id_meta but no created_at_version_meta.
        // The row IS found in the lookup, but the version defaults to 1.
        let existing_seq = RowIdSequence::from([50u64, 51].as_slice());
        let existing_fragment = Fragment {
            id: 1,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&existing_seq))),
            physical_rows: Some(2),
            created_at_version_meta: None,
            last_updated_at_version_meta: None,
        };

        let new_seq = RowIdSequence::from([50u64].as_slice());
        let new_fragment = Fragment {
            id: 10,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&new_seq))),
            physical_rows: Some(1),
            created_at_version_meta: None,
            last_updated_at_version_meta: None,
        };

        let manifest = make_stable_row_id_manifest(vec![existing_fragment]);
        let (result, _) = update_txn(vec![new_fragment])
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        // Row 50 is found in source but source has no created_at_version_meta → default 1
        assert_eq!(created_at_versions(&result, 10), vec![1]);
        assert_eq!(last_updated_at_versions(&result, 10), vec![5]);
    }

    #[test]
    fn test_update_version_tracking_no_row_id_meta_fallback() {
        let existing_seq = RowIdSequence::from([10u64, 11].as_slice());
        let existing_fragment = Fragment {
            id: 1,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&existing_seq))),
            physical_rows: Some(2),
            created_at_version_meta: None,
            last_updated_at_version_meta: None,
        };

        let new_fragment = Fragment {
            id: 10,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: None,
            physical_rows: Some(3),
            created_at_version_meta: None,
            last_updated_at_version_meta: None,
        };

        let manifest = make_stable_row_id_manifest(vec![existing_fragment]);
        let (result, _) = update_txn(vec![new_fragment])
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        // Fragment starts with no row_id_meta → assign_row_ids gives it fresh IDs →
        // those IDs have no source in existing fragments (INSERT branch) →
        // created_at == new_version (5) for each row.
        assert_eq!(created_at_versions(&result, 10), vec![5, 5, 5]);
        assert_eq!(last_updated_at_versions(&result, 10), vec![5, 5, 5]);
    }

    #[test]
    fn test_update_version_tracking_corrupt_created_at_defaults_to_1() {
        let existing_seq = RowIdSequence::from([10u64, 11].as_slice());
        let existing_fragment = Fragment {
            id: 1,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&existing_seq))),
            physical_rows: Some(2),
            created_at_version_meta: Some(RowDatasetVersionMeta::Inline(Arc::from(
                vec![0xFFu8; 8].as_slice(),
            ))),
            last_updated_at_version_meta: None,
        };

        let new_seq = RowIdSequence::from([10u64].as_slice());
        let new_fragment = Fragment {
            id: 10,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&new_seq))),
            physical_rows: Some(1),
            created_at_version_meta: None,
            last_updated_at_version_meta: None,
        };

        let manifest = make_stable_row_id_manifest(vec![existing_fragment]);
        let (result, _) = update_txn(vec![new_fragment])
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        // Corrupt metadata causes decode to fail → falls back to UNKNOWN_CREATED_AT_VERSION (1)
        assert_eq!(created_at_versions(&result, 10), vec![1]);
        assert_eq!(last_updated_at_versions(&result, 10), vec![5]);
    }

    // --- Proposal 1: range pre-filter ---

    /// Fragments whose row-ID range lies entirely outside the needed set must not
    /// affect the result.  Here fragment 1 has IDs [1000, 1001] which are far above
    /// the needed range [10, 11]; it is skipped by the range pre-filter and its
    /// created_at version (version 99) must never appear in the output.
    #[test]
    fn test_update_version_tracking_range_filter_skips_non_overlapping_fragment() {
        // Fragment in range – IDs [10, 11], created_at = 5
        let in_range_seq = RowIdSequence::from([10u64, 11].as_slice());
        let in_range_created = RowDatasetVersionSequence {
            runs: vec![RowDatasetVersionRun {
                span: U64Segment::Range(0..2),
                version: 5,
            }],
        };
        let in_range_frag = Fragment {
            id: 1,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&in_range_seq))),
            physical_rows: Some(2),
            created_at_version_meta: Some(
                RowDatasetVersionMeta::from_sequence(&in_range_created).unwrap(),
            ),
            last_updated_at_version_meta: None,
        };

        // Fragment outside range – IDs [1000, 1001], created_at = 99 (must never appear)
        let out_of_range_seq = RowIdSequence::from([1000u64, 1001].as_slice());
        let out_of_range_created = RowDatasetVersionSequence {
            runs: vec![RowDatasetVersionRun {
                span: U64Segment::Range(0..2),
                version: 99,
            }],
        };
        let out_of_range_frag = Fragment {
            id: 2,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&out_of_range_seq))),
            physical_rows: Some(2),
            created_at_version_meta: Some(
                RowDatasetVersionMeta::from_sequence(&out_of_range_created).unwrap(),
            ),
            last_updated_at_version_meta: None,
        };

        // New fragment rewrites both rows from the in-range fragment
        let new_seq = RowIdSequence::from([10u64, 11].as_slice());
        let new_frag = Fragment {
            id: 10,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&new_seq))),
            physical_rows: Some(2),
            created_at_version_meta: None,
            last_updated_at_version_meta: None,
        };

        let manifest = make_stable_row_id_manifest(vec![in_range_frag, out_of_range_frag]);
        let (result, _) = update_txn(vec![new_frag])
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        // Both rows originate from the in-range fragment (version 5).
        // The out-of-range fragment's version 99 must not appear.
        assert_eq!(created_at_versions(&result, 10), vec![5, 5]);
        assert_eq!(last_updated_at_versions(&result, 10), vec![5, 5]);
    }

    /// When the needed row IDs fall exactly at the boundary of a fragment's range,
    /// the range pre-filter must NOT skip the fragment (boundary values are inclusive).
    #[test]
    fn test_update_version_tracking_range_filter_boundary_inclusive() {
        // Fragment IDs [10, 11, 12], created_at = 7
        let seq = RowIdSequence::from([10u64, 11, 12].as_slice());
        let created = RowDatasetVersionSequence {
            runs: vec![RowDatasetVersionRun {
                span: U64Segment::Range(0..3),
                version: 7,
            }],
        };
        let existing = Fragment {
            id: 1,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&seq))),
            physical_rows: Some(3),
            created_at_version_meta: Some(RowDatasetVersionMeta::from_sequence(&created).unwrap()),
            last_updated_at_version_meta: None,
        };

        // New fragment takes the boundary IDs: 10 (min) and 12 (max)
        let new_seq = RowIdSequence::from([10u64, 12].as_slice());
        let new_frag = Fragment {
            id: 10,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&new_seq))),
            physical_rows: Some(2),
            created_at_version_meta: None,
            last_updated_at_version_meta: None,
        };

        let manifest = make_stable_row_id_manifest(vec![existing]);
        let (result, _) = update_txn(vec![new_frag])
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        // Boundary IDs must be found and resolved correctly
        assert_eq!(created_at_versions(&result, 10), vec![7, 7]);
    }

    // --- Proposal 2: version sequence cache ---

    /// When multiple updated rows all originate from the same source fragment,
    /// the created_at version sequence for that fragment must be decoded exactly
    /// once (not once per row).  The observable correctness requirement is that
    /// all rows get the right version regardless of how many there are.
    #[test]
    fn test_update_version_tracking_many_rows_same_source_fragment() {
        // Source fragment: 100 rows with IDs 0..100, mixed versions (2 runs).
        // First 50 rows at version 3, next 50 rows at version 4.
        let src_ids: Vec<u64> = (0u64..100).collect();
        let src_seq = RowIdSequence::from(src_ids.as_slice());
        let src_created = RowDatasetVersionSequence {
            runs: vec![
                RowDatasetVersionRun {
                    span: U64Segment::Range(0..50),
                    version: 3,
                },
                RowDatasetVersionRun {
                    span: U64Segment::Range(0..50),
                    version: 4,
                },
            ],
        };
        let src_frag = Fragment {
            id: 1,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&src_seq))),
            physical_rows: Some(100),
            created_at_version_meta: Some(
                RowDatasetVersionMeta::from_sequence(&src_created).unwrap(),
            ),
            last_updated_at_version_meta: None,
        };

        // New fragment rewrites all 100 rows preserving their stable IDs.
        let new_seq = RowIdSequence::from(src_ids.as_slice());
        let new_frag = Fragment {
            id: 10,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&new_seq))),
            physical_rows: Some(100),
            created_at_version_meta: None,
            last_updated_at_version_meta: None,
        };

        let manifest = make_stable_row_id_manifest(vec![src_frag]);
        let (result, _) = update_txn(vec![new_frag])
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        let versions = created_at_versions(&result, 10);
        assert_eq!(versions.len(), 100);
        // First 50 rows came from version 3, next 50 from version 4
        assert!(versions[..50].iter().all(|&v| v == 3));
        assert!(versions[50..].iter().all(|&v| v == 4));
    }

    /// Rows originating from multiple distinct source fragments must each get
    /// the version from their own source, even when all cached together.
    #[test]
    fn test_update_version_tracking_cache_multiple_source_fragments() {
        let seq_a = RowIdSequence::from([10u64, 11, 12].as_slice());
        let created_a = RowDatasetVersionSequence {
            runs: vec![RowDatasetVersionRun {
                span: U64Segment::Range(0..3),
                version: 2,
            }],
        };
        let seq_b = RowIdSequence::from([20u64, 21, 22].as_slice());
        let created_b = RowDatasetVersionSequence {
            runs: vec![RowDatasetVersionRun {
                span: U64Segment::Range(0..3),
                version: 8,
            }],
        };

        let manifest = make_stable_row_id_manifest(vec![
            Fragment {
                id: 1,
                files: vec![],
                overlays: vec![],
                deletion_file: None,
                row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&seq_a))),
                physical_rows: Some(3),
                created_at_version_meta: Some(
                    RowDatasetVersionMeta::from_sequence(&created_a).unwrap(),
                ),
                last_updated_at_version_meta: None,
            },
            Fragment {
                id: 2,
                files: vec![],
                overlays: vec![],
                deletion_file: None,
                row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&seq_b))),
                physical_rows: Some(3),
                created_at_version_meta: Some(
                    RowDatasetVersionMeta::from_sequence(&created_b).unwrap(),
                ),
                last_updated_at_version_meta: None,
            },
        ]);

        // New fragment takes rows from both sources: 12 (frag A, offset 2) and 20 (frag B, offset 0)
        let new_seq = RowIdSequence::from([12u64, 20].as_slice());
        let new_frag = Fragment {
            id: 10,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: Some(RowIdMeta::Inline(write_row_ids(&new_seq))),
            physical_rows: Some(2),
            created_at_version_meta: None,
            last_updated_at_version_meta: None,
        };

        let (result, _) = update_txn(vec![new_frag])
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        // Row 12 → frag A offset 2 → version 2; row 20 → frag B offset 0 → version 8
        assert_eq!(created_at_versions(&result, 10), vec![2, 8]);
    }

    #[test]
    fn test_encode_version_runs_empty() {
        let runs = encode_version_runs(&[]);
        assert!(runs.is_empty());
    }

    #[test]
    fn test_encode_version_runs_single_run() {
        let runs = encode_version_runs(&[3, 3, 3]);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].version, 3);
    }

    #[test]
    fn test_encode_version_runs_alternating() {
        let runs = encode_version_runs(&[1, 2, 1, 2]);
        assert_eq!(runs.len(), 4);
        assert_eq!(runs[0].version, 1);
        assert_eq!(runs[1].version, 2);
        assert_eq!(runs[2].version, 1);
        assert_eq!(runs[3].version, 2);
    }

    fn table_metadata_update(entries: Vec<(&str, Option<&str>)>, replace: bool) -> Operation {
        Operation::UpdateConfig {
            config_updates: None,
            table_metadata_updates: Some(UpdateMap {
                update_entries: entries.into_iter().map(UpdateMapEntry::from).collect(),
                replace,
            }),
            schema_metadata_updates: None,
            field_metadata_updates: HashMap::new(),
        }
    }

    #[test]
    fn test_table_metadata_conflicts_on_same_key() {
        let left = table_metadata_update(vec![("key", Some("1"))], false);
        let same_key = table_metadata_update(vec![("key", Some("2"))], false);
        let different_key = table_metadata_update(vec![("other", Some("2"))], false);
        let replace = table_metadata_update(vec![("other", Some("2"))], true);

        assert!(left.modifies_same_metadata(&same_key));
        assert!(!left.modifies_same_metadata(&different_key));
        assert!(left.modifies_same_metadata(&replace));
    }

    #[test]
    fn test_data_overlay_operation_roundtrips() {
        // A DataOverlay operation survives the protobuf round-trip, preserving
        // the target fragment, the overlay's coverage, and its committed_version.
        let mut bitmap = roaring::RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(4);
        let overlay = DataOverlayFile {
            data_file: DataFile::new_legacy_from_fields("overlay-0.lance", vec![3], None),
            coverage: OverlayCoverage::dense(bitmap.clone()),
            committed_version: 6,
        };
        let pb_overlay = pb::DataOverlayFile::from(&overlay);

        let message = pb::Transaction {
            read_version: 1,
            uuid: Uuid::new_v4().to_string(),
            operation: Some(pb::transaction::Operation::DataOverlay(
                pb::transaction::DataOverlay {
                    groups: vec![pb::transaction::DataOverlayGroup {
                        fragment_id: 7,
                        overlays: vec![pb_overlay],
                    }],
                },
            )),
            ..Default::default()
        };

        let txn = Transaction::try_from(message).unwrap();
        match txn.operation {
            Operation::DataOverlay { groups } => {
                assert_eq!(groups.len(), 1);
                assert_eq!(groups[0].fragment_id, 7);
                assert_eq!(groups[0].overlays.len(), 1);
                assert_eq!(groups[0].overlays[0].committed_version, 6);
                assert_eq!(
                    *groups[0].overlays[0].coverage_for_field(0).unwrap(),
                    bitmap
                );
            }
            other => panic!("expected DataOverlay, got {other:?}"),
        }
    }

    fn overlay_with_field(field: i32, committed_version: u64) -> DataOverlayFile {
        DataOverlayFile {
            data_file: DataFile::new_legacy_from_fields("o.lance", vec![field], None),
            coverage: OverlayCoverage::dense(roaring::RoaringBitmap::from_iter([0u32])),
            committed_version,
        }
    }

    #[test]
    fn test_prune_overlay_stale_fields_from_indices() {
        // Fragment 0 carried an overlay on field 1 committed at v5, and was
        // fully compacted into new fragment 7.
        let mut old_frag = Fragment::new(0);
        old_frag.overlays = vec![overlay_with_field(1, 5)];
        let groups = vec![RewriteGroup {
            old_fragments: vec![old_frag],
            new_fragments: vec![Fragment::new(7)],
        }];

        // Post-remap state: every index already covers the new fragment (7).
        let covering = || Some(RoaringBitmap::from_iter([7u32]));
        let mut indices = vec![
            // Stale: covers the overlaid field 1, built (v2) before the overlay.
            create_test_index("stale", 1, 2, covering(), false),
            // Not stale: covers field 1 but built at the overlay's version (v5);
            // `committed_version > dataset_version` is false at equality.
            create_test_index("fresh", 1, 5, covering(), false),
            // Unrelated: covers field 2, which the overlay never touched.
            create_test_index("unrelated", 2, 2, covering(), false),
        ];

        Transaction::prune_overlay_stale_fields_from_indices(&mut indices, &groups);

        assert!(
            !indices[0].fragment_bitmap.as_ref().unwrap().contains(7),
            "stale index must drop the rewritten fragment from its coverage"
        );
        assert!(
            indices[1].fragment_bitmap.as_ref().unwrap().contains(7),
            "an index built at/after the overlay is not stale"
        );
        assert!(
            indices[2].fragment_bitmap.as_ref().unwrap().contains(7),
            "an index on an un-overlaid field is unaffected"
        );
    }

    #[test]
    fn test_data_overlay_build_manifest_multi_fragment() {
        // Overlays targeting two distinct fragments are each applied and stamped.
        // A targeted fragment already carrying an overlay (committed at v3) gets
        // the new overlay appended and stamped while its existing overlay is
        // preserved, and a fragment the operation does not target is passed
        // through with its existing overlays untouched.
        let mut frag0 = Fragment::new(0);
        frag0.overlays = vec![overlay_with_field(5, 3)]; // targeted, pre-existing at v3
        let frag1 = Fragment::new(1);
        let mut frag2 = Fragment::new(2);
        frag2.overlays = vec![overlay_with_field(9, 3)]; // untargeted, committed at v3
        let schema = ArrowSchema::new(vec![ArrowField::new("id", DataType::Int32, false)]);
        let mut manifest = Manifest::new(
            LanceSchema::try_from(&schema).unwrap(),
            Arc::new(vec![frag0, frag1, frag2]),
            lance_table::format::DataStorageFormat::new(ConcreteFileVersion::V2_0),
            HashMap::new(),
        );
        // The pre-existing overlays were committed at v3, so the current
        // manifest must be at least that version; the new commit then stamps
        // its overlay at v4, keeping the fragment's overlays newest-last.
        manifest.version = 3;

        let txn = Transaction::new(
            manifest.version,
            Operation::DataOverlay {
                groups: vec![
                    DataOverlayGroup {
                        fragment_id: 0,
                        overlays: vec![overlay_with_field(1, 0)],
                    },
                    DataOverlayGroup {
                        fragment_id: 1,
                        overlays: vec![overlay_with_field(2, 0)],
                    },
                ],
            },
            None,
        );

        let (result, _) = txn
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        let frag = |id: u64| {
            result
                .fragments
                .iter()
                .find(|f| f.id == id)
                .unwrap_or_else(|| panic!("fragment {id} missing from result"))
        };
        // The already-overlaid target keeps its v3 overlay and appends the new
        // one, stamped to the new version.
        assert_eq!(frag(0).overlays.len(), 2);
        assert_eq!(frag(0).overlays[0].committed_version, 3);
        assert_eq!(frag(0).overlays[1].committed_version, result.version);
        // The fresh target gets its overlay, stamped to the new version.
        assert_eq!(frag(1).overlays.len(), 1);
        assert_eq!(frag(1).overlays[0].committed_version, result.version);
        // The untargeted fragment is unchanged: same overlay, original version.
        assert_eq!(frag(2).overlays.len(), 1);
        assert_eq!(frag(2).overlays[0].committed_version, 3);
        assert!(result.version > manifest.version);
    }

    #[test]
    fn test_data_replacement_tombstones_overlaid_fields() {
        // A DataReplacement writing new base values for field 5 must stop any
        // overlay from shadowing those cells: field 5 is tombstoned in place
        // (preserving the overlay's field 3), and an overlay covering only field
        // 5 is dropped entirely.
        let mut fragment = Fragment::new(0);
        fragment.files = vec![
            DataFile::new_legacy_from_fields("f3.lance", vec![3], None),
            DataFile::new_legacy_from_fields("f5.lance", vec![5], None),
        ];
        fragment.overlays = vec![
            DataOverlayFile {
                data_file: DataFile::new_legacy_from_fields("o35.lance", vec![3, 5], None),
                coverage: OverlayCoverage::sparse(vec![
                    roaring::RoaringBitmap::from_iter([0u32]),
                    roaring::RoaringBitmap::from_iter([0u32]),
                ]),
                committed_version: 3,
            },
            DataOverlayFile {
                data_file: DataFile::new_legacy_from_fields("o5.lance", vec![5], None),
                coverage: OverlayCoverage::dense(roaring::RoaringBitmap::from_iter([0u32])),
                committed_version: 3,
            },
        ];

        let schema = ArrowSchema::new(vec![ArrowField::new("id", DataType::Int32, false)]);
        let manifest = Manifest::new(
            LanceSchema::try_from(&schema).unwrap(),
            Arc::new(vec![fragment]),
            lance_table::format::DataStorageFormat::new(ConcreteFileVersion::V2_0),
            HashMap::new(),
        );

        let txn = Transaction::new(
            manifest.version,
            Operation::DataReplacement {
                replacements: vec![DataReplacementGroup(
                    0,
                    DataFile::new_legacy_from_fields("f5-new.lance", vec![5], None),
                )],
            },
            None,
        );

        let (result, _) = txn
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        let frag = &result.fragments[0];
        // The base data file for field 5 was swapped in.
        assert!(frag.files.iter().any(|f| f.path == "f5-new.lance"));
        // The [3, 5] overlay keeps field 3 and tombstones field 5; the [5]-only
        // overlay is dropped.
        assert_eq!(frag.overlays.len(), 1);
        assert_eq!(frag.overlays[0].data_file.fields.as_ref(), &[3, -2]);
    }

    #[test]
    fn test_data_overlay_build_manifest_merges_duplicate_groups() {
        // Two groups targeting the same fragment must both survive (a HashMap
        // collapse would have dropped the first).
        let manifest = sample_manifest();
        let txn = Transaction::new(
            manifest.version,
            Operation::DataOverlay {
                groups: vec![
                    DataOverlayGroup {
                        fragment_id: 0,
                        overlays: vec![overlay_with_field(1, 0)],
                    },
                    DataOverlayGroup {
                        fragment_id: 0,
                        overlays: vec![overlay_with_field(2, 0)],
                    },
                ],
            },
            None,
        );

        let (result, _) = txn
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap();

        let overlays = &result.fragments[0].overlays;
        assert_eq!(overlays.len(), 2);
        assert_eq!(overlays[0].data_file.fields.as_ref(), [1i32].as_slice());
        assert_eq!(overlays[1].data_file.fields.as_ref(), [2i32].as_slice());
    }

    #[test]
    fn test_data_overlay_build_manifest_rejects_unknown_fragment() {
        let manifest = sample_manifest();
        let txn = Transaction::new(
            manifest.version,
            Operation::DataOverlay {
                groups: vec![DataOverlayGroup {
                    fragment_id: 99,
                    overlays: vec![overlay_with_field(1, 0)],
                }],
            },
            None,
        );
        let err = txn
            .build_manifest(
                Some(&manifest),
                vec![],
                "txn",
                &ManifestWriteConfig::default(),
            )
            .unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{err}");
    }

    #[test]
    fn test_data_overlay_operation_eq() {
        let overlay = |field: i32| Operation::DataOverlay {
            groups: vec![DataOverlayGroup {
                fragment_id: 0,
                overlays: vec![overlay_with_field(field, 1)],
            }],
        };
        // Reflexive and value-based (the arm previously returned false for self).
        assert_eq!(overlay(1), overlay(1));
        assert_ne!(overlay(1), overlay(2));
        // Not equal to a different operation kind (previously returned true vs Rewrite).
        let rewrite = Operation::Rewrite {
            groups: vec![],
            rewritten_indices: vec![],
            frag_reuse_index: None,
        };
        assert_ne!(overlay(1), rewrite);
    }
}
