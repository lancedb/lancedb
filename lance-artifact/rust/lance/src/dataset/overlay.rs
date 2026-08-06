// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Resolution of data overlay files on read.
//!
//! An overlay supplies replacement values for some `(row, field)` cells without
//! rewriting the base data. Resolving a read means, for each row we return,
//! deciding whether its value comes from the base column or from an overlay.
//!
//! Three coordinate spaces show up throughout this module; keeping them straight
//! is most of the work:
//!
//! - `offset_in_frag`: a row's physical position in the fragment (0-based over all
//!   physical rows, ignoring deletions). This is how a cell is addressed on disk
//!   and in an overlay's coverage bitmap.
//! - `offset_in_batch`: a row's position within the batch we are currently
//!   assembling (0-based). The output column is indexed by this.
//! - `offset_in_overlay`: the position of a value in an overlay's value column.
//!   An overlay stores its values densely — one per covered cell, in ascending
//!   `offset_in_frag` order — so a covered cell's value is found by counting how
//!   many covered cells come before it. (That count is what a roaring bitmap calls
//!   the cell's "rank".)
//!
//! For a given field, the overlays covering it are consulted newest to oldest: the
//! first overlay that covers a row wins, and its value is read at that row's
//! `offset_in_overlay`. A row that no overlay covers keeps its base value.
//!
//! The rows to resolve are passed in as a list of `offset_in_frag` (one per output
//! row), so a single code path serves both scans (a contiguous range of offsets)
//! and `take` (arbitrary offsets).
//!
//! Deletions win over overlays, but nothing here handles that: the merge runs on
//! physical rows *before* deletions are applied, so an overlay value computed for a
//! deleted row is simply dropped along with the row. This matches the spec with no
//! special casing.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_select::interleave::interleave;
use futures::StreamExt;
use lance_core::datatypes::{Field, Schema};
use lance_core::{Error, Result};
use roaring::RoaringBitmap;

use lance_table::format::overlay::DataOverlayFile;
use lance_table::format::{DataFile, Fragment, IndexMetadata};
use lance_table::utils::stream::ReadBatchFut;

use crate::dataset::fragment::{FileFragment, FragReadConfig, GenericFileReader};

/// The physical offsets within a fragment whose value for an indexed field may be
/// stale relative to an index built at `index_version`, and so must be excluded
/// from that index's results and re-evaluated against current values on the flat
/// path.
///
/// The set is the union, over every overlay whose `committed_version` is newer
/// than `index_version`, of that overlay's coverage **restricted to the indexed
/// fields**. The restriction makes exclusion field-aware: an overlay that touches
/// only non-indexed fields contributes nothing. An overlay whose
/// `committed_version <= index_version` is already incorporated by the index and
/// is ignored.
pub fn overlay_exclusion_offsets(
    overlays: &[DataOverlayFile],
    indexed_field_ids: &[i32],
    index_version: u64,
    schema: &Schema,
) -> Result<RoaringBitmap> {
    let mut excluded = RoaringBitmap::new();
    for overlay in overlays {
        if overlay.committed_version <= index_version {
            continue;
        }
        for (field_pos, field_id) in overlay.data_file.fields.iter().enumerate() {
            let overlay_ancestry = schema.field_ancestry_by_id(*field_id);
            let affects_index = indexed_field_ids.iter().any(|indexed_field_id| {
                indexed_field_id == field_id
                    || overlay_ancestry.as_ref().is_some_and(|ancestry| {
                        ancestry
                            .iter()
                            .any(|ancestor| ancestor.id == *indexed_field_id)
                    })
                    || schema
                        .field_ancestry_by_id(*indexed_field_id)
                        .is_some_and(|ancestry| {
                            ancestry.iter().any(|ancestor| ancestor.id == *field_id)
                        })
            });
            if affects_index {
                excluded |= &*overlay.coverage_for_field(field_pos)?;
            }
        }
    }
    Ok(excluded)
}

// Stale row offsets contributed by one fragment's overlays for a given index version.
// Applies a cheap version gate first: if every overlay predates the segment it is already
// incorporated by the index, so there is nothing stale and the field/bitmap work is skipped.
fn stale_offsets_for_fragment(
    fragment: &Fragment,
    fields: &[i32],
    index_version: u64,
    schema: &Schema,
) -> Result<RoaringBitmap> {
    if fragment
        .overlays
        .iter()
        .all(|o| o.committed_version <= index_version)
    {
        return Ok(RoaringBitmap::new());
    }
    overlay_exclusion_offsets(&fragment.overlays, fields, index_version, schema)
}

// A missing `fragment_bitmap` means the index predates fragment-bitmap tracking; treat it as
// covering every fragment (matching `DatasetPreFilter::new`) so overlay-stale rows can't slip
// through unmasked. Only skip fragments explicitly absent from a present bitmap.
fn covers_fragment(coverage: Option<&RoaringBitmap>, frag_id: u32) -> bool {
    coverage.is_none_or(|c| c.contains(frag_id))
}

/// Index by fragment id the fragments that carry at least one overlay. Overlays are rare, so
/// this is empty on the common path, letting callers skip index loading entirely; when non-empty
/// it bounds the stale-collection loops to `O(overlaid fragments)`.
pub fn overlaid_fragments(fragments: &[Fragment]) -> HashMap<u32, &Fragment> {
    fragments
        .iter()
        .filter(|f| !f.overlays.is_empty())
        .map(|f| (f.id as u32, f))
        .collect()
}

/// Insert into `stale` the ids of fragments covered by `segment` whose index entries may be
/// stale because an overlay committed after the segment was built touches a field the segment
/// indexes. Field-aware and version-gated via [`overlay_exclusion_offsets`].
///
/// `overlaid_frags` holds only the fragments that actually carry overlays (rare), so the loop is
/// `O(overlaid_frags)` rather than `O(fragments the segment covers)`.
pub fn collect_overlay_stale_frags(
    segment: &IndexMetadata,
    overlaid_frags: &HashMap<u32, &Fragment>,
    stale: &mut RoaringBitmap,
    schema: &Schema,
) -> Result<()> {
    let coverage = segment.fragment_bitmap.as_ref();
    for (&frag_id, fragment) in overlaid_frags {
        if stale.contains(frag_id) || !covers_fragment(coverage, frag_id) {
            continue;
        }
        if !stale_offsets_for_fragment(fragment, &segment.fields, segment.dataset_version, schema)?
            .is_empty()
        {
            stale.insert(frag_id);
        }
    }
    Ok(())
}

/// Compute exactly which row offsets within each covered fragment are stale and accumulate them
/// into `stale` (fragment_id → stale row offsets).
///
/// Used by the scalar and vector paths to block only the affected rows from index results and
/// re-evaluate only those rows on the flat path, keeping overhead proportional to the number of
/// overlaid rows rather than the whole fragment size.
pub fn collect_overlay_stale_rows_for_segment(
    segment: &IndexMetadata,
    overlaid_frags: &HashMap<u32, &Fragment>,
    stale: &mut HashMap<u32, RoaringBitmap>,
    schema: &Schema,
) -> Result<()> {
    let coverage = segment.fragment_bitmap.as_ref();
    for (&frag_id, fragment) in overlaid_frags {
        if !covers_fragment(coverage, frag_id) {
            continue;
        }
        let excluded =
            stale_offsets_for_fragment(fragment, &segment.fields, segment.dataset_version, schema)?;
        if !excluded.is_empty() {
            *stale.entry(frag_id).or_default() |= &excluded;
        }
    }
    Ok(())
}

/// The plan for merging one field's overlays into one batch: which source (base or
/// a particular overlay) supplies each output row, and which overlay values must be
/// fetched to do it.
///
/// Built by [`route_overlays`] from the coverage bitmaps alone — before any value
/// column is read — so the caller can fetch only the overlay values it will
/// actually use (its `offsets_in_overlay`) rather than whole columns, then build the
/// merged column with [`assemble_overlay_column`].
struct OverlayRouting {
    /// One `(source, position)` pair per output row, ready to hand to `interleave`.
    /// Source `0` is the base column, with `position` = the row's `offset_in_batch`;
    /// source `k + 1` is overlay `k`'s fetched values, with `position` = the row's
    /// index into those fetched values.
    indices: Vec<(usize, usize)>,
    /// Per overlay (newest-first): the sorted, deduplicated `offset_in_overlay`
    /// values this batch needs from that overlay — i.e. exactly which entries of its
    /// value column to fetch.
    offsets_in_overlay: Vec<Vec<u32>>,
    /// Whether any row is covered by an overlay at all (false ⇒ every row falls
    /// through to the base column, so the base is already the answer and no overlay
    /// values need to be read).
    any_overlay: bool,
}

/// For each row in `offsets_in_frag`, decide whether its value comes from the base
/// column or from an overlay — and if from an overlay, at which `offset_in_overlay`.
///
/// Only the coverage bitmaps are consulted (newest-first), so this runs before any
/// value column is read and reports exactly which overlay values the caller must
/// fetch.
///
/// A scan asks for a contiguous, ascending range of offsets, which enables a faster
/// bitmap-driven path ([`route_contiguous`]); `take` asks for arbitrary offsets and
/// uses the general path ([`route_arbitrary`]). Both produce identical routing.
fn route_overlays(
    offsets_in_frag: &[u32],
    coverages_newest_first: &[&RoaringBitmap],
) -> OverlayRouting {
    match contiguous_frag_start(offsets_in_frag) {
        Some(frag_start) => {
            route_contiguous(frag_start, offsets_in_frag.len(), coverages_newest_first)
        }
        None => route_arbitrary(offsets_in_frag, coverages_newest_first),
    }
}

/// If `offsets_in_frag` is a contiguous ascending run `[start, start + 1, ...]`,
/// return `start`; otherwise `None` (including when empty).
fn contiguous_frag_start(offsets_in_frag: &[u32]) -> Option<u32> {
    let start = *offsets_in_frag.first()?;
    offsets_in_frag
        .iter()
        .enumerate()
        .all(|(i, &offset)| offset as u64 == start as u64 + i as u64)
        .then_some(start)
}

/// Fast path for a scan, where the batch is a contiguous run of offsets starting at
/// `frag_start`. Because the offsets are contiguous, a row's `offset_in_batch` is
/// just `offset_in_frag - frag_start`, so a coverage's set bits map straight to
/// output rows — no need to test each row against each coverage.
///
/// For each coverage we intersect it with the batch's offset range. Roaring does
/// this a block at a time, so a coverage that does not overlap the batch (e.g. a
/// scan batch past the last cell this overlay touches) is skipped cheaply without
/// inspecting individual bits.
///
/// Within the batch a coverage's cells appear in ascending order, so their
/// `offset_in_overlay` values are consecutive: the first in-batch cell sits at
/// `offset_in_overlay = <cells this coverage has before the batch>` (a single
/// `rank` lookup), and each following cell is one more. Coverages are applied
/// newest-first, and the first overlay to claim a row wins.
fn route_contiguous(
    frag_start: u32,
    len: usize,
    coverages_newest_first: &[&RoaringBitmap],
) -> OverlayRouting {
    let mut offsets_in_overlay: Vec<Vec<u32>> = vec![Vec::new(); coverages_newest_first.len()];
    // Indexed by offset_in_batch: which (overlay, fetch position) supplies the row.
    let mut routed: Vec<Option<(usize, usize)>> = vec![None; len];
    let range_end = (frag_start as u64 + len as u64).min(u32::MAX as u64) as u32;
    let mut batch_range = RoaringBitmap::new();
    batch_range.insert_range(frag_start..range_end);

    for (k, coverage) in coverages_newest_first.iter().enumerate() {
        let covered_in_batch = *coverage & &batch_range;
        if covered_in_batch.is_empty() {
            continue;
        }
        // offset_in_overlay of this coverage's first in-batch cell = the number of
        // its cells that lie before the batch.
        let first_offset_in_overlay = if frag_start == 0 {
            0
        } else {
            coverage.rank(frag_start - 1) as u32
        };
        for (nth_in_batch, offset_in_frag) in covered_in_batch.iter().enumerate() {
            let offset_in_batch = (offset_in_frag - frag_start) as usize;
            if routed[offset_in_batch].is_none() {
                routed[offset_in_batch] = Some((k, offsets_in_overlay[k].len()));
                offsets_in_overlay[k].push(first_offset_in_overlay + nth_in_batch as u32);
            }
        }
    }

    let mut any_overlay = false;
    let indices = routed
        .into_iter()
        .enumerate()
        .map(|(offset_in_batch, routed)| match routed {
            None => (0, offset_in_batch),
            Some((k, fetch_pos)) => {
                any_overlay = true;
                (k + 1, fetch_pos)
            }
        })
        .collect();

    OverlayRouting {
        indices,
        offsets_in_overlay,
        any_overlay,
    }
}

/// General path for arbitrary offsets (e.g. `take`): test each row's
/// `offset_in_frag` against the coverages newest-first. `take` batches are small,
/// so this `O(rows * overlays)` probing is not a concern.
fn route_arbitrary(
    offsets_in_frag: &[u32],
    coverages_newest_first: &[&RoaringBitmap],
) -> OverlayRouting {
    // Per overlay: the distinct offset_in_overlay values this batch needs, sorted.
    let mut offset_sets: Vec<BTreeSet<u32>> = vec![BTreeSet::new(); coverages_newest_first.len()];
    // Per output row: the (overlay, offset_in_overlay) that supplies it, if any.
    let mut routed_per_row: Vec<Option<(usize, u32)>> = Vec::with_capacity(offsets_in_frag.len());
    for &offset_in_frag in offsets_in_frag {
        let mut routed = None;
        for (k, coverage) in coverages_newest_first.iter().enumerate() {
            if coverage.contains(offset_in_frag) {
                // offset_in_overlay = number of covered cells before this one.
                let offset_in_overlay = coverage.rank(offset_in_frag) as u32 - 1;
                offset_sets[k].insert(offset_in_overlay);
                routed = Some((k, offset_in_overlay));
                break;
            }
        }
        routed_per_row.push(routed);
    }

    let offsets_in_overlay: Vec<Vec<u32>> = offset_sets
        .iter()
        .map(|offsets| offsets.iter().copied().collect())
        .collect();
    // For each overlay, map an offset_in_overlay to its position in the fetched
    // (sorted, deduplicated) value list.
    let fetch_positions: Vec<HashMap<u32, usize>> = offsets_in_overlay
        .iter()
        .map(|offsets| {
            offsets
                .iter()
                .enumerate()
                .map(|(pos, &o)| (o, pos))
                .collect()
        })
        .collect();

    let mut any_overlay = false;
    let indices = routed_per_row
        .into_iter()
        .enumerate()
        .map(|(offset_in_batch, routed)| match routed {
            None => (0, offset_in_batch),
            Some((k, offset_in_overlay)) => {
                any_overlay = true;
                (k + 1, fetch_positions[k][&offset_in_overlay])
            }
        })
        .collect();

    OverlayRouting {
        indices,
        offsets_in_overlay,
        any_overlay,
    }
}

/// Build the merged column from `base` and the overlay values fetched for the
/// `offset_in_overlay` values [`route_overlays`] asked for.
///
/// `fetched_newest_first[k]` holds overlay `k`'s values for `routing`'s
/// `offsets_in_overlay[k]`, in that order. The result has the same length and
/// type as `base`. A covered row whose overlay value is NULL resolves **to** NULL
/// (distinct from a fall-through, which keeps the base value).
fn assemble_overlay_column(
    base: &ArrayRef,
    routing: &OverlayRouting,
    fetched_newest_first: &[ArrayRef],
) -> Result<ArrayRef> {
    if !routing.any_overlay {
        return Ok(base.clone());
    }
    if fetched_newest_first.len() != routing.offsets_in_overlay.len() {
        return Err(Error::invalid_input(format!(
            "overlay assembly got {} value columns but routing expects {}",
            fetched_newest_first.len(),
            routing.offsets_in_overlay.len()
        )));
    }
    for (k, values) in fetched_newest_first.iter().enumerate() {
        if values.len() != routing.offsets_in_overlay[k].len() {
            return Err(Error::invalid_input(format!(
                "overlay value column {} has {} values but {} were requested",
                k,
                values.len(),
                routing.offsets_in_overlay[k].len()
            )));
        }
    }

    let mut sources: Vec<&dyn Array> = Vec::with_capacity(fetched_newest_first.len() + 1);
    sources.push(base.as_ref());
    for values in fetched_newest_first {
        sources.push(values.as_ref());
    }
    interleave(&sources, &routing.indices).map_err(Error::from)
}

/// One overlay's contribution to one projected atomic field, with its file reader opened.
#[derive(Debug, Clone)]
struct LoadedAtomicFieldOverlay {
    /// The `offset_in_frag` cells this overlay covers for the atomic field.
    coverage: Arc<RoaringBitmap>,
    /// Reader over the overlay data file, projected to the covered atomic fields; shared
    /// across the atomic fields that the same file covers.
    reader: Arc<dyn GenericFileReader>,
}

/// The overlays that apply to a single projected atomic field — a per-row field an overlay
/// can replace as a unit (a primitive leaf, or a whole list/map field; structs are
/// recursed through, not treated as atomic fields). Ordered newest-first, with readers opened
/// and pruned to a specific read. Produced by [`resolve_overlays`] and consumed by
/// [`merge_overlay_batch`].
#[derive(Debug, Clone)]
pub struct LoadedAtomicField {
    /// The top-level output column the atomic field lives in (its name locates the batch
    /// column; its field tree drives the descend/splice into that column).
    top_field: Arc<Field>,
    /// Child field ids from `top_field` down to the atomic field (empty when the atomic
    /// field *is* the top-level column). Drives descending to, and splicing back, the
    /// atomic field.
    ancestor_ids: Vec<i32>,
    /// Projection of exactly the atomic field (its ancestor path pruned to the atomic
    /// field subtree), used to fetch the atomic field's values from the overlay file.
    fetch_projection: Arc<Schema>,
    overlays_newest_first: Vec<LoadedAtomicFieldOverlay>,
}

/// One overlay file that may contribute to a read, before it is opened. Opened
/// lazily by [`resolve_overlays`], and only if the read actually touches it.
#[derive(Debug, Clone)]
struct PlannedOverlayFile {
    data_file: DataFile,
    /// The covered ∩ projected atomic fields to project when the file is opened, so a single
    /// reader serves every atomic field the file contributes to.
    open_projection: Arc<Schema>,
}

/// One overlay's contribution to one projected atomic field, before the file is opened.
#[derive(Debug, Clone)]
struct PlannedAtomicFieldOverlay {
    /// Index into [`OverlayReadPlanner::files`] of the file that supplies the value.
    file: usize,
    coverage: Arc<RoaringBitmap>,
}

/// The overlays that apply to a single projected atomic field, ordered newest-first, before
/// any file is opened.
#[derive(Debug, Clone)]
struct PlannedAtomicField {
    top_field: Arc<Field>,
    ancestor_ids: Vec<i32>,
    fetch_projection: Arc<Schema>,
    overlays_newest_first: Vec<PlannedAtomicFieldOverlay>,
}

/// A fragment's overlay-resolution plan for a projection, derived from coverage
/// metadata alone — no file opened, no IO. [`resolve_overlays`] turns it into opened
/// [`LoadedAtomicField`]s for one specific read, opening only the files whose cells
/// the read's rows actually touch.
#[derive(Debug, Clone)]
pub struct OverlayReadPlanner {
    files: Vec<PlannedOverlayFile>,
    atomic_fields: Vec<PlannedAtomicField>,
}

impl OverlayReadPlanner {
    /// True when no projected atomic field has any overlay, so there is nothing to resolve.
    pub fn is_empty(&self) -> bool {
        self.atomic_fields.is_empty()
    }
}

/// Plan `fragment`'s overlay resolution for a projection from coverage metadata
/// alone. No files are opened here (see [`resolve_overlays`]) — this only reads the
/// already-parsed coverage bitmaps, so it is cheap enough to run on every open.
///
/// Overlays are stored oldest-first (sorted newest-last on load, see
/// `sort_overlays_newest_last`), so walking them in reverse gives newest-first
/// precedence.
///
/// Resolution is per *atomic field* — a per-row field that an overlay replaces as a unit: a
/// primitive leaf, or a whole list/map field. Structs are internal nodes, so each
/// leaf of a struct is its own atomic field and can be overlaid independently of its
/// siblings. An overlay is written against the leaf ids it stores (the V2_1
/// structural encoding records only leaves), so an overlay contributes to a projected
/// atomic field when any id in its `data_file.fields` falls in that atomic field's leaf
/// set. At merge time the atomic field's value is fetched and spliced into its output
/// column, so an overlay on a sub-field never disturbs the column's other leaves. Each
/// contributing overlay *file* appears once in `files`, shared by every atomic field it
/// covers.
pub fn plan_overlays(fragment: &FileFragment, projection: &Schema) -> Result<OverlayReadPlanner> {
    let overlays = &fragment.metadata.overlays;
    debug_assert!(
        overlays
            .windows(2)
            .all(|w| w[0].committed_version <= w[1].committed_version),
        "overlays must be sorted newest-last (see sort_overlays_newest_last)"
    );

    // The projection's atomic fields, and a leaf-id -> atomic-field-index map so an
    // overlay's stored leaf ids resolve to the atomic field they belong to in O(1).
    struct AtomicFieldInfo<'a> {
        top_field: &'a Field,
        ancestor_ids: Vec<i32>,
        atomic_field_id: i32,
    }
    let mut atomic_field_infos: Vec<AtomicFieldInfo> = Vec::new();
    let mut leaf_to_atomic_field: HashMap<i32, usize> = HashMap::new();
    for top in &projection.fields {
        for (atomic_field, ancestor_ids) in enumerate_atomic_fields(top) {
            let idx = atomic_field_infos.len();
            let mut value_leaf_ids = Vec::new();
            collect_leaf_ids(atomic_field, &mut value_leaf_ids);
            for leaf in value_leaf_ids {
                leaf_to_atomic_field.insert(leaf, idx);
            }
            atomic_field_infos.push(AtomicFieldInfo {
                top_field: top,
                ancestor_ids,
                atomic_field_id: atomic_field.id,
            });
        }
    }

    // Walk overlays newest-first. For each overlay, find the atomic fields it covers and push
    // (newest-first, for free) into their per-atomic field overlay lists.
    let mut files = Vec::new();
    let mut atomic_field_overlays: Vec<Vec<PlannedAtomicFieldOverlay>> =
        vec![Vec::new(); atomic_field_infos.len()];
    for overlay in overlays.iter().rev() {
        // atomic field index -> the `data_file.fields` position whose coverage to read. An
        // overlay writes one value per row per atomic field, so its leaves share a coverage;
        // the first leaf of each atomic field to appear wins.
        let mut covered: HashMap<usize, usize> = HashMap::new();
        for (field_pos, &field_id) in overlay.data_file.fields.iter().enumerate() {
            if let Some(&atomic_field_idx) = leaf_to_atomic_field.get(&field_id) {
                covered.entry(atomic_field_idx).or_insert(field_pos);
            }
        }
        if covered.is_empty() {
            continue;
        }
        let file = files.len();
        let covered_ids: Vec<i32> = covered
            .keys()
            .map(|&i| atomic_field_infos[i].atomic_field_id)
            .collect();
        files.push(PlannedOverlayFile {
            data_file: overlay.data_file.clone(),
            open_projection: Arc::new(projection.project_by_ids(&covered_ids, true)),
        });
        for (atomic_field_idx, field_pos) in covered {
            atomic_field_overlays[atomic_field_idx].push(PlannedAtomicFieldOverlay {
                file,
                coverage: overlay.coverage_for_field(field_pos)?,
            });
        }
    }

    // Emit one PlannedAtomicField per projected atomic field that has overlays, in
    // atomic field order.
    let mut atomic_fields = Vec::new();
    for (idx, info) in atomic_field_infos.iter().enumerate() {
        let overlays_newest_first = std::mem::take(&mut atomic_field_overlays[idx]);
        if overlays_newest_first.is_empty() {
            continue;
        }
        atomic_fields.push(PlannedAtomicField {
            top_field: Arc::new(info.top_field.clone()),
            ancestor_ids: info.ancestor_ids.clone(),
            fetch_projection: Arc::new(projection.project_by_ids(&[info.atomic_field_id], true)),
            overlays_newest_first,
        });
    }
    Ok(OverlayReadPlanner {
        files,
        atomic_fields,
    })
}

/// The per-row atomic fields of a projected top-level field, each with the child-id path from
/// the top-level field down to it. Structs are recursed through; a primitive leaf or a
/// whole list/map field is an atomic field (values are one-per-row). A top-level primitive or
/// list yields a single atomic field with an empty path.
fn enumerate_atomic_fields(top: &Field) -> Vec<(&Field, Vec<i32>)> {
    fn recurse<'a>(field: &'a Field, path: &mut Vec<i32>, out: &mut Vec<(&'a Field, Vec<i32>)>) {
        if field.logical_type.is_struct() {
            for child in &field.children {
                path.push(child.id);
                recurse(child, path, out);
                path.pop();
            }
        } else {
            out.push((field, path.clone()));
        }
    }
    let mut out = Vec::new();
    let mut path = Vec::new();
    recurse(top, &mut path, &mut out);
    out
}

/// Collect the leaf field ids in `field`'s subtree — the ids an overlay stores for
/// this atomic field (its own id if primitive; its item leaves if a list/map).
fn collect_leaf_ids(field: &Field, out: &mut Vec<i32>) {
    if field.children.is_empty() {
        out.push(field.id);
    } else {
        for child in &field.children {
            collect_leaf_ids(child, out);
        }
    }
}

/// Follow a path of child field ids from `field` down through nested structs, taking
/// the corresponding child array at each step. Returns the array at the end of the
/// path (the whole `array` when `ancestor_ids` is empty).
fn descend_by_ids(array: &ArrayRef, field: &Field, ancestor_ids: &[i32]) -> Result<ArrayRef> {
    let mut arr = array.clone();
    let mut fld = field;
    for &id in ancestor_ids {
        let child_pos = fld
            .children
            .iter()
            .position(|c| c.id == id)
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "overlay descend: field id {id} not found under '{}'",
                    fld.name
                ))
            })?;
        let structs = arr.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
            Error::invalid_input(format!(
                "overlay descend: expected a struct at '{}'",
                fld.name
            ))
        })?;
        arr = structs.column(child_pos).clone();
        fld = &fld.children[child_pos];
    }
    Ok(arr)
}

/// Rebuild `array` with the array at `ancestor_ids` replaced by `new_atomic_field`, cloning
/// the struct spine along the path and preserving each struct's null buffer and other
/// children. With an empty path this is just `new_atomic_field` (whole-column replacement).
fn splice_by_ids(
    array: &ArrayRef,
    field: &Field,
    ancestor_ids: &[i32],
    new_atomic_field: ArrayRef,
) -> Result<ArrayRef> {
    let Some((&id, rest)) = ancestor_ids.split_first() else {
        return Ok(new_atomic_field);
    };
    let child_pos = field
        .children
        .iter()
        .position(|c| c.id == id)
        .ok_or_else(|| {
            Error::invalid_input(format!(
                "overlay splice: field id {id} not found under '{}'",
                field.name
            ))
        })?;
    let structs = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            Error::invalid_input(format!(
                "overlay splice: expected a struct at '{}'",
                field.name
            ))
        })?;
    let len = structs.len();
    let (fields, mut children, nulls) = structs.clone().into_parts();
    children[child_pos] = splice_by_ids(
        &children[child_pos],
        &field.children[child_pos],
        rest,
        new_atomic_field,
    )?;
    Ok(Arc::new(StructArray::try_new_with_length(
        fields, children, nulls, len,
    )?))
}

/// Open the overlay readers a specific read needs and return the per-field plans to
/// merge, pruned to that read.
///
/// `offsets_in_frag` are the rows the read will return. An overlay whose coverage is
/// disjoint from those rows contributes nothing, so it is dropped and its file is
/// never opened — a `take` that misses an overlay's cells pays no IO for it. Each
/// surviving file is opened once, concurrently, projected to the covered fields; the
/// value bytes are still not read here (the per-batch [`merge_overlay_batch`] fetches
/// only the values it needs).
pub async fn resolve_overlays(
    planner: &OverlayReadPlanner,
    offsets_in_frag: &[u32],
    fragment: &FileFragment,
    read_config: &FragReadConfig,
) -> Result<Vec<LoadedAtomicField>> {
    let read_offsets = read_offsets_bitmap(offsets_in_frag);

    // A file is opened only if some atomic field it covers has cells among the requested rows.
    // This is the row-selection pruning: overlays outside the read are skipped.
    let mut file_needed = vec![false; planner.files.len()];
    for atomic_field in &planner.atomic_fields {
        for overlay in &atomic_field.overlays_newest_first {
            if !overlay.coverage.is_disjoint(&read_offsets) {
                file_needed[overlay.file] = true;
            }
        }
    }

    // Open each needed file once, concurrently. The reader is shared (via `Arc`) by
    // every atomic field that file covers.
    //
    // These reads use priority 0 (highest): they are issued only when a ready
    // consumer polls the batch task (see `merge_overlay_batch`), so we have already
    // committed to reading this batch and the overlay reads cannot clog the
    // backpressure queue ahead of work we are not ready for. (A future optimization
    // could start the overlay fetches earlier to fill compute bubbles, which would
    // want a priority tied to the base read.)
    let opened: Vec<Option<Arc<dyn GenericFileReader>>> =
        futures::future::try_join_all(planner.files.iter().enumerate().map(|(i, file)| {
            let needed = file_needed[i];
            async move {
                if !needed {
                    return Ok::<_, Error>(None);
                }
                Ok(fragment
                    .open_reader(&file.data_file, Some(&file.open_projection), read_config)
                    .await?
                    .map(Arc::from))
            }
        }))
        .await?;

    let mut plans = Vec::new();
    for atomic_field in &planner.atomic_fields {
        let mut overlays_newest_first = Vec::new();
        for overlay in &atomic_field.overlays_newest_first {
            let Some(reader) = &opened[overlay.file] else {
                continue; // pruned: coverage disjoint from the read
            };
            overlays_newest_first.push(LoadedAtomicFieldOverlay {
                coverage: overlay.coverage.clone(),
                reader: reader.clone(),
            });
        }
        if !overlays_newest_first.is_empty() {
            plans.push(LoadedAtomicField {
                top_field: atomic_field.top_field.clone(),
                ancestor_ids: atomic_field.ancestor_ids.clone(),
                fetch_projection: atomic_field.fetch_projection.clone(),
                overlays_newest_first,
            });
        }
    }
    Ok(plans)
}

/// The set of `offset_in_frag` a read will return, as a bitmap for cheap
/// intersection against overlay coverages. Contiguous scans build a single range;
/// arbitrary `take` offsets (small batches) are inserted individually.
fn read_offsets_bitmap(offsets_in_frag: &[u32]) -> RoaringBitmap {
    let mut bitmap = RoaringBitmap::new();
    match contiguous_frag_start(offsets_in_frag) {
        Some(start) => {
            let end = (start as u64 + offsets_in_frag.len() as u64).min(u32::MAX as u64) as u32;
            bitmap.insert_range(start..end);
        }
        None => bitmap.extend(offsets_in_frag.iter().copied()),
    }
    bitmap
}

/// Resolve overlays for one base batch: route each projected atomic field against the batch's
/// `offsets_in_frag`, fetch only the overlay values the batch needs (concurrently with
/// the base read), assemble the merged atomic field, and splice it into its output column.
/// AtomicFields with no covered rows, and columns with no plan, pass through.
pub async fn merge_overlay_batch(
    base: ReadBatchFut,
    offsets_in_frag: &[u32],
    plans: &[LoadedAtomicField],
) -> Result<RecordBatch> {
    let atomic_field_work = futures::future::try_join_all(plans.iter().map(|plan| async move {
        let coverages: Vec<&RoaringBitmap> = plan
            .overlays_newest_first
            .iter()
            .map(|overlay| overlay.coverage.as_ref())
            .collect();
        let routing = route_overlays(offsets_in_frag, &coverages);
        if !routing.any_overlay {
            return Ok::<_, Error>((plan, None));
        }
        // Fetch each overlay's values and descend to the atomic field array. The fetch is
        // projected to the atomic field's ancestor path, so the fetched column is the pruned
        // top-level column; `descend_by_ids` walks it down to the atomic field.
        let atomic_field = &plan.fetch_projection.fields[0];
        let fetched = futures::future::try_join_all(
            plan.overlays_newest_first
                .iter()
                .zip(&routing.offsets_in_overlay)
                .map(|(overlay, offsets_in_overlay)| async move {
                    let column = fetch_overlay_values(
                        overlay.reader.as_ref(),
                        plan.fetch_projection.clone(),
                        offsets_in_overlay,
                    )
                    .await?;
                    descend_by_ids(&column, atomic_field, &plan.ancestor_ids)
                }),
        )
        .await?;
        Ok((plan, Some((routing, fetched))))
    }));

    // The base read and every overlay value read proceed concurrently.
    let (batch, resolved) = futures::future::try_join(base, atomic_field_work).await?;

    let schema = batch.schema();
    let mut columns = batch.columns().to_vec();
    for (plan, work) in resolved {
        let Some((routing, fetched)) = work else {
            continue;
        };
        let Some(idx) = schema.index_of(&plan.top_field.name).ok() else {
            // The plan's column is not in this batch's projection; skip it.
            continue;
        };
        let base_atomic_field = descend_by_ids(&columns[idx], &plan.top_field, &plan.ancestor_ids)?;
        let merged_atomic_field = assemble_overlay_column(&base_atomic_field, &routing, &fetched)?;
        columns[idx] = splice_by_ids(
            &columns[idx],
            &plan.top_field,
            &plan.ancestor_ids,
            merged_atomic_field,
        )?;
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Fetch one overlay's values at the given `offsets_in_overlay` (sorted, unique):
/// the corresponding entries of its value column, as the top-level column pruned to
/// `projection`. Returns `offsets_in_overlay.len()` rows in the same order; empty
/// input reads nothing and returns an empty column.
async fn fetch_overlay_values(
    reader: &dyn GenericFileReader,
    projection: Arc<Schema>,
    offsets_in_overlay: &[u32],
) -> Result<ArrayRef> {
    if offsets_in_overlay.is_empty() {
        return Ok(arrow_array::new_empty_array(
            &projection.fields[0].data_type(),
        ));
    }
    let mut tasks = reader
        .take_all_tasks(
            offsets_in_overlay,
            offsets_in_overlay.len() as u32,
            projection,
            None,
        )
        .await?;
    let mut chunks: Vec<ArrayRef> = Vec::new();
    while let Some(task) = tasks.next().await {
        let batch = task.task.await?;
        chunks.push(batch.column(0).clone());
    }
    let chunk_refs: Vec<&dyn arrow_array::Array> = chunks.iter().map(|a| a.as_ref()).collect();
    Ok(arrow_select::concat::concat(&chunk_refs)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray, UInt32Array};
    use lance_table::format::overlay::OverlayCoverage;
    use std::sync::Arc;

    fn i32_array(values: impl IntoIterator<Item = Option<i32>>) -> ArrayRef {
        Arc::new(Int32Array::from_iter(values))
    }

    fn bitmap(offsets: impl IntoIterator<Item = u32>) -> RoaringBitmap {
        RoaringBitmap::from_iter(offsets)
    }

    fn flat_test_schema() -> Schema {
        use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};

        let mut schema = Schema::try_from(&ArrowSchema::new(
            (0..5)
                .map(|id| ArrowField::new(format!("field_{id}"), DataType::Int32, true))
                .collect::<Vec<_>>(),
        ))
        .unwrap();
        schema.set_field_id(None);
        schema
    }

    /// Physical offsets for a contiguous range `[start, start + len)`.
    fn offsets(start: u32, len: usize) -> Vec<u32> {
        (start..start + len as u32).collect()
    }

    /// Drive the production flow purely in memory: route against the coverage
    /// bitmaps, then fetch just the requested `offset_in_overlay` entries from each
    /// overlay's *full* value column (exactly what the value-pushdown `take` does on
    /// disk), then assemble. `overlays_newest_first` holds each overlay's
    /// `(coverage, full value column indexed by offset_in_overlay)`.
    fn resolve(
        base: &ArrayRef,
        offsets: &[u32],
        overlays_newest_first: &[(RoaringBitmap, ArrayRef)],
    ) -> ArrayRef {
        let coverages: Vec<&RoaringBitmap> = overlays_newest_first.iter().map(|(c, _)| c).collect();
        let routing = route_overlays(offsets, &coverages);
        let fetched: Vec<ArrayRef> = overlays_newest_first
            .iter()
            .zip(&routing.offsets_in_overlay)
            .map(|((_, full), offsets_in_overlay)| {
                let indices = UInt32Array::from(offsets_in_overlay.clone());
                arrow_select::take::take(full.as_ref(), &indices, None).unwrap()
            })
            .collect();
        assemble_overlay_column(base, &routing, &fetched).unwrap()
    }

    fn assert_i32_eq(actual: &ArrayRef, expected: impl IntoIterator<Item = Option<i32>>) {
        let actual = actual.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(actual, &Int32Array::from_iter(expected));
    }

    #[test]
    fn test_no_overlays_returns_base() {
        let base = i32_array([Some(1), Some(2), Some(3)]);
        let resolved = resolve(&base, &offsets(0, 3), &[]);
        assert_i32_eq(&resolved, [Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn test_single_overlay_value_offset() {
        // Base ages [30, 25, 40, 22]; overlay sets offset_in_frag 1 -> 26, whose
        // value sits at offset_in_overlay 0.
        let base = i32_array([Some(30), Some(25), Some(40), Some(22)]);
        let overlay = (bitmap([1]), i32_array([Some(26)]));
        let resolved = resolve(&base, &offsets(0, 4), &[overlay]);
        assert_i32_eq(&resolved, [Some(30), Some(26), Some(40), Some(22)]);
    }

    #[test]
    fn test_value_offsets_multiple_cells() {
        // Coverage {0, 2, 3} -> values at offset_in_overlay 0, 1, 2.
        let base = i32_array([Some(10), Some(11), Some(12), Some(13)]);
        let overlay = (
            bitmap([0, 2, 3]),
            i32_array([Some(100), Some(120), Some(130)]),
        );
        let resolved = resolve(&base, &offsets(0, 4), &[overlay]);
        assert_i32_eq(&resolved, [Some(100), Some(11), Some(120), Some(130)]);
    }

    #[test]
    fn test_newest_overlay_wins() {
        // Two overlays both cover offset_in_frag 1; the newest (first in the slice)
        // wins.
        let base = i32_array([Some(0), Some(1), Some(2)]);
        let newest = (bitmap([1]), i32_array([Some(999)]));
        let older = (bitmap([1, 2]), i32_array([Some(111), Some(222)]));
        let resolved = resolve(&base, &offsets(0, 3), &[newest, older]);
        // offset 1 -> newest (999); offset 2 -> only older covers it (222).
        assert_i32_eq(&resolved, [Some(0), Some(999), Some(222)]);
    }

    #[test]
    fn test_null_override_vs_fall_through() {
        // A covered offset with a NULL value overrides the cell to NULL; an
        // absent offset falls through to the base.
        let base = i32_array([Some(1), Some(2), Some(3)]);
        let overlay = (bitmap([0]), i32_array([None]));
        let resolved = resolve(&base, &offsets(0, 3), &[overlay]);
        assert_i32_eq(&resolved, [None, Some(2), Some(3)]);
    }

    #[test]
    fn test_physical_start_offset() {
        // The batch covers physical rows [10, 13); the overlay covers offset 11.
        let base = i32_array([Some(0), Some(0), Some(0)]);
        let overlay = (bitmap([11]), i32_array([Some(7)]));
        let resolved = resolve(&base, &offsets(10, 3), &[overlay]);
        assert_i32_eq(&resolved, [Some(0), Some(7), Some(0)]);
    }

    #[test]
    fn test_string_column_merge() {
        let base: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let overlay = (
            bitmap([0, 2]),
            Arc::new(StringArray::from(vec!["A", "C"])) as ArrayRef,
        );
        let resolved = resolve(&base, &offsets(0, 3), &[overlay]);
        let expected: ArrayRef = Arc::new(StringArray::from(vec!["A", "b", "C"]));
        assert_eq!(&resolved, &expected);
    }

    #[test]
    fn test_non_contiguous_offsets() {
        // `take` supplies arbitrary, non-contiguous offsets_in_frag. The base rows
        // correspond to offsets 5, 1, 8 (in that order); the overlay covers offsets
        // {1, 8}, whose values sit at offset_in_overlay 0, 1.
        let base = i32_array([Some(50), Some(10), Some(80)]);
        let overlay = (bitmap([1, 8]), i32_array([Some(11), Some(88)]));
        let resolved = resolve(&base, &[5, 1, 8], &[overlay]);
        // offset 5 uncovered -> base 50; offset 1 -> offset_in_overlay 0 (11);
        // offset 8 -> offset_in_overlay 1 (88).
        assert_i32_eq(&resolved, [Some(50), Some(11), Some(88)]);
    }

    #[test]
    fn test_routing_dedups_repeated_offsets() {
        // A `take` may request the same offset twice; both rows must route to the
        // same overlay value, and that value is fetched only once.
        let coverage = bitmap([2, 5]);
        let routing = route_overlays(&[5, 2, 5], &[&coverage]);
        // offset_in_frag 5 is offset_in_overlay 1, offset_in_frag 2 is
        // offset_in_overlay 0: distinct values {0, 1}, sorted.
        assert_eq!(routing.offsets_in_overlay, vec![vec![0, 1]]);
        let full = i32_array([Some(20), Some(50)]); // values at offset_in_overlay 0, 1
        let fetched = vec![
            arrow_select::take::take(
                full.as_ref(),
                &UInt32Array::from(routing.offsets_in_overlay[0].clone()),
                None,
            )
            .unwrap(),
        ];
        let base = i32_array([Some(0), Some(0), Some(0)]);
        let resolved = assemble_overlay_column(&base, &routing, &fetched).unwrap();
        assert_i32_eq(&resolved, [Some(50), Some(20), Some(50)]);
    }

    #[test]
    fn test_assemble_value_count_mismatch_errors() {
        let coverage = bitmap([0, 1]);
        let routing = route_overlays(&[0, 1], &[&coverage]);
        let base = i32_array([Some(1), Some(2)]);
        // One value supplied for two requested offsets is a caller bug.
        let fetched = vec![i32_array([Some(9)])];
        assert!(assemble_overlay_column(&base, &routing, &fetched).is_err());
    }

    #[test]
    fn test_contiguous_fast_path_matches_general() {
        // The contiguous fast path must produce byte-for-byte identical routing to
        // the general offset-major path for any contiguous batch. Fuzz a range of
        // fragment starts, lengths, overlay counts, and coverage densities —
        // including bits outside the batch range — and compare both paths.
        let mut state = 0x9e3779b97f4a7c15u64;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as u32
        };
        for _ in 0..500 {
            let frag_start = next() % 64;
            let len = (next() % 48 + 1) as usize;
            let num_overlays = (next() % 5) as usize;
            let coverages: Vec<RoaringBitmap> = (0..num_overlays)
                .map(|_| {
                    let density = next() % 101;
                    let mut b = RoaringBitmap::new();
                    for off in frag_start.saturating_sub(3)..frag_start + len as u32 + 3 {
                        if next() % 100 < density {
                            b.insert(off);
                        }
                    }
                    b
                })
                .collect();
            let refs: Vec<&RoaringBitmap> = coverages.iter().collect();
            let contiguous_offsets: Vec<u32> = (frag_start..frag_start + len as u32).collect();

            let fast = route_contiguous(frag_start, len, &refs);
            let general = route_arbitrary(&contiguous_offsets, &refs);
            assert_eq!(fast.indices, general.indices, "indices differ");
            assert_eq!(
                fast.offsets_in_overlay, general.offsets_in_overlay,
                "offsets_in_overlay differ"
            );
            assert_eq!(fast.any_overlay, general.any_overlay, "any_overlay differs");
        }
    }

    /// `outer { middle { a, b } }` for exercising the descend/splice helpers.
    fn nested_struct() -> (Schema, ArrayRef) {
        use arrow_schema::{DataType, Field as ArrowField, Fields, Schema as ArrowSchema};
        let mid = Fields::from(vec![
            ArrowField::new("a", DataType::Int32, true),
            ArrowField::new("b", DataType::Int32, true),
        ]);
        let outer_fields =
            Fields::from(vec![ArrowField::new("middle", DataType::Struct(mid), true)]);
        let arrow_schema = ArrowSchema::new(vec![ArrowField::new(
            "outer",
            DataType::Struct(outer_fields),
            true,
        )]);
        let mut schema = Schema::try_from(&arrow_schema).unwrap();
        schema.set_field_id(None);
        let middle = StructArray::from(vec![
            (
                Arc::new(ArrowField::new("a", DataType::Int32, true)),
                i32_array([Some(1), Some(2), Some(3)]),
            ),
            (
                Arc::new(ArrowField::new("b", DataType::Int32, true)),
                i32_array([Some(10), Some(20), Some(30)]),
            ),
        ]);
        let outer: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(ArrowField::new("middle", middle.data_type().clone(), true)),
            Arc::new(middle) as ArrayRef,
        )]));
        (schema, outer)
    }

    #[test]
    fn test_descend_and_splice_roundtrip() {
        let (schema, outer_arr) = nested_struct();
        let outer_field = &schema.fields[0];
        let middle_id = outer_field.children[0].id;
        let a_id = outer_field.children[0].children[0].id;
        let path = [middle_id, a_id];

        // Descend to the deep leaf `outer.middle.a`.
        let a = descend_by_ids(&outer_arr, outer_field, &path).unwrap();
        assert_i32_eq(&a, [Some(1), Some(2), Some(3)]);

        // Splice a replacement in; only `a` changes, `b` is preserved.
        let spliced = splice_by_ids(
            &outer_arr,
            outer_field,
            &path,
            i32_array([Some(7), Some(8), Some(9)]),
        )
        .unwrap();
        let middle = spliced
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone();
        assert_i32_eq(&middle.column(0).clone(), [Some(7), Some(8), Some(9)]);
        assert_i32_eq(&middle.column(1).clone(), [Some(10), Some(20), Some(30)]);
    }

    #[test]
    fn test_splice_preserves_struct_nulls() {
        use arrow_buffer::NullBuffer;
        let (schema, base) = nested_struct();
        let outer_field = &schema.fields[0];
        // Rebuild `outer` with a null at row 1 (a null struct value).
        let base = base.as_any().downcast_ref::<StructArray>().unwrap();
        let (fields, children, _) = base.clone().into_parts();
        let outer_arr: ArrayRef = Arc::new(
            StructArray::try_new(
                fields,
                children,
                Some(NullBuffer::from(vec![true, false, true])),
            )
            .unwrap(),
        );
        let path = [
            outer_field.children[0].id,
            outer_field.children[0].children[0].id,
        ];
        let spliced = splice_by_ids(
            &outer_arr,
            outer_field,
            &path,
            i32_array([Some(7), Some(8), Some(9)]),
        )
        .unwrap();
        let spliced = spliced.as_any().downcast_ref::<StructArray>().unwrap();
        // The outer struct's null buffer survives the splice.
        assert!(!spliced.is_null(0));
        assert!(spliced.is_null(1));
        assert!(!spliced.is_null(2));
    }

    /// A dense overlay covering `offsets` for `field_ids`, committed at `version`.
    fn dense_overlay(
        field_ids: Vec<i32>,
        offsets: impl IntoIterator<Item = u32>,
        version: u64,
    ) -> lance_table::format::overlay::DataOverlayFile {
        DataOverlayFile {
            data_file: DataFile::new_legacy_from_fields("o.lance", field_ids, None),
            coverage: OverlayCoverage::dense(bitmap(offsets)),
            committed_version: version,
        }
    }

    #[test]
    fn test_exclusion_offsets_version_gate() {
        let schema = flat_test_schema();
        // index built at version 5; only overlays committed > 5 are excluded.
        let overlays = vec![
            dense_overlay(vec![3], [0, 1], 4),
            dense_overlay(vec![3], [2, 7], 6),
        ];
        let excluded = overlay_exclusion_offsets(&overlays, &[3], 5, &schema).unwrap();
        assert_eq!(excluded, bitmap([2, 7]));
        // An overlay exactly at the index version is already incorporated.
        let overlays = vec![dense_overlay(vec![3], [9], 5)];
        assert!(
            overlay_exclusion_offsets(&overlays, &[3], 5, &schema)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn test_exclusion_offsets_is_field_aware() {
        let schema = flat_test_schema();
        // An overlay touching only an unrelated field excludes nothing.
        let overlays = vec![dense_overlay(vec![2], [0, 1, 2], 9)];
        assert!(
            overlay_exclusion_offsets(&overlays, &[3], 1, &schema)
                .unwrap()
                .is_empty()
        );
        // The union spans only the indexed fields the overlay actually carries.
        let overlays = vec![dense_overlay(vec![2, 3], [4], 9)];
        assert_eq!(
            overlay_exclusion_offsets(&overlays, &[3], 1, &schema).unwrap(),
            bitmap([4])
        );
    }

    #[test]
    fn test_exclusion_offsets_matches_nested_fields() {
        let (schema, _) = nested_struct();
        let outer = &schema.fields[0];
        let middle = &outer.children[0];
        let a = &middle.children[0];
        let b = &middle.children[1];

        let overlays = vec![dense_overlay(vec![a.id], [1], 9)];
        assert_eq!(
            overlay_exclusion_offsets(&overlays, &[outer.id], 1, &schema).unwrap(),
            bitmap([1])
        );
        assert!(
            overlay_exclusion_offsets(&overlays, &[b.id], 1, &schema)
                .unwrap()
                .is_empty()
        );

        let overlays = vec![dense_overlay(vec![middle.id], [2], 9)];
        assert_eq!(
            overlay_exclusion_offsets(&overlays, &[a.id], 1, &schema).unwrap(),
            bitmap([2])
        );
    }

    #[test]
    fn test_exclusion_offsets_sparse_per_field() {
        let schema = flat_test_schema();
        // Sparse overlay: field 2 covers {2,3}, field 4 covers {1}.
        let overlay = DataOverlayFile {
            data_file: DataFile::new_legacy_from_fields("o.lance", vec![2, 4], None),
            coverage: OverlayCoverage::sparse(vec![bitmap([2, 3]), bitmap([1])]),
            committed_version: 9,
        };
        let overlays = vec![overlay];
        // Only the bitmap for the indexed field (4) contributes.
        assert_eq!(
            overlay_exclusion_offsets(&overlays, &[4], 1, &schema).unwrap(),
            bitmap([1])
        );
        assert_eq!(
            overlay_exclusion_offsets(&overlays, &[2], 1, &schema).unwrap(),
            bitmap([2, 3])
        );
    }

    #[test]
    fn test_exclusion_offsets_unions_multiple_overlays() {
        let schema = flat_test_schema();
        let overlays = vec![
            dense_overlay(vec![3], [1], 6),
            dense_overlay(vec![3], [4, 5], 7),
        ];
        assert_eq!(
            overlay_exclusion_offsets(&overlays, &[3], 1, &schema).unwrap(),
            bitmap([1, 4, 5])
        );
    }

    /// An index segment covering `fields`, built at `dataset_version`, with the given
    /// fragment coverage (`None` = legacy index predating fragment-bitmap tracking).
    fn segment(
        fields: Vec<i32>,
        dataset_version: u64,
        fragment_bitmap: Option<RoaringBitmap>,
    ) -> IndexMetadata {
        IndexMetadata {
            uuid: uuid::Uuid::new_v4(),
            name: "idx".into(),
            fields,
            dataset_version,
            fragment_bitmap,
            index_details: None,
            index_version: 0,
            created_at: None,
            base_id: None,
            files: None,
        }
    }

    fn fragment_with_overlay(id: u64, overlay: DataOverlayFile) -> Fragment {
        let mut fragment = Fragment::new(id);
        fragment.overlays.push(overlay);
        fragment
    }

    #[test]
    fn test_collect_frags_missing_bitmap_covers_all() {
        let schema = flat_test_schema();
        let fragment = fragment_with_overlay(3, dense_overlay(vec![3], [1, 2], 9));
        let overlaid: HashMap<u32, &Fragment> = HashMap::from([(3u32, &fragment)]);

        let mut stale = RoaringBitmap::new();
        collect_overlay_stale_frags(&segment(vec![3], 1, None), &overlaid, &mut stale, &schema)
            .unwrap();
        assert_eq!(stale, bitmap([3]), "missing bitmap must cover fragment 3");

        let mut stale = RoaringBitmap::new();
        collect_overlay_stale_frags(
            &segment(vec![3], 1, Some(bitmap([0]))),
            &overlaid,
            &mut stale,
            &schema,
        )
        .unwrap();
        assert!(
            stale.is_empty(),
            "fragment absent from bitmap is not covered"
        );

        let mut stale = RoaringBitmap::new();
        collect_overlay_stale_frags(
            &segment(vec![3], 1, Some(bitmap([3]))),
            &overlaid,
            &mut stale,
            &schema,
        )
        .unwrap();
        assert_eq!(stale, bitmap([3]));
    }

    #[test]
    fn test_collect_rows_missing_bitmap_covers_all() {
        let schema = flat_test_schema();
        // Same covers-all guarantee at row-level granularity.
        let fragment = fragment_with_overlay(3, dense_overlay(vec![3], [1, 2], 9));
        let overlaid: HashMap<u32, &Fragment> = HashMap::from([(3u32, &fragment)]);

        let mut stale = HashMap::new();
        collect_overlay_stale_rows_for_segment(
            &segment(vec![3], 1, None),
            &overlaid,
            &mut stale,
            &schema,
        )
        .unwrap();
        assert_eq!(
            stale.get(&3),
            Some(&bitmap([1, 2])),
            "missing bitmap must cover fragment 3"
        );

        // A present bitmap that excludes fragment 3 yields no stale rows.
        let mut stale = HashMap::new();
        collect_overlay_stale_rows_for_segment(
            &segment(vec![3], 1, Some(bitmap([0]))),
            &overlaid,
            &mut stale,
            &schema,
        )
        .unwrap();
        assert!(
            stale.is_empty(),
            "fragment absent from bitmap contributes no rows"
        );

        // A present bitmap that includes fragment 3 contributes its stale rows.
        let mut stale = HashMap::new();
        collect_overlay_stale_rows_for_segment(
            &segment(vec![3], 1, Some(bitmap([3]))),
            &overlaid,
            &mut stale,
            &schema,
        )
        .unwrap();
        assert_eq!(stale.get(&3), Some(&bitmap([1, 2])));
    }
}
