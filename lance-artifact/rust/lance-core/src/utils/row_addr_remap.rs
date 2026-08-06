// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Compact row-address remapping for compaction.
//!
//! Compaction rewrites rows into new fragments, so indices that store physical
//! row addresses need an old-address to new-address mapping without building an
//! O(total rows) `HashMap<u64, Option<u64>>`.
//!
//! Layout:
//!
//! * Old rows: `old_fragment_id -> (old_offsets, old_rows_before)`
//!     * `old_offsets`: rewritten old row offsets in this old fragment.
//!     * `old_rows_before`: rewritten row count before this old fragment.
//! * New rows: ordered new-fragment ranges
//!   `(fragment_id, new_rows_before, physical_rows)`
//!     * `new_rows_before`: rewritten row count before this new fragment.
//!
//! Lookup:
//!
//! * An address whose fragment was not rewritten returns `None`.
//! * For an address whose fragment was rewritten:
//!     * Read `(old_offsets, old_rows_before)` from the old-row layout.
//!     * If `offset` is not in `old_offsets`, return `Some(None)` because the
//!       row was deleted.
//!     * Otherwise, `old_offsets.rank(offset) - 1` is this row's 0-based
//!       position among rewritten old rows in this old fragment. Add
//!       `old_rows_before` to get `k`, the row's 0-based position among all
//!       rewritten old rows.
//!     * In the new-row layout, find the range
//!       `(fragment_id, new_rows_before, physical_rows)` where
//!       `new_rows_before <= k < new_rows_before + physical_rows`.
//!     * The new address is `(fragment_id, k - new_rows_before)`.
//!
//! Ordering:
//!
//! Compact remap does not store each old-to-new row mapping. It computes `k`
//! from the old-row layout, then maps it to the k-th row written to the new
//! fragments. This requires the reader-to-writer pipeline to preserve row order.
//!
//! * `old_frag_ids` must match the order old fragments are read. Within each
//!   old fragment, rewritten rows are interpreted by ascending old row offset.
//! * `new_frags` must match the order new rows are written.
//! * Current compaction satisfies this because it scans selected fragments in
//!   order and writes the resulting stream without reordering rows.

use crate::utils::address::RowAddress;
use crate::{Error, Result};
use roaring::{RoaringBitmap, RoaringTreemap};
use std::collections::HashMap;

/// A queryable row-address remapping with the exact semantics of
/// `HashMap<u64, Option<u64>>::get(&addr).copied()`:
///
/// * `None` — the address is not affected by this remap (keep it unchanged)
/// * `Some(None)` — the row was deleted
/// * `Some(Some(addr))` — the row moved to `addr`
#[derive(Clone)]
pub enum RowAddrRemap {
    /// Compact, `O(#fragments)` remap built from per-group rewritten-row
    /// bitmaps and new-fragment layouts.
    Compact(CompactRowAddrRemap),
    /// Full materialized old-to-new address map. Uses `O(#rows)` memory.
    Direct(HashMap<u64, Option<u64>>),
}

impl RowAddrRemap {
    pub fn compact(groups: impl IntoIterator<Item = GroupInput>) -> Result<Self> {
        Ok(Self::Compact(CompactRowAddrRemap::new(groups)?))
    }

    /// Build a remap from a fully materialized old-to-new address map.
    pub fn direct(map: HashMap<u64, Option<u64>>) -> Self {
        Self::Direct(map)
    }

    /// An empty remap that leaves every address unchanged.
    pub fn empty() -> Self {
        Self::Direct(HashMap::new())
    }

    /// Look up `addr`. See [`RowAddrRemap`] for the tri-state return semantics.
    #[inline]
    pub fn get(&self, addr: u64) -> Option<Option<u64>> {
        match self {
            Self::Compact(c) => c.get(addr),
            Self::Direct(m) => m.get(&addr).copied(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Compact(c) => c.is_empty(),
            Self::Direct(m) => m.is_empty(),
        }
    }

    pub fn affected_fragments(&self) -> RoaringBitmap {
        match self {
            Self::Compact(c) => RoaringBitmap::from_iter(c.frag_to_group.keys().copied()),
            Self::Direct(m) => RoaringBitmap::from_iter(m.keys().map(|addr| (addr >> 32) as u32)),
        }
    }

    pub fn fully_deleted_fragments(&self) -> Option<RoaringBitmap> {
        match self {
            Self::Compact(c) => c.fully_deleted_fragments(),
            Self::Direct(m) => {
                if m.values().all(|v| v.is_none()) {
                    Some(RoaringBitmap::from_iter(
                        m.keys().map(|addr| (addr >> 32) as u32),
                    ))
                } else {
                    None
                }
            }
        }
    }
}

/// Input describing one rewrite group: the old row addresses that were
/// rewritten plus the fragment layout before/after the rewrite.
pub struct GroupInput {
    /// Old row addresses that were read and re-written into the new fragments.
    pub rewritten_old_row_addrs: RoaringTreemap,
    /// Old fragment ids covered by this group.
    pub old_frag_ids: Vec<u32>,
    /// New fragments produced by this group, as `(fragment_id, physical_rows)`,
    pub new_frags: Vec<(u32, u32)>,
}

#[derive(Clone)]
struct GroupRemap {
    /// Old fragment id -> (rewritten old row offsets in that fragment,
    /// rewritten row count before this fragment in the group).
    frags: HashMap<u32, (RoaringBitmap, u64)>,
    /// New fragment ranges as `(fragment_id, rewritten_rows_before, physical_rows)`,
    /// used to map a rewritten row's group-local index to its new address via binary search.
    new_frag_row_ranges: Vec<(u32, u64, u32)>,
}

impl GroupRemap {
    fn new(input: GroupInput) -> Result<Self> {
        // `compute_new_addr` maps a rewritten row's group-local index to a new
        // address by accumulating `physical_rows` in `new_frags` order, so that
        // order must be the order rows were written. New fragment ids are
        // reserved monotonically in write order (see `reserve_fragment_ids` in
        // compaction), so ascending id is a proxy for write order; reject any
        // input that violates it before it can silently misplace addresses.
        let mut new_frag_row_ranges = Vec::with_capacity(input.new_frags.len());
        let mut rewritten_rows_before = 0u64;
        let mut prev_frag_id: Option<u32> = None;
        for (frag_id, physical_rows) in input.new_frags {
            if physical_rows == 0 {
                continue;
            }
            if let Some(prev) = prev_frag_id
                && frag_id <= prev
            {
                return Err(Error::invalid_input(format!(
                    "compaction new fragments must be in ascending id (write) order, but fragment {frag_id} follows {prev}",
                )));
            }
            prev_frag_id = Some(frag_id);
            new_frag_row_ranges.push((frag_id, rewritten_rows_before, physical_rows));
            rewritten_rows_before += physical_rows as u64;
        }
        let total_new_rows = rewritten_rows_before;

        let mut per_frag: HashMap<u32, RoaringBitmap> = input
            .rewritten_old_row_addrs
            .bitmaps()
            .map(|(frag_id, bitmap)| (frag_id, bitmap.clone()))
            .collect();
        let mut frags = HashMap::new();
        let mut rewritten_rows_before = 0u64;
        for &frag_id in &input.old_frag_ids {
            // A fragment with no rewritten rows (fully deleted) contributes
            // nothing to the rewritten row sequence.
            if let Some(bitmap) = per_frag.remove(&frag_id) {
                let num_rewritten_rows = bitmap.len();
                frags.insert(frag_id, (bitmap, rewritten_rows_before));
                rewritten_rows_before += num_rewritten_rows;
            }
        }
        // Rewritten old row addresses must reference only fragments listed in `old_frag_ids`.
        if !per_frag.is_empty() {
            return Err(Error::invalid_input(format!(
                "compaction rewritten old row addresses reference fragments {:?} not in the rewrite group's old fragments {:?}",
                per_frag.keys().collect::<Vec<_>>(),
                input.old_frag_ids,
            )));
        }

        // Rewritten old rows are mapped positionally onto the new rows, so the
        // two counts must match exactly
        let total_rewritten_old_rows = input.rewritten_old_row_addrs.len();
        if total_new_rows != total_rewritten_old_rows {
            return Err(Error::invalid_input(format!(
                "compaction rewrote {total_rewritten_old_rows} old rows from fragments {:?} but the new fragments hold {total_new_rows} rows",
                input.old_frag_ids,
            )));
        }

        Ok(Self {
            frags,
            new_frag_row_ranges,
        })
    }

    fn compute_new_addr(&self, rewritten_row_index: u64) -> u64 {
        let idx =
            match self
                .new_frag_row_ranges
                .binary_search_by(|(_, rewritten_rows_before, _)| {
                    rewritten_rows_before.cmp(&rewritten_row_index)
                }) {
                Ok(i) => i,
                Err(i) => i - 1,
            };
        let (frag_id, rewritten_rows_before, _rows) = self.new_frag_row_ranges[idx];
        let offset = (rewritten_row_index - rewritten_rows_before) as u32;
        u64::from(RowAddress::new_from_parts(frag_id, offset))
    }

    /// Compute the new address for an old row in this group.
    /// Returns `None` if the old row was not rewritten.
    #[inline]
    fn get(&self, frag: u32, offset: u32) -> Option<u64> {
        match self.frags.get(&frag) {
            Some((bitmap, rewritten_rows_before)) if bitmap.contains(offset) => {
                let rewritten_row_index = rewritten_rows_before + bitmap.rank(offset) - 1;
                Some(self.compute_new_addr(rewritten_row_index))
            }
            _ => None,
        }
    }
}

/// Compact remap backed by per-group rewritten row bitmaps + new-fragment layouts.
#[derive(Clone)]
pub struct CompactRowAddrRemap {
    groups: Vec<GroupRemap>,
    /// Old fragment id -> index into `groups`. Size is O(#fragments), not rows.
    frag_to_group: HashMap<u32, usize>,
}

impl CompactRowAddrRemap {
    fn new(groups: impl IntoIterator<Item = GroupInput>) -> Result<Self> {
        let mut frag_to_group = HashMap::new();
        let mut group_remaps = Vec::new();
        for input in groups {
            let gi = group_remaps.len();
            for &frag_id in &input.old_frag_ids {
                frag_to_group.insert(frag_id, gi);
            }
            group_remaps.push(GroupRemap::new(input)?);
        }
        Ok(Self {
            groups: group_remaps,
            frag_to_group,
        })
    }

    #[inline]
    pub fn get(&self, addr: u64) -> Option<Option<u64>> {
        let frag = (addr >> 32) as u32;
        // Not in any rewrite group -> unaffected by this remap.
        let gi = *self.frag_to_group.get(&frag)?;
        Some(self.groups[gi].get(frag, addr as u32))
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    fn fully_deleted_fragments(&self) -> Option<RoaringBitmap> {
        // A group with any rewritten row moved at least one row.
        if self.groups.iter().any(|g| !g.frags.is_empty()) {
            return None;
        }
        Some(RoaringBitmap::from_iter(self.frag_to_group.keys().copied()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(frag: u32, offset: u32) -> u64 {
        u64::from(RowAddress::new_from_parts(frag, offset))
    }

    #[test]
    fn test_compact_lookup() {
        // Group A: out-of-order old frags [4, 3], split new frags (11 empty),
        // some deletions. frag 4 (5 rows) keeps 0,2,4; frag 3 keeps 0,1, so the
        // rewritten rows (4,0)(4,2)(4,4)(3,0)(3,1) go to new frags 10(2), 12(3).
        // Group B is a fully-deleted fragment.
        let group_a = GroupInput {
            rewritten_old_row_addrs: RoaringTreemap::from_iter([
                addr(4, 0),
                addr(4, 2),
                addr(4, 4),
                addr(3, 0),
                addr(3, 1),
            ]),
            old_frag_ids: vec![4, 3],
            new_frags: vec![(10, 2), (11, 0), (12, 3)],
        };
        let group_b = GroupInput {
            rewritten_old_row_addrs: RoaringTreemap::new(),
            old_frag_ids: vec![7],
            new_frags: vec![],
        };
        let remap = RowAddrRemap::compact([group_a, group_b]).unwrap();

        // Moves, in rewrite order; frag 4 comes first despite the larger id.
        assert_eq!(remap.get(addr(4, 0)), Some(Some(addr(10, 0))));
        assert_eq!(remap.get(addr(4, 2)), Some(Some(addr(10, 1))));
        // Rank 2 skips the zero-row new fragment 11 and lands in fragment 12.
        assert_eq!(remap.get(addr(4, 4)), Some(Some(addr(12, 0))));
        assert_eq!(remap.get(addr(3, 0)), Some(Some(addr(12, 1))));
        assert_eq!(remap.get(addr(3, 1)), Some(Some(addr(12, 2))));
        // Deleted offsets inside a rewritten fragment.
        assert_eq!(remap.get(addr(4, 1)), Some(None));
        assert_eq!(remap.get(addr(4, 3)), Some(None));
        // Covered but fully-deleted fragment -> Some(None), not None.
        assert_eq!(remap.get(addr(7, 0)), Some(None));
        // Fragment in no group -> unaffected.
        assert_eq!(remap.get(addr(9, 0)), None);
        assert!(!remap.is_empty());
    }

    #[test]
    fn test_fragment_sets() {
        // No rewritten rows at all: every covered fragment is fully deleted.
        let dead = RowAddrRemap::compact([GroupInput {
            rewritten_old_row_addrs: RoaringTreemap::new(),
            old_frag_ids: vec![3, 7],
            new_frags: vec![],
        }])
        .unwrap();
        assert_eq!(
            dead.fully_deleted_fragments(),
            Some(RoaringBitmap::from_iter([3u32, 7u32]))
        );
        assert_eq!(
            dead.affected_fragments(),
            RoaringBitmap::from_iter([3u32, 7u32])
        );

        // At least one rewritten row -> not fully deleted, but both covered
        // fragments (including the fully-deleted frag 1) are still affected.
        let alive = RowAddrRemap::compact([GroupInput {
            rewritten_old_row_addrs: RoaringTreemap::from_iter([addr(0, 0)]),
            old_frag_ids: vec![0, 1],
            new_frags: vec![(10, 1)],
        }])
        .unwrap();
        assert!(alive.fully_deleted_fragments().is_none());
        assert_eq!(
            alive.affected_fragments(),
            RoaringBitmap::from_iter([0u32, 1u32])
        );
    }

    #[test]
    fn test_compact_rejects_rewritten_addrs_outside_old_frags() {
        // Rewritten addresses reference frag 5, not in old_frag_ids. The count
        // still matches (2 == 2), so only the per-fragment split catches it.
        let input = GroupInput {
            rewritten_old_row_addrs: RoaringTreemap::from_iter([addr(0, 0), addr(5, 0)]),
            old_frag_ids: vec![0],
            new_frags: vec![(10, 2)],
        };
        assert!(RowAddrRemap::compact([input]).is_err());
    }

    #[test]
    fn test_compact_rejects_new_frags_out_of_write_order() {
        // New fragments out of ascending id (write) order would make
        // `compute_new_addr` accumulate rows in the wrong order, silently
        // misplacing addresses. A zero-row fragment between them is ignored.
        let input = GroupInput {
            rewritten_old_row_addrs: RoaringTreemap::from_iter([addr(0, 0), addr(0, 1)]),
            old_frag_ids: vec![0],
            new_frags: vec![(12, 1), (11, 1)],
        };
        assert!(RowAddrRemap::compact([input]).is_err());
    }

    #[test]
    fn test_direct_and_empty() {
        // Direct covers arbitrary maps the compact form can't express.
        let mut map = HashMap::new();
        map.insert(addr(2, 0), Some(addr(9, 9)));
        map.insert(addr(5, 1), None);
        let remap = RowAddrRemap::direct(map);
        assert_eq!(remap.get(addr(2, 0)), Some(Some(addr(9, 9))));
        assert_eq!(remap.get(addr(5, 1)), Some(None));
        assert_eq!(remap.get(addr(2, 1)), None);
        // affected_fragments over an explicit map: the fragment of every key.
        assert_eq!(
            remap.affected_fragments(),
            RoaringBitmap::from_iter([2u32, 5u32])
        );

        let empty = RowAddrRemap::empty();
        assert!(empty.is_empty());
        assert_eq!(empty.get(addr(0, 0)), None);
    }
}
