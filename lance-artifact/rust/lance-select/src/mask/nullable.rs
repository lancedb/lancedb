// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::deepsize::DeepSizeOf;

use super::{RowAddrMask, RowAddrTreeMap, RowSetOps};

/// A set of row ids, with optional set of nulls.
///
/// This is often a result of a filter, where `selected` represents the rows that
/// passed the filter, and `nulls` represents the rows where the filter evaluated
/// to null. For example, in SQL `NULL > 5` evaluates to null. This is distinct
/// from being deselected to support proper three-valued logic for NOT.
/// (`NOT FALSE` is TRUE, `NOT TRUE` is FALSE, but `NOT NULL` is NULL.
/// `NULL | TRUE = TRUE`, `NULL & FALSE = FALSE`, but `NULL | FALSE = NULL`
/// and `NULL & TRUE = NULL`).
#[derive(Clone, Debug, Default, DeepSizeOf)]
pub struct NullableRowAddrSet {
    selected: RowAddrTreeMap,
    // Rows that are NULL. These rows are considered NULL even if they are also in `selected`.
    nulls: RowAddrTreeMap,
}

impl NullableRowAddrSet {
    /// Create a new RowSelection from selected rows and null rows.
    ///
    /// `nulls` may have overlap with `selected`. Rows in `nulls` are considered NULL,
    /// even if they are also in `selected`.
    pub fn new(selected: RowAddrTreeMap, nulls: RowAddrTreeMap) -> Self {
        Self { selected, nulls }
    }

    pub fn with_nulls(mut self, nulls: RowAddrTreeMap) -> Self {
        self.nulls = nulls;
        self
    }

    /// Create an empty selection. Alias for [Default::default]
    pub fn empty() -> Self {
        Default::default()
    }

    /// Get the number of TRUE rows (selected but not null).
    ///
    /// Returns None if the number of TRUE rows cannot be determined. This happens
    /// if the underlying RowAddrTreeMap has full fragments selected.
    pub fn len(&self) -> Option<u64> {
        self.true_rows().len()
    }

    pub fn is_empty(&self) -> bool {
        self.selected.is_empty()
    }

    /// Check if a row_id is selected (TRUE)
    pub fn selected(&self, row_id: u64) -> bool {
        self.selected.contains(row_id) && !self.nulls.contains(row_id)
    }

    /// Get the null rows
    pub fn null_rows(&self) -> &RowAddrTreeMap {
        &self.nulls
    }

    /// Get the raw `selected` bitmap.
    ///
    /// This is the backing field, **not** a semantic "TRUE ∪ NULL" set: a NULL
    /// row may be stored only in `nulls` without appearing in `selected`. Use
    /// this when you want a zero-copy view of the raw representation (e.g.
    /// wire serialization that sends `selected` and `nulls` as separate sets).
    /// For "TRUE rows only", use [`Self::true_rows`].
    pub fn selected_rows(&self) -> &RowAddrTreeMap {
        &self.selected
    }

    /// Get the TRUE rows (selected but not null)
    pub fn true_rows(&self) -> RowAddrTreeMap {
        self.selected.clone() - self.nulls.clone()
    }

    pub fn union_all(selections: &[Self]) -> Self {
        let selected = RowAddrTreeMap::union_all(
            &selections
                .iter()
                .map(|s| &s.selected)
                .collect::<Vec<&RowAddrTreeMap>>(),
        );
        let nulls = RowAddrTreeMap::union_all(
            &selections
                .iter()
                .map(|s| &s.nulls)
                .collect::<Vec<&RowAddrTreeMap>>(),
        );
        // TRUE | NULL = TRUE, so remove any TRUE rows from nulls.
        // A row is TRUE in some input iff it's in that input's (selected - nulls).
        let any_true = selections
            .iter()
            .map(|s| s.selected.clone() - &s.nulls)
            .fold(RowAddrTreeMap::new(), |acc, t| acc | t);
        let nulls = nulls - &any_true;
        Self { selected, nulls }
    }
}

impl PartialEq for NullableRowAddrSet {
    fn eq(&self, other: &Self) -> bool {
        // Semantic equality: two sets are equal iff they decode to the same
        // Kleene state on every row. Comparing raw `selected` would be wrong
        // because a NULL row can be represented either inside or outside the
        // `selected` bitmap.
        self.true_rows() == other.true_rows() && self.nulls == other.nulls
    }
}

impl std::ops::BitAndAssign<&Self> for NullableRowAddrSet {
    fn bitand_assign(&mut self, rhs: &Self) {
        self.nulls = if self.nulls.is_empty() && rhs.nulls.is_empty() {
            RowAddrTreeMap::new() // Fast path
        } else {
            (self.nulls.clone() & &rhs.nulls) // null and null -> null
            | (self.nulls.clone() & &rhs.selected) // null and true -> null
            | (rhs.nulls.clone() & &self.selected) // true and null -> null
        };

        self.selected &= &rhs.selected;
    }
}

impl std::ops::BitOrAssign<&Self> for NullableRowAddrSet {
    fn bitor_assign(&mut self, rhs: &Self) {
        self.nulls = if self.nulls.is_empty() && rhs.nulls.is_empty() {
            RowAddrTreeMap::new() // Fast path
        } else {
            // null or null -> null (excluding rows that are true in either)
            let true_rows =
                (self.selected.clone() - &self.nulls) | (rhs.selected.clone() - &rhs.nulls);
            (self.nulls.clone() | &rhs.nulls) - true_rows
        };

        self.selected |= &rhs.selected;
    }
}

/// A version of [`RowAddrMask`] that supports nulls.
///
/// This mask handles three-valued logic for SQL expressions, where a filter can
/// evaluate to TRUE, FALSE, or NULL. The `selected` set includes rows that are
/// TRUE or NULL. The `nulls` set includes rows that are NULL.
#[derive(Clone, Debug, PartialEq)]
pub enum NullableRowAddrMask {
    AllowList(NullableRowAddrSet),
    BlockList(NullableRowAddrSet),
}

impl NullableRowAddrMask {
    /// A mask that matches no rows (TRUE = ∅, NULL = ∅).
    pub fn allow_nothing() -> Self {
        Self::AllowList(NullableRowAddrSet::empty())
    }

    /// A mask that matches every row (TRUE = universe, NULL = ∅).
    pub fn all_rows() -> Self {
        Self::BlockList(NullableRowAddrSet::empty())
    }

    pub fn selected(&self, row_id: u64) -> bool {
        match self {
            Self::AllowList(NullableRowAddrSet { selected, nulls }) => {
                selected.contains(row_id) && !nulls.contains(row_id)
            }
            Self::BlockList(NullableRowAddrSet { selected, nulls }) => {
                !selected.contains(row_id) && !nulls.contains(row_id)
            }
        }
    }

    pub fn drop_nulls(self) -> RowAddrMask {
        match self {
            Self::AllowList(NullableRowAddrSet { selected, nulls }) => {
                RowAddrMask::AllowList(selected - nulls)
            }
            Self::BlockList(NullableRowAddrSet { selected, nulls }) => {
                RowAddrMask::BlockList(selected | nulls)
            }
        }
    }
}

impl std::ops::Not for NullableRowAddrMask {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::AllowList(set) => Self::BlockList(set),
            Self::BlockList(set) => Self::AllowList(set),
        }
    }
}

impl std::ops::BitAnd for NullableRowAddrMask {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        // Null handling:
        // * null and true -> null
        // * null and null -> null
        // * null and false -> false
        match (self, rhs) {
            (Self::AllowList(a), Self::AllowList(b)) => {
                let nulls = if a.nulls.is_empty() && b.nulls.is_empty() {
                    RowAddrTreeMap::new() // Fast path
                } else {
                    (a.nulls.clone() & &b.nulls) // null and null -> null
                    | (a.nulls & &b.selected) // null and true -> null
                    | (b.nulls & &a.selected) // true and null -> null
                };
                let selected = a.selected & b.selected;
                Self::AllowList(NullableRowAddrSet { selected, nulls })
            }
            (Self::AllowList(allow), Self::BlockList(block))
            | (Self::BlockList(block), Self::AllowList(allow)) => {
                let nulls = if allow.nulls.is_empty() && block.nulls.is_empty() {
                    RowAddrTreeMap::new() // Fast path
                } else {
                    (allow.nulls.clone() & &block.nulls) // null and null -> null
                    | (allow.nulls - &block.selected) // null and true -> null
                    | (block.nulls & &allow.selected) // true and null -> null
                };
                let selected = allow.selected - block.selected;
                Self::AllowList(NullableRowAddrSet { selected, nulls })
            }
            (Self::BlockList(a), Self::BlockList(b)) => {
                let nulls = if a.nulls.is_empty() && b.nulls.is_empty() {
                    RowAddrTreeMap::new() // Fast path
                } else {
                    (a.nulls.clone() & &b.nulls) // null and null -> null
                    | (a.nulls - &b.selected) // null and true -> null
                    | (b.nulls - &a.selected) // true and null -> null
                };
                let selected = a.selected | b.selected;
                Self::BlockList(NullableRowAddrSet { selected, nulls })
            }
        }
    }
}

impl std::ops::BitOr for NullableRowAddrMask {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        // Null handling:
        // * null or true -> true
        // * null or null -> null
        // * null or false -> null
        match (self, rhs) {
            (Self::AllowList(a), Self::AllowList(b)) => {
                let nulls = if a.nulls.is_empty() && b.nulls.is_empty() {
                    RowAddrTreeMap::new() // Fast path
                } else {
                    // null or null -> null (excluding rows that are true in either)
                    let true_rows =
                        (a.selected.clone() - &a.nulls) | (b.selected.clone() - &b.nulls);
                    (a.nulls | b.nulls) - true_rows
                };
                let selected = (a.selected | b.selected) | &nulls;
                Self::AllowList(NullableRowAddrSet { selected, nulls })
            }
            (Self::AllowList(allow), Self::BlockList(block))
            | (Self::BlockList(block), Self::AllowList(allow)) => {
                let allow_true = allow.selected.clone() - &allow.nulls;
                let block_false = block.selected.clone() - &block.nulls;

                let nulls = if allow.nulls.is_empty() && block.nulls.is_empty() {
                    RowAddrTreeMap::new() // Fast path
                } else {
                    // NULL|FALSE=NULL, FALSE|NULL=NULL, NULL|NULL=NULL, TRUE|NULL=TRUE.
                    // So NULL rows are: (allow NULL & block FALSE) or (block NULL & allow not TRUE).
                    (allow.nulls & &block_false) | (block.nulls - &allow_true)
                };
                let selected = (block_false - &allow_true) | &nulls;
                Self::BlockList(NullableRowAddrSet { selected, nulls })
            }
            (Self::BlockList(a), Self::BlockList(b)) => {
                let a_false = a.selected.clone() - &a.nulls;
                let b_false = b.selected.clone() - &b.nulls;
                let nulls = if a.nulls.is_empty() && b.nulls.is_empty() {
                    RowAddrTreeMap::new() // Fast path
                } else {
                    // NULL if: (A NULL & B FALSE) or (A FALSE & B NULL) or (A NULL & B NULL).
                    (a.nulls.clone() & &b_false)
                        | (b.nulls.clone() & &a_false)
                        | (a.nulls & &b.nulls)
                };
                let selected = (a_false & b_false) | &nulls;
                Self::BlockList(NullableRowAddrSet { selected, nulls })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(ids: &[u64]) -> RowAddrTreeMap {
        RowAddrTreeMap::from_iter(ids)
    }

    fn nullable_set(selected: &[u64], nulls: &[u64]) -> NullableRowAddrSet {
        NullableRowAddrSet::new(rows(selected), rows(nulls))
    }

    fn allow(selected: &[u64], nulls: &[u64]) -> NullableRowAddrMask {
        NullableRowAddrMask::AllowList(nullable_set(selected, nulls))
    }

    fn block(selected: &[u64], nulls: &[u64]) -> NullableRowAddrMask {
        NullableRowAddrMask::BlockList(nullable_set(selected, nulls))
    }

    fn assert_mask_selects(mask: &NullableRowAddrMask, selected: &[u64], not_selected: &[u64]) {
        for &id in selected {
            assert!(mask.selected(id), "Expected row {} to be selected", id);
        }
        for &id in not_selected {
            assert!(!mask.selected(id), "Expected row {} to NOT be selected", id);
        }
    }

    #[test]
    fn test_not_with_nulls() {
        // Test case from issue #4756: x != 5 on data [0, 5, null]
        // x = 5 should return: AllowList with selected=[1,2], nulls=[2]
        // NOT(x = 5) should return: BlockList with selected=[1,2], nulls=[2]
        // selected() should return TRUE for row 0, FALSE for rows 1 and 2
        let mask = allow(&[1, 2], &[2]);
        let not_mask = !mask;

        // Row 0: selected (x=0, which is != 5)
        // Row 1: NOT selected (x=5, which is == 5)
        // Row 2: NOT selected (x=null, comparison result is null)
        assert_mask_selects(&not_mask, &[0], &[1, 2]);
    }

    #[test]
    fn test_and_with_nulls() {
        // Test Kleene AND logic: true AND null = null, false AND null = false

        // Case 1: TRUE mask AND mask with nulls
        let true_mask = allow(&[0, 1, 2, 3, 4], &[]);
        let null_mask = allow(&[0, 1, 2, 3, 4], &[1, 3]);
        let result = true_mask & null_mask.clone();

        // TRUE AND TRUE = TRUE; TRUE AND NULL = NULL (filtered out)
        assert_mask_selects(&result, &[0, 2, 4], &[1, 3]);

        // Case 2: FALSE mask AND mask with nulls
        let false_mask = block(&[0, 1, 2, 3, 4], &[]);
        let result = false_mask & null_mask;

        // FALSE AND anything = FALSE
        assert_mask_selects(&result, &[], &[0, 1, 2, 3, 4]);

        // Case 3: Both masks have nulls - union of null sets
        let mask1 = allow(&[0, 1, 2], &[1]);
        let mask2 = allow(&[0, 2, 3], &[2]);
        let result = mask1 & mask2;

        // Only row 0 is TRUE in both; rows 1,2 are null in at least one; row 3 not in first
        assert_mask_selects(&result, &[0], &[1, 2, 3]);
    }

    #[test]
    fn test_or_with_nulls() {
        // Test Kleene OR logic: true OR null = true, false OR null = null

        // Case 1: FALSE mask OR mask with nulls
        let false_mask = block(&[0, 1, 2], &[]);
        let null_mask = allow(&[0, 1, 2], &[1, 2]);
        let result = false_mask | null_mask.clone();

        // FALSE OR TRUE = TRUE; FALSE OR NULL = NULL (filtered out)
        assert_mask_selects(&result, &[0], &[1, 2]);

        // Case 2: TRUE mask OR mask with nulls
        let true_mask = allow(&[0, 1, 2], &[]);
        let result = true_mask | null_mask;

        // TRUE OR anything = TRUE
        assert_mask_selects(&result, &[0, 1, 2], &[]);

        // Case 3: Both have nulls
        let mask1 = block(&[0, 1, 2, 3], &[1, 2]);
        let mask2 = block(&[0, 1, 2, 3], &[2, 3]);
        let result = mask1 | mask2;

        // Row 0: FALSE in both; Rows 1,2,3: NULL in at least one
        assert_mask_selects(&result, &[], &[0, 1, 2, 3]);
    }

    #[test]
    fn test_or_allow_block_keeps_block_nulls() {
        // Allow|Block OR must preserve NULLs from block even when block.selected is empty.
        // allow: TRUE=[1], NULL=[0]; block: FALSE=[], NULL=[0]
        let allow_mask = allow(&[1], &[0]);
        let block_mask = block(&[], &[0]);
        let result = allow_mask | block_mask;

        // Row 1 is TRUE; row 0 remains NULL (not selected)
        assert_mask_selects(&result, &[1], &[0]);
    }

    #[test]
    fn test_or_allow_block_keeps_block_nulls_with_false_rows() {
        // Ensure FALSE stays FALSE and NULL stays NULL when both appear on the block side.
        // allow: TRUE=[2], NULL=[]; block: FALSE=[1], NULL=[0]
        let allow_mask = allow(&[2], &[]);
        let block_mask = block(&[1], &[0]);
        let result = allow_mask | block_mask;

        // Row 2 is TRUE; row 1 is FALSE; row 0 remains NULL (not selected)
        assert_mask_selects(&result, &[2], &[0, 1]);
    }

    #[test]
    fn test_or_block_block_true_overrides_null() {
        // TRUE OR NULL should be TRUE, even when both sides are BlockList.
        let true_mask = block(&[], &[]);
        let null_mask = block(&[], &[0]);
        let result = true_mask | null_mask;

        // Row 0 should be TRUE.
        assert_mask_selects(&result, &[0], &[]);
    }

    #[test]
    fn test_row_selection_bit_or() {
        // [T, N, T, N, F, F, F]
        let left = nullable_set(&[1, 2, 3, 4], &[2, 4]);
        // [F, F, T, N, T, N, N]
        let right = nullable_set(&[3, 4, 5, 6], &[4, 6, 7]);
        // [T, N, T, N, T, N, N]
        let expected_true = rows(&[1, 3, 5]);
        let expected_nulls = rows(&[2, 4, 6, 7]);

        let mut result = left.clone();
        result |= &right;
        assert_eq!(&result.true_rows(), &expected_true);
        assert_eq!(result.null_rows(), &expected_nulls);

        // Commutative property holds
        let mut result = right.clone();
        result |= &left;
        assert_eq!(&result.true_rows(), &expected_true);
        assert_eq!(result.null_rows(), &expected_nulls);
    }

    #[test]
    fn test_row_selection_bit_and() {
        // [T, N, T, N, F, F, F]
        let left = nullable_set(&[1, 2, 3, 4], &[2, 4]);
        // [F, F, T, N, T, N, N]
        let right = nullable_set(&[3, 4, 5, 6], &[4, 6, 7]);
        // [F, F, T, N, F, F, F]
        let expected_true = rows(&[3]);
        let expected_nulls = rows(&[4]);

        let mut result = left.clone();
        result &= &right;
        assert_eq!(&result.true_rows(), &expected_true);
        assert_eq!(result.null_rows(), &expected_nulls);

        // Commutative property holds
        let mut result = right.clone();
        result &= &left;
        assert_eq!(&result.true_rows(), &expected_true);
        assert_eq!(result.null_rows(), &expected_nulls);
    }

    #[test]
    fn test_union_all() {
        // Union all is basically a series of ORs.
        // [T, T, T, N, N, N, F, F, F]
        let set1 = nullable_set(&[1, 2, 3, 4], &[4, 5, 6]);
        // [T, N, F, T, N, F, T, N, F]
        let set2 = nullable_set(&[1, 4, 7, 8], &[2, 5, 8]);
        let set3 = NullableRowAddrSet::empty();

        let result = NullableRowAddrSet::union_all(&[set1, set2, set3]);

        // [T, T, T, T, N, N, T, N, F]
        assert_eq!(&result.true_rows(), &rows(&[1, 2, 3, 4, 7]));
        assert_eq!(result.null_rows(), &rows(&[5, 6, 8]));
    }

    #[test]
    fn test_partial_eq_semantic_equivalence() {
        // Two representations of "row 5 is NULL, nothing is TRUE":
        //   a: selected={5}, nulls={5}  (NULL row also in selected)
        //   b: selected={},  nulls={5}  (NULL row only in nulls)
        // Both decode to the same Kleene state on every row, so they must
        // compare equal under semantic PartialEq.
        let a = NullableRowAddrSet::new(rows(&[5]), rows(&[5]));
        let b = NullableRowAddrSet::new(rows(&[]), rows(&[5]));
        assert_eq!(a, b);
        assert_eq!(a.true_rows(), b.true_rows());
        assert_eq!(a.null_rows(), b.null_rows());
    }

    #[test]
    fn test_union_all_true_overrides_null() {
        // Critical conflict case: row 5 is TRUE in set1 but NULL in set2.
        // Kleene: TRUE ∨ NULL = TRUE → row 5 must end up TRUE, not NULL.
        let set1 = nullable_set(&[5], &[]);
        let set2 = nullable_set(&[5], &[5]);

        let result = NullableRowAddrSet::union_all(&[set1, set2]);

        assert_eq!(result.true_rows(), rows(&[5]));
        assert!(result.null_rows().is_empty());
    }

    #[test]
    fn test_union_all_null_only_input() {
        // Input where a row is NULL but NOT in `selected` (the type allows
        // `selected` to be a strict subset of TRUE ∪ NULL).
        let set1 = NullableRowAddrSet::new(rows(&[]), rows(&[5]));
        let set2 = NullableRowAddrSet::new(rows(&[1]), rows(&[]));

        let result = NullableRowAddrSet::union_all(&[set1, set2]);

        assert_eq!(result.true_rows(), rows(&[1]));
        assert_eq!(result.null_rows(), &rows(&[5]));
    }

    #[test]
    fn test_union_all_all_null_for_a_row() {
        // Every input marks row 7 as NULL; nothing makes it TRUE.
        let set1 = nullable_set(&[7], &[7]);
        let set2 = nullable_set(&[7], &[7]);
        let set3 = NullableRowAddrSet::new(rows(&[]), rows(&[7]));

        let result = NullableRowAddrSet::union_all(&[set1, set2, set3]);

        assert!(result.true_rows().is_empty());
        assert_eq!(result.null_rows(), &rows(&[7]));
    }

    #[test]
    fn test_union_all_empty_inputs() {
        let result = NullableRowAddrSet::union_all(&[]);
        assert!(result.true_rows().is_empty());
        assert!(result.null_rows().is_empty());
    }

    #[test]
    fn test_union_all_single_input() {
        // One input → state of every row preserved.
        let set = nullable_set(&[1, 2, 3, 4], &[2, 4]);
        let result = NullableRowAddrSet::union_all(std::slice::from_ref(&set));

        assert_eq!(result.true_rows(), rows(&[1, 3]));
        assert_eq!(result.null_rows(), &rows(&[2, 4]));
    }

    #[test]
    fn test_union_all_all_empty_inputs() {
        let inputs = [
            NullableRowAddrSet::empty(),
            NullableRowAddrSet::empty(),
            NullableRowAddrSet::empty(),
        ];
        let result = NullableRowAddrSet::union_all(&inputs);
        assert!(result.true_rows().is_empty());
        assert!(result.null_rows().is_empty());
    }

    #[test]
    fn test_union_all_disjoint_inputs() {
        // No row appears in more than one input.
        let set1 = nullable_set(&[1, 2], &[2]);
        let set2 = nullable_set(&[10, 11], &[11]);
        let set3 = nullable_set(&[20], &[]);

        let result = NullableRowAddrSet::union_all(&[set1, set2, set3]);

        assert_eq!(result.true_rows(), rows(&[1, 10, 20]));
        assert_eq!(result.null_rows(), &rows(&[2, 11]));
    }

    #[test]
    fn test_union_all_three_state_row() {
        // Same row 42 across three inputs in three different states:
        //   set1: TRUE   set2: NULL   set3: FALSE
        // Kleene OR: TRUE ∨ NULL ∨ FALSE = TRUE.
        let set1 = nullable_set(&[42], &[]);
        let set2 = nullable_set(&[42], &[42]);
        let set3 = NullableRowAddrSet::empty();

        let result = NullableRowAddrSet::union_all(&[set1, set2, set3]);

        assert_eq!(result.true_rows(), rows(&[42]));
        assert!(result.null_rows().is_empty());
    }

    #[test]
    fn test_union_all_matches_repeated_bitor() {
        // union_all(a, b, c) must equal ((a | b) | c) — same Kleene operator,
        // applied pairwise via the independently-implemented BitOrAssign.
        let set1 = nullable_set(&[1, 2, 3, 4], &[4, 5, 6]);
        let set2 = nullable_set(&[1, 4, 7, 8], &[2, 5, 8]);
        let set3 = nullable_set(&[2, 6, 10], &[6, 10]);

        let via_union_all =
            NullableRowAddrSet::union_all(&[set1.clone(), set2.clone(), set3.clone()]);

        let mut via_bitor = set1;
        via_bitor |= &set2;
        via_bitor |= &set3;

        assert_eq!(via_union_all.true_rows(), via_bitor.true_rows());
        assert_eq!(via_union_all.null_rows(), via_bitor.null_rows());
    }

    #[test]
    fn test_nullable_row_addr_set_with_nulls() {
        let set = NullableRowAddrSet::new(rows(&[1, 2, 3]), RowAddrTreeMap::new());
        let set_with_nulls = set.with_nulls(rows(&[2]));

        assert!(set_with_nulls.selected(1) && set_with_nulls.selected(3));
        assert!(!set_with_nulls.selected(2)); // null
    }

    #[test]
    fn test_nullable_row_addr_set_len_and_is_empty() {
        let set = nullable_set(&[1, 2, 3, 4, 5], &[2, 4]);

        // len() returns count of TRUE rows (selected - nulls)
        assert_eq!(set.len(), Some(3)); // 1, 3, 5
        assert!(!set.is_empty());

        let empty_set = NullableRowAddrSet::empty();
        assert!(empty_set.is_empty());
        assert_eq!(empty_set.len(), Some(0));
    }

    #[test]
    fn test_nullable_row_addr_set_selected() {
        let set = nullable_set(&[1, 2, 3], &[2]);

        // selected() returns true only for TRUE rows (in selected and not in nulls)
        assert!(set.selected(1) && set.selected(3));
        assert!(!set.selected(2)); // null
        assert!(!set.selected(4)); // not in selected
    }

    #[test]
    fn test_nullable_row_addr_set_partial_eq() {
        let set1 = nullable_set(&[1, 2, 3], &[2]);
        let set2 = nullable_set(&[1, 2, 3], &[2]);
        // set3 has same true_rows but different nulls
        let set3 = nullable_set(&[1, 3], &[3]);

        assert_eq!(set1, set2);
        assert_ne!(set1, set3); // different nulls
    }

    #[test]
    fn test_nullable_row_addr_set_bitand_fast_path() {
        // Test fast path when both have no nulls
        let set1 = nullable_set(&[1, 2, 3], &[]);
        let set2 = nullable_set(&[2, 3, 4], &[]);

        let mut result = set1;
        result &= &set2;

        // Intersection: [2, 3]
        assert!(result.selected(2) && result.selected(3));
        assert!(!result.selected(1) && !result.selected(4));
        assert!(result.null_rows().is_empty());
    }

    #[test]
    fn test_nullable_row_addr_set_bitor_fast_path() {
        // Test fast path when both have no nulls
        let set1 = nullable_set(&[1, 2], &[]);
        let set2 = nullable_set(&[3, 4], &[]);

        let mut result = set1;
        result |= &set2;

        // Union: [1, 2, 3, 4]
        for id in [1, 2, 3, 4] {
            assert!(result.selected(id));
        }
        assert!(result.null_rows().is_empty());
    }

    #[test]
    fn test_nullable_row_id_mask_drop_nulls() {
        // Test drop_nulls for AllowList
        let allow_mask = allow(&[1, 2, 3, 4], &[2, 4]);
        let dropped = allow_mask.drop_nulls();
        // Should be AllowList([1, 3]) after removing nulls
        assert!(dropped.selected(1) && dropped.selected(3));
        assert!(!dropped.selected(2) && !dropped.selected(4));

        // Test drop_nulls for BlockList
        let block_mask = block(&[1, 2], &[3]);
        let dropped = block_mask.drop_nulls();
        // BlockList: blocked = [1, 2] | [3] = [1, 2, 3]
        assert!(!dropped.selected(1) && !dropped.selected(2) && !dropped.selected(3));
        assert!(dropped.selected(4) && dropped.selected(5));
    }

    #[test]
    fn test_nullable_row_id_mask_not_blocklist() {
        let block_mask = block(&[1, 2], &[2]);
        let not_mask = !block_mask;

        // NOT(BlockList) = AllowList
        assert!(matches!(not_mask, NullableRowAddrMask::AllowList(_)));
    }

    #[test]
    fn test_nullable_row_id_mask_bitand_allow_allow_fast_path() {
        // Test AllowList & AllowList with no nulls (fast path)
        let mask1 = allow(&[1, 2, 3], &[]);
        let mask2 = allow(&[2, 3, 4], &[]);

        let result = mask1 & mask2;
        assert_mask_selects(&result, &[2, 3], &[1, 4]);
    }

    #[test]
    fn test_nullable_row_id_mask_bitand_allow_block() {
        let allow_mask = allow(&[1, 2, 3, 4, 5], &[2]);
        let block_mask = block(&[3, 4], &[4]);

        let result = allow_mask & block_mask;
        // allow: T=[1,3,4,5], N=[2]
        // block: F=[3,4], N=[4]
        // T & T = T; N & T = N (filtered); T & F = F; T & N = N (filtered)
        assert_mask_selects(&result, &[1, 5], &[2, 3, 4]);
    }

    #[test]
    fn test_nullable_row_id_mask_bitand_allow_block_fast_path() {
        // Test AllowList & BlockList fast path (no nulls)
        let allow_mask = allow(&[1, 2, 3], &[]);
        let block_mask = block(&[2], &[]);

        let result = allow_mask & block_mask;
        assert_mask_selects(&result, &[1, 3], &[2]);
    }

    #[test]
    fn test_nullable_row_id_mask_bitand_block_block() {
        let block1 = block(&[1, 2], &[2]);
        let block2 = block(&[2, 3], &[3]);

        let result = block1 & block2;
        // block1: F=[1], N=[2]; block2: F=[2], N=[3]
        // F & T = F; N & F = F; T & N = N (filtered); T & T = T
        assert_mask_selects(&result, &[4], &[1, 2, 3]);
    }

    #[test]
    fn test_nullable_row_id_mask_bitand_block_block_fast_path() {
        // Test BlockList & BlockList fast path (no nulls)
        let block1 = block(&[1], &[]);
        let block2 = block(&[2], &[]);

        let result = block1 & block2;
        assert_mask_selects(&result, &[3], &[1, 2]);
    }

    #[test]
    fn test_nullable_row_id_mask_bitor_allow_allow_fast_path() {
        // Test AllowList | AllowList with no nulls (fast path)
        let mask1 = allow(&[1, 2], &[]);
        let mask2 = allow(&[3, 4], &[]);

        let result = mask1 | mask2;
        assert_mask_selects(&result, &[1, 2, 3, 4], &[5]);
    }

    #[test]
    fn test_nullable_row_id_mask_bitor_allow_block() {
        let allow_mask = allow(&[1, 2, 3], &[2]);
        let block_mask = block(&[1, 4], &[4]);

        let result = allow_mask | block_mask;
        // allow: T=[1,3], N=[2]; block: F=[1], N=[4], T=everything else
        // T|F=T, T|T=T, N|T=T
        assert_mask_selects(&result, &[1, 2, 3], &[]);
    }

    #[test]
    fn test_nullable_row_id_mask_bitor_allow_block_fast_path() {
        // Test AllowList | BlockList fast path (no nulls)
        let allow_mask = allow(&[1], &[]);
        let block_mask = block(&[2], &[]);

        let result = allow_mask | block_mask;
        // AllowList([1]) | BlockList([2]) = BlockList([2] - [1]) = BlockList([2])
        assert_mask_selects(&result, &[1, 3], &[2]);
    }

    #[test]
    fn test_nullable_row_id_mask_bitor_block_block_fast_path() {
        // Test BlockList | BlockList with no nulls (fast path)
        let block1 = block(&[1, 2], &[]);
        let block2 = block(&[2, 3], &[]);

        let result = block1 | block2;
        // OR of BlockLists: BlockList([1,2] & [2,3]) = BlockList([2])
        assert_mask_selects(&result, &[1, 3, 4], &[2]);
    }
}
