// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Interval-shaped wrappers around a row-address mask returned by a
//! scalar-index expression evaluation.
//!
//! Each result describes a closed interval `[lower, upper]` in the
//! lattice of subsets:
//!
//! * `lower` — rows the index *guarantees* are in the answer.
//! * `upper` — rows that *might* be in the answer; rows outside `upper`
//!   are guaranteed not in the answer.
//!
//! The three pre-existing "shapes" map onto degenerate intervals:
//!
//! | Old variant | Interval form                          |
//! |-------------|----------------------------------------|
//! | `Exact(m)`  | `{lower: m, upper: m}`                 |
//! | `AtMost(m)` | `{lower: allow_nothing(), upper: m}`   |
//! | `AtLeast(m)`| `{lower: m, upper: all_rows()}`        |
//!
//! Use [`IndexExprResult::exact`] / [`IndexExprResult::at_most`] /
//! [`IndexExprResult::at_least`] to construct those shapes, and the
//! matching [`IndexExprResult::is_exact`] etc. predicates to inspect
//! them. Intervals that are neither (the "Refined" case — a non-empty
//! `lower` strictly inside a non-universe `upper`) arise from indices
//! that can distinguish guaranteed-match from candidate-match rows
//! within a single search (e.g. a zone map answering `IS NOT NULL`).
//!
//! The boolean algebra (`Not` / `BitAnd` / `BitOr`) is elementwise on
//! the endpoints:
//!
//! ```text
//! !{l, u}                = {!u, !l}
//! {l1, u1} & {l2, u2}    = {l1 & l2, u1 & u2}
//! {l1, u1} | {l2, u2}    = {l1 | l2, u1 | u2}
//! ```
//!
//! This works for both the post-`drop_nulls` form ([`IndexExprResult`],
//! backed by [`RowAddrMask`]) and the during-evaluation form
//! ([`NullableIndexExprResult`], backed by [`NullableRowAddrMask`]) —
//! the per-endpoint algebra already implements two-valued and SQL
//! three-valued logic correctly inside each mask type.

use std::sync::{Arc, LazyLock};

use arrow_array::{Array, RecordBatch, UInt32Array, builder::BinaryBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use roaring::RoaringBitmap;

use lance_core::{Error, Result};

use crate::mask::{NullableRowAddrMask, RowAddrMask, RowSetOps};

/// Result of an index search before NULL rows are dropped. Each endpoint
/// is a [`NullableRowAddrMask`] carrying SQL three-valued logic info.
#[derive(Debug, Clone)]
pub struct NullableIndexExprResult {
    /// Rows the index *guarantees* are TRUE.
    pub lower: NullableRowAddrMask,
    /// Rows that may be TRUE. Rows outside `upper` are guaranteed to be
    /// FALSE / NULL (and so not in a `WHERE` answer set).
    pub upper: NullableRowAddrMask,
    // O(1) cache for is_exact(). Set by constructors and propagated
    // elementwise through the boolean algebra.
    exact: bool,
}

impl NullableIndexExprResult {
    /// Precise result — every row in `mask` is in the answer and every
    /// row outside is not. Equivalent to the old `Exact` variant.
    pub fn exact(mask: NullableRowAddrMask) -> Self {
        Self {
            lower: mask.clone(),
            upper: mask,
            exact: true,
        }
    }

    /// Upper-bound-only result — rows outside `mask` are guaranteed not
    /// to match; rows inside may match and require a recheck.
    /// Equivalent to the old `AtMost` variant.
    pub fn at_most(mask: NullableRowAddrMask) -> Self {
        Self {
            lower: NullableRowAddrMask::allow_nothing(),
            upper: mask,
            exact: false,
        }
    }

    /// Lower-bound-only result — rows in `mask` are guaranteed to match;
    /// rows outside may match too and require a recheck. Equivalent to
    /// the old `AtLeast` variant.
    pub fn at_least(mask: NullableRowAddrMask) -> Self {
        Self {
            lower: mask,
            upper: NullableRowAddrMask::all_rows(),
            exact: false,
        }
    }

    /// Construct an interval result from lower/upper bounds.
    ///
    /// `lower` rows are guaranteed TRUE and `upper` rows may be TRUE, with
    /// NULL state preserved at both endpoints. Equal endpoints are canonicalized
    /// to [`Self::exact`] so `is_exact()` remains structural.
    pub fn new(lower: NullableRowAddrMask, upper: NullableRowAddrMask) -> Self {
        if lower == upper {
            Self::exact(lower)
        } else {
            Self {
                lower,
                upper,
                exact: false,
            }
        }
    }

    /// True if the result is exact — the answer is precisely the lower
    /// (== upper) mask.
    ///
    /// This is a **structural** check on the canonical form produced by
    /// the constructors / algebra: an `Exact(m)` built with
    /// [`Self::exact`] holds equal masks, and elementwise `&` / `|` / `!`
    /// preserve that. It is not a semantic emptiness test — a
    /// hand-constructed `IndexExprResult` whose endpoints are
    /// representationally distinct but semantically equal (e.g.
    /// `AllowList(universe)` vs `BlockList(empty)`) will report
    /// `is_exact() == false`. All in-tree code paths construct results
    /// through the canonical builders, so this is sound in practice.
    ///
    /// The three shape predicates are not mutually exclusive — see the
    /// note on [`Self::is_at_least`] for the precedence convention.
    pub fn is_exact(&self) -> bool {
        self.exact
    }

    /// True if `lower` matches no rows (canonical `AllowList(∅)`) — the
    /// index gives only an upper bound on the answer.
    ///
    /// Like [`Self::is_exact`], this is a structural check on the
    /// canonical form. See that doc for the caveat.
    pub fn is_at_most(&self) -> bool {
        matches!(&self.lower, NullableRowAddrMask::AllowList(set) if set.is_empty())
    }

    /// True if `upper` covers every row (canonical `BlockList(∅)`) — the
    /// index gives only a lower bound on the answer.
    ///
    /// **Precedence convention** for consumers branching on shape: check
    /// [`Self::is_exact`] *first* (Exact-of-empty satisfies both
    /// `is_exact` and `is_at_most`; Exact-of-universe satisfies both
    /// `is_exact` and `is_at_least`); then `is_at_least`; finally treat
    /// the residual as `is_at_most` or Refined. The branches in
    /// `filtered_read::apply_index_to_fragment` follow this order.
    pub fn is_at_least(&self) -> bool {
        matches!(&self.upper, NullableRowAddrMask::BlockList(set) if set.is_empty())
    }

    /// Project NULL rows out of the result.
    ///
    /// Under a `WHERE` clause NULL is treated as FALSE, so `drop_nulls`
    /// folds NULL rows out of the answer at each endpoint.
    pub fn drop_nulls(self) -> IndexExprResult {
        IndexExprResult {
            lower: self.lower.drop_nulls(),
            upper: self.upper.drop_nulls(),
            exact: self.exact,
        }
    }
}

impl std::ops::Not for NullableIndexExprResult {
    type Output = Self;

    fn not(self) -> Self {
        Self {
            lower: !self.upper,
            upper: !self.lower,
            exact: self.exact,
        }
    }
}

impl std::ops::BitAnd<Self> for NullableIndexExprResult {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        Self {
            lower: self.lower & rhs.lower,
            upper: self.upper & rhs.upper,
            exact: self.exact && rhs.exact,
        }
    }
}

impl std::ops::BitOr<Self> for NullableIndexExprResult {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Self {
            lower: self.lower | rhs.lower,
            upper: self.upper | rhs.upper,
            exact: self.exact && rhs.exact,
        }
    }
}

/// Result of an index search after NULL rows have been dropped. This is
/// what the read planner consumes.
#[derive(Debug, Clone)]
pub struct IndexExprResult {
    /// Rows the index *guarantees* are in the answer.
    pub lower: RowAddrMask,
    /// Rows that may be in the answer. Rows outside `upper` are
    /// guaranteed not in the answer.
    pub upper: RowAddrMask,
    // O(1) cache for is_exact(). Set by constructors and propagated
    // elementwise through the boolean algebra.
    exact: bool,
}

impl IndexExprResult {
    /// Precise result — every row in `mask` is in the answer and every
    /// row outside is not. Equivalent to the old `Exact` variant.
    pub fn exact(mask: RowAddrMask) -> Self {
        Self {
            lower: mask.clone(),
            upper: mask,
            exact: true,
        }
    }

    /// Upper-bound-only result. Equivalent to the old `AtMost` variant.
    pub fn at_most(mask: RowAddrMask) -> Self {
        Self {
            lower: RowAddrMask::allow_nothing(),
            upper: mask,
            exact: false,
        }
    }

    /// Lower-bound-only result. Equivalent to the old `AtLeast` variant.
    pub fn at_least(mask: RowAddrMask) -> Self {
        Self {
            lower: mask,
            upper: RowAddrMask::all_rows(),
            exact: false,
        }
    }

    /// Construct a refined interval result — `lower` rows are guaranteed
    /// matches and `upper` rows are candidates, with `lower ⊆ upper`.
    ///
    /// Use [`Self::exact`] / [`Self::at_most`] / [`Self::at_least`] for
    /// the three degenerate shapes; this constructor is only needed when
    /// both endpoints are non-trivial.
    pub fn new(lower: RowAddrMask, upper: RowAddrMask) -> Self {
        Self {
            lower,
            upper,
            exact: false,
        }
    }

    /// True if the result is exact — the answer is precisely the lower
    /// (== upper) mask. See [`NullableIndexExprResult::is_exact`] for the
    /// structural-form caveat and the precedence convention shared with
    /// [`Self::is_at_most`] / [`Self::is_at_least`].
    pub fn is_exact(&self) -> bool {
        self.exact
    }

    /// True if `lower` matches no rows (canonical `AllowList(∅)`) — the
    /// index gives only an upper bound on the answer. See
    /// [`NullableIndexExprResult::is_exact`] for caveats.
    pub fn is_at_most(&self) -> bool {
        matches!(&self.lower, RowAddrMask::AllowList(set) if set.is_empty())
    }

    /// True if `upper` covers every row (canonical `BlockList(∅)`) — the
    /// index gives only a lower bound on the answer. See
    /// [`NullableIndexExprResult::is_at_least`] for the precedence
    /// convention consumers should follow.
    pub fn is_at_least(&self) -> bool {
        matches!(&self.upper, RowAddrMask::BlockList(set) if set.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NullableRowAddrSet, RowAddrTreeMap};

    fn allow(rows: &[u64]) -> NullableRowAddrMask {
        NullableRowAddrMask::AllowList(NullableRowAddrSet::new(
            RowAddrTreeMap::from_iter(rows.iter().copied()),
            RowAddrTreeMap::new(),
        ))
    }

    #[test]
    fn nullable_index_expr_result_new_canonicalizes_exact() {
        let lower = allow(&[1, 2]);
        let result = NullableIndexExprResult::new(lower.clone(), lower.clone());

        assert!(result.is_exact());
        assert_eq!(result.lower, lower);
        assert_eq!(result.upper, lower);
    }

    #[test]
    fn nullable_index_expr_result_new_preserves_interval() {
        let lower = allow(&[1, 2]);
        let upper = allow(&[1, 2, 3]);
        let result = NullableIndexExprResult::new(lower.clone(), upper.clone());

        assert!(!result.is_exact());
        assert_eq!(result.lower, lower);
        assert_eq!(result.upper, upper);
    }
}

impl std::ops::Not for IndexExprResult {
    type Output = Self;

    fn not(self) -> Self {
        Self {
            lower: !self.upper,
            upper: !self.lower,
            exact: self.exact,
        }
    }
}

impl std::ops::BitAnd<Self> for IndexExprResult {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        Self {
            lower: self.lower & rhs.lower,
            upper: self.upper & rhs.upper,
            exact: self.exact && rhs.exact,
        }
    }
}

impl std::ops::BitOr<Self> for IndexExprResult {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Self {
            lower: self.lower | rhs.lower,
            upper: self.upper | rhs.upper,
            exact: self.exact && rhs.exact,
        }
    }
}

static TWO_MASK_RESULT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("lower", DataType::Binary, true),
        Field::new("upper", DataType::Binary, true),
        Field::new("fragments_covered", DataType::Binary, true),
    ]))
});

static THREE_VARIANT_RESULT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("result".to_string(), DataType::Binary, true),
        Field::new("discriminant".to_string(), DataType::UInt32, true),
        Field::new("fragments_covered".to_string(), DataType::Binary, true),
    ]))
});

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexExprResultWireFormat {
    ThreeVariant, // A legacy format that used AtMost/AtLeast/Exact variants
    #[default]
    TwoMask, // The two-mask format with upper and lower
}

impl IndexExprResultWireFormat {
    pub fn schema(&self) -> &SchemaRef {
        match self {
            Self::ThreeVariant => &THREE_VARIANT_RESULT_SCHEMA,
            Self::TwoMask => &TWO_MASK_RESULT_SCHEMA,
        }
    }
}

impl IndexExprResult {
    /// Serialize into the `INDEX_EXPR_RESULT_SCHEMA` record-batch layout used to
    /// hand scalar-index results to the read planner.
    #[tracing::instrument(skip_all)]
    fn serialize_standard(&self, fragments_covered: &RoaringBitmap) -> Result<RecordBatch> {
        let lower_arr = self.lower.into_arrow()?;
        let upper_arr = if self.is_exact() {
            let mut b = BinaryBuilder::new();
            b.append_null();
            b.append_null();
            b.finish()
        } else {
            self.upper.into_arrow()?
        };
        let mut frags_builder = BinaryBuilder::new();
        let mut frags_bytes = Vec::with_capacity(fragments_covered.serialized_size());
        fragments_covered.serialize_into(&mut frags_bytes)?;
        frags_builder.append_value(frags_bytes);
        frags_builder.append_null();
        Ok(RecordBatch::try_new(
            TWO_MASK_RESULT_SCHEMA.clone(),
            vec![
                Arc::new(lower_arr),
                Arc::new(upper_arr),
                Arc::new(frags_builder.finish()) as Arc<dyn Array>,
            ],
        )?)
    }

    /// Serialize into the legacy three-variant record-batch layout.
    ///
    /// Refined intervals (a non-empty `lower` strictly inside a non-universe `upper`)
    /// cannot be represented in the legacy encoding and are degraded to `AtMost(upper)`.
    fn serialize_three_variant(&self, fragments_covered: &RoaringBitmap) -> Result<RecordBatch> {
        let (mask, discriminant) = if self.is_exact() {
            (&self.lower, 0u32)
        } else if self.is_at_most() {
            (&self.upper, 1)
        } else if self.is_at_least() {
            (&self.lower, 2)
        } else {
            tracing::warn!(
                "Legacy serialization of refined index-expr result: degrading to AtMost(upper); \
                 answer will remain correct but query will be more expensive"
            );
            (&self.upper, 1)
        };
        let mask_arr = mask.into_arrow()?;
        let discriminant_arr =
            Arc::new(UInt32Array::from(vec![discriminant, discriminant])) as Arc<dyn Array>;
        let mut frags_builder = BinaryBuilder::new();
        let mut frags_bytes = Vec::with_capacity(fragments_covered.serialized_size());
        fragments_covered.serialize_into(&mut frags_bytes)?;
        frags_builder.append_value(frags_bytes);
        frags_builder.append_null();
        Ok(RecordBatch::try_new(
            THREE_VARIANT_RESULT_SCHEMA.clone(),
            vec![
                Arc::new(mask_arr),
                discriminant_arr,
                Arc::new(frags_builder.finish()) as Arc<dyn Array>,
            ],
        )?)
    }

    pub fn serialize(
        &self,
        fragments_covered: &RoaringBitmap,
        format: IndexExprResultWireFormat,
    ) -> Result<RecordBatch> {
        match format {
            IndexExprResultWireFormat::ThreeVariant => {
                self.serialize_three_variant(fragments_covered)
            }
            IndexExprResultWireFormat::TwoMask => self.serialize_standard(fragments_covered),
        }
    }

    /// Deserialize from a record batch produced by [`Self::serialize`].
    pub fn deserialize(batch: &RecordBatch) -> Result<(Self, RoaringBitmap)> {
        use arrow_array::cast::AsArray;

        if batch.num_rows() != 2 {
            return Err(Error::invalid_input_source(
                format!(
                    "Expected a batch with exactly 2 rows but there are {} rows",
                    batch.num_rows()
                )
                .into(),
            ));
        }
        if batch.num_columns() != 3 {
            return Err(Error::invalid_input_source(
                format!(
                    "Expected a batch with exactly three columns but there are {} columns",
                    batch.num_columns()
                )
                .into(),
            ));
        }

        let first_col_name = batch.schema().field(0).name().clone();
        let index_result = if first_col_name == "lower" {
            let lower = RowAddrMask::from_arrow(batch.column(0).as_binary())?;
            let upper_col = batch.column(1).as_binary::<i32>();
            if upper_col.is_null(0) && upper_col.is_null(1) {
                // Null upper column is the serialized form of an exact result.
                Self::exact(lower)
            } else {
                let upper = RowAddrMask::from_arrow(upper_col)?;
                Self {
                    lower,
                    upper,
                    exact: false,
                }
            }
        } else if first_col_name == "result" {
            let row_addr_mask = RowAddrMask::from_arrow(batch.column(0).as_binary())?;
            let match_type = batch
                .column(1)
                .as_primitive::<arrow_array::types::UInt32Type>()
                .values()[0];
            if match_type == 0 {
                Self::exact(row_addr_mask)
            } else if match_type == 1 {
                Self::at_most(row_addr_mask)
            } else if match_type == 2 {
                Self::at_least(row_addr_mask)
            } else {
                return Err(Error::internal(format!(
                    "Unexpected match type: {match_type}"
                )));
            }
        } else {
            return Err(Error::internal(format!(
                "Unexpected column name: {first_col_name}"
            )));
        };

        let frags_col = batch.column(2).as_binary::<i32>();
        let fragments = RoaringBitmap::deserialize_from(frags_col.value(0))?;

        Ok((index_result, fragments))
    }
}
