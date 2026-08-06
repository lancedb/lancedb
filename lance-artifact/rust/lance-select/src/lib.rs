// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Row-selection primitives shared across Lance.
//!
//! This crate contains:
//!
//! * [`mask`] — `RowAddrMask` / `NullableRowAddrMask` and their underlying
//!   set types. These describe which rows survive a filter and are produced
//!   by scalar-index searches, prefilters, and the read planner.
//! * [`result`] — `IndexExprResult` / `NullableIndexExprResult`: the
//!   certainty-tagged wrappers around a `RowAddrMask` returned by a scalar-
//!   index expression evaluation, plus their boolean algebra
//!   (`Not`/`BitAnd`/`BitOr`).
//!
//! These types were extracted from `lance-core` and `lance-index` so that
//! consumers (benchmarks, downstream filtering code) can depend on the
//! mask substrate without pulling in either of those larger crates.

pub mod mask;
pub mod result;

pub use mask::{
    NullableRowAddrMask, NullableRowAddrSet, RowAddrMask, RowAddrSelection, RowAddrTreeMap,
    RowIdMask, RowIdSet, RowSetOps, bitmap_to_ranges, ranges_to_bitmap,
};
pub use result::{IndexExprResult, NullableIndexExprResult};
