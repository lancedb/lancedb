// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! System indices: table-level structure persisted as indices.
//!
//! Unlike normal indices, whose internals stay opaque behind
//! [`crate::format::IndexMetadata::index_details`], the table format genuinely
//! interprets the contents of these indices (fragment remapping, row
//! visibility). They therefore live at the table layer.
//!
//! The `Index`-trait adapters for these structs live in `lance-index`, which
//! re-exports the structs defined here.

pub mod frag_reuse;
pub mod mem_wal;
