// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Generic bloom filter primitives.
//!
//! These are storage-agnostic data structures with no Lance semantics, used by
//! higher-level crates (e.g. the bloom filter scalar index in `lance-index`).

pub mod as_bytes;
pub mod sbbf;
