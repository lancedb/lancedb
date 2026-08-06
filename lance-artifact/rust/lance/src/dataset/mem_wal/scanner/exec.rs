// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Execution plan nodes for LSM scanner.
//!
//! This module contains custom DataFusion execution plan implementations
//! for LSM tree query execution:
//!
//! - [`MemtableGenTagExec`]: Wraps a scan to add `_memtable_gen` column
//! - [`BloomFilterGuardExec`]: Guards child execution with bloom filter check
//! - [`CoalesceFirstExec`]: Returns first non-empty result with short-circuit
//! - [`PkBlockFilterExec`]: Drops rows whose PK was superseded by a newer generation (the cross-generation block-list)

mod bloom_guard;
mod coalesce_first;
mod generation_tag;
mod pk;
mod pk_block_filter;

pub use bloom_guard::{BloomFilterGuardExec, compute_pk_hash_from_scalars};
pub use coalesce_first::CoalesceFirstExec;
pub use generation_tag::{MEMTABLE_GEN_COLUMN, MemtableGenTagExec};
pub use pk::{
    ROW_ADDRESS_COLUMN, compute_pk_hash, is_supported_pk_type, resolve_pk_indices,
    validate_pk_types,
};
pub use pk_block_filter::PkBlockFilterExec;
