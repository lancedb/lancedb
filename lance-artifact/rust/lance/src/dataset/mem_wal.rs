// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! MemWAL - Log-Structured Merge (LSM) tree for Lance tables
//!
//! This module implements an LSM tree architecture for high-performance
//! streaming writes with durability guarantees via Write-Ahead Log (WAL).
//!
//! ## Architecture
//!
//! Each shard has:
//! - A **MemTable** for in-memory data (immediately queryable)
//! - A **WAL Buffer** for durability (persisted to object storage)
//! - **In-memory indexes** (BTree, IVF-PQ, FTS) for indexed queries
//!
//! ## Write Path
//!
//! ```text
//! put(batch) → MemTable.insert() → WalBuffer.append() → [async flush to storage]
//!                   ↓
//!           IndexRegistry.update()
//! ```
//!
//! ## Durability
//!
//! Writers can be configured for:
//! - **Durable writes**: Wait for WAL flush before returning
//! - **Non-durable writes**: Buffer in memory, accept potential loss on crash
//!
//! ## Epoch-Based Fencing
//!
//! Each shard has exactly one active writer at any time, enforced via
//! monotonically increasing writer epochs in the shard manifest.

mod api;
mod hnsw;
pub mod index;
mod manifest;
pub mod memtable;
pub mod scanner;
pub mod sharding;
#[cfg(test)]
pub(crate) mod test_util;
pub mod util;
mod wal;
pub mod write;

use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};

/// Column name for the mem_wal tombstone (delete sentinel) marker.
///
/// `_tombstone` is a *physical* column present only in mem_wal memtables and
/// SSTables — it is deliberately kept out of the base table (hard
/// delete), so it is **not** a virtual [`is_system_column`](lance_core::is_system_column).
/// A row with `_tombstone = true` is a delete sentinel: the newest value for
/// its primary key, carrying null in every non-PK column, that wins
/// newest-per-PK resolution and is then silently dropped from query results.
///
/// The column is owned end-to-end by lance: callers pass the base schema and
/// lance injects the column on the write path ([`write::ShardWriter::put`] /
/// [`write::ShardWriter::delete`]), so no caller ever constructs or names it.
pub const TOMBSTONE: &str = "_tombstone";

/// The mem_wal tombstone field appended to the base schema to form the
/// memtable/generation schema.
///
/// Non-nullable: the write path always populates it (`false` for normal rows,
/// `true` for tombstones). Non-nullability also lets the point-lookup base arm
/// synthesize a matching `Literal(false)` column for the `CoalesceFirstExec`
/// exact-schema check.
pub fn tombstone_field() -> ArrowField {
    ArrowField::new(TOMBSTONE, DataType::Boolean, false)
}

/// Extend a base schema with the trailing `_tombstone` column to form the
/// mem_wal memtable/generation schema.
///
/// Idempotent: a schema that already carries `_tombstone` (a reopen/replay
/// path) is returned unchanged. Schema-level metadata and per-field metadata
/// (e.g. the `lance-schema:unenforced-primary-key` marker) are preserved.
pub fn schema_with_tombstone(base: &ArrowSchema) -> Arc<ArrowSchema> {
    if base.column_with_name(TOMBSTONE).is_some() {
        return Arc::new(base.clone());
    }
    let mut fields: Vec<ArrowField> = base.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(tombstone_field());
    Arc::new(ArrowSchema::new_with_metadata(
        fields,
        base.metadata().clone(),
    ))
}

pub use api::{DatasetMemWalExt, InitializeMemWalBuilder};
pub use index::{MemIndexKind, is_maintainable_index_type};
pub use manifest::ShardManifestStore;
pub use memtable::scanner::MemTableScanner;
pub use scanner::{LsmDataSource, LsmGeneration, LsmScanner, ShardSnapshot};
pub use sharding::{
    evaluate_sharding_spec, evaluate_sharding_spec_with_embedded_columns,
    evaluate_sharding_spec_with_source_columns,
};
pub use wal::{BatchDurableWatcher, WalAppendResult, WalAppender, WalReadEntry, WalTailer};
pub use write::SealFence;
pub use write::ShardWriter;
pub use write::ShardWriterConfig;
pub use write::WriteResult;
