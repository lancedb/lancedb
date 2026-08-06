// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! DataFusion ExecutionPlan implementations for MemWAL read path.
//!
//! This module contains execution nodes for:
//! - `MemTableScanExec` - Full table scan with MVCC visibility
//! - `BTreeIndexExec` - BTree index queries
//! - `VectorIndexExec` - HNSW vector search
//! - `MemTableBruteForceVectorExec` - KNN over the active memtable without an HNSW
//! - `FtsIndexExec` - Full-text search

use std::collections::{HashMap, HashSet};

use arrow_array::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::error::Result as DataFusionResult;

mod brute_force_vector;
mod btree;
mod dedup_scan;
mod fts;
mod scan;
mod vector;

use crate::dataset::mem_wal::scanner::exec::resolve_pk_indices;
use crate::dataset::mem_wal::write::BatchStore;

pub use brute_force_vector::MemTableBruteForceVectorExec;
pub use btree::BTreeIndexExec;
pub use dedup_scan::MemTableDedupScanExec;
pub use fts::{FtsIndexExec, SCORE_COLUMN};
pub use scan::{MemTableScanExec, ROW_ADDRESS_COLUMN};
pub use vector::VectorIndexExec;

pub(super) fn newest_pk_positions(
    batch_store: &BatchStore,
    pk_columns: &[String],
    visible_count: usize,
    max_visible_row: u64,
) -> DataFusionResult<HashSet<u64>> {
    let mut newest: HashMap<Vec<ScalarValue>, u64> = HashMap::new();
    let mut current_row: u64 = 0;
    for (batch_position, stored_batch) in batch_store.iter().enumerate() {
        let n = stored_batch.num_rows;
        if n == 0 {
            continue;
        }
        if batch_position >= visible_count {
            current_row += n as u64;
            continue;
        }
        let pk_indices = resolve_pk_indices(&stored_batch.data, pk_columns)?;
        for row in 0..n {
            let pos = current_row + row as u64;
            if pos > max_visible_row {
                break;
            }
            let key = pk_key(&stored_batch.data, &pk_indices, row)?;
            newest.insert(key, pos);
        }
        current_row += n as u64;
    }
    Ok(newest.into_values().collect())
}

fn pk_key(
    batch: &RecordBatch,
    pk_indices: &[usize],
    row: usize,
) -> DataFusionResult<Vec<ScalarValue>> {
    pk_indices
        .iter()
        .map(|&col_idx| ScalarValue::try_from_array(batch.column(col_idx).as_ref(), row))
        .collect()
}
