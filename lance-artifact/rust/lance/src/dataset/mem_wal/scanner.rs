// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! LSM Scanner - Unified scanner for LSM tree data
//!
//! This module provides scanners that read from multiple data sources
//! in an LSM tree architecture:
//! - Base table (compacted data)
//! - SSTables (persisted but not yet compacted)
//! - Active MemTable (in-memory buffer)
//!
//! The scanner handles deduplication by primary key, keeping the newest
//! version based on generation number and row address.
//!
//! ## Supported Query Types
//!
//! - **Scan**: Full table scan with deduplication
//! - **Point Lookup**: Primary key-based lookup with bloom filter optimization
//! - **Vector Search**: KNN search with staleness detection
//!
//! ## Example
//!
//! ```ignore
//! use lance::dataset::mem_wal::scanner::LsmScanner;
//!
//! let scanner = LsmScanner::new(base_table, shard_snapshots, vec!["pk".to_string()])
//!     .project(&["id", "name"])?
//!     .filter("id > 10")?
//!     .limit(Some(100), None)?;
//!
//! let stream = scanner.try_into_stream().await?;
//! ```

use arrow_schema::Schema as ArrowSchema;
use datafusion::common::ToDFSchema;
use datafusion::prelude::{Expr, SessionContext};
use lance_core::{Error, Result};

mod block_list;
mod builder;
mod collector;
mod data_source;
pub mod exec;
mod fts_search;
mod planner;
mod point_lookup;
mod projection;
pub(crate) mod sstable_cache;
mod vector_search;

pub use block_list::write_pk_sidecar;
pub use builder::LsmScanner;
pub use collector::{
    ActiveMemTableRef, InMemoryMemTableRef, InMemoryMemTables, LsmDataSourceCollector,
};
pub use data_source::{FreshTierWatermark, LsmDataSource, LsmGeneration, ShardSnapshot, SsTable};
pub use fts_search::{LsmFtsSearchPlanner, SCORE_COLUMN};
pub use point_lookup::LsmPointLookupPlanner;
pub use projection::DISTANCE_COLUMN;
pub use sstable_cache::{DatasetCache, SsTableCache, SsTableWarmer};
pub use vector_search::LsmVectorSearchPlanner;

/// Parse a SQL filter expression against a MemWAL source schema.
pub fn parse_filter_expr(schema: &ArrowSchema, filter_expr: &str) -> Result<Expr> {
    let ctx = SessionContext::new();
    let df_schema = schema
        .clone()
        .to_dfschema()
        .map_err(|e| Error::invalid_input(format!("Failed to create DFSchema: {}", e)))?;
    ctx.parse_sql_expr(filter_expr, &df_schema)
        .map_err(|e| Error::invalid_input(format!("Failed to parse filter expression: {}", e)))
}

pub(crate) fn validate_overfetch_factor(factor: f64) -> Result<f64> {
    if !factor.is_finite() || factor < 1.0 {
        return Err(Error::invalid_input(format!(
            "overfetch_factor must be finite and at least 1.0, got {}",
            factor
        )));
    }
    Ok(factor)
}
