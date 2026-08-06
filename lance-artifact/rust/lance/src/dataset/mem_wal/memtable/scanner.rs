// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Read path for MemTable.
//!
//! This module provides query execution over MemTable data using DataFusion.
//!
//! ## Architecture
//!
//! ```text
//!                     MemTableScanner (Builder)
//!                            |
//!                     create_plan()
//!                            |
//!               +------------+------------+
//!               |                         |
//!          Full Scan                 Index Query
//!               |                         |
//!               v                         v
//!         MemTableScanExec            IndexExec
//!               |                         |
//!               +------------+------------+
//!                            |
//!                     DataFusion Execution
//!                            |
//!                            v
//!                   SendableRecordBatchStream
//! ```
//!
//! ## Key Features
//!
//! - **MVCC Visibility**: All scans respect visibility sequence numbers
//! - **Index Support**: BTree, HNSW vector, and FTS indexes
//! - **DataFusion Integration**: Full ExecutionPlan compatibility

mod builder;
mod exec;

pub use builder::MemTableScanner;
pub use exec::{BTreeIndexExec, FtsIndexExec, MemTableScanExec, VectorIndexExec};
