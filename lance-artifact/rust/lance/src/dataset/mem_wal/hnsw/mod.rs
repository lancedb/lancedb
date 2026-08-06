// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Fast HNSW primitives for MemWAL's in-memory vector index.
//!
//! The graph takes the hnswlib layout as the performance baseline while
//! keeping MemWAL-specific constraints explicit: vector data is borrowed from
//! Arrow batches, graph publication follows a multi-reader / single-writer
//! lifecycle, and flush snapshots emit Lance's HNSW sub-index batch format.

#![allow(dead_code, unused_imports)]

mod graph;
mod storage;

pub use graph::{BuildParams, HnswGraph, SearchParams};
pub use storage::{ArrowFixedSizeListVectorStore, VectorSource};

#[cfg(test)]
pub use graph::{LanceHnswMetadata, ScoredPoint, SearchResult};
#[cfg(test)]
pub use storage::{VectorStoreSnapshot, compute_f32_distance};
