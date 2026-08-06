// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Builder of Hnsw Graph.

use arrow::array::{AsArray, ListBuilder, UInt32Builder};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, UInt32Type};
use arrow_array::{ArrayRef, Float32Array, ListArray, RecordBatch, UInt64Array};
use crossbeam_queue::ArrayQueue;
use itertools::Itertools;
use lance_core::deepsize::DeepSizeOf;
use lance_core::utils::row_addr_remap::RowAddrRemap;

use lance_core::utils::tokio::get_num_compute_intensive_cpus;
use lance_linalg::distance::DistanceType;
use rayon::prelude::*;
use std::cmp::min;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::instrument;

use lance_core::{Error, Result};
use rand::{Rng, SeedableRng, rngs::SmallRng};
use serde::{Deserialize, Serialize};

use super::super::graph::beam_search;
use super::{
    HNSW_TYPE, HnswMetadata, VECTOR_ID_COL, VECTOR_ID_FIELD, select_neighbors_heuristic_owned,
};
use crate::metrics::MetricsCollector;
use crate::prefilter::PreFilter;
use crate::vector::flat::storage::{FlatBinStorage, FlatFloatStorage};
use crate::vector::graph::builder::GraphBuilderNode;
use crate::vector::graph::{
    BorrowingGraph, DISTS_FIELD, Graph, NEIGHBORS_COL, NEIGHBORS_FIELD, OrderedFloat, OrderedNode,
    VisitedGenerator,
};
use crate::vector::graph::{
    Visited, beam_search_acorn, beam_search_borrowed, greedy_search, greedy_search_borrowed,
};
use crate::vector::storage::{DistCalculator, VectorStore};
use crate::vector::v3::subindex::IvfSubIndex;
use crate::vector::{ApproxMode, Query, VECTOR_RESULT_SCHEMA};

pub const HNSW_METADATA_KEY: &str = "lance:hnsw";

/// Fixed seed for HNSW node-level assignment.
///
/// A constant seed makes graph construction reproducible (same data + params =>
/// same graph), which keeps index builds deterministic and tests stable. Recall
/// is statistically unaffected — the level distribution is identical, only the
/// random draws become fixed. Shared by the offline ([`HNSWBuilder`]) and online
/// ([`super::online::OnlineHnswBuilder`]) builders so both produce comparable graphs.
pub(crate) const HNSW_LEVEL_RNG_SEED: u64 = 42;

/// Minimum edge count that avoids severely fragmented graphs during parallel
/// construction while still allowing deliberately small test and index sizes.
pub(crate) const MIN_HNSW_M: usize = 4;

/// Draw a node level using the distribution from Algorithm 1.
pub(crate) fn random_level_with<R: Rng + ?Sized>(params: &HnswBuildParams, rng: &mut R) -> u16 {
    let ml = 1.0 / (params.m as f32).ln();
    min(
        (-rng.random::<f32>().ln() * ml) as u16,
        params.max_level - 1,
    )
}

/// Parameters of building HNSW index
#[derive(Debug, Clone, Serialize, Deserialize, DeepSizeOf)]
pub struct HnswBuildParams {
    /// Maximum number of levels in the graph.
    pub max_level: u16,

    /// number of connections to establish while inserting new element
    pub m: usize,

    /// size of the dynamic list for the candidates
    pub ef_construction: usize,

    /// number of vectors ahead to prefetch while building the graph
    pub prefetch_distance: Option<usize>,
}

impl From<&HnswBuildParams> for crate::pb::HnswParameters {
    fn from(params: &HnswBuildParams) -> Self {
        Self {
            max_connections: params.m as u32,
            construction_ef: params.ef_construction as u32,
            max_level: params.max_level as u32,
        }
    }
}

impl Default for HnswBuildParams {
    fn default() -> Self {
        Self {
            max_level: 7,
            m: 20,
            ef_construction: 150,
            prefetch_distance: Some(2),
        }
    }
}

impl HnswBuildParams {
    /// The maximum level of the graph.
    /// The default value is `7`.
    pub fn max_level(mut self, max_level: u16) -> Self {
        self.max_level = max_level;
        self
    }

    /// The number of connections to establish while inserting new element
    ///
    /// Must be at least 4. Smaller values produce severely fragmented graphs
    /// during parallel construction.
    /// The default value is `20`.
    pub fn num_edges(mut self, m: usize) -> Self {
        self.m = m;
        self
    }

    /// Number of candidates to be considered when searching for the nearest neighbors
    /// during the construction of the graph.
    ///
    /// The default value is `150`.
    pub fn ef_construction(mut self, ef_construction: usize) -> Self {
        self.ef_construction = ef_construction;
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_level == 0 {
            return Err(Error::invalid_input(format!(
                "HnswBuildParams::max_level must be greater than 0, got {}",
                self.max_level
            )));
        }
        if self.m < MIN_HNSW_M {
            return Err(Error::invalid_input(format!(
                "HnswBuildParams::m must be at least {MIN_HNSW_M} to avoid severely fragmented graphs, got {}",
                self.m,
            )));
        }
        if self.m > usize::MAX / 2 {
            return Err(Error::invalid_input(format!(
                "HnswBuildParams::m must be at most {} so the level-0 reciprocal limit can be represented, got {}",
                usize::MAX / 2,
                self.m
            )));
        }
        if self.ef_construction < self.m {
            return Err(Error::invalid_input(format!(
                "HnswBuildParams::ef_construction must be at least m ({}), got {}",
                self.m, self.ef_construction
            )));
        }
        Ok(())
    }

    /// Build the HNSW index from the given data.
    ///
    /// # Parameters
    /// - `data`: A FixedSizeList to build the HNSW.
    /// - `distance_type`: The distance type to use.
    pub async fn build(self, data: ArrayRef, distance_type: DistanceType) -> Result<HNSW> {
        let vectors = data.as_fixed_size_list().clone();
        match (vectors.value_type(), distance_type) {
            (DataType::UInt8, DistanceType::Hamming) => {
                let vec_store = Arc::new(FlatBinStorage::new(vectors, distance_type));
                HNSW::index_vectors(vec_store.as_ref(), self)
            }
            (DataType::UInt8, _) => Err(Error::invalid_input(format!(
                "HNSW only supports hamming distance for UInt8 vectors, got {}",
                distance_type
            ))),
            (_, DistanceType::Hamming) => Err(Error::invalid_input(format!(
                "HNSW hamming distance only supports UInt8 vectors, got {}",
                vectors.value_type()
            ))),
            _ => {
                let vec_store = Arc::new(FlatFloatStorage::new(vectors, distance_type));
                HNSW::index_vectors(vec_store.as_ref(), self)
            }
        }
    }
}

/// Build a HNSW graph.
///
/// Currently, the HNSW graph is fully built in memory.
///
/// During the build, the graph is built layer by layer.
///
/// Each node in the graph has a global ID which is the index on the base layer.
#[derive(Clone, DeepSizeOf)]
pub struct HNSW {
    inner: Arc<HnswCore>,
}

struct HnswCore {
    params: HnswBuildParams,
    graph: HnswGraph,
    level_count: Vec<usize>,
    entry_point: u32,
    visited_generator_queue: Arc<ArrayQueue<VisitedGenerator>>,
}

impl DeepSizeOf for HnswCore {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.params.deep_size_of_children(context)
            + self.graph.deep_size_of_children(context)
            + self.level_count.deep_size_of_children(context)
        // Skipping the visited_generator_queue
    }
}

impl HnswCore {
    fn max_level(&self) -> u16 {
        self.level_count
            .iter()
            .rposition(|count| *count != 0)
            .map_or(0, |level| level + 1) as u16
    }

    fn num_nodes(&self, level: usize) -> usize {
        self.level_count[level]
    }
}

impl Debug for HNSW {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HNSW(max_layers: {})", self.inner.max_level() as usize,)
    }
}

impl HNSW {
    /// Construct an HNSW from its constituent parts. Used by the online
    /// builder when finalizing.
    pub(crate) fn from_parts(
        params: HnswBuildParams,
        nodes: Vec<GraphBuilderNode>,
        level_count: Vec<usize>,
        entry_point: u32,
    ) -> Self {
        let queue_size = get_num_compute_intensive_cpus().max(1) * 2;
        let visited_generator_queue = Arc::new(ArrayQueue::new(queue_size));
        for _ in 0..queue_size {
            let _ = visited_generator_queue.push(VisitedGenerator::new(0));
        }
        Self {
            inner: Arc::new(HnswCore {
                params,
                graph: HnswGraph::Built(Arc::new(nodes)),
                level_count,
                entry_point,
                visited_generator_queue,
            }),
        }
    }

    pub fn empty() -> Self {
        Self {
            inner: Arc::new(HnswCore {
                params: HnswBuildParams::default(),
                graph: HnswGraph::Built(Arc::new(Vec::new())),
                level_count: Vec::new(),
                entry_point: 0,
                visited_generator_queue: Arc::new(ArrayQueue::new(1)),
            }),
        }
    }

    pub fn len(&self) -> usize {
        match &self.inner.graph {
            HnswGraph::Built(nodes) => nodes.len(),
            // `level_count[0]` is the bottom-level (== total) node count.
            HnswGraph::Loaded(graph) => graph.level_count[0],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn max_level(&self) -> u16 {
        self.inner.max_level()
    }

    pub fn num_nodes(&self, level: usize) -> usize {
        self.inner.num_nodes(level)
    }

    /// Returns the in-memory builder nodes, if this graph was freshly built.
    ///
    /// A disk-loaded graph is Arrow-backed and has no `GraphBuilderNode`s,
    /// so this returns `None` for it.
    pub fn nodes(&self) -> Option<Arc<Vec<GraphBuilderNode>>> {
        match &self.inner.graph {
            HnswGraph::Built(nodes) => Some(nodes.clone()),
            HnswGraph::Loaded(_) => None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn search_inner(
        &self,
        query: ArrayRef,
        k: usize,
        params: &HnswQueryParams,
        bitset: Option<Visited>,
        visited_generator: &mut VisitedGenerator,
        storage: &impl VectorStore,
        prefetch_distance: Option<usize>,
    ) -> Result<Vec<OrderedNode>> {
        let dist_calc = storage.dist_calculator(query, params.dist_q_c);
        let entry = self.inner.entry_point;
        let ep = OrderedNode::new(entry, dist_calc.distance(entry).into());

        // The level descent + bottom beam search are identical across
        // graph backends; only the view types differ. `run_search` is
        // generic over those view types so the loop is single-sourced:
        // each backend supplies a per-level view closure and a
        // bottom-level view.
        let result = match &self.inner.graph {
            HnswGraph::Built(nodes) => {
                let nodes = nodes.as_slice();
                self.run_search(
                    ep,
                    k,
                    params,
                    bitset.as_ref(),
                    visited_generator,
                    storage.len(),
                    prefetch_distance,
                    &dist_calc,
                    |level| ImmutableHnswLevelView::new(level, nodes),
                    ImmutableHnswBottomView::new(nodes),
                )
            }
            HnswGraph::Loaded(graph) => {
                let graph = graph.as_ref();
                self.run_search(
                    ep,
                    k,
                    params,
                    bitset.as_ref(),
                    visited_generator,
                    storage.len(),
                    prefetch_distance,
                    &dist_calc,
                    |level| LoadedHnswLevelView::new(level, graph),
                    LoadedHnswBottomView::new(graph),
                )
            }
        };
        Ok(result)
    }

    /// Drives the shared HNSW query path over backend-specific graph
    /// views: a per-level view produced by `make_level` and a
    /// bottom-level view `bottom`. The views borrow their backing store
    /// and are created, used, and dropped entirely within this call;
    /// only the owned result escapes. Monomorphizing over `L`/`B` is the
    /// single seam that lets the in-memory and disk-loaded backends
    /// share one search loop.
    #[allow(clippy::too_many_arguments)]
    fn run_search<L, B>(
        &self,
        ep: OrderedNode,
        k: usize,
        params: &HnswQueryParams,
        bitset: Option<&Visited>,
        visited_generator: &mut VisitedGenerator,
        storage_len: usize,
        prefetch_distance: Option<usize>,
        dist_calc: &impl DistCalculator,
        make_level: impl Fn(u16) -> L,
        bottom: B,
    ) -> Vec<OrderedNode>
    where
        L: BorrowingGraph,
        B: BorrowingGraph,
    {
        // Greedy descent stops at level 1 (HNSW paper, Algorithm 2): level 0
        // is searched only by the ef-bounded beam below. Greedily descending
        // into level 0 first collapses the entry point into a deep local
        // minimum, which costs extra distance computations and measurably
        // hurts recall at low ef (https://github.com/lance-format/lance/issues/5208).
        let mut ep = ep;
        for level in (1..self.max_level()).rev() {
            let cur_level = make_level(level);
            ep = greedy_search_borrowed(
                &cur_level,
                ep,
                dist_calc,
                self.inner.params.prefetch_distance,
            );
        }
        let mut visited = visited_generator.generate(storage_len);
        beam_search_borrowed(
            &bottom,
            &ep,
            params,
            dist_calc,
            bitset,
            prefetch_distance,
            &mut visited,
        )
        .into_iter()
        .take(k)
        .collect::<Vec<OrderedNode>>()
    }

    #[instrument(level = "debug", skip(self, query, bitset, storage))]
    pub fn search_basic(
        &self,
        query: ArrayRef,
        k: usize,
        params: &HnswQueryParams,
        bitset: Option<Visited>,
        storage: &impl VectorStore,
    ) -> Result<Vec<OrderedNode>> {
        let mut visited_generator = self
            .inner
            .visited_generator_queue
            .pop()
            .unwrap_or_else(|| VisitedGenerator::new(storage.len()));
        let result = self.search_inner(
            query,
            k,
            params,
            bitset,
            &mut visited_generator,
            storage,
            Some(2),
        );

        match self.inner.visited_generator_queue.push(visited_generator) {
            Ok(_) => {}
            Err(_) => {
                log::warn!("visited_generator_queue is full");
            }
        }

        result
    }

    /// Like [Self::search_basic] but the bottom level runs
    /// [beam_search_acorn], which only scores mask-passing nodes.
    pub fn search_acorn(
        &self,
        query: ArrayRef,
        k: usize,
        params: &HnswQueryParams,
        bitset: &Visited,
        storage: &impl VectorStore,
    ) -> Result<Vec<OrderedNode>> {
        let mut visited_generator = self
            .inner
            .visited_generator_queue
            .pop()
            .unwrap_or_else(|| VisitedGenerator::new(storage.len()));
        let mut expanded_generator = self
            .inner
            .visited_generator_queue
            .pop()
            .unwrap_or_else(|| VisitedGenerator::new(storage.len()));

        let result = self.search_acorn_inner(
            query,
            k,
            params,
            bitset,
            &mut visited_generator,
            &mut expanded_generator,
            storage,
            Some(2),
        );

        // if the queue is full, we just don't push it back, so ignore the error here
        let _ = self.inner.visited_generator_queue.push(visited_generator);
        let _ = self.inner.visited_generator_queue.push(expanded_generator);
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn search_acorn_inner(
        &self,
        query: ArrayRef,
        k: usize,
        params: &HnswQueryParams,
        bitset: &Visited,
        visited_generator: &mut VisitedGenerator,
        expanded_generator: &mut VisitedGenerator,
        storage: &impl VectorStore,
        prefetch_distance: Option<usize>,
    ) -> Result<Vec<OrderedNode>> {
        let dist_calc = storage.dist_calculator(query, params.dist_q_c);
        let entry = self.inner.entry_point;
        let ep = OrderedNode::new(entry, dist_calc.distance(entry).into());

        let result = match &self.inner.graph {
            HnswGraph::Built(nodes) => {
                let nodes = nodes.as_slice();
                self.run_search_acorn(
                    ep,
                    params,
                    bitset,
                    visited_generator,
                    expanded_generator,
                    storage.len(),
                    prefetch_distance,
                    &dist_calc,
                    |level| ImmutableHnswLevelView::new(level, nodes),
                    ImmutableHnswBottomView::new(nodes),
                )
            }
            HnswGraph::Loaded(graph) => {
                let graph = graph.as_ref();
                self.run_search_acorn(
                    ep,
                    params,
                    bitset,
                    visited_generator,
                    expanded_generator,
                    storage.len(),
                    prefetch_distance,
                    &dist_calc,
                    |level| LoadedHnswLevelView::new(level, graph),
                    LoadedHnswBottomView::new(graph),
                )
            }
        };
        Ok(result.into_iter().take(k).collect())
    }

    /// [Self::run_search] for the ACORN traversal: same level descent, but
    /// the bottom level runs [beam_search_acorn].
    #[allow(clippy::too_many_arguments)]
    fn run_search_acorn<L, B>(
        &self,
        ep: OrderedNode,
        params: &HnswQueryParams,
        bitset: &Visited,
        visited_generator: &mut VisitedGenerator,
        expanded_generator: &mut VisitedGenerator,
        storage_len: usize,
        prefetch_distance: Option<usize>,
        dist_calc: &impl DistCalculator,
        make_level: impl Fn(u16) -> L,
        bottom: B,
    ) -> Vec<OrderedNode>
    where
        L: BorrowingGraph,
        B: BorrowingGraph,
    {
        // Same level descent as [Self::run_search]: greedy stops at level 1
        // (see the comment there); level 0 is left to the beam below.
        let mut ep = ep;
        for level in (1..self.max_level()).rev() {
            let cur_level = make_level(level);
            ep = greedy_search_borrowed(
                &cur_level,
                ep,
                dist_calc,
                self.inner.params.prefetch_distance,
            );
        }
        let mut visited = visited_generator.generate(storage_len);
        let mut expanded = expanded_generator.generate(storage_len);
        beam_search_acorn(
            &bottom,
            &ep,
            params,
            dist_calc,
            bitset,
            prefetch_distance,
            &mut visited,
            &mut expanded,
        )
    }

    #[instrument(level = "debug", skip(self, storage, query, prefilter_bitset))]
    fn flat_search(
        &self,
        storage: &impl VectorStore,
        query: ArrayRef,
        k: usize,
        prefilter_bitset: Visited,
        params: &HnswQueryParams,
    ) -> Vec<OrderedNode> {
        let lower_bound: OrderedFloat = params.lower_bound.unwrap_or(f32::MIN).into();
        let upper_bound: OrderedFloat = params.upper_bound.unwrap_or(f32::MAX).into();

        let dist_calc = storage.dist_calculator(query, params.dist_q_c);
        let mut heap = BinaryHeap::<OrderedNode>::with_capacity(k);

        match self.inner.params.prefetch_distance {
            Some(ahead) if ahead > 0 => {
                let mut ids_iter = prefilter_bitset.iter_ones().map(|i| i as u32);
                let mut buffer = VecDeque::with_capacity(ahead + 1);
                for _ in 0..=ahead {
                    if let Some(id) = ids_iter.next() {
                        buffer.push_back(id);
                    } else {
                        break;
                    }
                }

                while let Some(node_id) = buffer.pop_front() {
                    if let Some(&prefetch_id) = buffer.get(ahead - 1) {
                        dist_calc.prefetch(prefetch_id);
                    }
                    if let Some(next) = ids_iter.next() {
                        buffer.push_back(next);
                    }

                    let dist: OrderedFloat = dist_calc.distance(node_id).into();
                    if dist <= lower_bound || dist > upper_bound {
                        continue;
                    }
                    if heap.len() < k {
                        heap.push((dist, node_id).into());
                    } else if dist < heap.peek().unwrap().dist {
                        heap.pop();
                        heap.push((dist, node_id).into());
                    }
                }
            }
            _ => {
                for node_id in prefilter_bitset.iter_ones().map(|i| i as u32) {
                    let dist: OrderedFloat = dist_calc.distance(node_id).into();
                    if dist <= lower_bound || dist > upper_bound {
                        continue;
                    }
                    if heap.len() < k {
                        heap.push((dist, node_id).into());
                    } else if dist < heap.peek().unwrap().dist {
                        heap.pop();
                        heap.push((dist, node_id).into());
                    }
                }
            }
        };
        heap.into_sorted_vec()
    }

    /// Returns the metadata of this [`HNSW`].
    pub fn metadata(&self) -> HnswMetadata {
        // Version-1 readers use params.max_level as the number of level
        // batches and index them directly. Preserve that configured shape,
        // padding levels above the sampled graph height with empty ranges.
        let configured_levels = self.inner.params.max_level as usize;
        let mut level_offsets = Vec::with_capacity(configured_levels + 1);
        let mut offset = 0;
        level_offsets.push(0);
        for level in 0..configured_levels {
            let level_count = self.inner.level_count.get(level).copied().unwrap_or(0);
            offset += level_count;
            level_offsets.push(offset);
        }

        HnswMetadata {
            entry_point: self.inner.entry_point,
            params: self.inner.params.clone(),
            level_offsets,
        }
    }
}

struct HnswBuilder {
    params: HnswBuildParams,

    nodes: Arc<Vec<RwLock<GraphBuilderNode>>>,
    level_count: Vec<AtomicUsize>,

    entry_point: u32,

    visited_generator_queue: Arc<ArrayQueue<VisitedGenerator>>,
}

impl DeepSizeOf for HnswBuilder {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.params.deep_size_of_children(context)
            + self.nodes.deep_size_of_children(context)
            + self.level_count.deep_size_of_children(context)
        // Skipping the visited_generator_queue
    }
}

impl HnswBuilder {
    fn finish(self) -> HNSW {
        let nodes: Vec<GraphBuilderNode> = match Arc::try_unwrap(self.nodes) {
            Ok(nodes) => nodes
                .into_iter()
                .map(|node| node.into_inner().expect("builder lock poisoned"))
                .collect(),
            Err(nodes) => nodes
                .iter()
                .map(|node| node.read().expect("builder lock poisoned").clone())
                .collect(),
        };

        let actual_levels = nodes
            .get(self.entry_point as usize)
            .map(|node| node.level_neighbors.len())
            .unwrap_or(0);
        let level_count = self
            .level_count
            .into_iter()
            .take(actual_levels)
            .map(|count| count.load(Ordering::Relaxed))
            .collect();

        HNSW {
            inner: Arc::new(HnswCore {
                params: self.params,
                graph: HnswGraph::Built(Arc::new(nodes)),
                level_count,
                entry_point: self.entry_point,
                visited_generator_queue: self.visited_generator_queue,
            }),
        }
    }

    /// Create a new [`HNSWBuilder`] with prepared params and in memory vector storage.
    pub fn with_params(params: HnswBuildParams, storage: &impl VectorStore) -> Self {
        let len = storage.len();
        let max_level = params.max_level;

        let level_count = (0..max_level)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>();

        let visited_generator_queue = Arc::new(ArrayQueue::new(get_num_compute_intensive_cpus()));
        for _ in 0..get_num_compute_intensive_cpus() {
            visited_generator_queue
                .push(VisitedGenerator::new(0))
                .unwrap();
        }
        let mut builder = Self {
            params,
            nodes: Arc::new(Vec::new()),
            level_count,
            entry_point: 0,
            visited_generator_queue,
        };

        if storage.is_empty() {
            return builder;
        }

        let mut nodes = Vec::with_capacity(len);
        let mut level_rng = SmallRng::seed_from_u64(HNSW_LEVEL_RNG_SEED);
        let mut highest_level = 0;
        for i in 0..len {
            let target_level = random_level_with(&builder.params, &mut level_rng);
            if target_level > highest_level {
                highest_level = target_level;
                builder.entry_point = i as u32;
            }
            nodes.push(RwLock::new(GraphBuilderNode::new(
                i as u32,
                target_level as usize + 1,
            )));
        }
        builder.nodes = Arc::new(nodes);

        builder
    }

    /// Insert one node.
    fn insert(
        &self,
        node: u32,
        visited_generator: &mut VisitedGenerator,
        storage: &impl VectorStore,
    ) {
        let nodes = &self.nodes;
        let target_level = nodes[node as usize].read().unwrap().level_neighbors.len() as u16 - 1;
        let entry_level = nodes[self.entry_point as usize]
            .read()
            .unwrap()
            .level_neighbors
            .len() as u16
            - 1;
        let dist_calc = storage.dist_calculator_from_id(node);
        let mut ep = OrderedNode::new(
            self.entry_point,
            dist_calc.distance(self.entry_point).into(),
        );

        //
        // Search for entry point in paper.
        // ```
        //   for l_c in (L..l+1) {
        //     W = Search-Layer(q, ep, ef=1, l_c)
        //    ep = Select-Neighbors(W, 1)
        //  }
        // ```
        for level in (target_level + 1..=entry_level).rev() {
            let cur_level = HnswLevelView::new(level, nodes);
            ep = greedy_search(&cur_level, ep, &dist_calc, self.params.prefetch_distance);
        }

        let mut pruned_neighbors_per_level: Vec<Vec<_>> =
            vec![Vec::new(); (target_level + 1) as usize];
        {
            let mut current_node = nodes[node as usize].write().unwrap();
            for level in (0..=target_level).rev() {
                self.level_count[level as usize].fetch_add(1, Ordering::Relaxed);

                let neighbors = self.search_level(&ep, level, &dist_calc, nodes, visited_generator);
                for neighbor in &neighbors {
                    current_node.add_neighbor(neighbor.id, neighbor.dist, level);
                }
                // Algorithm 1 selects M neighbors for the new node. Mmax0 is
                // reserved for reciprocal edges on existing level-0 nodes.
                self.prune(storage, &mut current_node, level, self.params.m);
                pruned_neighbors_per_level[level as usize]
                    .clone_from(&current_node.level_neighbors_ranked[level as usize]);

                ep = neighbors[0].clone();
            }
        }
        for (level, pruned_neighbors) in pruned_neighbors_per_level.iter().enumerate() {
            let level = level as u16;
            let reciprocal_limit = if level == 0 {
                self.params.m * 2
            } else {
                self.params.m
            };
            for selected_edge in pruned_neighbors {
                // Algorithm 1 adds the reciprocal candidate before applying
                // SELECT-NEIGHBORS. A distance-only cutoff would incorrectly
                // discard farther candidates that improve directional diversity.
                let mut chosen_node = nodes[selected_edge.id as usize].write().unwrap();
                chosen_node.add_neighbor(node, selected_edge.dist, level);
                self.prune(storage, &mut chosen_node, level, reciprocal_limit);
            }
        }
    }

    fn search_level(
        &self,
        ep: &OrderedNode,
        level: u16,
        dist_calc: &impl DistCalculator,
        nodes: &[RwLock<GraphBuilderNode>],
        visited_generator: &mut VisitedGenerator,
    ) -> Vec<OrderedNode> {
        let cur_level = HnswLevelView::new(level, nodes);
        let mut visited = visited_generator.generate(nodes.len());
        beam_search(
            &cur_level,
            ep,
            &HnswQueryParams {
                ef: self.params.ef_construction,
                lower_bound: None,
                upper_bound: None,
                dist_q_c: 0.0,
                use_acorn: false,
            },
            dist_calc,
            None,
            self.params.prefetch_distance,
            &mut visited,
        )
    }

    fn prune(
        &self,
        storage: &impl VectorStore,
        builder_node: &mut GraphBuilderNode,
        level: u16,
        max_connections: usize,
    ) {
        let neighbors_ranked = &mut builder_node.level_neighbors_ranked[level as usize];
        if neighbors_ranked.len() <= max_connections {
            builder_node.update_from_ranked_neighbors(level);
            return;
        }

        let level_neighbors = std::mem::take(neighbors_ranked);
        *neighbors_ranked =
            select_neighbors_heuristic_owned(storage, level_neighbors, max_connections);
        builder_node.update_from_ranked_neighbors(level);
    }
}

// View of a level in HNSW graph.
// This is used to iterate over neighbors in a specific level.
pub(crate) struct HnswLevelView<'a> {
    level: u16,
    nodes: &'a [RwLock<GraphBuilderNode>],
}

impl<'a> HnswLevelView<'a> {
    pub fn new(level: u16, nodes: &'a [RwLock<GraphBuilderNode>]) -> Self {
        Self { level, nodes }
    }
}

impl Graph for HnswLevelView<'_> {
    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn neighbors(&self, key: u32) -> Arc<Vec<u32>> {
        let node = &self.nodes[key as usize];
        node.read().unwrap().level_neighbors[self.level as usize].clone()
    }
}

pub(crate) struct ImmutableHnswLevelView<'a> {
    level: u16,
    nodes: &'a [GraphBuilderNode],
}

impl<'a> ImmutableHnswLevelView<'a> {
    pub fn new(level: u16, nodes: &'a [GraphBuilderNode]) -> Self {
        Self { level, nodes }
    }
}

impl Graph for ImmutableHnswLevelView<'_> {
    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn neighbors(&self, key: u32) -> Arc<Vec<u32>> {
        self.nodes[key as usize].level_neighbors[self.level as usize].clone()
    }
}

impl BorrowingGraph for ImmutableHnswLevelView<'_> {
    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn neighbors(&self, key: u32) -> &[u32] {
        self.nodes[key as usize].level_neighbors[self.level as usize].as_slice()
    }
}

pub(crate) struct ImmutableHnswBottomView<'a> {
    nodes: &'a [GraphBuilderNode],
}

impl<'a> ImmutableHnswBottomView<'a> {
    pub fn new(nodes: &'a [GraphBuilderNode]) -> Self {
        Self { nodes }
    }
}

impl Graph for ImmutableHnswBottomView<'_> {
    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn neighbors(&self, key: u32) -> Arc<Vec<u32>> {
        self.nodes[key as usize].bottom_neighbors.clone()
    }
}

impl BorrowingGraph for ImmutableHnswBottomView<'_> {
    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn neighbors(&self, key: u32) -> &[u32] {
        self.nodes[key as usize].bottom_neighbors.as_slice()
    }
}

/// Per-level node-id -> row-index lookup for a disk-loaded HNSW graph.
enum LevelLookup {
    /// `row == node id`. Used only for level 0, where [`HNSW::to_batch`]
    /// writes every node once in ascending `__vector_id` (== node id) order,
    /// so the level-0 slice is exactly `[0, N)` with `row == id`.
    Dense,
    /// Upper level: an explicit `node_id -> row` map built from the level's
    /// `__vector_id` column.
    ///
    /// We do *not* assume the column is sorted or that the slice is aligned
    /// to a true level boundary. Indices written before issue #5156 was fixed
    /// omitted the entry point from every upper-level count, so their slices
    /// start progressively earlier than the true boundaries. Keying by the
    /// `__vector_id` value -- exactly what the old per-node `load` did --
    /// preserves their behavior bit-for-bit. Upper levels shrink
    /// geometrically, so this map stays tiny.
    Sparse(HashMap<u32, u32>),
}

/// A search-only HNSW graph backed directly by the Arrow buffers of the
/// on-disk `RecordBatch`.
///
/// Loading performs no per-node reconstruction: neighbor adjacency is served
/// as `&[u32]` slices straight out of the `__neighbors` `ListArray` value
/// buffer (zero copy). The full `batch` is retained so [`HNSW::to_batch`] is a
/// near-free passthrough -- required, because the IVF partition cache
/// re-serializes loaded indices through `to_batch()`
/// (`lance/src/index/vector/ivf/partition_serde.rs`) -- and so a future
/// zero-copy `CacheCodec` (#6745) can write/read it through
/// `lance_arrow::ipc` without rebuilding the graph.
struct LoadedHnswGraph {
    /// The full loaded batch (all levels concatenated, level 0 first),
    /// retained verbatim for `to_batch()` and #6745.
    batch: RecordBatch,
    /// Per-level `__neighbors` `List<UInt32>`, zero-copy slices of `batch`.
    level_neighbors: Vec<ListArray>,
    /// Per-level node-id -> row lookup (see [`LevelLookup`]).
    level_lookup: Vec<LevelLookup>,
    /// Number of nodes present at each level (`level_count[0]` == total).
    level_count: Vec<usize>,
}

impl DeepSizeOf for LoadedHnswGraph {
    fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
        // `level_neighbors` are zero-copy views into `batch`, so counting
        // `batch` alone avoids double counting (mirrors
        // `vector/flat/storage.rs`). The upper-level `level_lookup` maps are
        // sized to the geometrically-shrinking node counts above level 0 --
        // negligible next to the batch and not separately accounted here.
        self.batch.get_array_memory_size()
    }
}

impl LoadedHnswGraph {
    /// Borrow the neighbor ids of `key` at `level` directly from the Arrow
    /// `ListArray` value buffer -- no allocation, no copy.
    #[inline]
    fn neighbors_at(&self, level: usize, key: u32) -> &[u32] {
        let row = match &self.level_lookup[level] {
            LevelLookup::Dense => key as usize,
            LevelLookup::Sparse(id_to_row) => match id_to_row.get(&key) {
                Some(&row) => row as usize,
                // The node is absent at this level -- e.g. an empty upper
                // level the search descends through, or a node that only
                // exists at lower levels. Mirror the old representation
                // (`level_neighbors[level]` defaulted to empty): no
                // neighbors here, so greedy search stays put and descends.
                None => return &[],
            },
        };
        let list = &self.level_neighbors[level];
        let offsets = list.value_offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        // The `__neighbors` list child is `UInt32` per `HNSW::schema()`.
        // Validity bitmap is ignored on purpose: `to_batch` never writes null
        // neighbor lists, matching the previous `.unwrap()`-based load.
        let values = list.values().as_primitive::<UInt32Type>();
        &values.values()[start..end]
    }
}

/// Per-level search view over a disk-loaded [`LoadedHnswGraph`].
pub(crate) struct LoadedHnswLevelView<'a> {
    level: usize,
    graph: &'a LoadedHnswGraph,
}

impl<'a> LoadedHnswLevelView<'a> {
    fn new(level: u16, graph: &'a LoadedHnswGraph) -> Self {
        Self {
            level: level as usize,
            graph,
        }
    }
}

impl Graph for LoadedHnswLevelView<'_> {
    fn len(&self) -> usize {
        // Mirrors `ImmutableHnswLevelView::len` (total node count).
        self.graph.level_count[0]
    }

    fn neighbors(&self, key: u32) -> Arc<Vec<u32>> {
        // Non-hot fallback: HNSW search goes through `BorrowingGraph`. Kept
        // only so the `Graph` trait / legacy `greedy_search` need no
        // special-casing for loaded graphs.
        Arc::new(self.graph.neighbors_at(self.level, key).to_vec())
    }
}

impl BorrowingGraph for LoadedHnswLevelView<'_> {
    fn len(&self) -> usize {
        self.graph.level_count[0]
    }

    fn neighbors(&self, key: u32) -> &[u32] {
        self.graph.neighbors_at(self.level, key)
    }
}

/// Bottom-level (level 0) search view over a disk-loaded [`LoadedHnswGraph`].
pub(crate) struct LoadedHnswBottomView<'a> {
    graph: &'a LoadedHnswGraph,
}

impl<'a> LoadedHnswBottomView<'a> {
    fn new(graph: &'a LoadedHnswGraph) -> Self {
        Self { graph }
    }
}

impl Graph for LoadedHnswBottomView<'_> {
    fn len(&self) -> usize {
        self.graph.level_count[0]
    }

    fn neighbors(&self, key: u32) -> Arc<Vec<u32>> {
        Arc::new(self.graph.neighbors_at(0, key).to_vec())
    }
}

impl BorrowingGraph for LoadedHnswBottomView<'_> {
    fn len(&self) -> usize {
        self.graph.level_count[0]
    }

    fn neighbors(&self, key: u32) -> &[u32] {
        self.graph.neighbors_at(0, key)
    }
}

/// The graph backing an [`HNSW`]: either built in memory or disk-loaded.
enum HnswGraph {
    /// Built in memory by the (online) builder / `index_vectors` /
    /// `from_parts`. Mutable-shaped `GraphBuilderNode`s; `to_batch()`
    /// re-encodes from these (it needs the per-node ranked distances).
    Built(Arc<Vec<GraphBuilderNode>>),
    /// Loaded from disk, Arrow-backed, search-only.
    Loaded(Arc<LoadedHnswGraph>),
}

impl DeepSizeOf for HnswGraph {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        match self {
            Self::Built(nodes) => nodes.deep_size_of_children(context),
            Self::Loaded(graph) => graph.deep_size_of_children(context),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HnswQueryParams {
    pub ef: usize,
    pub lower_bound: Option<f32>,
    pub upper_bound: Option<f32>,
    pub dist_q_c: f32,
    pub use_acorn: bool,
}

impl From<&Query> for HnswQueryParams {
    fn from(query: &Query) -> Self {
        let k = query.k * query.refine_factor.unwrap_or(1) as usize;
        Self {
            ef: query.ef.unwrap_or(k + k / 2),
            lower_bound: query.lower_bound,
            upper_bound: query.upper_bound,
            dist_q_c: query.dist_q_c,
            use_acorn: query.approx_mode == ApproxMode::Fast,
        }
    }
}

impl IvfSubIndex for HNSW {
    type BuildParams = HnswBuildParams;
    type QueryParams = HnswQueryParams;

    fn load(data: RecordBatch) -> Result<Self>
    where
        Self: Sized,
    {
        if data.num_rows() == 0 {
            return Ok(Self::empty());
        }

        let hnsw_metadata = data
            .schema_ref()
            .metadata()
            .get(HNSW_METADATA_KEY)
            .ok_or(Error::index(format!("{} not found", HNSW_METADATA_KEY)))?;
        let hnsw_metadata: HnswMetadata = serde_json::from_str(hnsw_metadata).map_err(|e| {
            Error::index(format!(
                "Failed to decode HNSW metadata: {}, json: {}",
                e, hnsw_metadata
            ))
        })?;

        // Slice the concatenated batch into one (zero-copy) view per level.
        let level_batches: Vec<RecordBatch> = hnsw_metadata
            .level_offsets
            .iter()
            .tuple_windows()
            .map(|(start, end)| data.slice(*start, end - start))
            .collect();

        let level_count = level_batches
            .iter()
            .map(|b| b.num_rows())
            .collect::<Vec<_>>();

        // No per-node reconstruction: keep the Arrow adjacency buffers as-is
        // and only build the tiny per-upper-level id->row lookups. The
        // `__distance` column is never materialized here -- search doesn't
        // need it, and `to_batch()` returns the retained `data` verbatim.
        let mut level_neighbors = Vec::with_capacity(level_batches.len());
        let mut level_lookup = Vec::with_capacity(level_batches.len());
        for (level, batch) in level_batches.iter().enumerate() {
            // `.clone()` on an Arrow array bumps a refcount; buffers stay
            // shared with `data` (zero copy).
            let neighbors = batch[NEIGHBORS_COL].as_list::<i32>().clone();
            let ids = batch[VECTOR_ID_COL].as_primitive::<UInt32Type>();
            if level == 0 {
                // `to_batch` writes every node at level 0 exactly once in
                // ascending `__vector_id` (== node id) order, so the level-0
                // slice is exactly `[0, N)` and the row index *is* the node
                // id. The `Dense` lookup below depends on this: in a release
                // build a violated invariant would silently make search read
                // the wrong neighbor list, so enforce it at load time (not via
                // `debug_assert!`) and reject a malformed or version-
                // incompatible batch.
                if let Some((row, id)) = ids
                    .values()
                    .iter()
                    .enumerate()
                    .find(|&(row, id)| *id != row as u32)
                {
                    return Err(Error::index(format!(
                        "HNSW level-0 __vector_id must equal the row index, but \
                         row {row} has __vector_id {id}; the on-disk batch is \
                         malformed or was written by an incompatible version"
                    )));
                }
                level_lookup.push(LevelLookup::Dense);
            } else {
                // Upper levels: explicit id -> row map. No ordering/alignment
                // assumption (see `LevelLookup::Sparse`). On the rare
                // duplicate id (a misaligned slice can repeat one across a
                // level boundary) the last wins, matching the old load's
                // `nodes[id].level_neighbors[level] = ...` last-write.
                let id_to_row: HashMap<u32, u32> = ids
                    .values()
                    .iter()
                    .enumerate()
                    .map(|(row, id)| (*id, row as u32))
                    .collect();
                level_lookup.push(LevelLookup::Sparse(id_to_row));
            }
            level_neighbors.push(neighbors);
        }

        // `entry_point` is read from untrusted metadata and indexes the `Dense`
        // level-0 lookup directly; an out-of-range value would read past the
        // level-0 neighbor buffer during search. Validate it under the same
        // persisted-format invariant as the level-0 ids above.
        let num_nodes = level_count[0];
        if hnsw_metadata.entry_point as usize >= num_nodes {
            return Err(Error::index(format!(
                "HNSW entry_point {} is out of range for a graph with {num_nodes} \
                 nodes; the on-disk batch is malformed or was written by an \
                 incompatible version",
                hnsw_metadata.entry_point
            )));
        }

        let visited_generator_queue =
            Arc::new(ArrayQueue::new(get_num_compute_intensive_cpus() * 2));
        for _ in 0..get_num_compute_intensive_cpus() * 2 {
            visited_generator_queue
                .push(VisitedGenerator::new(0))
                .unwrap();
        }

        let graph = LoadedHnswGraph {
            batch: data,
            level_neighbors,
            level_lookup,
            level_count: level_count.clone(),
        };
        let inner = HnswCore {
            params: hnsw_metadata.params,
            graph: HnswGraph::Loaded(Arc::new(graph)),
            level_count,
            entry_point: hnsw_metadata.entry_point,
            visited_generator_queue,
        };

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    fn name() -> &'static str {
        HNSW_TYPE
    }

    fn metadata_key() -> &'static str {
        "lance:hnsw"
    }

    /// Return the schema of the sub index
    fn schema() -> arrow_schema::SchemaRef {
        arrow_schema::Schema::new(vec![
            VECTOR_ID_FIELD.clone(),
            NEIGHBORS_FIELD.clone(),
            DISTS_FIELD.clone(),
        ])
        .into()
    }

    // `schema()` governs the on-disk index file, and readers older than v8.0.0
    // index the distance column there unconditionally, panicking when it is
    // absent, so it cannot be dropped from what we write. Skipping it on read
    // costs nothing in compatibility and keeps most of the graph bytes off the
    // wire.
    fn read_columns() -> Option<&'static [&'static str]> {
        Some(&[VECTOR_ID_COL, NEIGHBORS_COL])
    }

    #[instrument(level = "debug", skip(self, query, storage, prefilter, _metrics))]
    fn search(
        &self,
        query: ArrayRef,
        k: usize,
        params: Self::QueryParams,
        storage: &impl VectorStore,
        prefilter: Arc<dyn PreFilter>,
        _metrics: &dyn MetricsCollector,
    ) -> Result<RecordBatch> {
        if params.ef < k {
            return Err(Error::index(
                "ef must be greater than or equal to k".to_string(),
            ));
        }

        let schema = VECTOR_RESULT_SCHEMA.clone();
        if self.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }

        let mut prefilter_generator = self
            .inner
            .visited_generator_queue
            .pop()
            .unwrap_or_else(|| VisitedGenerator::new(storage.len()));
        let results = if prefilter.is_empty() {
            self.search_basic(query, k, &params, None, storage)?
        } else {
            // the bitset must be moved into a callee on every path so its
            // borrow of `prefilter_generator` ends before the push below
            let indices = prefilter.filter_row_ids(Box::new(storage.row_ids()));
            let mut prefilter_bitset = prefilter_generator.generate(storage.len());
            for index in indices {
                prefilter_bitset.insert(index as u32);
            }
            let remained = prefilter_bitset.count_ones();
            if remained == storage.len() {
                // mask passes every row: same as unfiltered
                drop(prefilter_bitset);
                self.search_basic(query, k, &params, None, storage)?
            } else if remained < self.len() * 10 / 100 {
                // few matching rows: brute force is cheaper and exact
                self.flat_search(storage, query, k, prefilter_bitset, &params)
            } else if params.use_acorn {
                let acorn_results =
                    self.search_acorn(query.clone(), k, &params, &prefilter_bitset, storage)?;
                // under-delivery means the budget ran out on a fragmented
                // mask, except range-bounded queries which return short
                // legitimately
                let bounded = params.lower_bound.is_some() || params.upper_bound.is_some();
                if !bounded && acorn_results.len() < k.min(remained) {
                    self.search_basic(query, k, &params, Some(prefilter_bitset), storage)?
                } else {
                    drop(prefilter_bitset);
                    acorn_results
                }
            } else {
                self.search_basic(query, k, &params, Some(prefilter_bitset), storage)?
            }
        };
        // if the queue is full, we just don't push it back, so ignore the error here
        let _ = self.inner.visited_generator_queue.push(prefilter_generator);

        // need to unique by row ids in case of searching multivector
        let (row_ids, dists): (Vec<_>, Vec<_>) = results
            .into_iter()
            .map(|r| (storage.row_id(r.id), r.dist.0))
            .unique_by(|r| r.0)
            .unzip();
        let row_ids = Arc::new(UInt64Array::from(row_ids));
        let distances = Arc::new(Float32Array::from(dists));

        Ok(RecordBatch::try_new(schema, vec![distances, row_ids])?)
    }

    /// Given a vector storage, containing all the data for the IVF partition, build the sub index.
    fn index_vectors(storage: &impl VectorStore, params: Self::BuildParams) -> Result<Self>
    where
        Self: Sized,
    {
        params.validate()?;
        let builder = HnswBuilder::with_params(params, storage);

        log::debug!(
            "Building HNSW graph: num={}, max_levels={}, m={}, ef_construction={}, distance_type:{}",
            storage.len(),
            builder.params.max_level,
            builder.params.m,
            builder.params.ef_construction,
            storage.distance_type(),
        );

        if storage.is_empty() {
            return Ok(builder.finish());
        }

        let len = storage.len();
        let entry_levels = builder.nodes[builder.entry_point as usize]
            .read()
            .unwrap()
            .level_neighbors
            .len();
        for count in builder.level_count.iter().take(entry_levels) {
            count.fetch_add(1, Ordering::Relaxed);
        }
        (0..len)
            .into_par_iter()
            .filter(|node| *node as u32 != builder.entry_point)
            .for_each_init(
                || VisitedGenerator::new(len),
                |visited_generator, node| {
                    builder.insert(node as u32, visited_generator, storage);
                },
            );

        assert_eq!(builder.level_count[0].load(Ordering::Relaxed), len);
        Ok(builder.finish())
    }

    fn remap(
        &self,
        _mapping: &RowAddrRemap, // we don't need the mapping here because we rebuild the graph from remapped storage
        store: &impl VectorStore,
    ) -> Result<Self> {
        // We can't simply remap the row ids in the graph because the vectors are changed,
        // so the graph needs to be rebuilt.
        Self::index_vectors(store, self.inner.params.clone())
    }

    /// Encode the sub index into a record batch
    fn to_batch(&self) -> Result<RecordBatch> {
        let nodes = match &self.inner.graph {
            HnswGraph::Built(nodes) => nodes,
            HnswGraph::Loaded(graph) => {
                // A loaded graph is already Arrow-backed: return the retained
                // batch verbatim, re-stamped with up-to-date HNSW metadata.
                // The IVF partition cache re-serializes loaded indices through
                // here (`ivf/partition_serde.rs`), so this must round-trip.
                //
                // Merge into (not replace) the existing schema metadata: a
                // disk-loaded batch inherits other keys from the index file
                // schema (e.g. `INDEX_METADATA_SCHEMA_KEY`, `IVF_METADATA_KEY`),
                // and `RecordBatch::with_schema` requires the new metadata to be
                // a superset of the current one. Dropping those keys here makes
                // the new schema a non-superset and fails the round-trip with
                // "target schema is not superset of current schema".
                let metadata = serde_json::to_string(&self.metadata())?;
                let mut schema_metadata = graph.batch.schema_ref().metadata().clone();
                schema_metadata.insert(HNSW_METADATA_KEY.to_string(), metadata);
                let schema = graph
                    .batch
                    .schema()
                    .as_ref()
                    .clone()
                    .with_metadata(schema_metadata);
                return Ok(graph.batch.clone().with_schema(Arc::new(schema))?);
            }
        };

        let mut vector_id_builder = UInt32Builder::with_capacity(self.len());
        let mut neighbors_builder = ListBuilder::with_capacity(UInt32Builder::new(), self.len());
        let mut distances_builder =
            ListBuilder::with_capacity(arrow_array::builder::Float32Builder::new(), self.len());
        let mut batches = Vec::with_capacity(self.max_level() as usize);
        for level in 0..self.max_level() {
            let level = level as usize;
            for (id, node) in nodes.iter().enumerate() {
                if level >= node.level_neighbors.len() {
                    continue;
                }
                let neighbors = node.level_neighbors[level].iter().map(|n| Some(*n));
                let distances = node.level_neighbors_ranked[level]
                    .iter()
                    .map(|n| Some(n.dist.0));
                vector_id_builder.append_value(id as u32);
                neighbors_builder.append_value(neighbors);
                distances_builder.append_value(distances);
            }

            let batch = RecordBatch::try_new(
                Self::schema(),
                vec![
                    Arc::new(vector_id_builder.finish()),
                    Arc::new(neighbors_builder.finish()),
                    Arc::new(distances_builder.finish()),
                ],
            )?;
            batches.push(batch);
        }

        let metadata = self.metadata();
        let metadata = serde_json::to_string(&metadata)?;
        let schema = Self::schema()
            .as_ref()
            .clone()
            .with_metadata(HashMap::from_iter(vec![(
                HNSW_METADATA_KEY.to_string(),
                metadata,
            )]));
        let batch = concat_batches(&Self::schema(), batches.iter())?;
        let batch = batch.with_schema(Arc::new(schema))?;
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow_array::{
        ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt8Array, UInt32Array,
    };
    use arrow_schema::Schema;
    use lance_arrow::FixedSizeListArrayExt;
    use lance_core::{Error, deepsize::DeepSizeOf};
    use lance_file::versions::v1::{
        reader::FileReader as V1FileReader,
        writer::{FileWriter as V1FileWriter, FileWriterOptions as V1FileWriterOptions},
    };
    use lance_io::object_store::ObjectStore;
    use lance_linalg::distance::DistanceType;
    use lance_table::format::SelfDescribingFileReader;
    use lance_table::io::manifest::ManifestDescribing;
    use lance_testing::datagen::generate_random_array;
    use object_store::path::Path;
    use rand::{Rng, SeedableRng, rngs::SmallRng};
    use rstest::rstest;

    use super::{
        HNSW_LEVEL_RNG_SEED, HNSW_METADATA_KEY, HnswBuilder, HnswGraph, ImmutableHnswBottomView,
        ImmutableHnswLevelView, MIN_HNSW_M, random_level_with,
    };
    use crate::vector::graph::builder::GraphBuilderNode;
    use crate::vector::storage::{DistCalculator, VectorStore};
    use crate::vector::v3::subindex::IvfSubIndex;
    use crate::vector::{
        flat::storage::{FlatBinStorage, FlatFloatStorage},
        graph::{DISTS_FIELD, NEIGHBORS_FIELD, OrderedNode, VisitedGenerator},
        hnsw::{
            HNSW, HnswMetadata, VECTOR_ID_FIELD,
            builder::{HnswBuildParams, HnswQueryParams},
        },
    };

    fn with_hnsw_metadata(batch: &RecordBatch, hnsw_metadata: HnswMetadata) -> RecordBatch {
        let mut metadata = batch.schema_ref().metadata().clone();
        metadata.insert(
            HNSW_METADATA_KEY.to_string(),
            serde_json::to_string(&hnsw_metadata).unwrap(),
        );
        let schema = batch.schema().as_ref().clone().with_metadata(metadata);
        RecordBatch::try_new(Arc::new(schema), batch.columns().to_vec()).unwrap()
    }

    #[tokio::test]
    async fn test_builder_write_load() {
        const DIM: usize = 32;
        const TOTAL: usize = 2048;
        const NUM_EDGES: usize = 20;
        let data = generate_random_array(TOTAL * DIM);
        let fsl = FixedSizeListArray::try_new_from_values(data, DIM as i32).unwrap();
        let store = Arc::new(FlatFloatStorage::new(fsl.clone(), DistanceType::L2));
        let builder = HNSW::index_vectors(
            store.as_ref(),
            HnswBuildParams::default()
                .num_edges(NUM_EDGES)
                .ef_construction(50),
        )
        .unwrap();

        let object_store = ObjectStore::memory();
        let path = Path::from("test_builder_write_load");
        let writer = object_store.create(&path).await.unwrap();
        let schema = Schema::new(vec![
            VECTOR_ID_FIELD.clone(),
            NEIGHBORS_FIELD.clone(),
            DISTS_FIELD.clone(),
        ]);
        let schema = lance_core::datatypes::Schema::try_from(&schema).unwrap();
        let mut writer = V1FileWriter::<ManifestDescribing>::with_object_writer(
            writer,
            schema,
            &V1FileWriterOptions::default(),
        )
        .unwrap();
        let batch = builder.to_batch().unwrap();
        let metadata = batch.schema_ref().metadata().clone();
        writer.write(&[batch]).await.unwrap();
        writer.finish_with_metadata(&metadata).await.unwrap();

        let reader = V1FileReader::try_new_self_described(&object_store, &path, None)
            .await
            .unwrap();
        let batch = reader
            .read_range(0..reader.len(), reader.schema())
            .await
            .unwrap();
        let loaded_hnsw = HNSW::load(batch).unwrap();

        let query = fsl.value(0);
        let k = 10;
        let params = HnswQueryParams {
            ef: 50,
            lower_bound: None,
            upper_bound: None,
            dist_q_c: 0.0,
            use_acorn: false,
        };
        let builder_results = builder
            .search_basic(query.clone(), k, &params, None, store.as_ref())
            .unwrap();
        let loaded_results = loaded_hnsw
            .search_basic(query, k, &params, None, store.as_ref())
            .unwrap();
        assert_eq!(builder_results, loaded_results);
    }

    #[tokio::test]
    async fn test_builder_write_load_binary_hamming() {
        const DIM: usize = 8;
        const TOTAL: usize = 256;
        const NUM_EDGES: usize = 20;
        let data = UInt8Array::from_iter_values((0..TOTAL * DIM).map(|v| (v % 16) as u8));
        let fsl = FixedSizeListArray::try_new_from_values(data, DIM as i32).unwrap();
        let store = Arc::new(FlatBinStorage::new(fsl.clone(), DistanceType::Hamming));
        let builder = HnswBuildParams::default()
            .num_edges(NUM_EDGES)
            .ef_construction(50)
            .build(Arc::new(fsl.clone()), DistanceType::Hamming)
            .await
            .unwrap();

        let object_store = ObjectStore::memory();
        let path = Path::from("test_builder_write_load_binary_hamming");
        let writer = object_store.create(&path).await.unwrap();
        let schema = Schema::new(vec![
            VECTOR_ID_FIELD.clone(),
            NEIGHBORS_FIELD.clone(),
            DISTS_FIELD.clone(),
        ]);
        let schema = lance_core::datatypes::Schema::try_from(&schema).unwrap();
        let mut writer = V1FileWriter::<ManifestDescribing>::with_object_writer(
            writer,
            schema,
            &V1FileWriterOptions::default(),
        )
        .unwrap();
        let batch = builder.to_batch().unwrap();
        let metadata = batch.schema_ref().metadata().clone();
        writer.write(&[batch]).await.unwrap();
        writer.finish_with_metadata(&metadata).await.unwrap();

        let reader = V1FileReader::try_new_self_described(&object_store, &path, None)
            .await
            .unwrap();
        let batch = reader
            .read_range(0..reader.len(), reader.schema())
            .await
            .unwrap();
        let loaded_hnsw = HNSW::load(batch).unwrap();

        let query = fsl.value(0);
        let k = 10;
        let params = HnswQueryParams {
            ef: 50,
            lower_bound: None,
            upper_bound: None,
            dist_q_c: 0.0,
            use_acorn: false,
        };
        let builder_results = builder
            .search_basic(query.clone(), k, &params, None, store.as_ref())
            .unwrap();
        let loaded_results = loaded_hnsw
            .search_basic(query, k, &params, None, store.as_ref())
            .unwrap();
        assert_eq!(builder_results, loaded_results);
    }

    /// Brute-force top-`k` node ids by distance -- recall ground truth.
    fn brute_force_topk(store: &FlatFloatStorage, query: ArrayRef, k: usize) -> Vec<u32> {
        let dist_calc = store.dist_calculator(query, 0.0);
        let mut all: Vec<(f32, u32)> = (0..store.len() as u32)
            .map(|id| (dist_calc.distance(id), id))
            .collect();
        all.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        all.into_iter().take(k).map(|(_, id)| id).collect()
    }

    #[rstest]
    #[case::zero_max_level(
        HnswBuildParams::default().max_level(0),
        "max_level must be greater than 0"
    )]
    #[case::zero_m(
        HnswBuildParams::default().num_edges(0),
        "m must be at least 4"
    )]
    #[case::one_m(
        HnswBuildParams::default().num_edges(1),
        "m must be at least 4"
    )]
    #[case::three_m(
        HnswBuildParams::default().num_edges(3),
        "m must be at least 4"
    )]
    #[case::small_ef(
        HnswBuildParams::default().num_edges(20).ef_construction(19),
        "ef_construction must be at least m (20)"
    )]
    #[case::overflowing_level_zero_limit(
        HnswBuildParams::default()
            .num_edges(usize::MAX)
            .ef_construction(usize::MAX),
        "level-0 reciprocal limit can be represented"
    )]
    fn test_rejects_invalid_build_params(
        #[case] params: HnswBuildParams,
        #[case] expected_message: &str,
    ) {
        let fsl =
            FixedSizeListArray::try_new_from_values(Float32Array::from(vec![0.0, 0.0]), 2).unwrap();
        let store = FlatFloatStorage::new(fsl, DistanceType::L2);

        let error = HNSW::index_vectors(&store, params).unwrap_err();
        assert!(matches!(error, Error::InvalidInput { .. }));
        assert!(
            error.to_string().contains(expected_message),
            "unexpected error: {error}"
        );
    }

    /// The lowest accepted construction settings retain a useful graph, but
    /// do not promise full reachability. This seeded floor makes that quality
    /// trade-off explicit and guards against catastrophic fragmentation.
    #[test]
    fn test_minimum_params_reachability() {
        const DIM: usize = 32;
        const TOTAL: usize = 2048;
        let mut rng = SmallRng::seed_from_u64(0);
        let values = Float32Array::from(
            (0..TOTAL * DIM)
                .map(|_| rng.random::<f32>())
                .collect::<Vec<_>>(),
        );
        let vectors = FixedSizeListArray::try_new_from_values(values, DIM as i32).unwrap();
        let store = FlatFloatStorage::new(vectors.clone(), DistanceType::L2);
        let hnsw = HNSW::index_vectors(
            &store,
            HnswBuildParams::default()
                .num_edges(MIN_HNSW_M)
                .ef_construction(MIN_HNSW_M),
        )
        .unwrap();
        let results = hnsw
            .search_basic(
                vectors.value(0),
                TOTAL,
                &HnswQueryParams {
                    ef: TOTAL,
                    lower_bound: None,
                    upper_bound: None,
                    dist_q_c: 0.0,
                    use_acorn: false,
                },
                None,
                &store,
            )
            .unwrap();

        let minimum_reachable = TOTAL * 90 / 100;
        assert!(
            results.len() >= minimum_reachable,
            "minimum HNSW construction settings reached only {} of {TOTAL} nodes; expected at least {minimum_reachable}",
            results.len(),
        );
    }

    /// Algorithm 1 limits a newly inserted node to M connections even on
    /// level 0. The larger Mmax0 limit only applies when old nodes receive
    /// reciprocal connections.
    #[test]
    fn test_new_node_uses_m_connections() {
        // Four equidistant, mutually diverse points around the final point.
        // With the old shared Mmax0 limit, node 4 retained all four.
        let values = Float32Array::from(vec![
            1.0, 0.0, // east
            0.0, 1.0, // north
            -1.0, 0.0, // west
            0.0, -1.0, // south
            0.0, 0.0, // final node
        ]);
        let fsl = FixedSizeListArray::try_new_from_values(values, 2).unwrap();
        let store = FlatFloatStorage::new(fsl, DistanceType::L2);
        let params = HnswBuildParams::default()
            .max_level(1)
            .num_edges(2)
            .ef_construction(5);
        let builder = HnswBuilder::with_params(params, &store);
        let mut visited_generator = VisitedGenerator::new(store.len());

        for node in 1..store.len() as u32 {
            builder.insert(node, &mut visited_generator, &store);
        }

        let final_node = builder.nodes[4].read().unwrap();
        assert_eq!(final_node.level_neighbors_ranked[0].len(), 2);
        assert!(
            builder
                .nodes
                .iter()
                .all(|node| node.read().unwrap().level_neighbors_ranked[0].len() <= 4),
            "existing level-0 nodes must remain bounded by Mmax0"
        );
    }

    /// Offline construction pre-assigns the same random node heights as the
    /// online builder and uses the first globally highest node as the anchor
    /// for parallel insertion. This matches the final entry point produced by
    /// sequential dynamic promotion without forcing node 0 to full height.
    #[test]
    fn test_offline_entry_point_uses_random_node_levels() {
        const TOTAL: usize = 2048;
        let fsl =
            FixedSizeListArray::try_new_from_values(generate_random_array(TOTAL * 2), 2).unwrap();
        let store = FlatFloatStorage::new(fsl, DistanceType::L2);
        let params = HnswBuildParams::default();
        let builder = HnswBuilder::with_params(params.clone(), &store);

        let mut level_rng = SmallRng::seed_from_u64(HNSW_LEVEL_RNG_SEED);
        let expected_levels = (0..TOTAL)
            .map(|_| random_level_with(&params, &mut level_rng))
            .collect::<Vec<_>>();
        let highest_level = *expected_levels.iter().max().unwrap();
        let expected_entry = expected_levels
            .iter()
            .position(|level| *level == highest_level)
            .unwrap() as u32;

        for (node, expected_level) in builder.nodes.iter().zip(expected_levels) {
            assert_eq!(
                node.read().unwrap().level_neighbors.len(),
                expected_level as usize + 1
            );
        }
        assert_eq!(builder.entry_point, expected_entry);
    }

    /// The Arrow-backed loaded graph must search bit-identically to the
    /// in-memory build, across distance types and graph sizes (single node,
    /// pair, and a multi-level graph exercising the sparse upper-level
    /// id->row lookup).
    #[rstest]
    #[case::l2_single(DistanceType::L2, 1)]
    #[case::l2_pair(DistanceType::L2, 2)]
    #[case::l2_multi_level(DistanceType::L2, 2048)]
    #[case::dot_multi_level(DistanceType::Dot, 2048)]
    #[tokio::test]
    async fn test_loaded_search_parity_and_recall(
        #[case] distance_type: DistanceType,
        #[case] total: usize,
    ) {
        const DIM: usize = 32;
        let fsl =
            FixedSizeListArray::try_new_from_values(generate_random_array(total * DIM), DIM as i32)
                .unwrap();
        let store = Arc::new(FlatFloatStorage::new(fsl.clone(), distance_type));
        let builder = HNSW::index_vectors(
            store.as_ref(),
            HnswBuildParams::default().num_edges(20).ef_construction(50),
        )
        .unwrap();
        assert!(!matches!(builder.inner.graph, HnswGraph::Loaded(_)));

        let loaded = HNSW::load(builder.to_batch().unwrap()).unwrap();
        assert!(matches!(loaded.inner.graph, HnswGraph::Loaded(_)));
        assert_eq!(loaded.len(), total);

        let k = total.min(10);
        let params = HnswQueryParams {
            ef: 50,
            lower_bound: None,
            upper_bound: None,
            dist_q_c: 0.0,
            use_acorn: false,
        };
        let query = fsl.value(0);

        let builder_results = builder
            .search_basic(query.clone(), k, &params, None, store.as_ref())
            .unwrap();
        let loaded_results = loaded
            .search_basic(query.clone(), k, &params, None, store.as_ref())
            .unwrap();
        assert_eq!(builder_results, loaded_results);

        // Recall vs brute-force ground truth (project rule: >= 0.5).
        let truth: std::collections::HashSet<u32> = brute_force_topk(store.as_ref(), query, k)
            .into_iter()
            .collect();
        let hits = loaded_results
            .iter()
            .filter(|n| truth.contains(&n.id))
            .count();
        let recall = hits as f32 / k as f32;
        assert!(recall >= 0.5, "recall {recall} below 0.5 (k={k})");
    }

    /// A [`DistCalculator`] over fixed per-node distances that counts how
    /// many distances it computes, so a test can assert exactly which search
    /// phases ran.
    struct CountingDistCalculator<'a> {
        distances: &'a [f32],
        calls: &'a AtomicUsize,
    }

    impl DistCalculator for CountingDistCalculator<'_> {
        fn distance(&self, id: u32) -> f32 {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.distances[id as usize]
        }

        fn distance_all(&self, _k_hint: usize) -> Vec<f32> {
            self.distances.to_vec()
        }
    }

    /// Regression test for <https://github.com/lance-format/lance/issues/5208>:
    /// the query-time greedy descent must stop at level 1 (HNSW paper,
    /// Algorithm 2); level 0 must be searched only by the ef-bounded beam.
    ///
    /// Drives [`HNSW::run_search`] / [`HNSW::run_search_acorn`] directly over
    /// a hand-built graph: node 0 is the entry point and the only node at
    /// level 1 (with no level-1 neighbors), so the greedy descent over
    /// levels >= 1 computes no distances, and with ef == N the bottom beam
    /// visits every level-0 node exactly once. A greedy step at level 0
    /// would show up as 10 extra distance computations: the level-0 degrees
    /// along the path are 1,2,2,2,2,1 and the strictly decreasing distances
    /// make greedy walk it end to end.
    #[test]
    fn test_greedy_descent_stops_before_level_0() {
        const N: usize = 6;
        // Strictly decreasing along the path; the true top-3 is nodes 5,4,3.
        let distances: Vec<f32> = (0..N).map(|id| (N - id) as f32).collect();

        // Level 0 is the path 0-1-...-5, mirrored into both the bottom view
        // and the level-0 view (only the bottom view may be used at query
        // time, which is what this test asserts).
        let mut nodes: Vec<GraphBuilderNode> = (0..N as u32)
            .map(|id| GraphBuilderNode::new(id, 2))
            .collect();
        for (id, node) in nodes.iter_mut().enumerate() {
            let mut adjacency = Vec::new();
            if id > 0 {
                adjacency.push(id as u32 - 1);
            }
            if id + 1 < N {
                adjacency.push(id as u32 + 1);
            }
            let adjacency = Arc::new(adjacency);
            node.bottom_neighbors = adjacency.clone();
            node.level_neighbors[0] = adjacency;
        }

        let build_params = HnswBuildParams {
            max_level: 2,
            m: 4,
            ef_construction: 10,
            prefetch_distance: None,
        };
        let hnsw = HNSW::from_parts(build_params, nodes.clone(), vec![N, 1], 0);
        let query_params = HnswQueryParams {
            ef: N,
            lower_bound: None,
            upper_bound: None,
            dist_q_c: 0.0,
            use_acorn: false,
        };

        let calls = AtomicUsize::new(0);
        let dist_calc = CountingDistCalculator {
            distances: &distances,
            calls: &calls,
        };
        // The entry-point distance (1 call) mirrors `search_inner`.
        let ep = OrderedNode::new(0, dist_calc.distance(0).into());
        let mut visited_generator = VisitedGenerator::new(N);

        let results = hnsw.run_search(
            ep.clone(),
            3,
            &query_params,
            None,
            &mut visited_generator,
            N,
            None,
            &dist_calc,
            |level| ImmutableHnswLevelView::new(level, &nodes),
            ImmutableHnswBottomView::new(&nodes),
        );
        assert_eq!(
            results.iter().map(|node| node.id).collect::<Vec<_>>(),
            vec![5, 4, 3]
        );
        // Exactly 1 entry-point distance + 5 beam visits (one per remaining
        // node); the pre-fix level-0 greedy step brought this to 16.
        assert_eq!(calls.load(Ordering::Relaxed), N);

        // The ACORN traversal shares the descent. Its beam additionally seeds
        // mask-passing nodes with distance computations, so only bound the
        // total: a level-0 greedy step would push it past one call per node.
        let mut mask_generator = VisitedGenerator::new(N);
        let mut mask = mask_generator.generate(N);
        for id in 0..N as u32 {
            mask.insert(id);
        }
        let mut expanded_generator = VisitedGenerator::new(N);
        calls.store(0, Ordering::Relaxed);
        let results = hnsw.run_search_acorn(
            ep,
            &query_params,
            &mask,
            &mut visited_generator,
            &mut expanded_generator,
            N,
            None,
            &dist_calc,
            |level| ImmutableHnswLevelView::new(level, &nodes),
            ImmutableHnswBottomView::new(&nodes),
        );
        assert_eq!(
            results
                .iter()
                .take(3)
                .map(|node| node.id)
                .collect::<Vec<_>>(),
            vec![5, 4, 3]
        );
        assert!(
            calls.load(Ordering::Relaxed) <= N,
            "level-0 greedy descent adds distance computations beyond the \
             beam's one per node"
        );
    }

    /// Brute-force top-`k` restricted to mask-passing ids.
    fn brute_force_topk_masked(
        store: &FlatFloatStorage,
        query: ArrayRef,
        k: usize,
        passes: impl Fn(u32) -> bool,
    ) -> Vec<u32> {
        let dist_calc = store.dist_calculator(query, 0.0);
        let mut matching: Vec<(f32, u32)> = (0..store.len() as u32)
            .filter(|id| passes(*id))
            .map(|id| (dist_calc.distance(id), id))
            .collect();
        matching.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        matching.into_iter().take(k).map(|(_, id)| id).collect()
    }

    /// ACORN returns only mask-passing nodes, searches built and loaded
    /// graphs identically, and holds recall vs brute force over the mask.
    #[tokio::test]
    async fn test_acorn_filtered_search() {
        const DIM: usize = 32;
        const TOTAL: usize = 2048;
        let fsl =
            FixedSizeListArray::try_new_from_values(generate_random_array(TOTAL * DIM), DIM as i32)
                .unwrap();
        let store = Arc::new(FlatFloatStorage::new(fsl.clone(), DistanceType::L2));
        let builder = HNSW::index_vectors(
            store.as_ref(),
            HnswBuildParams::default().num_edges(20).ef_construction(50),
        )
        .unwrap();
        let loaded = HNSW::load(builder.to_batch().unwrap()).unwrap();

        let mut mask_generator = VisitedGenerator::new(TOTAL);
        let k = 10;
        let params = HnswQueryParams {
            ef: 50,
            lower_bound: None,
            upper_bound: None,
            dist_q_c: 0.0,
            use_acorn: false,
        };
        let query = fsl.value(0);
        let truth: std::collections::HashSet<u32> =
            brute_force_topk_masked(store.as_ref(), query.clone(), k, |id| id % 2 == 0)
                .into_iter()
                .collect();

        let mut all_results = vec![];
        for hnsw in [&builder, &loaded] {
            let mut bitset = mask_generator.generate(TOTAL);
            for id in (0..TOTAL as u32).step_by(2) {
                bitset.insert(id);
            }
            let results = hnsw
                .search_acorn(query.clone(), k, &params, &bitset, store.as_ref())
                .unwrap();
            assert_eq!(results.len(), k);
            assert!(results.iter().all(|node| node.id % 2 == 0));
            assert!(results.windows(2).all(|w| w[0].dist <= w[1].dist));
            let hits = results.iter().filter(|n| truth.contains(&n.id)).count();
            let recall = hits as f32 / k as f32;
            assert!(recall >= 0.5, "recall {recall} below 0.5 (k={k})");
            all_results.push(results);
        }
        assert_eq!(all_results[0], all_results[1]);

        // default ef (k + k/2) and a deletion-style mask (all but a few rows)
        let default_ef_params = HnswQueryParams {
            ef: k + k / 2,
            ..params
        };
        for excluded_stride in [2, 400] {
            let passes = |id: u32| id % excluded_stride != 1;
            let mut bitset = mask_generator.generate(TOTAL);
            for id in (0..TOTAL as u32).filter(|id| passes(*id)) {
                bitset.insert(id);
            }
            let truth: std::collections::HashSet<u32> =
                brute_force_topk_masked(store.as_ref(), query.clone(), k, passes)
                    .into_iter()
                    .collect();
            let results = builder
                .search_acorn(
                    query.clone(),
                    k,
                    &default_ef_params,
                    &bitset,
                    store.as_ref(),
                )
                .unwrap();
            assert_eq!(results.len(), k);
            assert!(results.iter().all(|node| passes(node.id)));
            let hits = results.iter().filter(|n| truth.contains(&n.id)).count();
            let recall = hits as f32 / k as f32;
            assert!(recall >= 0.5, "recall {recall} below 0.5 (k={k})");
        }
    }

    /// Dispatch: dense prefilters take the graph traversal, sparse ones the
    /// exact flat scan, and both return only mask-passing row ids.
    #[tokio::test]
    async fn test_subindex_prefilter_dispatch() {
        use arrow_array::cast::AsArray;
        use async_trait::async_trait;
        use lance_core::Result;
        use lance_select::{RowAddrMask, RowAddrTreeMap};

        use crate::metrics::NoOpMetricsCollector;
        use crate::prefilter::PreFilter;

        struct MaskPreFilter {
            mask: Arc<RowAddrMask>,
        }

        #[async_trait]
        impl PreFilter for MaskPreFilter {
            async fn wait_for_ready(&self) -> Result<()> {
                Ok(())
            }
            fn is_empty(&self) -> bool {
                false
            }
            fn mask(&self) -> Arc<RowAddrMask> {
                self.mask.clone()
            }
            fn filter_row_ids<'a>(
                &self,
                row_ids: Box<dyn Iterator<Item = &'a u64> + 'a>,
            ) -> Vec<u64> {
                self.mask.selected_indices(row_ids)
            }
        }

        const DIM: usize = 32;
        const TOTAL: usize = 2048;
        let fsl =
            FixedSizeListArray::try_new_from_values(generate_random_array(TOTAL * DIM), DIM as i32)
                .unwrap();
        let store = Arc::new(FlatFloatStorage::new(fsl.clone(), DistanceType::L2));
        let hnsw = HNSW::index_vectors(
            store.as_ref(),
            HnswBuildParams::default().num_edges(20).ef_construction(50),
        )
        .unwrap();

        let k = 10;
        let query_key = fsl.value(0);

        let search_row_ids = |allowed: Vec<u64>, use_acorn: bool| {
            let params = HnswQueryParams {
                ef: 50,
                lower_bound: None,
                upper_bound: None,
                dist_q_c: 0.0,
                use_acorn,
            };
            let filter = Arc::new(MaskPreFilter {
                mask: Arc::new(RowAddrMask::from_allowed(RowAddrTreeMap::from_iter(
                    allowed,
                ))),
            });
            let batch = hnsw
                .search(
                    query_key.clone(),
                    k,
                    params,
                    store.as_ref(),
                    filter,
                    &NoOpMetricsCollector,
                )
                .unwrap();
            batch[lance_core::ROW_ID]
                .as_primitive::<arrow_array::types::UInt64Type>()
                .values()
                .to_vec()
        };

        // Dense mask (50% of rows), in both modes.
        let dense: Vec<u64> = (0..TOTAL as u64).step_by(2).collect();
        for use_acorn in [false, true] {
            let row_ids = search_row_ids(dense.clone(), use_acorn);
            assert_eq!(row_ids.len(), k);
            assert!(row_ids.iter().all(|id| id % 2 == 0));
        }

        // All-pass mask: shortcuts to the unfiltered path.
        let all: Vec<u64> = (0..TOTAL as u64).collect();
        let unfiltered = hnsw
            .search_basic(
                query_key.clone(),
                k,
                &HnswQueryParams {
                    ef: 50,
                    lower_bound: None,
                    upper_bound: None,
                    dist_q_c: 0.0,
                    use_acorn: false,
                },
                None,
                store.as_ref(),
            )
            .unwrap();
        let row_ids = search_row_ids(all, true);
        assert_eq!(
            row_ids,
            unfiltered.iter().map(|n| n.id as u64).collect::<Vec<_>>()
        );

        // Sparse mask (< 10% of rows): the flat scan, which is exact.
        let sparse: Vec<u64> = (0..TOTAL as u64).step_by(25).collect();
        let row_ids = search_row_ids(sparse.clone(), true);
        assert_eq!(row_ids.len(), k);
        let truth = brute_force_topk_masked(store.as_ref(), query_key.clone(), k, |id| {
            sparse.contains(&(id as u64))
        });
        let mut got: Vec<u32> = row_ids.iter().map(|id| *id as u32).collect();
        got.sort_unstable();
        let mut expected = truth;
        expected.sort_unstable();
        assert_eq!(got, expected);
    }

    /// Every fresh `level_offsets` range must exactly delimit the rows emitted
    /// for that HNSW level (issue #5156).
    #[test]
    fn test_level_offsets_match_serialized_levels() {
        use arrow::array::AsArray;
        use arrow::datatypes::UInt32Type;

        const DIM: usize = 32;
        const TOTAL: usize = 2048;
        let fsl =
            FixedSizeListArray::try_new_from_values(generate_random_array(TOTAL * DIM), DIM as i32)
                .unwrap();
        let store = Arc::new(FlatFloatStorage::new(fsl, DistanceType::L2));
        let builder = HNSW::index_vectors(
            store.as_ref(),
            HnswBuildParams::default().num_edges(20).ef_construction(50),
        )
        .unwrap();

        // The scenario only exists on a multi-level graph.
        assert!(
            builder.max_level() >= 2,
            "expected a multi-level graph (got max_level {})",
            builder.max_level()
        );

        let batch = builder.to_batch().unwrap();
        let metadata = builder.metadata();
        assert_eq!(
            *metadata.level_offsets.last().unwrap(),
            batch.num_rows(),
            "level offsets must cover every serialized row",
        );

        let nodes = builder.nodes().unwrap();
        for level in 0..builder.max_level() as usize {
            let start = metadata.level_offsets[level];
            let end = metadata.level_offsets[level + 1];
            let level_batch = batch.slice(start, end - start);
            let ids = level_batch.column(0).as_primitive::<UInt32Type>();
            let expected_ids = nodes
                .iter()
                .enumerate()
                .filter_map(|(id, node)| (level < node.level_neighbors.len()).then_some(id as u32))
                .collect::<Vec<_>>();

            assert_eq!(
                ids.values().as_ref(),
                expected_ids.as_slice(),
                "serialized ids do not match level {level}",
            );
            assert_eq!(builder.num_nodes(level), expected_ids.len());
        }
    }

    /// Version-1 readers use the configured max level to index serialized
    /// level batches directly. Empty trailing ranges keep that persisted
    /// shape while current readers use only the sampled, non-empty height.
    #[test]
    fn test_metadata_preserves_configured_empty_levels() {
        const CONFIGURED_LEVELS: usize = 7;
        let params = HnswBuildParams::default().max_level(CONFIGURED_LEVELS as u16);
        let hnsw = HNSW::from_parts(params, vec![GraphBuilderNode::new(0, 1)], vec![1], 0);

        assert_eq!(hnsw.max_level(), 1);
        let metadata = hnsw.metadata();
        assert_eq!(metadata.level_offsets.len(), CONFIGURED_LEVELS + 1);
        assert_eq!(metadata.level_offsets[0], 0);
        assert!(
            metadata.level_offsets[1..]
                .iter()
                .all(|offset| *offset == 1)
        );

        let loaded = HNSW::load(hnsw.to_batch().unwrap()).unwrap();
        assert_eq!(loaded.max_level(), 1);
        match &loaded.inner.graph {
            HnswGraph::Loaded(graph) => {
                assert_eq!(graph.level_neighbors.len(), CONFIGURED_LEVELS);
                assert_eq!(graph.level_count[0], 1);
                assert!(graph.level_count[1..].iter().all(|count| *count == 0));
            }
            HnswGraph::Built(_) => panic!("expected an Arrow-backed loaded graph"),
        }
    }

    /// Indices written before issue #5156 was fixed omitted the entry point
    /// from every upper-level count. Loading those misaligned slices must keep
    /// the previous id-keyed, last-write-wins behavior.
    #[test]
    fn test_load_legacy_misaligned_level_offsets() {
        const DIM: usize = 32;
        const TOTAL: usize = 2048;
        let fsl =
            FixedSizeListArray::try_new_from_values(generate_random_array(TOTAL * DIM), DIM as i32)
                .unwrap();
        let store = Arc::new(FlatFloatStorage::new(fsl.clone(), DistanceType::L2));
        let builder = HNSW::index_vectors(
            store.as_ref(),
            HnswBuildParams::default().num_edges(20).ef_construction(50),
        )
        .unwrap();
        assert!(builder.max_level() >= 2);

        let batch = builder.to_batch().unwrap();
        let mut metadata = builder.metadata();
        let mut legacy_offsets = Vec::with_capacity(metadata.level_offsets.len());
        legacy_offsets.push(0);
        for level in 0..builder.max_level() as usize {
            let level_count = metadata.level_offsets[level + 1] - metadata.level_offsets[level];
            let legacy_level_count = level_count - usize::from(level > 0);
            legacy_offsets.push(legacy_offsets.last().unwrap() + legacy_level_count);
        }
        metadata.level_offsets = legacy_offsets;
        assert!(*metadata.level_offsets.last().unwrap() < batch.num_rows());

        let legacy_batch = with_hnsw_metadata(&batch, metadata);
        let loaded = HNSW::load(legacy_batch).unwrap();
        assert!(matches!(loaded.inner.graph, HnswGraph::Loaded(_)));
        let params = HnswQueryParams {
            ef: 50,
            lower_bound: None,
            upper_bound: None,
            dist_q_c: 0.0,
            use_acorn: false,
        };
        let entry_point = builder.inner.entry_point as usize;
        let query_indices = [0, 1, TOTAL / 3, TOTAL - 1]
            .into_iter()
            .filter(|query_index| *query_index != entry_point)
            .take(3)
            .collect::<Vec<_>>();
        assert_eq!(query_indices.len(), 3);
        for query_index in query_indices {
            let query = fsl.value(query_index);
            let builder_results = builder
                .search_basic(query.clone(), 10, &params, None, store.as_ref())
                .unwrap();
            let loaded_results = loaded
                .search_basic(query, 10, &params, None, store.as_ref())
                .unwrap();
            assert_eq!(builder_results, loaded_results);
        }
    }

    /// `load()` must reject a batch whose level-0 `__vector_id` no longer
    /// matches the row index. The `LevelLookup::Dense` fast path relies on
    /// `row == id`, and the old `debug_assert!` was compiled out of release
    /// builds -- so a corrupt batch must fail at the `load()` boundary instead
    /// of silently searching the wrong neighbor lists.
    #[tokio::test]
    async fn test_load_rejects_misaligned_level0_id() {
        use arrow::array::AsArray;
        use arrow::datatypes::UInt32Type;

        const DIM: usize = 16;
        const TOTAL: usize = 256;
        let fsl =
            FixedSizeListArray::try_new_from_values(generate_random_array(TOTAL * DIM), DIM as i32)
                .unwrap();
        let store = Arc::new(FlatFloatStorage::new(fsl, DistanceType::L2));
        let builder = HNSW::index_vectors(
            store.as_ref(),
            HnswBuildParams::default().num_edges(20).ef_construction(50),
        )
        .unwrap();

        let batch = builder.to_batch().unwrap();
        // Row 0 is always a level-0 node; break its `__vector_id == row`
        // invariant while preserving the (metadata-bearing) schema.
        let mut ids = batch
            .column(0)
            .as_primitive::<UInt32Type>()
            .values()
            .to_vec();
        ids[0] = ids.len() as u32;
        let mut columns = batch.columns().to_vec();
        columns[0] = Arc::new(UInt32Array::from(ids));
        let corrupted = RecordBatch::try_new(batch.schema(), columns).unwrap();

        assert!(
            HNSW::load(corrupted).is_err(),
            "load() must reject a misaligned level-0 __vector_id"
        );
    }

    /// `load()` must reject metadata whose `entry_point` is out of range for
    /// the node count: it indexes the `Dense` level-0 lookup directly, so an
    /// out-of-range value would read past the level-0 neighbor buffer at search
    /// time.
    #[tokio::test]
    async fn test_load_rejects_out_of_range_entry_point() {
        const DIM: usize = 16;
        const TOTAL: usize = 256;
        let fsl =
            FixedSizeListArray::try_new_from_values(generate_random_array(TOTAL * DIM), DIM as i32)
                .unwrap();
        let store = Arc::new(FlatFloatStorage::new(fsl, DistanceType::L2));
        let builder = HNSW::index_vectors(
            store.as_ref(),
            HnswBuildParams::default().num_edges(20).ef_construction(50),
        )
        .unwrap();

        let batch = builder.to_batch().unwrap();
        let mut md: HnswMetadata = serde_json::from_str(
            batch
                .schema_ref()
                .metadata()
                .get(HNSW_METADATA_KEY)
                .unwrap(),
        )
        .unwrap();
        // Valid entry points are `[0, N)`; `level_offsets[1]` == N is one past.
        let n = md.level_offsets[1];
        md.entry_point = n as u32;
        let corrupted = with_hnsw_metadata(&batch, md);

        assert!(
            HNSW::load(corrupted).is_err(),
            "load() must reject an out-of-range entry_point"
        );
    }

    /// An empty index round-trips: 0-row `to_batch` -> `load` -> empty graph.
    #[tokio::test]
    async fn test_loaded_empty_index() {
        const DIM: usize = 16;
        let fsl =
            FixedSizeListArray::try_new_from_values(generate_random_array(0), DIM as i32).unwrap();
        let store = Arc::new(FlatFloatStorage::new(fsl, DistanceType::L2));
        let builder = HNSW::index_vectors(store.as_ref(), HnswBuildParams::default()).unwrap();
        assert!(builder.is_empty());

        let batch = builder.to_batch().unwrap();
        assert_eq!(batch.num_rows(), 0);

        let loaded = HNSW::load(batch).unwrap();
        assert!(loaded.is_empty());
        assert_eq!(loaded.len(), 0);
        // A 0-row load short-circuits to the empty (Built) graph.
        assert!(!matches!(loaded.inner.graph, HnswGraph::Loaded(_)));
        assert_eq!(loaded.to_batch().unwrap().num_rows(), 0);
    }

    /// build -> `to_batch` (b1) -> `load` -> `to_batch` (b2) must satisfy
    /// `b1 == b2`, and the round-tripped batch must reload and search
    /// identically. This is exactly the IVF partition-cache path:
    /// `ivf/partition_serde.rs` calls `to_batch()` on a *loaded* index.
    #[tokio::test]
    async fn test_to_batch_roundtrip_loaded() {
        const DIM: usize = 24;
        const TOTAL: usize = 1500;
        let fsl =
            FixedSizeListArray::try_new_from_values(generate_random_array(TOTAL * DIM), DIM as i32)
                .unwrap();
        let store = Arc::new(FlatFloatStorage::new(fsl.clone(), DistanceType::L2));
        let builder = HNSW::index_vectors(
            store.as_ref(),
            HnswBuildParams::default().num_edges(16).ef_construction(50),
        )
        .unwrap();

        let b1 = builder.to_batch().unwrap();
        let loaded = HNSW::load(b1.clone()).unwrap();
        assert!(matches!(loaded.inner.graph, HnswGraph::Loaded(_)));
        let b2 = loaded.to_batch().unwrap();
        assert_eq!(b1, b2);

        let reloaded = HNSW::load(b2).unwrap();
        let params = HnswQueryParams {
            ef: 50,
            lower_bound: None,
            upper_bound: None,
            dist_q_c: 0.0,
            use_acorn: false,
        };
        let query = fsl.value(7);
        let a = builder
            .search_basic(query.clone(), 10, &params, None, store.as_ref())
            .unwrap();
        let b = reloaded
            .search_basic(query, 10, &params, None, store.as_ref())
            .unwrap();
        assert_eq!(a, b);
    }

    /// Regression for the IVF partition-cache round-trip: a disk-loaded batch
    /// inherits extra schema metadata keys from the index file (e.g.
    /// `INDEX_METADATA_SCHEMA_KEY`, `IVF_METADATA_KEY`). `to_batch()` on the
    /// loaded graph must *merge* the HNSW key into that metadata rather than
    /// replacing it -- otherwise the new schema is not a superset of the
    /// current one and `RecordBatch::with_schema` fails with "target schema is
    /// not superset of current schema".
    #[tokio::test]
    async fn test_to_batch_loaded_preserves_extra_schema_metadata() {
        use super::HNSW_METADATA_KEY;

        const DIM: usize = 24;
        const TOTAL: usize = 512;
        let fsl =
            FixedSizeListArray::try_new_from_values(generate_random_array(TOTAL * DIM), DIM as i32)
                .unwrap();
        let store = Arc::new(FlatFloatStorage::new(fsl, DistanceType::L2));
        let builder = HNSW::index_vectors(
            store.as_ref(),
            HnswBuildParams::default().num_edges(16).ef_construction(50),
        )
        .unwrap();

        // Simulate the disk-load path (`ivf/v2.rs::load_partition_entry`):
        // the batch reaching `HNSW::load` carries the index file's schema
        // metadata in addition to the HNSW key.
        let built_batch = builder.to_batch().unwrap();
        let mut metadata = built_batch.schema_ref().metadata().clone();
        metadata.insert(
            "lance:index_metadata".to_string(),
            "{\"distance_type\":\"l2\"}".to_string(),
        );
        metadata.insert("lance:ivf".to_string(), "42".to_string());
        let schema = built_batch
            .schema()
            .as_ref()
            .clone()
            .with_metadata(metadata);
        let batch_with_extra =
            RecordBatch::try_new(Arc::new(schema), built_batch.columns().to_vec()).unwrap();

        let loaded = HNSW::load(batch_with_extra).unwrap();
        assert!(matches!(loaded.inner.graph, HnswGraph::Loaded(_)));

        // Before the fix this fails: the re-stamped schema dropped the extra
        // keys, so `with_schema`'s superset check rejected the round-trip.
        let out = loaded.to_batch().unwrap();
        let out_metadata = out.schema_ref().metadata();
        assert!(out_metadata.contains_key(HNSW_METADATA_KEY));
        assert_eq!(
            out_metadata.get("lance:index_metadata").map(String::as_str),
            Some("{\"distance_type\":\"l2\"}"),
        );
        assert_eq!(
            out_metadata.get("lance:ivf").map(String::as_str),
            Some("42")
        );

        // The HNSW key must still decode to valid metadata after the merge.
        let reloaded = HNSW::load(out).unwrap();
        assert_eq!(reloaded.len(), loaded.len());
    }

    /// The loaded graph shares the Arrow batch and reconstructs no per-node
    /// `Vec<GraphBuilderNode>` / `Vec<OrderedNode>`, so it is strictly
    /// lighter than the in-memory build representation.
    #[tokio::test]
    async fn test_loaded_graph_is_arrow_backed() {
        const DIM: usize = 32;
        const TOTAL: usize = 2048;
        let fsl =
            FixedSizeListArray::try_new_from_values(generate_random_array(TOTAL * DIM), DIM as i32)
                .unwrap();
        let store = Arc::new(FlatFloatStorage::new(fsl, DistanceType::L2));
        let builder = HNSW::index_vectors(
            store.as_ref(),
            HnswBuildParams::default().num_edges(20).ef_construction(50),
        )
        .unwrap();
        assert!(!matches!(builder.inner.graph, HnswGraph::Loaded(_)));

        let loaded = HNSW::load(builder.to_batch().unwrap()).unwrap();
        assert!(matches!(loaded.inner.graph, HnswGraph::Loaded(_)));
        assert!(
            loaded.deep_size_of() < builder.deep_size_of(),
            "loaded graph ({}) should be lighter than built ({})",
            loaded.deep_size_of(),
            builder.deep_size_of(),
        );
    }
}
