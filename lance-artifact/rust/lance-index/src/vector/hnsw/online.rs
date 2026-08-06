// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Online (incremental) HNSW builder.
//!
//! Supports concurrent search while a single writer is appending nodes.
//! Designed for in-memory MemTable indexes that need to be queryable during
//! build and convertible to the on-disk Lance HNSW format at flush time.
//!
//! # Concurrency
//!
//! - Single writer (typically the MemTable flush handler thread).
//! - Many concurrent readers (search threads).
//!
//! Per-level neighbor lists are stored in `ArcSwap<Vec<u32>>` so search reads
//! are lock-free. Writer-only state (ranked neighbor heaps used during prune)
//! is behind a `Mutex`; with a single writer, the mutex is uncontended.
//!
//! # Lifecycle
//!
//! 1. `OnlineHnswBuilder::try_with_capacity(...)` pre-allocates fixed-size node
//!    arrays. Each slot has its target level pre-assigned so concurrent
//!    inserts don't need to allocate.
//! 2. Writer calls `insert(id, storage)` for `id` in `0..capacity`. The vector
//!    at `id` must already be in `storage` (committed before `insert` is
//!    called).
//! 3. Readers call `search(...)` at any time; visible nodes are those whose
//!    `insert` returned.
//! 4. `finalize()` consumes the builder and returns an immutable
//!    [`super::HNSW`] that can be serialized via `HNSW::to_batch()`.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use arc_swap::ArcSwap;
use crossbeam_queue::ArrayQueue;
use rand::{SeedableRng, rngs::SmallRng};

use super::builder::{
    HNSW, HNSW_LEVEL_RNG_SEED, HnswBuildParams, HnswQueryParams, random_level_with,
};
use super::select_neighbors_heuristic;
use crate::vector::graph::builder::GraphBuilderNode;
use crate::vector::graph::{
    Graph, OrderedFloat, OrderedNode, VisitedGenerator, beam_search, greedy_search,
};
use crate::vector::storage::{DistCalculator, VectorStore};
use lance_core::Result;
use lance_core::utils::tokio::get_num_compute_intensive_cpus;

/// A node in the online HNSW graph.
///
/// Read-visible neighbor lists use [`ArcSwap`] for lock-free reads. The
/// writer-only ranked heap used during prune is wrapped in a [`Mutex`] for
/// interior mutability — under single-writer use the mutex is always
/// uncontended.
pub struct OnlineGraphBuilderNode {
    /// Per-level read-visible neighbor lists. Length = target_level + 1.
    /// Empty `Arc<Vec<u32>>` means the node is allocated but not yet inserted.
    pub(crate) level_neighbors: Vec<ArcSwap<Vec<u32>>>,
    /// Writer-only per-level ranked neighbors used during prune.
    pub(crate) level_neighbors_ranked: Mutex<Vec<Vec<OrderedNode>>>,
    /// Convenience hot path mirror of `level_neighbors[0]`.
    pub(crate) bottom_neighbors: ArcSwap<Vec<u32>>,
}

impl OnlineGraphBuilderNode {
    pub fn new(target_level: u16) -> Self {
        let levels = (target_level as usize) + 1;
        let level_neighbors = (0..levels)
            .map(|_| ArcSwap::from_pointee(Vec::new()))
            .collect();
        let level_neighbors_ranked = (0..levels).map(|_| Vec::new()).collect();
        Self {
            level_neighbors,
            level_neighbors_ranked: Mutex::new(level_neighbors_ranked),
            bottom_neighbors: ArcSwap::from_pointee(Vec::new()),
        }
    }

    fn target_level(&self) -> u16 {
        self.level_neighbors.len() as u16 - 1
    }

    /// Returns true if this node exists at the given level.
    fn has_level(&self, level: u16) -> bool {
        (level as usize) < self.level_neighbors.len()
    }

    fn add_neighbor(&self, v: u32, dist: OrderedFloat, level: u16) {
        if !self.has_level(level) {
            return;
        }
        let mut ranked = self
            .level_neighbors_ranked
            .lock()
            .expect("level_neighbors_ranked mutex poisoned");
        ranked[level as usize].push(OrderedNode { dist, id: v });
    }

    /// Rebuild `level_neighbors[level]` from the current ranked list and
    /// publish it via `ArcSwap`. Also updates `bottom_neighbors` for level 0.
    fn publish_from_ranked(&self, level: u16) {
        if !self.has_level(level) {
            return;
        }
        let ranked = self
            .level_neighbors_ranked
            .lock()
            .expect("level_neighbors_ranked mutex poisoned");
        let new_list: Vec<u32> = ranked[level as usize].iter().map(|n| n.id).collect();
        drop(ranked);
        let new_arc = Arc::new(new_list);
        self.level_neighbors[level as usize].store(new_arc.clone());
        if level == 0 {
            self.bottom_neighbors.store(new_arc);
        }
    }
}

/// Online HNSW builder with pre-allocated capacity.
///
/// # Pre-allocation
///
/// All `capacity` nodes are allocated up front with a randomly-chosen target
/// level (matching the offline builder's level distribution). This avoids
/// any concurrent reallocation of the node array during inserts.
pub struct OnlineHnswBuilder {
    params: HnswBuildParams,
    nodes: Vec<OnlineGraphBuilderNode>,
    /// Number of nodes per level (level → count of nodes inserted whose
    /// `target_level >= level`).
    level_count: Vec<AtomicUsize>,
    /// Entry point id of the graph. Updated when a node with a new max level
    /// is inserted.
    entry_point: AtomicU32,
    /// Number of nodes whose insert has fully completed.
    inserted_len: AtomicUsize,
    /// Pool of visited bitmaps to amortize allocations during search.
    visited_generator_queue: Arc<ArrayQueue<VisitedGenerator>>,
}

impl OnlineHnswBuilder {
    /// Create a new builder with validated parameters.
    ///
    /// Each node's target level is pre-assigned at random.
    ///
    /// # Examples
    ///
    /// ```
    /// use lance_index::vector::hnsw::{OnlineHnswBuilder, builder::HnswBuildParams};
    ///
    /// let builder = OnlineHnswBuilder::try_with_capacity(
    ///     1_024,
    ///     HnswBuildParams::default(),
    /// )?;
    /// assert_eq!(builder.capacity(), 1_024);
    /// # Ok::<(), lance_core::Error>(())
    /// ```
    pub fn try_with_capacity(capacity: usize, params: HnswBuildParams) -> Result<Self> {
        params.validate()?;
        Ok(Self::new(capacity, params))
    }

    /// Create a new builder with the given capacity.
    ///
    /// Use [`Self::try_with_capacity`] to handle invalid parameters without
    /// panicking.
    ///
    /// # Panics
    ///
    /// Panics when `params` violates an HNSW construction precondition.
    #[deprecated(note = "use OnlineHnswBuilder::try_with_capacity")]
    pub fn with_capacity(capacity: usize, params: HnswBuildParams) -> Self {
        Self::try_with_capacity(capacity, params)
            .unwrap_or_else(|error| panic!("invalid HNSW build parameters: {error}"))
    }

    fn new(capacity: usize, params: HnswBuildParams) -> Self {
        let max_level = params.max_level;
        let level_count = (0..max_level).map(|_| AtomicUsize::new(0)).collect();

        let mut level_rng = SmallRng::seed_from_u64(HNSW_LEVEL_RNG_SEED);
        let nodes: Vec<_> = (0..capacity)
            .map(|_| OnlineGraphBuilderNode::new(random_level_with(&params, &mut level_rng)))
            .collect();

        let queue_size = get_num_compute_intensive_cpus().max(1);
        let visited_generator_queue = Arc::new(ArrayQueue::new(queue_size));
        for _ in 0..queue_size {
            let _ = visited_generator_queue.push(VisitedGenerator::new(0));
        }

        Self {
            params,
            nodes,
            level_count,
            entry_point: AtomicU32::new(0),
            inserted_len: AtomicUsize::new(0),
            visited_generator_queue,
        }
    }

    pub fn capacity(&self) -> usize {
        self.nodes.len()
    }

    pub fn len(&self) -> usize {
        self.inserted_len.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn params(&self) -> &HnswBuildParams {
        &self.params
    }

    /// Insert the node at `id` into the graph.
    ///
    /// The vector at `id` must already be visible via `storage`'s
    /// `dist_calculator_from_id(id)` and `dist_calculator(query, _)` — i.e.
    /// the storage's committed length must include `id`.
    pub fn insert(&self, id: u32, storage: &impl VectorStore) {
        let mut visited_generator = self
            .visited_generator_queue
            .pop()
            .unwrap_or_else(|| VisitedGenerator::new(self.nodes.len()));

        self.insert_with_generator(id, storage, &mut visited_generator);

        // Best-effort return to pool. If full, drop.
        let _ = self.visited_generator_queue.push(visited_generator);
    }

    fn insert_with_generator(
        &self,
        id: u32,
        storage: &impl VectorStore,
        visited_generator: &mut VisitedGenerator,
    ) {
        let nodes = self.nodes.as_slice();
        let target_level = nodes[id as usize].target_level();
        let dist_calc = storage.dist_calculator_from_id(id);

        // First insert anchors the graph.
        if self.inserted_len.load(Ordering::Acquire) == 0 {
            for level in 0..=target_level {
                self.level_count[level as usize].fetch_add(1, Ordering::Relaxed);
            }
            self.entry_point.store(id, Ordering::Release);
            self.inserted_len.store(1, Ordering::Release);
            return;
        }

        let entry = self.entry_point.load(Ordering::Acquire);
        let entry_target_level = nodes[entry as usize].target_level();
        let mut ep = OrderedNode::new(entry, dist_calc.distance(entry).into());

        // Walk down upper levels with greedy_search to refine the entry point.
        for level in (target_level + 1..=entry_target_level).rev() {
            let cur_level = OnlineHnswLevelView::new(level, nodes);
            ep = greedy_search(&cur_level, ep, &dist_calc, self.params.prefetch_distance);
        }

        // Insert at each level from target_level down to 0.
        let mut pruned_neighbors_per_level: Vec<Vec<OrderedNode>> =
            vec![Vec::new(); (target_level + 1) as usize];

        let current_node = &nodes[id as usize];
        for level in (0..=target_level).rev() {
            self.level_count[level as usize].fetch_add(1, Ordering::Relaxed);

            let neighbors = self.search_level(&ep, level, &dist_calc, nodes, visited_generator);
            // Only nodes that actually exist at this level can be neighbors here.
            // Beam search may return the entry point even if it lacks this level
            // (when the upper-level graph is sparse during early inserts).
            for neighbor in &neighbors {
                if !nodes[neighbor.id as usize].has_level(level) {
                    continue;
                }
                current_node.add_neighbor(neighbor.id, neighbor.dist, level);
            }
            // New nodes select M connections at every level. The larger
            // level-0 limit applies only to reciprocal edges on old nodes.
            self.prune(storage, current_node, level, self.params.m);
            // Snapshot the pruned ranked list before publishing.
            let snapshot = {
                let ranked = current_node
                    .level_neighbors_ranked
                    .lock()
                    .expect("level_neighbors_ranked mutex poisoned");
                ranked[level as usize].clone()
            };
            current_node.publish_from_ranked(level);
            pruned_neighbors_per_level[level as usize] = snapshot;

            // Use the closest valid neighbor as the next entry point. If
            // the only result is an invalid (level-lacking) entry, fall back
            // to the existing ep.
            if let Some(next) = neighbors
                .iter()
                .find(|n| nodes[n.id as usize].has_level(level))
            {
                ep = next.clone();
            }
        }

        // Add reverse edges to chosen neighbors, prune them too.
        for (level, pruned_neighbors) in pruned_neighbors_per_level.iter().enumerate() {
            let level = level as u16;
            let reciprocal_limit = if level == 0 {
                self.params.m * 2
            } else {
                self.params.m
            };
            for selected_edge in pruned_neighbors {
                let chosen = &nodes[selected_edge.id as usize];
                chosen.add_neighbor(id, selected_edge.dist, level);
                self.prune(storage, chosen, level, reciprocal_limit);
                chosen.publish_from_ranked(level);
            }
        }

        // Promote entry point if this node has a higher target level than the
        // current entry point's target level. The contract is single-writer,
        // so this CAS is uncontested in practice; we use CAS rather than a
        // plain store to keep `entry_point` updates atomic against concurrent
        // searches and to leave the door open if the writer ever becomes
        // multi-threaded.
        if target_level > entry_target_level {
            let _ =
                self.entry_point
                    .compare_exchange(entry, id, Ordering::AcqRel, Ordering::Acquire);
        }

        self.inserted_len.fetch_add(1, Ordering::AcqRel);
    }

    fn search_level(
        &self,
        ep: &OrderedNode,
        level: u16,
        dist_calc: &impl DistCalculator,
        nodes: &[OnlineGraphBuilderNode],
        visited_generator: &mut VisitedGenerator,
    ) -> Vec<OrderedNode> {
        let cur_level = OnlineHnswLevelView::new(level, nodes);
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
        node: &OnlineGraphBuilderNode,
        level: u16,
        max_connections: usize,
    ) {
        let mut ranked = node
            .level_neighbors_ranked
            .lock()
            .expect("level_neighbors_ranked mutex poisoned");
        let level_neighbors = ranked[level as usize].clone();
        if level_neighbors.len() <= max_connections {
            return;
        }
        ranked[level as usize] =
            select_neighbors_heuristic(storage, &level_neighbors, max_connections);
    }

    /// Search the graph for the k nearest neighbors of `query`.
    ///
    /// Visible nodes are those whose insert has fully returned. Returns
    /// (node_id, distance) pairs sorted by ascending distance.
    /// Return the top candidates from beam search.
    ///
    /// Returns up to `ef.max(k)` candidates ordered by distance. The caller
    /// is responsible for any post-filtering (visibility, etc.) and the final
    /// truncate to `k`. Returning the full beam — not just `k` — lets a
    /// post-filter that drops some candidates still produce up to `k`
    /// results before recall regresses.
    pub fn search(
        &self,
        query: arrow_array::ArrayRef,
        k: usize,
        ef: usize,
        storage: &impl VectorStore,
    ) -> Vec<OrderedNode> {
        let visible = self.inserted_len.load(Ordering::Acquire);
        if visible == 0 {
            return Vec::new();
        }

        let mut visited_generator = self
            .visited_generator_queue
            .pop()
            .unwrap_or_else(|| VisitedGenerator::new(self.nodes.len()));

        let dist_calc = storage.dist_calculator(query, 0.0);
        let entry = self.entry_point.load(Ordering::Acquire);
        let mut ep = OrderedNode::new(entry, dist_calc.distance(entry).into());

        let nodes = self.nodes.as_slice();
        for level in (1..=nodes[entry as usize].target_level()).rev() {
            let cur_level = OnlineHnswLevelView::new(level, nodes);
            ep = greedy_search(&cur_level, ep, &dist_calc, self.params.prefetch_distance);
        }

        let bottom = OnlineHnswBottomView::new(nodes);
        let mut visited = visited_generator.generate(nodes.len());
        let params = HnswQueryParams {
            ef: ef.max(k),
            lower_bound: None,
            upper_bound: None,
            dist_q_c: 0.0,
            use_acorn: false,
        };
        let result = beam_search(
            &bottom,
            &ep,
            &params,
            &dist_calc,
            None,
            self.params.prefetch_distance,
            &mut visited,
        );
        drop(visited);

        let _ = self.visited_generator_queue.push(visited_generator);

        // Return up to `ef.max(k)` so post-filtering at the caller has more
        // headroom than just `k`.
        let limit = ef.max(k);
        result.into_iter().take(limit).collect()
    }

    /// Snapshot the current graph as an immutable on-disk Lance HNSW.
    ///
    /// Only nodes whose insert has fully completed are included. Caller must
    /// ensure no concurrent inserts while this runs.
    ///
    /// `level_count` is recomputed from the actual per-level emissions so the
    /// serialized batch and metadata stay in sync.
    pub fn to_hnsw(&self) -> HNSW {
        let inserted = self.inserted_len.load(Ordering::Acquire);
        let entry_point = self.entry_point.load(Ordering::Acquire);
        let actual_levels = if inserted == 0 {
            0
        } else {
            self.nodes[entry_point as usize].level_neighbors.len()
        };

        let mut frozen_nodes: Vec<GraphBuilderNode> = Vec::with_capacity(inserted);
        for node in self.nodes.iter().take(inserted) {
            let level_neighbors: Vec<Arc<Vec<u32>>> = node
                .level_neighbors
                .iter()
                .map(|sl| sl.load_full())
                .collect();
            let level_neighbors_ranked = node
                .level_neighbors_ranked
                .lock()
                .expect("level_neighbors_ranked mutex poisoned")
                .clone();

            let bottom_neighbors = level_neighbors
                .first()
                .cloned()
                .unwrap_or_else(|| Arc::new(Vec::new()));
            frozen_nodes.push(GraphBuilderNode::from_parts(
                level_neighbors,
                level_neighbors_ranked,
                bottom_neighbors,
            ));
        }

        let mut level_count: Vec<usize> = vec![0; actual_levels];
        for node in &frozen_nodes {
            let levels = node.level_neighbors.len().min(actual_levels);
            for count in level_count.iter_mut().take(levels) {
                *count += 1;
            }
        }

        HNSW::from_parts(self.params.clone(), frozen_nodes, level_count, entry_point)
    }

    /// Backwards-compat shim: equivalent to `to_hnsw()` but consumes self.
    pub fn finalize(self) -> HNSW {
        self.to_hnsw()
    }
}

/// View of a single level of an [`OnlineHnswBuilder`]'s graph for use with
/// the shared [`Graph`]-trait search algorithms.
pub struct OnlineHnswLevelView<'a> {
    level: u16,
    nodes: &'a [OnlineGraphBuilderNode],
}

impl<'a> OnlineHnswLevelView<'a> {
    pub fn new(level: u16, nodes: &'a [OnlineGraphBuilderNode]) -> Self {
        Self { level, nodes }
    }
}

impl Graph for OnlineHnswLevelView<'_> {
    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn neighbors(&self, key: u32) -> Arc<Vec<u32>> {
        let node = &self.nodes[key as usize];
        let level_idx = self.level as usize;
        if level_idx >= node.level_neighbors.len() {
            // Node doesn't exist at this level (target_level too low).
            return Arc::new(Vec::new());
        }
        node.level_neighbors[level_idx].load_full()
    }
}

/// View of the bottom level (level 0) of an online HNSW for search.
pub struct OnlineHnswBottomView<'a> {
    nodes: &'a [OnlineGraphBuilderNode],
}

impl<'a> OnlineHnswBottomView<'a> {
    pub fn new(nodes: &'a [OnlineGraphBuilderNode]) -> Self {
        Self { nodes }
    }
}

impl Graph for OnlineHnswBottomView<'_> {
    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn neighbors(&self, key: u32) -> Arc<Vec<u32>> {
        self.nodes[key as usize].bottom_neighbors.load_full()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::flat::storage::FlatFloatStorage;
    use arrow_array::{FixedSizeListArray, Float32Array};
    use lance_arrow::FixedSizeListArrayExt;
    use lance_linalg::distance::DistanceType;
    use lance_testing::datagen::generate_random_array;
    use rstest::rstest;
    use std::sync::Arc;

    fn build_storage(n: usize, dim: usize) -> (Arc<FlatFloatStorage>, FixedSizeListArray) {
        let data = generate_random_array(n * dim);
        let fsl = FixedSizeListArray::try_new_from_values(data, dim as i32).unwrap();
        let storage = Arc::new(FlatFloatStorage::new(fsl.clone(), DistanceType::L2));
        (storage, fsl)
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
    fn test_try_with_capacity_rejects_invalid_params(
        #[case] params: HnswBuildParams,
        #[case] expected_message: &str,
    ) {
        let Err(error) = OnlineHnswBuilder::try_with_capacity(2, params) else {
            panic!("expected invalid HNSW parameters to be rejected");
        };
        assert!(matches!(&error, lance_core::Error::InvalidInput { .. }));
        assert!(
            error.to_string().contains(expected_message),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_online_hnsw_recall() {
        const N: usize = 1000;
        const DIM: usize = 32;

        let (storage, fsl) = build_storage(N, DIM);
        let params = HnswBuildParams::default()
            .num_edges(16)
            .ef_construction(100);
        let builder = OnlineHnswBuilder::try_with_capacity(N, params).unwrap();

        for i in 0..N {
            builder.insert(i as u32, storage.as_ref());
        }
        assert_eq!(builder.len(), N);

        // Pick a few queries and check recall against brute force.
        let k = 10;
        let mut total_correct = 0usize;
        for q_idx in 0..50 {
            let query = fsl.value(q_idx);

            // brute force
            let mut all_dists: Vec<(usize, f32)> = (0..N)
                .map(|i| {
                    let v = fsl.value(i);
                    let q = query
                        .as_any()
                        .downcast_ref::<arrow_array::Float32Array>()
                        .unwrap();
                    let vv = v
                        .as_any()
                        .downcast_ref::<arrow_array::Float32Array>()
                        .unwrap();
                    let mut s = 0.0f32;
                    for j in 0..DIM {
                        let d = q.value(j) - vv.value(j);
                        s += d * d;
                    }
                    (i, s)
                })
                .collect();
            all_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let truth: std::collections::HashSet<usize> =
                all_dists.iter().take(k).map(|(i, _)| *i).collect();

            let results = builder.search(query, k, 64, storage.as_ref());
            let found: std::collections::HashSet<usize> =
                results.iter().map(|r| r.id as usize).collect();
            total_correct += truth.intersection(&found).count();
        }

        let recall = total_correct as f32 / (50 * k) as f32;
        assert!(recall >= 0.85, "recall too low: {}", recall);
    }

    #[test]
    fn test_online_hnsw_finalize_matches_search() {
        const N: usize = 256;
        const DIM: usize = 16;

        let (storage, fsl) = build_storage(N, DIM);
        let params = HnswBuildParams::default()
            .num_edges(16)
            .ef_construction(100);
        let builder = OnlineHnswBuilder::try_with_capacity(N, params).unwrap();
        for i in 0..N {
            builder.insert(i as u32, storage.as_ref());
        }

        let online_results = builder.search(fsl.value(0), 10, 64, storage.as_ref());

        let hnsw = builder.finalize();
        let mut visited = VisitedGenerator::new(N);
        let bottom_results = hnsw
            .search_inner(
                fsl.value(0),
                10,
                &HnswQueryParams {
                    ef: 64,
                    lower_bound: None,
                    upper_bound: None,
                    dist_q_c: 0.0,
                    use_acorn: false,
                },
                None,
                &mut visited,
                storage.as_ref(),
                Some(2),
            )
            .unwrap();

        let online_ids: std::collections::HashSet<u32> =
            online_results.iter().map(|r| r.id).collect();
        let frozen_ids: std::collections::HashSet<u32> =
            bottom_results.iter().map(|r| r.id).collect();
        // Allow a small disagreement window since the two paths use slightly
        // different entry-point handling, but the bulk of results should
        // overlap.
        let overlap = online_ids.intersection(&frozen_ids).count();
        assert!(
            overlap >= 7,
            "frozen vs online overlap too low: {}",
            overlap
        );
    }

    #[test]
    fn test_online_hnsw_empty_search() {
        let params = HnswBuildParams::default();
        let builder = OnlineHnswBuilder::try_with_capacity(16, params).unwrap();
        let (storage, fsl) = build_storage(1, 8);
        let results = builder.search(fsl.value(0), 10, 32, storage.as_ref());
        assert!(results.is_empty());
    }

    /// Online construction uses the same Algorithm 1 distinction as the
    /// offline builder: M for a new node and Mmax0 for reciprocal edges.
    #[test]
    fn test_online_new_node_uses_m_connections() {
        let values = Float32Array::from(vec![
            1.0, 0.0, // east
            0.0, 1.0, // north
            -1.0, 0.0, // west
            0.0, -1.0, // south
            0.0, 0.0, // final node
        ]);
        let fsl = FixedSizeListArray::try_new_from_values(values, 2).unwrap();
        let storage = FlatFloatStorage::new(fsl, DistanceType::L2);
        let params = HnswBuildParams::default()
            .max_level(1)
            .num_edges(2)
            .ef_construction(5);
        // Bypass public validation to isolate the Algorithm 1 degree rule.
        let builder = OnlineHnswBuilder::new(5, params);

        for id in 0..5 {
            builder.insert(id, &storage);
        }

        let final_node = builder.nodes[4].level_neighbors_ranked.lock().unwrap();
        assert_eq!(final_node[0].len(), 2);
        drop(final_node);
        assert!(
            builder
                .nodes
                .iter()
                .all(|node| { node.level_neighbors_ranked.lock().unwrap()[0].len() <= 4 }),
            "existing level-0 nodes must remain bounded by Mmax0"
        );
    }

    #[test]
    fn test_online_promotes_first_highest_random_level() {
        const TOTAL: usize = 128;
        let (storage, _) = build_storage(TOTAL, 4);
        let params = HnswBuildParams::default();
        let builder = OnlineHnswBuilder::try_with_capacity(TOTAL, params.clone()).unwrap();

        let mut level_rng = SmallRng::seed_from_u64(HNSW_LEVEL_RNG_SEED);
        let expected_levels = (0..TOTAL)
            .map(|_| random_level_with(&params, &mut level_rng))
            .collect::<Vec<_>>();
        for (node, expected_level) in builder.nodes.iter().zip(&expected_levels) {
            assert_eq!(node.target_level(), *expected_level);
        }

        for id in 0..TOTAL as u32 {
            builder.insert(id, storage.as_ref());
        }
        let highest_level = *expected_levels.iter().max().unwrap();
        let expected_entry = expected_levels
            .iter()
            .position(|level| *level == highest_level)
            .unwrap() as u32;
        assert_eq!(builder.entry_point.load(Ordering::Acquire), expected_entry);
    }
}
