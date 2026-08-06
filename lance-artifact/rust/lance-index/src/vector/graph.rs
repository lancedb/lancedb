// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Generic Graph implementation.
//!

use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::Arc;

use arrow_schema::{DataType, Field};
use lance_core::deepsize::DeepSizeOf;

use crate::vector::hnsw::builder::HnswQueryParams;

pub mod builder;

use crate::vector::DIST_COL;

use crate::vector::storage::DistCalculator;

pub(crate) const NEIGHBORS_COL: &str = "__neighbors";

use std::sync::LazyLock;

/// NEIGHBORS field.
pub static NEIGHBORS_FIELD: LazyLock<Field> = LazyLock::new(|| {
    Field::new(
        NEIGHBORS_COL,
        DataType::List(Field::new_list_field(DataType::UInt32, true).into()),
        true,
    )
});
pub static DISTS_FIELD: LazyLock<Field> = LazyLock::new(|| {
    Field::new(
        DIST_COL,
        DataType::List(Field::new_list_field(DataType::Float32, true).into()),
        true,
    )
});

pub struct GraphNode<I = u32> {
    pub id: I,
    pub neighbors: Vec<I>,
}

impl<I> GraphNode<I> {
    pub fn new(id: I, neighbors: Vec<I>) -> Self {
        Self { id, neighbors }
    }
}

impl<I> From<I> for GraphNode<I> {
    fn from(id: I) -> Self {
        Self {
            id,
            neighbors: vec![],
        }
    }
}

/// A wrapper for f32 to make it ordered, so that we can put it into
/// a BTree or Heap
#[derive(Debug, PartialEq, Clone, Copy, DeepSizeOf)]
pub struct OrderedFloat(pub f32);

impl PartialOrd for OrderedFloat {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for OrderedFloat {}

impl Ord for OrderedFloat {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl From<f32> for OrderedFloat {
    fn from(f: f32) -> Self {
        Self(f)
    }
}

impl From<OrderedFloat> for f32 {
    fn from(f: OrderedFloat) -> Self {
        f.0
    }
}

#[derive(Debug, Eq, PartialEq, Clone, DeepSizeOf)]
pub struct OrderedNode<T = u32>
where
    T: PartialEq + Eq,
{
    pub id: T,
    pub dist: OrderedFloat,
}

impl<T: PartialEq + Eq> OrderedNode<T> {
    pub fn new(id: T, dist: OrderedFloat) -> Self {
        Self { id, dist }
    }
}

impl<T: PartialEq + Eq> PartialOrd for OrderedNode<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: PartialEq + Eq> Ord for OrderedNode<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dist.cmp(&other.dist)
    }
}

impl<T: PartialEq + Eq> From<(OrderedFloat, T)> for OrderedNode<T> {
    fn from((dist, id): (OrderedFloat, T)) -> Self {
        Self { id, dist }
    }
}

impl<T: PartialEq + Eq> From<OrderedNode<T>> for (OrderedFloat, T) {
    fn from(node: OrderedNode<T>) -> Self {
        (node.dist, node.id)
    }
}

/// Distance calculator.
///
/// This trait is used to calculate a query vector to a stream of vector IDs.
///
pub trait DistanceCalculator {
    /// Compute distances between one query vector to all the vectors in the
    /// list of IDs.
    fn compute_distances(&self, ids: &[u32]) -> Box<dyn Iterator<Item = f32>>;
}

/// Graph trait.
///
/// Type parameters
/// ---------------
/// K: Vertex Index type
/// T: the data type of vector, i.e., ``f32`` or ``f16``.
pub trait Graph {
    /// Get the number of nodes in the graph.
    fn len(&self) -> usize;

    /// Returns true if the graph is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the neighbors of a graph node, identifyied by the index.
    fn neighbors(&self, key: u32) -> Arc<Vec<u32>>;
}

pub trait BorrowingGraph {
    /// Get the number of nodes in the graph.
    fn len(&self) -> usize;

    /// Returns true if the graph is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrow the neighbors of a graph node, identified by the index.
    fn neighbors(&self, key: u32) -> &[u32];
}

const WORD_BITS: usize = usize::BITS as usize;

/// Compact visited list for graph traversals.
pub struct Visited<'a> {
    visited: &'a mut Vec<usize>,
    recently_visited: &'a mut Vec<u32>,
}

impl Visited<'_> {
    pub fn insert(&mut self, node_id: u32) {
        let node_id_usize = node_id as usize;
        let word_index = node_id_usize / WORD_BITS;
        let mask = 1usize << (node_id_usize % WORD_BITS);
        if self.visited[word_index] & mask == 0 {
            self.visited[word_index] |= mask;
            self.recently_visited.push(node_id);
        }
    }

    pub fn contains(&self, node_id: u32) -> bool {
        let node_id_usize = node_id as usize;
        let word_index = node_id_usize / WORD_BITS;
        let mask = 1usize << (node_id_usize % WORD_BITS);
        self.visited[word_index] & mask != 0
    }

    #[inline(always)]
    pub fn iter_ones(&self) -> impl Iterator<Item = usize> + '_ {
        self.recently_visited
            .iter()
            .map(|node_id| *node_id as usize)
    }

    pub fn count_ones(&self) -> usize {
        self.recently_visited.len()
    }
}

impl Drop for Visited<'_> {
    fn drop(&mut self) {
        for node_id in self.recently_visited.iter().copied() {
            let node_id_usize = node_id as usize;
            let word_index = node_id_usize / WORD_BITS;
            let mask = 1usize << (node_id_usize % WORD_BITS);
            self.visited[word_index] &= !mask;
        }
        self.recently_visited.clear();
    }
}

#[derive(Debug, Clone)]
pub struct VisitedGenerator {
    visited: Vec<usize>,
    recently_visited: Vec<u32>,
    capacity: usize,
}

impl VisitedGenerator {
    pub fn new(capacity: usize) -> Self {
        Self {
            visited: vec![0; capacity.div_ceil(WORD_BITS)],
            recently_visited: Vec::new(),
            capacity,
        }
    }

    pub fn generate(&mut self, node_count: usize) -> Visited<'_> {
        if node_count > self.capacity {
            let new_capacity = self.capacity.max(node_count).next_power_of_two();
            self.visited.resize(new_capacity.div_ceil(WORD_BITS), 0);
            self.capacity = new_capacity;
        }
        Visited {
            visited: &mut self.visited,
            recently_visited: &mut self.recently_visited,
        }
    }
}

fn process_neighbors_with_look_ahead<F>(
    neighbors: &[u32],
    mut process_neighbor: F,
    look_ahead: Option<usize>,
    dist_calc: &impl DistCalculator,
) where
    F: FnMut(u32),
{
    match look_ahead {
        Some(look_ahead) => {
            for i in 0..neighbors.len().saturating_sub(look_ahead) {
                dist_calc.prefetch(neighbors[i + look_ahead]);
                process_neighbor(neighbors[i]);
            }
            for neighbor in &neighbors[neighbors.len().saturating_sub(look_ahead)..] {
                process_neighbor(*neighbor);
            }
        }
        None => {
            for neighbor in neighbors.iter() {
                process_neighbor(*neighbor);
            }
        }
    }
}

#[inline]
fn furthest_distance(results: &BinaryHeap<OrderedNode>) -> OrderedFloat {
    results
        .peek()
        .map(|node| node.dist)
        .unwrap_or(OrderedFloat(f32::INFINITY))
}

#[inline]
fn push_result(results: &mut BinaryHeap<OrderedNode>, candidate: OrderedNode, k: usize) {
    if results.len() < k {
        results.push(candidate);
    } else if candidate.dist < results.peek().unwrap().dist {
        results.pop();
        results.push(candidate);
    }
}

macro_rules! beam_search_loop {
    (
        $candidates:ident,
        $results:ident,
        $visited:ident,
        $k:expr,
        $dist_calc:expr,
        $prefetch_distance:expr,
        $accepts_result:expr,
        |$current:ident, $process_neighbor:ident| $visit_neighbors:block
    ) => {{
        while !$candidates.is_empty() {
            let $current = $candidates.pop().expect("candidates is empty").0;
            let furthest = furthest_distance(&$results);

            if $current.dist > furthest && $results.len() == $k {
                break;
            }

            let $process_neighbor = |neighbor: u32| {
                if $visited.contains(neighbor) {
                    return;
                }
                $visited.insert(neighbor);
                let dist: OrderedFloat = $dist_calc.distance(neighbor).into();
                // Algorithm 2 refreshes the furthest result after every
                // update to W, including updates made by an earlier neighbor
                // of this same expanded node.
                let furthest = furthest_distance(&$results);
                if dist <= furthest || $results.len() < $k {
                    if $accepts_result(neighbor, dist) {
                        push_result(&mut $results, (dist, neighbor).into(), $k);
                    }
                    $candidates.push(Reverse((dist, neighbor).into()));
                }
            };
            $visit_neighbors
        }
    }};
}

macro_rules! greedy_search_loop {
    (
        $current:ident,
        $closest_dist:ident,
        $dist_calc:expr,
        $prefetch_distance:expr,
        |$process_neighbor:ident| $visit_neighbors:block
    ) => {{
        loop {
            let mut next = None;
            let $process_neighbor = |neighbor: u32| {
                let dist = $dist_calc.distance(neighbor);
                if dist < $closest_dist {
                    $closest_dist = dist;
                    next = Some(neighbor);
                }
            };
            $visit_neighbors

            if let Some(next) = next {
                $current = next;
            } else {
                break;
            }
        }
    }};
}

/// Beam search over a graph
///
/// This is the same as ``search-layer`` in HNSW.
///
/// Parameters
/// ----------
/// graph : Graph
///  The graph to search.
/// start : &[OrderedNode]
///  The starting point.
/// query : &[f32]
///  The query vector.
/// k : usize
///  The number of results to return.
/// bitset : Option<&RoaringBitmap>
///  The bitset of node IDs to filter the results, bit 1 for the node to keep, and bit 0 for the node to discard.
///
/// Returns
/// -------
/// A descending sorted list of ``(dist, node_id)`` pairs.
///
/// WARNING: Internal API,  API stability is not guaranteed
///
/// TODO: This isn't actually beam search, function should probably be renamed
pub fn beam_search(
    graph: &dyn Graph,
    ep: &OrderedNode,
    params: &HnswQueryParams,
    dist_calc: &impl DistCalculator,
    bitset: Option<&Visited>,
    prefetch_distance: Option<usize>,
    visited: &mut Visited,
) -> Vec<OrderedNode> {
    let k = params.ef;
    let mut candidates = BinaryHeap::with_capacity(k);
    visited.insert(ep.id);
    candidates.push(Reverse(ep.clone()));

    let mut results = BinaryHeap::with_capacity(k);
    let no_filter =
        bitset.is_none() && params.lower_bound.is_none() && params.upper_bound.is_none();

    if no_filter {
        results.push(ep.clone());
        let accepts_result = |_: u32, _: OrderedFloat| true;
        beam_search_loop!(
            candidates,
            results,
            visited,
            k,
            dist_calc,
            prefetch_distance,
            accepts_result,
            |current, process_neighbor| {
                let neighbors = graph.neighbors(current.id);
                process_neighbors_with_look_ahead(
                    &neighbors,
                    process_neighbor,
                    prefetch_distance,
                    dist_calc,
                );
            }
        );
        return results.into_sorted_vec();
    }

    // add range search support
    let lower_bound: OrderedFloat = params.lower_bound.unwrap_or(f32::MIN).into();
    let upper_bound: OrderedFloat = params.upper_bound.unwrap_or(f32::MAX).into();

    if bitset.map(|bitset| bitset.contains(ep.id)).unwrap_or(true)
        && ep.dist >= lower_bound
        && ep.dist < upper_bound
    {
        results.push(ep.clone());
    }

    let accepts_result = |node_id: u32, dist: OrderedFloat| {
        bitset
            .map(|bitset| bitset.contains(node_id))
            .unwrap_or(true)
            && dist >= lower_bound
            && dist < upper_bound
    };
    beam_search_loop!(
        candidates,
        results,
        visited,
        k,
        dist_calc,
        prefetch_distance,
        accepts_result,
        |current, process_neighbor| {
            let neighbors = graph.neighbors(current.id);
            process_neighbors_with_look_ahead(
                &neighbors,
                process_neighbor,
                prefetch_distance,
                dist_calc,
            );
        }
    );
    results.into_sorted_vec()
}

pub fn beam_search_borrowed(
    graph: &impl BorrowingGraph,
    ep: &OrderedNode,
    params: &HnswQueryParams,
    dist_calc: &impl DistCalculator,
    bitset: Option<&Visited>,
    prefetch_distance: Option<usize>,
    visited: &mut Visited,
) -> Vec<OrderedNode> {
    let k = params.ef;
    let mut candidates = BinaryHeap::with_capacity(k);
    visited.insert(ep.id);
    candidates.push(Reverse(ep.clone()));

    let mut results = BinaryHeap::with_capacity(k);
    let no_filter =
        bitset.is_none() && params.lower_bound.is_none() && params.upper_bound.is_none();

    if no_filter {
        results.push(ep.clone());
        let accepts_result = |_: u32, _: OrderedFloat| true;
        beam_search_loop!(
            candidates,
            results,
            visited,
            k,
            dist_calc,
            prefetch_distance,
            accepts_result,
            |current, process_neighbor| {
                let neighbors = graph.neighbors(current.id);
                process_neighbors_with_look_ahead(
                    neighbors,
                    process_neighbor,
                    prefetch_distance,
                    dist_calc,
                );
            }
        );
        return results.into_sorted_vec();
    }

    let lower_bound: OrderedFloat = params.lower_bound.unwrap_or(f32::MIN).into();
    let upper_bound: OrderedFloat = params.upper_bound.unwrap_or(f32::MAX).into();

    if bitset.map(|bitset| bitset.contains(ep.id)).unwrap_or(true)
        && ep.dist >= lower_bound
        && ep.dist < upper_bound
    {
        results.push(ep.clone());
    }

    let accepts_result = |node_id: u32, dist: OrderedFloat| {
        bitset
            .map(|bitset| bitset.contains(node_id))
            .unwrap_or(true)
            && dist >= lower_bound
            && dist < upper_bound
    };
    beam_search_loop!(
        candidates,
        results,
        visited,
        k,
        dist_calc,
        prefetch_distance,
        accepts_result,
        |current, process_neighbor| {
            let neighbors = graph.neighbors(current.id);
            process_neighbors_with_look_ahead(
                neighbors,
                process_neighbor,
                prefetch_distance,
                dist_calc,
            );
        }
    );
    results.into_sorted_vec()
}

/// Number of mask-passing nodes used to seed [beam_search_acorn]'s frontier.
const ACORN_SEED_COUNT: usize = 16;

/// Cap on starved-frontier waypoint expansions in [beam_search_acorn],
/// as a multiple of `ef`.
const ACORN_BRIDGE_BUDGET_FACTOR: usize = 4;

/// Beam search over the mask-passing subgraph (ACORN-1).
///
/// Only nodes in `bitset` get distances. A filtered-out neighbor contributes
/// its own neighbors instead, expanded once via `expanded`. Deeper masked
/// chains are crossed through unscored waypoints under a budget. The frontier
/// starts from the entry point plus mask-sampled seeds. May return fewer than
/// `min(ef, passing)` results if the budget runs out, so callers needing a
/// guarantee must check the count.
#[allow(clippy::too_many_arguments)]
pub fn beam_search_acorn(
    graph: &impl BorrowingGraph,
    ep: &OrderedNode,
    params: &HnswQueryParams,
    dist_calc: &impl DistCalculator,
    bitset: &Visited,
    prefetch_distance: Option<usize>,
    visited: &mut Visited,
    expanded: &mut Visited,
) -> Vec<OrderedNode> {
    let ef = params.ef;
    let lower_bound: OrderedFloat = params.lower_bound.unwrap_or(f32::MIN).into();
    let upper_bound: OrderedFloat = params.upper_bound.unwrap_or(f32::MAX).into();
    let passing_total = bitset.count_ones();
    let mut candidates = BinaryHeap::with_capacity(ef);
    let mut results = BinaryHeap::with_capacity(ef);
    // collected per node before scoring so prefetch targets are the ids
    // that actually get distances
    let mut passing: Vec<u32> = Vec::with_capacity(64);
    // masked nodes seen two hops out, expandable if the frontier starves,
    // deduped against `expanded` at pop rather than at push
    let mut waypoints: VecDeque<u32> = VecDeque::new();
    let mut bridge_budget = ACORN_BRIDGE_BUDGET_FACTOR * ef;

    // the entry point seeds the traversal even if it fails the mask
    visited.insert(ep.id);
    candidates.push(Reverse(ep.clone()));
    if bitset.contains(ep.id) && ep.dist >= lower_bound && ep.dist < upper_bound {
        results.push(ep.clone());
    }

    let stride = (passing_total / ACORN_SEED_COUNT).max(1);
    for seed in bitset.iter_ones().step_by(stride).take(ACORN_SEED_COUNT) {
        let seed = seed as u32;
        if visited.contains(seed) {
            continue;
        }
        visited.insert(seed);
        let dist: OrderedFloat = dist_calc.distance(seed).into();
        if dist >= lower_bound && dist < upper_bound {
            push_result(&mut results, (dist, seed).into(), ef);
        }
        candidates.push(Reverse((dist, seed).into()));
    }

    loop {
        let Some(Reverse(current)) = candidates.pop() else {
            // frontier starved: burn bridge budget through masked waypoints
            // until a new passing node is found
            if results.len() >= ef.min(passing_total) {
                break;
            }
            let mut found = false;
            while let Some(waypoint) = waypoints.pop_front() {
                if bridge_budget == 0 {
                    break;
                }
                if expanded.contains(waypoint) {
                    continue;
                }
                expanded.insert(waypoint);
                bridge_budget -= 1;
                for &neighbor in graph.neighbors(waypoint) {
                    if bitset.contains(neighbor) {
                        if !visited.contains(neighbor) {
                            visited.insert(neighbor);
                            let dist: OrderedFloat = dist_calc.distance(neighbor).into();
                            if dist >= lower_bound && dist < upper_bound {
                                push_result(&mut results, (dist, neighbor).into(), ef);
                            }
                            candidates.push(Reverse((dist, neighbor).into()));
                            found = true;
                        }
                    } else if !expanded.contains(neighbor) {
                        waypoints.push_back(neighbor);
                    }
                }
                if found {
                    break;
                }
            }
            if !found {
                break;
            }
            continue;
        };
        if current.dist > furthest_distance(&results) && results.len() == ef {
            break;
        }

        passing.clear();
        for &neighbor in graph.neighbors(current.id) {
            if bitset.contains(neighbor) {
                if !visited.contains(neighbor) {
                    visited.insert(neighbor);
                    passing.push(neighbor);
                }
            } else if !expanded.contains(neighbor) {
                expanded.insert(neighbor);
                for &second_hop in graph.neighbors(neighbor) {
                    if bitset.contains(second_hop) {
                        if !visited.contains(second_hop) {
                            visited.insert(second_hop);
                            passing.push(second_hop);
                        }
                    } else if !expanded.contains(second_hop) {
                        waypoints.push_back(second_hop);
                    }
                }
            }
        }

        process_neighbors_with_look_ahead(
            &passing,
            |node| {
                let dist: OrderedFloat = dist_calc.distance(node).into();
                if dist <= furthest_distance(&results) || results.len() < ef {
                    if dist >= lower_bound && dist < upper_bound {
                        push_result(&mut results, (dist, node).into(), ef);
                    }
                    candidates.push(Reverse((dist, node).into()));
                }
            },
            prefetch_distance,
            dist_calc,
        );
    }
    results.into_sorted_vec()
}

/// Greedy search over a graph
///
/// This searches for only one result, only used for finding the entry point
///
/// Parameters
/// ----------
/// graph : Graph
///    The graph to search.
/// start : u32
///   The index starting point.
/// query : &[f32]
///   The query vector.
///
/// Returns
/// -------
/// A ``(dist, node_id)`` pair.
///
/// WARNING: Internal API,  API stability is not guaranteed
pub fn greedy_search(
    graph: &dyn Graph,
    start: OrderedNode,
    dist_calc: &impl DistCalculator,
    prefetch_distance: Option<usize>,
) -> OrderedNode {
    let mut current = start.id;
    let mut closest_dist = start.dist.0;
    greedy_search_loop!(
        current,
        closest_dist,
        dist_calc,
        prefetch_distance,
        |process_neighbor| {
            let neighbors = graph.neighbors(current);
            process_neighbors_with_look_ahead(
                &neighbors,
                process_neighbor,
                prefetch_distance,
                dist_calc,
            );
        }
    );
    OrderedNode::new(current, closest_dist.into())
}

pub fn greedy_search_borrowed(
    graph: &impl BorrowingGraph,
    start: OrderedNode,
    dist_calc: &impl DistCalculator,
    prefetch_distance: Option<usize>,
) -> OrderedNode {
    let mut current = start.id;
    let mut closest_dist = start.dist.0;
    greedy_search_loop!(
        current,
        closest_dist,
        dist_calc,
        prefetch_distance,
        |process_neighbor| {
            let neighbors = graph.neighbors(current);
            process_neighbors_with_look_ahead(
                neighbors,
                process_neighbor,
                prefetch_distance,
                dist_calc,
            );
        }
    );
    OrderedNode::new(current, closest_dist.into())
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    struct ChainGraph {
        neighbors: Vec<Vec<u32>>,
    }

    impl BorrowingGraph for ChainGraph {
        fn len(&self) -> usize {
            self.neighbors.len()
        }

        fn neighbors(&self, key: u32) -> &[u32] {
            &self.neighbors[key as usize]
        }
    }

    struct ZeroDistance;

    impl DistCalculator for ZeroDistance {
        fn distance(&self, _id: u32) -> f32 {
            0.0
        }

        fn distance_all(&self, _k_hint: usize) -> Vec<f32> {
            Vec::new()
        }
    }

    struct CountingDistCalculator<'a> {
        distances: &'a [f32],
        computations: Cell<usize>,
    }

    impl DistCalculator for CountingDistCalculator<'_> {
        fn distance(&self, id: u32) -> f32 {
            self.computations.set(self.computations.get() + 1);
            self.distances[id as usize]
        }

        fn distance_all(&self, _k_hint: usize) -> Vec<f32> {
            self.distances.to_vec()
        }
    }

    #[test]
    fn test_beam_search_refreshes_furthest_neighbor_bound() {
        // The entry point is closer than the first candidates. Once those
        // candidates fill W, its furthest distance grows from 1 to 10. The
        // next neighbor must be compared with that new bound so it can enter
        // the candidate queue and lead the search to node 4.
        let graph = ChainGraph {
            neighbors: vec![vec![1, 2, 3], vec![], vec![], vec![4], vec![]],
        };
        let distances = [1.0, 10.0, 9.0, 8.0, 7.0];
        let dist_calc = CountingDistCalculator {
            distances: &distances,
            computations: Cell::new(0),
        };
        let params = HnswQueryParams {
            ef: 3,
            lower_bound: None,
            upper_bound: None,
            dist_q_c: 0.0,
            use_acorn: false,
        };
        let entry = OrderedNode::new(0, distances[0].into());
        let mut visited_generator = VisitedGenerator::new(graph.len());

        let results = beam_search_borrowed(
            &graph,
            &entry,
            &params,
            &dist_calc,
            None,
            None,
            &mut visited_generator.generate(graph.len()),
        );

        assert_eq!(
            results.iter().map(|node| node.id).collect::<Vec<_>>(),
            vec![0, 4, 3]
        );
        assert_eq!(dist_calc.computations.get(), 4);
    }

    /// Passing components joined only through chains of two masked nodes
    /// must still all be found (from review: without waypoint expansion
    /// only the seeded nodes return).
    #[test]
    fn test_acorn_reaches_across_masked_chains() {
        const PASSING_COUNT: usize = 20;
        const FAILING_COUNT: usize = (PASSING_COUNT - 1) * 2;
        let mut neighbors = vec![Vec::new(); PASSING_COUNT + FAILING_COUNT];
        for index in 0..PASSING_COUNT - 1 {
            let left = index as u32;
            let first_failing = (PASSING_COUNT + index * 2) as u32;
            let second_failing = first_failing + 1;
            let right = left + 1;

            neighbors[left as usize].push(first_failing);
            neighbors[first_failing as usize].extend([left, second_failing]);
            neighbors[second_failing as usize].extend([first_failing, right]);
            neighbors[right as usize].push(second_failing);
        }
        let graph = ChainGraph { neighbors };
        let params = HnswQueryParams {
            ef: 30,
            lower_bound: None,
            upper_bound: None,
            dist_q_c: 0.0,
            use_acorn: false,
        };
        let entry = OrderedNode::new(0, 0.0.into());

        let mut mask_generator = VisitedGenerator::new(graph.len());
        let mut mask = mask_generator.generate(graph.len());
        for id in 0..PASSING_COUNT as u32 {
            mask.insert(id);
        }

        let mut acorn_visited_generator = VisitedGenerator::new(graph.len());
        let mut acorn_expanded_generator = VisitedGenerator::new(graph.len());
        let acorn_results = beam_search_acorn(
            &graph,
            &entry,
            &params,
            &ZeroDistance,
            &mask,
            None,
            &mut acorn_visited_generator.generate(graph.len()),
            &mut acorn_expanded_generator.generate(graph.len()),
        );

        let mut basic_visited_generator = VisitedGenerator::new(graph.len());
        let basic_results = beam_search_borrowed(
            &graph,
            &entry,
            &params,
            &ZeroDistance,
            Some(&mask),
            None,
            &mut basic_visited_generator.generate(graph.len()),
        );

        assert_eq!(basic_results.len(), PASSING_COUNT);
        assert_eq!(acorn_results.len(), PASSING_COUNT);
        assert!(acorn_results.iter().all(|node| mask.contains(node.id)));
    }
}
