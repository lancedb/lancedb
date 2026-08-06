// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::deepsize::DeepSizeOf;

use super::OrderedFloat;
use super::OrderedNode;
use std::sync::Arc;

/// GraphNode during build.
///
/// WARNING: Internal API,  API stability is not guaranteed
#[derive(Debug, Clone, DeepSizeOf)]
pub struct GraphBuilderNode {
    /// neighbors of each level of the node.
    pub(crate) bottom_neighbors: Arc<Vec<u32>>,
    pub(crate) level_neighbors: Vec<Arc<Vec<u32>>>,
    pub(crate) level_neighbors_ranked: Vec<Vec<OrderedNode>>,
}

impl GraphBuilderNode {
    /// Construct a node from already-computed parts. Used by the online
    /// builder to convert its mutable state into an immutable `HNSW`.
    pub(crate) fn from_parts(
        level_neighbors: Vec<Arc<Vec<u32>>>,
        level_neighbors_ranked: Vec<Vec<OrderedNode>>,
        bottom_neighbors: Arc<Vec<u32>>,
    ) -> Self {
        Self {
            bottom_neighbors,
            level_neighbors,
            level_neighbors_ranked,
        }
    }

    pub(crate) fn new(_id: u32, max_level: usize) -> Self {
        let bottom_neighbors = Arc::new(Vec::new());
        let level_neighbors = (0..max_level).map(|_| Arc::new(Vec::new())).collect();
        let level_neighbors_ranked = (0..max_level).map(|_| Vec::new()).collect();
        Self {
            bottom_neighbors,
            level_neighbors,
            level_neighbors_ranked,
        }
    }

    pub(crate) fn add_neighbor(&mut self, v: u32, dist: OrderedFloat, level: u16) {
        self.level_neighbors_ranked[level as usize].push(OrderedNode { dist, id: v });
    }

    pub(crate) fn update_from_ranked_neighbors(&mut self, level: u16) {
        let level_index = level as usize;
        self.level_neighbors[level_index] = Arc::new(
            self.level_neighbors_ranked[level_index]
                .iter()
                .map(|ordered_node| ordered_node.id)
                .collect(),
        );
        if level == 0 {
            self.bottom_neighbors = self.level_neighbors[0].clone();
        }
    }
}
