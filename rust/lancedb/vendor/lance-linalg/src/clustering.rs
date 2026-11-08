// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Clustering algorithms.
//!

use arrow_array::UInt32Array;
use num_traits::Num;

use crate::Result;

/// Clustering Trait.
pub trait Clustering<T: Num> {
    /// The dimension of the vectors.
    fn dimension(&self) -> u32;

    /// The number of clusters.
    fn num_clusters(&self) -> u32;

    /// Find n-nearest partitions for the given query.
    ///
    /// ## Parameters
    /// * `query`: a `D`-dimensional query vector.
    /// * `nprobes`: The number of probes to return.
    ///
    /// ## Returns
    ///
    /// `n` of nearest partitions.
    fn find_partitions(&self, query: &[T], nprobes: usize) -> Result<UInt32Array>;

    /// Get the n nearest partitions for an array of vectors.
    ///
    /// ## Parameters:
    /// * `data`: an `N * D` of D-dimensional vectors.
    /// * `nprobes`: If provided, the number of partitions per vector to return.
    ///   If not provided, return 1 partition per vector.
    ///
    /// ## Returns:
    /// * An `N * nprobes` matrix of partition IDs.
    fn compute_membership(&self, data: &[T], nprobes: Option<usize>) -> Vec<Option<u32>>;
}
