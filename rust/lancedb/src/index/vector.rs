// Copyright 2023 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Vector indices are approximate indices that are used to find rows similar to
//! a query vector.  Vector indices speed up vector searches.
//!
//! Vector indices are only supported on fixed-size-list (tensor) columns of floating point
//! values
use std::cmp::max;

use serde::Deserialize;

use lance::table::format::{Index, Manifest};

use crate::DistanceType;

pub struct VectorIndex {
    pub columns: Vec<String>,
    pub index_name: String,
    pub index_uuid: String,
}

impl VectorIndex {
    pub fn new_from_format(manifest: &Manifest, index: &Index) -> Self {
        let fields = index
            .fields
            .iter()
            .map(|i| manifest.schema.fields[*i as usize].name.clone())
            .collect();
        Self {
            columns: fields,
            index_name: index.name.clone(),
            index_uuid: index.uuid.to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct VectorIndexStatistics {
    pub num_indexed_rows: usize,
    pub num_unindexed_rows: usize,
}

/// Builder for an IVF PQ index.
///
/// This index stores a compressed (quantized) copy of every vector.  These vectors
/// are grouped into partitions of similar vectors.  Each partition keeps track of
/// a centroid which is the average value of all vectors in the group.
///
/// During a query the centroids are compared with the query vector to find the closest
/// partitions.  The compressed vectors in these partitions are then searched to find
/// the closest vectors.
///
/// The compression scheme is called product quantization.  Each vector is divided into
/// subvectors and then each subvector is quantized into a small number of bits.  the
/// parameters `num_bits` and `num_subvectors` control this process, providing a tradeoff
/// between index size (and thus search speed) and index accuracy.
///
/// The partitioning process is called IVF and the `num_partitions` parameter controls how
/// many groups to create.
///
/// Note that training an IVF PQ index on a large dataset is a slow operation and
/// currently is also a memory intensive operation.
#[derive(Debug, Clone)]
pub struct IvfPqIndexBuilder {
    pub(crate) distance_type: DistanceType,
    pub(crate) num_partitions: Option<u32>,
    pub(crate) num_sub_vectors: Option<u32>,
    pub(crate) sample_rate: u32,
    pub(crate) max_iterations: u32,
}

impl Default for IvfPqIndexBuilder {
    fn default() -> Self {
        Self {
            distance_type: DistanceType::L2,
            num_partitions: None,
            num_sub_vectors: None,
            sample_rate: 256,
            max_iterations: 50,
        }
    }
}

impl IvfPqIndexBuilder {
    /// [DistanceType] to use to build the index.
    ///
    /// Default value is [DistanceType::L2].
    ///
    /// This is used when training the index to calculate the IVF partitions (vectors are
    /// grouped in partitions with similar vectors according to this distance type) and to
    /// calculate a subvector's code during quantization.
    ///
    /// The metric type used to train an index MUST match the metric type used to search the
    /// index.  Failure to do so will yield inaccurate results.
    pub fn distance_type(mut self, distance_type: DistanceType) -> Self {
        self.distance_type = distance_type;
        self
    }

    /// The number of IVF partitions to create.
    ///
    /// This value should generally scale with the number of rows in the dataset.  By default
    /// the number of partitions is the square root of the number of rows.
    ///
    /// If this value is too large then the first part of the search (picking the right partition)
    /// will be slow.  If this value is too small then the second part of the search (searching
    /// within a partition) will be slow.
    pub fn num_partitions(mut self, num_partitions: u32) -> Self {
        self.num_partitions = Some(num_partitions);
        self
    }

    /// Number of sub-vectors of PQ.
    ///
    /// This value controls how much the vector is compressed during the quantization step.
    /// The more sub vectors there are the less the vector is compressed.  The default is
    /// the dimension of the vector divided by 16.  If the dimension is not evenly divisible
    /// by 16 we use the dimension divded by 8.
    ///
    /// The above two cases are highly preferred.  Having 8 or 16 values per subvector allows
    /// us to use efficient SIMD instructions.
    ///
    /// If the dimension is not visible by 8 then we use 1 subvector.  This is not ideal and
    /// will likely result in poor performance.
    pub fn num_sub_vectors(mut self, num_sub_vectors: u32) -> Self {
        self.num_sub_vectors = Some(num_sub_vectors);
        self
    }

    /// The rate used to calculate the number of training vectors for kmeans.
    ///
    /// When an IVF PQ index is trained, we need to calculate partitions.  These are groups
    /// of vectors that are similar to each other.  To do this we use an algorithm called kmeans.
    ///
    /// Running kmeans on a large dataset can be slow.  To speed this up we run kmeans on a
    /// random sample of the data.  This parameter controls the size of the sample.  The total
    /// number of vectors used to train the index is `sample_rate * num_partitions`.
    ///
    /// Increasing this value might improve the quality of the index but in most cases the
    /// default should be sufficient.
    ///
    /// The default value is 256.
    pub fn sample_rate(mut self, sample_rate: u32) -> Self {
        self.sample_rate = sample_rate;
        self
    }

    /// Max iterations to train kmeans.
    ///
    /// When training an IVF PQ index we use kmeans to calculate the partitions.  This parameter
    /// controls how many iterations of kmeans to run.
    ///
    /// Increasing this might improve the quality of the index but in most cases the parameter
    /// is unused because kmeans will converge with fewer iterations.  The parameter is only
    /// used in cases where kmeans does not appear to converge.  In those cases it is unlikely
    /// that setting this larger will lead to the index converging anyways.
    ///
    /// The default value is 50.
    pub fn max_iterations(mut self, max_iterations: u32) -> Self {
        self.max_iterations = max_iterations;
        self
    }
}

pub(crate) fn suggested_num_partitions(rows: usize) -> u32 {
    let num_partitions = (rows as f64).sqrt() as u32;
    max(1, num_partitions)
}

pub(crate) fn suggested_num_sub_vectors(dim: u32) -> u32 {
    if dim % 16 == 0 {
        // Should be more aggressive than this default.
        dim / 16
    } else if dim % 8 == 0 {
        dim / 8
    } else {
        log::warn!(
            "The dimension of the vector is not divisible by 8 or 16, \
                which may cause performance degradation in PQ"
        );
        1
    }
}
