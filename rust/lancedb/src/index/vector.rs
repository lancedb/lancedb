// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Vector indices are approximate indices that are used to find rows similar to
//! a query vector.  Vector indices speed up vector searches.
//!
//! Vector indices are only supported on fixed-size-list (tensor) columns of floating point
//! values
use std::cmp::max;

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
            .map(|field_id| {
                manifest
                    .schema
                    .field_by_id(*field_id)
                    .unwrap_or_else(|| {
                        panic!(
                            "field {field_id} of index {} must exist in schema",
                            index.name
                        )
                    })
                    .name
                    .clone()
            })
            .collect();
        Self {
            columns: fields,
            index_name: index.name.clone(),
            index_uuid: index.uuid.to_string(),
        }
    }
}

macro_rules! impl_distance_type_setter {
    () => {
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
    };
}

macro_rules! impl_ivf_params_setter {
    () => {
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

        /// The rate used to calculate the number of training vectors for kmeans.
        ///
        /// When an IVF index is trained, we need to calculate partitions.  These are groups
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
        /// When training an IVF index we use kmeans to calculate the partitions.  This parameter
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

        /// The target size of each partition.
        ///
        /// This value controls the tradeoff between search performance and accuracy.
        /// The higher the value the faster the search but the less accurate the results will be.
        pub fn target_partition_size(mut self, target_partition_size: u32) -> Self {
            self.target_partition_size = Some(target_partition_size);
            self
        }
    };
}

macro_rules! impl_pq_params_setter {
    () => {
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
        pub fn num_bits(mut self, num_bits: u32) -> Self {
            self.num_bits = Some(num_bits);
            self
        }
    };
}

macro_rules! impl_hnsw_params_setter {
    () => {
        /// The number of neighbors to select for each vector in the HNSW graph.
        /// This value controls the tradeoff between search speed and accuracy.
        /// The higher the value the more accurate the search but the slower it will be.
        /// The default value is 20.
        pub fn num_edges(mut self, m: u32) -> Self {
            self.m = m;
            self
        }

        /// The number of candidates to evaluate during the construction of the HNSW graph.
        /// This value controls the tradeoff between build speed and accuracy.
        /// The higher the value the more accurate the build but the slower it will be.
        /// This value should be set to a value that is not less than `ef` in the search phase.
        /// The default value is 300.
        pub fn ef_construction(mut self, ef_construction: u32) -> Self {
            self.ef_construction = ef_construction;
            self
        }
    };
}

/// Builder for an IVF Flat index.
///
/// This index stores raw vectors. These vectors are grouped into partitions of similar vectors.
/// Each partition keeps track of a centroid which is the average value of all vectors in the group.
///
/// During a query the centroids are compared with the query vector to find the closest partitions.
/// The raw vectors in these partitions are then searched to find the closest vectors.
///
/// The partitioning process is called IVF and the `num_partitions` parameter controls how many groups to create.
///
/// Note that training an IVF Flat index on a large dataset is a slow operation and currently is also a memory intensive operation.
#[derive(Debug, Clone)]
pub struct IvfFlatIndexBuilder {
    pub(crate) distance_type: DistanceType,

    // IVF
    pub(crate) num_partitions: Option<u32>,
    pub(crate) sample_rate: u32,
    pub(crate) max_iterations: u32,
    pub(crate) target_partition_size: Option<u32>,
}

impl Default for IvfFlatIndexBuilder {
    fn default() -> Self {
        Self {
            distance_type: DistanceType::L2,
            num_partitions: None,
            sample_rate: 256,
            max_iterations: 50,
            target_partition_size: None,
        }
    }
}

impl IvfFlatIndexBuilder {
    impl_distance_type_setter!();
    impl_ivf_params_setter!();
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

    // IVF
    pub(crate) num_partitions: Option<u32>,
    pub(crate) sample_rate: u32,
    pub(crate) max_iterations: u32,
    pub(crate) target_partition_size: Option<u32>,

    // PQ
    pub(crate) num_sub_vectors: Option<u32>,
    pub(crate) num_bits: Option<u32>,
}

impl Default for IvfPqIndexBuilder {
    fn default() -> Self {
        Self {
            distance_type: DistanceType::L2,
            num_partitions: None,
            num_sub_vectors: None,
            num_bits: None,
            sample_rate: 256,
            max_iterations: 50,
            target_partition_size: None,
        }
    }
}

impl IvfPqIndexBuilder {
    impl_distance_type_setter!();
    impl_ivf_params_setter!();
    impl_pq_params_setter!();
}

pub(crate) fn suggested_num_partitions(rows: usize) -> u32 {
    let num_partitions = (rows as f64).sqrt() as u32;
    max(1, num_partitions)
}

pub(crate) fn suggested_num_partitions_for_hnsw(rows: usize, dim: u32) -> u32 {
    let num_partitions = (((rows as u64) * (dim as u64)) / (256 * 5_000_000)) as u32;
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

/// Builder for an IVF HNSW PQ index.
///
/// This index is a combination of IVF and HNSW.
/// The IVF part is the same as the IVF PQ index.
/// For each IVF partition, this builds a HNSW graph, the graph is used to
/// quickly find the closest vectors to a query vector.
///
/// The PQ (product quantizer) is used to compress the vectors as the same as IVF PQ.
#[derive(Debug, Clone)]
pub struct IvfHnswPqIndexBuilder {
    // IVF
    pub(crate) distance_type: DistanceType,
    pub(crate) num_partitions: Option<u32>,
    pub(crate) sample_rate: u32,
    pub(crate) max_iterations: u32,
    pub(crate) target_partition_size: Option<u32>,

    // HNSW
    pub(crate) m: u32,
    pub(crate) ef_construction: u32,

    // PQ
    pub(crate) num_sub_vectors: Option<u32>,
    pub(crate) num_bits: Option<u32>,
}

impl Default for IvfHnswPqIndexBuilder {
    fn default() -> Self {
        Self {
            distance_type: DistanceType::L2,
            num_partitions: None,
            num_sub_vectors: None,
            num_bits: None,
            sample_rate: 256,
            max_iterations: 50,
            m: 20,
            ef_construction: 300,
            target_partition_size: None,
        }
    }
}

impl IvfHnswPqIndexBuilder {
    impl_distance_type_setter!();
    impl_ivf_params_setter!();
    impl_hnsw_params_setter!();
    impl_pq_params_setter!();
}

/// Builder for an IVF_HNSW_SQ index.
///
/// This index is a combination of IVF and HNSW.
/// The IVF part is the same as the IVF PQ index.
/// For each IVF partition, this builds a HNSW graph, the graph is used to
/// quickly find the closest vectors to a query vector.
///
/// The SQ (scalar quantizer) is used to compress the vectors,
/// each vector is mapped to a 8-bit integer vector, 4x compression ratio for float32 vector.
#[derive(Debug, Clone)]
pub struct IvfHnswSqIndexBuilder {
    // IVF
    pub(crate) distance_type: DistanceType,
    pub(crate) num_partitions: Option<u32>,
    pub(crate) sample_rate: u32,
    pub(crate) max_iterations: u32,
    pub(crate) target_partition_size: Option<u32>,

    // HNSW
    pub(crate) m: u32,
    pub(crate) ef_construction: u32,
    // SQ
    // TODO add num_bits for SQ after it supports another num_bits besides 8
}

impl Default for IvfHnswSqIndexBuilder {
    fn default() -> Self {
        Self {
            distance_type: DistanceType::L2,
            num_partitions: None,
            sample_rate: 256,
            max_iterations: 50,
            m: 20,
            ef_construction: 300,
            target_partition_size: None,
        }
    }
}

impl IvfHnswSqIndexBuilder {
    impl_distance_type_setter!();
    impl_ivf_params_setter!();
    impl_hnsw_params_setter!();
}
