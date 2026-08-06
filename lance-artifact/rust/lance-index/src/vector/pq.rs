// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Product Quantization
//!

use std::sync::Arc;

use arrow::datatypes::{self, ArrowPrimitiveType};
use arrow_array::{Array, FixedSizeListArray, UInt8Array, cast::AsArray};
use arrow_array::{ArrayRef, Float32Array, PrimitiveArray};
use arrow_schema::{DataType, Field};
use distance::build_distance_table_dot;
use lance_arrow::*;
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, Result, assume_eq};
use lance_linalg::distance::{DistanceType, Dot, L2, l2::L2Prepared};
use lance_table::utils::LanceIteratorExtension;
use num_traits::Float;
use prost::Message;
use storage::{PQ_METADATA_KEY, ProductQuantizationMetadata, ProductQuantizationStorage};
use tracing::instrument;

pub mod builder;
pub mod distance;
pub mod storage;
pub mod transform;
pub(crate) mod utils;

use self::distance::{
    build_distance_table_l2, build_distance_table_l2_prepared, compute_pq_distance,
};
pub use self::utils::num_centroids;
use super::quantizer::{
    Quantization, QuantizationMetadata, QuantizationType, Quantizer, QuantizerBuildParams,
};
use super::{PQ_CODE_COLUMN, pb};
use crate::vector::kmeans::compute_partition;
pub use builder::PQBuildParams;
use utils::get_sub_vector_centroids;

#[derive(Debug, Clone)]
pub struct ProductQuantizer {
    pub num_sub_vectors: usize,
    pub num_bits: u32,
    pub dimension: usize,
    pub codebook: FixedSizeListArray,
    pub distance_type: DistanceType,
    /// Pre-transposed L2 targets per sub-vector for fast f32 L2 batch computation.
    /// Only populated when codebook is f32 and distance_type is L2
    /// (Cosine is converted to L2 before construction, so it benefits too).
    /// Wrapped in Arc so all clones (one per cached IVF partition) share one copy.
    l2_targets: Option<Arc<Vec<L2Prepared>>>,
}

impl DeepSizeOf for ProductQuantizer {
    fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
        self.codebook.get_array_memory_size()
            + self.num_sub_vectors.deep_size_of_children(_context)
            + self.num_bits.deep_size_of_children(_context)
            + self.dimension.deep_size_of_children(_context)
            + self.distance_type.deep_size_of_children(_context)
            // deep_size_of_children on the Arc de-duplicates shared allocations
            // via the context, so partitions sharing one l2_targets are counted once.
            + self.l2_targets.deep_size_of_children(_context)
    }
}

impl ProductQuantizer {
    /// Build per-sub-vector L2Prepared from the codebook if applicable (f32 + L2).
    fn build_l2_targets(
        codebook: &FixedSizeListArray,
        distance_type: DistanceType,
        num_sub_vectors: usize,
        num_bits: u32,
        dimension: usize,
    ) -> Option<Arc<Vec<L2Prepared>>> {
        if codebook.value_type() != DataType::Float32 || distance_type != DistanceType::L2 {
            return None;
        }
        let values = codebook
            .values()
            .as_primitive::<datatypes::Float32Type>()
            .values();
        let sub_dim = dimension / num_sub_vectors;
        let num_centroids = 2_usize.pow(num_bits);
        let block_size = sub_dim * num_centroids;

        let targets: Vec<L2Prepared> = (0..num_sub_vectors)
            .map(|sub_idx| {
                let block_start = sub_idx * block_size;
                let block = &values[block_start..block_start + block_size];
                L2Prepared::new(block, sub_dim)
            })
            .collect();
        Some(Arc::new(targets))
    }

    pub fn new(
        num_sub_vectors: usize,
        num_bits: u32,
        dimension: usize,
        codebook: FixedSizeListArray,
        distance_type: DistanceType,
    ) -> Self {
        let l2_targets = Self::build_l2_targets(
            &codebook,
            distance_type,
            num_sub_vectors,
            num_bits,
            dimension,
        );
        Self {
            num_bits,
            num_sub_vectors,
            dimension,
            codebook,
            distance_type,
            l2_targets,
        }
    }

    pub fn from_proto(proto: &pb::Pq, distance_type: DistanceType) -> Result<Self> {
        let distance_type = match distance_type {
            DistanceType::Cosine => DistanceType::L2,
            _ => distance_type,
        };
        let codebook = match proto.codebook_tensor.as_ref() {
            Some(tensor) => FixedSizeListArray::try_from(tensor)?,
            None => FixedSizeListArray::try_new_from_values(
                Float32Array::from(proto.codebook.clone()),
                proto.dimension as i32,
            )?,
        };
        Ok(Self::new(
            proto.num_sub_vectors as usize,
            proto.num_bits,
            proto.dimension as usize,
            codebook,
            distance_type,
        ))
    }

    #[instrument(name = "ProductQuantizer::transform", level = "debug", skip_all)]
    fn transform<T: ArrowPrimitiveType>(&self, vectors: &dyn Array) -> Result<ArrayRef>
    where
        T::Native: Float + L2 + Dot,
    {
        match self.num_bits {
            4 => self.transform_impl::<4, T>(vectors),
            8 => self.transform_impl::<8, T>(vectors),
            _ => Err(Error::index(format!(
                "ProductQuantization: num_bits {} not supported",
                self.num_bits
            ))),
        }
    }

    fn transform_impl<const NUM_BITS: u32, T: ArrowPrimitiveType>(
        &self,
        vectors: &dyn Array,
    ) -> Result<ArrayRef>
    where
        T::Native: Float + L2 + Dot,
    {
        let fsl = vectors
            .as_fixed_size_list_opt()
            .ok_or(Error::index(format!(
                "Expect to be a FixedSizeList<float> vector array, got: {:?} array",
                vectors.data_type()
            )))?;
        let num_sub_vectors = self.num_sub_vectors;
        let dim = self.dimension;
        if NUM_BITS == 4 && !num_sub_vectors.is_multiple_of(2) {
            return Err(Error::index(format!(
                "PQ: num_sub_vectors must be divisible by 2 for num_bits=4, but got {}",
                num_sub_vectors,
            )));
        }
        let codebook = self.codebook.values().as_primitive::<T>();

        let distance_type = self.distance_type;

        let flatten_data = fsl.values().as_primitive::<T>();
        let sub_dim = dim / num_sub_vectors;
        let num_centroids = 2_usize.pow(NUM_BITS);
        let total_code_length = fsl.len() * num_sub_vectors / (8 / NUM_BITS as usize);

        let values = if let Some(targets) = &self.l2_targets {
            // Fast path for f32 + L2: use pre-transposed codebook.
            // SAFETY: l2_targets is only populated when T::Native is f32.
            let flat_f32: &[f32] = unsafe {
                std::slice::from_raw_parts(
                    flatten_data.values().as_ptr() as *const f32,
                    flatten_data.values().len(),
                )
            };
            let mut values = vec![0u8; total_code_length];
            let bytes_per_vector = num_sub_vectors / (8 / NUM_BITS as usize);
            let mut dist_buf = vec![0.0f32; num_centroids];
            for (vec_idx, vector) in flat_f32.chunks_exact(dim).enumerate() {
                let out = &mut values[vec_idx * bytes_per_vector..][..bytes_per_vector];
                if NUM_BITS == 4 {
                    for (pair_idx, pair) in vector.chunks_exact(sub_dim * 2).enumerate() {
                        let lo = targets[pair_idx * 2]
                            .nearest_into(&pair[..sub_dim], &mut dist_buf)
                            .unwrap_or(0) as u8;
                        let hi = targets[pair_idx * 2 + 1]
                            .nearest_into(&pair[sub_dim..], &mut dist_buf)
                            .unwrap_or(0) as u8;
                        out[pair_idx] = (hi << 4) | lo;
                    }
                } else {
                    for (sub_idx, sv) in vector.chunks_exact(sub_dim).enumerate() {
                        out[sub_idx] = targets[sub_idx]
                            .nearest_into(sv, &mut dist_buf)
                            .unwrap_or(0) as u8;
                    }
                }
            }
            values
        } else {
            flatten_data
                .values()
                .chunks_exact(dim)
                .flat_map(|vector| {
                    let sub_vec_code: Vec<u8> = vector
                        .chunks_exact(sub_dim)
                        .enumerate()
                        .map(|(sub_idx, sub_vector)| {
                            let centroids = get_sub_vector_centroids::<NUM_BITS, _>(
                                codebook.values(),
                                dim,
                                num_sub_vectors,
                                sub_idx,
                            );
                            assume_eq!(centroids.len(), num_centroids * sub_dim);
                            compute_partition(centroids, sub_vector, distance_type).unwrap_or(0)
                                as u8
                        })
                        .collect();
                    if NUM_BITS == 4 {
                        sub_vec_code
                            .chunks_exact(2)
                            .map(|v| (v[1] << 4) | v[0])
                            .collect::<Vec<_>>()
                    } else {
                        sub_vec_code
                    }
                })
                .exact_size(total_code_length)
                .collect::<Vec<_>>()
        };

        let num_sub_vectors_in_byte = if NUM_BITS == 4 {
            num_sub_vectors / 2
        } else {
            num_sub_vectors
        };

        debug_assert_eq!(values.len(), fsl.len() * num_sub_vectors_in_byte);
        Ok(Arc::new(FixedSizeListArray::try_new_from_values(
            UInt8Array::from(values),
            num_sub_vectors_in_byte as i32,
        )?))
    }

    // the code must be transposed
    pub fn compute_distances(&self, query: &dyn Array, code: &UInt8Array) -> Result<Float32Array> {
        if code.is_empty() {
            return Ok(Float32Array::from(Vec::<f32>::new()));
        }

        match self.distance_type {
            DistanceType::L2 => self.l2_distances(query, code),
            DistanceType::Cosine => {
                // it seems we implemented cosine distance at some version,
                // but from now on, we should use normalized L2 distance.
                debug_assert!(
                    false,
                    "cosine distance should be converted to normalized L2 distance"
                );
                // L2 over normalized vectors:  ||x - y|| = x^2 + y^2 - 2 * xy = 1 + 1 - 2 * xy = 2 * (1 - xy)
                // Cosine distance: 1 - |xy| / (||x|| * ||y||) = 1 - xy / (x^2 * y^2) = 1 - xy / (1 * 1) = 1 - xy
                // Therefore, Cosine = L2 / 2
                let l2_dists = self.l2_distances(query, code)?;
                Ok(l2_dists.values().iter().map(|v| *v / 2.0).collect())
            }
            DistanceType::Dot => self.dot_distances(query, code),
            _ => panic!(
                "ProductQuantization: distance type {} not supported",
                self.distance_type
            ),
        }
    }

    /// Pre-compute L2 distance from the query to all code.
    ///
    /// It returns the squared L2 distance.
    fn l2_distances(&self, key: &dyn Array, code: &UInt8Array) -> Result<Float32Array> {
        let distance_table = self.build_l2_distance_table(key)?;
        Ok(self.compute_l2_distance(&distance_table, code.values()))
    }

    /// Parameters
    /// ----------
    ///  - query: the query vector, with shape (dimension, )
    ///  - code: the PQ code in one partition.
    ///
    fn dot_distances(&self, key: &dyn Array, code: &UInt8Array) -> Result<Float32Array> {
        match key.data_type() {
            DataType::Float16 => {
                self.dot_distances_impl::<datatypes::Float16Type>(key.as_primitive(), code)
            }
            DataType::Float32 => {
                self.dot_distances_impl::<datatypes::Float32Type>(key.as_primitive(), code)
            }
            DataType::Float64 => {
                self.dot_distances_impl::<datatypes::Float64Type>(key.as_primitive(), code)
            }
            _ => Err(Error::index(format!(
                "unsupported data type: {}",
                key.data_type()
            ))),
        }
    }

    fn dot_distances_impl<T: ArrowPrimitiveType>(
        &self,
        key: &PrimitiveArray<T>,
        code: &UInt8Array,
    ) -> Result<Float32Array>
    where
        T::Native: Dot,
    {
        let distance_table = build_distance_table_dot(
            self.codebook.values().as_primitive::<T>().values(),
            self.num_bits,
            self.num_sub_vectors,
            key.values(),
        );

        let distances = compute_pq_distance(
            &distance_table,
            self.num_bits,
            self.num_sub_vectors,
            code.values(),
            0,
        );

        let diff = self.num_sub_vectors as f32 - 1.0;
        let distances = distances.into_iter().map(|d| d - diff).collect::<Vec<_>>();
        Ok(distances.into())
    }

    fn build_l2_distance_table(&self, key: &dyn Array) -> Result<Vec<f32>> {
        match key.data_type() {
            DataType::Float16 => {
                Ok(self.build_l2_distance_table_impl::<datatypes::Float16Type>(key.as_primitive()))
            }
            DataType::Float32 => {
                if let Some(targets) = &self.l2_targets {
                    let query = key.as_primitive::<datatypes::Float32Type>().values();
                    Ok(build_distance_table_l2_prepared(targets.as_slice(), query))
                } else {
                    Ok(self
                        .build_l2_distance_table_impl::<datatypes::Float32Type>(key.as_primitive()))
                }
            }
            DataType::Float64 => {
                Ok(self.build_l2_distance_table_impl::<datatypes::Float64Type>(key.as_primitive()))
            }
            _ => Err(Error::index(format!(
                "unsupported data type: {}",
                key.data_type()
            ))),
        }
    }

    fn build_l2_distance_table_impl<T: ArrowPrimitiveType>(
        &self,
        key: &PrimitiveArray<T>,
    ) -> Vec<f32>
    where
        T::Native: L2,
    {
        build_distance_table_l2(
            self.codebook.values().as_primitive::<T>().values(),
            self.num_bits,
            self.num_sub_vectors,
            key.values(),
        )
    }

    /// Compute L2 distance from the query to all code.
    ///
    /// Type parameters
    /// ---------------
    /// - C: the tile size of code-book to run at once.
    /// - V: the tile size of PQ code to run at once.
    ///
    /// Parameters
    /// ----------
    /// - distance_table: the pre-computed L2 distance table.
    ///   It is a flatten array of [num_sub_vectors, num_centroids] f32.
    /// - code: the PQ code to be used to compute the distances.
    ///
    /// Returns
    /// -------
    ///  The squared L2 distance.
    #[inline]
    fn compute_l2_distance(&self, distance_table: &[f32], code: &[u8]) -> Float32Array {
        Float32Array::from(compute_pq_distance(
            distance_table,
            self.num_bits,
            self.num_sub_vectors,
            code,
            100,
        ))
    }

    /// Get the centroids for one sub-vector.
    ///
    /// Returns a flatten `num_centroids * sub_vector_width` f32 array.
    pub fn centroids<T: ArrowPrimitiveType>(&self, sub_vector_idx: usize) -> &[T::Native] {
        match self.num_bits {
            4 => get_sub_vector_centroids::<4, _>(
                self.codebook.values().as_primitive::<T>().values(),
                self.dimension,
                self.num_sub_vectors,
                sub_vector_idx,
            ),
            8 => get_sub_vector_centroids::<8, _>(
                self.codebook.values().as_primitive::<T>().values(),
                self.dimension,
                self.num_sub_vectors,
                sub_vector_idx,
            ),
            _ => panic!(
                "ProductQuantization: num_bits {} not supported",
                self.num_bits
            ),
        }
    }
}

impl Quantization for ProductQuantizer {
    type BuildParams = PQBuildParams;
    type Metadata = ProductQuantizationMetadata;
    type Storage = ProductQuantizationStorage;

    fn build(
        data: &dyn Array,
        distance_type: DistanceType,
        params: &Self::BuildParams,
    ) -> Result<Self> {
        assert_eq!(data.null_count(), 0);
        let fsl = data.as_fixed_size_list_opt().ok_or(Error::index(format!(
            "PQ builder: input is not a FixedSizeList: {}",
            data.data_type()
        )))?;

        if let Some(codebook) = params.codebook.as_ref() {
            return Ok(Self::new(
                params.num_sub_vectors,
                params.num_bits as u32,
                fsl.value_length() as usize,
                FixedSizeListArray::try_new_from_values(codebook.clone(), fsl.value_length())?,
                distance_type,
            ));
        }

        params.build(data, distance_type)
    }

    fn retrain(&mut self, data: &dyn Array) -> Result<()> {
        assert_eq!(data.null_count(), 0);
        let params = PQBuildParams::with_codebook(
            self.num_sub_vectors,
            self.num_bits as usize,
            Arc::new(self.codebook.clone()),
        );

        *self = params.build(data, self.distance_type)?;
        Ok(())
    }

    fn code_dim(&self) -> usize {
        self.num_sub_vectors
    }

    fn column(&self) -> &'static str {
        PQ_CODE_COLUMN
    }

    fn use_residual(distance_type: DistanceType) -> bool {
        PQBuildParams::use_residual(distance_type)
    }

    fn quantize(&self, vectors: &dyn Array) -> Result<ArrayRef> {
        let fsl = vectors
            .as_fixed_size_list_opt()
            .ok_or(Error::index(format!(
                "Expect to be a FixedSizeList<float> vector array, got: {:?} array",
                vectors.data_type()
            )))?;

        match fsl.value_type() {
            DataType::Float16 => self.transform::<datatypes::Float16Type>(vectors),
            DataType::Float32 => self.transform::<datatypes::Float32Type>(vectors),
            DataType::Float64 => self.transform::<datatypes::Float64Type>(vectors),
            _ => Err(Error::index(format!(
                "unsupported data type: {}",
                fsl.value_type()
            ))),
        }
    }

    fn metadata_key() -> &'static str {
        PQ_METADATA_KEY
    }

    fn quantization_type() -> QuantizationType {
        QuantizationType::Product
    }

    fn metadata(&self, args: Option<QuantizationMetadata>) -> Self::Metadata {
        let codebook_position = match &args {
            Some(args) => args.codebook_position,
            None => Some(0),
        };

        let codebook_position = codebook_position.expect("codebook position should be set");
        ProductQuantizationMetadata {
            codebook_position,
            nbits: self.num_bits,
            num_sub_vectors: self.num_sub_vectors,
            dimension: self.dimension,
            codebook: Some(self.codebook.clone()),
            codebook_tensor: Vec::new(),
            transposed: args.map(|args| args.transposed).unwrap_or_default(),
        }
    }

    fn from_metadata(metadata: &Self::Metadata, distance_type: DistanceType) -> Result<Quantizer> {
        let distance_type = match distance_type {
            DistanceType::Cosine => DistanceType::L2,
            _ => distance_type,
        };
        let codebook = match metadata.codebook.as_ref() {
            Some(fsl) => fsl.clone(),
            None => {
                let tensor = pb::Tensor::decode(metadata.codebook_tensor.as_ref())?;
                FixedSizeListArray::try_from(&tensor)?
            }
        };
        Ok(Quantizer::Product(Self::new(
            metadata.num_sub_vectors,
            metadata.nbits,
            metadata.dimension,
            codebook,
            distance_type,
        )))
    }

    fn field(&self) -> Field {
        let num_bytes_per_sub_vector = self.num_sub_vectors * self.num_bits as usize / 8;
        Field::new(
            PQ_CODE_COLUMN,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::UInt8, true)),
                num_bytes_per_sub_vector as i32,
            ),
            true,
        )
    }
}

impl TryFrom<&ProductQuantizer> for pb::Pq {
    type Error = Error;

    fn try_from(pq: &ProductQuantizer) -> Result<Self> {
        let tensor = pb::Tensor::try_from(&pq.codebook)?;
        Ok(Self {
            num_bits: pq.num_bits,
            num_sub_vectors: pq.num_sub_vectors as u32,
            dimension: pq.dimension as u32,
            codebook: vec![],
            codebook_tensor: Some(tensor),
        })
    }
}

impl TryFrom<Quantizer> for ProductQuantizer {
    type Error = Error;
    fn try_from(value: Quantizer) -> Result<Self> {
        match value {
            Quantizer::Product(pq) => Ok(pq),
            _ => Err(Error::index("Expect to be a ProductQuantizer".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::iter::repeat_n;

    use approx::assert_relative_eq;
    use arrow::datatypes::UInt8Type;
    use arrow_array::Float16Array;
    use half::f16;
    use lance_linalg::distance::l2_distance_batch;
    use lance_linalg::kernels::argmin;
    use lance_testing::datagen::generate_random_array;
    use num_traits::Zero;
    use storage::transpose;

    #[test]
    fn test_f16_pq_to_protobuf() {
        let pq = ProductQuantizer::new(
            4,
            8,
            16,
            FixedSizeListArray::try_new_from_values(
                Float16Array::from_iter_values(repeat_n(f16::zero(), 256 * 16)),
                16,
            )
            .unwrap(),
            DistanceType::L2,
        );
        let proto: pb::Pq = pb::Pq::try_from(&pq).unwrap();
        assert_eq!(proto.num_bits, 8);
        assert_eq!(proto.num_sub_vectors, 4);
        assert_eq!(proto.dimension, 16);
        assert!(proto.codebook.is_empty());
        assert!(proto.codebook_tensor.is_some());

        let tensor = proto.codebook_tensor.as_ref().unwrap();
        assert_eq!(tensor.data_type, pb::tensor::DataType::Float16 as i32);
        assert_eq!(tensor.shape, vec![256, 16]);
    }

    #[test]
    fn test_l2_distance() {
        const DIM: usize = 512;
        const TOTAL: usize = 66; // 64 + 2 to make sure reminder is handled correctly.
        let codebook = generate_random_array(256 * DIM);
        let pq = ProductQuantizer::new(
            16,
            8,
            DIM,
            FixedSizeListArray::try_new_from_values(codebook, DIM as i32).unwrap(),
            DistanceType::L2,
        );
        let pq_code = UInt8Array::from_iter_values((0..16 * TOTAL).map(|v| v as u8));
        let query = generate_random_array(DIM);

        let transposed_pq_codes = transpose(&pq_code, TOTAL, 16);
        let dists = pq.compute_distances(&query, &transposed_pq_codes).unwrap();

        let sub_vec_len = DIM / 16;
        let expected = pq_code
            .values()
            .chunks(16)
            .map(|code| {
                code.iter()
                    .enumerate()
                    .flat_map(|(sub_idx, c)| {
                        let subvec_centroids = pq.centroids::<datatypes::Float32Type>(sub_idx);
                        let subvec =
                            &query.values()[sub_idx * sub_vec_len..(sub_idx + 1) * sub_vec_len];
                        l2_distance_batch(
                            subvec,
                            &subvec_centroids
                                [*c as usize * sub_vec_len..(*c as usize + 1) * sub_vec_len],
                            sub_vec_len,
                        )
                    })
                    .sum::<f32>()
            })
            .collect::<Vec<_>>();
        dists
            .values()
            .iter()
            .zip(expected.iter())
            .for_each(|(v, e)| {
                assert_relative_eq!(*v, *e, epsilon = 1e-4);
            });
    }

    #[test]
    fn test_pq_transform() {
        const DIM: usize = 16;
        const TOTAL: usize = 64;
        let codebook = generate_random_array(DIM * 256);
        let pq = ProductQuantizer::new(
            4,
            8,
            DIM,
            FixedSizeListArray::try_new_from_values(codebook, DIM as i32).unwrap(),
            DistanceType::L2,
        );

        let vectors = generate_random_array(DIM * TOTAL);
        let fsl = FixedSizeListArray::try_new_from_values(vectors.clone(), DIM as i32).unwrap();
        let pq_code = pq.quantize(&fsl).unwrap();

        let mut expected = Vec::with_capacity(TOTAL * 4);
        vectors.values().chunks_exact(DIM).for_each(|vec| {
            vec.chunks_exact(DIM / 4)
                .enumerate()
                .for_each(|(sub_idx, sub_vec)| {
                    let centroids = pq.centroids::<datatypes::Float32Type>(sub_idx);
                    let dists = l2_distance_batch(sub_vec, centroids, DIM / 4);
                    let code = argmin(dists).unwrap() as u8;
                    expected.push(code);
                });
        });

        assert_eq!(pq_code.len(), TOTAL);
        assert_eq!(
            &expected,
            pq_code
                .as_fixed_size_list()
                .values()
                .as_primitive::<UInt8Type>()
                .values()
        );
    }
}
