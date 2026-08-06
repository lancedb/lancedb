// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow::{
    array::{ArrayData, make_array},
    buffer::Buffer,
    compute::cast,
};
use arrow_array::types::{Float16Type, Float32Type, Float64Type};
use arrow_array::{Array, ArrayRef, BooleanArray, FixedSizeListArray, cast::AsArray};
use arrow_schema::{DataType, Field};
use lance_arrow::{BufferExt, DataTypeExt, FixedSizeListArrayExt};
use lance_core::{Error, Result};
use lance_linalg::distance::DistanceType;
use prost::bytes;
use std::sync::LazyLock;
use std::{ops::Range, sync::Arc};

use super::pb;
use crate::pb::Tensor;
use crate::vector::flat::storage::FlatBinStorage;
use crate::vector::flat::storage::FlatFloatStorage;
use crate::vector::hnsw::HNSW;
use crate::vector::hnsw::builder::{HnswBuildParams, HnswQueryParams};
use crate::vector::v3::subindex::IvfSubIndex;

enum SimpleIndexStatus {
    Auto,
    Enabled,
    Disabled,
}

static USE_HNSW_SPEEDUP_INDEXING: LazyLock<SimpleIndexStatus> = LazyLock::new(|| {
    if let Ok(v) = std::env::var("LANCE_USE_HNSW_SPEEDUP_INDEXING") {
        if v == "enabled" {
            SimpleIndexStatus::Enabled
        } else if v == "disabled" {
            SimpleIndexStatus::Disabled
        } else {
            SimpleIndexStatus::Auto
        }
    } else {
        SimpleIndexStatus::Auto
    }
});

#[derive(Debug)]
pub struct SimpleIndex {
    store: SimpleStore,
    index: HNSW,
}

#[derive(Debug)]
enum SimpleStore {
    Float(FlatFloatStorage),
    Binary(FlatBinStorage),
}

impl SimpleIndex {
    fn try_new(store: SimpleStore) -> Result<Self> {
        let hnsw = match &store {
            SimpleStore::Float(store) => HNSW::index_vectors(
                store,
                HnswBuildParams::default().ef_construction(15).num_edges(12),
            )?,
            SimpleStore::Binary(store) => HNSW::index_vectors(
                store,
                HnswBuildParams::default().ef_construction(15).num_edges(12),
            )?,
        };
        Ok(Self { store, index: hnsw })
    }

    // train HNSW over the centroids to speed up finding the nearest clusters,
    // only train if all conditions are met:
    //  - the centroids are float16/float32 or uint8 with hamming distance
    //  - `num_centroids * dimension >= 1_000_000`
    //      we benchmarked that it's 2x faster in the case of 1024 centroids and 1024 dimensions,
    //      so set the threshold to 1_000_000.
    pub fn may_train_index(
        centroids: ArrayRef,
        dimension: usize,
        distance_type: DistanceType,
    ) -> Result<Option<Self>> {
        match *USE_HNSW_SPEEDUP_INDEXING {
            SimpleIndexStatus::Auto => {
                if centroids.len() < 1_000_000 {
                    return Ok(None);
                }
            }
            SimpleIndexStatus::Disabled => return Ok(None),
            _ => {}
        }

        let store = match (centroids.data_type(), distance_type) {
            (DataType::Float16 | DataType::Float32 | DataType::Float64, _) => {
                let fsl = FixedSizeListArray::try_new_from_values(centroids, dimension as i32)?;
                SimpleStore::Float(FlatFloatStorage::new(fsl, distance_type))
            }
            (DataType::UInt8, DistanceType::Hamming) => {
                let fsl = FixedSizeListArray::try_new_from_values(centroids, dimension as i32)?;
                SimpleStore::Binary(FlatBinStorage::new(fsl, distance_type))
            }
            _ => return Ok(None),
        };
        Self::try_new(store).map(Some)
    }

    pub(crate) fn search(&self, query: ArrayRef) -> Result<(u32, f32)> {
        let params = HnswQueryParams {
            ef: 15,
            lower_bound: None,
            upper_bound: None,
            dist_q_c: 0.0,
            use_acorn: false,
        };
        let res = match &self.store {
            SimpleStore::Float(store) => self.index.search_basic(query, 1, &params, None, store)?,
            SimpleStore::Binary(store) => {
                let query = if query.data_type() == &DataType::UInt8 {
                    query
                } else {
                    cast(&query, &DataType::UInt8).map_err(|e| Error::index(e.to_string()))?
                };
                self.index.search_basic(query, 1, &params, None, store)?
            }
        };
        Ok((res[0].id, res[0].dist.0))
    }
}

#[inline]
pub(crate) fn do_prefetch<T>(ptrs: Range<*const T>) {
    // TODO use rust intrinsics instead of x86 intrinsics
    // TODO finish this
    unsafe {
        let (ptr, end_ptr) = (ptrs.start as *const i8, ptrs.end as *const i8);
        let mut current_ptr = ptr;
        while current_ptr < end_ptr {
            const CACHE_LINE_SIZE: usize = 64;
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            {
                use core::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
                _mm_prefetch(current_ptr, _MM_HINT_T0);
            }
            current_ptr = current_ptr.add(CACHE_LINE_SIZE);
        }
    }
}

impl From<pb::tensor::DataType> for DataType {
    fn from(dt: pb::tensor::DataType) -> Self {
        match dt {
            pb::tensor::DataType::Uint8 => Self::UInt8,
            pb::tensor::DataType::Uint16 => Self::UInt16,
            pb::tensor::DataType::Uint32 => Self::UInt32,
            pb::tensor::DataType::Uint64 => Self::UInt64,
            pb::tensor::DataType::Float16 => Self::Float16,
            pb::tensor::DataType::Float32 => Self::Float32,
            pb::tensor::DataType::Float64 => Self::Float64,
            pb::tensor::DataType::Bfloat16 => unimplemented!(),
        }
    }
}

impl TryFrom<&DataType> for pb::tensor::DataType {
    type Error = Error;

    fn try_from(dt: &DataType) -> Result<Self> {
        match dt {
            DataType::UInt8 => Ok(Self::Uint8),
            DataType::UInt16 => Ok(Self::Uint16),
            DataType::UInt32 => Ok(Self::Uint32),
            DataType::UInt64 => Ok(Self::Uint64),
            DataType::Float16 => Ok(Self::Float16),
            DataType::Float32 => Ok(Self::Float32),
            DataType::Float64 => Ok(Self::Float64),
            _ => Err(Error::index(format!(
                "pb tensor type not supported: {:?}",
                dt
            ))),
        }
    }
}

impl TryFrom<DataType> for pb::tensor::DataType {
    type Error = Error;

    fn try_from(dt: DataType) -> Result<Self> {
        (&dt).try_into()
    }
}

impl TryFrom<&FixedSizeListArray> for pb::Tensor {
    type Error = Error;

    fn try_from(array: &FixedSizeListArray) -> Result<Self> {
        let mut tensor = Self::default();
        tensor.data_type = pb::tensor::DataType::try_from(array.value_type())? as i32;
        tensor.shape = vec![Array::len(array) as u32, array.value_length() as u32];
        let flat_array = array.values();
        tensor.data = flat_array.into_data().buffers()[0].to_vec();
        Ok(tensor)
    }
}

impl TryFrom<&pb::Tensor> for FixedSizeListArray {
    type Error = Error;

    fn try_from(tensor: &Tensor) -> Result<Self> {
        if tensor.shape.len() != 2 {
            return Err(Error::index(format!(
                "only accept 2-D tensor shape, got: {:?}",
                tensor.shape
            )));
        }
        let dim = tensor.shape[1] as usize;
        let num_rows = tensor.shape[0] as usize;
        let num_values = dim.checked_mul(num_rows).ok_or_else(|| {
            Error::index(format!(
                "Tensor shape {:?} exceeds the supported size",
                tensor.shape
            ))
        })?;
        let data_type = DataType::from(pb::tensor::DataType::try_from(tensor.data_type).unwrap());
        let expected_data_len =
            num_values
                .checked_mul(data_type.byte_width())
                .ok_or_else(|| {
                    Error::index(format!(
                        "Tensor shape {:?} exceeds the supported byte length",
                        tensor.shape
                    ))
                })?;
        if tensor.data.len() != expected_data_len {
            return Err(Error::index(format!(
                "Tensor shape {:?} with data type {data_type} requires {expected_data_len} bytes, got {}",
                tensor.shape,
                tensor.data.len()
            )));
        }

        let buffer = Buffer::from_bytes_bytes(
            bytes::Bytes::from(tensor.data.clone()),
            data_type.byte_width() as u64,
        );
        let data = ArrayData::builder(data_type)
            .len(num_values)
            .null_count(0)
            .add_buffer(buffer)
            .build()?;
        let flat_array = make_array(data);
        let field = Field::new("item", flat_array.data_type().clone(), true);
        Ok(Self::try_new(
            Arc::new(field),
            dim as i32,
            flat_array,
            None,
        )?)
    }
}

/// Check if all vectors in the FixedSizeListArray are finite
/// null values are considered as not finite
/// returns a BooleanArray
/// with the same length as the FixedSizeListArray
/// with true for finite values and false for non-finite values
pub fn is_finite(fsl: &FixedSizeListArray) -> BooleanArray {
    let is_finite = fsl
        .iter()
        .map(|v| match v {
            Some(v) => match v.data_type() {
                DataType::Float16 => {
                    let v = v.as_primitive::<Float16Type>();
                    Array::null_count(v) == 0 && v.values().iter().all(|v| v.is_finite())
                }
                DataType::Float32 => {
                    let v = v.as_primitive::<Float32Type>();
                    Array::null_count(v) == 0 && v.values().iter().all(|v| v.is_finite())
                }
                DataType::Float64 => {
                    let v = v.as_primitive::<Float64Type>();
                    Array::null_count(v) == 0 && v.values().iter().all(|v| v.is_finite())
                }
                _ => Array::null_count(&v) == 0,
            },
            None => false,
        })
        .collect::<Vec<_>>();
    BooleanArray::from(is_finite)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Float16Array, Float32Array, Float64Array, UInt8Array};
    use half::f16;
    use lance_arrow::FixedSizeListArrayExt;
    use num_traits::identities::Zero;
    use rayon::ThreadPoolBuilder;

    use arrow::compute::cast;
    use rstest::rstest;

    fn build_index(centroids: ArrayRef, dim: usize) -> SimpleIndex {
        let f32_centroids = cast(&centroids, &DataType::Float32).unwrap();
        let fsl = FixedSizeListArray::try_new_from_values(f32_centroids, dim as i32).unwrap();
        let store = SimpleStore::Float(FlatFloatStorage::new(fsl, DistanceType::L2));
        SimpleIndex::try_new(store).unwrap()
    }

    fn build_binary_index(centroids: ArrayRef, dim: usize) -> SimpleIndex {
        let u8_centroids = if centroids.data_type() == &DataType::UInt8 {
            centroids
        } else {
            cast(&centroids, &DataType::UInt8).unwrap()
        };
        let fsl = FixedSizeListArray::try_new_from_values(u8_centroids, dim as i32).unwrap();
        let store = SimpleStore::Binary(FlatBinStorage::new(fsl, DistanceType::Hamming));
        SimpleIndex::try_new(store).unwrap()
    }

    #[rstest]
    #[case::f16(Arc::new(Float16Array::from(
        (0..100).flat_map(|i| std::iter::repeat_n(f16::from_f32(i as f32), 16)).collect::<Vec<_>>(),
    )) as ArrayRef, 42.0f32)]
    #[case::f32(Arc::new(Float32Array::from(
        (0..100).flat_map(|i| std::iter::repeat_n(i as f32, 16)).collect::<Vec<_>>(),
    )) as ArrayRef, 42.0f32)]
    fn test_simple_index_nearest_centroid(#[case] centroids: ArrayRef, #[case] query_val: f32) {
        let thread_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
        let index = thread_pool.install(|| build_index(centroids, 16));
        let query: ArrayRef = Arc::new(Float32Array::from(vec![query_val; 16]));
        let (id, dist) = index.search(query).unwrap();
        assert_eq!(id, 42);
        assert_eq!(dist, 0.0);
    }

    #[test]
    fn test_simple_index_nearest_centroid_binary() {
        let centroids: ArrayRef = Arc::new(UInt8Array::from(
            (0..100)
                .flat_map(|i| std::iter::repeat_n(i as u8, 16))
                .collect::<Vec<_>>(),
        ));
        let index = build_binary_index(centroids, 16);
        let query: ArrayRef = Arc::new(UInt8Array::from(vec![42u8; 16]));
        let (id, dist) = index.search(query).unwrap();
        assert_eq!(id, 42);
        assert_eq!(dist, 0.0);
    }

    #[test]
    fn test_simple_index_rejects_f64() {
        let centroids: ArrayRef = Arc::new(Float64Array::from(vec![0.0; 1600]));
        let result = SimpleIndex::may_train_index(centroids, 16, DistanceType::L2).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_simple_index_rejects_uint8_non_hamming() {
        let centroids: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; 1600]));
        let result = SimpleIndex::may_train_index(centroids, 16, DistanceType::L2).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_fsl_to_tensor() {
        let fsl =
            FixedSizeListArray::try_new_from_values(Float16Array::from(vec![f16::zero(); 20]), 5)
                .unwrap();
        let tensor = pb::Tensor::try_from(&fsl).unwrap();
        assert_eq!(tensor.data_type, pb::tensor::DataType::Float16 as i32);
        assert_eq!(tensor.shape, vec![4, 5]);
        assert_eq!(tensor.data.len(), 20 * 2);
        let decoded = FixedSizeListArray::try_from(&tensor).unwrap();
        assert_eq!(decoded.values().to_data(), fsl.values().to_data());

        let fsl =
            FixedSizeListArray::try_new_from_values(Float32Array::from(vec![0.0; 20]), 5).unwrap();
        let tensor = pb::Tensor::try_from(&fsl).unwrap();
        assert_eq!(tensor.data_type, pb::tensor::DataType::Float32 as i32);
        assert_eq!(tensor.shape, vec![4, 5]);
        assert_eq!(tensor.data.len(), 20 * 4);
        let decoded = FixedSizeListArray::try_from(&tensor).unwrap();
        assert_eq!(decoded.values().to_data(), fsl.values().to_data());

        let fsl =
            FixedSizeListArray::try_new_from_values(Float64Array::from(vec![0.0; 20]), 5).unwrap();
        let tensor = pb::Tensor::try_from(&fsl).unwrap();
        assert_eq!(tensor.data_type, pb::tensor::DataType::Float64 as i32);
        assert_eq!(tensor.shape, vec![4, 5]);
        assert_eq!(tensor.data.len(), 20 * 8);
        let decoded = FixedSizeListArray::try_from(&tensor).unwrap();
        assert_eq!(decoded.values().to_data(), fsl.values().to_data());
    }

    #[rstest]
    #[case::too_short(vec![0; 7])]
    #[case::too_long(vec![0; 9])]
    fn test_tensor_to_fsl_rejects_invalid_data_length(#[case] data: Vec<u8>) {
        let tensor = pb::Tensor {
            data_type: pb::tensor::DataType::Uint32 as i32,
            shape: vec![1, 2],
            data,
        };

        let error = FixedSizeListArray::try_from(&tensor).unwrap_err();
        assert!(error.to_string().contains("requires 8 bytes"));
    }
}
