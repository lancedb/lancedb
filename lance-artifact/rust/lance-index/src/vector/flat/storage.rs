// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{borrow::Cow, sync::Arc};

use super::index::FlatMetadata;
use crate::frag_reuse::FragReuseIndex;
use crate::vector::quantizer::QuantizerStorage;
use crate::vector::storage::{DistCalculator, VectorStore};
use crate::vector::utils::do_prefetch;
use arrow::array::AsArray;
use arrow::compute::concat_batches;
use arrow::datatypes::{Float16Type, Float64Type, UInt8Type};
use arrow_array::ArrowPrimitiveType;
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, RecordBatch, UInt64Array,
    types::{Float32Type, UInt64Type},
};
use arrow_schema::{DataType, SchemaRef};
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, ROW_ID, Result};
use lance_file::versions::v1::reader::FileReader as V1FileReader;
use lance_linalg::distance::hamming::hamming;
use lance_linalg::distance::{Cosine, DistanceType, Dot, L2};

pub const FLAT_COLUMN: &str = "flat";

/// All data are stored in memory
#[derive(Debug, Clone)]
pub struct FlatFloatStorage {
    metadata: FlatMetadata,
    batch: RecordBatch,
    distance_type: DistanceType,

    // helper fields
    pub(super) row_ids: Arc<UInt64Array>,
    vectors: Arc<FixedSizeListArray>,
}

impl DeepSizeOf for FlatFloatStorage {
    fn deep_size_of_children(&self, _: &mut lance_core::deepsize::Context) -> usize {
        self.batch.get_array_memory_size()
    }
}

#[async_trait::async_trait]
impl QuantizerStorage for FlatFloatStorage {
    type Metadata = FlatMetadata;

    fn try_from_batch(
        batch: RecordBatch,
        metadata: &Self::Metadata,
        distance_type: DistanceType,
        frag_reuse_index: Option<Arc<FragReuseIndex>>,
    ) -> Result<Self> {
        let batch = if let Some(frag_reuse_index_ref) = frag_reuse_index.as_ref() {
            frag_reuse_index_ref.remap_row_ids_record_batch(batch, 0)?
        } else {
            batch
        };

        let row_ids = Arc::new(
            batch
                .column_by_name(ROW_ID)
                .ok_or(Error::schema(format!("column {} not found", ROW_ID)))?
                .as_primitive::<UInt64Type>()
                .clone(),
        );
        let vectors = Arc::new(
            batch
                .column_by_name(FLAT_COLUMN)
                .ok_or(Error::schema("column flat not found".to_string()))?
                .as_fixed_size_list()
                .clone(),
        );
        Ok(Self {
            metadata: metadata.clone(),
            batch,
            distance_type,
            row_ids,
            vectors,
        })
    }

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    async fn load_partition(
        _: &V1FileReader,
        _: std::ops::Range<usize>,
        _: DistanceType,
        _: &Self::Metadata,
        _: Option<Arc<FragReuseIndex>>,
    ) -> Result<Self> {
        unimplemented!("Flat will be used in new index builder which doesn't require this")
    }
}

impl FlatFloatStorage {
    // used for only testing
    pub fn new(vectors: FixedSizeListArray, distance_type: DistanceType) -> Self {
        let row_ids = Arc::new(UInt64Array::from_iter_values(0..vectors.len() as u64));
        let vectors = Arc::new(vectors);

        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            (ROW_ID, row_ids.clone() as ArrayRef, true),
            (FLAT_COLUMN, vectors.clone() as ArrayRef, true),
        ])
        .unwrap();

        Self {
            metadata: FlatMetadata {
                dim: vectors.value_length() as usize,
            },
            batch,
            distance_type,
            row_ids,
            vectors,
        }
    }

    pub fn vector(&self, id: u32) -> ArrayRef {
        self.vectors.value(id as usize)
    }
}

impl VectorStore for FlatFloatStorage {
    type DistanceCalculator<'a> = FlatFloatDistanceCalc<'a>;

    fn to_batches(&self) -> Result<impl Iterator<Item = RecordBatch>> {
        Ok([self.batch.clone()].into_iter())
    }

    fn append_batch(&self, batch: RecordBatch, _vector_column: &str) -> Result<Self> {
        // TODO: use chunked storage
        let new_batch = concat_batches(&batch.schema(), vec![&self.batch, &batch])?;
        let mut storage = self.clone();
        storage.row_ids = Arc::new(
            new_batch
                .column_by_name(ROW_ID)
                .ok_or(Error::schema(format!("column {} not found", ROW_ID)))?
                .as_primitive::<UInt64Type>()
                .clone(),
        );
        storage.vectors = Arc::new(
            new_batch
                .column_by_name(FLAT_COLUMN)
                .ok_or(Error::schema("column flat not found".to_string()))?
                .as_fixed_size_list()
                .clone(),
        );
        storage.batch = new_batch;
        Ok(storage)
    }

    fn schema(&self) -> &SchemaRef {
        self.batch.schema_ref()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn len(&self) -> usize {
        self.vectors.len()
    }

    fn distance_type(&self) -> DistanceType {
        self.distance_type
    }

    fn row_id(&self, id: u32) -> u64 {
        self.row_ids.values()[id as usize]
    }

    fn row_ids(&self) -> impl Iterator<Item = &u64> {
        self.row_ids.values().iter()
    }

    fn dist_calculator(&self, query: ArrayRef, _dist_q_c: f32) -> Self::DistanceCalculator<'_> {
        Self::DistanceCalculator::new(self.vectors.as_ref(), query, self.distance_type)
    }

    fn dist_calculator_from_id(&self, id: u32) -> Self::DistanceCalculator<'_> {
        Self::DistanceCalculator::new_from_id(self.vectors.as_ref(), id, self.distance_type)
    }
}

/// All data are stored in memory
#[derive(Debug, Clone)]
pub struct FlatBinStorage {
    metadata: FlatMetadata,
    batch: RecordBatch,
    distance_type: DistanceType,

    // helper fields
    pub(super) row_ids: Arc<UInt64Array>,
    vectors: Arc<FixedSizeListArray>,
}

impl DeepSizeOf for FlatBinStorage {
    fn deep_size_of_children(&self, _: &mut lance_core::deepsize::Context) -> usize {
        self.batch.get_array_memory_size()
    }
}

#[async_trait::async_trait]
impl QuantizerStorage for FlatBinStorage {
    type Metadata = FlatMetadata;

    fn try_from_batch(
        batch: RecordBatch,
        metadata: &Self::Metadata,
        distance_type: DistanceType,
        frag_reuse_index: Option<Arc<FragReuseIndex>>,
    ) -> Result<Self> {
        let batch = if let Some(frag_reuse_index_ref) = frag_reuse_index.as_ref() {
            frag_reuse_index_ref.remap_row_ids_record_batch(batch, 0)?
        } else {
            batch
        };

        let row_ids = Arc::new(
            batch
                .column_by_name(ROW_ID)
                .ok_or(Error::schema(format!("column {} not found", ROW_ID)))?
                .as_primitive::<UInt64Type>()
                .clone(),
        );
        let vectors = Arc::new(
            batch
                .column_by_name(FLAT_COLUMN)
                .ok_or(Error::schema("column flat not found".to_string()))?
                .as_fixed_size_list()
                .clone(),
        );
        Ok(Self {
            metadata: metadata.clone(),
            batch,
            distance_type,
            row_ids,
            vectors,
        })
    }

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    async fn load_partition(
        _: &V1FileReader,
        _: std::ops::Range<usize>,
        _: DistanceType,
        _: &Self::Metadata,
        _: Option<Arc<FragReuseIndex>>,
    ) -> Result<Self> {
        unimplemented!("Flat will be used in new index builder which doesn't require this")
    }
}

impl FlatBinStorage {
    // used for only testing
    pub fn new(vectors: FixedSizeListArray, distance_type: DistanceType) -> Self {
        let row_ids = Arc::new(UInt64Array::from_iter_values(0..vectors.len() as u64));
        let vectors = Arc::new(vectors);

        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            (ROW_ID, row_ids.clone() as ArrayRef, true),
            (FLAT_COLUMN, vectors.clone() as ArrayRef, true),
        ])
        .unwrap();

        Self {
            metadata: FlatMetadata {
                dim: vectors.value_length() as usize,
            },
            batch,
            distance_type,
            row_ids,
            vectors,
        }
    }

    pub fn vector(&self, id: u32) -> ArrayRef {
        self.vectors.value(id as usize)
    }
}

impl VectorStore for FlatBinStorage {
    type DistanceCalculator<'a> = FlatDistanceCal<'a, UInt8Type>;

    fn to_batches(&self) -> Result<impl Iterator<Item = RecordBatch>> {
        Ok([self.batch.clone()].into_iter())
    }

    fn append_batch(&self, batch: RecordBatch, _vector_column: &str) -> Result<Self> {
        // TODO: use chunked storage
        let new_batch = concat_batches(&batch.schema(), vec![&self.batch, &batch])?;
        let mut storage = self.clone();
        storage.row_ids = Arc::new(
            new_batch
                .column_by_name(ROW_ID)
                .ok_or(Error::schema(format!("column {} not found", ROW_ID)))?
                .as_primitive::<UInt64Type>()
                .clone(),
        );
        storage.vectors = Arc::new(
            new_batch
                .column_by_name(FLAT_COLUMN)
                .ok_or(Error::schema("column flat not found".to_string()))?
                .as_fixed_size_list()
                .clone(),
        );
        storage.batch = new_batch;
        Ok(storage)
    }

    fn schema(&self) -> &SchemaRef {
        self.batch.schema_ref()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn len(&self) -> usize {
        self.vectors.len()
    }

    fn distance_type(&self) -> DistanceType {
        self.distance_type
    }

    fn row_id(&self, id: u32) -> u64 {
        self.row_ids.values()[id as usize]
    }

    fn row_ids(&self) -> impl Iterator<Item = &u64> {
        self.row_ids.values().iter()
    }

    fn dist_calculator(&self, query: ArrayRef, _dist_q_c: f32) -> Self::DistanceCalculator<'_> {
        Self::DistanceCalculator::new_binary(self.vectors.as_ref(), query, self.distance_type)
    }

    fn dist_calculator_from_id(&self, id: u32) -> Self::DistanceCalculator<'_> {
        Self::DistanceCalculator::new_binary_from_id(self.vectors.as_ref(), id, self.distance_type)
    }
}

pub struct FlatDistanceCal<'a, T: ArrowPrimitiveType> {
    vectors: &'a [T::Native],
    query: Cow<'a, [T::Native]>,
    dimension: usize,
    #[allow(clippy::type_complexity)]
    distance_fn: fn(&[T::Native], &[T::Native]) -> f32,
}

impl<'a, T> FlatDistanceCal<'a, T>
where
    T: ArrowPrimitiveType,
    T::Native: L2 + Cosine + Dot,
{
    fn new(vectors: &'a FixedSizeListArray, query: ArrayRef, distance_type: DistanceType) -> Self {
        // Gained significant performance improvement by using strong typed primitive slice.
        let flat_array = vectors.values().as_primitive::<T>();
        let dimension = vectors.value_length() as usize;
        Self {
            vectors: flat_array.values(),
            query: Cow::Owned(query.as_primitive::<T>().values().to_vec()),
            dimension,
            distance_fn: distance_type.func(),
        }
    }

    fn new_from_id(vectors: &'a FixedSizeListArray, id: u32, distance_type: DistanceType) -> Self {
        let flat_array = vectors.values().as_primitive::<T>();
        let dimension = vectors.value_length() as usize;
        let vectors = flat_array.values();
        let id = id as usize;
        Self {
            vectors,
            query: Cow::Borrowed(&vectors[dimension * id..dimension * (id + 1)]),
            dimension,
            distance_fn: distance_type.func(),
        }
    }
}

impl<'a> FlatDistanceCal<'a, UInt8Type> {
    fn new_binary(
        vectors: &'a FixedSizeListArray,
        query: ArrayRef,
        _distance_type: DistanceType,
    ) -> Self {
        // Gained significant performance improvement by using strong typed primitive slice.
        // TODO: to support other data types other than `f32`, make FlatDistanceCal a generic struct.
        let flat_array = vectors.values().as_primitive::<UInt8Type>();
        let dimension = vectors.value_length() as usize;
        Self {
            vectors: flat_array.values(),
            query: Cow::Owned(query.as_primitive::<UInt8Type>().values().to_vec()),
            dimension,
            distance_fn: hamming,
        }
    }

    fn new_binary_from_id(
        vectors: &'a FixedSizeListArray,
        id: u32,
        _distance_type: DistanceType,
    ) -> Self {
        let flat_array = vectors.values().as_primitive::<UInt8Type>();
        let dimension = vectors.value_length() as usize;
        let vectors = flat_array.values();
        let id = id as usize;
        Self {
            vectors,
            query: Cow::Borrowed(&vectors[dimension * id..dimension * (id + 1)]),
            dimension,
            distance_fn: hamming,
        }
    }
}

impl<T: ArrowPrimitiveType> FlatDistanceCal<'_, T> {
    #[inline]
    fn get_vector(&self, id: u32) -> &[T::Native] {
        &self.vectors[self.dimension * id as usize..self.dimension * (id + 1) as usize]
    }
}

impl<T: ArrowPrimitiveType> DistCalculator for FlatDistanceCal<'_, T> {
    #[inline]
    fn distance(&self, id: u32) -> f32 {
        let query = self.query.as_ref();
        let vector = self.get_vector(id);
        (self.distance_fn)(query, vector)
    }

    fn distance_all(&self, _k_hint: usize) -> Vec<f32> {
        let query = self.query.as_ref();
        self.vectors
            .chunks_exact(self.dimension)
            .map(|vector| (self.distance_fn)(query, vector))
            .collect()
    }

    #[inline]
    fn prefetch(&self, id: u32) {
        let vector = self.get_vector(id);
        do_prefetch(vector.as_ptr_range())
    }
}

pub enum FlatFloatDistanceCalc<'a> {
    Float16(FlatDistanceCal<'a, Float16Type>),
    Float32(FlatDistanceCal<'a, Float32Type>),
    Float64(FlatDistanceCal<'a, Float64Type>),
}

impl<'a> FlatFloatDistanceCalc<'a> {
    fn new(vectors: &'a FixedSizeListArray, query: ArrayRef, distance_type: DistanceType) -> Self {
        match vectors.value_type() {
            DataType::Float16 => Self::Float16(FlatDistanceCal::<Float16Type>::new(
                vectors,
                query,
                distance_type,
            )),
            DataType::Float32 => Self::Float32(FlatDistanceCal::<Float32Type>::new(
                vectors,
                query,
                distance_type,
            )),
            DataType::Float64 => Self::Float64(FlatDistanceCal::<Float64Type>::new(
                vectors,
                query,
                distance_type,
            )),
            dt => panic!("flat float storage does not support data type {dt}"),
        }
    }

    fn new_from_id(vectors: &'a FixedSizeListArray, id: u32, distance_type: DistanceType) -> Self {
        match vectors.value_type() {
            DataType::Float16 => Self::Float16(FlatDistanceCal::<Float16Type>::new_from_id(
                vectors,
                id,
                distance_type,
            )),
            DataType::Float32 => Self::Float32(FlatDistanceCal::<Float32Type>::new_from_id(
                vectors,
                id,
                distance_type,
            )),
            DataType::Float64 => Self::Float64(FlatDistanceCal::<Float64Type>::new_from_id(
                vectors,
                id,
                distance_type,
            )),
            dt => panic!("flat float storage does not support data type {dt}"),
        }
    }
}

impl DistCalculator for FlatFloatDistanceCalc<'_> {
    fn distance(&self, id: u32) -> f32 {
        match self {
            Self::Float16(calc) => calc.distance(id),
            Self::Float32(calc) => calc.distance(id),
            Self::Float64(calc) => calc.distance(id),
        }
    }

    fn distance_all(&self, k_hint: usize) -> Vec<f32> {
        match self {
            Self::Float16(calc) => calc.distance_all(k_hint),
            Self::Float32(calc) => calc.distance_all(k_hint),
            Self::Float64(calc) => calc.distance_all(k_hint),
        }
    }

    fn prefetch(&self, id: u32) {
        match self {
            Self::Float16(calc) => calc.prefetch(id),
            Self::Float32(calc) => calc.prefetch(id),
            Self::Float64(calc) => calc.prefetch(id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Float16Array, Float64Array};
    use half::f16;
    use lance_arrow::FixedSizeListArrayExt;

    fn make_f16_storage() -> FlatFloatStorage {
        let values = Float16Array::from(vec![
            f16::from_f32(1.0),
            f16::from_f32(2.0),
            f16::from_f32(4.0),
            f16::from_f32(6.0),
        ]);
        let vectors = FixedSizeListArray::try_new_from_values(values, 2).unwrap();
        FlatFloatStorage::new(vectors, DistanceType::L2)
    }

    fn make_f64_storage() -> FlatFloatStorage {
        let values = Float64Array::from(vec![1.0, 2.0, 4.0, 6.0]);
        let vectors = FixedSizeListArray::try_new_from_values(values, 2).unwrap();
        FlatFloatStorage::new(vectors, DistanceType::L2)
    }

    #[test]
    fn test_flat_float_storage_distance_f16() {
        let storage = make_f16_storage();
        let query: ArrayRef = Arc::new(Float16Array::from(vec![
            f16::from_f32(1.0),
            f16::from_f32(2.0),
        ]));

        let calc = storage.dist_calculator(query, 0.0);
        let distances = calc.distance_all(2);

        assert_eq!(distances.len(), 2);
        assert_eq!(distances[0], 0.0);
        assert!((distances[1] - 25.0).abs() < 1e-4);
    }

    #[test]
    fn test_flat_float_storage_distance_f64() {
        let storage = make_f64_storage();
        let query: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0]));

        let calc = storage.dist_calculator(query, 0.0);
        let distances = calc.distance_all(2);

        assert_eq!(distances.len(), 2);
        assert_eq!(distances[0], 0.0);
        assert!((distances[1] - 25.0).abs() < 1e-6);
    }
}
