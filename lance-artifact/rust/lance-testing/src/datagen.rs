// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Data generation utilities for unit tests

use std::collections::HashSet;
use std::sync::Arc;
use std::{iter::repeat_with, ops::Range};

use arrow_array::types::ArrowPrimitiveType;
use arrow_array::{
    Float32Array, Int8Array, Int32Array, PrimitiveArray, RecordBatch, RecordBatchIterator,
    RecordBatchReader,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use lance_arrow::{ArrowFloatType, FixedSizeListArrayExt, fixed_size_list_type};
use num_traits::{FromPrimitive, real::Real};
use rand::distr::uniform::SampleUniform;
use rand::{
    Rng, SeedableRng, distr::Uniform, prelude::Distribution, rngs::StdRng, seq::SliceRandom,
};

pub trait ArrayGenerator {
    fn generate(&mut self, length: usize) -> Arc<dyn arrow_array::Array>;
    fn data_type(&self) -> &DataType;
    fn name(&self) -> Option<&str>;
}

pub struct IncrementingInt32 {
    name: Option<String>,
    current: i32,
    step: i32,
}

impl Default for IncrementingInt32 {
    fn default() -> Self {
        Self {
            name: None,
            current: 0,
            step: 1,
        }
    }
}

impl IncrementingInt32 {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn start(mut self, start: i32) -> Self {
        self.current = start;
        self
    }

    pub fn step(mut self, step: i32) -> Self {
        self.step = step;
        self
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

impl ArrayGenerator for IncrementingInt32 {
    fn generate(&mut self, length: usize) -> Arc<dyn arrow_array::Array> {
        let mut values = Vec::with_capacity(length);
        for _ in 0..length {
            values.push(self.current);
            self.current += self.step;
        }
        Arc::new(Int32Array::from(values))
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn data_type(&self) -> &DataType {
        &DataType::Int32
    }
}

pub struct RandomVector {
    name: Option<String>,
    vec_width: i32,
    data_type: DataType,
}

impl Default for RandomVector {
    fn default() -> Self {
        Self {
            name: None,
            vec_width: 4,
            data_type: fixed_size_list_type(4, DataType::Float32),
        }
    }
}

impl RandomVector {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn vec_width(mut self, vec_width: i32) -> Self {
        self.vec_width = vec_width;
        self.data_type = fixed_size_list_type(self.vec_width, DataType::Float32);
        self
    }

    pub fn named(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }
}

impl ArrayGenerator for RandomVector {
    fn generate(&mut self, length: usize) -> Arc<dyn arrow_array::Array> {
        let values = generate_random_array(length * (self.vec_width as usize));
        Arc::new(
            <arrow_array::FixedSizeListArray as FixedSizeListArrayExt>::try_new_from_values(
                values,
                self.vec_width,
            )
            .expect("Create fixed size list"),
        )
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

#[derive(Default)]
pub struct BatchGenerator {
    generators: Vec<Box<dyn ArrayGenerator>>,
}

impl BatchGenerator {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn col(mut self, genn: Box<dyn ArrayGenerator>) -> Self {
        self.generators.push(genn);
        self
    }

    fn gen_batch(&mut self, num_rows: u32) -> RecordBatch {
        let mut fields = Vec::with_capacity(self.generators.len());
        let mut arrays = Vec::with_capacity(self.generators.len());
        for (field_index, genn) in self.generators.iter_mut().enumerate() {
            let arr = genn.generate(num_rows as usize);
            let default_name = format!("field_{}", field_index);
            let name = genn.name().unwrap_or(&default_name);
            fields.push(Field::new(name, arr.data_type().clone(), true));
            arrays.push(arr);
        }
        let schema = Arc::new(ArrowSchema::new(fields));
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    pub fn batch(&mut self, num_rows: i32) -> impl RecordBatchReader + use<> {
        let batch = self.gen_batch(num_rows as u32);
        let schema = batch.schema();
        RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema)
    }

    pub fn batches(
        &mut self,
        num_batches: u32,
        rows_per_batch: u32,
    ) -> impl RecordBatchReader + use<> {
        let batches = (0..num_batches)
            .map(|_| self.gen_batch(rows_per_batch))
            .collect::<Vec<_>>();
        let schema = batches[0].schema();
        RecordBatchIterator::new(batches.into_iter().map(Ok), schema)
    }
}

/// Returns a batch of data that has a column that can be used to create an ANN index
///
/// The indexable column will be named "indexable"
/// The batch will not be empty
/// There will only be one batch
///
/// There are no other assumptions it is safe to make about the returned reader
pub fn some_indexable_batch() -> impl RecordBatchReader {
    let x = Box::new(RandomVector::new().named("indexable".to_string()));
    BatchGenerator::new().col(x).batch(512)
}

/// Returns a non-empty batch of data
///
/// The batch will not be empty
/// There will only be one batch
///
/// There are no other assumptions it is safe to make about the returned reader
pub fn some_batch() -> impl RecordBatchReader {
    some_indexable_batch()
}

/// Create a random float32 array.
pub fn generate_random_array_with_seed<T: ArrowFloatType>(n: usize, seed: [u8; 32]) -> T::ArrayType
where
    T::Native: Real + FromPrimitive,
{
    let mut rng = StdRng::from_seed(seed);

    <T::ArrayType as lance_arrow::FloatArray<T>>::from_iter_values(
        repeat_with(|| T::Native::from_f32(rng.random::<f32>()).unwrap()).take(n),
    )
}

/// Create a random float32 array where each element is uniformly
/// distributed between [0..1]
pub fn generate_random_array(n: usize) -> Float32Array {
    let mut rng = rand::rng();
    Float32Array::from_iter_values(repeat_with(|| rng.random::<f32>()).take(n))
}

/// Create a random float32 array where each element is uniformly
/// distributed between [0..1]
pub fn generate_random_int8_array(n: usize) -> Int8Array {
    let mut rng = rand::rng();
    Int8Array::from_iter_values(repeat_with(|| rng.random::<i8>()).take(n))
}

/// Create a random primitive array where each element is uniformly distributed a
/// given range.
pub fn generate_random_array_with_range<T: ArrowPrimitiveType>(
    n: usize,
    range: Range<T::Native>,
) -> PrimitiveArray<T>
where
    T::Native: SampleUniform,
{
    let mut rng = StdRng::from_seed([13; 32]);
    let distribution = Uniform::new(range.start, range.end).unwrap();
    PrimitiveArray::<T>::from_iter_values(repeat_with(|| distribution.sample(&mut rng)).take(n))
}

/// Create a random float32 array where each element is uniformly
/// distributed across the given range
pub fn generate_scaled_random_array(n: usize, min: f32, max: f32) -> Float32Array {
    let mut rng = rand::rng();
    let distribution = Uniform::new(min, max).unwrap();
    Float32Array::from_iter_values(repeat_with(|| distribution.sample(&mut rng)).take(n))
}

pub fn sample_indices(range: Range<usize>, num_picks: u32) -> Vec<usize> {
    let mut rng = rand::rng();
    let dist = Uniform::new(range.start, range.end).unwrap();
    let ratio = num_picks as f32 / range.len() as f32;
    if ratio < 0.1_f32 && num_picks > 1000 {
        // We want to pick a large number of values from a big range.  Better to
        // use a set and potential retries
        let mut picked = HashSet::<usize>::with_capacity(num_picks as usize);
        let mut ordered_picked = Vec::with_capacity(num_picks as usize);
        while picked.len() < num_picks as usize {
            let val = dist.sample(&mut rng);
            if picked.insert(val) {
                ordered_picked.push(val);
            }
        }
        ordered_picked
    } else {
        // We want to pick most of the range, or a small number of values.  Go ahead
        // and just materialize the range and shuffle
        let mut values = Vec::from_iter(range);
        values.partial_shuffle(&mut rng, num_picks as usize);
        values.truncate(num_picks as usize);
        values
    }
}

pub fn sample_without_replacement<T: Copy>(choices: &[T], num_picks: u32) -> Vec<T> {
    let mut rng = rand::rng();
    let mut shuffled = Vec::from(choices);
    shuffled.partial_shuffle(&mut rng, num_picks as usize);
    shuffled.truncate(num_picks as usize);
    shuffled
}
