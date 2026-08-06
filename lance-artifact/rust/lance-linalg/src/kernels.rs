// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::cmp::Ordering;
use std::iter::Sum;
use std::sync::Arc;
use std::{collections::hash_map::DefaultHasher, hash::Hash, hash::Hasher};

use arrow_array::{
    Array, ArrayRef, ArrowNumericType, ArrowPrimitiveType, FixedSizeListArray, GenericStringArray,
    OffsetSizeTrait, PrimitiveArray, UInt64Array,
    cast::{AsArray, as_largestring_array, as_primitive_array, as_string_array},
    types::{
        Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
        UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_schema::{ArrowError, DataType};
use num_traits::AsPrimitive;
use num_traits::{Float, Num, bounds::Bounded};

use crate::{Error, Result};

/// Argmax on a [PrimitiveArray].
///
/// Returns the index of the max value in the array.
pub fn argmax<T: Num + Bounded + PartialOrd>(iter: impl Iterator<Item = T>) -> Option<u32> {
    let mut max_idx: Option<u32> = None;
    let mut max_value = T::min_value();
    for (idx, value) in iter.enumerate() {
        if let Some(Ordering::Greater) = value.partial_cmp(&max_value) {
            max_value = value;
            max_idx = Some(idx as u32);
        }
    }
    max_idx
}

pub fn argmax_opt<T: Num + Bounded + PartialOrd>(
    iter: impl Iterator<Item = Option<T>>,
) -> Option<u32> {
    let mut max_idx: Option<u32> = None;
    let mut max_value = T::min_value();
    for (idx, value) in iter.enumerate() {
        if let Some(value) = value
            && let Some(Ordering::Greater) = value.partial_cmp(&max_value)
        {
            max_value = value;
            max_idx = Some(idx as u32);
        }
    }
    max_idx
}

/// Argmin over an iterator. Fused the operation in iterator to avoid memory allocation.
///
/// Returns the index of the min value in the array.
///
pub fn argmin<T: Num + PartialOrd + Copy + Bounded>(iter: impl Iterator<Item = T>) -> Option<u32> {
    argmin_value(iter).map(|(idx, _)| idx)
}

/// Return both argmin and minimal value over an iterator.
///
/// Return
/// ------
/// - `Some(idx, min_value)` or
/// - `None` if iterator is empty or all are `Nan/Inf`.
pub fn argmin_value<T: Num + Bounded + PartialOrd + Copy>(
    iter: impl Iterator<Item = T>,
) -> Option<(u32, T)> {
    argmin_value_opt(iter.map(Some))
}

/// Returns the minimal value (float) and the index (argmin) from an Iterator.
///
/// Return `None` if the iterator is empty or all are `Nan/Inf`.
#[inline]
pub fn argmin_value_float<T: Float>(iter: impl Iterator<Item = T>) -> Option<(u32, T)> {
    let mut min_idx = None;
    let mut min_value = T::infinity();
    for (idx, value) in iter.enumerate() {
        if value < min_value {
            min_value = value;
            min_idx = Some(idx as u32);
        }
    }
    min_idx.map(|idx| (idx, min_value))
}

#[inline]
pub fn argmin_value_float_with_bias<T: Float>(
    iter: impl Iterator<Item = T>,
    bias: Option<impl Iterator<Item = T>>,
) -> Option<(u32, T)> {
    let Some(bias) = bias else {
        return argmin_value_float(iter);
    };

    let mut min_idx = None;
    let mut min_value = T::infinity();
    let mut min_original_value = T::infinity();
    for (idx, (value, bias)) in iter.zip(bias).enumerate() {
        if value + bias < min_value {
            min_value = value + bias;
            min_original_value = value;
            min_idx = Some(idx as u32);
        }
    }
    min_idx.map(|idx| (idx, min_original_value))
}

pub fn argmin_value_opt<T: Num + Bounded + PartialOrd>(
    iter: impl Iterator<Item = Option<T>>,
) -> Option<(u32, T)> {
    let mut min_idx: Option<u32> = None;
    let mut min_value = T::max_value();
    for (idx, value) in iter.enumerate() {
        if let Some(value) = value
            && let Some(Ordering::Less) = value.partial_cmp(&min_value)
        {
            min_value = value;
            min_idx = Some(idx as u32);
        }
    }
    min_idx.map(|idx| (idx, min_value))
}

/// Argmin over an `Option<Float>` iterator.
///
#[inline]
pub fn argmin_opt<T: Num + Bounded + PartialOrd>(
    iter: impl Iterator<Item = Option<T>>,
) -> Option<u32> {
    argmin_value_opt(iter).map(|(idx, _)| idx)
}

/// L2 normalize a vector.
///
/// Returns an iterator of normalized values.
pub fn normalize<T: Float + Sum + AsPrimitive<f32>>(
    v: &[T],
) -> (impl Iterator<Item = T> + '_, f32) {
    let l2_norm = v.iter().map(|x| x.powi(2)).sum::<T>().sqrt();
    (v.iter().map(move |&x| x / l2_norm), l2_norm.as_())
}

fn do_normalize_arrow<T: ArrowPrimitiveType>(arr: &dyn Array) -> Result<(ArrayRef, f32)>
where
    <T as ArrowPrimitiveType>::Native: Float + Sum + AsPrimitive<f32>,
{
    let v = arr.as_primitive::<T>();
    let (iter, l2_norm) = normalize(v.values());
    Ok((
        Arc::new(PrimitiveArray::<T>::from_iter_values(iter)) as ArrayRef,
        l2_norm,
    ))
}

pub fn normalize_arrow(v: &dyn Array) -> Result<(ArrayRef, f32)> {
    match v.data_type() {
        DataType::Float16 => do_normalize_arrow::<Float16Type>(v),
        DataType::Float32 => do_normalize_arrow::<Float32Type>(v),
        DataType::Float64 => do_normalize_arrow::<Float64Type>(v),
        _ => Err(Error::SchemaError(format!(
            "Normalize only supports float array, got: {}",
            v.data_type()
        ))),
    }
}

fn do_normalize_fsl<T: ArrowPrimitiveType>(fsl: &FixedSizeListArray) -> Result<FixedSizeListArray>
where
    T::Native: Float + Sum + AsPrimitive<f32>,
{
    let dim = fsl.value_length() as usize;
    let norm_arr = PrimitiveArray::<T>::from_iter_values(
        fsl.values()
            .as_primitive::<T>()
            .values()
            .chunks(dim)
            .flat_map(|chunk| normalize(chunk).0),
    );

    // Extract the field from the data type
    let field = match fsl.data_type() {
        DataType::FixedSizeList(field, _) => field.clone(),
        _ => unreachable!("FixedSizeListArray must have FixedSizeList data type"),
    };

    // Use try_new to preserve the null buffer from the original array
    FixedSizeListArray::try_new(
        field,
        fsl.value_length(),
        Arc::new(norm_arr),
        fsl.nulls().cloned(),
    )
}

/// L2 normalize a [FixedSizeListArray] (of vectors).
pub fn normalize_fsl(fsl: &FixedSizeListArray) -> Result<FixedSizeListArray> {
    match fsl.value_type() {
        DataType::Float16 => do_normalize_fsl::<Float16Type>(fsl),
        DataType::Float32 => do_normalize_fsl::<Float32Type>(fsl),
        DataType::Float64 => do_normalize_fsl::<Float64Type>(fsl),
        _ => Err(ArrowError::SchemaError(format!(
            "Normalize only supports float array, got: {}",
            fsl.value_type()
        ))),
    }
}

fn do_normalize_fsl_inplace<T: ArrowPrimitiveType>(
    fsl: FixedSizeListArray,
) -> Result<FixedSizeListArray>
where
    T::Native: Float + Sum + AsPrimitive<f32>,
{
    let dim = fsl.value_length() as usize;
    let (field, size, values_array, nulls) = fsl.into_parts();

    // Clone the PrimitiveArray (shares the underlying buffer), then drop the
    // Arc<dyn Array> so the buffer's refcount drops to 1.
    let prim = values_array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .expect("values must be PrimitiveArray")
        .clone();
    drop(values_array);

    // into_builder gives mutable access when the buffer is uniquely owned,
    // avoiding a full copy of the (potentially multi-GB) training data.
    match prim.into_builder() {
        Ok(mut builder) => {
            for chunk in builder.values_slice_mut().chunks_mut(dim) {
                let l2_norm = chunk.iter().map(|x| x.powi(2)).sum::<T::Native>().sqrt();
                for x in chunk.iter_mut() {
                    *x = *x / l2_norm;
                }
            }
            FixedSizeListArray::try_new(field, size, Arc::new(builder.finish()), nulls)
        }
        Err(prim) => {
            let fsl = FixedSizeListArray::try_new(field, size, Arc::new(prim), nulls)?;
            do_normalize_fsl::<T>(&fsl)
        }
    }
}

/// L2 normalize a [FixedSizeListArray] (of vectors), attempting in-place mutation.
///
/// If the underlying buffer is uniquely owned, normalization is performed in-place
/// to avoid allocating a second copy. Otherwise falls back to the copy path used
/// by [`normalize_fsl`].
pub fn normalize_fsl_owned(fsl: FixedSizeListArray) -> Result<FixedSizeListArray> {
    match fsl.value_type() {
        DataType::Float16 => do_normalize_fsl_inplace::<Float16Type>(fsl),
        DataType::Float32 => do_normalize_fsl_inplace::<Float32Type>(fsl),
        DataType::Float64 => do_normalize_fsl_inplace::<Float64Type>(fsl),
        _ => Err(ArrowError::SchemaError(format!(
            "Normalize only supports float array, got: {}",
            fsl.value_type()
        ))),
    }
}

fn hash_numeric_type<T: ArrowNumericType>(array: &PrimitiveArray<T>) -> Result<UInt64Array>
where
    T::Native: Hash,
{
    let mut builder = UInt64Array::builder(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            let mut s = DefaultHasher::new();
            array.value(i).hash(&mut s);
            builder.append_value(s.finish());
        }
    }
    Ok(builder.finish())
}

fn hash_string_type<O: OffsetSizeTrait>(array: &GenericStringArray<O>) -> Result<UInt64Array> {
    let mut builder = UInt64Array::builder(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            let mut s = DefaultHasher::new();
            array.value(i).hash(&mut s);
            builder.append_value(s.finish());
        }
    }
    Ok(builder.finish())
}

/// Calculate hash values for an Arrow Array, using `std::hash::Hash` in rust.
pub fn hash(array: &dyn Array) -> Result<UInt64Array> {
    match array.data_type() {
        DataType::UInt8 => hash_numeric_type(as_primitive_array::<UInt8Type>(array)),
        DataType::UInt16 => hash_numeric_type(as_primitive_array::<UInt16Type>(array)),
        DataType::UInt32 => hash_numeric_type(as_primitive_array::<UInt32Type>(array)),
        DataType::UInt64 => hash_numeric_type(as_primitive_array::<UInt64Type>(array)),
        DataType::Int8 => hash_numeric_type(as_primitive_array::<Int8Type>(array)),
        DataType::Int16 => hash_numeric_type(as_primitive_array::<Int16Type>(array)),
        DataType::Int32 => hash_numeric_type(as_primitive_array::<Int32Type>(array)),
        DataType::Int64 => hash_numeric_type(as_primitive_array::<Int64Type>(array)),
        DataType::Utf8 => hash_string_type(as_string_array(array)),
        DataType::LargeUtf8 => hash_string_type(as_largestring_array(array)),
        _ => Err(ArrowError::SchemaError(format!(
            "Hash only supports integer or string array, got: {}",
            array.data_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;

    use approx::assert_relative_eq;
    use arrow_array::{
        Float32Array, Int8Array, Int16Array, LargeStringArray, StringArray, UInt8Array, UInt32Array,
    };
    use arrow_buffer::NullBuffer;
    use arrow_schema::Field;

    #[test]
    fn test_argmax() {
        let f = Float32Array::from(vec![1.0, 5.0, 3.0, 2.0, 20.0, 8.2, 3.5]);
        assert_eq!(argmax(f.values().iter().copied()), Some(4));

        let f = Float32Array::from(vec![1.0, 5.0, f32::NAN, 3.0, 2.0, 20.0, f32::INFINITY, 3.5]);
        assert_eq!(argmax_opt(f.iter()), Some(6));

        let f = Float32Array::from_iter(vec![Some(2.0), None, Some(20.0), Some(f32::NAN)]);
        assert_eq!(argmax_opt(f.iter()), Some(2));

        let f = Float32Array::from(vec![f32::NAN; 3]);
        assert_eq!(argmax(f.values().iter().copied()), None);

        let i = Int16Array::from(vec![1, 5, 3, 2, 20, 8, 16]);
        assert_eq!(argmax(i.values().iter().copied()), Some(4));

        let u = UInt32Array::from(vec![1, 5, 3, 2, 20, 8, 16]);
        assert_eq!(argmax(u.values().iter().copied()), Some(4));

        let empty_vec: Vec<i16> = vec![];
        let empty = Int16Array::from(empty_vec);
        assert_eq!(argmax_opt(empty.iter()), None)
    }

    #[test]
    fn test_argmin() {
        let f = Float32Array::from_iter(vec![5.0, 3.0, 2.0, 20.0, 8.2, 3.5]);
        assert_eq!(argmin(f.values().iter().copied()), Some(2));

        let f = Float32Array::from_iter(vec![5.0, 3.0, 2.0, 20.0, f32::NAN]);
        assert_eq!(argmin_opt(f.iter()), Some(2));

        let f = Float32Array::from_iter(vec![Some(2.0), None, Some(f32::NAN)]);
        assert_eq!(argmin_opt(f.iter()), Some(0));

        let f = Float32Array::from_iter(vec![5.0, 3.0, 2.0, f32::NEG_INFINITY, f32::NAN]);
        assert_eq!(argmin(f.values().iter().copied()), Some(3));

        let f = Float32Array::from_iter(vec![f32::NAN; 4]);
        assert_eq!(argmin(f.values().iter().copied()), None);

        let f = Float32Array::from_iter(vec![5.0, 3.0, 2.0, 20.0, 8.2, 3.5]);
        assert_eq!(argmin(f.values().iter().copied()), Some(2));

        let i = Int16Array::from_iter(vec![5, 3, 2, 20, 8, 16]);
        assert_eq!(argmin(i.values().iter().copied()), Some(2));

        let u = UInt32Array::from_iter(vec![5, 3, 2, 20, 8, 16]);
        assert_eq!(argmin(u.values().iter().copied()), Some(2));

        let empty_vec: Vec<i16> = vec![];
        let empty = Int16Array::from(empty_vec);
        assert_eq!(argmin_opt(empty.iter()), None)
    }

    #[test]
    fn test_numeric_hashes() {
        let a: UInt8Array = [1_u8, 2, 3, 4, 5].iter().copied().collect();
        let ha = hash(&a).unwrap();
        let distinct_values: HashSet<u64> = ha.values().iter().copied().collect();
        assert_eq!(distinct_values.len(), 5, "hash should be distinct");

        let b: Int8Array = [1_i8, 2, 3, 4, 5].iter().copied().collect();
        let hb = hash(&b).unwrap();

        assert_eq!(ha, hb, "hash of the same numeric value should be the same");
    }

    #[test]
    fn test_string_hashes() {
        let a = StringArray::from(vec!["a", "b", "ccc", "dec", "e", "a"]);
        let h = hash(&a).unwrap();
        // first and last value are the same.
        assert_eq!(h.value(0), h.value(5));

        // Other than that, all values should be distinct
        let distinct_values: HashSet<u64> = h.values().iter().copied().collect();
        assert_eq!(distinct_values.len(), 5);

        let a = LargeStringArray::from(vec!["a", "b", "ccc", "dec", "e", "a"]);
        let h = hash(&a).unwrap();
        // first and last value are the same.
        assert_eq!(h.value(0), h.value(5));
    }

    #[test]
    fn test_hash_unsupported_type() {
        let a = Float32Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        assert!(hash(&a).is_err());
    }

    #[test]
    fn test_normalize_vector() {
        let v = vec![1.0_f32, 2.0, 3.0, 4.0, 5.0];
        let l2_norm = v.iter().map(|&x| x.powi(2)).sum::<f32>().sqrt();
        assert_relative_eq!(l2_norm, 55_f32.sqrt());
        let normalized = normalize(&v).0.collect::<Vec<f32>>();
        normalized
            .iter()
            .enumerate()
            .for_each(|(idx, &x)| assert_relative_eq!(x, (idx + 1) as f32 / 55.0_f32.sqrt()));
        assert_relative_eq!(1.0, normalized.iter().map(|&x| x.powi(2)).sum::<f32>());
    }

    #[test]
    fn test_normalize_fsl_with_nulls() {
        // Create test data with nulls
        let values = Float32Array::from_iter_values(vec![
            3.0, 4.0, // First vector: [3, 4] -> will be normalized to [0.6, 0.8]
            0.0, 0.0, // Second vector: null (values don't matter)
            5.0, 12.0, // Third vector: [5, 12] -> will be normalized to [5/13, 12/13]
        ]);

        // Create null buffer where second vector is null
        let null_buffer = NullBuffer::from(vec![true, false, true]);

        let field = Arc::new(Field::new("item", DataType::Float32, true));
        let fsl =
            FixedSizeListArray::try_new(field, 2, Arc::new(values), Some(null_buffer.clone()))
                .unwrap();

        // Normalize the array
        let normalized = normalize_fsl(&fsl).unwrap();

        // Verify nulls are preserved
        assert_eq!(normalized.nulls(), Some(&null_buffer));

        // Verify non-null vectors are normalized correctly
        let normalized_values = normalized.values().as_primitive::<Float32Type>();

        // First vector [3, 4] -> [0.6, 0.8]
        assert_relative_eq!(normalized_values.value(0), 0.6);
        assert_relative_eq!(normalized_values.value(1), 0.8);

        // Third vector [5, 12] -> [5/13, 12/13]
        assert_relative_eq!(normalized_values.value(4), 5.0 / 13.0);
        assert_relative_eq!(normalized_values.value(5), 12.0 / 13.0);
    }

    #[test]
    fn test_normalize_fsl_edge_cases() {
        // Test case 1: All nulls
        let values = Float32Array::from_iter_values(vec![0.0; 6]);
        let null_buffer = NullBuffer::from(vec![false, false, false]);
        let field = Arc::new(Field::new("item", DataType::Float32, true));
        let fsl = FixedSizeListArray::try_new(
            field.clone(),
            2,
            Arc::new(values),
            Some(null_buffer.clone()),
        )
        .unwrap();

        let normalized = normalize_fsl(&fsl).unwrap();
        assert_eq!(normalized.nulls(), Some(&null_buffer));

        // Test case 2: Empty array
        let empty_values = Float32Array::from(vec![] as Vec<f32>);
        let empty_fsl =
            FixedSizeListArray::try_new(field.clone(), 2, Arc::new(empty_values), None).unwrap();

        let normalized_empty = normalize_fsl(&empty_fsl).unwrap();
        assert_eq!(normalized_empty.len(), 0);

        // Test case 3: No nulls
        let values = Float32Array::from_iter_values(vec![1.0, 0.0, 0.0, 1.0]);
        let fsl_no_nulls = FixedSizeListArray::try_new(field, 2, Arc::new(values), None).unwrap();

        let normalized_no_nulls = normalize_fsl(&fsl_no_nulls).unwrap();
        assert_eq!(normalized_no_nulls.nulls(), None);
        let values = normalized_no_nulls.values().as_primitive::<Float32Type>();
        assert_relative_eq!(values.value(0), 1.0);
        assert_relative_eq!(values.value(1), 0.0);
        assert_relative_eq!(values.value(2), 0.0);
        assert_relative_eq!(values.value(3), 1.0);
    }

    fn make_fsl(values: &[f32], dim: i32) -> FixedSizeListArray {
        let field = Arc::new(Field::new("item", DataType::Float32, true));
        FixedSizeListArray::try_new(
            field,
            dim,
            Arc::new(Float32Array::from_iter_values(values.iter().copied())),
            None,
        )
        .unwrap()
    }

    /// Assert FSL values match expected, where None means NaN.
    fn assert_fsl_eq(actual: &FixedSizeListArray, expected: &[Option<f32>], label: &str) {
        let vals = actual.values().as_primitive::<Float32Type>();
        assert_eq!(vals.len(), expected.len(), "{label}: length mismatch");
        for (i, exp) in expected.iter().enumerate() {
            match exp {
                None => assert!(vals.value(i).is_nan(), "{label}[{i}]: expected NaN"),
                Some(v) => assert_relative_eq!(vals.value(i), *v, epsilon = 1e-6),
            }
        }
    }

    /// normalize_fsl_owned produces correct values and matches normalize_fsl.
    /// Zero vectors yield NaN (cosine is undefined; downstream is_finite filters them).
    #[test]
    fn test_normalize_fsl_owned_values() {
        #[allow(clippy::type_complexity)]
        let cases: &[(&str, &[f32], &[Option<f32>])] = &[
            (
                "basic",
                &[3.0, 4.0, 5.0, 12.0],
                &[Some(0.6), Some(0.8), Some(5.0 / 13.0), Some(12.0 / 13.0)],
            ),
            (
                "zero_vector",
                &[3.0, 4.0, 0.0, 0.0, 5.0, 12.0],
                &[
                    Some(0.6),
                    Some(0.8),
                    None,
                    None,
                    Some(5.0 / 13.0),
                    Some(12.0 / 13.0),
                ],
            ),
        ];
        for (name, input, expected) in cases {
            let fsl = make_fsl(input, 2);
            assert_fsl_eq(&normalize_fsl(&fsl).unwrap(), expected, name);
            assert_fsl_eq(&normalize_fsl_owned(fsl).unwrap(), expected, name);
        }
    }

    /// Uniquely-owned buffer is mutated in-place (no copy).
    #[test]
    fn test_normalize_fsl_owned_inplace() {
        let fsl = make_fsl(&[3.0, 4.0, 5.0, 12.0], 2);
        let ptr = fsl.values().as_primitive::<Float32Type>().values().as_ptr();
        let result = normalize_fsl_owned(fsl).unwrap();
        let new_ptr = result
            .values()
            .as_primitive::<Float32Type>()
            .values()
            .as_ptr();
        assert_eq!(ptr, new_ptr, "expected in-place mutation");
    }

    /// Sliced inputs normalize correctly via the by-reference path.
    /// (normalize_fsl_owned uses into_builder which does not support sliced
    /// arrays; use normalize_fsl for sliced data.)
    #[test]
    fn test_normalize_fsl_sliced_input() {
        let sliced = {
            let fsl = make_fsl(&[1.0, 0.0, 0.0, 1.0, 3.0, 4.0], 2);
            fsl.slice(1, 2)
        };

        let expected = &[Some(0.0), Some(1.0), Some(0.6), Some(0.8)];
        assert_fsl_eq(&normalize_fsl(&sliced).unwrap(), expected, "sliced_ref");
    }

    /// Shared buffer falls back to copy path and still produces correct values.
    #[test]
    fn test_normalize_fsl_owned_shared_buffer_fallback() {
        let fsl = make_fsl(&[3.0, 4.0, 5.0, 12.0], 2);
        let _hold = fsl.clone(); // force shared buffer
        let expected = &[Some(0.6), Some(0.8), Some(5.0 / 13.0), Some(12.0 / 13.0)];
        assert_fsl_eq(&normalize_fsl_owned(fsl).unwrap(), expected, "fallback");
    }

    /// Null buffer is preserved through normalization.
    #[test]
    fn test_normalize_fsl_owned_preserves_nulls() {
        let values = Float32Array::from_iter_values([3.0, 4.0, 0.0, 0.0, 5.0, 12.0]);
        let nulls = NullBuffer::from(vec![true, false, true]);
        let field = Arc::new(Field::new("item", DataType::Float32, true));
        let fsl =
            FixedSizeListArray::try_new(field, 2, Arc::new(values), Some(nulls.clone())).unwrap();
        assert_eq!(normalize_fsl_owned(fsl).unwrap().nulls(), Some(&nulls));
    }
}
