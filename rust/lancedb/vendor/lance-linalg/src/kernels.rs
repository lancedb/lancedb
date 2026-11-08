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
use half::{bf16, f16};
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

/// Finds the index of the maximum value in an iterator of optional values.
///
/// `None` elements are skipped.
///
/// Returns `None` if the iterator is empty, contains only `None` values,
/// or no value is strictly greater than the initial lower bound
/// (`T::min_value()`).
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

/// Finds the index and value of the element minimizing `value + bias`,
/// where `bias` comes from an optional second iterator.
///
/// The comparison uses `value + bias`, but the returned value is the
/// original `value` without the bias term. Passing `None` for `bias`
/// is equivalent to [`argmin_value_float`].
///
/// The two iterators are zipped, so iteration stops at the shorter one.
///
/// Returns `None` if the zipped iterator is empty or no `value + bias`
/// is strictly less than the initial bound (`T::infinity()`).
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

/// Finds the index and value of the minimum element in an iterator of
/// optional values.
///
/// `None` elements are skipped.
///
/// Returns `None` if the iterator is empty, contains only `None` values,
/// or no value is strictly less than the initial upper bound
/// (`T::max_value()`).
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

/// The accumulator used to sum squares when normalizing a `T` vector.
///
/// A type narrow enough that its squares leave its own range, and for which a
/// wider float exists, accumulates in that wider type: squaring an `f16`
/// saturates to `inf` at `|x| >= 256` and to zero at `|x| <= 1.726e-4`, so an
/// ordinary `f16` vector would otherwise normalize to all-zero or all-`inf`.
/// `f32` and `f64` accumulate in themselves — the same saturation still exists at
/// the extremes of their own range (an `f32` square overflows above `1.8447e19`),
/// but widening `f32` would perturb the output of existing f32 vectors by a few
/// ulp (about 10 at dimension 768, growing as sqrt(dim)), so it is deliberately
/// left alone; `f64` has nothing wider to widen to.
///
/// The width relation is a contract on the implementor, not something the bounds
/// can express. Tying the accumulator to the element type here still buys two
/// things over passing it in: the accumulator cannot drift between the three
/// dispatch sites, and no call site can pick the wrong one.
pub trait Normalizable: Float + AsPrimitive<Self::Acc> {
    /// Must be at least as wide as `Self` in both exponent and mantissa.
    type Acc: Float + Sum + AsPrimitive<Self> + AsPrimitive<f32>;
}

/// `f16` squares leave its own range, so it accumulates in `f32` — which has
/// 2^96 of headroom over `f16::MAX` squared.
impl Normalizable for f16 {
    type Acc = f32;
}

/// `bf16` has the same exponent range as `f32` (both 8 bits), so widening to
/// `f32` would buy no headroom for squaring — `bf16(1e20)` squared already
/// overflows `f32`. It needs `f64`.
impl Normalizable for bf16 {
    type Acc = f64;
}

impl Normalizable for f32 {
    type Acc = Self;
}

impl Normalizable for f64 {
    type Acc = Self;
}

/// L2 normalize a vector.
///
/// Returns an iterator of normalized values, and the norm as `f32`.
///
/// The sum of squares is accumulated in [`Normalizable::Acc`], which is wider
/// than `T` where `T` alone would overflow.
pub fn normalize<T: Normalizable>(v: &[T]) -> (impl Iterator<Item = T> + '_, f32) {
    let l2_norm = v
        .iter()
        .map(|x| {
            let x: T::Acc = x.as_();
            x * x
        })
        .sum::<T::Acc>()
        .sqrt();
    (
        v.iter().map(move |&x| (x.as_() / l2_norm).as_()),
        l2_norm.as_(),
    )
}

fn do_normalize_arrow<T: ArrowPrimitiveType>(arr: &dyn Array) -> Result<(ArrayRef, f32)>
where
    T::Native: Normalizable,
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
    T::Native: Normalizable,
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
    T::Native: Normalizable,
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
                // Accumulate in the wider type; see [`Normalizable`].
                let l2_norm = chunk
                    .iter()
                    .map(|x| {
                        let x: <T::Native as Normalizable>::Acc = x.as_();
                        x * x
                    })
                    .sum::<<T::Native as Normalizable>::Acc>()
                    .sqrt();
                for x in chunk.iter_mut() {
                    *x = (x.as_() / l2_norm).as_();
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
        Float16Array, Float32Array, Float64Array, Int8Array, Int16Array, LargeStringArray,
        StringArray, UInt8Array, UInt32Array,
    };
    use arrow_buffer::NullBuffer;
    use arrow_schema::Field;
    use half::f16;
    use rstest::rstest;

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

    #[rstest]
    #[case::no_bias(vec![5.0, 1.0, 3.0], None::<Vec<f32>>, (1, 1.0))]
    #[case::biased_value(vec![5.0, 8.0], Some(vec![-1.0, -5.0]), (1, 8.0))]
    // The unpaired value would win if the shorter iterator were not truncated.
    #[case::shorter_bias(vec![5.0, 8.0, 1.0], Some(vec![-1.0, -5.0]), (1, 8.0))]
    fn test_argmin_value_float_with_bias(
        #[case] values: Vec<f32>,
        #[case] bias: Option<Vec<f32>>,
        #[case] expected: (u32, f32),
    ) {
        let f = Float32Array::from(values);
        assert_eq!(
            argmin_value_float_with_bias(
                f.values().iter().copied(),
                bias.map(|bias| bias.into_iter())
            ),
            Some(expected)
        );
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

    /// The accumulator must not be *narrower* than the element type either.
    /// Accumulating f64 in f32 overflows above `|x| = 1.8447e19` (where f64 has
    /// headroom to 1.34e154), collapses to a zero norm at or below `|x| = 2^-75`
    /// (2.647e-23), and costs ~29 bits of mantissa on every ordinary vector.
    #[test]
    fn test_normalize_f64_accumulates_wide() {
        // Range: each case is finite and correctly normalizable in f64, but
        // overflows or underflows an f32 accumulator.
        let range_cases: &[(&str, Vec<f64>)] = &[
            ("square_overflows", vec![1e20, 0.0]),
            ("sum_overflows", vec![1e19; 12]),
            ("square_underflows", vec![1e-25, 1e-25]),
            ("element_underflows", vec![1e-100, 1e-100]),
        ];
        for (name, v) in range_cases {
            // Cover all three public entry points: each has its own `match` over
            // the element type, so a dispatch cell could regress on its own.
            for out in [
                ("normalize_arrow", normalize_f64(v)),
                ("normalize_fsl", normalize_f64_fsl(v, false)),
                ("normalize_fsl_owned", normalize_f64_fsl(v, true)),
            ] {
                let (entry, out) = out;
                let norm = out.iter().map(|x| x * x).sum::<f64>().sqrt();
                assert!(
                    approx::relative_eq!(norm, 1.0, max_relative = 1e-9),
                    "{entry} / {name}: normalized norm {norm} != 1, output {out:?}"
                );
            }
        }

        // Precision: an f32 accumulator would round the output to f32, leaving
        // a relative error around f32::EPSILON (~1.2e-7).
        let v = vec![1.0_f64, 2.0, 3.0];
        let expected_norm = 14.0_f64.sqrt();
        let out = normalize_f64(&v);
        for (i, (&got, &raw)) in out.iter().zip(v.iter()).enumerate() {
            let want = raw / expected_norm;
            assert!(
                approx::relative_eq!(got, want, max_relative = 1e-15),
                "element {i}: got {got:.17}, want {want:.17}"
            );
        }
    }

    /// Normalize an `f64` slice through the public Arrow entry point, so the
    /// test exercises the accumulator `normalize_arrow` actually selects.
    fn normalize_f64(v: &[f64]) -> Vec<f64> {
        let arr = Float64Array::from(v.to_vec());
        let (out, _) = normalize_arrow(&arr).unwrap();
        out.as_primitive::<Float64Type>().values().to_vec()
    }

    /// Same, through the FSL entry points. `owned` selects
    /// [`normalize_fsl_owned`], whose freshly built array takes the in-place
    /// branch of `do_normalize_fsl_inplace`.
    fn normalize_f64_fsl(v: &[f64], owned: bool) -> Vec<f64> {
        let values = Float64Array::from(v.to_vec());
        let field = Arc::new(Field::new("item", DataType::Float64, true));
        let fsl =
            FixedSizeListArray::try_new(field, v.len() as i32, Arc::new(values), None).unwrap();
        let out = if owned {
            normalize_fsl_owned(fsl).unwrap()
        } else {
            normalize_fsl(&fsl).unwrap()
        };
        out.values().as_primitive::<Float64Type>().values().to_vec()
    }

    /// `normalize` must accumulate the sum of squares in a type wider than the
    /// element type. `f16::powi` rounds each square back to `f16`, which
    /// saturates to `inf` at `|x| >= 256` and to zero at `|x| <= 1.726e-4`, so an
    /// ordinary vector normalizes to all-zero or all-`inf`.
    #[test]
    fn test_normalize_f16_accumulates_wide() {
        let cases: &[(&str, &[f32])] = &[
            // A single element whose square leaves the f16 range.
            ("square_overflows", &[256.0, 0.0]),
            // No element overflows, but the sum of squares does.
            ("sum_overflows", &[100.0; 7]),
            // Every square rounds to zero, so the norm is zero and x/0 is inf.
            ("square_underflows", &[1e-4; 8]),
        ];
        for (name, input) in cases {
            let v = input.iter().map(|&x| f16::from_f32(x)).collect::<Vec<_>>();
            // Independent reference: accumulate the same f16 inputs in f64.
            let expected = v
                .iter()
                .map(|x| x.to_f64() * x.to_f64())
                .sum::<f64>()
                .sqrt();
            let (normalized, norm) = normalize(&v);
            assert!(
                approx::relative_eq!(norm, expected as f32, max_relative = 1e-3),
                "{name}: norm {norm} != expected {expected}"
            );
            let normalized = normalized.collect::<Vec<_>>();
            assert!(
                normalized.iter().all(|x| x.is_finite()),
                "{name}: non-finite output {normalized:?}"
            );

            // The output must be a unit vector. This is the assertion that pins
            // the division: an independent f64 sum of the squares, not a
            // comparison against another call into the same code.
            let unit = normalized
                .iter()
                .map(|x| x.to_f64() * x.to_f64())
                .sum::<f64>();
            assert!(
                approx::relative_eq!(unit, 1.0, max_relative = 1e-2),
                "{name}: output is not a unit vector, sum of squares {unit}"
            );

            // Also drive the `normalize_arrow` Float16 arm, so the dispatch cell
            // is covered. Equality against the generic path only proves the two
            // agree — the unit-norm check above is what proves either is right.
            let (out, arrow_norm) = normalize_arrow(&Float16Array::from(v)).unwrap();
            assert!(
                approx::relative_eq!(arrow_norm, expected as f32, max_relative = 1e-3),
                "{name}: normalize_arrow norm {arrow_norm} != expected {expected}"
            );
            let out = out.as_primitive::<Float16Type>();
            assert_eq!(
                out.values().as_ref(),
                normalized.as_slice(),
                "{name}: normalize_arrow values differ from the generic path"
            );
        }
    }

    /// `bf16` shares f32's exponent range, so it needs an `f64` accumulator —
    /// `f32` would leave the same overflow the f16 case exists to fix.
    #[test]
    fn test_normalize_bf16_accumulates_wide() {
        let cases: &[(&str, &[f32])] = &[
            ("square_overflows", &[1e20, 0.0]),
            ("sum_overflows", &[1e19; 12]),
            ("square_underflows", &[1e-25, 1e-25]),
        ];
        for (name, input) in cases {
            let v = input.iter().map(|&x| bf16::from_f32(x)).collect::<Vec<_>>();
            let expected = v
                .iter()
                .map(|x| x.to_f64() * x.to_f64())
                .sum::<f64>()
                .sqrt();
            let (normalized, norm) = normalize(&v);
            let normalized = normalized.collect::<Vec<_>>();
            assert!(
                approx::relative_eq!(norm as f64, expected, max_relative = 1e-2),
                "{name}: norm {norm} != expected {expected}"
            );
            let unit = normalized
                .iter()
                .map(|x| x.to_f64() * x.to_f64())
                .sum::<f64>();
            assert!(
                approx::relative_eq!(unit, 1.0, max_relative = 1e-2),
                "{name}: output is not a unit vector, sum of squares {unit}"
            );
        }
    }

    /// Both FSL entry points share the defect, including the in-place path in
    /// [`do_normalize_fsl_inplace`], which has its own copy of the expression.
    #[test]
    fn test_normalize_fsl_f16_accumulates_wide() {
        // dim 2, row 0 overflows at the square, row 1 underflows at the square.
        let make = || {
            let values =
                Float16Array::from_iter_values([256.0f32, 0.0, 1e-4, 1e-4].map(f16::from_f32));
            let field = Arc::new(Field::new("item", DataType::Float16, true));
            FixedSizeListArray::try_new(field, 2, Arc::new(values), None).unwrap()
        };

        // `normalize_fsl_owned` gets a freshly built array so the buffer is
        // uniquely owned and the in-place branch is the one exercised.
        let outputs = [
            ("normalize_fsl", normalize_fsl(&make()).unwrap()),
            ("normalize_fsl_owned", normalize_fsl_owned(make()).unwrap()),
        ];
        for (label, out) in outputs {
            let got = out.values().as_primitive::<Float16Type>();
            for (row, chunk) in got.values().chunks(2).enumerate() {
                let norm = chunk
                    .iter()
                    .map(|x| x.to_f64() * x.to_f64())
                    .sum::<f64>()
                    .sqrt();
                assert!(
                    approx::relative_eq!(norm, 1.0, max_relative = 1e-2),
                    "{label} row {row}: normalized norm {norm} != 1, values {chunk:?}"
                );
            }
        }
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
