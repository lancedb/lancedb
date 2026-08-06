// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use half::{bf16, f16};
use proptest::prelude::*;

/// Arbitrary finite f16 value.
pub fn arbitrary_f16() -> impl Strategy<Value = f16> {
    any::<u16>().prop_map(|bits| {
        // Convert arbitrary u16 to f16
        let val = f16::from_bits(bits);
        // Convert Inf -> Max, -Inf -> Min, NaN -> 0
        if val.is_infinite() && val.is_sign_positive() {
            f16::MAX
        } else if val.is_infinite() && val.is_sign_negative() {
            f16::MIN
        } else if val.is_nan() {
            f16::from_f32(0.0)
        } else {
            val
        }
    })
}

pub fn arbitrary_bf16() -> impl Strategy<Value = bf16> {
    any::<u16>()
        .prop_map(|bits| {
            // Convert arbitrary u16 to bf16
            let val = bf16::from_bits(bits);
            // Convert Inf -> Max, -Inf -> Min, NaN -> 0
            if val.is_infinite() && val.is_sign_positive() {
                bf16::MAX
            } else if val.is_infinite() && val.is_sign_negative() {
                bf16::MIN
            } else if val.is_nan() {
                bf16::from_f32(0.0)
            } else {
                val
            }
        })
        .prop_map(|val: bf16| {
            let scaling = bf16::from_f32(1e12 / f32::MAX);
            val * scaling
        })
}

/// Arbitrary finite f32 value, in the range of +-1e12.
///
/// We limit the range to avoid overflow. The f32 Max is around 3.4e38, so this
/// gives enough room for multiplying and adding without overflow.
pub fn arbitrary_f32() -> impl Strategy<Value = f32> {
    use proptest::num::f32::*;
    let scaling = 1e12 / f32::MAX;
    (NORMAL | SUBNORMAL | POSITIVE | NEGATIVE).prop_map(move |val: f32| val * scaling)
}

/// Arbitrary finite f64 value, in the range of +-1e12.
///
/// We limit the range to avoid overflow. Right now, it's mainly limited to
/// keep L2 norm finite. If we changed L2 Norm to be able to return a f64, we
/// can broaden these test values.
pub fn arbitrary_f64() -> impl Strategy<Value = f64> {
    use proptest::num::f64::*;
    let scaling = 1e12 / f64::MAX;
    (NORMAL | SUBNORMAL | POSITIVE | NEGATIVE).prop_map(move |val: f64| val * scaling)
}

/// Two arbitrary vectors with matching dimensions
pub fn arbitrary_vector_pair<T: std::fmt::Debug, S>(
    values: impl Fn() -> S + 'static,
    dim_range: std::ops::Range<usize>,
) -> impl Strategy<Value = (Vec<T>, Vec<T>)>
where
    S: Strategy<Value = T>,
{
    dim_range.prop_flat_map(move |dim| {
        let x = prop::collection::vec(values(), dim);
        let y = prop::collection::vec(values(), dim);
        (x, y)
    })
}
