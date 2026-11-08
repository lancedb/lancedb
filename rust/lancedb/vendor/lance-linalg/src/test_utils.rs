// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use half::{bf16, f16};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseResult, TestRunner};
use std::ops::Range;

const CASES_PER_DIMENSION_SHARD: u32 = 16;
const MIN_TEST_DIMENSION: usize = 4;
const MAX_TEST_DIMENSION: usize = 4048;
const NUM_DIMENSION_SHARDS: usize = 16;

pub fn dimension_shard(shard: usize) -> Range<usize> {
    let dimensions_per_shard = (MAX_TEST_DIMENSION - MIN_TEST_DIMENSION) / NUM_DIMENSION_SHARDS;
    let start = MIN_TEST_DIMENSION + shard * dimensions_per_shard;
    let end = if shard + 1 == NUM_DIMENSION_SHARDS {
        MAX_TEST_DIMENSION
    } else {
        start + dimensions_per_shard
    };
    start..end
}

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

pub fn run_vector_pair_proptest<T, S, F>(values: fn() -> S, dim_range: Range<usize>, property: F)
where
    T: std::fmt::Debug,
    S: Strategy<Value = T> + 'static,
    F: Fn(Vec<T>, Vec<T>) -> TestCaseResult,
{
    let strategy = arbitrary_vector_pair(values, dim_range);
    let mut runner = TestRunner::new(Config {
        cases: CASES_PER_DIMENSION_SHARD,
        ..Config::default()
    });
    runner.run(&strategy, |(x, y)| property(x, y)).unwrap();
}

pub fn run_vector_proptest<T, S, F>(values: fn() -> S, dim_range: Range<usize>, property: F)
where
    T: std::fmt::Debug,
    S: Strategy<Value = T>,
    F: Fn(Vec<T>) -> TestCaseResult,
{
    let strategy = prop::collection::vec(values(), dim_range);
    let mut runner = TestRunner::new(Config {
        cases: CASES_PER_DIMENSION_SHARD,
        ..Config::default()
    });
    runner.run(&strategy, property).unwrap();
}

/// Input pairs shared by the u8 distance kernel tests.
pub mod u8_inputs {
    /// Covers empty input and both sides of the 32- and 64-element SIMD strides.
    pub const LENGTHS: &[usize] = &[
        0, 1, 7, 15, 16, 31, 32, 33, 63, 64, 65, 127, 128, 255, 256, 1024, 4096, 4097,
    ];

    #[derive(Clone, Copy, Debug)]
    pub enum Pattern {
        Random,
        Extremes,
        OneSidedZeros,
    }

    impl Pattern {
        /// Labeled input pairs of length `len`.
        pub fn pairs(self, len: usize) -> Vec<(&'static str, Vec<u8>, Vec<u8>)> {
            let filled = |value| vec![value; len];
            match self {
                Self::Random => (0..4)
                    .map(|seed| ("random", random(len, 2 * seed), random(len, 2 * seed + 1)))
                    .collect(),
                Self::Extremes => {
                    let alternating = |first| {
                        (0..len)
                            .map(|i| if i % 2 == 0 { first } else { u8::MAX - first })
                            .collect::<Vec<_>>()
                    };
                    vec![
                        ("max-0", filled(u8::MAX), filled(0)),
                        ("0-max", filled(0), filled(u8::MAX)),
                        ("max-max", filled(u8::MAX), filled(u8::MAX)),
                        ("0-0", filled(0), filled(0)),
                        ("1-1", filled(1), filled(1)),
                        ("alternating", alternating(0), alternating(u8::MAX)),
                    ]
                }
                Self::OneSidedZeros => vec![
                    ("b=0", random(len, 0), filled(0)),
                    ("a=0", filled(0), random(len, 1)),
                ],
            }
        }
    }

    pub fn random(len: usize, seed: u32) -> Vec<u8> {
        let mut state = 0xC0FFEE_u32.wrapping_add(seed.wrapping_mul(7919));
        (0..len)
            .map(|_| {
                state = state.wrapping_mul(1103515245).wrapping_add(12345);
                (state >> 16) as u8
            })
            .collect()
    }
}
