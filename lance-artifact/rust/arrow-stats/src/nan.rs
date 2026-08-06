// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_array::cast::AsArray;
use arrow_array::types::{Float16Type, Float32Type, Float64Type};
use arrow_array::{Array, ArrayRef};

macro_rules! count_nans_typed {
    ($array:expr, $arrow_type:ty) => {{
        let typed = $array.as_primitive::<$arrow_type>();
        let mut count = 0u64;
        for i in 0..typed.len() {
            if !typed.is_null(i) && typed.value(i).is_nan() {
                count += 1;
            }
        }
        count
    }};
}

/// Count the number of non-null NaN values in an array.
///
/// Returns 0 for non-float types.
pub fn count_nans(array: &ArrayRef) -> u64 {
    use arrow_schema::DataType::*;
    match array.data_type() {
        Float16 => count_nans_typed!(array, Float16Type),
        Float32 => count_nans_typed!(array, Float32Type),
        Float64 => count_nans_typed!(array, Float64Type),
        _ => 0,
    }
}
