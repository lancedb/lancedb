// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Vector preprocessing utilities for incoming data streams.
//!
//! Handles two separate concerns, split into submodules:
//!
//! 1. [`schema_inference`] — detects variable-length list columns whose names suggest
//!    they are vectors, peeks the first batch to determine the modal dimension, and
//!    converts them to [`FixedSizeList`](arrow_schema::DataType::FixedSizeList) columns.
//!    Mismatched-length vectors are handled via [`BadVectorStrategy`].
//!
//! 2. [`nan_handling`] — scans ALL float vector columns (`FixedSizeList`, `List`,
//!    and `LargeList`) regardless of name and handles rows containing NaN values
//!    via [`NanStrategy`].

mod nan_handling;
mod schema_inference;

pub use nan_handling::{handle_nan_vectors, NanStrategy};
pub use schema_inference::{
    find_vector_candidates, infer_vector_schema, BadVectorStrategy, FieldPath,
};

use arrow_array::BooleanArray;

use crate::error::Result;

/// OR two boolean arrays together.
fn or_boolean(a: &BooleanArray, b: &BooleanArray) -> Result<BooleanArray> {
    Ok(arrow::compute::or(a, b)?)
}
