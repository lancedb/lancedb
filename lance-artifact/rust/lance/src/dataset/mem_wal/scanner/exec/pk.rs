// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Shared primary-key helpers for the LSM scanner execution nodes.
//!
//! Centralizes PK column resolution and per-row hashing so that every
//! consumer (e.g. [`super::PkBlockFilterExec`])
//! resolves and hashes a primary key the same way. The row hash is kept
//! consistent with the variants supported by [`super::compute_pk_hash_from_scalars`]
//! so a single PK produces the same hash regardless of which exec consumes it.

use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Schema};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DFResult};
use lance_core::{Error, Result};

/// Column name for a row address (the in-source row offset).
pub const ROW_ADDRESS_COLUMN: &str = "_rowaddr";

/// Resolve the column index of each primary-key column in `batch`.
pub fn resolve_pk_indices(batch: &RecordBatch, pk_columns: &[String]) -> DFResult<Vec<usize>> {
    pk_columns
        .iter()
        .map(|col| {
            batch
                .schema()
                .column_with_name(col)
                .map(|(idx, _)| idx)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!("Primary key column '{}' not found", col))
                })
        })
        .collect()
}

/// Primary-key column types we can hash exactly in the fast path.
///
/// Anything else is rejected by [`validate_pk_types`] at the scanner boundary,
/// so the hot hash path never silently collapses distinct keys to one hash
/// (which would over-block in the block-list and drop live rows).
pub fn is_supported_pk_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Boolean
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
    )
}

/// Validate that every primary-key column has a type we can hash exactly.
///
/// Rejects unsupported types with a descriptive error at the API boundary
/// rather than degrading to a constant hash. Call this where a scanner that
/// hashes primary keys is built.
pub fn validate_pk_types(schema: &Schema, pk_columns: &[String]) -> Result<()> {
    for col in pk_columns {
        let field = schema.field_with_name(col).map_err(|_| {
            Error::invalid_input(format!("Primary key column '{}' not found in schema", col))
        })?;
        if !is_supported_pk_type(field.data_type()) {
            return Err(Error::invalid_input(format!(
                "Primary key column '{}' has unsupported type {:?} for hashing; supported types: \
                 Int8/16/32/64, UInt8/16/32/64, Boolean, Utf8/LargeUtf8, Binary/LargeBinary",
                col,
                field.data_type()
            )));
        }
    }
    Ok(())
}

/// Hash a single row's primary key, identified by the `pk_indices` column
/// positions and `row_idx`.
///
/// Must stay byte-for-byte consistent with
/// [`super::compute_pk_hash_from_scalars`] so a single PK hashes the same
/// regardless of which exec consumes it. Supported types are validated up
/// front by [`validate_pk_types`]; the trailing branch is a defensive,
/// value-distinguishing fallback that should be unreachable in validated plans.
pub fn compute_pk_hash(batch: &RecordBatch, pk_indices: &[usize], row_idx: usize) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    for &col_idx in pk_indices {
        let col = batch.column(col_idx);
        let is_null = col.is_null(row_idx);
        is_null.hash(&mut hasher);

        if !is_null {
            if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int8Array>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int16Array>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int32Array>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::UInt8Array>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::UInt16Array>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::UInt32Array>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::UInt64Array>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::BooleanArray>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::StringArray>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::LargeStringArray>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::BinaryArray>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::LargeBinaryArray>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Ok(scalar) = ScalarValue::try_from_array(col.as_ref(), row_idx) {
                // Defensive fallback: distinguish by value rather than collapse.
                format!("{:?}", scalar).hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}
