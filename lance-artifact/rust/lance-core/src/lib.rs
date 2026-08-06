// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
#![cfg_attr(coverage, feature(coverage_attribute))]

// Allow the derive macro to reference `lance_core::deepsize` from within this crate.
extern crate self as lance_core;

use arrow_schema::{DataType, Field as ArrowField};
use std::sync::LazyLock;

pub mod cache;
pub mod container;
pub mod datatypes;
pub mod deepsize;
pub mod error;
pub mod levenshtein;
pub mod traits;
pub mod utils;

pub use error::{ArrowResult, Error, FenceReason, Result, box_error};

/// Wildcard to indicate all non-system columns
pub const WILDCARD: &str = "*";
/// Column name for the meta row ID.
pub const ROW_ID: &str = "_rowid";
/// Column name for the meta row address.
pub const ROW_ADDR: &str = "_rowaddr";
/// Column name for the meta row offset.
pub const ROW_OFFSET: &str = "_rowoffset";
/// Column name for the row's last updated at dataset version.
pub const ROW_LAST_UPDATED_AT_VERSION: &str = "_row_last_updated_at_version";
/// Column name for the row's created at dataset version.
pub const ROW_CREATED_AT_VERSION: &str = "_row_created_at_version";

/// Row ID field. This is nullable because its validity bitmap is sometimes used
/// as a selection vector.
pub static ROW_ID_FIELD: LazyLock<ArrowField> =
    LazyLock::new(|| ArrowField::new(ROW_ID, DataType::UInt64, true));
/// Row address field. This is nullable because its validity bitmap is sometimes used
/// as a selection vector.
pub static ROW_ADDR_FIELD: LazyLock<ArrowField> =
    LazyLock::new(|| ArrowField::new(ROW_ADDR, DataType::UInt64, true));
/// Row offset field. This is nullable merely for compatibility with the other
/// fields.
pub static ROW_OFFSET_FIELD: LazyLock<ArrowField> =
    LazyLock::new(|| ArrowField::new(ROW_OFFSET, DataType::UInt64, true));
/// Row last updated at version field.
pub static ROW_LAST_UPDATED_AT_VERSION_FIELD: LazyLock<ArrowField> =
    LazyLock::new(|| ArrowField::new(ROW_LAST_UPDATED_AT_VERSION, DataType::UInt64, true));
/// Row created at version field.
pub static ROW_CREATED_AT_VERSION_FIELD: LazyLock<ArrowField> =
    LazyLock::new(|| ArrowField::new(ROW_CREATED_AT_VERSION, DataType::UInt64, true));

/// Check if a column name is a system column.
///
/// System columns are virtual columns that are computed at read time and don't
/// exist in the physical data files. They include:
/// - `_rowid`: The row ID
/// - `_rowaddr`: The row address
/// - `_rowoffset`: The row offset
/// - `_row_last_updated_at_version`: The version when the row was last updated
/// - `_row_created_at_version`: The version when the row was created
pub fn is_system_column(column_name: &str) -> bool {
    matches!(
        column_name,
        ROW_ID | ROW_ADDR | ROW_OFFSET | ROW_LAST_UPDATED_AT_VERSION | ROW_CREATED_AT_VERSION
    )
}
