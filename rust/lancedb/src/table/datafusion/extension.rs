// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! The Lance extension types the write path treats specially.
//!
//! An extension type is identified by field metadata alone, so a rule that inspected storage
//! types instead would silently mishandle one. Write-path rules ask [`ExtensionKind::of`] so
//! that the set of types the write path knows about can be read off in one place.

use std::collections::HashMap;

use arrow_schema::{DataType, Field};
use lance_arrow::json::{ARROW_JSON_EXT_NAME, is_arrow_json_field, is_json_field};
use lance_arrow::{ARROW_EXT_NAME_KEY, FieldExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExtensionKind {
    /// `lance.json`: JSONB bytes in a LargeBinary column, how Lance stores a json column.
    Json,
    /// `arrow.json`: JSON text, which is what PyArrow's `pa.json_()` produces. Lance-core
    /// encodes it into [`ExtensionKind::Json`] as it writes.
    ArrowJson,
    /// `lance.blob.v2`: a struct describing where a blob's bytes live.
    BlobV2,
    /// Not an extension type the write path handles specially.
    Plain,
}

impl ExtensionKind {
    pub(super) fn of(field: &Field) -> Self {
        if is_json_field(field) {
            Self::Json
        } else if is_arrow_json_field(field) {
            Self::ArrowJson
        } else if field.is_blob_v2() {
            Self::BlobV2
        } else {
            Self::Plain
        }
    }
}

/// The storage type `arrow.json` would use to hold JSON text of type `input`.
///
/// `None` for a type that cannot hold JSON text, which is how callers tell "this input is
/// JSON the writer should encode" from "this input is something else entirely".
pub(super) fn arrow_json_storage_type(input: &DataType) -> Option<DataType> {
    match input {
        // arrow.json only recognises Utf8 and LargeUtf8 storage, so a view has to be cast.
        DataType::Utf8 | DataType::Utf8View => Some(DataType::Utf8),
        DataType::LargeUtf8 => Some(DataType::LargeUtf8),
        _ => None,
    }
}

pub(super) fn arrow_json_field(name: &str, storage: DataType, nullable: bool) -> Field {
    Field::new(name, storage, nullable).with_metadata(HashMap::from([(
        ARROW_EXT_NAME_KEY.to_string(),
        ARROW_JSON_EXT_NAME.to_string(),
    )]))
}
