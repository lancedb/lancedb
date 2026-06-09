// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Lance blob v2 columns store large binary payloads out of line.
//!
//! Declare a column with [`blob`]. On write, [`crate::table::Table::add`] coerces
//! raw `Binary` / `LargeBinary` into the blob struct layout. Queries return
//! small descriptors, not bytes.
//!
//! Blob tables require Lance file format >= 2.2 and stable row ids at create.

use arrow_schema::{Field, Schema};
use lance::dataset::WriteParams;
use lance_arrow::FieldExt;
use lance_encoding::version::LanceFileVersion;

/// Creates an Arrow field for a Lance blob v2 column.
///
/// `Struct<data, uri>` with the `lance.blob.v2` marker. Same layout Lance
/// expects on write.
///
/// ```
/// use arrow_schema::{DataType, Field, Schema};
///
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Int64, false),
///     lancedb::blob("image", true),
/// ]);
/// ```
///
/// Blob tables use Lance file format >= 2.2 and stable row ids at create.
pub fn blob(name: impl AsRef<str>, nullable: bool) -> Field {
    lance::blob::blob_field(name.as_ref(), nullable)
}

/// Returns true if `schema` declares any blob v2 column.
pub(crate) fn has_blob_columns(schema: &Schema) -> bool {
    schema.fields().iter().any(|field| field.is_blob_v2())
}

/// Bumps storage format to at least [`LanceFileVersion::V2_2`] for blob schemas.
pub(crate) fn ensure_blob_storage_version(schema: &Schema, params: &mut WriteParams) {
    if !has_blob_columns(schema) {
        return;
    }

    let resolved = params
        .data_storage_version
        .unwrap_or(LanceFileVersion::Stable)
        .resolve();
    if resolved < LanceFileVersion::V2_2 {
        params.data_storage_version = Some(LanceFileVersion::V2_2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
    use lance_arrow::ARROW_EXT_NAME_KEY;

    fn blob_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            blob("image", true),
        ])
    }

    #[test]
    fn blob_field_carries_v2_extension_marker() {
        let field = blob("image", true);
        assert_eq!(
            field.metadata().get(ARROW_EXT_NAME_KEY).map(String::as_str),
            Some("lance.blob.v2")
        );
        assert!(matches!(field.data_type(), DataType::Struct(_)));
    }

    #[test]
    fn has_blob_columns_detects_blob_fields() {
        assert!(has_blob_columns(&blob_schema()));
        let plain = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        assert!(!has_blob_columns(&plain));
    }

    #[test]
    fn storage_version_bumps_to_v2_2() {
        let mut params = WriteParams::default();
        ensure_blob_storage_version(&blob_schema(), &mut params);
        assert_eq!(
            params.data_storage_version.unwrap().resolve(),
            LanceFileVersion::V2_2
        );
    }

    #[test]
    fn storage_version_overrides_lower_explicit_version() {
        let mut params = WriteParams {
            data_storage_version: Some(LanceFileVersion::V2_0),
            ..Default::default()
        };
        ensure_blob_storage_version(&blob_schema(), &mut params);
        assert_eq!(
            params.data_storage_version.unwrap().resolve(),
            LanceFileVersion::V2_2
        );
    }

    #[test]
    fn storage_version_keeps_higher_explicit_version() {
        let mut params = WriteParams {
            data_storage_version: Some(LanceFileVersion::V2_3),
            ..Default::default()
        };
        ensure_blob_storage_version(&blob_schema(), &mut params);
        assert_eq!(params.data_storage_version.unwrap(), LanceFileVersion::V2_3);
    }

    #[test]
    fn storage_version_noop_without_blob_columns() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let mut params = WriteParams::default();
        ensure_blob_storage_version(&schema, &mut params);
        assert!(params.data_storage_version.is_none());
    }
}
