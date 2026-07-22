// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Lance blob v2 columns store large binary payloads out of line.
//!
//! Declare a column with [`blob`]. On write, [`crate::table::Table::add`] coerces
//! raw `Binary` / `LargeBinary` into the blob struct layout. Queries return
//! small descriptors, not bytes.
//!
//! Blob tables require Lance file format >= 2.2 and stable row ids at create.

use std::sync::Arc;

use arrow_array::builder::LargeBinaryBuilder;
use arrow_array::{Array, LargeBinaryArray, RecordBatch, StructArray, UInt8Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::dataset::{BlobRangeRequest as LanceBlobRangeRequest, Dataset, WriteParams};
use lance_arrow::FieldExt;
use lance_core::datatypes::parse_field_path;
use lance_encoding::version::LanceFileVersion;

use crate::error::{Error, Result};

pub use lance::dataset::BlobFile;

/// One row-specific blob range read request.
///
/// `row_id` is obtained from a query with row ids enabled.
/// `offset` and `length` are relative to the beginning of the logical blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobRangeRequest {
    /// Row id of the blob value to read.
    pub row_id: u64,
    /// Byte offset from the beginning of the blob value.
    pub offset: u64,
    /// Number of bytes to read.
    pub length: u64,
}

impl BlobRangeRequest {
    /// Create a row-specific blob range request.
    pub const fn new(row_id: u64, offset: u64, length: u64) -> Self {
        Self {
            row_id,
            offset,
            length,
        }
    }
}

fn map_blob_read_error(source: lance::Error) -> Error {
    match source {
        source @ lance::Error::InvalidInput { .. } => Error::InvalidInput {
            message: source.to_string(),
        },
        source => source.into(),
    }
}

/// Creates an Arrow field for a Lance blob v2 column.
///
/// `Struct<data, uri>` with the `lance.blob.v2` marker. Same layout Lance
/// expects on write.
///
/// A blob column may be top-level or nested inside a struct or list. Nested
/// blobs are addressed by a dotted path (e.g. `info.blob`) in the read APIs.
///
/// ```
/// use arrow_schema::{DataType, Field, Schema};
///
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Int64, false),
///     lancedb::blob("image", true),
/// ]);
/// ```
pub fn blob(name: impl AsRef<str>, nullable: bool) -> Field {
    lance::blob::blob_field(name.as_ref(), nullable)
}

/// Returns true if `field` is a blob v2 column.
///
/// ```
/// let field = lancedb::blob("image", true);
/// assert!(lancedb::blob::is_blob(&field));
/// ```
pub fn is_blob(field: &Field) -> bool {
    field.is_blob_v2()
}

/// Returns true if `field`, or any field nested under it, is a blob v2 column.
fn field_tree_has_blob_v2(field: &Field) -> bool {
    if field.is_blob_v2() {
        return true;
    }
    match field.data_type() {
        DataType::Struct(children) => children.iter().any(|c| field_tree_has_blob_v2(c)),
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            field_tree_has_blob_v2(child)
        }
        _ => false,
    }
}

/// Collects the dotted paths of blob v2 columns under `field`, into `paths`.
fn collect_blob_paths(field: &Field, prefix: &str, paths: &mut Vec<String>) {
    let path = if prefix.is_empty() {
        field.name().clone()
    } else {
        format!("{prefix}.{}", field.name())
    };
    if field.is_blob_v2() {
        paths.push(path);
        return;
    }
    match field.data_type() {
        DataType::Struct(children) => {
            for child in children {
                collect_blob_paths(child, &path, paths);
            }
        }
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            collect_blob_paths(child, &path, paths)
        }
        _ => {}
    }
}

/// Returns true if `schema` declares any blob v2 column, including nested ones.
pub(crate) fn has_blob_columns(schema: &Schema) -> bool {
    schema.fields().iter().any(|f| field_tree_has_blob_v2(f))
}

/// Blob v2 column paths in `schema`, declaration order preserved. Nested blobs
/// are dotted paths (e.g. `info.blob`).
pub(crate) fn blob_column_names(schema: &Schema) -> Vec<String> {
    let mut paths = Vec::new();
    for field in schema.fields() {
        collect_blob_paths(field, "", &mut paths);
    }
    paths
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

/// Validate that `column` exists and is a blob v2 column.
///
/// Legacy v1 columns (`lance-encoding:blob`) error with a migration hint.
pub(crate) fn ensure_blob_v2_column(
    schema: &lance_core::datatypes::Schema,
    column: &str,
) -> Result<()> {
    match schema.field(column) {
        Some(field) if field.is_blob_v2() => Ok(()),
        Some(field) if field.is_blob() => Err(Error::InvalidInput {
            message: format!(
                "column '{column}' is a legacy blob column; blob APIs require blob v2 columns \
                 (ARROW:extension:name = \"lance.blob.v2\")"
            ),
        }),
        Some(_) => Err(Error::InvalidInput {
            message: format!("column '{column}' is not a blob column"),
        }),
        None => Err(Error::InvalidInput {
            message: format!("no column named '{column}' in this table"),
        }),
    }
}

/// Returns the leaf descriptor `StructArray` for `column` in a descriptor batch.
fn leaf_descriptor_struct<'a>(batch: &'a RecordBatch, column: &str) -> Result<&'a StructArray> {
    let path = parse_field_path(column).map_err(|e| Error::InvalidInput {
        message: format!("invalid blob column path '{column}': {e}"),
    })?;
    let not_struct = || Error::Runtime {
        message: format!("blob column '{column}' did not read back as a descriptor struct"),
    };
    let mut current = batch
        .column_by_name(&path[0])
        .and_then(|c| c.as_any().downcast_ref::<StructArray>())
        .ok_or_else(not_struct)?;
    for segment in &path[1..] {
        current = current
            .column_by_name(segment)
            .and_then(|c| c.as_any().downcast_ref::<StructArray>())
            .ok_or_else(not_struct)?;
    }
    Ok(current)
}

/// Null rows in `row_ids`, from a descriptor take.
///
/// Lance `read_blobs` / `take_blobs` skip null rows (`kind == 0 && position == 0 && size == 0`).
/// TODO(lance): aligned read API would drop this pass.
async fn blob_null_mask(
    dataset: &Arc<Dataset>,
    column: &str,
    row_ids: &[u64],
) -> Result<Vec<bool>> {
    let projection = dataset.schema().project(&[column])?;
    let descriptors = dataset.take_builder(row_ids, projection)?.execute().await?;
    if descriptors.num_rows() != row_ids.len() {
        return Err(Error::InvalidInput {
            message: format!(
                "blob take for column '{column}' requested {} row ids but only {} exist in the \
                 table; pass row ids collected from this table",
                row_ids.len(),
                descriptors.num_rows()
            ),
        });
    }
    let descriptor_struct = leaf_descriptor_struct(&descriptors, column)?;
    let child = |name: &str| {
        descriptor_struct
            .column_by_name(name)
            .ok_or_else(|| Error::Runtime {
                message: format!("blob descriptor for '{column}' is missing the '{name}' field"),
            })
    };
    let kinds = child("kind")?
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| Error::Runtime {
            message: format!("blob descriptor 'kind' for '{column}' is not a UInt8 array"),
        })?;
    let positions = child("position")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| Error::Runtime {
            message: format!("blob descriptor 'position' for '{column}' is not a UInt64 array"),
        })?;
    let sizes = child("size")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| Error::Runtime {
            message: format!("blob descriptor 'size' for '{column}' is not a UInt64 array"),
        })?;

    // Match Lance `collect_blob_entries_v2` skip condition (`BlobKind::Inline` == 0).
    Ok((0..descriptor_struct.len())
        .map(|i| {
            descriptor_struct.is_null(i)
                || kinds.is_null(i)
                || (kinds.value(i) == 0 && positions.value(i) == 0 && sizes.value(i) == 0)
        })
        .collect())
}

fn non_null_row_ids(row_ids: &[u64], null_mask: &[bool]) -> Vec<u64> {
    row_ids
        .iter()
        .zip(null_mask)
        .filter_map(|(row_id, is_null)| (!is_null).then_some(*row_id))
        .collect()
}

/// Materialize blob-local ranges (same length and order as `requests`, nulls preserved).
pub(crate) async fn take_blob_ranges_aligned(
    dataset: &Arc<Dataset>,
    column: &str,
    requests: &[BlobRangeRequest],
) -> Result<LargeBinaryArray> {
    ensure_blob_v2_column(dataset.schema(), column)?;
    if requests.is_empty() {
        return Ok(LargeBinaryBuilder::new().finish());
    }

    let lance_requests = requests
        .iter()
        .map(|request| LanceBlobRangeRequest::new(request.row_id, request.offset, request.length))
        .collect::<Vec<_>>();
    let reader = dataset
        .read_blob_ranges(column)
        .map_err(map_blob_read_error)?
        .with_row_ids(lance_requests)
        .preserve_order(false);

    let mut stream = reader
        .try_into_stream()
        .await
        .map_err(map_blob_read_error)?;
    let mut payloads = vec![None; requests.len()];
    while let Some(result) = stream.try_next().await.map_err(map_blob_read_error)? {
        let slot = payloads
            .get_mut(result.request_index)
            .ok_or_else(|| Error::Runtime {
                message: format!(
                    "blob range read for column '{column}' returned out-of-range request index {} for {} requests",
                    result.request_index,
                    requests.len()
                ),
            })?;
        if slot.replace(result.data).is_some() {
            return Err(Error::Runtime {
                message: format!(
                    "blob range read for column '{column}' returned request index {} more than once",
                    result.request_index
                ),
            });
        }
    }

    let missing_request_indices = payloads
        .iter()
        .enumerate()
        .filter_map(|(request_index, payload)| payload.is_none().then_some(request_index))
        .collect::<Vec<_>>();
    if !missing_request_indices.is_empty() {
        let missing_row_ids = missing_request_indices
            .iter()
            .map(|request_index| requests[*request_index].row_id)
            .collect::<Vec<_>>();
        let null_mask = blob_null_mask(dataset, column, &missing_row_ids).await?;
        for (request_index, is_null) in missing_request_indices.into_iter().zip(null_mask) {
            if !is_null {
                return Err(Error::Runtime {
                    message: format!(
                        "blob range read for column '{column}' did not return request index {request_index}"
                    ),
                });
            }
        }
    }

    let mut builder = LargeBinaryBuilder::new();
    for payload in payloads {
        match payload {
            Some(data) => builder.append_value(data.as_ref()),
            None => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

/// Materialize blob bytes for `row_ids` (same length and order, nulls preserved).
pub(crate) async fn take_blobs_aligned(
    dataset: &Arc<Dataset>,
    column: &str,
    row_ids: &[u64],
) -> Result<LargeBinaryArray> {
    ensure_blob_v2_column(dataset.schema(), column)?;
    if row_ids.is_empty() {
        return Ok(LargeBinaryBuilder::new().finish());
    }

    let null_mask = blob_null_mask(dataset, column, row_ids).await?;
    let non_null_row_ids = non_null_row_ids(row_ids, &null_mask);
    let non_null_count = non_null_row_ids.len();
    let payloads = if non_null_count == 0 {
        Vec::new()
    } else {
        dataset
            .read_blobs(column)?
            .with_row_ids(non_null_row_ids)
            .preserve_order(true)
            .execute()
            .await?
    };

    if payloads.len() != non_null_count {
        return Err(Error::Runtime {
            message: format!(
                "blob read for column '{column}' returned {} payloads for {} non-null rows",
                payloads.len(),
                non_null_count
            ),
        });
    }

    let mut builder = LargeBinaryBuilder::new();
    let mut payload_idx = 0;
    for is_null in &null_mask {
        if *is_null {
            builder.append_null();
        } else {
            builder.append_value(payloads[payload_idx].data.as_ref());
            payload_idx += 1;
        }
    }
    Ok(builder.finish())
}

/// Open lazy [`BlobFile`] handles for `row_ids` (same length and order, nulls as `None`).
pub(crate) async fn take_blob_files_aligned(
    dataset: &Arc<Dataset>,
    column: &str,
    row_ids: &[u64],
) -> Result<Vec<Option<BlobFile>>> {
    ensure_blob_v2_column(dataset.schema(), column)?;
    if row_ids.is_empty() {
        return Ok(Vec::new());
    }

    let null_mask = blob_null_mask(dataset, column, row_ids).await?;
    let non_null_row_ids = non_null_row_ids(row_ids, &null_mask);
    let handles = if non_null_row_ids.is_empty() {
        Vec::new()
    } else {
        dataset.take_blobs(&non_null_row_ids, column).await?
    };
    if handles.len() != non_null_row_ids.len() {
        return Err(Error::Runtime {
            message: format!(
                "blob take for column '{column}' returned {} handles for {} non-null rows",
                handles.len(),
                non_null_row_ids.len()
            ),
        });
    }

    let mut handles = handles.into_iter();
    Ok(null_mask
        .iter()
        .map(|is_null| {
            if *is_null {
                None
            } else {
                Some(handles.next().unwrap())
            }
        })
        .collect())
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
    fn legacy_v1_blob_column_is_rejected_with_migration_hint() {
        let legacy = Field::new("image", DataType::LargeBinary, true).with_metadata(
            std::collections::HashMap::from([(
                "lance-encoding:blob".to_string(),
                "true".to_string(),
            )]),
        );
        let arrow_schema = Schema::new(vec![legacy]);
        let lance_schema = lance_core::datatypes::Schema::try_from(&arrow_schema).unwrap();

        let err = ensure_blob_v2_column(&lance_schema, "image").unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
        assert!(err.to_string().contains("legacy blob column"));
        assert!(err.to_string().contains("lance.blob.v2"));
    }

    #[test]
    fn non_blob_and_unknown_columns_are_rejected_by_name() {
        let arrow_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let lance_schema = lance_core::datatypes::Schema::try_from(&arrow_schema).unwrap();

        let err = ensure_blob_v2_column(&lance_schema, "id").unwrap_err();
        assert!(err.to_string().contains("'id' is not a blob column"));

        let err = ensure_blob_v2_column(&lance_schema, "missing").unwrap_err();
        assert!(err.to_string().contains("no column named 'missing'"));
    }

    #[test]
    fn blob_column_names_includes_nested_path() {
        let blob_field = blob("blob", true);
        let info = Field::new(
            "info",
            DataType::Struct(vec![Field::new("name", DataType::Utf8, false), blob_field].into()),
            true,
        );
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false), info]);
        assert_eq!(blob_column_names(&schema), vec!["info.blob"]);
    }

    #[test]
    fn storage_version_noop_without_blob_columns() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let mut params = WriteParams::default();
        ensure_blob_storage_version(&schema, &mut params);
        assert!(params.data_storage_version.is_none());
    }
}
