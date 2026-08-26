// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Lance blob v2 columns store large binary payloads out of line.
//!
//! Declare a column with [`blob`]. On write, [`crate::table::Table::add`] coerces
//! raw `Binary` / `LargeBinary` into the blob struct layout. Queries return
//! small descriptors, not bytes.
//!
//! Blob tables require Lance file format >= 2.2 and stable row ids at create.

use std::ops::Range;
use std::sync::Arc;

use arrow_array::LargeBinaryArray;
use arrow_array::builder::LargeBinaryBuilder;
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::{BlobRangeRequest as LanceBlobRangeRequest, Dataset, WriteParams};
use lance_arrow::FieldExt;
use lance_file::version::{ConcreteFileVersion, LanceFileVersion};
use lance_io::object_store::ObjectStore;
use object_store::path::Path;

use crate::error::{Error, Result};

/// Seekable handle for one blob value, backed by local storage or a remote
/// HTTP byte-range endpoint.
#[derive(Debug)]
pub struct BlobFile {
    inner: BlobFileInner,
}

#[derive(Debug)]
enum BlobFileInner {
    Native(lance::dataset::BlobFile),
    #[cfg(feature = "remote")]
    Remote(Box<crate::remote::table::blobs::RemoteBlobFile>),
}

impl From<lance::dataset::BlobFile> for BlobFile {
    fn from(value: lance::dataset::BlobFile) -> Self {
        Self {
            inner: BlobFileInner::Native(value),
        }
    }
}

#[cfg(feature = "remote")]
impl From<crate::remote::table::blobs::RemoteBlobFile> for BlobFile {
    fn from(value: crate::remote::table::blobs::RemoteBlobFile) -> Self {
        Self {
            inner: BlobFileInner::Remote(Box::new(value)),
        }
    }
}

impl BlobFile {
    /// Inline reader over a data-file slice.
    pub fn new_inline(
        object_store: Arc<ObjectStore>,
        path: Path,
        position: u64,
        size: u64,
    ) -> Self {
        lance::dataset::BlobFile::new_inline(object_store, path, position, size).into()
    }

    /// Dedicated sidecar-file reader.
    pub fn new_dedicated(object_store: Arc<ObjectStore>, path: Path, size: u64) -> Self {
        lance::dataset::BlobFile::new_dedicated(object_store, path, size).into()
    }

    /// Packed reader for a slice in a shared sidecar.
    pub fn new_packed(
        object_store: Arc<ObjectStore>,
        path: Path,
        position: u64,
        size: u64,
    ) -> Self {
        lance::dataset::BlobFile::new_packed(object_store, path, position, size).into()
    }

    /// External reader at a resolved object location.
    pub fn new_external(
        object_store: Arc<ObjectStore>,
        path: Path,
        uri: String,
        position: u64,
        size: u64,
    ) -> Self {
        lance::dataset::BlobFile::new_external(object_store, path, uri, position, size).into()
    }

    /// Close the handle.
    pub async fn close(&self) -> lance_core::Result<()> {
        match &self.inner {
            BlobFileInner::Native(file) => file.close().await,
            #[cfg(feature = "remote")]
            BlobFileInner::Remote(file) => file.close().await,
        }
    }

    /// Whether the handle is closed.
    pub async fn is_closed(&self) -> bool {
        match &self.inner {
            BlobFileInner::Native(file) => file.is_closed().await,
            #[cfg(feature = "remote")]
            BlobFileInner::Remote(file) => file.is_closed(),
        }
    }

    /// Read a range without moving the cursor.
    pub async fn read_range(&self, range: Range<u64>) -> lance_core::Result<bytes::Bytes> {
        match &self.inner {
            BlobFileInner::Native(file) => file.read_range(range).await,
            #[cfg(feature = "remote")]
            BlobFileInner::Remote(file) => file.read_range(range).await,
        }
    }

    /// Read ranges without moving the cursor.
    pub async fn read_ranges(
        &self,
        ranges: &[Range<u64>],
    ) -> lance_core::Result<Vec<bytes::Bytes>> {
        match &self.inner {
            BlobFileInner::Native(file) => file.read_ranges(ranges).await,
            #[cfg(feature = "remote")]
            BlobFileInner::Remote(file) => file.read_ranges(ranges).await,
        }
    }

    /// Read from the cursor to the end.
    pub async fn read(&self) -> lance_core::Result<bytes::Bytes> {
        match &self.inner {
            BlobFileInner::Native(file) => file.read().await,
            #[cfg(feature = "remote")]
            BlobFileInner::Remote(file) => file.read().await,
        }
    }

    /// Read up to `len` bytes and advance the cursor.
    pub async fn read_up_to(&self, len: usize) -> lance_core::Result<bytes::Bytes> {
        match &self.inner {
            BlobFileInner::Native(file) => file.read_up_to(len).await,
            #[cfg(feature = "remote")]
            BlobFileInner::Remote(file) => file.read_up_to(len).await,
        }
    }

    /// Move the cursor to `new_cursor`.
    pub async fn seek(&self, new_cursor: u64) -> lance_core::Result<()> {
        match &self.inner {
            BlobFileInner::Native(file) => file.seek(new_cursor).await,
            #[cfg(feature = "remote")]
            BlobFileInner::Remote(file) => file.seek(new_cursor).await,
        }
    }

    /// Current cursor position.
    pub async fn tell(&self) -> lance_core::Result<u64> {
        match &self.inner {
            BlobFileInner::Native(file) => file.tell().await,
            #[cfg(feature = "remote")]
            BlobFileInner::Remote(file) => file.tell().await,
        }
    }

    /// Blob length in bytes.
    pub fn size(&self) -> u64 {
        match &self.inner {
            BlobFileInner::Native(file) => file.size(),
            #[cfg(feature = "remote")]
            BlobFileInner::Remote(file) => file.size(),
        }
    }

    /// Physical byte offset in the data file. `None` on remote handles. The
    /// Cloud byte-range route does not expose storage layout.
    pub fn position(&self) -> Option<u64> {
        match &self.inner {
            BlobFileInner::Native(file) => Some(file.position()),
            #[cfg(feature = "remote")]
            BlobFileInner::Remote(_) => None,
        }
    }

    /// Path of the data file holding the blob. `None` on remote handles. The
    /// Cloud byte-range route does not expose storage layout.
    pub fn data_path(&self) -> Option<&Path> {
        match &self.inner {
            BlobFileInner::Native(file) => Some(file.data_path()),
            #[cfg(feature = "remote")]
            BlobFileInner::Remote(_) => None,
        }
    }

    /// Native storage layout. `None` on remote handles. The Cloud byte-range
    /// route does not expose layout.
    pub fn kind(&self) -> Option<lance_core::datatypes::BlobKind> {
        match &self.inner {
            BlobFileInner::Native(file) => Some(file.kind()),
            #[cfg(feature = "remote")]
            BlobFileInner::Remote(_) => None,
        }
    }

    /// External URI for native handles. Remote handles do not expose storage URIs.
    pub fn uri(&self) -> Option<&str> {
        match &self.inner {
            BlobFileInner::Native(file) => file.uri(),
            #[cfg(feature = "remote")]
            BlobFileInner::Remote(_) => None,
        }
    }
}

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
    if matches!(
        resolved,
        ConcreteFileVersion::V1 | ConcreteFileVersion::V2_0 | ConcreteFileVersion::V2_1
    ) {
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

fn ensure_all_row_ids_resolved(column: &str, requested: usize, resolved: usize) -> Result<()> {
    if requested == resolved {
        return Ok(());
    }
    if resolved < requested {
        Err(Error::InvalidInput {
            message: format!(
                "blob read for column '{column}' requested {requested} row ids but only {resolved} \
                 exist in the table; pass row ids collected from this table"
            ),
        })
    } else {
        Err(Error::Runtime {
            message: format!(
                "blob read for column '{column}' returned {resolved} results for {requested} row ids"
            ),
        })
    }
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
    let payloads = dataset
        .read_blob_ranges(column)?
        .with_row_ids(lance_requests)
        .preserve_order(true)
        .execute()
        .await?;
    ensure_all_row_ids_resolved(column, requests.len(), payloads.len())?;

    let mut builder = LargeBinaryBuilder::new();
    for payload in payloads {
        match payload.data {
            Some(data) => builder.append_value(data),
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

    let payloads = dataset
        .read_blobs(column)?
        .with_row_ids(row_ids.to_vec())
        .preserve_order(true)
        .execute()
        .await?;
    ensure_all_row_ids_resolved(column, row_ids.len(), payloads.len())?;

    let mut builder = LargeBinaryBuilder::new();
    for payload in payloads {
        match payload.data {
            Some(data) => builder.append_value(data),
            None => builder.append_null(),
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

    let handles = dataset.take_blobs(row_ids, column).await?;
    ensure_all_row_ids_resolved(column, row_ids.len(), handles.len())?;
    Ok(handles
        .into_iter()
        .map(|handle| handle.map(Into::into))
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
            ConcreteFileVersion::V2_2
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
            ConcreteFileVersion::V2_2
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
