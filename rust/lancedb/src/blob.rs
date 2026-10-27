// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Lance blob columns store large binary payloads out of line.
//!
//! The read APIs support both legacy blob v1 and blob v2 columns.
//!
//! Declare a blob v2 column with [`blob`]. On write, [`crate::table::Table::add`] coerces
//! raw `Binary` / `LargeBinary` into the blob struct layout. Queries return
//! small descriptors, not bytes.
//!
//! Blob v2 tables require Lance file format >= 2.2. `_rowid` values stay valid
//! after compaction when the table has stable row ids. Overwrite is a new
//! create and does not keep the previous table's stable row id setting.

use std::ops::Range;
use std::sync::Arc;

use arrow_array::LargeBinaryArray;
use arrow_array::builder::LargeBinaryBuilder;
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::{BlobRangeRequest as LanceBlobRangeRequest, Dataset, WriteParams};
use lance_arrow::FieldExt;
use lance_file::version::{ConcreteFileVersion, LanceFileVersion};
use lance_io::object_store::{ObjectStore, uri_to_url};
use object_store::path::Path;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// A stored Blob V2 descriptor with the source needed to reopen its snapshot.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BlobDescriptor {
    /// Canonical Blob V2 storage kind (0 through 5).
    pub kind: u8,
    /// Stored offset within the backing object.
    pub position: u64,
    /// Stored length; zero on an external blob can mean unspecified.
    pub size: u64,
    /// Stored sidecar or base identifier.
    pub blob_id: u32,
    /// Stored external URI or managed path.
    pub blob_uri: String,
    /// Source identity captured by the query.
    pub source: BlobDescriptorSource,
}

/// The pinned source of a descriptor returned by SQL.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BlobDescriptorSource {
    /// URI of the dataset on the source branch.
    pub uri: String,
    /// Exact table version read by the query.
    pub version: u64,
    /// Commit timestamp in microseconds, distinguishing recreated tables.
    pub committed_at_micros: u64,
    /// Source branch; `None` identifies main.
    pub branch: Option<String>,
    /// Blob column path in the source table.
    pub column: String,
    /// Row ID or row address, as specified by `row_is_address`.
    pub row_reference: u64,
    /// Whether the row reference is a physical address.
    pub row_is_address: bool,
}

impl BlobDescriptor {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.kind > 5
            || self.source.version == 0
            || self.source.uri.is_empty()
            || self.source.column.is_empty()
        {
            return Err(Error::InvalidInput {
                message: "invalid stored blob descriptor or missing pinned source".into(),
            });
        }
        Ok(())
    }
}

pub(crate) async fn open_native_blob_file(
    dataset: &Dataset,
    descriptor: &BlobDescriptor,
) -> Result<BlobFile> {
    descriptor.validate()?;
    let source = &descriptor.source;
    let expected_uri = dataset
        .branch_location()
        .find_branch(source.branch.as_deref())?
        .uri;
    if uri_to_url(&expected_uri)? != uri_to_url(&source.uri)? {
        return Err(Error::InvalidInput {
            message: "blob descriptor belongs to a different table".into(),
        });
    }
    let dataset = Arc::new(
        dataset
            .checkout_version(lance::dataset::refs::Ref::Version(
                source.branch.clone(),
                Some(source.version),
            ))
            .await?,
    );
    if uri_to_url(dataset.uri())? != uri_to_url(&source.uri)? {
        return Err(Error::InvalidInput {
            message: "blob descriptor belongs to a different table".into(),
        });
    }
    if dataset.manifest().timestamp_nanos / 1_000 != u128::from(source.committed_at_micros) {
        return Err(Error::InvalidInput {
            message: "blob descriptor source snapshot has been replaced".into(),
        });
    }
    ensure_blob_v2_column(dataset.schema(), &source.column)?;
    let mut files = if source.row_is_address {
        dataset
            .take_blobs_by_addresses(&[source.row_reference], &source.column)
            .await?
    } else {
        dataset
            .take_blobs(&[source.row_reference], &source.column)
            .await?
    };
    if files.len() != 1 {
        return Err(Error::InvalidInput {
            message: "blob descriptor does not resolve to one stored value".into(),
        });
    }
    files
        .pop()
        .flatten()
        .map(Into::into)
        .ok_or_else(|| Error::InvalidInput {
            message: "non-null blob descriptor resolved to a null value".into(),
        })
}

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
/// `Struct<data, uri, position, size>` with nullable children and the
/// `lance.blob.v2` marker. Set both range fields to read a byte range from a
/// URI, or leave both null for a whole file. Inline bytes are also supported.
///
/// # Migration
///
/// This helper previously returned only `data` and `uri`. Rust callers building
/// a `StructArray` from this field must now supply four children: append typed
/// null UInt64 arrays for `position` and `size` when no range is needed:
///
/// ```
/// use std::sync::Arc;
/// use arrow_array::{ArrayRef, LargeBinaryArray, StringArray, StructArray, UInt64Array};
/// use arrow_schema::DataType;
/// let field = lancedb::blob("image", true);
/// let DataType::Struct(children) = field.data_type() else { unreachable!() };
/// let image = StructArray::new(children.clone(), vec![
///     Arc::new(LargeBinaryArray::from(vec![Some(b"image".as_slice())])) as ArrayRef,
///     Arc::new(StringArray::from(vec![None::<&str>])),
///     Arc::new(UInt64Array::new_null(1)),
///     Arc::new(UInt64Array::new_null(1)),
/// ], None);
/// ```
///
/// Existing two-field tables remain writable: append and merge-insert fill
/// missing range fields or drop all-null range fields to match the table.
/// Non-null ranges cannot be written to a two-field table. Overwrite replaces
/// the table schema with the input schema rather than normalizing its layout.
///
/// A blob column may be top-level or nested inside a struct or list. Blobs
/// nested inside structs use dotted paths (e.g. `info.blob`) in the read APIs.
/// Blobs inside lists can be written and queried as descriptors, but cannot
/// currently be fetched by row id.
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
    blob_with_options(name, nullable, BlobOptions::default())
}

static BLOB_STORAGE_TYPE: std::sync::LazyLock<DataType> = std::sync::LazyLock::new(|| {
    DataType::Struct(
        vec![
            Field::new("data", DataType::LargeBinary, true),
            Field::new("uri", DataType::Utf8, true),
            Field::new("position", DataType::UInt64, true),
            Field::new("size", DataType::UInt64, true),
        ]
        .into(),
    )
});

/// Options for creating a blob v2 column field with tier thresholds.
///
/// Re-exported from lance so the thresholds are built exactly the way
/// [`lance::blob::blob_field_with_options`] builds them. Note that lance does
/// not currently expose a pack file size threshold option, so only the inline
/// and dedicated size thresholds are supported here.
pub use lance::blob::BlobFieldOptions as BlobOptions;

/// Creates an Arrow field for a Lance blob v2 column with tier thresholds.
///
/// Same as [`blob`], but forwards `options` to
/// [`lance::blob::blob_field_with_options`], storing the inline and dedicated
/// size thresholds as field metadata. With [`BlobOptions::default()`] this is
/// identical to [`blob`].
///
/// ```
/// use std::num::NonZeroUsize;
///
/// use lancedb::blob::BlobOptions;
///
/// let options = BlobOptions {
///     inline_size_threshold: Some(8 * 1024),
///     dedicated_size_threshold: NonZeroUsize::new(1 << 20),
/// };
/// let field = lancedb::blob::blob_with_options("payload", true, options);
/// assert!(lancedb::blob::is_blob(&field));
/// ```
pub fn blob_with_options(name: impl AsRef<str>, nullable: bool, options: BlobOptions) -> Field {
    lance::blob::blob_field_with_options(name.as_ref(), nullable, options)
        .with_data_type(BLOB_STORAGE_TYPE.clone())
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
pub(crate) fn field_tree_has_blob_v2(field: &Field) -> bool {
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

fn field_tree_has_blob(field: &Field) -> bool {
    if field.is_blob() {
        return true;
    }
    match field.data_type() {
        DataType::Struct(children) => children.iter().any(|c| field_tree_has_blob(c)),
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            field_tree_has_blob(child)
        }
        _ => false,
    }
}

/// Collects the dotted paths of row-addressable blob columns under `field`.
fn collect_blob_paths(field: &Field, prefix: &str, paths: &mut Vec<String>) {
    let path = if prefix.is_empty() {
        field.name().clone()
    } else {
        format!("{prefix}.{}", field.name())
    };
    if field.is_blob() {
        paths.push(path);
        return;
    }
    if let DataType::Struct(children) = field.data_type() {
        for child in children {
            collect_blob_paths(child, &path, paths);
        }
    }
}

fn is_list_blob_path(field: &Field, prefix: &str, column: &str, inside_list: bool) -> bool {
    let path = if prefix.is_empty() {
        field.name().clone()
    } else {
        format!("{prefix}.{}", field.name())
    };
    if field.is_blob() {
        return inside_list && column == path;
    }
    match field.data_type() {
        DataType::Struct(children) => children
            .iter()
            .any(|child| is_list_blob_path(child, &path, column, inside_list)),
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            (column == path && field_tree_has_blob(child))
                || is_list_blob_path(child, &path, column, true)
        }
        _ => false,
    }
}

/// Give list blob paths a useful error before they reach the row-based reader.
pub(crate) fn reject_list_blob_column(schema: &Schema, column: &str) -> Result<()> {
    if schema
        .fields()
        .iter()
        .any(|field| is_list_blob_path(field, "", column, false))
    {
        return Err(Error::InvalidInput {
            message: format!(
                "column '{column}' contains a blob inside a list; blobs inside lists cannot be fetched by row id yet"
            ),
        });
    }
    Ok(())
}

/// Returns true if `schema` declares any blob v2 column, including nested ones.
pub(crate) fn has_blob_columns(schema: &Schema) -> bool {
    schema.fields().iter().any(|f| field_tree_has_blob_v2(f))
}

/// Row-addressable blob v1 and v2 column paths in `schema`, in declaration order.
/// Struct-nested blobs use dotted paths (e.g. `info.blob`); list-nested blobs
/// are omitted because a row id cannot identify an individual list element.
pub(crate) fn blob_column_names(schema: &Schema) -> Vec<String> {
    let mut paths = Vec::new();
    for field in schema.fields() {
        collect_blob_paths(field, "", &mut paths);
    }
    paths
}

/// Bumps storage format to at least [`LanceFileVersion::V2_2`] for blob schemas.
/// Leaves `enable_stable_row_ids` unchanged.
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

/// Validate that `column` exists and is a row-addressable blob v1 or v2 column.
pub(crate) fn ensure_blob_column(
    schema: &lance_core::datatypes::Schema,
    column: &str,
) -> Result<()> {
    // A top-level blob cannot have a list ancestor. Keep the common read path
    // from converting the entire schema just to validate that condition.
    if column.contains('.')
        || schema
            .field(column)
            .is_none_or(|field| !field.is_blob() && !field.is_blob_v2())
    {
        reject_list_blob_column(&Schema::from(schema), column)?;
    }
    match schema.field(column) {
        Some(field) if field.is_blob() || field.is_blob_v2() => Ok(()),
        Some(_) => Err(Error::InvalidInput {
            message: format!("column '{column}' is not a blob column"),
        }),
        None => Err(Error::InvalidInput {
            message: format!("no column named '{column}' in this table"),
        }),
    }
}

/// Validate a blob v2 descriptor source column.
///
/// Stored SQL descriptors only describe blob v2 layouts. Keep legacy v1
/// support on the row-id fetch APIs instead of broadening descriptor
/// reopening to a storage format it cannot encode.
fn ensure_blob_v2_column(schema: &lance_core::datatypes::Schema, column: &str) -> Result<()> {
    if column.contains('.') || schema.field(column).is_none_or(|field| !field.is_blob_v2()) {
        reject_list_blob_column(&Schema::from(schema), column)?;
    }
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

fn missing_blob_row_ids_error(column: &str, requested: usize, missing: &[u64]) -> Error {
    let detail = if missing.is_empty() {
        String::new()
    } else {
        format!("; first missing row ids: {missing:?}")
    };
    Error::InvalidInput {
        message: format!(
            "blob read for column '{column}' requested {requested} row ids but some \
             do not exist in the table{detail}; pass row ids collected from this table"
        ),
    }
}

/// Diagnose only after a failed read. Stable row IDs use Lance's deletion-aware
/// index; physical row IDs are fragment/offset pairs. Limit the error to three
/// distinct IDs so a large failed batch stays readable.
async fn find_missing_blob_row_ids(dataset: &Dataset, row_ids: &[u64]) -> Result<Vec<u64>> {
    let mut missing = Vec::new();
    if let Some(index) = lance::dataset::rowids::get_row_id_index(dataset).await? {
        for (row_id, address) in row_ids.iter().zip(index.get_many(row_ids)?) {
            if address.is_none() && !missing.contains(row_id) {
                missing.push(*row_id);
                if missing.len() == 3 {
                    break;
                }
            }
        }
    } else {
        let mut fragments = std::collections::HashMap::new();
        for &row_id in row_ids {
            let fragment_id = row_id >> 32;
            let offset = row_id as u32;
            if let std::collections::hash_map::Entry::Vacant(entry) = fragments.entry(fragment_id) {
                let fragment = dataset.get_fragment(fragment_id as usize);
                let state = if let Some(fragment) = fragment {
                    Some((
                        fragment.physical_rows().await?,
                        fragment.get_deletion_vector().await?,
                    ))
                } else {
                    None
                };
                entry.insert(state);
            }
            let is_missing = match fragments.get(&fragment_id) {
                Some(Some((physical_rows, deletions))) => {
                    offset as usize >= *physical_rows
                        || deletions
                            .as_ref()
                            .is_some_and(|deletions| deletions.contains(offset))
                }
                Some(None) => true,
                None => unreachable!(),
            };
            if is_missing && !missing.contains(&row_id) {
                missing.push(row_id);
                if missing.len() == 3 {
                    break;
                }
            }
        }
    }
    Ok(missing)
}

async fn ensure_all_row_ids_resolved(
    dataset: &Dataset,
    column: &str,
    row_ids: &[u64],
    resolved: usize,
) -> Result<()> {
    if row_ids.len() == resolved {
        return Ok(());
    }
    if resolved < row_ids.len() {
        let missing = find_missing_blob_row_ids(dataset, row_ids)
            .await
            .unwrap_or_default();
        Err(missing_blob_row_ids_error(column, row_ids.len(), &missing))
    } else {
        Err(Error::Runtime {
            message: format!(
                "blob read for column '{column}' returned {resolved} results for {} row ids",
                row_ids.len()
            ),
        })
    }
}

/// Lance reports missing physical and stable row ids through several error paths
/// without a shared typed error.
async fn map_blob_take_error(
    dataset: &Dataset,
    column: &str,
    row_ids: &[u64],
    err: lance::Error,
) -> Error {
    let message = err.to_string();
    let missing_row_id = message.contains("must not target deleted rows")
        || message.contains("belongs to non-existent fragment")
        || (message.contains("Unknown fragment id ")
            && message.contains("in fragment filter; not part of the current dataset version"))
        || (message.contains("Invalid read params ") && message.contains("addressable rows"))
        || message.contains("Could not resolve all requested row IDs");

    if missing_row_id {
        let missing = find_missing_blob_row_ids(dataset, row_ids)
            .await
            .unwrap_or_default();
        missing_blob_row_ids_error(column, row_ids.len(), &missing)
    } else {
        err.into()
    }
}

/// Materialize blob-local ranges (same length and order as `requests`, nulls preserved).
pub(crate) async fn take_blob_ranges_aligned(
    dataset: &Arc<Dataset>,
    column: &str,
    requests: &[BlobRangeRequest],
) -> Result<LargeBinaryArray> {
    ensure_blob_column(dataset.schema(), column)?;
    if requests.is_empty() {
        return Ok(LargeBinaryBuilder::new().finish());
    }

    let row_ids = requests
        .iter()
        .map(|request| request.row_id)
        .collect::<Vec<_>>();
    let lance_requests = requests
        .iter()
        .map(|request| LanceBlobRangeRequest::new(request.row_id, request.offset, request.length))
        .collect::<Vec<_>>();
    let payloads = match dataset
        .read_blob_ranges(column)?
        .with_row_ids(lance_requests)
        .preserve_order(true)
        .execute()
        .await
    {
        Ok(payloads) => payloads,
        Err(err) => return Err(map_blob_take_error(dataset, column, &row_ids, err).await),
    };
    ensure_all_row_ids_resolved(dataset, column, &row_ids, payloads.len()).await?;

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
    ensure_blob_column(dataset.schema(), column)?;
    if row_ids.is_empty() {
        return Ok(LargeBinaryBuilder::new().finish());
    }

    let payloads = match dataset
        .read_blobs(column)?
        .with_row_ids(row_ids.to_vec())
        .preserve_order(true)
        .execute()
        .await
    {
        Ok(payloads) => payloads,
        Err(err) => return Err(map_blob_take_error(dataset, column, row_ids, err).await),
    };
    ensure_all_row_ids_resolved(dataset, column, row_ids, payloads.len()).await?;

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
    ensure_blob_column(dataset.schema(), column)?;
    if row_ids.is_empty() {
        return Ok(Vec::new());
    }

    let handles = match dataset.take_blobs(row_ids, column).await {
        Ok(handles) => handles,
        Err(err) => return Err(map_blob_take_error(dataset, column, row_ids, err).await),
    };
    ensure_all_row_ids_resolved(dataset, column, row_ids, handles.len()).await?;
    Ok(handles
        .into_iter()
        .map(|handle| handle.map(Into::into))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    use arrow_schema::DataType;
    use lance_arrow::{
        ARROW_EXT_NAME_KEY, BLOB_DEDICATED_SIZE_THRESHOLD_META_KEY,
        BLOB_INLINE_SIZE_THRESHOLD_META_KEY,
    };

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
        let DataType::Struct(children) = field.data_type() else {
            panic!("expected struct")
        };
        assert_eq!(
            children
                .iter()
                .map(|f| (f.name().as_str(), f.data_type(), f.is_nullable()))
                .collect::<Vec<_>>(),
            vec![
                ("data", &DataType::LargeBinary, true),
                ("uri", &DataType::Utf8, true),
                ("position", &DataType::UInt64, true),
                ("size", &DataType::UInt64, true),
            ]
        );
    }

    #[test]
    fn blob_with_options_sets_threshold_metadata() {
        let options = BlobOptions {
            inline_size_threshold: Some(8 * 1024),
            dedicated_size_threshold: NonZeroUsize::new(1 << 20),
        };
        let field = blob_with_options("payload", true, options);
        assert!(is_blob(&field));
        assert_eq!(
            field
                .metadata()
                .get(BLOB_INLINE_SIZE_THRESHOLD_META_KEY)
                .map(String::as_str),
            Some("8192")
        );
        assert_eq!(
            field
                .metadata()
                .get(BLOB_DEDICATED_SIZE_THRESHOLD_META_KEY)
                .map(String::as_str),
            Some("1048576")
        );
    }

    #[test]
    fn blob_with_default_options_matches_plain_blob() {
        let field = blob_with_options("payload", true, BlobOptions::default());
        let plain = blob("payload", true);
        assert_eq!(field.metadata(), plain.metadata());
        assert_eq!(field.data_type(), plain.data_type());
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
        let resolved = params
            .data_storage_version
            .unwrap_or(LanceFileVersion::Stable)
            .resolve();
        assert_eq!(resolved, ConcreteFileVersion::V2_2);
        assert!(!params.enable_stable_row_ids);
    }

    #[test]
    fn storage_version_leaves_stable_row_ids_enabled() {
        let mut params = WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        };
        ensure_blob_storage_version(&blob_schema(), &mut params);
        assert!(params.enable_stable_row_ids);
        let resolved = params
            .data_storage_version
            .unwrap_or(LanceFileVersion::Stable)
            .resolve();
        assert_eq!(resolved, ConcreteFileVersion::V2_2);
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
    fn legacy_v1_blob_columns_are_row_addressable() {
        let legacy = Field::new("image", DataType::LargeBinary, true).with_metadata(
            std::collections::HashMap::from([(
                "lance-encoding:blob".to_string(),
                "true".to_string(),
            )]),
        );
        let arrow_schema = Schema::new(vec![
            legacy.clone(),
            Field::new("info", DataType::Struct(vec![legacy.clone()].into()), true),
            Field::new("images", DataType::List(Arc::new(legacy)), true),
        ]);
        let lance_schema = lance_core::datatypes::Schema::try_from(&arrow_schema).unwrap();

        ensure_blob_column(&lance_schema, "image").unwrap();
        ensure_blob_column(&lance_schema, "info.image").unwrap();
        assert_eq!(
            blob_column_names(&arrow_schema),
            vec!["image", "info.image"]
        );
        for path in ["images", "images.image"] {
            let err = ensure_blob_column(&lance_schema, path).unwrap_err();
            assert!(
                err.to_string()
                    .contains("blobs inside lists cannot be fetched")
            );
        }
        // Legacy columns do not require the blob v2 write format.
        assert!(!has_blob_columns(&arrow_schema));
    }

    #[test]
    fn non_blob_and_unknown_columns_are_rejected_by_name() {
        let arrow_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let lance_schema = lance_core::datatypes::Schema::try_from(&arrow_schema).unwrap();

        let err = ensure_blob_column(&lance_schema, "id").unwrap_err();
        assert!(err.to_string().contains("'id' is not a blob column"));

        let err = ensure_blob_column(&lance_schema, "missing").unwrap_err();
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
    fn list_blob_paths_are_not_row_addressable() {
        let schema = Schema::new(vec![
            blob("image", true),
            Field::new(
                "info",
                DataType::Struct(vec![blob("thumbnail", true)].into()),
                true,
            ),
            Field::new("images", DataType::List(Arc::new(blob("item", true))), true),
            Field::new(
                "groups",
                DataType::LargeList(Arc::new(Field::new(
                    "group",
                    DataType::Struct(vec![blob("photo", true)].into()),
                    true,
                ))),
                true,
            ),
            Field::new(
                "frames",
                DataType::FixedSizeList(Arc::new(blob("frame", true)), 2),
                true,
            ),
        ]);
        assert!(has_blob_columns(&schema));
        assert_eq!(blob_column_names(&schema), vec!["image", "info.thumbnail"]);

        for column in [
            "images",
            "images.item",
            "groups",
            "groups.group.photo",
            "frames",
            "frames.frame",
        ] {
            let err = reject_list_blob_column(&schema, column).unwrap_err();
            assert!(matches!(err, Error::InvalidInput { .. }));
            assert!(
                err.to_string()
                    .contains("blobs inside lists cannot be fetched")
            );
        }
        for column in ["image", "info.thumbnail", "info"] {
            reject_list_blob_column(&schema, column).unwrap();
        }
    }

    #[test]
    fn storage_version_noop_without_blob_columns() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let mut params = WriteParams::default();
        ensure_blob_storage_version(&schema, &mut params);
        assert!(params.data_storage_version.is_none());
        assert!(!params.enable_stable_row_ids);
    }
}
