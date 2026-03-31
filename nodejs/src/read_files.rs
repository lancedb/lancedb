// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use lancedb::data::scannable::Scannable;
use lancedb::ipc::schema_to_ipc_file;
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;

use crate::error::NapiErrorExt;

/// A data source backed by files on disk or object storage.
///
/// Created by [`readFiles`] and accepted by [`Table.add`].
///
/// Use [`readFiles`] from the TypeScript layer instead of calling this directly;
/// it wraps the schema buffer as an Arrow Schema.
#[napi]
pub struct NativeFileSource {
    pub(crate) pattern: String,
    /// Schema cached as Arrow IPC bytes, computed once during [`read_files`].
    schema_ipc: Vec<u8>,
}

#[napi]
impl NativeFileSource {
    /// Returns the schema as an Arrow IPC buffer.
    #[napi]
    pub fn schema(&self) -> Buffer {
        self.schema_ipc.clone().into()
    }
}

/// Create a [`NativeFileSource`] from a glob pattern or single path.
///
/// Supported formats: `.parquet`, `.csv`, `.lance`
///
/// Schema is inferred from the files eagerly; data is read lazily when
/// the source is passed to `Table.add`.
///
/// Use [`readFiles`] from the TypeScript layer instead of calling this directly;
/// it wraps the result in a `FileSource` with a typed `.schema` property.
#[napi(catch_unwind)]
pub async fn read_files(pattern: String) -> napi::Result<NativeFileSource> {
    let source = lancedb::read_files(&pattern).await.default_error()?;
    let schema_ipc = schema_to_ipc_file(&source.schema()).default_error()?;
    Ok(NativeFileSource {
        pattern,
        schema_ipc,
    })
}
