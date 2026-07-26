// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Cloud blob column listing and whole-byte fetch.

use std::sync::Arc;

use arrow_array::{Array, LargeBinaryArray};
use arrow_schema::DataType;
use futures::TryStreamExt;

use crate::Error;
use crate::blob::BlobFile;
use crate::error::Result;
use crate::remote::client::HttpSend;
use crate::table::BaseTable;

use super::RemoteTable;

impl<S: HttpSend> RemoteTable<S> {
    /// Blob v2 columns are marked in field metadata, which `describe` returns. Reading
    /// them from the cached schema needs no route of its own and no version gate.
    pub(super) async fn blob_columns_impl(&self) -> Result<Vec<String>> {
        let schema = self.schema().await?;
        Ok(crate::blob::blob_column_names(schema.as_ref()))
    }

    pub(super) async fn fetch_blobs_impl(
        &self,
        column: &str,
        row_ids: &[u64],
    ) -> Result<LargeBinaryArray> {
        // An empty selection already has its answer, so skip the round trip and the
        // server requirement entirely. Local fetch_blobs returns early the same way.
        if row_ids.is_empty() {
            return Ok(LargeBinaryArray::from(Vec::<Option<&[u8]>>::new()));
        }
        if !self.server_version.support_blobs() {
            return Err(Error::NotSupported {
                message: "fetch_blobs is not supported on this LanceDB Cloud server".into(),
            });
        }
        let version = self.current_version().await;
        let mut body = serde_json::json!({
            "version": version,
            "column": column,
            "row_ids": row_ids,
        });
        self.apply_branch_body(&mut body);

        let request = self
            .post_read(&format!("/v1/table/{}/fetch_blobs/", self.identifier))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        let mut stream = self.read_arrow_stream(&request_id, response).await?;

        let mut blob_chunks: Vec<Arc<dyn Array>> = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            let blob_column = batch.column_by_name(column).ok_or_else(|| Error::Http {
                source: format!("fetch_blobs response is missing the '{column}' column").into(),
                request_id: request_id.clone(),
                status_code: None,
            })?;
            // The server returns LargeBinary today. Accept the other binary types so a
            // server that switches encodings does not break older clients.
            if !matches!(
                blob_column.data_type(),
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            ) {
                return Err(Error::Http {
                    source: format!(
                        "fetch_blobs response column has type {}, expected Binary, LargeBinary, or BinaryView",
                        blob_column.data_type()
                    )
                    .into(),
                    request_id: request_id.clone(),
                    status_code: None,
                });
            }
            blob_chunks.push(arrow::compute::cast(blob_column, &DataType::LargeBinary)?);
        }
        // A server that sends no batches for a non-empty selection is caught by the
        // length check below, which names both counts.
        let blobs = if blob_chunks.is_empty() {
            LargeBinaryArray::from(Vec::<Option<&[u8]>>::new())
        } else {
            let blob_chunk_refs: Vec<&dyn Array> = blob_chunks.iter().map(AsRef::as_ref).collect();
            arrow::compute::concat(&blob_chunk_refs)?
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| Error::Http {
                    source: "fetch_blobs could not read the concatenated response as LargeBinary"
                        .into(),
                    request_id: request_id.clone(),
                    status_code: None,
                })?
                .clone()
        };
        // Same length/order contract as local fetch_blobs.
        if blobs.len() != row_ids.len() {
            return Err(Error::Http {
                source: format!(
                    "fetch_blobs returned {} rows for {} row ids",
                    blobs.len(),
                    row_ids.len()
                )
                .into(),
                request_id,
                status_code: None,
            });
        }
        Ok(blobs)
    }

    pub(super) async fn fetch_blob_files_impl(
        &self,
        _column: &str,
        _row_ids: &[u64],
    ) -> Result<Vec<Option<BlobFile>>> {
        // Only point at fetch_blobs when this server can actually serve it.
        let message = if self.server_version.support_blobs() {
            "fetch_blob_files is not supported on LanceDB Cloud yet. \
             Use fetch_blobs for full bytes"
        } else {
            "fetch_blob_files is not supported on LanceDB Cloud"
        };
        Err(Error::NotSupported {
            message: message.into(),
        })
    }
}
