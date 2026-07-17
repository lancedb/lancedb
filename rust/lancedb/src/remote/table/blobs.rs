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
use crate::remote::client::{HttpSend, RequestResultExt};

use super::RemoteTable;

impl<S: HttpSend> RemoteTable<S> {
    pub(super) async fn blob_columns_impl(&self) -> Result<Vec<String>> {
        if !self.server_version.support_blobs() {
            return Err(Error::NotSupported {
                message: "blob_columns is not supported on this LanceDB Cloud server".into(),
            });
        }
        let version = self.current_version().await;
        let mut body = serde_json::json!({ "version": version });
        self.apply_branch_body(&mut body);

        let request = self
            .post_read(&format!("/v1/table/{}/blobs/columns/", self.identifier))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        serde_json::from_str::<Vec<String>>(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse blob_columns response: {e}").into(),
            request_id,
            status_code: None,
        })
    }

    pub(super) async fn fetch_blobs_impl(
        &self,
        column: &str,
        row_ids: &[u64],
    ) -> Result<LargeBinaryArray> {
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

        let mut chunks: Vec<Arc<dyn Array>> = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            let blob_col = batch.column_by_name(column).ok_or_else(|| Error::Http {
                source: format!("fetch_blobs response is missing the '{column}' column").into(),
                request_id: request_id.clone(),
                status_code: None,
            })?;
            if !matches!(blob_col.data_type(), DataType::LargeBinary) {
                return Err(Error::Http {
                    source: format!(
                        "fetch_blobs response column has type {}, expected LargeBinary",
                        blob_col.data_type()
                    )
                    .into(),
                    request_id: request_id.clone(),
                    status_code: None,
                });
            }
            chunks.push(blob_col.clone());
        }
        let blobs = if chunks.is_empty() {
            LargeBinaryArray::from(Vec::<Option<&[u8]>>::new())
        } else {
            let chunk_refs: Vec<&dyn Array> = chunks.iter().map(AsRef::as_ref).collect();
            let blobs = arrow::compute::concat(&chunk_refs)?;
            // Each chunk was checked as LargeBinary above.
            blobs
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("concat of LargeBinary columns yields LargeBinary")
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
        Err(Error::NotSupported {
            message: "fetch_blob_files is not supported on LanceDB Cloud yet; \
                      use fetch_blobs for full bytes"
                .into(),
        })
    }
}
