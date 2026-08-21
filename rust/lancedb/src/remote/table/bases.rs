// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Cloud HTTP for registering and listing extra table storage bases.

use serde::Deserialize;

use crate::Error;
use crate::error::Result;
use crate::remote::client::{HttpSend, RequestResultExt};

use super::RemoteTable;

#[derive(Debug, Deserialize)]
struct AddBasesResponse {
    version: u64,
}

#[derive(Debug, Deserialize)]
struct ListBasesResponse {
    bases: Vec<crate::table::TableBase>,
}

impl<S: HttpSend> RemoteTable<S> {
    pub(super) async fn add_bases_impl(&self, bases: &[crate::table::TableBase]) -> Result<()> {
        self.check_mutable().await?;
        let mut body = serde_json::json!({ "bases": bases });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!("/v1/table/{}/bases/", self.identifier))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let parsed: AddBasesResponse = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!(
                "The server returned an invalid response while registering table bases: {e}"
            )
            .into(),
            request_id,
            status_code: None,
        })?;
        self.track_write_version(parsed.version);
        Ok(())
    }

    pub(super) async fn list_bases_impl(&self) -> Result<Vec<crate::table::TableBase>> {
        let version = self.current_version().await;
        let mut body = serde_json::json!({ "version": version });
        self.apply_branch_body(&mut body);
        let request = self
            .post_read(&format!("/v1/table/{}/bases/list/", self.identifier))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let parsed: ListBasesResponse = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!(
                "The server returned an invalid response while listing table bases: {e}"
            )
            .into(),
            request_id,
            status_code: None,
        })?;
        Ok(parsed.bases)
    }
}
