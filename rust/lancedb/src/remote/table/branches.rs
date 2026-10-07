// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

impl<S: HttpSend> RemoteTable<S> {
    pub(super) async fn create_branch_impl(
        &self,
        name: &str,
        from: lance::dataset::refs::Ref,
    ) -> Result<Arc<dyn BaseTable>> {
        use lance::dataset::refs::Ref;

        if name.trim().is_empty() {
            return Err(Error::InvalidInput {
                message: "branch name must be a non-empty string".into(),
            });
        }

        // Translate the source ref into the `from_branch` / `from_version` the
        // `/branches/create` contract accepts (it has no `from_tag`).
        let (from_branch, from_version) = match from {
            Ref::Version(branch, version) => (normalize_branch(branch), version),
            Ref::VersionNumber(version) => (normalize_branch(self.branch.clone()), Some(version)),
            Ref::Tag(tag) => {
                let (branch, version) = self.resolve_tag_ref(&tag).await?;
                (branch, Some(version))
            }
        };

        let mut body = serde_json::json!({ "name": name });
        if let Some(from_branch) = &from_branch {
            body["from_branch"] = serde_json::Value::String(from_branch.clone());
        }
        if let Some(from_version) = from_version {
            body["from_version"] = serde_json::json!(from_version);
        }

        let request = self
            .client
            .post(&format!("/v1/table/{}/branches/create/", self.identifier))
            .json(&body);

        // Send without retry so the expected 409 (branch already exists) is
        // surfaced as a response we can map, rather than being retried.
        let (request_id, response) = self.send_unfenced(request, false).await?;
        match response.status() {
            StatusCode::CONFLICT => {
                return Err(Error::TableAlreadyExists {
                    name: format!("{} (branch: {})", self.name, name),
                });
            }
            StatusCode::BAD_REQUEST => {
                let body = response.text().await.unwrap_or_default();
                return Err(Error::InvalidInput {
                    message: format!("invalid create_branch request: {}", body),
                });
            }
            StatusCode::NOT_FOUND => {
                // 404 covers both a missing table and a missing source ref; name
                // the source coordinate so the error isn't misattributed to the table.
                let body = response.text().await.unwrap_or_default();
                let source_desc = match (&from_branch, from_version) {
                    (Some(b), Some(v)) => format!(" (source: branch '{b}' version {v})"),
                    (Some(b), None) => format!(" (source: branch '{b}')"),
                    (None, Some(v)) => format!(" (source: version {v})"),
                    (None, None) => String::new(),
                };
                return Err(Error::TableNotFound {
                    name: format!("{}{}", self.name, source_desc),
                    source: Box::new(Error::Http {
                        source: body.into(),
                        request_id,
                        status_code: Some(StatusCode::NOT_FOUND),
                    }),
                });
            }
            _ => {}
        }
        self.check_table_response(&request_id, response).await?;

        Ok(Arc::new(self.with_branch(Some(name.to_string()))))
    }

    pub(super) async fn list_branches_impl(
        &self,
    ) -> Result<HashMap<String, lance::dataset::refs::BranchContents>> {
        use lance::dataset::refs::BranchContents;

        let request = self
            .client
            .post(&format!("/v1/table/{}/branches/list/", self.identifier));
        let (request_id, response) = self.send_unfenced(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        #[derive(Deserialize)]
        struct ListBranchesResponse {
            branches: HashMap<String, BranchContents>,
        }

        let parsed: ListBranchesResponse =
            serde_json::from_str(&body).map_err(|err| Error::Http {
                source: format!(
                    "Failed to parse list_branches response: {}, body: {}",
                    err, body
                )
                .into(),
                request_id,
                status_code: None,
            })?;

        Ok(parsed.branches)
    }

    pub(super) async fn delete_branch_impl(&self, name: &str) -> Result<()> {
        if name.trim().is_empty() {
            return Err(Error::InvalidInput {
                message: "branch name must be a non-empty string".into(),
            });
        }
        let request = self
            .client
            .post(&format!("/v1/table/{}/branches/delete/", self.identifier))
            .json(&serde_json::json!({ "name": name }));
        let (request_id, response) = self.send(request, true).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::TableNotFound {
                name: format!("{} (branch: {})", self.name, name),
                source: format!("branch '{}' does not exist", name).into(),
            });
        }
        self.check_table_response(&request_id, response).await?;
        Ok(())
    }

    pub(super) async fn diff_branch_impl(&self, from_branch: &str) -> Result<BranchDiff> {
        if from_branch.trim().is_empty() {
            return Err(Error::InvalidInput {
                message: "Branch name cannot be empty.".into(),
            });
        }
        let request = self
            .client
            .post(&format!("/v1/table/{}/branches/diff/", self.identifier))
            .json(&serde_json::json!({ "from_branch": from_branch }));
        let (request_id, response) = self.send_unfenced(request, true).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::TableNotFound {
                name: format!("{} (branch: {})", self.name, from_branch),
                source: format!("branch '{}' does not exist", from_branch).into(),
            });
        }
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        serde_json::from_str(&body).map_err(|err| Error::Http {
            source: format!(
                "Failed to parse diff_branch response: {}, body: {}",
                err, body
            )
            .into(),
            request_id,
            status_code: None,
        })
    }

    pub(super) async fn cherry_pick_impl(
        &self,
        from_branch: &str,
        dry_run: bool,
    ) -> Result<CherryPickResult> {
        if from_branch.trim().is_empty() {
            return Err(Error::InvalidInput {
                message: "Branch name cannot be empty.".into(),
            });
        }
        let read_snapshot = self.snapshot_read_state().await;
        let target_freshness = if self.branch.is_none() && read_snapshot.version.is_none() {
            Some(read_snapshot.freshness)
        } else {
            None
        };
        let request = self
            .client
            .post(&format!(
                "/v1/table/{}/branches/cherry_pick/",
                self.identifier
            ))
            .json(&serde_json::json!({
                "from_branch": from_branch,
                "dry_run": dry_run,
            }));
        // No retry. HTTP 409 is CherryPickStatus::Failed with a body, not a transport error.
        let (request_id, response) = self.send_unfenced(request, false).await?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Err(Error::TableNotFound {
                name: format!("{} (branch: {})", self.name, from_branch),
                source: format!("branch '{}' does not exist", from_branch).into(),
            });
        }
        // 200 and 409 both carry CherryPickResult.
        if status != StatusCode::OK && status != StatusCode::CONFLICT {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Http {
                source: format!("unexpected status {status} from cherry_pick: {body}").into(),
                request_id,
                status_code: Some(status),
            });
        }
        let body = response.text().await.err_to_http(request_id.clone())?;
        let result: CherryPickResult = serde_json::from_str(&body).map_err(|err| Error::Http {
            source: format!(
                "Failed to parse cherry_pick response: {}, body: {}",
                err, body
            )
            .into(),
            request_id,
            status_code: Some(status),
        })?;
        if !dry_run
            && status == StatusCode::OK
            && result.status == crate::table::CherryPickStatus::CherryPicked
            && let (Some(freshness), Some(version)) = (target_freshness, result.main_version_after)
        {
            freshness.observe_version(&self.freshness, version);
        }
        Ok(result)
    }
}
