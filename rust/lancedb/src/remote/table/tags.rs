// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

pub struct RemoteTags<'a, S: HttpSend = Sender> {
    pub(super) inner: &'a RemoteTable<S>,
}

#[async_trait]
impl<S: HttpSend + 'static> Tags for RemoteTags<'_, S> {
    async fn list(&self) -> Result<HashMap<String, TagContents>> {
        let request = self
            .inner
            .client
            .post(&format!("/v1/table/{}/tags/list/", self.inner.identifier));
        let (request_id, response) = self.inner.send_unfenced(request, true).await?;
        let response = self
            .inner
            .check_table_response(&request_id, response)
            .await?;

        match response.text().await {
            Ok(body) => {
                // Explicitly tell serde_json what type we want to deserialize into
                let tags_map: HashMap<String, TagContents> =
                    serde_json::from_str(&body).map_err(|e| Error::Http {
                        source: format!("Failed to parse tags list: {}", e).into(),
                        request_id,
                        status_code: None,
                    })?;

                Ok(tags_map)
            }
            Err(err) => {
                let status_code = err.status();
                Err(Error::Http {
                    source: Box::new(err),
                    request_id,
                    status_code,
                })
            }
        }
    }

    async fn get_version(&self, tag: &str) -> Result<u64> {
        let request = self.inner.client.post(&format!(
            "/v1/table/{}/tags/version/",
            self.inner.identifier
        ));
        self.inner
            .resolve_tag_version_with_request(tag, request, false)
            .await
    }

    async fn create(&mut self, tag: &str, version: u64) -> Result<()> {
        let mut body = serde_json::json!({
            "tag": tag,
            "version": version
        });
        self.inner.apply_branch_body(&mut body);
        let request = self
            .inner
            .client
            .post(&format!("/v1/table/{}/tags/create/", self.inner.identifier))
            .json(&body);

        let (request_id, response) = self.inner.send(request, true).await?;
        self.inner
            .check_table_response(&request_id, response)
            .await?;
        Ok(())
    }

    async fn delete(&mut self, tag: &str) -> Result<()> {
        let request = self
            .inner
            .client
            .post(&format!("/v1/table/{}/tags/delete/", self.inner.identifier))
            .json(&serde_json::json!({ "tag": tag }));

        let (request_id, response) = self.inner.send_unfenced(request, true).await?;
        self.inner
            .check_table_response(&request_id, response)
            .await?;
        Ok(())
    }

    async fn update(&mut self, tag: &str, version: u64) -> Result<()> {
        let mut body = serde_json::json!({
            "tag": tag,
            "version": version
        });
        self.inner.apply_branch_body(&mut body);
        let request = self
            .inner
            .client
            .post(&format!("/v1/table/{}/tags/update/", self.inner.identifier))
            .json(&body);

        let (request_id, response) = self.inner.send(request, true).await?;
        self.inner
            .check_table_response(&request_id, response)
            .await?;
        Ok(())
    }
}
