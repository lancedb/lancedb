// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

pub(super) struct AzureImdsSource {
    client_id: Option<String>,
    resource: String,
    http_client: Client,
}

impl std::fmt::Debug for AzureImdsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureImdsSource")
            .field("client_id", &self.client_id)
            .field("resource", &self.resource)
            .finish()
    }
}

impl AzureImdsSource {
    pub(super) fn new(scopes: Vec<String>, client_id: Option<String>) -> Result<Self> {
        let resource = Self::resource_from_scopes(&scopes)?;
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .no_proxy()
            .build()
            .map_err(|e| Error::Runtime {
                message: format!("Failed to create HTTP client for Azure IMDS OAuth: {e}"),
            })?;

        Ok(Self {
            client_id,
            resource,
            http_client,
        })
    }

    pub(super) fn resource_from_scopes(scopes: &[String]) -> Result<String> {
        let [scope] = scopes else {
            return Err(Error::InvalidInput {
                message: "AzureManagedIdentity flow requires exactly one OAuth scope or resource"
                    .to_string(),
            });
        };

        Ok(scope.strip_suffix("/.default").unwrap_or(scope).to_string())
    }
}

#[async_trait]
impl TokenSource for AzureImdsSource {
    async fn fetch_token(&self) -> Result<TokenResponse> {
        let mut url = format!(
            "{AZURE_IMDS_ENDPOINT}?api-version={AZURE_IMDS_API_VERSION}&resource={}",
            urlencoding::encode(&self.resource),
        );
        if let Some(cid) = self.client_id.as_deref() {
            url.push_str(&format!("&client_id={}", urlencoding::encode(cid)));
        }

        let resp = self
            .http_client
            .get(&url)
            .header("Metadata", "true")
            .send()
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Azure IMDS request failed: {e}"),
            })?;

        if !resp.status().is_success() {
            return Err(Error::Runtime {
                message: format!(
                    "Azure IMDS returned status {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                ),
            });
        }

        resp.json().await.map_err(|e| Error::Runtime {
            message: format!("Failed to parse IMDS token response: {e}"),
        })
    }
}
