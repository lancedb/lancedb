// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use pyo3::FromPyObject;

use lancedb::error::Error;
use lancedb::remote::oauth::{OAuthConfig, OAuthFlow};

/// Python-side OAuth configuration, extracted via FromPyObject.
/// Maps to `lancedb.remote.oauth.OAuthConfig` Python dataclass.
#[derive(FromPyObject)]
pub struct PyOAuthConfig {
    pub issuer_url: String,
    pub client_id: String,
    pub scopes: Vec<String>,
    pub flow: String,
    pub client_secret: Option<String>,
    pub managed_identity_client_id: Option<String>,
    pub refresh_buffer_secs: Option<u64>,
}

impl TryFrom<PyOAuthConfig> for OAuthConfig {
    type Error = Error;

    fn try_from(py: PyOAuthConfig) -> Result<Self, Self::Error> {
        let flow = match py.flow.as_str() {
            "client_credentials" => OAuthFlow::ClientCredentials,
            "azure_managed_identity" => OAuthFlow::AzureManagedIdentity {
                client_id: py.managed_identity_client_id,
            },
            other => {
                return Err(Error::InvalidInput {
                    message: format!("Unknown OAuth flow type: {other}"),
                });
            }
        };

        Ok(Self {
            issuer_url: py.issuer_url,
            client_id: py.client_id,
            client_secret: py.client_secret,
            scopes: py.scopes,
            flow,
            refresh_buffer_secs: py.refresh_buffer_secs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unknown_oauth_flow_returns_invalid_input() {
        let config = PyOAuthConfig {
            issuer_url: "https://issuer.example.com".to_string(),
            client_id: "client-id".to_string(),
            scopes: vec!["scope".to_string()],
            flow: "typo".to_string(),
            client_secret: None,
            managed_identity_client_id: None,
            refresh_buffer_secs: None,
        };

        let err = OAuthConfig::try_from(config).unwrap_err();
        assert!(matches!(
            err,
            Error::InvalidInput { message }
                if message == "Unknown OAuth flow type: typo"
        ));
    }
}
