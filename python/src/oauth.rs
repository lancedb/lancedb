// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use pyo3::FromPyObject;

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
    pub redirect_uri: Option<String>,
    pub callback_port: Option<u16>,
    pub managed_identity_client_id: Option<String>,
    pub token_file: Option<String>,
    pub refresh_buffer_secs: Option<u64>,
}

impl From<PyOAuthConfig> for OAuthConfig {
    fn from(py: PyOAuthConfig) -> Self {
        let flow = match py.flow.as_str() {
            "client_credentials" => OAuthFlow::ClientCredentials,
            "authorization_code_pkce" => OAuthFlow::AuthorizationCodePKCE {
                redirect_uri: py.redirect_uri,
                callback_port: py.callback_port,
            },
            "device_code" => OAuthFlow::DeviceCode,
            "azure_managed_identity" => OAuthFlow::AzureManagedIdentity {
                client_id: py.managed_identity_client_id,
            },
            "workload_identity" => OAuthFlow::WorkloadIdentity {
                token_file: py
                    .token_file
                    .expect("token_file is required for workload_identity flow"),
            },
            other => panic!("Unknown OAuth flow type: {other}"),
        };

        OAuthConfig {
            issuer_url: py.issuer_url,
            client_id: py.client_id,
            client_secret: py.client_secret,
            scopes: py.scopes,
            flow,
            refresh_buffer_secs: py.refresh_buffer_secs,
        }
    }
}
