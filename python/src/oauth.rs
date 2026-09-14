// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use pyo3::FromPyObject;

use lancedb::error::Error;
use lancedb::remote::oauth::{AuthorizationCodeOptions, OAuthConfig, OAuthFlow};

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
    pub use_pkce: bool,
    pub managed_identity_client_id: Option<String>,
    pub refresh_buffer_secs: Option<u64>,
}

impl TryFrom<PyOAuthConfig> for OAuthConfig {
    type Error = Error;

    fn try_from(py: PyOAuthConfig) -> Result<Self, Self::Error> {
        let flow = match py.flow.as_str() {
            "client_credentials" => OAuthFlow::ClientCredentials,
            "authorization_code" => {
                let mut options = AuthorizationCodeOptions::new().use_pkce(py.use_pkce);
                if let Some(redirect_uri) = py.redirect_uri {
                    options = options.redirect_uri(redirect_uri);
                }
                if let Some(callback_port) = py.callback_port {
                    options = options.callback_port(callback_port);
                }
                OAuthFlow::AuthorizationCode(options)
            }
            "device_code" => OAuthFlow::DeviceCode,
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
            redirect_uri: None,
            callback_port: None,
            use_pkce: true,
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

    #[test]
    fn test_authorization_code_conversion_preserves_options() {
        let config = PyOAuthConfig {
            issuer_url: "https://issuer.example.com".to_string(),
            client_id: "client-id".to_string(),
            scopes: vec!["openid".to_string()],
            flow: "authorization_code".to_string(),
            client_secret: Some("secret".to_string()),
            redirect_uri: Some("http://127.0.0.1:9000/callback".to_string()),
            callback_port: Some(9000),
            use_pkce: false,
            managed_identity_client_id: None,
            refresh_buffer_secs: None,
        };

        let converted = OAuthConfig::try_from(config).unwrap();
        let OAuthFlow::AuthorizationCode(options) = converted.flow else {
            panic!("expected authorization code flow");
        };
        assert_eq!(
            options.redirect_uri.as_deref(),
            Some("http://127.0.0.1:9000/callback")
        );
        assert_eq!(options.callback_port, Some(9000));
        assert!(!options.use_pkce);
    }

    #[test]
    fn test_device_code_conversion() {
        let config = PyOAuthConfig {
            issuer_url: "https://issuer.example.com".to_string(),
            client_id: "client-id".to_string(),
            scopes: vec!["openid".to_string()],
            flow: "device_code".to_string(),
            client_secret: None,
            redirect_uri: None,
            callback_port: None,
            use_pkce: true,
            managed_identity_client_id: None,
            refresh_buffer_secs: None,
        };

        let converted = OAuthConfig::try_from(config).unwrap();
        assert!(matches!(converted.flow, OAuthFlow::DeviceCode));
    }
}
