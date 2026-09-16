// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::path::PathBuf;
use std::sync::Arc;

use pyo3::{FromPyObject, PyResult, Python, pyclass, pymethods};

use crate::error::PythonErrorExt;
use crate::runtime::future_into_py;
use lancedb::error::Error;
use lancedb::remote::oauth::{AuthorizationCodeOptions, ClientAuthMethod, OAuthConfig, OAuthFlow};
use lancedb::remote::{OAuthSession, SessionLogout, SessionStatus, TokenCacheOptions};

/// Python-side persistent token cache options, extracted via FromPyObject.
/// Maps to `lancedb.remote.oauth.TokenCacheOptions` Python dataclass.
#[derive(FromPyObject, Default)]
pub struct PyTokenCacheOptions {
    pub cache_dir: Option<String>,
    pub lock_timeout_secs: Option<u64>,
}

impl From<PyTokenCacheOptions> for TokenCacheOptions {
    fn from(py: PyTokenCacheOptions) -> Self {
        TokenCacheOptions {
            cache_dir: py.cache_dir.map(PathBuf::from),
            lock_timeout_secs: py.lock_timeout_secs,
        }
    }
}

/// Python-side OAuth configuration, extracted via FromPyObject.
/// Maps to `lancedb.remote.oauth.OAuthConfig` Python dataclass.
#[derive(FromPyObject)]
pub struct PyOAuthConfig {
    pub issuer_url: String,
    pub client_id: String,
    pub scopes: Vec<String>,
    /// Optional resource indicator for authorization and token requests.
    pub resource: Option<String>,
    /// Optional provider-specific audience for authorization and token requests.
    pub audience: Option<String>,
    pub flow: String,
    pub client_secret: Option<String>,
    pub client_auth_method: Option<String>,
    pub redirect_uri: Option<String>,
    pub callback_port: Option<u16>,
    pub use_pkce: bool,
    pub managed_identity_client_id: Option<String>,
    pub refresh_buffer_secs: Option<u64>,
    pub token_cache: Option<PyTokenCacheOptions>,
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

        let client_auth_method = match py.client_auth_method.as_deref() {
            Some("none") => Some(ClientAuthMethod::None),
            Some("client_secret_basic") => Some(ClientAuthMethod::ClientSecretBasic),
            Some("client_secret_post") => Some(ClientAuthMethod::ClientSecretPost),
            None => None,
            Some(other) => {
                return Err(Error::InvalidInput {
                    message: format!("Unknown OAuth client auth method: {other}"),
                });
            }
        };

        Ok(Self {
            issuer_url: py.issuer_url,
            client_id: py.client_id,
            client_secret: py.client_secret,
            client_auth_method,
            scopes: py.scopes,
            resource: py.resource,
            audience: py.audience,
            flow,
            refresh_buffer_secs: py.refresh_buffer_secs,
            token_cache: py.token_cache.map(TokenCacheOptions::from),
        })
    }
}

/// Wrapper around [`lancedb::remote::SessionStatus`] exposing safe metadata.
#[pyclass(name = "SessionStatus", skip_from_py_object)]
#[derive(Clone)]
pub struct PySessionStatus {
    inner: SessionStatus,
}

#[pymethods]
impl PySessionStatus {
    /// Whether a cached session exists that can obtain tokens without
    /// interactive authentication.
    #[getter]
    pub fn refreshable(&self) -> bool {
        self.inner.refreshable
    }

    /// Canonical issuer URL of the cached session.
    #[getter]
    pub fn issuer_url(&self) -> String {
        self.inner.issuer_url.clone()
    }

    /// Client ID of the cached session.
    #[getter]
    pub fn client_id(&self) -> String {
        self.inner.client_id.clone()
    }

    /// Canonical (sorted, de-duplicated) scopes of the cached session.
    #[getter]
    pub fn scopes(&self) -> Vec<String> {
        self.inner.scopes.clone()
    }

    /// Resource indicator used to obtain the cached session.
    #[getter]
    pub fn resource(&self) -> Option<String> {
        self.inner.resource.clone()
    }

    /// Provider-specific audience used to obtain the cached session.
    #[getter]
    pub fn audience(&self) -> Option<String> {
        self.inner.audience.clone()
    }

    /// Flow that produced the cached session.
    #[getter]
    pub fn flow(&self) -> String {
        self.inner.flow.clone()
    }

    /// When the cached session was obtained, as Unix seconds.
    #[getter]
    pub fn obtained_at(&self) -> Option<u64> {
        self.inner.obtained_at
    }

    pub fn __repr__(&self) -> String {
        format!(
            "SessionStatus(refreshable={}, issuer_url='{}', client_id='{}', flow='{}')",
            self.inner.refreshable, self.inner.issuer_url, self.inner.client_id, self.inner.flow
        )
    }
}

impl From<SessionStatus> for PySessionStatus {
    fn from(inner: SessionStatus) -> Self {
        Self { inner }
    }
}

/// Wrapper around [`lancedb::remote::SessionLogout`].
#[pyclass(name = "SessionLogout", skip_from_py_object)]
#[derive(Clone)]
pub struct PySessionLogout {
    inner: SessionLogout,
}

#[pymethods]
impl PySessionLogout {
    /// Whether a cached credential was removed.
    #[getter]
    pub fn removed(&self) -> bool {
        self.inner.removed
    }

    pub fn __repr__(&self) -> String {
        format!("SessionLogout(removed={})", self.inner.removed)
    }
}

impl From<SessionLogout> for PySessionLogout {
    fn from(inner: SessionLogout) -> Self {
        Self { inner }
    }
}

/// Wrapper around [`lancedb::remote::OAuthSession`].
#[pyclass(name = "OAuthSession", skip_from_py_object)]
#[derive(Clone)]
pub struct PyOAuthSession {
    inner: Arc<OAuthSession>,
}

#[pymethods]
impl PyOAuthSession {
    /// Create a session manager for the given OAuth configuration.
    ///
    /// The configuration must set ``token_cache`` options and use a flow that
    /// supports persistent sessions (authorization code or device code).
    #[new]
    pub fn new(config: PyOAuthConfig) -> PyResult<Self> {
        let config: OAuthConfig = config.try_into().infer_error()?;
        let inner = OAuthSession::new(config).infer_error()?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Eagerly run the configured authentication flow and store the session.
    pub fn login<'py>(&self, py: Python<'py>) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            inner.login().await.map(PySessionStatus::from).infer_error()
        })
    }

    /// Report whether a matching cached session exists, with safe metadata.
    pub fn status<'py>(&self, py: Python<'py>) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            inner
                .status()
                .await
                .map(PySessionStatus::from)
                .infer_error()
        })
    }

    /// Remove the matching local cached credential.
    pub fn logout<'py>(&self, py: Python<'py>) -> PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            inner
                .logout()
                .await
                .map(PySessionLogout::from)
                .infer_error()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> PyOAuthConfig {
        PyOAuthConfig {
            issuer_url: "https://issuer.example.com".to_string(),
            client_id: "client-id".to_string(),
            scopes: vec!["scope".to_string()],
            flow: "device_code".to_string(),
            client_secret: None,
            client_auth_method: None,
            redirect_uri: None,
            callback_port: None,
            use_pkce: true,
            managed_identity_client_id: None,
            refresh_buffer_secs: None,
            resource: None,
            audience: None,
            token_cache: None,
        }
    }

    #[test]
    fn test_unknown_oauth_flow_returns_invalid_input() {
        let config = PyOAuthConfig {
            flow: "typo".to_string(),
            ..base_config()
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
            flow: "authorization_code".to_string(),
            client_secret: Some("secret".to_string()),
            redirect_uri: Some("http://127.0.0.1:9000/callback".to_string()),
            callback_port: Some(9000),
            use_pkce: false,
            resource: Some("urn:resource".into()),
            audience: Some("audience".into()),
            ..base_config()
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
        assert_eq!(converted.resource.as_deref(), Some("urn:resource"));
        assert_eq!(converted.audience.as_deref(), Some("audience"));
    }

    #[test]
    fn test_device_code_conversion() {
        let config = base_config();
        let converted = OAuthConfig::try_from(config).unwrap();
        assert!(matches!(converted.flow, OAuthFlow::DeviceCode));
    }

    #[test]
    fn test_client_auth_method_conversion() {
        for (value, expected) in [
            ("none", ClientAuthMethod::None),
            ("client_secret_basic", ClientAuthMethod::ClientSecretBasic),
            ("client_secret_post", ClientAuthMethod::ClientSecretPost),
        ] {
            let config = PyOAuthConfig {
                client_auth_method: Some(value.to_string()),
                ..base_config()
            };

            let converted = OAuthConfig::try_from(config).unwrap();
            assert_eq!(converted.client_auth_method, Some(expected));
        }
    }

    #[test]
    fn test_unknown_client_auth_method_returns_invalid_input() {
        let config = PyOAuthConfig {
            client_auth_method: Some("typo".to_string()),
            ..base_config()
        };

        let err = OAuthConfig::try_from(config).unwrap_err();
        assert!(matches!(
            err,
            Error::InvalidInput { message }
                if message == "Unknown OAuth client auth method: typo"
        ));
    }

    #[test]
    fn test_token_cache_conversion() {
        let config = PyOAuthConfig {
            resource: None,
            audience: None,
            token_cache: Some(PyTokenCacheOptions {
                cache_dir: Some("/tmp/oauth-cache".to_string()),
                lock_timeout_secs: Some(5),
            }),
            ..base_config()
        };

        let converted = OAuthConfig::try_from(config).unwrap();
        let cache = converted.token_cache.expect("token cache options");
        assert_eq!(
            cache.cache_dir.as_deref(),
            Some(std::path::Path::new("/tmp/oauth-cache"))
        );
        assert_eq!(cache.lock_timeout_secs, Some(5));
    }
}
