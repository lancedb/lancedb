// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/// A configuration naming any other delimiter is refused where it was
/// written, rather than producing identifiers no service splits the way the
/// caller meant.
#[test]
fn test_a_delimiter_other_than_the_supported_one_is_refused() {
    for delimiter in ["/", "?", "#", "%", "", ".", "..", "-", "_", "|", "::", "$$"] {
        let error = super::validate_id_delimiter(delimiter)
            .expect_err("only the supported delimiter may be configured");
        assert!(
            error.to_string().contains("id_delimiter"),
            "{delimiter:?}: {error}"
        );
    }
    super::validate_id_delimiter(super::ID_DELIMITER).unwrap();
}

/// Leaving it unset is how nearly every caller reaches the same delimiter.
#[test]
fn test_an_unset_delimiter_is_the_supported_one() {
    super::ClientConfig::default().validate().unwrap();
    super::ClientConfig {
        id_delimiter: Some(super::ID_DELIMITER.to_string()),
        ..Default::default()
    }
    .validate()
    .unwrap();
    super::ClientConfig {
        id_delimiter: Some("-".to_string()),
        ..Default::default()
    }
    .validate()
    .expect_err("a configured delimiter other than the supported one must be refused");
}

use super::*;
use serial_test::serial;
use std::time::Duration;

// Serializes the env-var-mutating tests below: cargo test runs tests in
// parallel, but several of these tests read and write the same process-
// global env vars (`LANCEDB_USER_ID*`), so they would race without this.
static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn lock_env() -> std::sync::MutexGuard<'static, ()> {
    ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner())
}

#[test]
fn test_parse_catalog_database_uri() {
    let parsed = parse_db_url("db://team%2Fsearch").unwrap();
    assert_eq!(parsed.db_name, "team/search");
    assert!(parsed.db_prefix.is_none());
    let parsed = parse_db_url("db://db/prefix").unwrap();
    assert_eq!(parsed.db_name, "db");
    assert_eq!(parsed.db_prefix.as_deref(), Some("prefix"));
}

#[test]
fn test_timeout_config_default() {
    let config = TimeoutConfig::default();
    assert!(config.timeout.is_none());
    assert!(config.connect_timeout.is_none());
    assert!(config.read_timeout.is_none());
    assert!(config.pool_idle_timeout.is_none());
}

#[test]
fn test_timeout_config_with_overall_timeout() {
    let config = TimeoutConfig {
        timeout: Some(Duration::from_secs(60)),
        connect_timeout: Some(Duration::from_secs(10)),
        read_timeout: Some(Duration::from_secs(30)),
        pool_idle_timeout: Some(Duration::from_secs(300)),
    };

    assert_eq!(config.timeout, Some(Duration::from_secs(60)));
    assert_eq!(config.connect_timeout, Some(Duration::from_secs(10)));
    assert_eq!(config.read_timeout, Some(Duration::from_secs(30)));
    assert_eq!(config.pool_idle_timeout, Some(Duration::from_secs(300)));
}

#[test]
fn test_client_config_with_timeout() {
    let timeout_config = TimeoutConfig {
        timeout: Some(Duration::from_secs(120)),
        ..Default::default()
    };

    let client_config = ClientConfig {
        timeout_config,
        ..Default::default()
    };

    assert_eq!(
        client_config.timeout_config.timeout,
        Some(Duration::from_secs(120))
    );
}

#[test]
fn test_tls_config_default() {
    let config = TlsConfig::default();
    assert!(config.cert_file.is_none());
    assert!(config.key_file.is_none());
    assert!(config.ssl_ca_cert.is_none());
    assert!(config.assert_hostname);
}

#[test]
fn test_tls_config_with_mtls() {
    let tls_config = TlsConfig {
        cert_file: Some("/path/to/cert.pem".to_string()),
        key_file: Some("/path/to/key.pem".to_string()),
        ssl_ca_cert: Some("/path/to/ca.pem".to_string()),
        assert_hostname: true,
    };

    assert_eq!(tls_config.cert_file, Some("/path/to/cert.pem".to_string()));
    assert_eq!(tls_config.key_file, Some("/path/to/key.pem".to_string()));
    assert_eq!(tls_config.ssl_ca_cert, Some("/path/to/ca.pem".to_string()));
    assert!(tls_config.assert_hostname);
}

#[test]
fn test_client_config_with_tls() {
    let tls_config = TlsConfig {
        cert_file: Some("/path/to/cert.pem".to_string()),
        key_file: Some("/path/to/key.pem".to_string()),
        ssl_ca_cert: None,
        assert_hostname: false,
    };

    let client_config = ClientConfig {
        tls_config: Some(tls_config.clone()),
        ..Default::default()
    };

    assert!(client_config.tls_config.is_some());
    let config_tls = client_config.tls_config.unwrap();
    assert_eq!(config_tls.cert_file, Some("/path/to/cert.pem".to_string()));
    assert_eq!(config_tls.key_file, Some("/path/to/key.pem".to_string()));
    assert!(config_tls.ssl_ca_cert.is_none());
    assert!(!config_tls.assert_hostname);
}

#[test]
fn test_default_headers_skip_empty_api_key() {
    let headers = RestfulLanceDbClient::<Sender>::default_headers(
        "",
        "us-east-1",
        "db-name",
        false,
        &RemoteOptions::default(),
        None,
        &ClientConfig::default(),
    )
    .unwrap();
    assert!(!headers.contains_key("x-api-key"));

    let headers = RestfulLanceDbClient::<Sender>::default_headers(
        "static-secret-value",
        "us-east-1",
        "db-name",
        false,
        &RemoteOptions::default(),
        None,
        &ClientConfig::default(),
    )
    .unwrap();
    let api_key = headers.get("x-api-key").unwrap();
    assert_eq!(api_key, "static-secret-value");
    assert!(api_key.is_sensitive());
    assert!(!format!("{headers:?}").contains("static-secret-value"));
}

#[test]
fn test_configured_authentication_headers_are_sensitive() {
    let config = ClientConfig {
        extra_headers: HashMap::from([
            (
                "Authorization".to_string(),
                "Bearer configured-secret".to_string(),
            ),
            ("X-API-Key".to_string(), "configured-api-key".to_string()),
            (
                "Cookie".to_string(),
                "session=configured-cookie".to_string(),
            ),
            (
                "Proxy-Authorization".to_string(),
                "Basic configured-proxy-secret".to_string(),
            ),
            ("X-Custom".to_string(), "visible-value".to_string()),
        ]),
        ..Default::default()
    };
    let headers = RestfulLanceDbClient::<Sender>::default_headers(
        "",
        "us-east-1",
        "db-name",
        false,
        &RemoteOptions::default(),
        None,
        &config,
    )
    .unwrap();

    assert!(headers.get("authorization").unwrap().is_sensitive());
    assert!(headers.get("x-api-key").unwrap().is_sensitive());
    assert!(headers.get("cookie").unwrap().is_sensitive());
    assert!(headers.get("proxy-authorization").unwrap().is_sensitive());
    assert!(!headers.get("x-custom").unwrap().is_sensitive());
    let debug = format!("{headers:?}");
    assert!(!debug.contains("configured-secret"));
    assert!(!debug.contains("configured-api-key"));
    assert!(!debug.contains("configured-cookie"));
    assert!(!debug.contains("configured-proxy-secret"));
    assert!(debug.contains("visible-value"));
}

/// `log_request` prints the request's Debug, and Debug for a request prints
/// its headers. Marking the value sensitive is the only thing standing
/// between the API key and every debug line; assert on the header map's own
/// Debug, which is what that printing reduces to.
#[test]
fn test_api_key_is_redacted_in_debug_output() {
    let headers = RestfulLanceDbClient::<Sender>::default_headers(
        "sk-live-sentinel",
        "us-east-1",
        "db-name",
        false,
        &RemoteOptions::default(),
        None,
        &ClientConfig::default(),
    )
    .unwrap();

    assert_eq!(headers.get("x-api-key").unwrap(), "sk-live-sentinel");
    assert!(
        !format!("{:?}", headers).contains("sk-live-sentinel"),
        "the API key must not survive Debug formatting"
    );
}

/// A suppressed body is suppressed whatever the content type says, and an
/// allowed one is logged in full.
#[test]
fn test_body_logging_is_decided_by_the_caller() {
    assert_ne!(BodyLogging::Allowed, BodyLogging::Suppressed);
    // `send` and `send_suppressing_body` differ only in what they pass, so
    // the enum is the whole contract: a caller states its intent and the
    // transport does not infer one from the route.
    assert_eq!(BodyLogging::Allowed, BodyLogging::Allowed);
}

#[test]
fn test_rejects_invalid_cloud_dns_hostname() {
    let invalid_database_names = ["a".repeat(64), "invalid..database".to_string()];

    for db_name in invalid_database_names {
        let parsed_url = parse_db_url(&format!("db://{db_name}")).unwrap();
        let error = RestfulLanceDbClient::<Sender>::try_new(
            &parsed_url,
            "us-east-1",
            None,
            HeaderMap::new(),
            ClientConfig::default(),
            None,
        )
        .unwrap_err();

        assert!(
            matches!(error, Error::InvalidInput { ref message } if message.contains("DNS labels must contain 1 to 63 bytes")),
            "unexpected error: {error}"
        );
    }
}

// Test implementation of HeaderProvider
#[derive(Debug, Clone)]
struct TestHeaderProvider {
    headers: HashMap<String, String>,
}

impl TestHeaderProvider {
    fn new(headers: HashMap<String, String>) -> Self {
        Self { headers }
    }
}

#[async_trait::async_trait]
impl HeaderProvider for TestHeaderProvider {
    async fn get_headers(&self) -> Result<HashMap<String, String>> {
        Ok(self.headers.clone())
    }
}

// Test implementation that returns an error
#[derive(Debug)]
struct ErrorHeaderProvider;

#[async_trait::async_trait]
impl HeaderProvider for ErrorHeaderProvider {
    async fn get_headers(&self) -> Result<HashMap<String, String>> {
        Err(Error::Runtime {
            message: "Failed to get headers".to_string(),
        })
    }
}

#[tokio::test]
async fn test_client_config_with_header_provider() {
    let mut headers = HashMap::new();
    headers.insert("X-API-Key".to_string(), "secret-key".to_string());

    let provider = TestHeaderProvider::new(headers);
    let client_config = ClientConfig {
        header_provider: Some(Arc::new(provider) as Arc<dyn HeaderProvider>),
        ..Default::default()
    };

    assert!(client_config.header_provider.is_some());
}

#[tokio::test]
async fn test_apply_dynamic_headers() {
    // Create a mock client with header provider
    let mut headers = HashMap::new();
    headers.insert("X-Dynamic".to_string(), "dynamic-value".to_string());

    let provider = TestHeaderProvider::new(headers);

    // Create a simple request
    let request = reqwest::Request::new(
        reqwest::Method::GET,
        "https://example.com/test".parse().unwrap(),
    );

    // Create client with header provider
    let client = RestfulLanceDbClient {
        client: reqwest::Client::new(),
        host: "https://example.com".to_string(),
        retry_config: RetryConfig::default().try_into().unwrap(),
        sender: Sender,
        header_provider: Some(Arc::new(provider) as Arc<dyn HeaderProvider>),
        read_consistency_interval: None,
        max_bytes_per_request: None,
        max_request_duration: None,
    };

    // Apply dynamic headers
    let updated_request = client.apply_dynamic_headers(request).await.unwrap();

    // Check that the header was added
    assert_eq!(
        updated_request.headers().get("X-Dynamic").unwrap(),
        "dynamic-value"
    );
}

#[tokio::test]
async fn test_apply_dynamic_headers_merge() {
    // Test that dynamic headers override existing headers
    let mut headers = HashMap::new();
    headers.insert("Authorization".to_string(), "Bearer new-token".to_string());
    headers.insert("X-API-Key".to_string(), "new-api-key".to_string());
    headers.insert("X-Custom".to_string(), "custom-value".to_string());

    let provider = TestHeaderProvider::new(headers);

    // Create request with existing Authorization header
    let mut request_builder = reqwest::Client::new().get("https://example.com/test");
    request_builder = request_builder.header("Authorization", "Bearer old-token");
    request_builder = request_builder.header("X-Existing", "existing-value");
    let request = request_builder.build().unwrap();

    // Create client with header provider
    let client = RestfulLanceDbClient {
        client: reqwest::Client::new(),
        host: "https://example.com".to_string(),
        retry_config: RetryConfig::default().try_into().unwrap(),
        sender: Sender,
        header_provider: Some(Arc::new(provider) as Arc<dyn HeaderProvider>),
        read_consistency_interval: None,
        max_bytes_per_request: None,
        max_request_duration: None,
    };

    // Apply dynamic headers
    let updated_request = client.apply_dynamic_headers(request).await.unwrap();

    // Check that dynamic headers override existing ones
    assert_eq!(
        updated_request.headers().get("Authorization").unwrap(),
        "Bearer new-token"
    );
    assert_eq!(
        updated_request.headers().get("X-Custom").unwrap(),
        "custom-value"
    );
    assert!(
        updated_request
            .headers()
            .get("Authorization")
            .unwrap()
            .is_sensitive()
    );
    assert!(
        updated_request
            .headers()
            .get("X-API-Key")
            .unwrap()
            .is_sensitive()
    );
    assert!(
        !updated_request
            .headers()
            .get("X-Custom")
            .unwrap()
            .is_sensitive()
    );
    let debug = format!("{updated_request:?}");
    assert!(!debug.contains("new-token"));
    assert!(!debug.contains("new-api-key"));
    assert!(debug.contains("custom-value"));
    // Existing headers should still be present
    assert_eq!(
        updated_request.headers().get("X-Existing").unwrap(),
        "existing-value"
    );
}

#[tokio::test]
async fn test_apply_dynamic_headers_with_error_provider() {
    let provider = ErrorHeaderProvider;

    let request = reqwest::Request::new(
        reqwest::Method::GET,
        "https://example.com/test".parse().unwrap(),
    );

    let client = RestfulLanceDbClient {
        client: reqwest::Client::new(),
        host: "https://example.com".to_string(),
        retry_config: RetryConfig::default().try_into().unwrap(),
        sender: Sender,
        header_provider: Some(Arc::new(provider) as Arc<dyn HeaderProvider>),
        read_consistency_interval: None,
        max_bytes_per_request: None,
        max_request_duration: None,
    };

    // Header provider errors should fail the request
    // This is important for security - if auth headers can't be fetched, don't proceed
    let result = client.apply_dynamic_headers(request).await;
    assert!(result.is_err());

    match result.unwrap_err() {
        Error::Runtime { message } => {
            assert_eq!(message, "Failed to get headers");
        }
        _ => panic!("Expected Runtime error"),
    }
}

#[test]
fn test_resolve_user_id_direct_value() {
    let config = ClientConfig {
        user_id: Some("direct-user-id".to_string()),
        ..Default::default()
    };
    assert_eq!(config.resolve_user_id(), Some("direct-user-id".to_string()));
}

#[test]
#[serial(user_id_env)]
fn test_resolve_user_id_none() {
    let _guard = lock_env();
    let config = ClientConfig::default();
    // Clear env vars that might be set from other tests
    // SAFETY: This is only called in tests
    unsafe {
        std::env::remove_var("LANCEDB_USER_ID");
        std::env::remove_var("LANCEDB_USER_ID_ENV_KEY");
    }
    assert_eq!(config.resolve_user_id(), None);
}

#[test]
#[serial(user_id_env)]
fn test_resolve_user_id_from_env() {
    let _guard = lock_env();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::set_var("LANCEDB_USER_ID", "env-user-id");
    }
    let config = ClientConfig::default();
    assert_eq!(config.resolve_user_id(), Some("env-user-id".to_string()));
    // SAFETY: This is only called in tests
    unsafe {
        std::env::remove_var("LANCEDB_USER_ID");
    }
}

#[test]
#[serial(user_id_env)]
fn test_resolve_user_id_from_env_key() {
    let _guard = lock_env();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::remove_var("LANCEDB_USER_ID");
        std::env::set_var("LANCEDB_USER_ID_ENV_KEY", "MY_CUSTOM_USER_ID");
        std::env::set_var("MY_CUSTOM_USER_ID", "custom-env-user-id");
    }
    let config = ClientConfig::default();
    assert_eq!(
        config.resolve_user_id(),
        Some("custom-env-user-id".to_string())
    );
    // SAFETY: This is only called in tests
    unsafe {
        std::env::remove_var("LANCEDB_USER_ID_ENV_KEY");
        std::env::remove_var("MY_CUSTOM_USER_ID");
    }
}

#[test]
#[serial(user_id_env)]
fn test_resolve_user_id_direct_takes_precedence() {
    let _guard = lock_env();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::set_var("LANCEDB_USER_ID", "env-user-id");
    }
    let config = ClientConfig {
        user_id: Some("direct-user-id".to_string()),
        ..Default::default()
    };
    assert_eq!(config.resolve_user_id(), Some("direct-user-id".to_string()));
    // SAFETY: This is only called in tests
    unsafe {
        std::env::remove_var("LANCEDB_USER_ID");
    }
}

#[test]
#[serial(user_id_env)]
fn test_resolve_user_id_empty_env_ignored() {
    let _guard = lock_env();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::set_var("LANCEDB_USER_ID", "");
        std::env::remove_var("LANCEDB_USER_ID_ENV_KEY");
    }
    let config = ClientConfig::default();
    assert_eq!(config.resolve_user_id(), None);
    // SAFETY: This is only called in tests
    unsafe {
        std::env::remove_var("LANCEDB_USER_ID");
    }
}

#[test]
fn test_resolve_max_bytes_passed_value_wins() {
    // An explicit config value is used verbatim; env/default are not consulted.
    let resolved =
        RestfulLanceDbClient::<Sender>::resolve_max_bytes_per_request(Some(1234)).unwrap();
    assert_eq!(resolved, Some(1234));
}

#[test]
fn test_resolve_max_bytes_zero_disables() {
    let resolved = RestfulLanceDbClient::<Sender>::resolve_max_bytes_per_request(Some(0)).unwrap();
    assert_eq!(resolved, None);
}

#[test]
#[serial(request_limits_env)]
fn test_resolve_max_bytes_default_when_unset() {
    let _guard = lock_env();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::remove_var("LANCE_CLIENT_MAX_BYTES_PER_REQUEST");
    }
    let resolved = RestfulLanceDbClient::<Sender>::resolve_max_bytes_per_request(None).unwrap();
    assert_eq!(resolved, Some(DEFAULT_MAX_BYTES_PER_REQUEST));
}

#[test]
#[serial(request_limits_env)]
fn test_resolve_max_bytes_from_env() {
    let _guard = lock_env();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::set_var("LANCE_CLIENT_MAX_BYTES_PER_REQUEST", "4096");
    }
    let resolved = RestfulLanceDbClient::<Sender>::resolve_max_bytes_per_request(None).unwrap();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::remove_var("LANCE_CLIENT_MAX_BYTES_PER_REQUEST");
    }
    assert_eq!(resolved, Some(4096));
}

#[test]
#[serial(request_limits_env)]
fn test_resolve_max_bytes_env_zero_disables() {
    let _guard = lock_env();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::set_var("LANCE_CLIENT_MAX_BYTES_PER_REQUEST", "0");
    }
    let resolved = RestfulLanceDbClient::<Sender>::resolve_max_bytes_per_request(None).unwrap();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::remove_var("LANCE_CLIENT_MAX_BYTES_PER_REQUEST");
    }
    assert_eq!(resolved, None);
}

#[test]
#[serial(request_limits_env)]
fn test_resolve_max_bytes_config_overrides_env() {
    let _guard = lock_env();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::set_var("LANCE_CLIENT_MAX_BYTES_PER_REQUEST", "4096");
    }
    // A config value takes precedence over the environment variable.
    let resolved =
        RestfulLanceDbClient::<Sender>::resolve_max_bytes_per_request(Some(1234)).unwrap();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::remove_var("LANCE_CLIENT_MAX_BYTES_PER_REQUEST");
    }
    assert_eq!(resolved, Some(1234));
}

#[test]
#[serial(request_limits_env)]
fn test_resolve_max_bytes_invalid_env_errors() {
    let _guard = lock_env();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::set_var("LANCE_CLIENT_MAX_BYTES_PER_REQUEST", "not-a-number");
    }
    let err = RestfulLanceDbClient::<Sender>::resolve_max_bytes_per_request(None).unwrap_err();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::remove_var("LANCE_CLIENT_MAX_BYTES_PER_REQUEST");
    }
    assert!(matches!(err, Error::InvalidInput { .. }), "got: {err:?}");
}

#[test]
fn test_resolve_max_request_duration_passed_value_wins() {
    let resolved = RestfulLanceDbClient::<Sender>::resolve_max_request_duration(
        Some(Duration::from_secs(42)),
        Duration::from_secs(300),
    )
    .unwrap();
    assert_eq!(resolved, Some(Duration::from_secs(42)));
}

#[test]
fn test_resolve_max_request_duration_zero_disables() {
    let resolved = RestfulLanceDbClient::<Sender>::resolve_max_request_duration(
        Some(Duration::ZERO),
        Duration::from_secs(300),
    )
    .unwrap();
    assert_eq!(resolved, None);
}

#[test]
#[serial(request_limits_env)]
fn test_resolve_max_request_duration_default_is_half_read_timeout() {
    let _guard = lock_env();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::remove_var("LANCE_CLIENT_MAX_REQUEST_DURATION");
    }
    let resolved = RestfulLanceDbClient::<Sender>::resolve_max_request_duration(
        None,
        Duration::from_secs(300),
    )
    .unwrap();
    assert_eq!(resolved, Some(Duration::from_secs(150)));
}

#[test]
#[serial(request_limits_env)]
fn test_resolve_max_request_duration_from_env_seconds() {
    let _guard = lock_env();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::set_var("LANCE_CLIENT_MAX_REQUEST_DURATION", "30");
    }
    let resolved = RestfulLanceDbClient::<Sender>::resolve_max_request_duration(
        None,
        Duration::from_secs(300),
    )
    .unwrap();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::remove_var("LANCE_CLIENT_MAX_REQUEST_DURATION");
    }
    assert_eq!(resolved, Some(Duration::from_secs(30)));
}

#[test]
#[serial(request_limits_env)]
fn test_resolve_max_request_duration_env_zero_disables() {
    let _guard = lock_env();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::set_var("LANCE_CLIENT_MAX_REQUEST_DURATION", "0");
    }
    let resolved = RestfulLanceDbClient::<Sender>::resolve_max_request_duration(
        None,
        Duration::from_secs(300),
    )
    .unwrap();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::remove_var("LANCE_CLIENT_MAX_REQUEST_DURATION");
    }
    assert_eq!(resolved, None);
}

#[test]
#[serial(request_limits_env)]
fn test_resolve_max_request_duration_invalid_env_errors() {
    let _guard = lock_env();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::set_var("LANCE_CLIENT_MAX_REQUEST_DURATION", "12.5");
    }
    let err = RestfulLanceDbClient::<Sender>::resolve_max_request_duration(
        None,
        Duration::from_secs(300),
    )
    .unwrap_err();
    // SAFETY: This is only called in tests
    unsafe {
        std::env::remove_var("LANCE_CLIENT_MAX_REQUEST_DURATION");
    }
    assert!(matches!(err, Error::InvalidInput { .. }), "got: {err:?}");
}
