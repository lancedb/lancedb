// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[tokio::test]
async fn test_device_authorization_polls_until_success() {
    let (issuer_url, token_requests, server) = spawn_device_server().await;
    let source = DeviceCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        Some("secret".to_string()),
        ClientAuthMethod::ClientSecretBasic,
        vec!["openid".to_string()],
        None,
        None,
    )
    .unwrap();

    let device = source.request_device_authorization().await.unwrap();
    let response = source.poll_for_token(&device).await.unwrap();

    assert_eq!(response.access_token.secret(), "device-token");
    assert_eq!(
        response
            .refresh_token()
            .map(|token| token.secret().as_str()),
        Some("device-refresh")
    );
    assert_eq!(token_requests.load(Ordering::SeqCst), 3);
    server.await.unwrap();
}

#[tokio::test]
async fn test_device_authorization_rejects_plaintext_verification_uri() {
    let (issuer_url, server) = spawn_insecure_device_verification_server().await;
    let source = DeviceCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        None,
        ClientAuthMethod::None,
        vec!["openid".to_string()],
        None,
        None,
    )
    .unwrap();

    let Err(err) = source.request_device_authorization().await else {
        panic!("expected insecure verification URI to be rejected");
    };
    assert!(matches!(
        err,
        Error::InvalidInput { message }
            if message.contains("verification_uri must use https")
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn test_device_authorization_retries_transient_failures() {
    let (issuer_url, token_requests, server) = spawn_device_transient_server().await;
    let source = DeviceCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        None,
        ClientAuthMethod::None,
        vec!["openid".to_string()],
        None,
        None,
    )
    .unwrap();
    let device = test_device_authorization_response(60, 1);

    let response = source.poll_for_token(&device).await.unwrap();

    assert_eq!(response.access_token.secret(), "device-token");
    assert_eq!(token_requests.load(Ordering::SeqCst), 4);
    server.await.unwrap();
}

#[tokio::test]
async fn test_device_authorization_reports_access_denied() {
    let (issuer_url, server) = spawn_device_error_server("access_denied").await;
    let source = DeviceCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        None,
        ClientAuthMethod::None,
        vec!["openid".to_string()],
        None,
        None,
    )
    .unwrap();
    let device = test_device_authorization_response(60, 1);

    let err = source.poll_for_token(&device).await.unwrap_err();
    assert!(matches!(
        err,
        Error::Runtime { message }
            if message == "Device authorization was denied by the user"
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn test_device_authorization_reports_provider_expiry() {
    let (issuer_url, server) = spawn_device_error_server("expired_token").await;
    let source = DeviceCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        None,
        ClientAuthMethod::None,
        vec!["openid".to_string()],
        None,
        None,
    )
    .unwrap();
    let device = test_device_authorization_response(60, 1);

    let err = source.poll_for_token(&device).await.unwrap_err();
    assert!(matches!(
        err,
        Error::Runtime { message }
            if message == "Device authorization expired before authentication completed"
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn test_device_authorization_stops_at_local_deadline() {
    let (issuer_url, server) = spawn_discovery_server(1).await;
    let source = DeviceCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        None,
        ClientAuthMethod::None,
        vec!["openid".to_string()],
        None,
        None,
    )
    .unwrap();
    let device = test_device_authorization_response(1, 1);

    let err = source.poll_for_token(&device).await.unwrap_err();
    assert!(matches!(
        err,
        Error::Runtime { message }
            if message == "Device authorization expired before authentication completed"
    ));
    server.await.unwrap();
}

#[derive(Debug)]
struct RefreshingTokenSource {
    fetches: Arc<AtomicUsize>,
    refreshes: Arc<AtomicUsize>,
}

#[async_trait]
impl TokenSource for RefreshingTokenSource {
    async fn fetch_token(&self) -> Result<TokenResponse> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        Ok(token_response("initial", Some("refresh"), Some(3600)))
    }

    async fn refresh_token(&self, refresh_token: &str) -> Result<RefreshResult> {
        assert_eq!(refresh_token, "refresh");
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        Ok(RefreshResult::Refreshed(token_response(
            "refreshed",
            None,
            Some(3600),
        )))
    }
}

#[tokio::test]
async fn test_header_provider_uses_and_retains_refresh_token() {
    let fetches = Arc::new(AtomicUsize::new(0));
    let refreshes = Arc::new(AtomicUsize::new(0));
    let provider = OAuthHeaderProvider {
        token_source: Box::new(RefreshingTokenSource {
            fetches: Arc::clone(&fetches),
            refreshes: Arc::clone(&refreshes),
        }),
        token_state: Arc::new(RwLock::new(TokenState::new())),
        refresh_buffer: Duration::ZERO,
        token_cache: None,
    };

    assert_eq!(provider.get_valid_token().await.unwrap(), "initial");
    provider.token_state.write().await.expires_at = Some(Instant::now() - Duration::from_secs(1));
    assert_eq!(provider.get_valid_token().await.unwrap(), "refreshed");
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(
        provider.token_state.read().await.refresh_token.as_deref(),
        Some("refresh")
    );
}

#[derive(Debug)]
struct FailedRefreshTokenSource {
    fetches: Arc<AtomicUsize>,
    refreshes: Arc<AtomicUsize>,
}

#[async_trait]
impl TokenSource for FailedRefreshTokenSource {
    async fn fetch_token(&self) -> Result<TokenResponse> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        Ok(token_response(
            "reauthenticated",
            Some("new-refresh"),
            Some(3600),
        ))
    }

    async fn refresh_token(&self, refresh_token: &str) -> Result<RefreshResult> {
        assert_eq!(refresh_token, "revoked-refresh");
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        Ok(RefreshResult::Reauthenticate)
    }
}

#[tokio::test]
async fn test_header_provider_reauthenticates_after_refresh_failure() {
    let fetches = Arc::new(AtomicUsize::new(0));
    let refreshes = Arc::new(AtomicUsize::new(0));
    let provider = OAuthHeaderProvider {
        token_source: Box::new(FailedRefreshTokenSource {
            fetches: Arc::clone(&fetches),
            refreshes: Arc::clone(&refreshes),
        }),
        token_state: Arc::new(RwLock::new(TokenState {
            access_token: Some("expired".to_string()),
            refresh_token: Some("revoked-refresh".to_string()),
            expires_at: Some(Instant::now() - Duration::from_secs(1)),
        })),
        refresh_buffer: Duration::ZERO,
        token_cache: None,
    };

    assert_eq!(provider.get_valid_token().await.unwrap(), "reauthenticated");
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(
        provider.token_state.read().await.refresh_token.as_deref(),
        Some("new-refresh")
    );
}

#[derive(Debug)]
struct TransientRefreshFailureSource {
    fetches: Arc<AtomicUsize>,
}

#[async_trait]
impl TokenSource for TransientRefreshFailureSource {
    async fn fetch_token(&self) -> Result<TokenResponse> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        unreachable!("a transient refresh failure must not start an interactive flow")
    }

    async fn refresh_token(&self, refresh_token: &str) -> Result<RefreshResult> {
        assert_eq!(refresh_token, "valid-refresh");
        Err(Error::Runtime {
            message: "token endpoint temporarily unavailable".to_string(),
        })
    }
}

#[tokio::test]
async fn test_header_provider_preserves_refresh_token_after_transient_failure() {
    let fetches = Arc::new(AtomicUsize::new(0));
    let provider = OAuthHeaderProvider {
        token_source: Box::new(TransientRefreshFailureSource {
            fetches: Arc::clone(&fetches),
        }),
        token_state: Arc::new(RwLock::new(TokenState {
            access_token: Some("expired".to_string()),
            refresh_token: Some("valid-refresh".to_string()),
            expires_at: Some(Instant::now() - Duration::from_secs(1)),
        })),
        refresh_buffer: Duration::ZERO,
        token_cache: None,
    };

    let err = provider.get_valid_token().await.unwrap_err();
    assert!(matches!(
        err,
        Error::Runtime { message }
            if message == "token endpoint temporarily unavailable"
    ));
    assert_eq!(fetches.load(Ordering::SeqCst), 0);
    assert_eq!(
        provider.token_state.read().await.refresh_token.as_deref(),
        Some("valid-refresh")
    );
}
