// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[test]
fn test_authorization_callback_parsing() {
    let callback = parse_authorization_callback(
        "GET /callback?code=abc%20123&state=expected HTTP/1.1\r\nHost: localhost\r\n",
        "/callback",
        "expected",
    )
    .unwrap();

    assert_eq!(callback, AuthorizationCallback::Code("abc 123".to_string()));
}

#[test]
fn test_authorization_callback_rejects_state_mismatch() {
    let err = parse_authorization_callback(
        "GET /callback?code=abc&state=wrong HTTP/1.1\r\nHost: localhost\r\n",
        "/callback",
        "expected",
    )
    .unwrap_err();

    assert!(matches!(
        err,
        Error::Runtime { message }
            if message == "OAuth authorization callback state did not match"
    ));
}

#[test]
fn test_authorization_callback_reports_provider_error() {
    let callback = parse_authorization_callback(
        "GET /callback?error=access_denied&error_description=user+cancelled&state=expected HTTP/1.1\r\nHost: localhost\r\n",
        "/callback",
        "expected",
    )
    .unwrap();

    assert_eq!(
        callback,
        AuthorizationCallback::ProviderError(
            "OAuth authorization failed: user cancelled".to_string()
        )
    );
}

#[tokio::test]
async fn test_authorization_callback_ignores_unrelated_connection_and_partial_read() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let source = AuthorizationCodeSource::new(
        "http://127.0.0.1:1".to_string(),
        "client-id".to_string(),
        None,
        ClientAuthMethod::None,
        vec!["openid".to_string()],
        None,
        None,
        AuthorizationCodeOptions::new().redirect_uri(format!("http://127.0.0.1:{port}/callback")),
    )
    .unwrap();

    let browser = tokio::spawn(async move {
        let mut unrelated = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        unrelated
            .write_all(b"GET /favicon.ico HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        unrelated.read_to_end(&mut response).await.unwrap();
        assert!(
            String::from_utf8(response)
                .unwrap()
                .starts_with("HTTP/1.1 400")
        );

        let mut callback = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        callback
            .write_all(b"GET /callback?code=auth")
            .await
            .unwrap();
        tokio::task::yield_now().await;
        callback
            .write_all(b"-code&state=expected HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
    });

    assert_eq!(
        source
            .wait_for_callback(&listener, "expected")
            .await
            .unwrap(),
        "auth-code"
    );
    browser.await.unwrap();
}

#[tokio::test]
async fn test_authorization_callback_read_respects_overall_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let client = tokio::spawn(async move {
        let _stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let (mut stream, _) = listener.accept().await.unwrap();

    let err = read_authorization_callback(
        &mut stream,
        "/callback",
        "expected",
        TokioInstant::now() + Duration::from_millis(20),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        err,
        Error::Runtime { message }
            if message == "Timed out reading the OAuth authorization callback"
    ));
    client.abort();
}

#[tokio::test]
async fn test_authorization_request_uses_pkce_by_default() {
    let (issuer_url, server) = spawn_discovery_server(1).await;
    let source = AuthorizationCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        None,
        ClientAuthMethod::None,
        vec!["openid".to_string(), "profile".to_string()],
        None,
        None,
        AuthorizationCodeOptions::new(),
    )
    .unwrap();

    let request = source.build_authorization_request().await.unwrap();
    let params: HashMap<_, _> = request.url.query_pairs().into_owned().collect();
    assert_eq!(
        params.get("response_type").map(String::as_str),
        Some("code")
    );
    assert_eq!(
        params.get("client_id").map(String::as_str),
        Some("client-id")
    );
    assert_eq!(
        params.get("redirect_uri").map(String::as_str),
        Some("http://127.0.0.1:8400/callback")
    );
    assert_eq!(
        params.get("scope").map(String::as_str),
        Some("openid profile")
    );
    assert!(params.get("state").is_some_and(|state| state.len() >= 16));
    assert_eq!(
        params.get("code_challenge_method").map(String::as_str),
        Some("S256")
    );
    assert!(params.contains_key("code_challenge"));
    assert!(request.code_verifier.is_some());
    server.await.unwrap();
}

#[tokio::test]
async fn test_authorization_request_rejects_plaintext_provider_endpoint() {
    let (issuer_url, server) = spawn_insecure_authorization_discovery_server().await;
    let source = AuthorizationCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        None,
        ClientAuthMethod::None,
        vec!["openid".to_string()],
        None,
        None,
        AuthorizationCodeOptions::new(),
    )
    .unwrap();

    let err = source.build_authorization_request().await.unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidInput { message }
            if message.contains("authorization_endpoint must use https")
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn test_authorization_request_can_disable_pkce() {
    let (issuer_url, server) = spawn_discovery_server(1).await;
    let source = AuthorizationCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        Some("secret".to_string()),
        ClientAuthMethod::ClientSecretBasic,
        vec!["openid".to_string()],
        None,
        None,
        AuthorizationCodeOptions::new().use_pkce(false),
    )
    .unwrap();

    let request = source.build_authorization_request().await.unwrap();
    let params: HashMap<_, _> = request.url.query_pairs().into_owned().collect();
    assert!(!params.contains_key("code_challenge"));
    assert!(!params.contains_key("code_challenge_method"));
    assert!(request.code_verifier.is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn test_authorization_code_exchange_public_client_sends_client_id_only() {
    let (issuer_url, request, server) = spawn_captured_token_server().await;
    let source = AuthorizationCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        None,
        ClientAuthMethod::None,
        vec!["openid".to_string()],
        None,
        None,
        AuthorizationCodeOptions::new(),
    )
    .unwrap();

    let response = source
        .exchange_code(
            "auth-code",
            Some(PkceCodeVerifier::new("verifier".to_string())),
        )
        .await
        .unwrap();
    assert_eq!(response.access_token.secret(), "token");

    let request = request.lock().unwrap().take().unwrap();
    assert_eq!(request.header("authorization"), None);
    assert!(request.body.contains("grant_type=authorization_code"));
    assert!(request.body.contains("code=auth-code"));
    assert!(request.body.contains("code_verifier=verifier"));
    assert!(request.body.contains("client_id=client-id"));
    assert!(!request.body.contains("client_secret"));
    server.await.unwrap();
}

#[tokio::test]
async fn test_authorization_code_exchange_uses_basic_auth_by_default() {
    let (issuer_url, request, server) = spawn_captured_token_server().await;
    let source = AuthorizationCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        Some("secret".to_string()),
        ClientAuthMethod::ClientSecretBasic,
        vec!["openid".to_string()],
        None,
        None,
        AuthorizationCodeOptions::new(),
    )
    .unwrap();

    source
        .exchange_code(
            "auth-code",
            Some(PkceCodeVerifier::new("verifier".to_string())),
        )
        .await
        .unwrap();

    let request = request.lock().unwrap().take().unwrap();
    assert_eq!(
        request.header("authorization").as_deref(),
        Some(basic_authorization("client-id", "secret").as_str())
    );
    assert!(request.body.contains("grant_type=authorization_code"));
    assert!(request.body.contains("code=auth-code"));
    assert!(request.body.contains("code_verifier=verifier"));
    assert!(!request.body.contains("client_secret"));
    server.await.unwrap();
}

#[tokio::test]
async fn test_authorization_code_exchange_supports_client_secret_post() {
    let (issuer_url, request, server) = spawn_captured_token_server().await;
    let source = AuthorizationCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        Some("secret".to_string()),
        ClientAuthMethod::ClientSecretPost,
        vec!["openid".to_string()],
        None,
        None,
        AuthorizationCodeOptions::new(),
    )
    .unwrap();

    source
        .exchange_code(
            "auth-code",
            Some(PkceCodeVerifier::new("verifier".to_string())),
        )
        .await
        .unwrap();

    let request = request.lock().unwrap().take().unwrap();
    assert_eq!(request.header("authorization"), None);
    assert!(request.body.contains("client_id=client-id"));
    assert!(request.body.contains("client_secret=secret"));
    server.await.unwrap();
}

#[tokio::test]
async fn test_refresh_uses_basic_auth_by_default() {
    let (issuer_url, request, server) = spawn_captured_token_server().await;
    let source = AuthorizationCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        Some("secret".to_string()),
        ClientAuthMethod::ClientSecretBasic,
        vec!["openid".to_string()],
        None,
        None,
        AuthorizationCodeOptions::new(),
    )
    .unwrap();

    assert!(matches!(
        source.oidc.refresh_token("refresh-token").await.unwrap(),
        RefreshResult::Refreshed(_)
    ));

    let request = request.lock().unwrap().take().unwrap();
    assert_eq!(
        request.header("authorization").as_deref(),
        Some(basic_authorization("client-id", "secret").as_str())
    );
    assert!(request.body.contains("grant_type=refresh_token"));
    assert!(request.body.contains("refresh_token=refresh-token"));
    assert!(!request.body.contains("client_secret"));
    server.await.unwrap();
}

#[tokio::test]
async fn test_refresh_supports_client_secret_post_and_rotation() {
    let (issuer_url, request, server) = spawn_captured_token_server().await;
    let source = AuthorizationCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        Some("secret".to_string()),
        ClientAuthMethod::ClientSecretPost,
        vec!["openid".to_string()],
        None,
        None,
        AuthorizationCodeOptions::new(),
    )
    .unwrap();

    let response = match source.oidc.refresh_token("old-refresh").await.unwrap() {
        RefreshResult::Refreshed(response) => response,
        other => panic!("expected refresh, got {other:?}"),
    };
    assert_eq!(response.refresh_token().unwrap().secret(), "refresh");

    let request = request.lock().unwrap().take().unwrap();
    assert_eq!(request.header("authorization"), None);
    assert!(request.body.contains("grant_type=refresh_token"));
    assert!(request.body.contains("refresh_token=old-refresh"));
    assert!(request.body.contains("client_id=client-id"));
    assert!(request.body.contains("client_secret=secret"));
    server.await.unwrap();
}

#[tokio::test]
async fn test_refresh_invalid_grant_requires_reauthentication() {
    let (issuer_url, server) =
        spawn_refresh_error_server("400 Bad Request", r#"{"error":"invalid_grant"}"#).await;
    let source = AuthorizationCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        None,
        ClientAuthMethod::None,
        vec!["openid".to_string()],
        None,
        None,
        AuthorizationCodeOptions::new(),
    )
    .unwrap();

    assert!(matches!(
        source.oidc.refresh_token("revoked").await.unwrap(),
        RefreshResult::Reauthenticate
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn test_refresh_transient_failure_remains_retryable() {
    let (issuer_url, server) = spawn_refresh_error_server(
        "503 Service Unavailable",
        r#"{"error":"temporarily_unavailable"}"#,
    )
    .await;
    let source = AuthorizationCodeSource::new(
        issuer_url,
        "client-id".to_string(),
        None,
        ClientAuthMethod::None,
        vec!["openid".to_string()],
        None,
        None,
        AuthorizationCodeOptions::new(),
    )
    .unwrap();

    let err = source.oidc.refresh_token("still-valid").await.unwrap_err();
    assert!(matches!(
        err,
        Error::Runtime { message }
            if message.contains("503 Service Unavailable") && message.contains("transient")
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn test_token_request_rejects_insecure_redirect() {
    let (issuer_url, server) = spawn_redirecting_token_server().await;
    let source = ClientCredentialsSource::new(
        issuer_url,
        "client-id".to_string(),
        Some("secret".to_string()),
        ClientAuthMethod::ClientSecretBasic,
        vec!["scope".to_string()],
        None,
        None,
    )
    .unwrap();

    let err = TokenSource::fetch_token(&source).await.unwrap_err();
    let Error::Runtime { message } = &err else {
        panic!("expected runtime error, got {err:?}");
    };
    assert!(message.contains("redirect"));
    // The insecure redirect target must never be contacted.
    assert!(!message.contains("idp.example.com"));
    server.await.unwrap();
}

#[tokio::test]
async fn test_malformed_token_response_error_does_not_leak_body() {
    let (issuer_url, server) = spawn_malformed_token_server().await;
    let source = ClientCredentialsSource::new(
        issuer_url,
        "client-id".to_string(),
        Some("secret".to_string()),
        ClientAuthMethod::ClientSecretBasic,
        vec!["scope".to_string()],
        None,
        None,
    )
    .unwrap();

    let err = TokenSource::fetch_token(&source).await.unwrap_err();
    let message = format!("{err:?}");
    assert!(!message.contains("leak-marker"));
    assert!(matches!(
        err,
        Error::Runtime { message }
            if message.contains("could not be parsed") || message.contains("Content-Type")
    ));
    server.await.unwrap();
}
