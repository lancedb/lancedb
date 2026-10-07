// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

fn test_config(flow: OAuthFlow, client_secret: Option<String>) -> OAuthConfig {
    OAuthConfig {
        issuer_url: "https://issuer.example.com".to_string(),
        client_id: "client-id".to_string(),
        client_secret,
        scopes: vec!["scope".to_string()],
        resource: None,
        audience: None,
        flow,
        client_auth_method: None,
        refresh_buffer_secs: None,
        token_cache: None,
    }
}

#[test]
fn test_oauth_config_debug_redacts_client_secret() {
    let mut config = test_config(
        OAuthFlow::ClientCredentials,
        Some("super-secret".to_string()),
    );
    config.client_auth_method = Some(ClientAuthMethod::ClientSecretBasic);

    let debug = format!("{config:?}");
    assert!(!debug.contains("super-secret"));
    assert!(debug.contains("client_secret: Some(\"<redacted>\")"));
    assert!(debug.contains("client_auth_method"));
}

#[test]
fn test_oauth_header_provider_debug_redacts_client_secret() {
    let config = test_config(
        OAuthFlow::ClientCredentials,
        Some("super-secret".to_string()),
    );

    let provider = OAuthHeaderProvider::new(config).unwrap();
    let debug = format!("{provider:?}");
    assert!(!debug.contains("super-secret"));
    assert!(debug.contains("client_secret: Some(\"<redacted>\")"));
}

#[test]
fn test_managed_identity_resource_from_default_scope() {
    assert_eq!(
        AzureImdsSource::resource_from_scopes(&["api://test/.default".to_string()]).unwrap(),
        "api://test"
    );
}

#[test]
fn test_managed_identity_resource_without_default_suffix() {
    assert_eq!(
        AzureImdsSource::resource_from_scopes(&["api://test".to_string()]).unwrap(),
        "api://test"
    );
}

#[test]
fn test_managed_identity_rejects_multiple_scopes() {
    let config = OAuthConfig {
        issuer_url: "https://login.microsoftonline.com/tenant/v2.0".to_string(),
        client_id: "app-id".to_string(),
        client_secret: None,
        scopes: vec![
            "api://test-a/.default".to_string(),
            "api://test-b/.default".to_string(),
        ],
        resource: None,
        audience: None,
        flow: OAuthFlow::AzureManagedIdentity { client_id: None },
        client_auth_method: None,
        refresh_buffer_secs: None,
        token_cache: None,
    };
    assert!(OAuthHeaderProvider::new(config).is_err());
}

#[tokio::test]
async fn test_token_endpoint_requires_discovery_success() {
    let (issuer_url, server) = spawn_discovery_error_server().await;
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

    let err = source.oidc.get_token_endpoint().await.unwrap_err();
    assert!(matches!(
        err,
        Error::Runtime { message }
            if message.contains("OIDC discovery failed with status 503")
    ));
    server.await.unwrap();
}

#[test]
fn test_client_credentials_requires_secret() {
    let config = test_config(OAuthFlow::ClientCredentials, None);
    assert!(OAuthHeaderProvider::new(config).is_err());
}

#[test]
fn test_client_credentials_rejects_insecure_non_loopback_issuer() {
    let mut config = test_config(OAuthFlow::ClientCredentials, Some("secret".to_string()));
    config.issuer_url = "http://issuer.example.com".to_string();

    let err = OAuthHeaderProvider::new(config).unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidInput { message }
            if message
                == "OAuth issuer_url must use https, except for http on a loopback host"
    ));
}

#[test]
fn test_empty_scopes_rejected() {
    let mut config = test_config(OAuthFlow::AzureManagedIdentity { client_id: None }, None);
    config.scopes = vec![];
    assert!(OAuthHeaderProvider::new(config).is_err());
}

#[tokio::test]
async fn test_client_credentials_token_lifecycle() {
    let (issuer_url, token_requests, server) = spawn_oauth_server().await;
    let config = OAuthConfig {
        issuer_url,
        client_id: "client-id".to_string(),
        client_secret: Some("secret".to_string()),
        scopes: vec!["scope".to_string()],
        resource: None,
        audience: None,
        flow: OAuthFlow::ClientCredentials,
        client_auth_method: None,
        refresh_buffer_secs: Some(0),
        token_cache: None,
    };
    let provider = OAuthHeaderProvider::new(config).unwrap();

    let headers = provider.get_headers().await.unwrap();
    assert_eq!(headers.get("authorization").unwrap(), "Bearer token-1");
    assert_eq!(token_requests.load(Ordering::SeqCst), 1);

    let headers = provider.get_headers().await.unwrap();
    assert_eq!(headers.get("authorization").unwrap(), "Bearer token-1");
    assert_eq!(token_requests.load(Ordering::SeqCst), 1);

    provider.token_state.write().await.expires_at = Some(Instant::now() - Duration::from_secs(1));

    let headers = provider.get_headers().await.unwrap();
    assert_eq!(headers.get("authorization").unwrap(), "Bearer token-2");
    assert_eq!(token_requests.load(Ordering::SeqCst), 2);

    server.await.unwrap();
}

#[tokio::test]
async fn test_client_credentials_supports_client_secret_post() {
    let (issuer_url, request, server) = spawn_captured_token_server().await;
    let config = OAuthConfig {
        issuer_url,
        client_id: "client-id".to_string(),
        client_secret: Some("secret".to_string()),
        scopes: vec!["scope".to_string()],
        resource: None,
        audience: None,
        flow: OAuthFlow::ClientCredentials,
        client_auth_method: Some(ClientAuthMethod::ClientSecretPost),
        refresh_buffer_secs: None,
        token_cache: None,
    };
    let provider = OAuthHeaderProvider::new(config).unwrap();

    provider.get_headers().await.unwrap();

    let request = request.lock().unwrap().take().unwrap();
    assert_eq!(request.header("authorization"), None);
    assert!(request.body.contains("grant_type=client_credentials"));
    assert!(request.body.contains("client_id=client-id"));
    assert!(request.body.contains("client_secret=secret"));
    assert!(request.body.contains("scope=scope"));
    server.await.unwrap();
}

async fn spawn_oauth_server() -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let issuer_url = format!("http://{addr}");
    let token_requests = Arc::new(AtomicUsize::new(0));
    let server_token_requests = Arc::clone(&token_requests);

    let server = tokio::spawn(async move {
        for _ in 0..3 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;

            if request
                .line
                .starts_with("GET /.well-known/openid-configuration ")
            {
                let discovery = format!(r#"{{"token_endpoint":"http://{addr}/token"}}"#);
                write_json_response(&mut stream, "200 OK", &discovery).await;
            } else if request.line.starts_with("POST /token ") {
                assert_eq!(
                    request.header("authorization").as_deref(),
                    Some(basic_authorization("client-id", "secret").as_str())
                );
                assert!(request.body.contains("grant_type=client_credentials"));
                assert!(request.body.contains("scope=scope"));
                assert!(!request.body.contains("client_secret"));
                assert!(!request.body.contains("client_id"));

                let token_num = server_token_requests.fetch_add(1, Ordering::SeqCst) + 1;
                let token = format!(
                    r#"{{"access_token":"token-{token_num}","expires_in":3600,"token_type":"Bearer"}}"#
                );
                write_json_response(&mut stream, "200 OK", &token).await;
            } else {
                write_json_response(&mut stream, "404 Not Found", "{}").await;
            }
        }
    });

    (issuer_url, token_requests, server)
}

async fn spawn_discovery_error_server() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let issuer_url = format!("http://{addr}");

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_http_request(&mut stream).await;
        assert!(
            request
                .line
                .starts_with("GET /.well-known/openid-configuration ")
        );
        write_json_response(&mut stream, "503 Service Unavailable", "{}").await;
    });

    (issuer_url, server)
}
