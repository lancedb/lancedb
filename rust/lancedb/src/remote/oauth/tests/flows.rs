// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[tokio::test]
async fn test_target_parameters_across_oauth_flows() {
    for (resource, audience) in [
        (None, None),
        (Some("https://api.example.com/a?x=1&y=two"), None),
        (None, Some("audience + & / ü")),
        (Some("urn:example:resource"), Some("audience + & / ü")),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let issuer = format!("http://{addr}");
        let server = tokio::spawn(async move {
            let mut grants = Vec::new();
            // Three discovery requests and six form submissions.
            for _ in 0..9 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut stream).await;
                let response = if request.line.starts_with("GET ") {
                    serde_json::json!({
                        "token_endpoint": format!("http://{addr}/token"),
                        "authorization_endpoint": format!("http://{addr}/authorize"),
                        "device_authorization_endpoint": format!("http://{addr}/device"),
                    })
                } else {
                    let params: Vec<_> =
                        url::form_urlencoded::parse(request.body.as_bytes()).collect();
                    for (key, expected) in [("resource", resource), ("audience", audience)] {
                        let values: Vec<_> = params
                            .iter()
                            .filter(|(name, _)| name == key)
                            .map(|(_, value)| value.as_ref())
                            .collect();
                        assert_eq!(values, expected.into_iter().collect::<Vec<_>>());
                    }
                    if request.line.starts_with("POST /device ") {
                        serde_json::json!({
                            "device_code": "device-code", "user_code": "ABCD",
                            "verification_uri": format!("http://{addr}/verify"),
                            "expires_in": 60, "interval": 1,
                        })
                    } else {
                        grants.push(
                            params
                                .iter()
                                .find(|(key, _)| key == "grant_type")
                                .unwrap()
                                .1
                                .to_string(),
                        );
                        serde_json::json!({"access_token": "access", "refresh_token": "refresh", "expires_in": 3600})
                    }
                };
                write_json_response(&mut stream, "200 OK", &response.to_string()).await;
            }
            assert_eq!(
                grants,
                [
                    "client_credentials",
                    "authorization_code",
                    "refresh_token",
                    "urn:ietf:params:oauth:grant-type:device_code",
                    "refresh_token"
                ]
            );
        });
        let credentials = ClientCredentialsSource::new(
            issuer.clone(),
            "client".into(),
            Some("secret".into()),
            ClientAuthMethod::ClientSecretBasic,
            vec!["scope".into()],
            resource.map(str::to_owned),
            audience.map(str::to_owned),
        )
        .unwrap();
        credentials.fetch_token().await.unwrap();
        let browser = AuthorizationCodeSource::new(
            issuer.clone(),
            "client".into(),
            None,
            ClientAuthMethod::None,
            vec!["scope".into()],
            resource.map(str::to_owned),
            audience.map(str::to_owned),
            AuthorizationCodeOptions::new(),
        )
        .unwrap();
        let request = browser.build_authorization_request().await.unwrap();
        for (key, expected) in [("resource", resource), ("audience", audience)] {
            let values: Vec<_> = request
                .url
                .query_pairs()
                .filter(|(name, _)| name == key)
                .map(|(_, value)| value.into_owned())
                .collect();
            assert_eq!(
                values,
                expected.into_iter().map(str::to_owned).collect::<Vec<_>>()
            );
        }
        browser
            .exchange_code("code", Some(PkceCodeVerifier::new("verifier".to_string())))
            .await
            .unwrap();
        browser.refresh_token("refresh").await.unwrap();
        let device = DeviceCodeSource::new(
            issuer,
            "client".into(),
            None,
            ClientAuthMethod::None,
            vec!["scope".into()],
            resource.map(str::to_owned),
            audience.map(str::to_owned),
        )
        .unwrap();
        let authorization = device.request_device_authorization().await.unwrap();
        device.poll_for_token(&authorization).await.unwrap();
        device.refresh_token("refresh").await.unwrap();
        server.await.unwrap();
    }
}

#[test]
fn test_managed_identity_rejects_target_parameters() {
    for (resource, audience) in [(Some("urn:resource"), None), (None, Some("audience"))] {
        let config = OAuthConfig {
            issuer_url: "https://issuer.example.com".into(),
            client_id: "client".into(),
            client_secret: None,
            scopes: vec!["api://app/.default".into()],
            resource: resource.map(str::to_owned),
            audience: audience.map(str::to_owned),
            flow: OAuthFlow::AzureManagedIdentity { client_id: None },
            client_auth_method: None,
            refresh_buffer_secs: None,
            token_cache: None,
        };
        let error = OAuthHeaderProvider::new(config).unwrap_err().to_string();
        assert!(error.contains("resource and audience are not supported for AzureManagedIdentity"));
    }
}

#[test]
fn test_token_state_expiry() {
    let mut state = TokenState::new();
    assert!(state.is_expired(Duration::from_secs(0)));

    state.access_token = Some("tok".to_string());
    state.expires_at = Some(Instant::now() + Duration::from_secs(600));
    assert!(!state.is_expired(Duration::from_secs(300)));
    assert!(state.is_expired(Duration::from_secs(601)));

    state.expires_at = None;
    assert!(state.is_expired(Duration::from_secs(0)));
}

#[test]
fn test_token_state_uses_default_expiry() {
    let mut state = TokenState::new();
    state.update(&token_response("tok", None, None));

    assert!(!state.is_expired(Duration::from_secs(DEFAULT_TOKEN_TTL_SECS - 1)));
    assert!(state.is_expired(Duration::from_secs(DEFAULT_TOKEN_TTL_SECS + 1)));
}

#[test]
fn test_token_state_retains_refresh_token_when_not_rotated() {
    let mut state = TokenState::new();
    state.update(&token_response("token-1", Some("refresh-1"), Some(60)));
    state.update(&token_response("token-2", None, Some(60)));

    assert_eq!(state.refresh_token.as_deref(), Some("refresh-1"));
}

#[test]
fn test_token_response_accepts_float_expires_in() {
    let response: TokenResponse =
        serde_json::from_str(r#"{"access_token":"tok","expires_in":3600.0}"#).unwrap();

    assert_eq!(response.expires_in, Some(3600));
}

#[test]
fn test_token_response_rejects_negative_expires_in() {
    let err = serde_json::from_str::<TokenResponse>(r#"{"access_token":"tok","expires_in":-1}"#)
        .unwrap_err();

    assert!(err.to_string().contains("invalid expires_in value: -1"));
}

#[test]
fn test_token_response_debug_redacts_access_token() {
    let response = TokenResponse {
        access_token: AccessToken::new("secret-token".to_string()),
        refresh_token: Some(RefreshToken::new("secret-refresh-token".to_string())),
        expires_in: Some(3600),
        token_type: Some(BasicTokenType::Bearer),
    };

    let debug = format!("{response:?}");
    assert!(!debug.contains("secret-token"));
    assert!(!debug.contains("secret-refresh-token"));
    assert!(debug.contains("access_token: \"<redacted>\""));
}

#[test]
fn test_client_auth_method_defaults() {
    assert_eq!(
        resolve_client_auth_method(None, Some("secret")).unwrap(),
        ClientAuthMethod::ClientSecretBasic
    );
    assert_eq!(
        resolve_client_auth_method(None, None).unwrap(),
        ClientAuthMethod::None
    );
}

#[test]
fn test_client_auth_method_explicit_values() {
    for method in [
        ClientAuthMethod::None,
        ClientAuthMethod::ClientSecretBasic,
        ClientAuthMethod::ClientSecretPost,
    ] {
        let secret = (method != ClientAuthMethod::None).then_some("secret");
        assert_eq!(
            resolve_client_auth_method(Some(method), secret).unwrap(),
            method
        );
    }
}

#[test]
fn test_client_auth_method_rejects_inconsistent_configuration() {
    let err = resolve_client_auth_method(Some(ClientAuthMethod::None), Some("secret")).unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidInput { message }
            if message == "client_auth_method None cannot be combined with client_secret"
    ));

    for method in [
        ClientAuthMethod::ClientSecretBasic,
        ClientAuthMethod::ClientSecretPost,
    ] {
        let err = resolve_client_auth_method(Some(method), None).unwrap_err();
        assert!(matches!(
            err,
            Error::InvalidInput { message }
                if message == format!("client_auth_method {method:?} requires client_secret to be set")
        ));
    }
}

#[test]
fn test_oauth_transport_requires_https_except_for_loopback() {
    assert!(validate_oauth_url("https://idp.example.com/token", "endpoint").is_ok());
    assert!(validate_oauth_url("http://localhost:8080/token", "endpoint").is_ok());
    assert!(validate_oauth_url("http://127.0.0.1:8080/token", "endpoint").is_ok());
    assert!(validate_oauth_url("http://[::1]:8080/token", "endpoint").is_ok());

    let err = validate_oauth_url("http://idp.example.com/token", "endpoint").unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidInput { message }
            if message == "OAuth endpoint must use https, except for http on a loopback host"
    ));
}

#[test]
fn test_interactive_prompts_use_default_visible_output() {
    let authorization_url = Url::parse("https://idp.example.com/authorize?state=abc").unwrap();
    let authorization = authorization_prompt(&authorization_url);
    let device = device_prompt("https://idp.example.com/device", "ABCD-EFGH");
    let mut output = Vec::new();

    write_oauth_prompt(&mut output, &authorization);
    write_oauth_prompt(&mut output, &device);

    let output = String::from_utf8(output).unwrap();
    assert!(output.contains(authorization_url.as_str()));
    assert!(output.contains("https://idp.example.com/device"));
    assert!(output.contains("ABCD-EFGH"));
}

#[test]
fn test_authorization_code_options_default_to_pkce() {
    let options = AuthorizationCodeOptions::new();

    assert!(options.use_pkce);
    assert!(options.redirect_uri.is_none());
    assert!(options.callback_port.is_none());
}

#[test]
fn test_authorization_redirect_defaults_to_ipv4_loopback() {
    let redirect = ResolvedRedirect::new(&AuthorizationCodeOptions::new()).unwrap();

    assert_eq!(
        redirect.uri,
        format!("http://127.0.0.1:{DEFAULT_CALLBACK_PORT}/callback")
    );
    assert!(redirect.bind_addr.ip().is_loopback());
    assert_eq!(redirect.bind_addr.port(), DEFAULT_CALLBACK_PORT);
    assert_eq!(redirect.callback_path, "/callback");
}

#[test]
fn test_authorization_redirect_rejects_non_loopback_host() {
    let options =
        AuthorizationCodeOptions::new().redirect_uri("https://client.example.com/oauth/callback");

    let err = ResolvedRedirect::new(&options).unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidInput { message }
            if message == "OAuth redirect_uri must use http with a loopback host"
    ));
}

#[test]
fn test_authorization_redirect_rejects_mismatched_port() {
    let options = AuthorizationCodeOptions::new()
        .redirect_uri("http://127.0.0.1:8401/callback")
        .callback_port(8400);

    let err = ResolvedRedirect::new(&options).unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidInput { message }
            if message.contains("does not match redirect_uri port")
    ));
}

#[test]
fn test_authorization_redirect_requires_explicit_port() {
    let options = AuthorizationCodeOptions::new().redirect_uri("http://127.0.0.1/callback");

    let err = ResolvedRedirect::new(&options).unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidInput { message }
            if message == "OAuth redirect_uri must include a port"
    ));
}

#[test]
fn test_authorization_redirect_accepts_ipv6_loopback() {
    let options = AuthorizationCodeOptions::new().redirect_uri("http://[::1]:8400/callback");

    let redirect = ResolvedRedirect::new(&options).unwrap();
    assert_eq!(
        redirect.bind_addr,
        "[::1]:8400".parse::<SocketAddr>().unwrap()
    );
}
