// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;
use axum::{
    extract::{Form, Query},
    http::{HeaderMap, StatusCode},
    response::Redirect,
    routing::{get, post},
    Json, Router,
};
use base64::Engine;
use std::fs;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

struct Provider {
    config: OidcConfig,
    requests: Arc<Mutex<Vec<HashMap<String, String>>>>,
    server: tokio::task::JoinHandle<()>,
    dir: TempDir,
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Provider {
    async fn start(flow: OidcFlow, lifetime: u64) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        let secret_file = dir.path().join("secret");
        fs::write(&secret_file, "test-secret\n").unwrap();
        let config = OidcConfig {
            client_auth_method: None,
            client_secret_file: (flow == OidcFlow::ClientCredentials).then_some(secret_file),
            flow,
            issuer_url: issuer.clone(),
            client_id: "cli-client".into(),
            scopes: vec!["openid".into(), "offline_access".into()],
            resource: Some("https://api.example.com/a?x=1&y=two".into()),
            audience: Some("lancedb + audience".into()),
            redirect_uri: None,
            callback_port: None,
            refresh_buffer_secs: Some(0),
        };
        let metadata = serde_json::json!({
            "issuer": issuer,
            "token_endpoint": format!("{issuer}/token"),
            "authorization_endpoint": format!("{issuer}/authorize"),
            "device_authorization_endpoint": format!("{issuer}/device"),
        });
        let device = serde_json::json!({
            "device_code": "device-code", "user_code": "CLI-CODE",
            "verification_uri": format!("{issuer}/verify"), "expires_in": 60, "interval": 1,
        });
        let requests = Arc::new(Mutex::new(Vec::new()));
        let device_requests = requests.clone();
        let auth_requests = requests.clone();
        let token_requests = requests.clone();
        let app = Router::new()
            .route("/.well-known/openid-configuration", get(move || {
                let metadata = metadata.clone();
                async move { Json(metadata) }
            }))
            .route("/verify", get(|| async { "verified" }))
            .route("/device", post(move |Form(form): Form<HashMap<String, String>>| {
                let device = device.clone();
                device_requests.lock().unwrap().push(form);
                async move { Json(device) }
            }))
            .route("/authorize", get(move |Query(form): Query<HashMap<String, String>>| {
                assert_eq!(form["code_challenge_method"], "S256");
                let mut redirect = url::Url::parse(&form["redirect_uri"]).unwrap();
                redirect.query_pairs_mut().append_pair("code", "auth-code").append_pair("state", &form["state"]);
                auth_requests.lock().unwrap().push(form);
                async move { Redirect::to(redirect.as_str()) }
            }))
            .route("/token", post(move |headers: HeaderMap, Form(form): Form<HashMap<String, String>>| {
                let mut requests = token_requests.lock().unwrap();
                requests.push(form.clone());
                let number = requests.iter().filter(|request| request.contains_key("grant_type")).count();
                async move {
                    if form.get("grant_type").is_some_and(|grant| grant == "client_credentials")
                        && headers.get("authorization").and_then(|value| value.to_str().ok())
                            != Some(format!("Basic {}", base64::engine::general_purpose::STANDARD.encode("cli-client:test-secret")).as_str()) {
                        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({
                            "error": "invalid_client", "error_description": "private-token test-secret invalid-secret"
                        })));
                    }
                    (StatusCode::OK, Json(serde_json::json!({
                        "access_token": format!("token-{number}"), "token_type": "Bearer", "expires_in": lifetime,
                        "refresh_token": "private-refresh-token",
                    })))
                }
            }));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            config,
            requests,
            server,
            dir,
        }
    }

    fn managed(&self) -> ManagedOidc {
        ManagedOidc::new(self.config.clone(), self.dir.path().join("cache")).unwrap()
    }

    fn assert_targets(&self) {
        for request in self.requests.lock().unwrap().iter() {
            assert_eq!(request.get("resource"), self.config.resource.as_ref());
            assert_eq!(request.get("audience"), self.config.audience.as_ref());
        }
    }

    async fn child(&self, action: &str) -> std::process::Output {
        let executable = std::env::current_exe().unwrap();
        let mut command = tokio::process::Command::new(&executable);
        command
            .args([
                "--exact",
                "auth::tests::session_process_helper",
                "--nocapture",
            ])
            .env("LANCEDB_TEST_AUTH_ACTION", action)
            .env(
                "LANCEDB_TEST_AUTH_CONFIG",
                serde_json::to_string(&self.config).unwrap(),
            )
            .env("LANCEDB_TEST_AUTH_CACHE", self.dir.path().join("cache"))
            .env_remove(CLIENT_SECRET_ENV);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let browser = self.dir.path().join("browser");
            fs::write(&browser, "#!/bin/sh\nLANCEDB_TEST_BROWSER_URL=\"$1\" exec \"$LANCEDB_TEST_AUTH_EXE\" --exact auth::tests::browser_process_helper --nocapture\n").unwrap();
            fs::set_permissions(&browser, fs::Permissions::from_mode(0o700)).unwrap();
            command
                .env("LANCEDB_OAUTH_BROWSER", browser)
                .env("LANCEDB_TEST_AUTH_EXE", executable);
        }
        let output = command.output().await.unwrap();
        assert!(
            output.status.success(),
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let output_text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!output_text.contains("private-refresh-token"));
        output
    }
}

#[tokio::test]
async fn sdk_provider_is_shared_and_renews_service_tokens_in_memory() {
    let fixture = Provider::start(OidcFlow::ClientCredentials, 1).await;
    let authentication = Authentication::ManagedOidc(Arc::new(fixture.managed()));
    let cloned = authentication.clone();
    let (first, second) = tokio::join!(authentication.headers(), cloned.headers());
    assert_eq!(first.unwrap()["authorization"], "Bearer token-1");
    assert_eq!(second.unwrap()["authorization"], "Bearer token-1");
    assert_eq!(fixture.requests.lock().unwrap().len(), 1);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let headers = authentication.headers().await.unwrap();
    assert_eq!(headers["authorization"], "Bearer token-2");
    assert_eq!(headers[OIDC_CREDENTIAL_HEADER], "oidc");
    assert!(headers["authorization"].is_sensitive());
    assert!(!format!("{headers:?}").contains("token-2"));
    assert!(!fixture.dir.path().join("cache").exists());
    fixture.assert_targets();
}

#[tokio::test]
async fn sdk_provider_errors_do_not_expose_response_bodies() {
    let fixture = Provider::start(OidcFlow::ClientCredentials, 3600).await;
    fs::write(
        fixture.config.client_secret_file.as_ref().unwrap(),
        "invalid-secret",
    )
    .unwrap();
    let error = fixture.managed().headers().await.unwrap_err();
    let rendered = format!("{error:#} {error:?}");
    assert!(rendered.contains("OIDC authentication failed"));
    for secret in ["private-token", "test-secret", "invalid-secret"] {
        assert!(!rendered.contains(secret));
    }
}

#[tokio::test]
async fn missing_secret_does_not_fall_back() {
    let fixture = Provider::start(OidcFlow::ClientCredentials, 3600).await;
    fs::remove_file(fixture.config.client_secret_file.as_ref().unwrap()).unwrap();
    assert!(fixture.managed().headers().await.is_err());
    assert!(fixture.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sdk_provider_reads_updated_external_token_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    fs::write(&path, "first-token\n").unwrap();
    let provider = Authentication::OidcAccessTokenFile(path.clone());
    assert_eq!(
        provider.get_headers().await.unwrap()["authorization"],
        "Bearer first-token"
    );
    fs::write(&path, "second-token\n").unwrap();
    assert_eq!(
        provider.get_headers().await.unwrap()["authorization"],
        "Bearer second-token"
    );
    fs::write(&path, " \n").unwrap();
    assert!(provider.get_headers().await.is_err());
    fs::write(&path, "token\r\ninjected: header").unwrap();
    assert!(provider.get_headers().await.is_err());
}

#[tokio::test]
async fn profile_client_auth_method_round_trips_and_reaches_sdk() {
    let profile = r#"
flow = "authorization_code"
issuer_url = "http://localhost:18888/realms/lancedb"
client_id = "cli-client"
scopes = ["openid"]
"#;
    for (value, expected) in [
        (None, None),
        (Some("none"), Some(ClientAuthMethod::None)),
        (
            Some("client_secret_basic"),
            Some(ClientAuthMethod::ClientSecretBasic),
        ),
        (
            Some("client_secret_post"),
            Some(ClientAuthMethod::ClientSecretPost),
        ),
    ] {
        let text = match value {
            Some(value) => format!("{profile}\nclient_auth_method = {value:?}\n"),
            None => profile.to_owned(),
        };
        let config: OidcConfig = toml::from_str(&text).unwrap();
        let serialized = toml::to_string(&config).unwrap();
        assert_eq!(serialized.contains("client_auth_method"), value.is_some());
        assert_eq!(toml::from_str::<OidcConfig>(&serialized).unwrap(), config);
        let dir = tempfile::tempdir().unwrap();
        let managed = ManagedOidc::new(config, dir.path().join("cache")).unwrap();
        assert_eq!(
            managed.sdk_config().await.unwrap().client_auth_method,
            expected
        );
    }
    assert!(toml::from_str::<OidcConfig>(&format!(
        "{profile}\nclient_auth_method = \"unsupported\"\n"
    ))
    .is_err());
}

#[tokio::test]
async fn sdk_rejects_public_client_auth_with_a_secret_before_requesting_tokens() {
    let mut fixture = Provider::start(OidcFlow::ClientCredentials, 3600).await;
    fixture.config.client_auth_method = Some(OidcClientAuthMethod::None);
    assert!(fixture.managed().provider().await.is_err());
    assert!(fixture.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sdk_config_preserves_profile_options() {
    let mut fixture = Provider::start(OidcFlow::AuthorizationCode, 3600).await;
    fixture.config.callback_port = Some(9123);
    fixture.config.redirect_uri = Some("http://127.0.0.1:9123/cli".into());
    let config = fixture.managed().sdk_config().await.unwrap();
    assert_eq!(config.resource, fixture.config.resource);
    assert_eq!(config.audience, fixture.config.audience);
    assert_eq!(config.scopes, fixture.config.scopes);
    assert_eq!(config.refresh_buffer_secs, Some(0));
    assert_eq!(
        config.token_cache.unwrap().cache_dir,
        Some(fixture.dir.path().join("cache"))
    );
    let OAuthFlow::AuthorizationCode(options) = config.flow else {
        panic!("expected browser flow")
    };
    assert!(options.use_pkce);
    assert_eq!(options.callback_port, Some(9123));
    assert_eq!(options.redirect_uri, fixture.config.redirect_uri);
    fixture.config.flow = OidcFlow::DeviceCode;
    assert!(ManagedOidc::new(fixture.config.clone(), fixture.dir.path().join("cache")).is_err());
}

#[tokio::test]
async fn profile_caches_are_isolated_without_creating_directories() {
    let fixture = Provider::start(OidcFlow::DeviceCode, 3600).await;
    let first = ManagedOidc::for_profile("a/../b", fixture.config.clone()).unwrap();
    let second = ManagedOidc::for_profile("b", fixture.config.clone()).unwrap();
    assert_ne!(first.cache_dir, second.cache_dir);
    assert_eq!(first.cache_dir.parent(), second.cache_dir.parent());
    assert_eq!(first.cache_dir.file_name().unwrap().len(), 64);
}

#[cfg(unix)]
#[tokio::test]
async fn sdk_interactive_sessions_survive_processes_and_logout() {
    for flow in [OidcFlow::AuthorizationCode, OidcFlow::DeviceCode] {
        let mut fixture = Provider::start(flow, 3600).await;
        if fixture.config.flow == OidcFlow::AuthorizationCode {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            fixture.config.callback_port = Some(listener.local_addr().unwrap().port());
        }
        // Ordinary requests trigger acquisition, without an explicit login first.
        fixture.child("fetch").await;
        let original_requests = fixture.requests.lock().unwrap().len();
        fixture.child("fetch").await;
        {
            let requests = fixture.requests.lock().unwrap();
            assert_eq!(requests.len(), original_requests + 1);
            assert_eq!(requests.last().unwrap()["grant_type"], "refresh_token");
        }
        fixture.child("status").await;
        fixture.child("logout").await;
        assert!(
            !fixture
                .managed()
                .session()
                .await
                .unwrap()
                .status()
                .await
                .unwrap()
                .refreshable
        );
        fixture.child("login").await;
        fixture.child("status").await;
        fixture.assert_targets();
    }
}

#[tokio::test]
async fn session_process_helper() {
    let Ok(action) = std::env::var("LANCEDB_TEST_AUTH_ACTION") else {
        return;
    };
    let config = serde_json::from_str(&std::env::var("LANCEDB_TEST_AUTH_CONFIG").unwrap()).unwrap();
    let managed = ManagedOidc::new(
        config,
        std::env::var_os("LANCEDB_TEST_AUTH_CACHE").unwrap().into(),
    )
    .unwrap();
    match action.as_str() {
        "fetch" => {
            managed.headers().await.unwrap();
        }
        "login" => {
            assert!(
                managed
                    .session()
                    .await
                    .unwrap()
                    .login()
                    .await
                    .unwrap()
                    .refreshable
            );
        }
        "status" => {
            assert!(
                managed
                    .session()
                    .await
                    .unwrap()
                    .status()
                    .await
                    .unwrap()
                    .refreshable
            );
        }
        "logout" => {
            assert!(
                managed
                    .session()
                    .await
                    .unwrap()
                    .logout()
                    .await
                    .unwrap()
                    .removed
            );
        }
        _ => panic!("unknown action"),
    }
}

#[tokio::test]
async fn browser_process_helper() {
    let Ok(url) = std::env::var("LANCEDB_TEST_BROWSER_URL") else {
        return;
    };
    reqwest::Client::new()
        .get(url)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}
