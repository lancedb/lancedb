// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::remote::HeaderProvider;
use crate::remote::oauth::OAuthHeaderProvider;
use oauth2::basic::BasicTokenType;
use oauth2::{AccessToken, RefreshToken};
use serial_test::serial;

/// Temp directory that satisfies the cache hardening checks. CI runners
/// can create temp directories with group/other bits set, which the
/// private-directory validation correctly rejects.
fn cache_tempdir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    dir
}

fn device_config(cache_dir: &Path) -> OAuthConfig {
    OAuthConfig {
        issuer_url: "https://issuer.example.com".to_string(),
        client_id: "client-id".to_string(),
        client_secret: None,
        client_auth_method: None,
        scopes: vec!["openid".to_string()],
        flow: OAuthFlow::DeviceCode,
        refresh_buffer_secs: None,
        resource: None,
        audience: None,
        token_cache: Some(TokenCacheOptions::new().cache_dir(cache_dir)),
    }
}

/// Stateful mock IdP covering discovery, device authorization, device
/// polling, client credentials, and refresh with strict rotation: a
/// refresh token that is not the currently issued one is rejected with
/// `invalid_grant`, which is exactly what real providers do on rotation.
struct MockIdp {
    issuer_url: String,
    requests: Arc<std::sync::Mutex<Vec<String>>>,
    device_authorizations: Arc<AtomicUsize>,
    refresh_attempts: Arc<AtomicUsize>,
    invalid_grant_rejections: Arc<AtomicUsize>,
    access_tokens_issued: Arc<AtomicUsize>,
    current_refresh: Arc<std::sync::Mutex<Option<String>>>,
    fail_refreshes: Arc<AtomicBool>,
    issue_refresh_tokens: Arc<AtomicBool>,
}

impl MockIdp {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let issuer_url = format!("http://{addr}");
        let server = Self {
            issuer_url: issuer_url.clone(),
            requests: Arc::new(std::sync::Mutex::new(Vec::new())),
            device_authorizations: Arc::new(AtomicUsize::new(0)),
            refresh_attempts: Arc::new(AtomicUsize::new(0)),
            invalid_grant_rejections: Arc::new(AtomicUsize::new(0)),
            access_tokens_issued: Arc::new(AtomicUsize::new(0)),
            current_refresh: Arc::new(std::sync::Mutex::new(None)),
            fail_refreshes: Arc::new(AtomicBool::new(false)),
            issue_refresh_tokens: Arc::new(AtomicBool::new(true)),
        };
        let requests = Arc::clone(&server.requests);
        let device_authorizations = Arc::clone(&server.device_authorizations);
        let refresh_attempts = Arc::clone(&server.refresh_attempts);
        let invalid_grant_rejections = Arc::clone(&server.invalid_grant_rejections);
        let access_tokens_issued = Arc::clone(&server.access_tokens_issued);
        let current_refresh = Arc::clone(&server.current_refresh);
        let fail_refreshes = Arc::clone(&server.fail_refreshes);
        let issue_refresh_tokens = Arc::clone(&server.issue_refresh_tokens);

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let requests = Arc::clone(&requests);
                let device_authorizations = Arc::clone(&device_authorizations);
                let refresh_attempts = Arc::clone(&refresh_attempts);
                let invalid_grant_rejections = Arc::clone(&invalid_grant_rejections);
                let access_tokens_issued = Arc::clone(&access_tokens_issued);
                let current_refresh = Arc::clone(&current_refresh);
                let fail_refreshes = Arc::clone(&fail_refreshes);
                let issue_refresh_tokens = Arc::clone(&issue_refresh_tokens);
                tokio::spawn(async move {
                    let (request_line, body) = read_http_request(&mut stream).await;
                    if request_line.starts_with("POST ") {
                        requests.lock().unwrap().push(body.clone());
                    }
                    if request_line.starts_with("GET /.well-known/openid-configuration ") {
                        let discovery = format!(
                            r#"{{"token_endpoint":"http://{addr}/token","device_authorization_endpoint":"http://{addr}/device"}}"#
                        );
                        write_json_response(&mut stream, "200 OK", &discovery).await;
                    } else if request_line.starts_with("POST /device ") {
                        device_authorizations.fetch_add(1, Ordering::SeqCst);
                        let device = format!(
                            r#"{{"device_code":"device-code","user_code":"ABCD-EFGH","verification_uri":"http://{addr}/verify","expires_in":60,"interval":1}}"#
                        );
                        write_json_response(&mut stream, "200 OK", &device).await;
                    } else if request_line.starts_with("POST /token ") {
                        if body.contains("grant_type=refresh_token") {
                            refresh_attempts.fetch_add(1, Ordering::SeqCst);
                            if fail_refreshes.load(Ordering::SeqCst) {
                                write_json_response(
                                    &mut stream,
                                    "503 Service Unavailable",
                                    r#"{"error":"temporarily_unavailable"}"#,
                                )
                                .await;
                                return;
                            }
                            let expected = current_refresh.lock().unwrap().clone();
                            let matched = body
                                .split('&')
                                .find_map(|pair| pair.strip_prefix("refresh_token="))
                                .map(|token| token.to_string())
                                .zip(expected)
                                .is_some_and(|(offered, expected)| offered == expected);
                            if !matched {
                                invalid_grant_rejections.fetch_add(1, Ordering::SeqCst);
                                write_json_response(
                                    &mut stream,
                                    "400 Bad Request",
                                    r#"{"error":"invalid_grant"}"#,
                                )
                                .await;
                                return;
                            }
                            issue_access_token(
                                &mut stream,
                                &access_tokens_issued,
                                &current_refresh,
                                issue_refresh_tokens.load(Ordering::SeqCst),
                            )
                            .await;
                        } else {
                            // Device polling or client credentials: issue
                            // a token and, for interactive grants, a fresh
                            // refresh token with strict rotation.
                            let grant_device = body.contains(
                                "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Adevice_code",
                            );
                            if grant_device {
                                issue_access_token(
                                    &mut stream,
                                    &access_tokens_issued,
                                    &current_refresh,
                                    issue_refresh_tokens.load(Ordering::SeqCst),
                                )
                                .await;
                            } else {
                                let token = format!(
                                    r#"{{"access_token":"access-{}","expires_in":3600}}"#,
                                    access_tokens_issued.fetch_add(1, Ordering::SeqCst) + 1
                                );
                                write_json_response(&mut stream, "200 OK", &token).await;
                            }
                        }
                    } else {
                        write_json_response(&mut stream, "404 Not Found", "{}").await;
                    }
                });
            }
        });
        server
    }

    fn config(&self, cache_dir: &Path) -> OAuthConfig {
        let mut config = device_config(cache_dir);
        config.issuer_url = self.issuer_url.clone();
        config
    }
}

async fn issue_access_token(
    stream: &mut TcpStream,
    access_tokens_issued: &AtomicUsize,
    current_refresh: &std::sync::Mutex<Option<String>>,
    with_refresh: bool,
) {
    let number = access_tokens_issued.fetch_add(1, Ordering::SeqCst) + 1;
    let token = if with_refresh {
        *current_refresh.lock().unwrap() = Some(format!("refresh-{number}"));
        format!(
            r#"{{"access_token":"access-{number}","refresh_token":"refresh-{number}","expires_in":3600}}"#
        )
    } else {
        format!(r#"{{"access_token":"access-{number}","expires_in":3600}}"#)
    };
    write_json_response(stream, "200 OK", &token).await;
}

async fn read_http_request(stream: &mut TcpStream) -> (String, String) {
    let mut buffer = Vec::new();
    let mut header_end = None;
    while header_end.is_none() {
        let mut chunk = [0; 1024];
        let read = stream.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "connection closed before request headers");
        buffer.extend_from_slice(&chunk[..read]);
        header_end = find_subsequence(&buffer, b"\r\n\r\n").map(|pos| pos + 4);
    }
    let header_end = header_end.unwrap();
    let headers = String::from_utf8_lossy(&buffer[..header_end]).to_string();
    let request_line = headers.lines().next().unwrap_or_default().to_string();
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    while buffer.len() < header_end + content_length {
        let mut chunk = [0; 1024];
        let read = stream.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "connection closed before request body");
        buffer.extend_from_slice(&chunk[..read]);
    }
    let body =
        String::from_utf8_lossy(&buffer[header_end..header_end + content_length]).to_string();
    (request_line, body)
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

async fn write_json_response(stream: &mut TcpStream, status: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await.unwrap();
}

fn suppress_browser() {
    // Point the OAuth browser helper at a no-op so device-flow tests never
    // open a real browser window.
    unsafe { std::env::set_var("LANCEDB_OAUTH_BROWSER", "/usr/bin/true") };
}

#[tokio::test]
#[serial]
async fn test_provider_reuses_cached_session_across_instances() {
    suppress_browser();
    let dir = cache_tempdir();
    let idp = MockIdp::start().await;

    let first = OAuthHeaderProvider::new(idp.config(dir.path())).unwrap();
    let headers = first.get_headers().await.unwrap();
    assert_eq!(headers.get("authorization").unwrap(), "Bearer access-1");

    // A second, independent provider (simulating a second process) must
    // refresh silently instead of starting another device flow.
    let second = OAuthHeaderProvider::new(idp.config(dir.path())).unwrap();
    let headers = second.get_headers().await.unwrap();
    assert_eq!(headers.get("authorization").unwrap(), "Bearer access-2");

    assert_eq!(idp.device_authorizations.load(Ordering::SeqCst), 1);
    assert_eq!(idp.refresh_attempts.load(Ordering::SeqCst), 1);

    let cache = crate::remote::token_cache::TokenCache::new(
        &idp.config(dir.path()),
        &TokenCacheOptions::new().cache_dir(dir.path()),
    )
    .unwrap();
    let record = cache.load().await.unwrap().unwrap();
    assert_eq!(record.refresh_token, "refresh-2");
}

#[tokio::test]
#[serial]
async fn test_concurrent_providers_serialize_rotation() {
    suppress_browser();
    let dir = cache_tempdir();
    let idp = MockIdp::start().await;

    let priming = OAuthHeaderProvider::new(idp.config(dir.path())).unwrap();
    priming.get_headers().await.unwrap();
    assert_eq!(idp.device_authorizations.load(Ordering::SeqCst), 1);

    let provider_a = OAuthHeaderProvider::new(idp.config(dir.path())).unwrap();
    let provider_b = OAuthHeaderProvider::new(idp.config(dir.path())).unwrap();
    let (headers_a, headers_b) = tokio::join!(provider_a.get_headers(), provider_b.get_headers());
    let token_a = headers_a.unwrap().remove("authorization").unwrap();
    let token_b = headers_b.unwrap().remove("authorization").unwrap();
    assert!(
        {
            let mut tokens = [token_a, token_b];
            tokens.sort();
            tokens
        } == ["Bearer access-2".to_string(), "Bearer access-3".to_string()],
        "each provider must observe its own refreshed token"
    );

    // Rotation raced would produce an invalid_grant and a second device
    // flow; the lock prevents both.
    assert_eq!(idp.invalid_grant_rejections.load(Ordering::SeqCst), 0);
    assert_eq!(idp.device_authorizations.load(Ordering::SeqCst), 1);
    assert_eq!(idp.refresh_attempts.load(Ordering::SeqCst), 2);

    let cache = crate::remote::token_cache::TokenCache::new(
        &idp.config(dir.path()),
        &TokenCacheOptions::new().cache_dir(dir.path()),
    )
    .unwrap();
    let record = cache.load().await.unwrap().unwrap();
    assert_eq!(record.refresh_token, "refresh-3");
}

#[tokio::test]
#[serial]
async fn test_transient_refresh_failure_retains_record() {
    suppress_browser();
    let dir = cache_tempdir();
    let idp = MockIdp::start().await;

    let priming = OAuthHeaderProvider::new(idp.config(dir.path())).unwrap();
    priming.get_headers().await.unwrap();

    idp.fail_refreshes.store(true, Ordering::SeqCst);
    let second = OAuthHeaderProvider::new(idp.config(dir.path())).unwrap();
    let error = second.get_headers().await.unwrap_err();
    assert!(error.to_string().contains("503"));

    let session = OAuthSession::new(idp.config(dir.path())).unwrap();
    let status = session.status().await.unwrap();
    assert!(
        status.refreshable,
        "transient failures must keep the record"
    );
}

#[tokio::test]
#[serial]
async fn test_invalid_grant_deletes_record_and_reauthenticates() {
    suppress_browser();
    let dir = cache_tempdir();
    let idp = MockIdp::start().await;

    let priming = OAuthHeaderProvider::new(idp.config(dir.path())).unwrap();
    priming.get_headers().await.unwrap();

    // Simulate a revoked refresh token by replacing the record with one
    // the provider never issued.
    let cache = crate::remote::token_cache::TokenCache::new(
        &idp.config(dir.path()),
        &TokenCacheOptions::new().cache_dir(dir.path()),
    )
    .unwrap();
    let mut record = cache.load().await.unwrap().unwrap();
    record.refresh_token = "revoked-refresh".to_string();
    cache.store(&record).await.unwrap();

    let second = OAuthHeaderProvider::new(idp.config(dir.path())).unwrap();
    let headers = second.get_headers().await.unwrap();
    assert_eq!(headers.get("authorization").unwrap(), "Bearer access-2");

    assert_eq!(idp.invalid_grant_rejections.load(Ordering::SeqCst), 1);
    assert_eq!(idp.device_authorizations.load(Ordering::SeqCst), 2);

    let record = cache.load().await.unwrap().unwrap();
    assert_eq!(record.refresh_token, "refresh-2");
}

#[tokio::test]
#[serial]
async fn test_session_login_status_logout_lifecycle() {
    suppress_browser();
    let dir = cache_tempdir();
    let idp = MockIdp::start().await;

    let session = OAuthSession::new(idp.config(dir.path())).unwrap();
    let status = session.status().await.unwrap();
    assert!(!status.refreshable);

    let status = session.login().await.unwrap();
    assert!(status.refreshable);
    assert_eq!(status.issuer_url, idp.issuer_url);
    assert_eq!(status.client_id, "client-id");
    assert_eq!(status.scopes, vec!["openid".to_string()]);
    assert_eq!(status.flow, "device_code");
    assert!(status.obtained_at.is_some());

    // An independent session manager sees the same cached login.
    let other = OAuthSession::new(idp.config(dir.path())).unwrap();
    assert!(other.status().await.unwrap().refreshable);

    assert!(session.logout().await.unwrap().removed);
    assert!(!session.logout().await.unwrap().removed);
    assert!(!other.status().await.unwrap().refreshable);
    assert_eq!(idp.device_authorizations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[serial]
async fn test_login_without_refresh_token_clears_prior_record() {
    suppress_browser();
    let dir = cache_tempdir();
    let idp = MockIdp::start().await;

    let session = OAuthSession::new(idp.config(dir.path())).unwrap();
    session.login().await.unwrap();
    assert!(session.status().await.unwrap().refreshable);

    // A provider that stops issuing refresh tokens (for example a login
    // without offline_access) must not leave the earlier account behind.
    idp.issue_refresh_tokens.store(false, Ordering::SeqCst);
    let status = session.login().await.unwrap();
    assert!(!status.refreshable);
    assert!(!session.status().await.unwrap().refreshable);
}

#[tokio::test]
async fn test_client_credentials_with_cache_stays_memory_only() {
    let dir = cache_tempdir();
    let idp = MockIdp::start().await;

    let mut config = idp.config(dir.path());
    config.flow = OAuthFlow::ClientCredentials;
    config.client_secret = Some("secret".to_string());

    let provider = OAuthHeaderProvider::new(config).unwrap();
    let headers = provider.get_headers().await.unwrap();
    assert_eq!(headers.get("authorization").unwrap(), "Bearer access-1");
    assert_eq!(dir.path().read_dir().unwrap().count(), 0);
}

#[test]
fn test_managed_identity_with_cache_is_rejected() {
    let dir = cache_tempdir();
    let mut config = device_config(dir.path());
    config.flow = OAuthFlow::AzureManagedIdentity { client_id: None };

    let err = OAuthHeaderProvider::new(config).unwrap_err();
    assert!(
        matches!(err, Error::InvalidInput { message } if message.contains("AzureManagedIdentity"))
    );
}

#[tokio::test]
#[serial]
async fn test_provider_debug_and_status_reveal_no_secrets() {
    suppress_browser();
    let dir = cache_tempdir();
    let idp = MockIdp::start().await;

    let provider = OAuthHeaderProvider::new(idp.config(dir.path())).unwrap();
    let headers = provider.get_headers().await.unwrap();
    assert_eq!(headers.get("authorization").unwrap(), "Bearer access-1");
    let debug = format!("{provider:?}");
    assert!(!debug.contains("access-1"));
    assert!(!debug.contains("refresh-1"));

    let session = OAuthSession::new(idp.config(dir.path())).unwrap();
    let status = session.status().await.unwrap();
    assert!(!format!("{status:?}").contains("refresh-"));
}

#[tokio::test]
async fn test_targeted_cache_refresh_and_logout_isolation() {
    let dir = cache_tempdir();
    let idp = MockIdp::start().await;
    let mut config = idp.config(dir.path());
    let options = config.token_cache.clone().unwrap();
    let untargeted_key = CacheKey::new(&config).unwrap().file_stem;
    assert_eq!(
        untargeted_key,
        hex_sha256(
            format!(
                "v1\n{}\nclient-id\nopenid\ndevice_code\npublic",
                idp.issuer_url
            )
            .as_bytes()
        )
    );
    let mut sessions = Vec::new();
    let mut keys = std::collections::HashSet::new();
    for (resource, audience) in [
        (None, None),
        (Some("urn:one"), None),
        (None, Some("audience & ü")),
        (Some("urn:one"), Some("audience & ü")),
        (Some("urn:two"), Some("audience & ü")),
        (Some("urn:one"), Some("other")),
    ] {
        config.resource = resource.map(str::to_owned);
        config.audience = audience.map(str::to_owned);
        let cache = TokenCache::new(&config, &options).unwrap();
        assert!(keys.insert(cache.key.file_stem.clone()));
        let record = cache
            .record_from_response(&TokenResponse {
                access_token: AccessToken::new("unused".into()),
                refresh_token: Some(RefreshToken::new("seed-refresh".into())),
                expires_in: Some(3600),
                token_type: None,
            })
            .unwrap();
        cache.store(&record).await.unwrap();
        *idp.current_refresh.lock().unwrap() = Some("seed-refresh".into());
        let provider = OAuthHeaderProvider::new(config.clone()).unwrap();
        provider.get_headers().await.unwrap();
        let request = idp.requests.lock().unwrap().last().unwrap().clone();
        let params: std::collections::HashMap<_, _> =
            url::form_urlencoded::parse(request.as_bytes()).collect();
        assert_eq!(params.get("resource").map(|s| s.as_ref()), resource);
        assert_eq!(params.get("audience").map(|s| s.as_ref()), audience);
        assert_eq!(params.get("grant_type").unwrap(), "refresh_token");
        let session = OAuthSession::new(config.clone()).unwrap();
        let status = session.status().await.unwrap();
        assert!(status.refreshable);
        assert_eq!(status.resource, config.resource);
        assert_eq!(status.audience, config.audience);
        sessions.push(session);
    }
    assert!(sessions.pop().unwrap().logout().await.unwrap().removed);
    for session in sessions {
        assert!(session.status().await.unwrap().refreshable);
    }
    assert_eq!(idp.device_authorizations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_legacy_cache_record_without_target_fields() {
    let dir = cache_tempdir();
    let config = device_config(dir.path());
    let cache = TokenCache::new(&config, config.token_cache.as_ref().unwrap()).unwrap();
    let legacy = br#"{"version":1,"issuer_url":"https://issuer.example.com","client_id":"client-id","scopes":["openid"],"flow":"device_code","client_auth":"public","refresh_token":"legacy","obtained_at":1}"#;
    write_record(dir.path(), &cache.record_path(), legacy).unwrap();
    let session = OAuthSession::new(config).unwrap();
    let status = session.status().await.unwrap();
    assert!(status.refreshable);
    assert_eq!(status.resource, None);
    assert_eq!(status.audience, None);
    assert!(session.logout().await.unwrap().removed);
}

#[test]
fn test_cache_key_canonicalizes_scopes_and_issuer() {
    let mut config = device_config(Path::new("/tmp/cache"));
    config.issuer_url = "https://issuer.example.com/".to_string();
    config.scopes = vec!["b".to_string(), " a ".to_string(), "a".to_string()];
    let key = CacheKey::new(&config).unwrap();
    assert_eq!(key.issuer_url, "https://issuer.example.com");
    assert_eq!(key.scopes, vec!["a".to_string(), "b".to_string()]);

    config.issuer_url = "https://issuer.example.com".to_string();
    let canonical = CacheKey::new(&config).unwrap();
    assert_eq!(canonical.file_stem, key.file_stem);
}

#[test]
fn test_cache_key_separates_identity_dimensions() {
    let base = device_config(Path::new("/tmp/cache"));
    let base_key = CacheKey::new(&base).unwrap();

    let mut other = base.clone();
    other.client_id = "other-client".to_string();
    assert_ne!(CacheKey::new(&other).unwrap().file_stem, base_key.file_stem);

    let mut other = base.clone();
    other.issuer_url = "https://other.example.com".to_string();
    assert_ne!(CacheKey::new(&other).unwrap().file_stem, base_key.file_stem);

    let mut other = base.clone();
    other.scopes = vec!["profile".to_string()];
    assert_ne!(CacheKey::new(&other).unwrap().file_stem, base_key.file_stem);

    let mut other = base.clone();
    other.flow = OAuthFlow::AuthorizationCode(Default::default());
    assert_ne!(CacheKey::new(&other).unwrap().file_stem, base_key.file_stem);

    let mut other = base.clone();
    other.client_secret = Some("secret".to_string());
    assert_ne!(CacheKey::new(&other).unwrap().file_stem, base_key.file_stem);
}

#[test]
fn test_cache_key_contains_no_secret_material() {
    let mut config = device_config(Path::new("/tmp/cache"));
    config.client_secret = Some("super-secret-value".to_string());
    let key = CacheKey::new(&config).unwrap();
    assert!(!key.file_stem.contains("super-secret-value"));
    assert_eq!(key.file_stem.len(), 64);
}

#[test]
fn test_flow_key_rejects_non_persistent_flows() {
    let mut config = device_config(Path::new("/tmp/cache"));
    config.flow = OAuthFlow::ClientCredentials;
    let err = CacheKey::new(&config).unwrap_err();
    assert!(matches!(err, Error::InvalidInput { message } if message.contains("not supported")));

    let mut config = device_config(Path::new("/tmp/cache"));
    config.flow = OAuthFlow::AzureManagedIdentity { client_id: None };
    assert!(CacheKey::new(&config).is_err());
}

#[test]
fn test_token_cache_options_defaults() {
    let options = TokenCacheOptions::new();
    assert!(options.cache_dir.is_none());
    assert!(options.lock_timeout_secs.is_none());
    assert_eq!(options.lock_timeout(), Duration::from_secs(30));

    let options = options.cache_dir("/tmp/x").lock_timeout_secs(5);
    assert_eq!(options.cache_dir.as_deref(), Some(Path::new("/tmp/x")));
    assert_eq!(options.lock_timeout(), Duration::from_secs(5));
}

#[test]
fn test_token_cache_options_rejects_empty_dir() {
    let options = TokenCacheOptions::new().cache_dir("");
    assert!(matches!(
        options.resolved_dir(),
        Err(Error::InvalidInput { message }) if message.contains("must not be empty")
    ));
}

#[tokio::test]
async fn test_session_lifecycle_without_cache_entry() {
    let dir = cache_tempdir();
    let session = OAuthSession::new(device_config(dir.path())).unwrap();

    let status = session.status().await.unwrap();
    assert!(!status.refreshable);
    assert_eq!(status.issuer_url, "https://issuer.example.com");
    assert_eq!(status.client_id, "client-id");
    assert_eq!(status.scopes, vec!["openid".to_string()]);
    assert_eq!(status.flow, "device_code");
    assert_eq!(status.obtained_at, None);

    let logout = session.logout().await.unwrap();
    assert!(!logout.removed);
}

#[tokio::test]
async fn test_record_round_trip_and_redaction() {
    let dir = cache_tempdir();
    let cache = TokenCache::new(
        &device_config(dir.path()),
        &TokenCacheOptions::new().cache_dir(dir.path()),
    )
    .unwrap();
    let response = TokenResponse {
        access_token: AccessToken::new("access-token".to_string()),
        refresh_token: Some(RefreshToken::new("refresh-token".to_string())),
        expires_in: Some(3600),
        token_type: Some(BasicTokenType::Bearer),
    };
    let record = cache.record_from_response(&response).unwrap();
    let debug = format!("{record:?}");
    assert!(!debug.contains("refresh-token"));
    assert!(debug.contains("<redacted>"));
    // No access-token material is persisted.
    let json = serde_json::to_string(&record).unwrap();
    assert!(!json.contains("access-token"));

    cache.store(&record).await.unwrap();
    let loaded = cache.load().await.unwrap().unwrap();
    assert_eq!(loaded.refresh_token, "refresh-token");
    assert_eq!(loaded.version, CACHE_RECORD_VERSION);

    // The on-disk file must not be group/other readable and must not be a symlink target.
    let path = cache.record_path();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0);
    }
    assert!(std::fs::symlink_metadata(&path).unwrap().is_file());

    assert!(cache.delete().await.unwrap());
    assert!(cache.load().await.unwrap().is_none());
    assert!(!cache.delete().await.unwrap());
}

#[cfg(unix)]
#[tokio::test]
async fn test_record_rejects_symlink() {
    let dir = cache_tempdir();
    let cache = TokenCache::new(
        &device_config(dir.path()),
        &TokenCacheOptions::new().cache_dir(dir.path()),
    )
    .unwrap();
    let record = cache
        .record_from_response(&TokenResponse {
            access_token: AccessToken::new("a".to_string()),
            refresh_token: Some(RefreshToken::new("r".to_string())),
            expires_in: None,
            token_type: None,
        })
        .unwrap();
    cache.store(&record).await.unwrap();
    let path = cache.record_path();
    let target = dir.path().join("evil.json");
    std::fs::write(&target, "{}").unwrap();
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink(&target, &path).unwrap();

    let err = cache.load().await.unwrap_err();
    assert!(
        matches!(err, Error::InvalidInput { message } if message.contains("not a regular file"))
    );
}

#[cfg(unix)]
#[tokio::test]
async fn test_record_rejects_world_readable_file() {
    let dir = cache_tempdir();
    let cache = TokenCache::new(
        &device_config(dir.path()),
        &TokenCacheOptions::new().cache_dir(dir.path()),
    )
    .unwrap();
    let path = cache.record_path();
    std::fs::write(&path, "{}").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

    let err = cache.load().await.unwrap_err();
    assert!(matches!(err, Error::InvalidInput { message } if message.contains("group or other")));
}

/// Write a record file that passes the permission hardening checks.
fn write_record_file(path: &Path, contents: &str) {
    std::fs::write(path, contents).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

#[tokio::test]
async fn test_record_rejects_unknown_version_and_corruption() {
    let dir = cache_tempdir();
    let cache = TokenCache::new(
        &device_config(dir.path()),
        &TokenCacheOptions::new().cache_dir(dir.path()),
    )
    .unwrap();
    let path = cache.record_path();

    // A complete, well-formed record with an unknown schema version.
    let record = cache
        .record_from_response(&TokenResponse {
            access_token: AccessToken::new("a".to_string()),
            refresh_token: Some(RefreshToken::new("r".to_string())),
            expires_in: None,
            token_type: None,
        })
        .unwrap();
    let mut json = serde_json::to_value(&record).unwrap();
    json["version"] = serde_json::json!(99);
    write_record_file(&path, &json.to_string());
    let err = cache.load().await.unwrap_err();
    assert!(matches!(err, Error::Runtime { message } if message.contains("unsupported version")));

    write_record_file(&path, r#"{"version":1,"issuer_url":"x""#);
    let err = cache.load().await.unwrap_err();
    assert!(matches!(err, Error::Runtime { message } if message.contains("corrupt")));

    write_record_file(&path, "");
    assert!(
        cache
            .load()
            .await
            .unwrap_err()
            .to_string()
            .contains("corrupt")
    );
}

#[cfg(unix)]
#[tokio::test]
async fn test_cache_dir_rejects_open_permissions() {
    let dir = cache_tempdir();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let err = TokenCache::new(
        &device_config(dir.path()),
        &TokenCacheOptions::new().cache_dir(dir.path()),
    )
    .unwrap_err();
    assert!(matches!(err, Error::InvalidInput { message } if message.contains("chmod 700")));
}

#[tokio::test]
async fn test_lock_serializes_and_releases() {
    let dir = cache_tempdir();
    let cache = TokenCache::new(
        &device_config(dir.path()),
        &TokenCacheOptions::new().cache_dir(dir.path()),
    )
    .unwrap();
    let guard = cache.acquire_lock().await.unwrap();

    let contender = {
        let dir = dir.path().to_path_buf();
        let cache2 = TokenCache::new(
            &device_config(&dir),
            &TokenCacheOptions::new()
                .cache_dir(&dir)
                .lock_timeout_secs(1),
        )
        .unwrap();
        tokio::time::timeout(Duration::from_millis(300), cache2.acquire_lock()).await
    };
    assert!(contender.is_err(), "second acquire must block while held");
    drop(guard);

    let cache3 = TokenCache::new(
        &device_config(dir.path()),
        &TokenCacheOptions::new().cache_dir(dir.path()),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), cache3.acquire_lock())
        .await
        .expect("lock re-acquirable after release")
        .unwrap();
}

#[test]
fn test_token_cache_for_config_gating() {
    let dir = cache_tempdir();
    assert!(
        token_cache_for_config(&device_config(dir.path()))
            .unwrap()
            .is_some()
    );

    let mut config = device_config(dir.path());
    config.token_cache = None;
    assert!(token_cache_for_config(&config).unwrap().is_none());

    let mut config = device_config(dir.path());
    config.flow = OAuthFlow::ClientCredentials;
    assert!(token_cache_for_config(&config).unwrap().is_none());

    let mut config = device_config(dir.path());
    config.flow = OAuthFlow::AzureManagedIdentity { client_id: None };
    let err = token_cache_for_config(&config).unwrap_err();
    assert!(
        matches!(err, Error::InvalidInput { message } if message.contains("AzureManagedIdentity"))
    );
}

#[test]
fn test_oauth_session_requires_cache_options() {
    let mut config = device_config(Path::new("/tmp/cache"));
    config.token_cache = None;
    let err = OAuthSession::new(config).unwrap_err();
    assert!(matches!(err, Error::InvalidInput { message } if message.contains("token_cache")));
}

#[tokio::test]
async fn test_store_skips_responses_without_refresh_token() {
    let dir = cache_tempdir();
    let cache = TokenCache::new(
        &device_config(dir.path()),
        &TokenCacheOptions::new().cache_dir(dir.path()),
    )
    .unwrap();
    let response = TokenResponse {
        access_token: AccessToken::new("access-token".to_string()),
        refresh_token: None,
        expires_in: Some(3600),
        token_type: None,
    };
    assert!(cache.record_from_response(&response).is_none());
    cache.store_if_refreshable(&response).await.unwrap();
    assert!(cache.load().await.unwrap().is_none());
}

#[test]
fn test_session_status_debug_has_no_secrets() {
    let status = SessionStatus {
        refreshable: true,
        issuer_url: "https://issuer.example.com".to_string(),
        client_id: "client-id".to_string(),
        scopes: vec!["openid".to_string()],
        resource: None,
        audience: None,
        flow: "device_code".to_string(),
        obtained_at: Some(100),
    };
    let debug = format!("{status:?}");
    assert!(!debug.contains("refresh-token"));
}
