// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;
use base64::Engine;
use oauth2::TokenResponse as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

fn token_response(
    access_token: &str,
    refresh_token: Option<&str>,
    expires_in: Option<u64>,
) -> TokenResponse {
    TokenResponse {
        access_token: AccessToken::new(access_token.to_string()),
        refresh_token: refresh_token.map(|token| RefreshToken::new(token.to_string())),
        expires_in,
        token_type: None,
    }
}

fn basic_authorization(client_id: &str, client_secret: &str) -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{client_id}:{client_secret}"))
    )
}

struct CapturedRequest {
    line: String,
    headers: String,
    body: String,
}

impl CapturedRequest {
    fn header(&self, name: &str) -> Option<String> {
        self.headers.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.trim()
                .eq_ignore_ascii_case(name)
                .then(|| value.trim().to_string())
        })
    }
}

async fn spawn_discovery_server(expected_requests: usize) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let issuer_url = format!("http://{addr}");

    let server = tokio::spawn(async move {
        for _ in 0..expected_requests {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            assert!(
                request
                    .line
                    .starts_with("GET /.well-known/openid-configuration ")
            );
            let discovery = format!(
                r#"{{"token_endpoint":"http://{addr}/token","authorization_endpoint":"http://{addr}/authorize","device_authorization_endpoint":"http://{addr}/device"}}"#
            );
            write_json_response(&mut stream, "200 OK", &discovery).await;
        }
    });

    (issuer_url, server)
}

async fn spawn_insecure_authorization_discovery_server() -> (String, JoinHandle<()>) {
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
        write_json_response(
            &mut stream,
            "200 OK",
            r#"{"token_endpoint":"https://idp.example.com/token","authorization_endpoint":"http://idp.example.com/authorize"}"#,
        )
        .await;
    });

    (issuer_url, server)
}

async fn spawn_insecure_device_verification_server() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let issuer_url = format!("http://{addr}");
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            if request
                .line
                .starts_with("GET /.well-known/openid-configuration ")
            {
                let discovery = format!(
                    r#"{{"token_endpoint":"http://{addr}/token","device_authorization_endpoint":"http://{addr}/device"}}"#
                );
                write_json_response(&mut stream, "200 OK", &discovery).await;
            } else {
                assert!(request.line.starts_with("POST /device "));
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"device_code":"device-code","user_code":"ABCD-EFGH","verification_uri":"http://idp.example.com/device","expires_in":60}"#,
                )
                .await;
            }
        }
    });

    (issuer_url, server)
}

async fn spawn_captured_token_server() -> (
    String,
    Arc<std::sync::Mutex<Option<CapturedRequest>>>,
    JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let issuer_url = format!("http://{addr}");
    let request = Arc::new(std::sync::Mutex::new(None));
    let server_request = Arc::clone(&request);

    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let captured = read_http_request(&mut stream).await;
            if captured
                .line
                .starts_with("GET /.well-known/openid-configuration ")
            {
                let discovery = format!(r#"{{"token_endpoint":"http://{addr}/token"}}"#);
                write_json_response(&mut stream, "200 OK", &discovery).await;
            } else if captured.line.starts_with("POST /token ") {
                *server_request.lock().unwrap() = Some(captured);
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"access_token":"token","refresh_token":"refresh","expires_in":3600,"token_type":"Bearer"}"#,
                )
                .await;
            } else {
                write_json_response(&mut stream, "404 Not Found", "{}").await;
            }
        }
    });

    (issuer_url, request, server)
}

async fn spawn_redirecting_token_server() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let issuer_url = format!("http://{addr}");

    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            if request
                .line
                .starts_with("GET /.well-known/openid-configuration ")
            {
                let discovery = format!(r#"{{"token_endpoint":"http://{addr}/token"}}"#);
                write_json_response(&mut stream, "200 OK", &discovery).await;
            } else {
                assert!(request.line.starts_with("POST /token "));
                let response = "HTTP/1.1 302 Found\r\nlocation: http://idp.example.com/steal\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_string();
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        }
    });

    (issuer_url, server)
}

async fn spawn_malformed_token_server() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let issuer_url = format!("http://{addr}");

    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            if request
                .line
                .starts_with("GET /.well-known/openid-configuration ")
            {
                let discovery = format!(r#"{{"token_endpoint":"http://{addr}/token"}}"#);
                write_json_response(&mut stream, "200 OK", &discovery).await;
            } else {
                assert!(request.line.starts_with("POST /token "));
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"access_token":{"nested":"leak-marker-12345"}}"#,
                )
                .await;
            }
        }
    });

    (issuer_url, server)
}

async fn spawn_refresh_error_server(
    status: &'static str,
    response_body: &'static str,
) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let issuer_url = format!("http://{addr}");

    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            if request
                .line
                .starts_with("GET /.well-known/openid-configuration ")
            {
                let discovery = format!(r#"{{"token_endpoint":"http://{addr}/token"}}"#);
                write_json_response(&mut stream, "200 OK", &discovery).await;
            } else if request.line.starts_with("POST /token ") {
                assert!(request.body.contains("grant_type=refresh_token"));
                assert!(request.body.contains("refresh_token="));
                write_json_response(&mut stream, status, response_body).await;
            } else {
                write_json_response(&mut stream, "404 Not Found", "{}").await;
            }
        }
    });

    (issuer_url, server)
}

async fn spawn_device_server() -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let issuer_url = format!("http://{addr}");
    let token_requests = Arc::new(AtomicUsize::new(0));
    let server_token_requests = Arc::clone(&token_requests);

    let server = tokio::spawn(async move {
        for _ in 0..5 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            if request
                .line
                .starts_with("GET /.well-known/openid-configuration ")
            {
                let discovery = format!(
                    r#"{{"token_endpoint":"http://{addr}/token","device_authorization_endpoint":"http://{addr}/device"}}"#
                );
                write_json_response(&mut stream, "200 OK", &discovery).await;
            } else if request.line.starts_with("POST /device ") {
                // The resolved default for a confidential client is HTTP Basic.
                assert_eq!(
                    request.header("authorization").as_deref(),
                    Some(basic_authorization("client-id", "secret").as_str())
                );
                assert!(request.body.contains("scope=openid"));
                assert!(!request.body.contains("client_secret"));
                let device = format!(
                    r#"{{"device_code":"device-code","user_code":"ABCD-EFGH","verification_uri":"http://{addr}/verify","verification_uri_complete":"http://{addr}/verify?user_code=ABCD-EFGH","expires_in":60,"interval":1}}"#
                );
                write_json_response(&mut stream, "200 OK", &device).await;
            } else if request.line.starts_with("POST /token ") {
                assert_eq!(
                    request.header("authorization").as_deref(),
                    Some(basic_authorization("client-id", "secret").as_str())
                );
                assert!(
                    request.body.contains(
                        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Adevice_code"
                    )
                );
                assert!(request.body.contains("device_code=device-code"));
                assert!(!request.body.contains("client_secret"));
                let request = server_token_requests.fetch_add(1, Ordering::SeqCst);
                match request {
                    0 => {
                        write_json_response(
                            &mut stream,
                            "400 Bad Request",
                            r#"{"error":"authorization_pending"}"#,
                        )
                        .await;
                    }
                    1 => {
                        write_json_response(
                            &mut stream,
                            "400 Bad Request",
                            r#"{"error":"slow_down"}"#,
                        )
                        .await;
                    }
                    _ => {
                        write_json_response(
                            &mut stream,
                            "200 OK",
                            r#"{"access_token":"device-token","refresh_token":"device-refresh","expires_in":3600,"token_type":"Bearer"}"#,
                        )
                        .await;
                    }
                }
            } else {
                write_json_response(&mut stream, "404 Not Found", "{}").await;
            }
        }
    });

    (issuer_url, token_requests, server)
}

async fn spawn_device_transient_server() -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let issuer_url = format!("http://{addr}");
    let token_requests = Arc::new(AtomicUsize::new(0));
    let server_token_requests = Arc::clone(&token_requests);

    let server = tokio::spawn(async move {
        for _ in 0..5 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            if request
                .line
                .starts_with("GET /.well-known/openid-configuration ")
            {
                let discovery = format!(r#"{{"token_endpoint":"http://{addr}/token"}}"#);
                write_json_response(&mut stream, "200 OK", &discovery).await;
                continue;
            }

            assert!(request.line.starts_with("POST /token "));
            match server_token_requests.fetch_add(1, Ordering::SeqCst) {
                0 => drop(stream),
                1 => {
                    write_json_response(
                        &mut stream,
                        "503 Service Unavailable",
                        r#"{"error":"server_error"}"#,
                    )
                    .await;
                }
                2 => {
                    write_json_response(
                        &mut stream,
                        "400 Bad Request",
                        r#"{"error":"temporarily_unavailable"}"#,
                    )
                    .await;
                }
                _ => {
                    write_json_response(
                        &mut stream,
                        "200 OK",
                        r#"{"access_token":"device-token","expires_in":3600,"token_type":"Bearer"}"#,
                    )
                    .await;
                }
            }
        }
    });

    (issuer_url, token_requests, server)
}

fn test_device_authorization_response(
    expires_in: u64,
    interval: u64,
) -> StandardDeviceAuthorizationResponse {
    serde_json::from_str(&format!(
        r#"{{"device_code":"device-code","user_code":"ABCD-EFGH","verification_uri":"http://127.0.0.1/verify","expires_in":{expires_in},"interval":{interval}}}"#
    ))
    .unwrap()
}

async fn spawn_device_error_server(error: &'static str) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let issuer_url = format!("http://{addr}");

    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            if request
                .line
                .starts_with("GET /.well-known/openid-configuration ")
            {
                let discovery = format!(r#"{{"token_endpoint":"http://{addr}/token"}}"#);
                write_json_response(&mut stream, "200 OK", &discovery).await;
            } else if request.line.starts_with("POST /token ") {
                write_json_response(
                    &mut stream,
                    "400 Bad Request",
                    &format!(r#"{{"error":"{error}"}}"#),
                )
                .await;
            } else {
                write_json_response(&mut stream, "404 Not Found", "{}").await;
            }
        }
    });

    (issuer_url, server)
}

async fn read_http_request(stream: &mut TcpStream) -> CapturedRequest {
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
    let line = headers.lines().next().unwrap_or_default().to_string();
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

    CapturedRequest {
        line,
        headers,
        body,
    }
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

mod authorization;
mod config;
mod device;
mod flows;
