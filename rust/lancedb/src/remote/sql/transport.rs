// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SqlTarget {
    pub(super) uri: String,
    pub(super) tls: bool,
}

pub(super) fn resolve_sql_host_override(
    host_override: Option<&str>,
    sql_host_override: Option<&str>,
) -> Result<SqlTarget> {
    if let Some(uri) = sql_host_override {
        return normalize_sql_host_override(uri);
    }
    let host_override = host_override.ok_or_else(|| Error::InvalidInput {
        message: "sql_host_override is required when the SQL service endpoint cannot be derived from host_override".to_string(),
    })?;
    let parsed = url::Url::parse(host_override).map_err(|err| Error::InvalidInput {
        message: format!("Invalid host_override: {err}"),
    })?;
    if parsed.scheme() != "http" {
        return Err(Error::InvalidInput {
            message: "sql_host_override is required for TLS or non-HTTP host overrides".to_string(),
        });
    }
    validate_endpoint_url(&parsed, "host_override")?;
    let port = match parsed.port().or(explicit_port(host_override)) {
        Some(u16::MAX) => {
            return Err(Error::InvalidInput {
                message: "sql_host_override is required when host_override uses port 65535"
                    .to_string(),
            });
        }
        Some(port) => port + 1,
        None => DEFAULT_SQL_PORT,
    };
    Ok(SqlTarget {
        uri: endpoint_uri("http", parsed.host_str().unwrap(), port),
        tls: false,
    })
}

pub(super) fn normalize_sql_host_override(uri: &str) -> Result<SqlTarget> {
    let parsed = url::Url::parse(uri).map_err(|err| Error::InvalidInput {
        message: format!("Invalid sql_host_override: {err}"),
    })?;
    validate_endpoint_url(&parsed, "sql_host_override")?;
    let tls = match parsed.scheme().to_ascii_lowercase().as_str() {
        "grpc" | "grpc+tcp" | "http" => false,
        "grpc+tls" | "grpcs" | "https" => true,
        _ => {
            return Err(Error::InvalidInput {
                message:
                    "sql_host_override must use grpc, grpc+tcp, grpc+tls, grpcs, http, or https"
                        .to_string(),
            });
        }
    };
    let port = parsed.port().or(explicit_port(uri)).unwrap_or(if tls {
        DEFAULT_SQL_TLS_PORT
    } else {
        DEFAULT_SQL_PORT
    });
    if port == 0 {
        return Err(Error::InvalidInput {
            message: "sql_host_override port must be greater than zero".to_string(),
        });
    }
    Ok(SqlTarget {
        uri: endpoint_uri(
            if tls { "https" } else { "http" },
            parsed.host_str().unwrap(),
            port,
        ),
        tls,
    })
}

pub(super) fn explicit_port(uri: &str) -> Option<u16> {
    let authority = uri.split_once("://")?.1.split(['/', '?', '#']).next()?;
    let suffix = if authority.starts_with('[') {
        authority.split_once(']')?.1.strip_prefix(':')?
    } else {
        authority.rsplit_once(':')?.1
    };
    suffix.parse().ok()
}

pub(super) fn validate_endpoint_url(parsed: &url::Url, name: &str) -> Result<()> {
    if parsed.host_str().is_none() {
        return Err(Error::InvalidInput {
            message: format!("{name} must include a hostname"),
        });
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(Error::InvalidInput {
            message: format!("{name} must not include user information"),
        });
    }
    if !matches!(parsed.path(), "" | "/") || parsed.query().is_some() || parsed.fragment().is_some()
    {
        return Err(Error::InvalidInput {
            message: format!("{name} must not include a path, query, or fragment"),
        });
    }
    Ok(())
}

pub(super) fn endpoint_uri(scheme: &str, host: &str, port: u16) -> String {
    if host.contains(':') {
        let host = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host);
        format!("{scheme}://[{host}]:{port}")
    } else {
        format!("{scheme}://{host}:{port}")
    }
}

pub(super) async fn connect_channel(
    target: &SqlTarget,
    config: &ClientConfig,
    request_id: &str,
) -> Result<Channel> {
    let connect_timeout = resolve_timeout(
        config.timeout_config.connect_timeout,
        "LANCE_CLIENT_CONNECT_TIMEOUT",
        Some(DEFAULT_CONNECT_TIMEOUT),
    )?
    .unwrap();
    let mut endpoint = Endpoint::from_shared(target.uri.clone())
        .map_err(|err| sql_error(request_id, err))?
        .connect_timeout(connect_timeout);
    if target.tls {
        endpoint = endpoint
            .tls_config(tls_config(config.tls_config.as_ref())?)
            .map_err(|err| sql_error(request_id, err))?;
    }
    tokio::time::timeout(connect_timeout, endpoint.connect())
        .await
        .map_err(|_| sql_error(request_id, "SQL connection timed out"))?
        .map_err(|err| sql_error(request_id, err))
}

pub(super) fn tls_config(config: Option<&TlsConfig>) -> Result<ClientTlsConfig> {
    let mut tls = ClientTlsConfig::new().with_enabled_roots();
    if let Some(config) = config {
        if !config.assert_hostname {
            return Err(Error::InvalidInput {
                message: "SQL cannot disable TLS hostname verification".to_string(),
            });
        }
        if let Some(path) = &config.ssl_ca_cert {
            let pem = fs::read(path).map_err(|err| Error::InvalidInput {
                message: format!("Failed to read SQL CA certificate {path}: {err}"),
            })?;
            tls = tls.ca_certificate(Certificate::from_pem(pem));
        }
        match (&config.cert_file, &config.key_file) {
            (Some(cert), Some(key)) => {
                let cert_pem = fs::read(cert).map_err(|err| Error::InvalidInput {
                    message: format!("Failed to read SQL client certificate {cert}: {err}"),
                })?;
                let key_pem = fs::read(key).map_err(|err| Error::InvalidInput {
                    message: format!("Failed to read SQL client key {key}: {err}"),
                })?;
                tls = tls.identity(Identity::from_pem(cert_pem, key_pem));
            }
            (None, None) => {}
            _ => {
                return Err(Error::InvalidInput {
                    message: "SQL mTLS requires both cert_file and key_file".to_string(),
                });
            }
        }
    }
    Ok(tls)
}

pub(super) fn client_with_headers(
    client: FlightServiceClient<Channel>,
    headers: &HeaderMap,
) -> Result<FlightClient> {
    let mut client = FlightClient::new_from_inner(client);
    for (key, value) in headers {
        let value = value.to_str().map_err(|err| Error::InvalidInput {
            message: format!("Invalid SQL metadata value for {key:?}: {err}"),
        })?;
        client
            .add_header(key.as_str(), value)
            .map_err(|err| Error::InvalidInput {
                message: format!("Invalid SQL metadata header {key:?}: {err}"),
            })?;
    }
    Ok(client)
}

pub(super) fn merge_headers(
    destination: &mut HeaderMap,
    source: &HashMap<String, String>,
) -> Result<()> {
    for (key, value) in source {
        insert_header(destination, key, value)?;
    }
    Ok(())
}

pub(super) fn insert_header(headers: &mut HeaderMap, key: &str, value: &str) -> Result<()> {
    let key = HeaderName::from_bytes(key.as_bytes()).map_err(|err| Error::InvalidInput {
        message: format!("Invalid SQL metadata key {key:?}: {err}"),
    })?;
    let value = HeaderValue::try_from(value).map_err(|err| Error::InvalidInput {
        message: format!("Invalid SQL metadata value for {key:?}: {err}"),
    })?;
    headers.insert(key, value);
    Ok(())
}

pub(super) fn validate_namespace_path(path: &[String]) -> Result<()> {
    for component in path {
        if component.is_empty()
            || !component.is_ascii()
            || component.contains('$')
            || component.bytes().any(|byte| !(0x20..=0x7e).contains(&byte))
        {
            return Err(Error::InvalidInput {
                message: "default_namespace_path components must be non-empty printable ASCII strings without '$'".to_string(),
            });
        }
    }
    Ok(())
}

pub(super) fn poll_retry_delay(config: &ResolvedRetryConfig, retry_count: u8) -> Duration {
    let exponent = i32::from(retry_count.saturating_sub(1).min(16));
    let backoff = config.backoff_factor * 2.0_f32.powi(exponent);
    let jitter = rand::random::<f32>() * config.backoff_jitter;
    Duration::from_secs_f32((backoff + jitter).clamp(MIN_POLL_INTERVAL.as_secs_f32(), 60.0))
}

pub(super) fn cancellation_status_is_ambiguous(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Cancelled
            | tonic::Code::Unknown
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Internal
            | tonic::Code::Unavailable
            | tonic::Code::DataLoss
    )
}

pub(super) fn resolve_timeout(
    configured: Option<Duration>,
    env_name: &str,
    default: Option<Duration>,
) -> Result<Option<Duration>> {
    if configured.is_some() {
        return Ok(configured);
    }
    match std::env::var(env_name) {
        Ok(value) => value
            .parse::<u64>()
            .map(Duration::from_secs)
            .map(Some)
            .map_err(|_| Error::InvalidInput {
                message: format!("Invalid value for {env_name} environment variable: {value:?}"),
            }),
        Err(_) => Ok(default),
    }
}

pub(super) async fn with_overall_timeout<T>(
    timeout: Option<Duration>,
    operation: &str,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    match timeout {
        Some(timeout) => {
            tokio::time::timeout(timeout, future)
                .await
                .map_err(|_| Error::Runtime {
                    message: format!("{operation} timed out"),
                })?
        }
        None => future.await,
    }
}

pub(super) fn sql_error(request_id: &str, error: impl std::fmt::Display) -> Error {
    Error::Runtime {
        message: format!("SQL error (request_id={request_id}): {error}"),
    }
}
