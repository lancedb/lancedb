// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;
use std::fs;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_flight::FlightEndpoint;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::error::{Error, Result};
use crate::remote::client::{ClientConfig, TlsConfig};

const DEFAULT_FLIGHT_SQL_PORT: u16 = 10025;
const DEFAULT_FLIGHT_SQL_TLS_PORT: u16 = 10026;
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(300);
const REUSE_CONNECTION_URI: &str = "arrow-flight-reuse-connection:";

#[derive(Clone, Debug)]
pub(super) struct FlightSqlClientConfig {
    database: String,
    api_key: String,
    host_override: Option<String>,
    client_config: ClientConfig,
}

impl FlightSqlClientConfig {
    pub(super) fn new(
        database: String,
        api_key: String,
        host_override: Option<String>,
        client_config: ClientConfig,
    ) -> Self {
        Self {
            database,
            api_key,
            host_override,
            client_config,
        }
    }

    pub(super) async fn execute(
        &self,
        query: &str,
        default_namespace_path: &[String],
        flight_sql_uri: Option<&str>,
    ) -> Result<Vec<RecordBatch>> {
        validate_namespace_path(default_namespace_path)?;
        let request_id = uuid::Uuid::new_v4().to_string();
        let future = self.execute_inner(query, default_namespace_path, flight_sql_uri, &request_id);
        match resolve_timeout(
            self.client_config.timeout_config.timeout,
            "LANCE_CLIENT_TIMEOUT",
            None,
        )? {
            Some(timeout) => tokio::time::timeout(timeout, future)
                .await
                .map_err(|_| flight_error(&request_id, "Flight SQL statement timed out"))?,
            None => future.await,
        }
    }

    async fn execute_inner(
        &self,
        query: &str,
        default_namespace_path: &[String],
        flight_sql_uri: Option<&str>,
        request_id: &str,
    ) -> Result<Vec<RecordBatch>> {
        let target = resolve_flight_sql_uri(self.host_override.as_deref(), flight_sql_uri)?;
        let headers = self.headers(default_namespace_path, request_id).await?;
        let read_timeout = resolve_timeout(
            self.client_config.timeout_config.read_timeout,
            "LANCE_CLIENT_READ_TIMEOUT",
            Some(DEFAULT_READ_TIMEOUT),
        )?
        .unwrap();

        let channel = connect_channel(&target, &self.client_config, request_id).await?;
        let mut client = flight_client(channel.clone(), &headers);
        let info = tokio::time::timeout(read_timeout, client.execute(query.to_string(), None))
            .await
            .map_err(|_| flight_error(request_id, "Flight SQL query planning timed out"))?
            .map_err(|err| flight_error(request_id, err))?;
        let mut result_schema = if info.schema.is_empty() {
            None
        } else {
            Some(std::sync::Arc::new(
                info.clone()
                    .try_decode_schema()
                    .map_err(|err| flight_error(request_id, err))?,
            ))
        };

        let mut batches = Vec::new();
        for endpoint in info.endpoint {
            let ticket = endpoint.ticket.clone().ok_or_else(|| {
                flight_error(request_id, "Flight SQL endpoint did not include a ticket")
            })?;
            let endpoint_channel = endpoint_channel(
                &endpoint,
                &target,
                &self.client_config,
                channel.clone(),
                request_id,
            )
            .await?;
            let mut endpoint_client = flight_client(endpoint_channel, &headers);
            let mut stream = tokio::time::timeout(read_timeout, endpoint_client.do_get(ticket))
                .await
                .map_err(|_| flight_error(request_id, "Flight SQL DoGet timed out"))?
                .map_err(|err| flight_error(request_id, err))?;
            loop {
                let next = tokio::time::timeout(read_timeout, stream.try_next())
                    .await
                    .map_err(|_| flight_error(request_id, "Flight SQL result read timed out"))?
                    .map_err(|err| flight_error(request_id, err))?;
                match next {
                    Some(batch) => batches.push(batch),
                    None => break,
                }
            }
            if result_schema.is_none() {
                result_schema = stream.schema().cloned();
            }
        }
        if batches.is_empty()
            && let Some(schema) = result_schema
        {
            batches.push(RecordBatch::new_empty(schema));
        }
        Ok(batches)
    }

    async fn headers(
        &self,
        default_namespace_path: &[String],
        request_id: &str,
    ) -> Result<HashMap<String, String>> {
        let mut headers = HashMap::new();
        merge_headers(&mut headers, &self.client_config.extra_headers)?;
        if let Some(provider) = &self.client_config.header_provider {
            merge_headers(&mut headers, &provider.get_headers().await?)?;
        }

        let has_authorization = headers.contains_key("authorization");
        let has_api_key = headers.contains_key("x-api-key");
        if has_authorization && has_api_key {
            return Err(Error::InvalidInput {
                message: "Flight SQL accepts either authorization or x-api-key, not both"
                    .to_string(),
            });
        }
        if !has_authorization && !has_api_key {
            if self.api_key.is_empty() {
                return Err(Error::InvalidInput {
                    message: "Flight SQL authentication credentials are required".to_string(),
                });
            }
            insert_header(&mut headers, "x-api-key", &self.api_key)?;
        }

        insert_header(&mut headers, "database", &self.database)?;
        let namespace_path = if default_namespace_path.is_empty() {
            "public".to_string()
        } else {
            default_namespace_path.join("$")
        };
        insert_header(&mut headers, "namespace-path", &namespace_path)?;
        insert_header(&mut headers, "x-request-id", request_id)?;
        if let Some(user_id) = self.client_config.resolve_user_id() {
            insert_header(&mut headers, "x-lancedb-user-id", &user_id)?;
        }
        Ok(headers)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FlightTarget {
    uri: String,
    tls: bool,
}

fn resolve_flight_sql_uri(
    host_override: Option<&str>,
    flight_sql_uri: Option<&str>,
) -> Result<FlightTarget> {
    if let Some(uri) = flight_sql_uri {
        return normalize_flight_sql_uri(uri);
    }
    let host_override = host_override.ok_or_else(|| Error::InvalidInput {
        message: "flight_sql_uri is required when the Flight SQL endpoint cannot be derived from host_override".to_string(),
    })?;
    let parsed = url::Url::parse(host_override).map_err(|err| Error::InvalidInput {
        message: format!("Invalid host_override: {err}"),
    })?;
    if parsed.scheme() != "http" {
        return Err(Error::InvalidInput {
            message: "flight_sql_uri is required for TLS or non-HTTP host overrides".to_string(),
        });
    }
    validate_endpoint_url(&parsed, "host_override")?;
    let port = match parsed.port().or(explicit_port(host_override)) {
        Some(u16::MAX) => {
            return Err(Error::InvalidInput {
                message: "flight_sql_uri is required when host_override uses port 65535"
                    .to_string(),
            });
        }
        Some(port) => port + 1,
        None => DEFAULT_FLIGHT_SQL_PORT,
    };
    Ok(FlightTarget {
        uri: endpoint_uri("http", parsed.host_str().unwrap(), port),
        tls: false,
    })
}

fn normalize_flight_sql_uri(uri: &str) -> Result<FlightTarget> {
    let parsed = url::Url::parse(uri).map_err(|err| Error::InvalidInput {
        message: format!("Invalid flight_sql_uri: {err}"),
    })?;
    validate_endpoint_url(&parsed, "flight_sql_uri")?;
    let tls = match parsed.scheme().to_ascii_lowercase().as_str() {
        "grpc" | "grpc+tcp" | "http" => false,
        "grpc+tls" | "grpcs" | "https" => true,
        _ => {
            return Err(Error::InvalidInput {
                message: "flight_sql_uri must use grpc, grpc+tcp, grpc+tls, grpcs, http, or https"
                    .to_string(),
            });
        }
    };
    let port = parsed.port().or(explicit_port(uri)).unwrap_or(if tls {
        DEFAULT_FLIGHT_SQL_TLS_PORT
    } else {
        DEFAULT_FLIGHT_SQL_PORT
    });
    if port == 0 {
        return Err(Error::InvalidInput {
            message: "flight_sql_uri port must be greater than zero".to_string(),
        });
    }
    Ok(FlightTarget {
        uri: endpoint_uri(
            if tls { "https" } else { "http" },
            parsed.host_str().unwrap(),
            port,
        ),
        tls,
    })
}

fn explicit_port(uri: &str) -> Option<u16> {
    let authority = uri.split_once("://")?.1.split(['/', '?', '#']).next()?;
    let suffix = if authority.starts_with('[') {
        authority.split_once(']')?.1.strip_prefix(':')?
    } else {
        authority.rsplit_once(':')?.1
    };
    suffix.parse().ok()
}

fn validate_endpoint_url(parsed: &url::Url, name: &str) -> Result<()> {
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

fn endpoint_uri(scheme: &str, host: &str, port: u16) -> String {
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

async fn endpoint_channel(
    endpoint: &FlightEndpoint,
    primary: &FlightTarget,
    config: &ClientConfig,
    primary_channel: Channel,
    request_id: &str,
) -> Result<Channel> {
    let Some(location) = endpoint.location.first() else {
        return Ok(primary_channel);
    };
    if is_reuse_connection_uri(&location.uri) {
        return Ok(primary_channel);
    }
    let target = normalize_flight_sql_uri(&location.uri)?;
    if primary.tls && !target.tls {
        return Err(Error::InvalidInput {
            message: "Flight SQL endpoint attempted to downgrade TLS".to_string(),
        });
    }
    connect_channel(&target, config, request_id).await
}

fn is_reuse_connection_uri(uri: &str) -> bool {
    uri.starts_with(REUSE_CONNECTION_URI)
}

async fn connect_channel(
    target: &FlightTarget,
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
        .map_err(|err| flight_error(request_id, err))?
        .connect_timeout(connect_timeout);
    if target.tls {
        endpoint = endpoint
            .tls_config(tls_config(config.tls_config.as_ref())?)
            .map_err(|err| flight_error(request_id, err))?;
    }
    tokio::time::timeout(connect_timeout, endpoint.connect())
        .await
        .map_err(|_| flight_error(request_id, "Flight SQL connection timed out"))?
        .map_err(|err| flight_error(request_id, err))
}

fn tls_config(config: Option<&TlsConfig>) -> Result<ClientTlsConfig> {
    let mut tls = ClientTlsConfig::new().with_enabled_roots();
    if let Some(config) = config {
        if !config.assert_hostname {
            return Err(Error::InvalidInput {
                message: "Flight SQL cannot disable TLS hostname verification".to_string(),
            });
        }
        if let Some(path) = &config.ssl_ca_cert {
            let pem = fs::read(path).map_err(|err| Error::InvalidInput {
                message: format!("Failed to read Flight SQL CA certificate {path}: {err}"),
            })?;
            tls = tls.ca_certificate(Certificate::from_pem(pem));
        }
        match (&config.cert_file, &config.key_file) {
            (Some(cert), Some(key)) => {
                let cert_pem = fs::read(cert).map_err(|err| Error::InvalidInput {
                    message: format!("Failed to read Flight SQL client certificate {cert}: {err}"),
                })?;
                let key_pem = fs::read(key).map_err(|err| Error::InvalidInput {
                    message: format!("Failed to read Flight SQL client key {key}: {err}"),
                })?;
                tls = tls.identity(Identity::from_pem(cert_pem, key_pem));
            }
            (None, None) => {}
            _ => {
                return Err(Error::InvalidInput {
                    message: "Flight SQL mTLS requires both cert_file and key_file".to_string(),
                });
            }
        }
    }
    Ok(tls)
}

fn flight_client(
    channel: Channel,
    headers: &HashMap<String, String>,
) -> FlightSqlServiceClient<Channel> {
    let mut client = FlightSqlServiceClient::new(channel);
    for (key, value) in headers {
        client.set_header(key, value);
    }
    client
}

fn merge_headers(
    destination: &mut HashMap<String, String>,
    source: &HashMap<String, String>,
) -> Result<()> {
    for (key, value) in source {
        insert_header(destination, key, value)?;
    }
    Ok(())
}

fn insert_header(headers: &mut HashMap<String, String>, key: &str, value: &str) -> Result<()> {
    let key = key.to_ascii_lowercase();
    let valid_key = !key.is_empty()
        && key.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"-_.".contains(&byte)
        });
    if !valid_key {
        return Err(Error::InvalidInput {
            message: format!("Invalid Flight SQL metadata key: {key:?}"),
        });
    }
    if !value.is_ascii() || value.bytes().any(|byte| !(0x20..=0x7e).contains(&byte)) {
        return Err(Error::InvalidInput {
            message: format!("Flight SQL metadata must be printable ASCII: {key:?}"),
        });
    }
    headers.insert(key, value.to_string());
    Ok(())
}

fn validate_namespace_path(path: &[String]) -> Result<()> {
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

fn resolve_timeout(
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

fn flight_error(request_id: &str, error: impl std::fmt::Display) -> Error {
    Error::Runtime {
        message: format!("Flight SQL error (request_id={request_id}): {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_supported_uris() {
        assert_eq!(
            normalize_flight_sql_uri("grpc://localhost").unwrap(),
            FlightTarget {
                uri: "http://localhost:10025".to_string(),
                tls: false,
            }
        );
        assert_eq!(
            normalize_flight_sql_uri("grpcs://example.com").unwrap(),
            FlightTarget {
                uri: "https://example.com:10026".to_string(),
                tls: true,
            }
        );
        assert_eq!(
            normalize_flight_sql_uri("grpc://[::1]:10025").unwrap(),
            FlightTarget {
                uri: "http://[::1]:10025".to_string(),
                tls: false,
            }
        );
        assert_eq!(
            normalize_flight_sql_uri("https://example.com:443").unwrap(),
            FlightTarget {
                uri: "https://example.com:443".to_string(),
                tls: true,
            }
        );
    }

    #[test]
    fn derives_plaintext_endpoint_from_host_override() {
        assert_eq!(
            resolve_flight_sql_uri(Some("http://localhost:10024"), None).unwrap(),
            FlightTarget {
                uri: "http://localhost:10025".to_string(),
                tls: false,
            }
        );
        assert_eq!(
            resolve_flight_sql_uri(Some("http://localhost:80"), None).unwrap(),
            FlightTarget {
                uri: "http://localhost:81".to_string(),
                tls: false,
            }
        );
    }

    #[test]
    fn rejects_unsafe_or_ambiguous_endpoints() {
        assert!(normalize_flight_sql_uri("ftp://localhost").is_err());
        assert!(normalize_flight_sql_uri("grpc://user@localhost").is_err());
        assert!(normalize_flight_sql_uri("grpc://localhost/path").is_err());
        assert!(resolve_flight_sql_uri(Some("https://localhost"), None).is_err());
    }

    #[test]
    fn validates_namespace_components() {
        assert!(validate_namespace_path(&[]).is_ok());
        assert!(validate_namespace_path(&["events".into(), "raw".into()]).is_ok());
        assert!(validate_namespace_path(&["events$raw".into()]).is_err());
        assert!(validate_namespace_path(&["".into()]).is_err());
        assert!(validate_namespace_path(&["café".into()]).is_err());
    }

    #[test]
    fn accepts_reuse_connection_uri_variants() {
        assert!(is_reuse_connection_uri("arrow-flight-reuse-connection:"));
        assert!(is_reuse_connection_uri("arrow-flight-reuse-connection://?"));
        assert!(!is_reuse_connection_uri("grpc://localhost:10025"));
    }
}
