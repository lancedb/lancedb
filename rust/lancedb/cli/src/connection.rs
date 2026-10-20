// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::server_config::{
    parse_endpoint, Authentication, ResolvedConnection, TlsOptions, DEFAULT_HTTP_PORT,
};
use anyhow::Result;
use lancedb::Connection;
pub async fn create_database_connection(conn: &ResolvedConnection) -> Result<Connection> {
    create_connection(
        &conn.http_server_url,
        &conn.database,
        &conn.authentication,
        &conn.tls,
        conn.azure_storage_account_name.as_deref(),
    )
    .await
}

pub fn is_native_database_url(server_url: &str) -> bool {
    server_url.starts_with("file://") || server_url.starts_with('/')
}

pub async fn create_lancedb_connection(
    server_url: &str,
    database: &str,
    authentication: &Authentication,
    tls: &TlsOptions,
) -> Result<Connection> {
    create_connection(server_url, database, authentication, tls, None).await
}

async fn create_connection(
    server_url: &str,
    database: &str,
    authentication: &Authentication,
    tls: &TlsOptions,
    azure_storage_account_name: Option<&str>,
) -> Result<Connection> {
    // For local testing, use a file-based database
    if is_native_database_url(server_url) {
        let path = server_url.strip_prefix("file://").unwrap_or(server_url);
        return Ok(lancedb::connect(path).execute().await?);
    }

    // For remote LanceDB connections (including local server via db://)
    let mut builder = lancedb::connect(&format!("db://{}", database));

    if let Authentication::ApiKey(api_key) = authentication {
        builder = builder.api_key(api_key);
    } else {
        // The SDK requires an explicit API key even with a header provider.
        // An empty key omits x-api-key; client_config supplies the OIDC headers.
        builder = builder.api_key("");
    }

    let mut config = crate::lancedb_client_config(tls, authentication).unwrap_or_default();
    if let Some(account) = azure_storage_account_name {
        config
            .extra_headers
            .insert("x-azure-storage-account-name".into(), account.into());
    }
    builder = builder.client_config(config);

    // Set region if specified
    if let Ok(region) = std::env::var("LANCEDB_REGION") {
        builder = builder.region(&region);
    } else {
        builder = builder.region("us-east-1");
    }

    // For enterprise/custom endpoints (including local). Parsed the same way as
    // the SQL shell's address, so one profile resolves to the same endpoint
    // whichever command reads it.
    if !server_url.contains("lancedb.com") {
        let endpoint = parse_endpoint(server_url, DEFAULT_HTTP_PORT)?;
        builder = builder.host_override(&endpoint.url());
    }

    Ok(builder.execute().await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn oidc_connection_sends_bearer_headers_without_api_key() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (headers_tx, mut headers_rx) = tokio::sync::mpsc::unbounded_channel();
        let app = axum::Router::new().fallback(move |headers: http::HeaderMap| {
            let headers_tx = headers_tx.clone();
            async move {
                headers_tx.send(headers).unwrap();
                axum::Json(serde_json::json!({"tables": []}))
            }
        });
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        std::fs::write(&path, "test-token").unwrap();
        let connection = create_lancedb_connection(
            &format!("http://{address}"),
            "test",
            &Authentication::OidcAccessTokenFile(path.clone()),
            &TlsOptions::default(),
        )
        .await
        .unwrap();
        connection.list_tables(Default::default()).await.unwrap();
        let headers = headers_rx.try_recv().unwrap();
        assert_eq!(headers["authorization"], "Bearer test-token");
        assert_eq!(headers["x-lancedb-credential-type"], "oidc");
        assert!(!headers.contains_key("x-api-key"));
        std::fs::write(path, "replacement-token").unwrap();
        connection.list_tables(Default::default()).await.unwrap();
        let headers = headers_rx.try_recv().unwrap();
        assert_eq!(headers["authorization"], "Bearer replacement-token");
        assert!(!headers.contains_key("x-api-key"));
        server.abort();
    }
}
