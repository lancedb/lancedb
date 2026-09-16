// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use http::StatusCode;
use lance_namespace::models::{
    CreateNamespaceRequest, DescribeNamespaceRequest, DropNamespaceRequest, ListNamespacesRequest,
};

use super::db::RemoteDatabase;
use super::{ClientConfig, HeaderProvider, OAuthConfig, OAuthHeaderProvider};
use crate::catalog::{
    Catalog, CreateDatabaseRequest, DropDatabaseRequest, ListDatabasesRequest,
    ListDatabasesResponse,
};
use crate::database::Database;
use crate::{Error, Result};

/// Authentication and client settings shared by a catalog and its databases.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct RemoteCatalogOptions {
    /// Optional API key for catalog and database requests.
    pub api_key: Option<String>,
    /// Shared transport and authentication settings.
    pub client_config: ClientConfig,
    /// SQL service endpoint used by returned database connections.
    /// Required for SQL when the catalog endpoint uses HTTPS.
    pub sql_host_override: Option<String>,
    /// Read consistency interval for tables opened in returned databases.
    pub read_consistency_interval: Option<Duration>,
    /// OAuth authentication, mutually exclusive with an API key or header provider.
    pub oauth_config: Option<OAuthConfig>,
}

impl std::fmt::Debug for RemoteCatalogOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteCatalogOptions")
            .field("read_consistency_interval", &self.read_consistency_interval)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub(crate) struct ScopedHeaderProvider {
    pub provider: Option<Arc<dyn HeaderProvider>>,
    pub database: Option<String>,
}

impl std::fmt::Debug for ScopedHeaderProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopedHeaderProvider")
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

impl ScopedHeaderProvider {
    pub(crate) fn apply(&self, headers: &mut HashMap<String, String>) {
        headers.retain(|name, _| {
            !name.eq_ignore_ascii_case("x-lancedb-database")
                && !name.eq_ignore_ascii_case("x-lancedb-database-prefix")
        });
        if let Some(database) = &self.database {
            headers.insert("x-lancedb-database".into(), database.clone());
        }
    }
}

#[async_trait]
impl HeaderProvider for ScopedHeaderProvider {
    async fn get_headers(&self) -> Result<HashMap<String, String>> {
        let mut headers = match &self.provider {
            Some(provider) => provider.get_headers().await?,
            None => HashMap::new(),
        };
        self.apply(&mut headers);
        Ok(headers)
    }
}

/// A catalog backed by the server's root namespace APIs.
///
/// Database management requests omit database-selection headers. Opened database
/// connections retain their own scope and authentication independently.
pub struct RemoteCatalog {
    endpoint: String,
    root: RemoteDatabase,
    options: RemoteCatalogOptions,
}

impl fmt::Debug for RemoteCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteCatalog")
            .field("uri", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl RemoteCatalog {
    /// Connect to an HTTP(S) root namespace endpoint.
    ///
    /// ```
    /// # use lancedb::remote::{RemoteCatalog, RemoteCatalogOptions};
    /// # fn example() -> lancedb::Result<()> {
    /// let catalog = RemoteCatalog::try_new("https://my-server.example", RemoteCatalogOptions::default())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_new(endpoint: impl AsRef<str>, mut options: RemoteCatalogOptions) -> Result<Self> {
        let url = url::Url::parse(endpoint.as_ref()).map_err(|err| Error::InvalidInput {
            message: format!("Invalid catalog endpoint: {err}"),
        })?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(Error::InvalidInput { message: "Catalog endpoint must be an HTTP(S) URL without credentials, query, or fragment".into() });
        }
        if options
            .client_config
            .id_delimiter
            .as_ref()
            .is_some_and(|d| d.is_empty())
        {
            return Err(Error::InvalidInput {
                message: "Catalog identifier delimiter cannot be empty".into(),
            });
        }
        if let Some(oauth) = options.oauth_config.take() {
            if options.api_key.is_some() || options.client_config.header_provider.is_some() {
                return Err(Error::InvalidInput {
                    message: "oauth_config cannot be combined with api_key or header_provider"
                        .into(),
                });
            }
            options.client_config.header_provider =
                Some(Arc::new(OAuthHeaderProvider::new(oauth)?));
        }
        let endpoint = url.to_string().trim_end_matches('/').to_string();
        let root = RemoteDatabase::for_catalog(&endpoint, None, &options)?;
        Ok(Self {
            endpoint,
            root,
            options,
        })
    }

    fn validate_name(&self, name: &str) -> Result<()> {
        let delimiter = self
            .options
            .client_config
            .id_delimiter
            .as_deref()
            .unwrap_or("$");
        if name.is_empty()
            || name.trim() != name
            || !name.is_ascii()
            || name.chars().any(char::is_control)
            || name.contains(delimiter)
            || matches!(name, "." | "..")
        {
            return Err(Error::InvalidInput {
                message: format!(
                    "Invalid database name '{name}': expected a nonempty ASCII name without surrounding whitespace, control characters, or namespace delimiter '{delimiter}'"
                ),
            });
        }
        Ok(())
    }

    fn database(&self, name: &str) -> Result<Arc<dyn Database>> {
        Ok(Arc::new(RemoteDatabase::for_catalog(
            &self.endpoint,
            Some(name),
            &self.options,
        )?))
    }

    fn map_missing(name: &str, err: Error) -> Error {
        match err {
            Error::Http {
                status_code: Some(StatusCode::NOT_FOUND),
                ..
            } => Error::DatabaseNotFound { name: name.into() },
            err => err,
        }
    }
}

#[async_trait]
impl Catalog for RemoteCatalog {
    fn uri(&self) -> &str {
        &self.endpoint
    }

    async fn create_database(&self, request: CreateDatabaseRequest) -> Result<Arc<dyn Database>> {
        self.validate_name(&request.name)?;
        self.root
            .create_namespace(CreateNamespaceRequest {
                id: Some(vec![request.name.clone()]),
                mode: Some(
                    if request.exist_ok {
                        "ExistOk"
                    } else {
                        "Create"
                    }
                    .into(),
                ),
                ..Default::default()
            })
            .await
            .map_err(|err| match err {
                Error::Http {
                    status_code: Some(StatusCode::CONFLICT),
                    ..
                } => Error::DatabaseAlreadyExists {
                    name: request.name.clone(),
                },
                err => err,
            })?;
        self.database(&request.name)
    }

    async fn drop_database(&self, request: DropDatabaseRequest) -> Result<()> {
        self.validate_name(&request.name)?;
        let result = self
            .root
            .drop_namespace(DropNamespaceRequest {
                id: Some(vec![request.name.clone()]),
                mode: Some(
                    if request.ignore_missing {
                        "Skip"
                    } else {
                        "Fail"
                    }
                    .into(),
                ),
                behavior: Some("Restrict".into()),
                ..Default::default()
            })
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(Error::Http {
                status_code: Some(StatusCode::NOT_FOUND),
                ..
            }) if request.ignore_missing => Ok(()),
            Err(err) => Err(Self::map_missing(&request.name, err)),
        }
    }

    async fn list_databases(&self, request: ListDatabasesRequest) -> Result<ListDatabasesResponse> {
        let limit = request
            .limit
            .map(|limit| {
                i32::try_from(limit)
                    .ok()
                    .filter(|limit| *limit > 0)
                    .ok_or_else(|| Error::InvalidInput {
                        message: "Database list limit must be between 1 and 2147483647".into(),
                    })
            })
            .transpose()?;
        let response = self
            .root
            .list_namespaces(ListNamespacesRequest {
                id: Some(vec![]),
                limit,
                page_token: request.page_token,
                ..Default::default()
            })
            .await?;
        Ok(ListDatabasesResponse {
            databases: response.namespaces,
            page_token: response.page_token.filter(|token| !token.is_empty()),
        })
    }

    async fn connect_database(&self, name: &str) -> Result<Arc<dyn Database>> {
        self.validate_name(name)?;
        self.root
            .describe_namespace(DescribeNamespaceRequest {
                id: Some(vec![name.into()]),
                ..Default::default()
            })
            .await
            .map_err(|err| Self::map_missing(name, err))?;
        self.database(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::CatalogConnection;
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    #[derive(Debug)]
    struct Request {
        line: String,
        headers: HashMap<String, String>,
        body: Value,
    }

    async fn server(responses: Vec<(u16, Value)>) -> (String, JoinHandle<Vec<Request>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (status, body) in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let header_end = loop {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(pos) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let header = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
                let mut lines = header.lines();
                let line = lines.next().unwrap().to_string();
                let headers: HashMap<_, _> = lines
                    .filter_map(|line| line.split_once(':'))
                    .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
                    .collect();
                let length: usize = headers
                    .get("content-length")
                    .map(|s| s.parse().unwrap())
                    .unwrap_or(0);
                while bytes.len() < header_end + length {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                }
                let request_body = if length == 0 {
                    Value::Null
                } else {
                    serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap()
                };
                requests.push(Request {
                    line,
                    headers,
                    body: request_body,
                });
                let body = if status == 204 {
                    String::new()
                } else {
                    body.to_string()
                };
                socket.write_all(format!("HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
            requests
        });
        (endpoint, task)
    }

    #[derive(Debug)]
    struct AuthProvider;

    #[async_trait]
    impl HeaderProvider for AuthProvider {
        async fn get_headers(&self) -> Result<HashMap<String, String>> {
            Ok(HashMap::from([
                ("Authorization".into(), "Bearer refreshed".into()),
                ("X-LanceDB-Database".into(), "wrong-dynamic".into()),
                ("X-LanceDB-Database-Prefix".into(), "wrong-prefix".into()),
            ]))
        }
    }

    #[tokio::test]
    async fn catalog_routes_root_and_independent_database_scopes() {
        let (endpoint, task) = server(vec![
            (
                200,
                json!({"namespaces": ["team/search"], "page_token": "next"}),
            ),
            (204, Value::Null),
            (200, json!({"namespaces": []})),
            (200, json!({})),
            (200, json!({"namespaces": []})),
            (200, json!({"namespaces": []})),
            (200, json!({"namespaces": [], "page_token": ""})),
            (204, Value::Null),
        ])
        .await;
        let mut options = RemoteCatalogOptions {
            api_key: Some("test-key".into()),
            ..Default::default()
        };
        options.client_config.extra_headers = HashMap::from([
            ("x-lancedb-database".into(), "wrong-static".into()),
            (
                "x-lancedb-database-prefix".into(),
                "wrong-static-prefix".into(),
            ),
        ]);
        options.client_config.header_provider = Some(Arc::new(AuthProvider));
        let catalog = CatalogConnection::new(Arc::new(
            RemoteCatalog::try_new(&endpoint, options).unwrap(),
        ));
        let page = catalog
            .list_databases(ListDatabasesRequest::default().limit(1).page_token("a/b"))
            .await
            .unwrap();
        assert_eq!(page.databases, ["team/search"]);
        assert_eq!(page.page_token.as_deref(), Some("next"));
        let first = catalog
            .create_database(CreateDatabaseRequest::new("team/search").exist_ok(true))
            .await
            .unwrap();
        first
            .database()
            .list_namespaces(ListNamespacesRequest::default())
            .await
            .unwrap();
        let second = catalog.connect_database("other").await.unwrap();
        second
            .database()
            .list_namespaces(ListNamespacesRequest::default())
            .await
            .unwrap();
        first
            .database()
            .list_namespaces(ListNamespacesRequest::default())
            .await
            .unwrap();
        assert!(
            catalog
                .list_databases(ListDatabasesRequest::default())
                .await
                .unwrap()
                .page_token
                .is_none()
        );
        catalog
            .drop_database(DropDatabaseRequest::new("team/search").ignore_missing(true))
            .await
            .unwrap();
        let requests = task.await.unwrap();
        for (i, request) in requests.iter().enumerate() {
            let database = match i {
                2 | 5 => Some("team/search"),
                4 => Some("other"),
                _ => None,
            };
            assert_eq!(
                request
                    .headers
                    .get("x-lancedb-database")
                    .map(String::as_str),
                database
            );
            assert!(!request.headers.contains_key("x-lancedb-database-prefix"));
            assert_eq!(request.headers["authorization"], "Bearer refreshed");
        }
        assert_eq!(
            requests[0].line,
            "GET /v1/namespace/%24/list?limit=1&page_token=a%2Fb HTTP/1.1"
        );
        assert_eq!(
            requests[1].line,
            "POST /v1/namespace/team%2Fsearch/create HTTP/1.1"
        );
        assert_eq!(requests[1].body, json!({"mode": "ExistOk"}));
        assert_eq!(
            requests[3].line,
            "POST /v1/namespace/other/describe HTTP/1.1"
        );
        assert_eq!(
            requests[7].body,
            json!({"mode": "Skip", "behavior": "Restrict"})
        );
    }

    #[tokio::test]
    async fn catalog_preserves_errors_and_never_cascades() {
        let (endpoint, task) = server(vec![
            (404, json!({"error": "missing"})),
            (409, json!({"error": "exists"})),
            (400, json!({"error": "not empty"})),
            (404, json!({"error": "missing"})),
            (404, json!({"error": "missing"})),
            (401, json!({"error": "unauthorized"})),
        ])
        .await;
        let catalog = RemoteCatalog::try_new(endpoint, RemoteCatalogOptions::default()).unwrap();
        assert!(matches!(
            catalog.connect_database("missing").await,
            Err(Error::DatabaseNotFound { .. })
        ));
        assert!(matches!(
            catalog.create_database("exists".into()).await,
            Err(Error::DatabaseAlreadyExists { .. })
        ));
        assert!(matches!(
            catalog.drop_database("full".into()).await,
            Err(Error::Http {
                status_code: Some(StatusCode::BAD_REQUEST),
                ..
            })
        ));
        assert!(matches!(
            catalog.drop_database("missing".into()).await,
            Err(Error::DatabaseNotFound { .. })
        ));
        catalog
            .drop_database(DropDatabaseRequest::new("missing").ignore_missing(true))
            .await
            .unwrap();
        assert!(
            catalog
                .list_databases(ListDatabasesRequest::default())
                .await
                .is_err()
        );
        let requests = task.await.unwrap();
        assert_eq!(requests[1].body, json!({"mode": "Create"}));
        assert_eq!(
            requests[2].body,
            json!({"mode": "Fail", "behavior": "Restrict"})
        );
    }

    #[tokio::test]
    async fn catalog_validates_before_sending_requests() {
        for endpoint in [
            "/tmp/catalog",
            "s3://bucket",
            "db://database",
            "https://user:pass@host",
            "https://host?q=1",
            "https://host#fragment",
        ] {
            assert!(RemoteCatalog::try_new(endpoint, RemoteCatalogOptions::default()).is_err());
        }
        let catalog =
            RemoteCatalog::try_new("http://127.0.0.1:1", RemoteCatalogOptions::default()).unwrap();
        for name in ["", "a$b", "\r\ninjected", "..", "café", " padded "] {
            assert!(matches!(
                catalog.create_database(name.into()).await,
                Err(Error::InvalidInput { .. })
            ));
            assert!(matches!(
                catalog.connect_database(name).await,
                Err(Error::InvalidInput { .. })
            ));
            assert!(matches!(
                catalog.drop_database(name.into()).await,
                Err(Error::InvalidInput { .. })
            ));
        }
        for limit in [0, u32::MAX] {
            assert!(matches!(
                catalog
                    .list_databases(ListDatabasesRequest::default().limit(limit))
                    .await,
                Err(Error::InvalidInput { .. })
            ));
        }
    }

    #[test]
    fn catalog_debug_redacts_credentials() {
        let mut options = RemoteCatalogOptions {
            api_key: Some("catalog-secret-key".into()),
            ..Default::default()
        };
        options
            .client_config
            .extra_headers
            .insert("authorization".into(), "Bearer catalog-secret-token".into());
        let catalog = RemoteCatalog::try_new("https://catalog.example", options).unwrap();
        let catalog_debug = format!("{catalog:?}");
        let connection = CatalogConnection::new(Arc::new(catalog));
        for debug in [catalog_debug, format!("{connection:?}")] {
            assert!(debug.contains("https://catalog.example"));
            assert!(!debug.contains("catalog-secret-key"));
            assert!(!debug.contains("catalog-secret-token"));
        }
    }
}
