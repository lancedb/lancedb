// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatchIterator;
use async_trait::async_trait;
use http::StatusCode;
use lance_io::object_store::StorageOptions;
use moka::future::Cache;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use tokio::task::spawn_blocking;

use crate::database::{
    CreateNamespaceRequest, CreateTableData, CreateTableMode, CreateTableRequest, Database,
    DatabaseOptions, DropNamespaceRequest, ListNamespacesRequest, OpenTableRequest,
    TableNamesRequest,
};
use crate::error::Result;
use crate::table::BaseTable;
use crate::Error;

use super::client::{ClientConfig, HttpSend, RequestResultExt, RestfulLanceDbClient, Sender};
use super::table::RemoteTable;
use super::util::{batches_to_ipc_bytes, parse_server_version};
use super::ARROW_STREAM_CONTENT_TYPE;

// the versions of the server that we support
// for any new feature that we need to change the SDK behavior, we should bump the server version,
// and add a feature flag as method of `ServerVersion` here.
pub const DEFAULT_SERVER_VERSION: semver::Version = semver::Version::new(0, 1, 0);
#[derive(Debug, Clone)]
pub struct ServerVersion(pub semver::Version);

impl Default for ServerVersion {
    fn default() -> Self {
        Self(DEFAULT_SERVER_VERSION.clone())
    }
}

impl ServerVersion {
    pub fn parse(version: &str) -> Result<Self> {
        let version = Self(
            semver::Version::parse(version).map_err(|e| Error::InvalidInput {
                message: e.to_string(),
            })?,
        );
        Ok(version)
    }

    pub fn support_multivector(&self) -> bool {
        self.0 >= semver::Version::new(0, 2, 0)
    }

    pub fn support_structural_fts(&self) -> bool {
        self.0 >= semver::Version::new(0, 3, 0)
    }
}

pub const OPT_REMOTE_PREFIX: &str = "remote_database_";
pub const OPT_REMOTE_API_KEY: &str = "remote_database_api_key";
pub const OPT_REMOTE_REGION: &str = "remote_database_region";
pub const OPT_REMOTE_HOST_OVERRIDE: &str = "remote_database_host_override";
// TODO: add support for configuring client config via key/value options

#[derive(Clone, Debug, Default)]
pub struct RemoteDatabaseOptions {
    /// The LanceDB Cloud API key
    pub api_key: Option<String>,
    /// The LanceDB Cloud region
    pub region: Option<String>,
    /// The LanceDB Enterprise host override
    ///
    /// This is required when connecting to LanceDB Enterprise and should be
    /// provided if using an on-premises LanceDB Enterprise instance.
    pub host_override: Option<String>,
    /// Storage options configure the storage layer (e.g. S3, GCS, Azure, etc.)
    ///
    /// See available options at <https://lancedb.github.io/lancedb/guides/storage/>
    ///
    /// These options are only used for LanceDB Enterprise and only a subset of options
    /// are supported.
    pub storage_options: HashMap<String, String>,
}

impl RemoteDatabaseOptions {
    pub fn builder() -> RemoteDatabaseOptionsBuilder {
        RemoteDatabaseOptionsBuilder::new()
    }

    pub(crate) fn parse_from_map(map: &HashMap<String, String>) -> Result<Self> {
        let api_key = map.get(OPT_REMOTE_API_KEY).cloned();
        let region = map.get(OPT_REMOTE_REGION).cloned();
        let host_override = map.get(OPT_REMOTE_HOST_OVERRIDE).cloned();
        let storage_options = map
            .iter()
            .filter(|(key, _)| !key.starts_with(OPT_REMOTE_PREFIX))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Ok(Self {
            api_key,
            region,
            host_override,
            storage_options,
        })
    }
}

impl DatabaseOptions for RemoteDatabaseOptions {
    fn serialize_into_map(&self, map: &mut HashMap<String, String>) {
        for (key, value) in &self.storage_options {
            map.insert(key.clone(), value.clone());
        }
        if let Some(api_key) = &self.api_key {
            map.insert(OPT_REMOTE_API_KEY.to_string(), api_key.clone());
        }
        if let Some(region) = &self.region {
            map.insert(OPT_REMOTE_REGION.to_string(), region.clone());
        }
        if let Some(host_override) = &self.host_override {
            map.insert(OPT_REMOTE_HOST_OVERRIDE.to_string(), host_override.clone());
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RemoteDatabaseOptionsBuilder {
    options: RemoteDatabaseOptions,
}

impl RemoteDatabaseOptionsBuilder {
    pub fn new() -> Self {
        Self {
            options: RemoteDatabaseOptions::default(),
        }
    }

    /// Set the LanceDB Cloud API key
    ///
    /// # Arguments
    ///
    /// * `api_key` - The LanceDB Cloud API key
    pub fn api_key(mut self, api_key: String) -> Self {
        self.options.api_key = Some(api_key);
        self
    }

    /// Set the LanceDB Cloud region
    ///
    /// # Arguments
    ///
    /// * `region` - The LanceDB Cloud region
    pub fn region(mut self, region: String) -> Self {
        self.options.region = Some(region);
        self
    }

    /// Set the LanceDB Enterprise host override
    ///
    /// # Arguments
    ///
    /// * `host_override` - The LanceDB Enterprise host override
    pub fn host_override(mut self, host_override: String) -> Self {
        self.options.host_override = Some(host_override);
        self
    }
}

#[derive(Deserialize)]
struct ListTablesResponse {
    tables: Vec<String>,
}

#[derive(Debug)]
pub struct RemoteDatabase<S: HttpSend = Sender> {
    client: RestfulLanceDbClient<S>,
    table_cache: Cache<String, Arc<RemoteTable<S>>>,
}

impl RemoteDatabase {
    pub fn try_new(
        uri: &str,
        api_key: &str,
        region: &str,
        host_override: Option<String>,
        client_config: ClientConfig,
        options: RemoteOptions,
    ) -> Result<Self> {
        let client = RestfulLanceDbClient::try_new(
            uri,
            api_key,
            region,
            host_override,
            client_config,
            &options,
        )?;

        let table_cache = Cache::builder()
            .time_to_live(std::time::Duration::from_secs(300))
            .max_capacity(10_000)
            .build();

        Ok(Self {
            client,
            table_cache,
        })
    }
}

#[cfg(all(test, feature = "remote"))]
mod test_utils {
    use super::*;
    use crate::remote::client::test_utils::MockSender;
    use crate::remote::client::test_utils::{client_with_handler, client_with_handler_and_config};
    use crate::remote::ClientConfig;

    impl RemoteDatabase<MockSender> {
        pub fn new_mock<F, T>(handler: F) -> Self
        where
            F: Fn(reqwest::Request) -> http::Response<T> + Send + Sync + 'static,
            T: Into<reqwest::Body>,
        {
            let client = client_with_handler(handler);
            Self {
                client,
                table_cache: Cache::new(0),
            }
        }

        pub fn new_mock_with_config<F, T>(handler: F, config: ClientConfig) -> Self
        where
            F: Fn(reqwest::Request) -> http::Response<T> + Send + Sync + 'static,
            T: Into<reqwest::Body>,
        {
            let client = client_with_handler_and_config(handler, config);
            Self {
                client,
                table_cache: Cache::new(0),
            }
        }
    }
}

impl<S: HttpSend> std::fmt::Display for RemoteDatabase<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RemoteDatabase(host={})", self.client.host())
    }
}

impl From<&CreateTableMode> for &'static str {
    fn from(val: &CreateTableMode) -> Self {
        match val {
            CreateTableMode::Create => "create",
            CreateTableMode::Overwrite => "overwrite",
            CreateTableMode::ExistOk(_) => "exist_ok",
        }
    }
}

fn build_table_identifier(name: &str, namespace: &[String], delimiter: &str) -> String {
    if !namespace.is_empty() {
        let mut parts = namespace.to_vec();
        parts.push(name.to_string());
        parts.join(delimiter)
    } else {
        name.to_string()
    }
}

fn build_namespace_identifier(namespace: &[String], delimiter: &str) -> String {
    if namespace.is_empty() {
        // According to the namespace spec, use delimiter to represent root namespace
        delimiter.to_string()
    } else {
        namespace.join(delimiter)
    }
}

/// Build a secure cache key using length prefixes.
/// This format is completely unambiguous regardless of delimiter or content.
/// Format: [u32_len][namespace1][u32_len][namespace2]...[u32_len][table_name]
/// Returns a hex-encoded string for use as a cache key.
fn build_cache_key(name: &str, namespace: &[String]) -> String {
    let mut key = Vec::new();

    // Add each namespace component with length prefix
    for ns in namespace {
        let bytes = ns.as_bytes();
        key.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        key.extend_from_slice(bytes);
    }

    // Add table name with length prefix
    let name_bytes = name.as_bytes();
    key.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    key.extend_from_slice(name_bytes);

    // Convert to hex string for use as a cache key
    key.iter().map(|b| format!("{:02x}", b)).collect()
}

#[async_trait]
impl<S: HttpSend> Database for RemoteDatabase<S> {
    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>> {
        let mut req = if !request.namespace.is_empty() {
            let namespace_id =
                build_namespace_identifier(&request.namespace, &self.client.id_delimiter);
            self.client
                .get(&format!("/v1/namespace/{}/table/list", namespace_id))
        } else {
            // TODO: use new API for all listing operations once stable
            self.client.get("/v1/table/")
        };

        if let Some(limit) = request.limit {
            req = req.query(&[("limit", limit)]);
        }
        if let Some(start_after) = request.start_after {
            req = req.query(&[("page_token", start_after)]);
        }
        let (request_id, rsp) = self.client.send_with_retry(req, None, true).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let version = parse_server_version(&request_id, &rsp)?;
        let tables = rsp
            .json::<ListTablesResponse>()
            .await
            .err_to_http(request_id)?
            .tables;
        for table in &tables {
            let table_identifier =
                build_table_identifier(table, &request.namespace, &self.client.id_delimiter);
            let cache_key = build_cache_key(table, &request.namespace);
            let remote_table = Arc::new(RemoteTable::new(
                self.client.clone(),
                table.clone(),
                request.namespace.clone(),
                table_identifier.clone(),
                version.clone(),
            ));
            self.table_cache.insert(cache_key, remote_table).await;
        }
        Ok(tables)
    }

    async fn create_table(&self, request: CreateTableRequest) -> Result<Arc<dyn BaseTable>> {
        let data = match request.data {
            CreateTableData::Data(data) => data,
            CreateTableData::StreamingData(_) => {
                return Err(Error::NotSupported {
                    message: "Creating a remote table from a streaming source".to_string(),
                })
            }
            CreateTableData::Empty(table_definition) => {
                let schema = table_definition.schema.clone();
                Box::new(RecordBatchIterator::new(vec![], schema))
            }
        };

        // TODO: https://github.com/lancedb/lancedb/issues/1026
        // We should accept data from an async source.  In the meantime, spawn this as blocking
        // to make sure we don't block the tokio runtime if the source is slow.
        let data_buffer = spawn_blocking(move || batches_to_ipc_bytes(data))
            .await
            .unwrap()?;

        let identifier =
            build_table_identifier(&request.name, &request.namespace, &self.client.id_delimiter);
        let req = self
            .client
            .post(&format!("/v1/table/{}/create/", identifier))
            .query(&[("mode", Into::<&str>::into(&request.mode))])
            .body(data_buffer)
            .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE);

        let (request_id, rsp) = self.client.send(req).await?;

        if rsp.status() == StatusCode::BAD_REQUEST {
            let body = rsp.text().await.err_to_http(request_id.clone())?;
            if body.contains("already exists") {
                return match request.mode {
                    CreateTableMode::Create => {
                        Err(crate::Error::TableAlreadyExists { name: request.name })
                    }
                    CreateTableMode::ExistOk(callback) => {
                        let req = OpenTableRequest {
                            name: request.name.clone(),
                            namespace: request.namespace.clone(),
                            index_cache_size: None,
                            lance_read_params: None,
                        };
                        let req = (callback)(req);
                        self.open_table(req).await
                    }

                    // This should not happen, as we explicitly set the mode to overwrite and the server
                    // shouldn't return an error if the table already exists.
                    //
                    // However if the server is an older version that doesn't support the mode parameter,
                    // then we'll get the 400 response.
                    CreateTableMode::Overwrite => Err(crate::Error::Http {
                        source: format!(
                            "unexpected response from server for create mode overwrite: {}",
                            body
                        )
                        .into(),
                        request_id,
                        status_code: Some(StatusCode::BAD_REQUEST),
                    }),
                };
            } else {
                return Err(crate::Error::InvalidInput { message: body });
            }
        }
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let version = parse_server_version(&request_id, &rsp)?;
        let table_identifier =
            build_table_identifier(&request.name, &request.namespace, &self.client.id_delimiter);
        let cache_key = build_cache_key(&request.name, &request.namespace);
        let table = Arc::new(RemoteTable::new(
            self.client.clone(),
            request.name.clone(),
            request.namespace.clone(),
            table_identifier,
            version,
        ));
        self.table_cache.insert(cache_key, table.clone()).await;

        Ok(table)
    }

    async fn open_table(&self, request: OpenTableRequest) -> Result<Arc<dyn BaseTable>> {
        let identifier =
            build_table_identifier(&request.name, &request.namespace, &self.client.id_delimiter);
        let cache_key = build_cache_key(&request.name, &request.namespace);

        // We describe the table to confirm it exists before moving on.
        if let Some(table) = self.table_cache.get(&cache_key).await {
            Ok(table.clone())
        } else {
            let req = self
                .client
                .post(&format!("/v1/table/{}/describe/", identifier));
            let (request_id, rsp) = self.client.send_with_retry(req, None, true).await?;
            if rsp.status() == StatusCode::NOT_FOUND {
                return Err(crate::Error::TableNotFound {
                    name: identifier.clone(),
                });
            }
            let rsp = self.client.check_response(&request_id, rsp).await?;
            let version = parse_server_version(&request_id, &rsp)?;
            let table_identifier = build_table_identifier(
                &request.name,
                &request.namespace,
                &self.client.id_delimiter,
            );
            let table = Arc::new(RemoteTable::new(
                self.client.clone(),
                request.name.clone(),
                request.namespace.clone(),
                table_identifier,
                version,
            ));
            let cache_key = build_cache_key(&request.name, &request.namespace);
            self.table_cache.insert(cache_key, table.clone()).await;
            Ok(table)
        }
    }

    async fn rename_table(
        &self,
        current_name: &str,
        new_name: &str,
        cur_namespace: &[String],
        new_namespace: &[String],
    ) -> Result<()> {
        let current_identifier =
            build_table_identifier(current_name, cur_namespace, &self.client.id_delimiter);
        let current_cache_key = build_cache_key(current_name, cur_namespace);
        let new_cache_key = build_cache_key(new_name, new_namespace);

        let mut body = serde_json::json!({ "new_table_name": new_name });
        if !new_namespace.is_empty() {
            body["new_namespace"] = serde_json::Value::Array(
                new_namespace
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            );
        }
        let req = self
            .client
            .post(&format!("/v1/table/{}/rename/", current_identifier))
            .json(&body);
        let (request_id, resp) = self.client.send(req).await?;
        self.client.check_response(&request_id, resp).await?;
        let table = self.table_cache.remove(&current_cache_key).await;
        if let Some(table) = table {
            self.table_cache.insert(new_cache_key, table).await;
        }
        Ok(())
    }

    async fn drop_table(&self, name: &str, namespace: &[String]) -> Result<()> {
        let identifier = build_table_identifier(name, namespace, &self.client.id_delimiter);
        let cache_key = build_cache_key(name, namespace);
        let req = self.client.post(&format!("/v1/table/{}/drop/", identifier));
        let (request_id, resp) = self.client.send(req).await?;
        self.client.check_response(&request_id, resp).await?;
        self.table_cache.remove(&cache_key).await;
        Ok(())
    }

    async fn drop_all_tables(&self, namespace: &[String]) -> Result<()> {
        // TODO: Implement namespace-aware drop_all_tables
        let _namespace = namespace; // Suppress unused warning for now
        Err(crate::Error::NotSupported {
            message: "Dropping all tables is not currently supported in the remote API".to_string(),
        })
    }

    async fn list_namespaces(&self, request: ListNamespacesRequest) -> Result<Vec<String>> {
        let namespace_id =
            build_namespace_identifier(request.namespace.as_slice(), &self.client.id_delimiter);
        let mut req = self
            .client
            .get(&format!("/v1/namespace/{}/list", namespace_id));
        if let Some(limit) = request.limit {
            req = req.query(&[("limit", limit)]);
        }
        if let Some(page_token) = request.page_token {
            req = req.query(&[("page_token", page_token)]);
        }

        let (request_id, resp) = self.client.send(req).await?;
        let resp = self.client.check_response(&request_id, resp).await?;

        #[derive(Deserialize)]
        struct ListNamespacesResponse {
            namespaces: Vec<String>,
        }

        let parsed: ListNamespacesResponse = resp.json().await.map_err(|e| Error::Runtime {
            message: format!("Failed to parse namespace response: {}", e),
        })?;
        Ok(parsed.namespaces)
    }

    async fn create_namespace(&self, request: CreateNamespaceRequest) -> Result<()> {
        let namespace_id =
            build_namespace_identifier(request.namespace.as_slice(), &self.client.id_delimiter);
        let req = self
            .client
            .post(&format!("/v1/namespace/{}/create", namespace_id));
        let (request_id, resp) = self.client.send(req).await?;
        self.client.check_response(&request_id, resp).await?;
        Ok(())
    }

    async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<()> {
        let namespace_id =
            build_namespace_identifier(request.namespace.as_slice(), &self.client.id_delimiter);
        let req = self
            .client
            .post(&format!("/v1/namespace/{}/drop", namespace_id));
        let (request_id, resp) = self.client.send(req).await?;
        self.client.check_response(&request_id, resp).await?;
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// RemoteOptions contains a subset of StorageOptions that are compatible with Remote LanceDB connections
#[derive(Clone, Debug, Default)]
pub struct RemoteOptions(pub HashMap<String, String>);

impl RemoteOptions {
    pub fn new(options: HashMap<String, String>) -> Self {
        Self(options)
    }
}

impl From<StorageOptions> for RemoteOptions {
    fn from(options: StorageOptions) -> Self {
        let supported_opts = vec!["account_name", "azure_storage_account_name"];
        let mut filtered = HashMap::new();
        for opt in supported_opts {
            if let Some(v) = options.0.get(opt) {
                filtered.insert(opt.to_string(), v.to_string());
            }
        }
        Self::new(filtered)
    }
}

#[cfg(test)]
mod tests {
    use super::build_cache_key;
    use std::collections::HashMap;
    use std::sync::{Arc, OnceLock};

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};

    use crate::connection::ConnectBuilder;
    use crate::{
        database::CreateTableMode,
        remote::{ClientConfig, HeaderProvider, ARROW_STREAM_CONTENT_TYPE, JSON_CONTENT_TYPE},
        Connection, Error,
    };

    #[test]
    fn test_cache_key_security() {
        // Test that cache keys are unique regardless of delimiter manipulation

        // Case 1: Different delimiters should not affect cache key
        let key1 = build_cache_key("table1", &["ns1".to_string(), "ns2".to_string()]);
        let key2 = build_cache_key("table1", &["ns1$ns2".to_string()]);
        assert_ne!(
            key1, key2,
            "Cache keys should differ for different namespace structures"
        );

        // Case 2: Table name containing delimiter should not cause collision
        let key3 = build_cache_key("ns2$table1", &["ns1".to_string()]);
        assert_ne!(
            key1, key3,
            "Cache key should be different when table name contains delimiter"
        );

        // Case 3: Empty namespace vs namespace with empty string
        let key4 = build_cache_key("table1", &[]);
        let key5 = build_cache_key("table1", &["".to_string()]);
        assert_ne!(
            key4, key5,
            "Empty namespace should differ from namespace with empty string"
        );

        // Case 4: Verify same inputs produce same key (consistency)
        let key6 = build_cache_key("table1", &["ns1".to_string(), "ns2".to_string()]);
        assert_eq!(key1, key6, "Same inputs should produce same cache key");
    }

    #[tokio::test]
    async fn test_retries() {
        // We'll record the request_id here, to check it matches the one in the error.
        let seen_request_id = Arc::new(OnceLock::new());
        let seen_request_id_ref = seen_request_id.clone();
        let conn = Connection::new_with_handler(move |request| {
            // Request id should be the same on each retry.
            let request_id = request.headers()["x-request-id"]
                .to_str()
                .unwrap()
                .to_string();
            let seen_id = seen_request_id_ref.get_or_init(|| request_id.clone());
            assert_eq!(&request_id, seen_id);

            http::Response::builder()
                .status(500)
                .body("internal server error")
                .unwrap()
        });
        let result = conn.table_names().execute().await;
        if let Err(Error::Retry {
            request_id,
            request_failures,
            max_request_failures,
            source,
            ..
        }) = result
        {
            let expected_id = seen_request_id.get().unwrap();
            assert_eq!(&request_id, expected_id);
            assert_eq!(request_failures, max_request_failures);
            assert!(
                source.to_string().contains("internal server error"),
                "source: {:?}",
                source
            );
        } else {
            panic!("unexpected result: {:?}", result);
        };
    }

    #[tokio::test]
    async fn test_table_names() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::GET);
            assert_eq!(request.url().path(), "/v1/table/");
            assert_eq!(request.url().query(), None);

            http::Response::builder()
                .status(200)
                .body(r#"{"tables": ["table1", "table2"]}"#)
                .unwrap()
        });
        let names = conn.table_names().execute().await.unwrap();
        assert_eq!(names, vec!["table1", "table2"]);
    }

    #[tokio::test]
    async fn test_table_names_pagination() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::GET);
            assert_eq!(request.url().path(), "/v1/table/");
            assert!(request.url().query().unwrap().contains("limit=2"));
            assert!(request.url().query().unwrap().contains("page_token=table2"));

            http::Response::builder()
                .status(200)
                .body(r#"{"tables": ["table3", "table4"], "page_token": "token"}"#)
                .unwrap()
        });
        let names = conn
            .table_names()
            .start_after("table2")
            .limit(2)
            .execute()
            .await
            .unwrap();
        assert_eq!(names, vec!["table3", "table4"]);
    }

    #[tokio::test]
    async fn test_open_table() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/table1/describe/");
            assert_eq!(request.url().query(), None);

            http::Response::builder()
                .status(200)
                .body(r#"{"table": "table1"}"#)
                .unwrap()
        });
        let table = conn.open_table("table1").execute().await.unwrap();
        assert_eq!(table.name(), "table1");

        // Storage options should be ignored.
        let table = conn
            .open_table("table1")
            .storage_option("key", "value")
            .execute()
            .await
            .unwrap();
        assert_eq!(table.name(), "table1");
    }

    #[tokio::test]
    async fn test_open_table_not_found() {
        let conn = Connection::new_with_handler(|_| {
            http::Response::builder()
                .status(404)
                .body("table not found")
                .unwrap()
        });
        let result = conn.open_table("table1").execute().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(crate::Error::TableNotFound { .. })));
    }

    #[tokio::test]
    async fn test_create_table() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/table1/create/");
            assert_eq!(
                request
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .unwrap(),
                ARROW_STREAM_CONTENT_TYPE.as_bytes()
            );

            http::Response::builder().status(200).body("").unwrap()
        });
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let table = conn.create_table("table1", reader).execute().await.unwrap();
        assert_eq!(table.name(), "table1");
    }

    #[tokio::test]
    async fn test_create_table_already_exists() {
        let conn = Connection::new_with_handler(|_| {
            http::Response::builder()
                .status(400)
                .body("table table1 already exists")
                .unwrap()
        });
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let result = conn.create_table("table1", reader).execute().await;
        assert!(result.is_err());
        assert!(
            matches!(result, Err(crate::Error::TableAlreadyExists { name }) if name == "table1")
        );
    }

    #[tokio::test]
    async fn test_create_table_modes() {
        let test_cases = [
            (None, "mode=create"),
            (Some(CreateTableMode::Create), "mode=create"),
            (Some(CreateTableMode::Overwrite), "mode=overwrite"),
            (
                Some(CreateTableMode::ExistOk(Box::new(|b| b))),
                "mode=exist_ok",
            ),
        ];

        for (mode, expected_query_string) in test_cases {
            let conn = Connection::new_with_handler(move |request| {
                assert_eq!(request.method(), &reqwest::Method::POST);
                assert_eq!(request.url().path(), "/v1/table/table1/create/");
                assert_eq!(request.url().query(), Some(expected_query_string));

                http::Response::builder().status(200).body("").unwrap()
            });

            let data = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let reader = RecordBatchIterator::new([Ok(data.clone())], data.schema());
            let mut builder = conn.create_table("table1", reader);
            if let Some(mode) = mode {
                builder = builder.mode(mode);
            }
            builder.execute().await.unwrap();
        }

        // check that the open table callback is called with exist_ok
        let conn = Connection::new_with_handler(|request| match request.url().path() {
            "/v1/table/table1/create/" => http::Response::builder()
                .status(400)
                .body("Table table1 already exists")
                .unwrap(),
            "/v1/table/table1/describe/" => http::Response::builder().status(200).body("").unwrap(),
            _ => {
                panic!("unexpected path: {:?}", request.url().path());
            }
        });
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let called: Arc<OnceLock<bool>> = Arc::new(OnceLock::new());
        let reader = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let called_in_cb = called.clone();
        conn.create_table("table1", reader)
            .mode(CreateTableMode::ExistOk(Box::new(move |b| {
                called_in_cb.clone().set(true).unwrap();
                b
            })))
            .execute()
            .await
            .unwrap();

        let called = *called.get().unwrap_or(&false);
        assert!(called);
    }

    #[tokio::test]
    async fn test_create_table_empty() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/table1/create/");
            assert_eq!(
                request
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .unwrap(),
                ARROW_STREAM_CONTENT_TYPE.as_bytes()
            );

            http::Response::builder().status(200).body("").unwrap()
        });
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        conn.create_empty_table("table1", schema)
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_drop_table() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/table1/drop/");
            assert_eq!(request.url().query(), None);
            assert!(request.body().is_none());

            http::Response::builder().status(200).body("").unwrap()
        });
        conn.drop_table("table1", &[]).await.unwrap();
        // NOTE: the API will return 200 even if the table does not exist. So we shouldn't expect 404.
    }

    #[tokio::test]
    async fn test_rename_table() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/table1/rename/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(body["new_table_name"], "table2");

            http::Response::builder().status(200).body("").unwrap()
        });
        conn.rename_table("table1", "table2", &[], &[])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_connect_remote_options() {
        let db_uri = "db://my-container/my-prefix";
        let _ = ConnectBuilder::new(db_uri)
            .region("us-east-1")
            .api_key("my-api-key")
            .storage_options(vec![("azure_storage_account_name", "my-storage-account")])
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_table_names_with_root_namespace() {
        // When namespace is empty (root namespace), should use /v1/table/ for backwards compatibility
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::GET);
            assert_eq!(request.url().path(), "/v1/table/");
            assert_eq!(request.url().query(), None);

            http::Response::builder()
                .status(200)
                .body(r#"{"tables": ["table1", "table2"]}"#)
                .unwrap()
        });
        let names = conn
            .table_names()
            .namespace(vec![])
            .execute()
            .await
            .unwrap();
        assert_eq!(names, vec!["table1", "table2"]);
    }

    #[tokio::test]
    async fn test_table_names_with_namespace() {
        // When namespace is non-empty, should use /v1/namespace/{id}/table/list
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::GET);
            assert_eq!(request.url().path(), "/v1/namespace/test/table/list");
            assert_eq!(request.url().query(), None);

            http::Response::builder()
                .status(200)
                .body(r#"{"tables": ["table1", "table2"]}"#)
                .unwrap()
        });
        let names = conn
            .table_names()
            .namespace(vec!["test".to_string()])
            .execute()
            .await
            .unwrap();
        assert_eq!(names, vec!["table1", "table2"]);
    }

    #[tokio::test]
    async fn test_table_names_with_nested_namespace() {
        // When namespace is vec!["ns1", "ns2"], should use /v1/namespace/ns1$ns2/table/list
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::GET);
            assert_eq!(request.url().path(), "/v1/namespace/ns1$ns2/table/list");
            assert_eq!(request.url().query(), None);

            http::Response::builder()
                .status(200)
                .body(r#"{"tables": ["ns1$ns2$table1", "ns1$ns2$table2"]}"#)
                .unwrap()
        });
        let names = conn
            .table_names()
            .namespace(vec!["ns1".to_string(), "ns2".to_string()])
            .execute()
            .await
            .unwrap();
        assert_eq!(names, vec!["ns1$ns2$table1", "ns1$ns2$table2"]);
    }

    #[tokio::test]
    async fn test_open_table_with_namespace() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/ns1$ns2$table1/describe/");
            assert_eq!(request.url().query(), None);

            http::Response::builder()
                .status(200)
                .body(r#"{"table": "table1"}"#)
                .unwrap()
        });
        let table = conn
            .open_table("table1")
            .namespace(vec!["ns1".to_string(), "ns2".to_string()])
            .execute()
            .await
            .unwrap();
        assert_eq!(table.name(), "table1");
    }

    #[tokio::test]
    async fn test_create_table_with_namespace() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/ns1$table1/create/");
            assert_eq!(
                request
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .unwrap(),
                ARROW_STREAM_CONTENT_TYPE.as_bytes()
            );

            http::Response::builder().status(200).body("").unwrap()
        });
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let table = conn
            .create_table("table1", reader)
            .namespace(vec!["ns1".to_string()])
            .execute()
            .await
            .unwrap();
        assert_eq!(table.name(), "table1");
    }

    #[tokio::test]
    async fn test_drop_table_with_namespace() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/ns1$ns2$table1/drop/");
            assert_eq!(request.url().query(), None);
            assert!(request.body().is_none());

            http::Response::builder().status(200).body("").unwrap()
        });
        conn.drop_table("table1", &["ns1".to_string(), "ns2".to_string()])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_rename_table_with_namespace() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/ns1$table1/rename/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(body["new_table_name"], "table2");
            assert_eq!(body["new_namespace"], serde_json::json!(["ns2"]));

            http::Response::builder().status(200).body("").unwrap()
        });
        conn.rename_table(
            "table1",
            "table2",
            &["ns1".to_string()],
            &["ns2".to_string()],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_create_empty_table_with_namespace() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/prod$data$metrics/create/");
            assert_eq!(
                request
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .unwrap(),
                ARROW_STREAM_CONTENT_TYPE.as_bytes()
            );

            http::Response::builder().status(200).body("").unwrap()
        });
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        conn.create_empty_table("metrics", schema)
            .namespace(vec!["prod".to_string(), "data".to_string()])
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_header_provider_in_request() {
        // Test HeaderProvider implementation that adds custom headers
        #[derive(Debug, Clone)]
        struct TestHeaderProvider {
            headers: HashMap<String, String>,
        }

        #[async_trait::async_trait]
        impl HeaderProvider for TestHeaderProvider {
            async fn get_headers(&self) -> crate::Result<HashMap<String, String>> {
                Ok(self.headers.clone())
            }
        }

        // Create a test header provider with custom headers
        let mut headers = HashMap::new();
        headers.insert("X-Custom-Auth".to_string(), "test-token".to_string());
        headers.insert("X-Request-Id".to_string(), "test-123".to_string());
        let provider = Arc::new(TestHeaderProvider { headers }) as Arc<dyn HeaderProvider>;

        // Create client config with the header provider
        let client_config = ClientConfig {
            header_provider: Some(provider),
            ..Default::default()
        };

        // Create connection with handler that verifies the headers are present
        let conn = Connection::new_with_handler_and_config(
            move |request| {
                // Verify that our custom headers are present
                assert_eq!(
                    request.headers().get("X-Custom-Auth").unwrap(),
                    "test-token"
                );
                assert_eq!(request.headers().get("X-Request-Id").unwrap(), "test-123");

                // Also check standard headers are still there
                assert_eq!(request.method(), &reqwest::Method::GET);
                assert_eq!(request.url().path(), "/v1/table/");

                http::Response::builder()
                    .status(200)
                    .body(r#"{"tables": ["table1", "table2"]}"#)
                    .unwrap()
            },
            client_config,
        );

        // Make a request that should include the custom headers
        let names = conn.table_names().execute().await.unwrap();
        assert_eq!(names, vec!["table1", "table2"]);
    }

    #[tokio::test]
    async fn test_header_provider_error_handling() {
        // Test HeaderProvider that returns an error
        #[derive(Debug)]
        struct ErrorHeaderProvider;

        #[async_trait::async_trait]
        impl HeaderProvider for ErrorHeaderProvider {
            async fn get_headers(&self) -> crate::Result<HashMap<String, String>> {
                Err(crate::Error::Runtime {
                    message: "Failed to fetch auth token".to_string(),
                })
            }
        }

        let provider = Arc::new(ErrorHeaderProvider) as Arc<dyn HeaderProvider>;
        let client_config = ClientConfig {
            header_provider: Some(provider),
            ..Default::default()
        };

        // Create connection - handler won't be called because header provider fails
        let conn = Connection::new_with_handler_and_config(
            move |_request| -> http::Response<&'static str> {
                panic!("Handler should not be called when header provider fails");
            },
            client_config,
        );

        // Request should fail due to header provider error
        let result = conn.table_names().execute().await;
        assert!(result.is_err());

        match result.unwrap_err() {
            crate::Error::Runtime { message } => {
                assert_eq!(message, "Failed to fetch auth token");
            }
            _ => panic!("Expected Runtime error from header provider"),
        }
    }
}
