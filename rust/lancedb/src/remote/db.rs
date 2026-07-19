// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use lance_io::object_store::StorageOptions;
use lance_namespace_impls::{DynamicContextProvider, OperationInfo};
use moka::future::Cache;
use reqwest::header::CONTENT_TYPE;

use lance_namespace::models::{
    CreateNamespaceRequest, CreateNamespaceResponse, DescribeNamespaceRequest,
    DescribeNamespaceResponse, DropNamespaceRequest, DropNamespaceResponse, ListNamespacesRequest,
    ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
};

use crate::Error;
use crate::database::{
    CloneTableRequest, CreateFunctionRequest, CreateMaterializedViewRequest, CreateTableMode,
    CreateTableRequest, Database, DatabaseOptions, FunctionInfo, JobErrorInfo, JobHistoryInfo,
    JobInfo, MaterializedViewInfo, MvRefreshPlan, OpenTableRequest, PlatformJobDescription,
    ReadConsistency, RefreshMaterializedViewRequest, TableLineageRequest, TableNamesRequest,
};
use crate::error::Result;
use crate::remote::util::stream_as_body;
use crate::table::BaseTable;

use super::ARROW_STREAM_CONTENT_TYPE;
use super::client::{
    ClientConfig, HeaderProvider, HttpSend, RequestResultExt, RestfulLanceDbClient, Sender,
};
use super::table::RemoteTable;
use super::util::parse_server_version;

// Wire types for the derived-compute routes (functions, materialized
// views, jobs). Field shapes mirror the server's REST contract.
#[derive(serde::Serialize)]
struct RemoteCreateFunctionRequest {
    language: String,
    return_type: String,
    body: String,
    options: std::collections::HashMap<String, String>,
}

#[derive(serde::Deserialize)]
struct RemoteFunctionEntry {
    name: String,
    language: String,
    return_type: String,
    #[serde(default)]
    description: String,
}

#[derive(serde::Deserialize)]
struct RemoteListFunctionsResponse {
    functions: Vec<RemoteFunctionEntry>,
}

#[derive(serde::Serialize)]
struct RemoteCreateMaterializedViewRequest {
    query: String,
    auto_refresh: bool,
    with_no_data: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    partition_by: Option<String>,
}

#[derive(serde::Deserialize)]
struct RemoteCreateMaterializedViewResponse {
    #[serde(default)]
    job_id: Option<String>,
}

#[derive(serde::Serialize)]
struct RemoteRefreshMaterializedViewRequest {
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    full: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    src_version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    num_workers: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_workers: Option<u32>,
}

#[derive(serde::Deserialize)]
struct RemoteRefreshMaterializedViewResponse {
    job_id: String,
}

#[derive(serde::Serialize)]
struct RemoteExplainRefreshRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    full: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    src_version: Option<u64>,
}

#[derive(serde::Deserialize)]
struct RemoteExplainRefreshResponse {
    table_name: String,
    has_work: bool,
    source_version: u64,
    last_refreshed_version: Option<u64>,
    full_refresh: bool,
    rebuild: bool,
    units_total: u64,
}

#[derive(serde::Serialize)]
struct RemoteAlterMaterializedViewRequest {
    auto_refresh: bool,
}

#[derive(serde::Deserialize)]
struct RemoteMaterializedViewEntry {
    name: String,
    source_table: String,
    #[serde(default)]
    projection: Vec<String>,
    #[serde(default)]
    udf_columns: Vec<String>,
    #[serde(default)]
    filter: Option<String>,
    #[serde(default)]
    auto_refresh: bool,
}

#[derive(serde::Deserialize)]
struct RemoteListMaterializedViewsResponse {
    views: Vec<RemoteMaterializedViewEntry>,
}

#[derive(serde::Deserialize)]
struct RemoteDescribePlatformJobResponse {
    job_id: String,
    job_type: String,
    #[serde(default)]
    job_subtype: String,
    job_state: String,
    #[serde(default)]
    creation_ms: i64,
    #[serde(default)]
    status: serde_json::Value,
}

#[derive(serde::Deserialize)]
struct RemoteListPlatformJobsResponse {
    #[serde(default)]
    jobs: Vec<RemotePlatformJobRow>,
}

#[derive(serde::Deserialize)]
struct RemotePlatformJobRow {
    job_id: String,
    #[serde(default)]
    table: String,
    #[serde(default)]
    job_subtype: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    created_at_millis: i64,
    #[serde(default)]
    status: serde_json::Value,
}

/// Platform list-row state -> the client's job vocabulary.
fn platform_state_to_client(state: &str) -> String {
    match state {
        "in_progress" => "running",
        "done" => "finished",
        other => other,
    }
    .to_string()
}

/// Describe job_state -> the client's job vocabulary.
fn describe_state_to_client(state: &str) -> String {
    match state {
        "IN_PROGRESS" => "running",
        "DONE" => "finished",
        "FAILED" => "failed",
        "CANCELLED" => "cancelled",
        other => other,
    }
    .to_string()
}

fn payload_i64(status: &serde_json::Value, key: &str) -> Option<i64> {
    status.get(key).and_then(serde_json::Value::as_i64)
}

fn payload_error(status: &serde_json::Value) -> Option<String> {
    status
        .get("error")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

#[derive(serde::Deserialize)]
struct RemoteErrorEntry {
    job_id: String,
    table: String,
    column: String,
    error_type: String,
    error_message: String,
    #[serde(default)]
    fragment_id: Option<i64>,
    #[serde(default)]
    source_row_id: Option<i64>,
    #[serde(default)]
    table_version: Option<i64>,
    #[serde(default)]
    age_seconds: Option<i64>,
}

#[derive(serde::Deserialize)]
struct RemoteErrorsResponse {
    errors: Vec<RemoteErrorEntry>,
}

impl From<RemoteErrorEntry> for JobErrorInfo {
    fn from(e: RemoteErrorEntry) -> Self {
        Self {
            job_id: e.job_id,
            table: e.table,
            column: e.column,
            error_type: e.error_type,
            error_message: e.error_message,
            fragment_id: e.fragment_id,
            source_row_id: e.source_row_id,
            table_version: e.table_version,
            age_seconds: e.age_seconds,
        }
    }
}

// Request structure for the remote clone table API
#[derive(serde::Serialize)]
struct RemoteCloneTableRequest {
    source_location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_shallow: Option<bool>,
}

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

    pub fn support_multipart_write(&self) -> bool {
        self.0 >= semver::Version::new(0, 4, 0)
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
    /// See available options at <https://docs.lancedb.com/storage/>
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

#[derive(Debug)]
pub struct RemoteDatabase<S: HttpSend = Sender> {
    client: RestfulLanceDbClient<S>,
    table_cache: Cache<String, Arc<RemoteTable<S>>>,
    uri: String,
    /// Headers to pass to the namespace client for authentication
    namespace_headers: HashMap<String, String>,
    namespace_context_provider: Option<Arc<dyn DynamicContextProvider>>,
    /// TLS configuration for mTLS support
    tls_config: Option<super::client::TlsConfig>,
}

#[derive(Clone)]
struct NamespaceHeaderProviderContext {
    header_provider: Arc<dyn HeaderProvider>,
}

impl std::fmt::Debug for NamespaceHeaderProviderContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceHeaderProviderContext")
            .field("header_provider", &"Some(...)")
            .finish()
    }
}

impl DynamicContextProvider for NamespaceHeaderProviderContext {
    fn provide_context(&self, _info: &OperationInfo) -> HashMap<String, String> {
        let header_provider = Arc::clone(&self.header_provider);
        let handle = match std::thread::Builder::new()
            .name("lancedb-namespace-headers".to_string())
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| Error::Runtime {
                        message: format!(
                            "Failed to create runtime for namespace header provider: {e}"
                        ),
                    })?
                    .block_on(header_provider.get_headers())
            }) {
            Ok(handle) => handle,
            Err(err) => {
                log::warn!("Failed to spawn dynamic namespace header provider thread: {err}");
                return HashMap::new();
            }
        };

        let headers = handle.join();

        match headers {
            Ok(Ok(headers)) => headers
                .into_iter()
                .map(|(key, value)| (format!("headers.{key}"), value))
                .collect(),
            Ok(Err(err)) => {
                log::warn!("Failed to get dynamic namespace headers: {err}");
                HashMap::new()
            }
            Err(_) => {
                log::warn!("Dynamic namespace header provider panicked");
                HashMap::new()
            }
        }
    }
}

impl RemoteDatabase {
    pub fn try_new(
        uri: &str,
        api_key: &str,
        region: &str,
        host_override: Option<String>,
        client_config: ClientConfig,
        options: RemoteOptions,
        read_consistency_interval: Option<std::time::Duration>,
    ) -> Result<Self> {
        let parsed = super::client::parse_db_url(uri)?;
        let header_map = RestfulLanceDbClient::<Sender>::default_headers(
            api_key,
            region,
            &parsed.db_name,
            host_override.is_some(),
            &options,
            parsed.db_prefix.as_deref(),
            &client_config,
        )?;

        let namespace_headers: HashMap<String, String> = header_map
            .iter()
            .filter_map(|(k, v)| {
                v.to_str()
                    .ok()
                    .map(|val| (k.as_str().to_string(), val.to_string()))
            })
            .collect();

        let namespace_context_provider =
            client_config
                .header_provider
                .as_ref()
                .map(|header_provider| {
                    Arc::new(NamespaceHeaderProviderContext {
                        header_provider: Arc::clone(header_provider),
                    }) as Arc<dyn DynamicContextProvider>
                });

        let client = RestfulLanceDbClient::try_new(
            &parsed,
            region,
            host_override,
            header_map,
            client_config.clone(),
            read_consistency_interval,
        )?;

        let table_cache = Cache::builder()
            .time_to_live(std::time::Duration::from_secs(300))
            .max_capacity(10_000)
            .build();

        Ok(Self {
            client,
            table_cache,
            uri: uri.to_owned(),
            namespace_headers,
            namespace_context_provider,
            tls_config: client_config.tls_config,
        })
    }
}

#[cfg(all(test, feature = "remote"))]
mod test_utils {
    use super::*;
    use crate::remote::ClientConfig;
    use crate::remote::client::test_utils::MockSender;
    use crate::remote::client::test_utils::{client_with_handler, client_with_handler_and_config};

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
                uri: "http://localhost".to_string(),
                namespace_headers: HashMap::new(),
                namespace_context_provider: None,
                tls_config: None,
            }
        }

        pub fn new_mock_with_config<F, T>(handler: F, config: ClientConfig) -> Self
        where
            F: Fn(reqwest::Request) -> http::Response<T> + Send + Sync + 'static,
            T: Into<reqwest::Body>,
        {
            let client = client_with_handler_and_config(handler, config.clone());
            let namespace_context_provider =
                config.header_provider.as_ref().map(|header_provider| {
                    Arc::new(NamespaceHeaderProviderContext {
                        header_provider: Arc::clone(header_provider),
                    }) as Arc<dyn DynamicContextProvider>
                });
            Self {
                client,
                table_cache: Cache::new(0),
                uri: "http://localhost".to_string(),
                namespace_headers: config.extra_headers.clone(),
                namespace_context_provider,
                tls_config: config.tls_config.clone(),
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
    fn uri(&self) -> &str {
        &self.uri
    }

    async fn read_consistency(&self) -> Result<ReadConsistency> {
        Err(Error::NotSupported {
            message: "Getting the read consistency of a remote database is not yet supported"
                .to_string(),
        })
    }

    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>> {
        let mut req = if !request.namespace_path.is_empty() {
            let namespace_id =
                build_namespace_identifier(&request.namespace_path, &self.client.id_delimiter);
            self.client
                .get(&format!("/v1/namespace/{}/table/list", namespace_id))
        } else {
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
                build_table_identifier(table, &request.namespace_path, &self.client.id_delimiter);
            let cache_key = build_cache_key(table, &request.namespace_path);
            let remote_table = Arc::new(RemoteTable::new(
                self.client.clone(),
                table.clone(),
                request.namespace_path.clone(),
                table_identifier.clone(),
                version.clone(),
            ));
            self.table_cache.insert(cache_key, remote_table).await;
        }
        Ok(tables)
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        let namespace_parts = request.id.as_deref().unwrap_or(&[]);
        let namespace_id = build_namespace_identifier(namespace_parts, &self.client.id_delimiter);
        let mut req = self
            .client
            .get(&format!("/v1/namespace/{}/table/list", namespace_id));

        if let Some(limit) = request.limit {
            req = req.query(&[("limit", limit)]);
        }
        if let Some(ref page_token) = request.page_token {
            req = req.query(&[("page_token", page_token)]);
        }

        let (request_id, rsp) = self.client.send_with_retry(req, None, true).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let version = parse_server_version(&request_id, &rsp)?;
        let response: ListTablesResponse = rsp.json().await.err_to_http(request_id)?;

        // Cache the tables for future use
        let namespace_vec = namespace_parts.to_vec();
        for table in &response.tables {
            let table_identifier =
                build_table_identifier(table, &namespace_vec, &self.client.id_delimiter);
            let cache_key = build_cache_key(table, &namespace_vec);
            let remote_table = Arc::new(RemoteTable::new(
                self.client.clone(),
                table.clone(),
                namespace_vec.clone(),
                table_identifier.clone(),
                version.clone(),
            ));
            self.table_cache.insert(cache_key, remote_table).await;
        }

        Ok(response)
    }

    async fn create_table(&self, mut request: CreateTableRequest) -> Result<Arc<dyn BaseTable>> {
        let body = stream_as_body(request.data.scan_as_stream())?;

        let identifier = build_table_identifier(
            &request.name,
            &request.namespace_path,
            &self.client.id_delimiter,
        );
        let req = self
            .client
            .post(&format!("/v1/table/{}/create/", identifier))
            .query(&[("mode", Into::<&str>::into(&request.mode))])
            .body(body)
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
                            namespace_path: request.namespace_path.clone(),
                            index_cache_size: None,
                            lance_read_params: None,
                            location: None,
                            namespace_client: None,
                            managed_versioning: None,
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
        let table_identifier = build_table_identifier(
            &request.name,
            &request.namespace_path,
            &self.client.id_delimiter,
        );
        let cache_key = build_cache_key(&request.name, &request.namespace_path);
        let table = Arc::new(RemoteTable::new(
            self.client.clone(),
            request.name.clone(),
            request.namespace_path.clone(),
            table_identifier,
            version,
        ));
        self.table_cache.insert(cache_key, table.clone()).await;

        Ok(table)
    }

    async fn clone_table(&self, request: CloneTableRequest) -> Result<Arc<dyn BaseTable>> {
        let table_identifier = build_table_identifier(
            &request.target_table_name,
            &request.target_namespace_path,
            &self.client.id_delimiter,
        );

        let remote_request = RemoteCloneTableRequest {
            source_location: request.source_uri,
            source_version: request.source_version,
            source_tag: request.source_tag,
            is_shallow: Some(request.is_shallow),
        };

        let req = self
            .client
            .post(&format!("/v1/table/{}/clone", table_identifier.clone()))
            .json(&remote_request);

        let (request_id, rsp) = self.client.send(req).await?;

        let status = rsp.status();
        if status != StatusCode::OK {
            let body = rsp.text().await.err_to_http(request_id.clone())?;
            return Err(crate::Error::Http {
                source: format!("Failed to clone table: {}", body).into(),
                request_id,
                status_code: Some(status),
            });
        }

        let version = parse_server_version(&request_id, &rsp)?;
        let cache_key = build_cache_key(&request.target_table_name, &request.target_namespace_path);
        let table = Arc::new(RemoteTable::new(
            self.client.clone(),
            request.target_table_name.clone(),
            request.target_namespace_path.clone(),
            table_identifier,
            version,
        ));
        self.table_cache.insert(cache_key, table.clone()).await;

        Ok(table)
    }

    async fn create_function(&self, request: CreateFunctionRequest) -> Result<()> {
        let body = RemoteCreateFunctionRequest {
            language: request.language,
            return_type: request.return_type,
            body: request.body,
            options: request.options,
        };
        let req = self
            .client
            .post(&format!("/v1/function/{}/create", request.name))
            .json(&body);
        let (request_id, rsp) = self.client.send(req).await?;
        self.client.check_response(&request_id, rsp).await?;
        Ok(())
    }

    async fn list_functions(&self) -> Result<Vec<FunctionInfo>> {
        let req = self.client.get("/v1/function/list");
        let (request_id, rsp) = self.client.send(req).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let body: RemoteListFunctionsResponse = rsp.json().await.err_to_http(request_id)?;
        Ok(body
            .functions
            .into_iter()
            .map(|f| FunctionInfo {
                name: f.name,
                language: f.language,
                return_type: f.return_type,
                description: f.description,
            })
            .collect())
    }

    async fn drop_function(&self, name: &str) -> Result<()> {
        let req = self.client.post(&format!("/v1/function/{}/drop", name));
        let (request_id, rsp) = self.client.send(req).await?;
        self.client.check_response(&request_id, rsp).await?;
        Ok(())
    }

    async fn create_materialized_view(
        &self,
        request: CreateMaterializedViewRequest,
    ) -> Result<Option<String>> {
        let body = RemoteCreateMaterializedViewRequest {
            query: request.query,
            auto_refresh: request.auto_refresh,
            with_no_data: request.with_no_data,
            partition_by: request.partition_by,
        };
        let req = self
            .client
            .post(&format!("/v1/materialized_view/{}/create", request.name))
            .json(&body);
        let (request_id, rsp) = self.client.send(req).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let body: RemoteCreateMaterializedViewResponse =
            rsp.json().await.err_to_http(request_id)?;
        Ok(body.job_id)
    }

    async fn refresh_materialized_view(
        &self,
        request: RefreshMaterializedViewRequest,
    ) -> Result<String> {
        let body = RemoteRefreshMaterializedViewRequest {
            full: request.full,
            src_version: request.src_version,
            num_workers: request.num_workers,
            max_workers: request.max_workers,
        };
        let req = self
            .client
            .post(&format!("/v1/materialized_view/{}/refresh", request.name))
            .json(&body);
        let (request_id, rsp) = self.client.send(req).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let body: RemoteRefreshMaterializedViewResponse =
            rsp.json().await.err_to_http(request_id)?;
        Ok(body.job_id)
    }

    async fn table_lineage(&self, request: TableLineageRequest) -> Result<String> {
        let mut req = self
            .client
            .get(&format!("/v1/table/{}/lineage", request.name));
        if let Some(column) = &request.column {
            req = req.query(&[("column", column)]);
        }
        if let Some(direction) = &request.direction {
            req = req.query(&[("direction", direction)]);
        }
        if let Some(depth) = request.depth {
            req = req.query(&[("depth", depth.to_string())]);
        }
        let (request_id, rsp) = self.client.send(req).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        // Server-defined lineage JSON, returned opaque (the client does not
        // model the lineage schema; the Python layer deserializes it).
        rsp.text().await.err_to_http(request_id)
    }

    async fn explain_refresh_materialized_view(
        &self,
        name: &str,
        full: bool,
        src_version: Option<u64>,
    ) -> Result<MvRefreshPlan> {
        let body = RemoteExplainRefreshRequest {
            full: Some(full),
            src_version,
        };
        let req = self
            .client
            .post(&format!("/v1/materialized_view/{}/explain_refresh", name))
            .json(&body);
        let (request_id, rsp) = self.client.send(req).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let body: RemoteExplainRefreshResponse = rsp.json().await.err_to_http(request_id)?;
        Ok(MvRefreshPlan {
            table_name: body.table_name,
            has_work: body.has_work,
            source_version: body.source_version,
            last_refreshed_version: body.last_refreshed_version,
            full_refresh: body.full_refresh,
            rebuild: body.rebuild,
            units_total: body.units_total,
        })
    }

    async fn alter_materialized_view(&self, name: &str, auto_refresh: bool) -> Result<()> {
        let req = self
            .client
            .post(&format!("/v1/materialized_view/{}/alter", name))
            .json(&RemoteAlterMaterializedViewRequest { auto_refresh });
        let (request_id, rsp) = self.client.send(req).await?;
        self.client.check_response(&request_id, rsp).await?;
        Ok(())
    }

    async fn drop_materialized_view(&self, name: &str) -> Result<()> {
        let req = self
            .client
            .post(&format!("/v1/materialized_view/{}/drop", name));
        let (request_id, rsp) = self.client.send(req).await?;
        self.client.check_response(&request_id, rsp).await?;
        Ok(())
    }

    async fn list_materialized_views(&self) -> Result<Vec<MaterializedViewInfo>> {
        let req = self.client.get("/v1/materialized_view/list");
        let (request_id, rsp) = self.client.send(req).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let body: RemoteListMaterializedViewsResponse = rsp.json().await.err_to_http(request_id)?;
        Ok(body
            .views
            .into_iter()
            .map(|v| MaterializedViewInfo {
                name: v.name,
                source_table: v.source_table,
                projection: v.projection,
                udf_columns: v.udf_columns,
                filter: v.filter,
                auto_refresh: v.auto_refresh,
            })
            .collect())
    }

    async fn list_jobs(&self) -> Result<Vec<JobInfo>> {
        let req = self
            .client
            .post("/v1/jobs/list")
            .json(&serde_json::json!({ "include_status": true }));
        let (request_id, rsp) = self.client.send(req).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let body: RemoteListPlatformJobsResponse = rsp.json().await.err_to_http(request_id)?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        Ok(body
            .jobs
            .into_iter()
            .map(|row| JobInfo {
                table: row.table,
                job_id: row.job_id,
                // The platform job_type is always "indexer"; the subtype
                // (udf / mv_refresh / compaction / ...) is the useful label.
                job_type: row.job_subtype,
                state: platform_state_to_client(&row.state),
                column: None,
                age_seconds: (row.created_at_millis > 0)
                    .then(|| (now_ms - row.created_at_millis) / 1000),
                command: None,
                units_done: payload_i64(&row.status, "units_done"),
                units_total: payload_i64(&row.status, "units_total"),
                committed: row.state == "done",
                rows_skipped: payload_i64(&row.status, "rows_skipped").unwrap_or(0) as u64,
                error: payload_error(&row.status),
            })
            .collect())
    }

    async fn get_job(&self, job_id: &str, table: Option<&str>) -> Result<Option<JobInfo>> {
        // A point snapshot from the platform API: resolve the submission id,
        // then describe. The snapshot keeps the caller's id.
        let Some(platform_id) = self.resolve_platform_job_id(job_id, table).await? else {
            return Ok(None);
        };
        let Some(described) = self.describe_platform_job(&platform_id).await? else {
            return Ok(None);
        };
        Ok(Some(JobInfo {
            table: table.unwrap_or_default().to_string(),
            job_id: job_id.to_string(),
            job_type: described.job_subtype,
            state: describe_state_to_client(&described.job_state),
            column: None,
            age_seconds: None,
            command: None,
            units_done: payload_i64(&described.status, "units_done"),
            units_total: payload_i64(&described.status, "units_total"),
            committed: described.job_state == "DONE",
            rows_skipped: payload_i64(&described.status, "rows_skipped").unwrap_or(0) as u64,
            error: payload_error(&described.status),
        }))
    }

    async fn describe_platform_job(
        &self,
        platform_job_id: &str,
    ) -> Result<Option<PlatformJobDescription>> {
        let req = self
            .client
            .post("/v1/jobs/describe")
            .json(&serde_json::json!({ "job_id": platform_job_id }));
        let (request_id, rsp) = self.client.send(req).await?;
        if rsp.status().as_u16() == 404 {
            return Ok(None);
        }
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let body: RemoteDescribePlatformJobResponse = rsp.json().await.err_to_http(request_id)?;
        Ok(Some(PlatformJobDescription {
            job_id: body.job_id,
            job_type: body.job_type,
            job_subtype: body.job_subtype,
            job_state: body.job_state,
            creation_ms: body.creation_ms,
            status: body.status,
        }))
    }

    async fn resolve_platform_job_id(
        &self,
        manifest_job_id: &str,
        table_hint: Option<&str>,
    ) -> Result<Option<String>> {
        let req = self.client.post("/v1/jobs/list").json(&serde_json::json!({
            "manifest_job_id": manifest_job_id,
            "table_name": table_hint,
            "job_type": "indexer",
        }));
        let (request_id, rsp) = self.client.send(req).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let body: RemoteListPlatformJobsResponse = rsp.json().await.err_to_http(request_id)?;
        Ok(body.jobs.into_iter().next().map(|row| row.job_id))
    }

    async fn cancel_platform_job(&self, platform_job_id: &str) -> Result<()> {
        let req = self
            .client
            .post("/v1/jobs/cancel")
            .json(&serde_json::json!({ "job_id": platform_job_id }));
        let (request_id, rsp) = self.client.send(req).await?;
        self.client.check_response(&request_id, rsp).await?;
        Ok(())
    }

    async fn cancel_job(&self, job_id: &str) -> Result<bool> {
        // Resolve the submission id and cancel through the platform API.
        // False when no matching job has registered (the legacy best-effort
        // contract).
        let Some(platform_id) = self.resolve_platform_job_id(job_id, None).await? else {
            return Ok(false);
        };
        self.cancel_platform_job(&platform_id).await?;
        Ok(true)
    }

    async fn job_history(&self, job_id: Option<&str>) -> Result<Vec<JobHistoryInfo>> {
        // One job: describe (identity) plus query_events (timeline). No id:
        // a registry listing, timeline-free -- pass an id for the event log.
        let Some(caller_id) = job_id else {
            let rows = self.list_jobs().await?;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            return Ok(rows
                .into_iter()
                .map(|j| JobHistoryInfo {
                    table: j.table,
                    job_id: j.job_id,
                    job_type: j.job_type,
                    state: j.state,
                    column: j.column,
                    created_ms: j.age_seconds.map(|a| now_ms - a * 1000).unwrap_or_default(),
                    updated_ms: 0,
                    completed_ms: None,
                    rows_processed: None,
                    rows_skipped: j.rows_skipped.try_into().ok(),
                    error: j.error,
                    events: None,
                })
                .collect());
        };
        // Accept either a platform id or a submission id.
        let platform_id = match self.describe_platform_job(caller_id).await? {
            Some(_) => caller_id.to_string(),
            None => match self.resolve_platform_job_id(caller_id, None).await? {
                Some(id) => id,
                None => return Ok(Vec::new()),
            },
        };
        let Some(described) = self.describe_platform_job(&platform_id).await? else {
            return Ok(Vec::new());
        };
        let req = self
            .client
            .post("/v1/jobs/query_events")
            .json(&serde_json::json!({ "job_id": platform_id }));
        let (request_id, rsp) = self.client.send(req).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let body = rsp.bytes().await.err_to_http(request_id.clone())?;
        let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(body), None)
            .map_err(|e| Error::Http {
                source: format!("failed to read job-events IPC stream: {e}").into(),
                request_id: request_id.clone(),
                status_code: None,
            })?;

        let mut created_ms = i64::MAX;
        let mut updated_ms = 0i64;
        let mut completed_ms = None;
        let mut last_error = None;
        let mut events = Vec::new();
        for batch in reader {
            let batch = batch.map_err(|e| Error::Http {
                source: format!("failed to decode job-events batch: {e}").into(),
                request_id: request_id.clone(),
                status_code: None,
            })?;
            let states = batch
                .column_by_name("state")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::StringArray>());
            let times = batch
                .column_by_name("updated_at_millis")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::Int64Array>());
            let payloads = batch
                .column_by_name("payload")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::StringArray>());
            let (Some(states), Some(times)) = (states, times) else {
                continue;
            };
            for i in 0..batch.num_rows() {
                let state = states.value(i);
                let ts = times.value(i);
                created_ms = created_ms.min(ts);
                updated_ms = updated_ms.max(ts);
                if matches!(state, "succeeded" | "failed" | "timed_out" | "canceled") {
                    completed_ms = Some(ts);
                }
                if let Some(payloads) = payloads
                    && !arrow_array::Array::is_null(payloads, i)
                    && let Ok(payload) =
                        serde_json::from_str::<serde_json::Value>(payloads.value(i))
                    && let Some(e) = payload_error(&payload)
                {
                    last_error = Some(e);
                }
                events.push(format!("{state} {ts}"));
            }
        }
        Ok(vec![JobHistoryInfo {
            table: String::new(),
            job_id: caller_id.to_string(),
            job_type: described.job_subtype,
            state: describe_state_to_client(&described.job_state),
            column: None,
            created_ms: if created_ms == i64::MAX {
                described.creation_ms
            } else {
                created_ms
            },
            updated_ms,
            completed_ms,
            rows_processed: payload_i64(&described.status, "rows_committed"),
            rows_skipped: payload_i64(&described.status, "rows_skipped"),
            error: last_error.or_else(|| payload_error(&described.status)),
            events: (!events.is_empty()).then(|| events.join("\n")),
        }])
    }

    async fn errors(&self, job_id: Option<&str>, table: Option<&str>) -> Result<Vec<JobErrorInfo>> {
        let mut req = self.client.get("/v1/errors");
        if let Some(j) = job_id {
            req = req.query(&[("job", j)]);
        }
        if let Some(t) = table {
            req = req.query(&[("table", t)]);
        }
        let (request_id, rsp) = self.client.send(req).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let body: RemoteErrorsResponse = rsp.json().await.err_to_http(request_id)?;
        Ok(body.errors.into_iter().map(JobErrorInfo::from).collect())
    }

    async fn open_table(&self, request: OpenTableRequest) -> Result<Arc<dyn BaseTable>> {
        let identifier = build_table_identifier(
            &request.name,
            &request.namespace_path,
            &self.client.id_delimiter,
        );
        let cache_key = build_cache_key(&request.name, &request.namespace_path);

        // We describe the table to confirm it exists before moving on.
        if let Some(table) = self.table_cache.get(&cache_key).await {
            Ok(table.clone())
        } else {
            let req = self
                .client
                .post(&format!("/v1/table/{}/describe/", identifier));
            let (request_id, rsp) = self.client.send_with_retry(req, None, true).await?;
            let rsp =
                RemoteTable::<S>::handle_table_not_found(&request.name, rsp, &request_id).await?;
            let rsp = self.client.check_response(&request_id, rsp).await?;
            let version = parse_server_version(&request_id, &rsp)?;
            let table_identifier = build_table_identifier(
                &request.name,
                &request.namespace_path,
                &self.client.id_delimiter,
            );
            let table = Arc::new(RemoteTable::new(
                self.client.clone(),
                request.name.clone(),
                request.namespace_path.clone(),
                table_identifier,
                version,
            ));
            let cache_key = build_cache_key(&request.name, &request.namespace_path);
            self.table_cache.insert(cache_key, table.clone()).await;
            Ok(table)
        }
    }

    async fn rename_table(
        &self,
        current_name: &str,
        new_name: &str,
        cur_namespace_path: &[String],
        new_namespace_path: &[String],
    ) -> Result<()> {
        let current_identifier =
            build_table_identifier(current_name, cur_namespace_path, &self.client.id_delimiter);
        let current_cache_key = build_cache_key(current_name, cur_namespace_path);
        let new_cache_key = build_cache_key(new_name, new_namespace_path);

        let mut body = serde_json::json!({ "new_table_name": new_name });
        if !new_namespace_path.is_empty() {
            body["new_namespace"] = serde_json::Value::Array(
                new_namespace_path
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

    async fn drop_table(&self, name: &str, namespace_path: &[String]) -> Result<()> {
        let identifier = build_table_identifier(name, namespace_path, &self.client.id_delimiter);
        let cache_key = build_cache_key(name, namespace_path);
        let req = self.client.post(&format!("/v1/table/{}/drop/", identifier));
        let (request_id, resp) = self.client.send(req).await?;
        self.client.check_response(&request_id, resp).await?;
        self.table_cache.remove(&cache_key).await;
        Ok(())
    }

    async fn drop_all_tables(&self, namespace_path: &[String]) -> Result<()> {
        // TODO: Implement namespace-aware drop_all_tables
        let _namespace_path = namespace_path; // Suppress unused warning for now
        Err(crate::Error::NotSupported {
            message: "Dropping all tables is not currently supported in the remote API".to_string(),
        })
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        let namespace_parts = request.id.as_deref().unwrap_or(&[]);
        let namespace_id = build_namespace_identifier(namespace_parts, &self.client.id_delimiter);
        let mut req = self
            .client
            .get(&format!("/v1/namespace/{}/list", namespace_id));
        if let Some(limit) = request.limit {
            req = req.query(&[("limit", limit)]);
        }
        if let Some(ref page_token) = request.page_token {
            req = req.query(&[("page_token", page_token)]);
        }

        let (request_id, resp) = self.client.send(req).await?;
        let resp = self.client.check_response(&request_id, resp).await?;

        resp.json().await.err_to_http(request_id)
    }

    async fn create_namespace(
        &self,
        request: CreateNamespaceRequest,
    ) -> Result<CreateNamespaceResponse> {
        let namespace_parts = request.id.as_deref().unwrap_or(&[]);
        let namespace_id = build_namespace_identifier(namespace_parts, &self.client.id_delimiter);
        let mut req = self
            .client
            .post(&format!("/v1/namespace/{}/create", namespace_id));

        // Build request body with mode and properties if present
        #[derive(serde::Serialize)]
        struct CreateNamespaceRequestBody {
            #[serde(skip_serializing_if = "Option::is_none")]
            mode: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            properties: Option<HashMap<String, String>>,
        }

        let body = CreateNamespaceRequestBody {
            mode: request.mode.as_ref().map(|m| format!("{:?}", m)),
            properties: request.properties,
        };

        req = req.json(&body);
        let (request_id, resp) = self.client.send(req).await?;
        let resp = self.client.check_response(&request_id, resp).await?;

        resp.json().await.err_to_http(request_id)
    }

    async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<DropNamespaceResponse> {
        let namespace_parts = request.id.as_deref().unwrap_or(&[]);
        let namespace_id = build_namespace_identifier(namespace_parts, &self.client.id_delimiter);
        let mut req = self
            .client
            .post(&format!("/v1/namespace/{}/drop", namespace_id));

        // Build request body with mode and behavior if present
        #[derive(serde::Serialize)]
        struct DropNamespaceRequestBody {
            #[serde(skip_serializing_if = "Option::is_none")]
            mode: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            behavior: Option<String>,
        }

        let body = DropNamespaceRequestBody {
            mode: request.mode.as_ref().map(|m| format!("{:?}", m)),
            behavior: request.behavior.as_ref().map(|b| format!("{:?}", b)),
        };

        req = req.json(&body);
        let (request_id, resp) = self.client.send(req).await?;
        let resp = self.client.check_response(&request_id, resp).await?;

        resp.json().await.err_to_http(request_id)
    }

    async fn describe_namespace(
        &self,
        request: DescribeNamespaceRequest,
    ) -> Result<DescribeNamespaceResponse> {
        let namespace_parts = request.id.as_deref().unwrap_or(&[]);
        let namespace_id = build_namespace_identifier(namespace_parts, &self.client.id_delimiter);
        let req = self
            .client
            .post(&format!("/v1/namespace/{}/describe", namespace_id))
            .json(&DescribeNamespaceRequest::default());

        let (request_id, resp) = self.client.send(req).await?;
        let resp = self.client.check_response(&request_id, resp).await?;

        resp.json().await.err_to_http(request_id)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn namespace_client(&self) -> Result<Arc<dyn lance_namespace::LanceNamespace>> {
        // Create a RestNamespace pointing to the same remote host with the same authentication headers
        let mut builder = lance_namespace_impls::RestNamespaceBuilder::new(self.client.host())
            .delimiter(&self.client.id_delimiter)
            .headers(self.namespace_headers.clone());

        if let Some(context_provider) = &self.namespace_context_provider {
            builder = builder.context_provider(Arc::clone(context_provider));
        }

        // Apply mTLS configuration if present
        if let Some(tls_config) = &self.tls_config {
            if let Some(cert_file) = &tls_config.cert_file {
                builder = builder.cert_file(cert_file);
            }
            if let Some(key_file) = &tls_config.key_file {
                builder = builder.key_file(key_file);
            }
            if let Some(ssl_ca_cert) = &tls_config.ssl_ca_cert {
                builder = builder.ssl_ca_cert(ssl_ca_cert);
            }
            builder = builder.assert_hostname(tls_config.assert_hostname);
        }

        let namespace = builder.build();
        Ok(Arc::new(namespace) as Arc<dyn lance_namespace::LanceNamespace>)
    }

    async fn namespace_client_config(&self) -> Result<(String, HashMap<String, String>)> {
        if self.namespace_context_provider.is_some() {
            return Err(Error::NotSupported {
                message:
                    "Cannot export a namespace client config when dynamic headers are configured; use LanceDB connection namespace methods instead"
                        .to_string(),
            });
        }

        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), self.client.host().to_string());
        properties.insert("delimiter".to_string(), self.client.id_delimiter.clone());
        for (key, value) in &self.namespace_headers {
            properties.insert(format!("header.{}", key), value.clone());
        }
        // Add TLS configuration if present
        if let Some(tls_config) = &self.tls_config {
            if let Some(cert_file) = &tls_config.cert_file {
                properties.insert("tls.cert_file".to_string(), cert_file.clone());
            }
            if let Some(key_file) = &tls_config.key_file {
                properties.insert("tls.key_file".to_string(), key_file.clone());
            }
            if let Some(ssl_ca_cert) = &tls_config.ssl_ca_cert {
                properties.insert("tls.ssl_ca_cert".to_string(), ssl_ca_cert.clone());
            }
            properties.insert(
                "tls.assert_hostname".to_string(),
                tls_config.assert_hostname.to_string(),
            );
        }
        Ok(("rest".to_string(), properties))
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
                filtered.insert(opt.to_string(), v.clone());
            }
        }
        Self::new(filtered)
    }
}

#[cfg(test)]
mod tests {
    use super::{NamespaceHeaderProviderContext, build_cache_key};
    use std::collections::HashMap;
    use std::sync::{Arc, OnceLock};

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use lance_namespace_impls::{DynamicContextProvider, OperationInfo};

    use crate::connection::ConnectBuilder;
    use crate::{
        Connection, Error,
        database::CreateTableMode,
        remote::{ARROW_STREAM_CONTENT_TYPE, ClientConfig, HeaderProvider, JSON_CONTENT_TYPE},
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
    async fn test_open_table_branch_and_version() {
        let conn = Connection::new_with_handler(|request| {
            let body = if request.url().path() == "/v1/table/t/branches/list/" {
                // checkout_branch validates the branch exists via list_branches.
                r#"{"branches":{"exp":{"parentVersion":1,"createAt":1,"manifestSize":1}}}"#
            } else {
                // describe (table open + version/branch validation)
                r#"{"table": "t", "version": 2, "schema": {"fields": [
                    {"name": "a", "type": { "type": "int32" }, "nullable": false}
                ]}}"#
            };
            http::Response::builder().status(200).body(body).unwrap()
        });

        // version-only (and "main" + version) time-travel the main chain
        let v2 = conn.open_table("t").version(2).execute().await.unwrap();
        assert_eq!(v2.current_branch(), None);
        let main_v2 = conn
            .open_table("t")
            .branch("main")
            .version(2)
            .execute()
            .await
            .unwrap();
        assert_eq!(main_v2.current_branch(), None);

        // a non-main branch opens a handle scoped to that branch
        let exp = conn.open_table("t").branch("exp").execute().await.unwrap();
        assert_eq!(exp.current_branch(), Some("exp".to_string()));
        let exp_v2 = conn
            .open_table("t")
            .branch("exp")
            .version(2)
            .execute()
            .await
            .unwrap();
        assert_eq!(exp_v2.current_branch(), Some("exp".to_string()));
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
        let table = conn.create_table("table1", data).execute().await.unwrap();
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
        let result = conn.create_table("table1", data).execute().await;
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
            let mut builder = conn.create_table("table1", data.clone());
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
        let called_in_cb = called.clone();
        conn.create_table("table1", data)
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
        let table = conn
            .create_table("table1", data)
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

    #[tokio::test]
    async fn test_derived_compute_routes() {
        // create_function
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/function/embed/create");
            let body: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            assert_eq!(body["language"], "python");
            assert_eq!(body["return_type"], "FLOAT[4]");
            assert_eq!(body["body"], "def embed(x): ...");
            assert_eq!(body["options"]["pip"], "torch");
            http::Response::builder()
                .status(200)
                .body(r#"{"name":"embed","status":"OK"}"#)
                .unwrap()
        });
        conn.create_function(crate::database::CreateFunctionRequest {
            name: "embed".into(),
            language: "python".into(),
            return_type: "FLOAT[4]".into(),
            body: "def embed(x): ...".into(),
            options: [("pip".to_string(), "torch".to_string())].into(),
        })
        .await
        .unwrap();

        // list_functions
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::GET);
            assert_eq!(request.url().path(), "/v1/function/list");
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"functions":[{"name":"embed","language":"python","return_type":"Float32","description":""}]}"#,
                )
                .unwrap()
        });
        let functions = conn.list_functions().await.unwrap();
        assert_eq!(functions.len(), 1);
        assert_eq!(functions[0].name, "embed");

        // drop_function
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/function/embed/drop");
            http::Response::builder()
                .status(200)
                .body(r#"{"name":"embed","status":"OK"}"#)
                .unwrap()
        });
        conn.drop_function("embed").await.unwrap();

        // create_materialized_view
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/materialized_view/mv1/create");
            let body: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            assert_eq!(body["query"], "SELECT id, embed(body) AS vec FROM docs");
            assert_eq!(body["auto_refresh"], true);
            assert_eq!(body["with_no_data"], false);
            http::Response::builder()
                .status(200)
                .body(r#"{"name":"mv1","job_id":"j-1"}"#)
                .unwrap()
        });
        let mut request = crate::database::CreateMaterializedViewRequest::new(
            "mv1",
            "SELECT id, embed(body) AS vec FROM docs",
        );
        request.auto_refresh = true;
        let job_id = conn.create_materialized_view(request).await.unwrap();
        assert_eq!(job_id.as_deref(), Some("j-1"));

        // refresh_materialized_view
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/materialized_view/mv1/refresh");
            let body: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            assert_eq!(body["num_workers"], 2);
            assert!(body.get("src_version").is_none());
            http::Response::builder()
                .status(202)
                .body(r#"{"job_id":"j-2"}"#)
                .unwrap()
        });
        let mut request = crate::database::RefreshMaterializedViewRequest::new("mv1");
        request.num_workers = Some(2);
        let job_id = conn.refresh_materialized_view(request).await.unwrap();
        assert_eq!(job_id, "j-2");

        // alter_materialized_view
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/materialized_view/mv1/alter");
            let body: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            assert_eq!(body["auto_refresh"], false);
            http::Response::builder()
                .status(200)
                .body(r#"{"name":"mv1","status":"OK"}"#)
                .unwrap()
        });
        conn.alter_materialized_view("mv1", false).await.unwrap();

        // drop_materialized_view
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.url().path(), "/v1/materialized_view/mv1/drop");
            http::Response::builder()
                .status(200)
                .body(r#"{"name":"mv1","status":"OK"}"#)
                .unwrap()
        });
        conn.drop_materialized_view("mv1").await.unwrap();

        // list_materialized_views
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::GET);
            assert_eq!(request.url().path(), "/v1/materialized_view/list");
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"views":[{"name":"mv1","source_table":"docs","projection":["id"],"udf_columns":["vec=embed(body)"],"filter":null,"auto_refresh":true}]}"#,
                )
                .unwrap()
        });
        let views = conn.list_materialized_views().await.unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].source_table, "docs");
        assert!(views[0].auto_refresh);

        // list_jobs: platform listing with status payloads
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/jobs/list");
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"jobs":[{"job_id":"plat-3","table":"docs","job_type":"indexer","job_subtype":"udf","state":"in_progress","created_at_millis":1000,"status":{"units_done":1,"units_total":2}}]}"#,
                )
                .unwrap()
        });
        let jobs = conn.list_jobs().await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].state, "running");
        assert_eq!(jobs[0].job_type, "udf");
        assert_eq!(jobs[0].units_total, Some(2));

        // cancel_job: resolve via the manifest-id list filter, then cancel
        let conn = Connection::new_with_handler(|request| match request.url().path() {
            "/v1/jobs/list" => http::Response::builder()
                .status(200)
                .body(r#"{"jobs":[{"job_id":"plat-3","state":"in_progress"}]}"#)
                .unwrap(),
            "/v1/jobs/cancel" => {
                assert_eq!(request.method(), &reqwest::Method::POST);
                http::Response::builder()
                    .status(200)
                    .body(r#"{"job_id":"plat-3"}"#)
                    .unwrap()
            }
            other => panic!("unexpected path {other}"),
        });
        assert!(conn.cancel_job("j-3").await.unwrap());

        // cancel_job: never registered -> false, and no cancel request
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.url().path(), "/v1/jobs/list");
            http::Response::builder()
                .status(200)
                .body(r#"{"jobs":[]}"#)
                .unwrap()
        });
        assert!(!conn.cancel_job("gone").await.unwrap());

        // job_history(None): a registry listing, timeline-free
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.url().path(), "/v1/jobs/list");
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"jobs":[{"job_id":"plat-1","table":"docs","job_type":"indexer","job_subtype":"udf","state":"done","created_at_millis":1000,"status":{"rows_skipped":3}}]}"#,
                )
                .unwrap()
        });
        let hist = conn.job_history(None).await.unwrap();
        assert_eq!(hist.len(), 1);
        assert_eq!(hist[0].state, "finished");
        assert_eq!(hist[0].rows_skipped, Some(3));

        // job_history(id): unknown everywhere -> empty
        let conn = Connection::new_with_handler(|request| match request.url().path() {
            "/v1/jobs/describe" => http::Response::builder().status(404).body("").unwrap(),
            "/v1/jobs/list" => http::Response::builder()
                .status(200)
                .body(r#"{"jobs":[]}"#)
                .unwrap(),
            other => panic!("unexpected path {other}"),
        });
        assert!(conn.job_history(Some("j-1")).await.unwrap().is_empty());

        // errors: GET /v1/errors with job + table filters
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::GET);
            assert_eq!(request.url().path(), "/v1/errors");
            assert_eq!(request.url().query(), Some("job=j-1&table=docs"));
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"errors":[{"job_id":"j-1","table":"docs","column":"vec","error_type":"ValueError","error_message":"boom","fragment_id":0,"source_row_id":42,"table_version":7,"age_seconds":5}]}"#,
                )
                .unwrap()
        });
        let errs = conn.errors(Some("j-1"), Some("docs")).await.unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].error_type, "ValueError");
        assert_eq!(errs[0].source_row_id, Some(42));
    }

    #[tokio::test]
    async fn test_clone_table() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/cloned_table/clone");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(body["source_location"], "s3://bucket/source_table");
            assert_eq!(body["is_shallow"], true);

            http::Response::builder().status(200).body("").unwrap()
        });

        let table = conn
            .clone_table("cloned_table", "s3://bucket/source_table")
            .execute()
            .await
            .unwrap();
        assert_eq!(table.name(), "cloned_table");
    }

    #[tokio::test]
    async fn test_clone_table_with_version() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/cloned_table/clone");

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(body["source_location"], "s3://bucket/source_table");
            assert_eq!(body["source_version"], 42);
            assert_eq!(body["is_shallow"], true);

            http::Response::builder().status(200).body("").unwrap()
        });

        let table = conn
            .clone_table("cloned_table", "s3://bucket/source_table")
            .source_version(42)
            .execute()
            .await
            .unwrap();
        assert_eq!(table.name(), "cloned_table");
    }

    #[tokio::test]
    async fn test_clone_table_with_tag() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/cloned_table/clone");

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(body["source_location"], "s3://bucket/source_table");
            assert_eq!(body["source_tag"], "v1.0");
            assert_eq!(body["is_shallow"], true);

            http::Response::builder().status(200).body("").unwrap()
        });

        let table = conn
            .clone_table("cloned_table", "s3://bucket/source_table")
            .source_tag("v1.0")
            .execute()
            .await
            .unwrap();
        assert_eq!(table.name(), "cloned_table");
    }

    #[tokio::test]
    async fn test_clone_table_deep_clone() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/cloned_table/clone");

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(body["source_location"], "s3://bucket/source_table");
            assert_eq!(body["is_shallow"], false);

            http::Response::builder().status(200).body("").unwrap()
        });

        let table = conn
            .clone_table("cloned_table", "s3://bucket/source_table")
            .is_shallow(false)
            .execute()
            .await
            .unwrap();
        assert_eq!(table.name(), "cloned_table");
    }

    #[tokio::test]
    async fn test_clone_table_with_namespace() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/ns1$ns2$cloned_table/clone");

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(body["source_location"], "s3://bucket/source_table");
            assert_eq!(body["is_shallow"], true);

            http::Response::builder().status(200).body("").unwrap()
        });

        let table = conn
            .clone_table("cloned_table", "s3://bucket/source_table")
            .target_namespace(vec!["ns1".to_string(), "ns2".to_string()])
            .execute()
            .await
            .unwrap();
        assert_eq!(table.name(), "cloned_table");
    }

    #[tokio::test]
    async fn test_clone_table_error() {
        let conn = Connection::new_with_handler(|_| {
            http::Response::builder()
                .status(500)
                .body("Internal server error")
                .unwrap()
        });

        let result = conn
            .clone_table("cloned_table", "s3://bucket/source_table")
            .execute()
            .await;

        assert!(result.is_err());
        if let Err(crate::Error::Http { source, .. }) = result {
            assert!(source.to_string().contains("Failed to clone table"));
        } else {
            panic!("Expected HTTP error");
        }
    }

    #[tokio::test]
    async fn test_namespace_client() {
        let conn = Connection::new_with_handler(|_| {
            http::Response::builder()
                .status(200)
                .body(r#"{"tables": []}"#)
                .unwrap()
        });

        // Get the namespace client from the connection's internal database
        let namespace_client = conn.namespace_client().await;
        assert!(namespace_client.is_ok());
    }

    #[tokio::test]
    async fn test_namespace_client_with_tls_config() {
        use crate::remote::client::TlsConfig;

        let tls_config = TlsConfig {
            cert_file: Some("/path/to/cert.pem".to_string()),
            key_file: Some("/path/to/key.pem".to_string()),
            ssl_ca_cert: Some("/path/to/ca.pem".to_string()),
            assert_hostname: true,
        };

        let client_config = ClientConfig {
            tls_config: Some(tls_config),
            ..Default::default()
        };

        let conn = Connection::new_with_handler_and_config(
            |_| {
                http::Response::builder()
                    .status(200)
                    .body(r#"{"tables": []}"#)
                    .unwrap()
            },
            client_config,
        );

        // Get the namespace client - it should be created with the TLS config
        let namespace_client = conn.namespace_client().await;
        assert!(namespace_client.is_ok());
    }

    #[tokio::test]
    async fn test_namespace_client_with_headers() {
        let mut extra_headers = HashMap::new();
        extra_headers.insert("X-Custom-Header".to_string(), "custom-value".to_string());

        let client_config = ClientConfig {
            extra_headers,
            ..Default::default()
        };

        let conn = Connection::new_with_handler_and_config(
            |_| {
                http::Response::builder()
                    .status(200)
                    .body(r#"{"tables": []}"#)
                    .unwrap()
            },
            client_config,
        );

        // Get the namespace client - it should be created with the extra headers
        let namespace_client = conn.namespace_client().await;
        assert!(namespace_client.is_ok());
    }

    #[test]
    fn test_namespace_header_provider_context_maps_headers() {
        #[derive(Debug)]
        struct TestHeaderProvider;

        #[async_trait::async_trait]
        impl HeaderProvider for TestHeaderProvider {
            async fn get_headers(&self) -> crate::Result<HashMap<String, String>> {
                Ok(HashMap::from([(
                    "authorization".to_string(),
                    "Bearer token".to_string(),
                )]))
            }
        }

        let context_provider = NamespaceHeaderProviderContext {
            header_provider: Arc::new(TestHeaderProvider) as Arc<dyn HeaderProvider>,
        };

        let context =
            context_provider.provide_context(&OperationInfo::new("list_tables", "namespace"));

        assert_eq!(
            context.get("headers.authorization"),
            Some(&"Bearer token".to_string())
        );
    }

    #[tokio::test]
    async fn test_namespace_client_supports_dynamic_headers() {
        #[derive(Debug)]
        struct TestHeaderProvider;

        #[async_trait::async_trait]
        impl HeaderProvider for TestHeaderProvider {
            async fn get_headers(&self) -> crate::Result<HashMap<String, String>> {
                Ok(HashMap::from([(
                    "authorization".to_string(),
                    "Bearer token".to_string(),
                )]))
            }
        }

        let client_config = ClientConfig {
            header_provider: Some(Arc::new(TestHeaderProvider) as Arc<dyn HeaderProvider>),
            ..Default::default()
        };

        let conn = Connection::new_with_handler_and_config(
            |_| {
                http::Response::builder()
                    .status(200)
                    .body(r#"{"tables": []}"#)
                    .unwrap()
            },
            client_config,
        );

        let namespace_client = conn.namespace_client().await;
        assert!(namespace_client.is_ok());

        match conn.namespace_client_config().await {
            Err(Error::NotSupported { message })
                if message.contains("dynamic headers are configured") => {}
            Err(err) => panic!("expected NotSupported, got {err:?}"),
            Ok(_) => panic!("expected namespace_client_config to reject dynamic headers"),
        }
    }

    /// Integration tests using RestAdapter to run RemoteDatabase against a real namespace server
    mod rest_adapter_integration {
        use super::*;
        use lance_namespace::models::ListTablesRequest;
        use lance_namespace_impls::{DirectoryNamespaceBuilder, RestAdapter, RestAdapterConfig};
        use std::sync::Arc;
        use tempfile::TempDir;

        /// Test fixture that manages a REST server backed by DirectoryNamespace
        struct RestServerFixture {
            _temp_dir: TempDir,
            server_handle: lance_namespace_impls::RestAdapterHandle,
            server_url: String,
        }

        impl RestServerFixture {
            async fn new() -> Self {
                let temp_dir = TempDir::new().unwrap();
                let temp_path = temp_dir.path().to_str().unwrap().to_string();

                // Create DirectoryNamespace backend
                let backend = DirectoryNamespaceBuilder::new(&temp_path)
                    .build()
                    .await
                    .unwrap();
                let backend = Arc::new(backend);

                // Start REST server with port 0 (OS assigns available port)
                let config = RestAdapterConfig {
                    port: 0,
                    ..Default::default()
                };

                let server = RestAdapter::new(backend, config);
                let server_handle = server.start().await.unwrap();

                // Get the actual port assigned by OS
                let actual_port = server_handle.port();
                let server_url = format!("http://127.0.0.1:{}", actual_port);

                Self {
                    _temp_dir: temp_dir,
                    server_handle,
                    server_url,
                }
            }
        }

        impl Drop for RestServerFixture {
            fn drop(&mut self) {
                self.server_handle.shutdown();
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_remote_database_with_rest_adapter() {
            use lance_namespace::models::CreateNamespaceRequest;

            let fixture = RestServerFixture::new().await;

            // Connect to the REST server using lancedb Connection
            // Use db://dummy as URI and set actual server URL via host_override
            let conn = ConnectBuilder::new("db://dummy")
                .api_key("test-api-key")
                .region("us-east-1")
                .host_override(&fixture.server_url)
                .execute()
                .await
                .unwrap();

            // Create a child namespace first
            let namespace = vec!["test_ns".to_string()];
            conn.create_namespace(CreateNamespaceRequest {
                id: Some(namespace.clone()),
                ..Default::default()
            })
            .await
            .expect("Failed to create namespace");

            // Create a table in the child namespace
            let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
            let data = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let table = conn
                .create_table("test_table", data)
                .namespace(namespace.clone())
                .execute()
                .await;
            assert!(table.is_ok(), "Failed to create table: {:?}", table.err());

            // List tables in the child namespace
            let list_response = conn
                .list_tables(ListTablesRequest {
                    id: Some(namespace.clone()),
                    ..Default::default()
                })
                .await
                .expect("Failed to list tables");
            assert_eq!(list_response.tables, vec!["test_table"]);

            // Get namespace client and verify it can also list tables
            let namespace_client = conn.namespace_client().await.unwrap();
            let list_response = namespace_client
                .list_tables(ListTablesRequest {
                    id: Some(namespace.clone()),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(list_response.tables, vec!["test_table"]);

            // Open the table from the child namespace
            let opened_table = conn
                .open_table("test_table")
                .namespace(namespace.clone())
                .execute()
                .await;
            assert!(
                opened_table.is_ok(),
                "Failed to open table: {:?}",
                opened_table.err()
            );
            assert_eq!(opened_table.unwrap().name(), "test_table");
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_remote_database_with_multiple_tables() {
            use lance_namespace::models::CreateNamespaceRequest;

            let fixture = RestServerFixture::new().await;

            // Connect to the REST server
            // Use db://dummy as URI and set actual server URL via host_override
            let conn = ConnectBuilder::new("db://dummy")
                .api_key("test-api-key")
                .region("us-east-1")
                .host_override(&fixture.server_url)
                .execute()
                .await
                .unwrap();

            // Create a child namespace first
            let namespace = vec!["multi_table_ns".to_string()];
            conn.create_namespace(CreateNamespaceRequest {
                id: Some(namespace.clone()),
                ..Default::default()
            })
            .await
            .expect("Failed to create namespace");

            // Create multiple tables in the child namespace
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

            for i in 1..=3 {
                let data =
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![i]))])
                        .unwrap();
                conn.create_table(format!("table{}", i), data)
                    .namespace(namespace.clone())
                    .execute()
                    .await
                    .unwrap_or_else(|e| panic!("Failed to create table{}: {:?}", i, e));
            }

            // List tables in the child namespace
            let list_response = conn
                .list_tables(ListTablesRequest {
                    id: Some(namespace.clone()),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(list_response.tables.len(), 3);
            assert!(list_response.tables.contains(&"table1".to_string()));
            assert!(list_response.tables.contains(&"table2".to_string()));
            assert!(list_response.tables.contains(&"table3".to_string()));
        }
    }
}
