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
    CloneTableRequest, CreateTableMode, CreateTableRequest, Database, DatabaseOptions,
    JobDescription, JobInfo, OpenTableRequest, ReadConsistency, TableNamesRequest,
};
use crate::error::Result;
use crate::function::schema_admission::reject_caller_authored_generated_column_schema;
use crate::function::{Function, FunctionId, RegisterFunctionJobSpec};
use crate::remote::util::stream_as_body;
use crate::table::BaseTable;

use super::ARROW_STREAM_CONTENT_TYPE;
use super::client::{
    ClientConfig, HeaderProvider, HttpSend, RequestResultExt, RestfulLanceDbClient, Sender,
};
use super::table::RemoteTable;
use super::util::parse_server_version;

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

    pub fn support_blobs(&self) -> bool {
        self.0 >= semver::Version::new(0, 5, 0)
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

#[derive(serde::Deserialize)]
struct RemoteListJobRow {
    job_id: String,
    #[serde(default)]
    table: String,
    #[serde(default)]
    job_type: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    created_at_millis: i64,
}

#[derive(serde::Deserialize)]
struct RemoteListJobsResponse {
    #[serde(default)]
    jobs: Vec<RemoteListJobRow>,
    #[serde(default)]
    page_token: Option<String>,
}

/// Bound on `list_jobs` page walking; a warning is logged when the listing
/// is truncated at this many pages.
const MAX_LIST_JOBS_PAGES: usize = 100;

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

    fn job(&self, job_id: &str) -> Result<crate::job::Job> {
        Ok(crate::job::Job::new(Box::new(super::job::RemoteJob::new(
            self.client.clone(),
            job_id.to_string(),
        ))))
    }

    async fn list_jobs(&self) -> Result<Vec<JobInfo>> {
        let mut out = Vec::new();
        let mut page_token: Option<String> = None;
        for page in 0..MAX_LIST_JOBS_PAGES {
            let mut body = serde_json::json!({});
            if let Some(token) = &page_token {
                body["page_token"] = serde_json::Value::String(token.clone());
            }
            let req = self.client.post("/v1/jobs/list").json(&body);
            let (request_id, rsp) = self.client.send(req).await?;
            let rsp = self.client.check_response(&request_id, rsp).await?;
            let body: RemoteListJobsResponse = rsp.json().await.err_to_http(request_id)?;
            out.extend(body.jobs.into_iter().map(|row| JobInfo {
                job_id: row.job_id,
                table: row.table,
                job_type: row.job_type,
                state: super::job::job_state_to_client(&row.state),
                created_at_millis: row.created_at_millis,
            }));
            page_token = body.page_token;
            if page_token.is_none() {
                break;
            }
            if page + 1 == MAX_LIST_JOBS_PAGES {
                log::warn!(
                    "list_jobs truncated after {} pages ({} jobs)",
                    MAX_LIST_JOBS_PAGES,
                    out.len()
                );
            }
        }
        Ok(out)
    }

    async fn get_job(&self, job_id: &str) -> Result<Option<JobDescription>> {
        let req = self
            .client
            .post("/v1/jobs/describe")
            .json(&serde_json::json!({ "job_id": job_id }));
        let (request_id, rsp) = self.client.send(req).await?;
        let rsp = match self.client.check_response(&request_id, rsp).await {
            Ok(rsp) => rsp,
            Err(Error::Http {
                status_code: Some(StatusCode::NOT_FOUND),
                ..
            }) => return Ok(None),
            Err(err) => return Err(err),
        };
        let body: super::job::DescribeJobResponse =
            rsp.json().await.err_to_http(request_id.clone())?;
        let job_id = body.require_job_id(request_id.clone())?;
        let result = body
            .project_success_result(request_id)?
            .into_description_result();
        let state = body.client_state();
        Ok(Some(JobDescription {
            job_id,
            job_type: body.job_type,
            state,
            creation_ms: body.creation_ms,
            spec: body.spec,
            result,
            failure: body
                .failure
                .map(super::job::ReportedFailure::into_job_failure),
        }))
    }

    async fn cancel_job(&self, job_id: &str) -> Result<bool> {
        let req = self
            .client
            .post("/v1/jobs/cancel")
            .json(&serde_json::json!({ "job_id": job_id }));
        let (request_id, rsp) = self.client.send(req).await?;
        match self.client.check_response(&request_id, rsp).await {
            Ok(_) => Ok(true),
            Err(Error::Http {
                status_code: Some(StatusCode::NOT_FOUND),
                ..
            }) => Ok(false),
            Err(err) => Err(err),
        }
    }

    async fn job_history(&self, job_id: Option<&str>) -> Result<Vec<arrow_array::RecordBatch>> {
        let mut body = serde_json::json!({});
        if let Some(job_id) = job_id {
            body["job_id"] = serde_json::Value::String(job_id.to_string());
        }
        let req = self.client.post("/v1/jobs/query_events").json(&body);
        let (request_id, rsp) = self.client.send(req).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let bytes = rsp.bytes().await.err_to_http(request_id)?;
        let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
        reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    async fn register_function(&self, spec: RegisterFunctionJobSpec) -> Result<crate::job::Job> {
        let req = self.client.post("/v1/functions/register").json(&spec);
        let (request_id, rsp) = self
            .client
            .send_sensitive_with_retry(req, None, true)
            .await?;
        let rsp = self
            .client
            .check_sensitive_response(&request_id, rsp)
            .await?;

        // Payload-free protocol failure: never fold response bytes into Error::Http.
        let bytes = rsp.bytes().await.err_to_http(request_id.clone())?;
        let value: serde_json::Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => {
                return Err(Error::Http {
                    source: "register function response is not valid JSON".into(),
                    request_id,
                    status_code: None,
                });
            }
        };
        let job_id = match value.get("job_id") {
            Some(serde_json::Value::String(job_id)) if !job_id.is_empty() => job_id.clone(),
            _ => {
                return Err(Error::Http {
                    source: "register function response missing or invalid job_id".into(),
                    request_id,
                    status_code: None,
                });
            }
        };

        Ok(crate::job::Job::new(Box::new(super::job::RemoteJob::new(
            self.client.clone(),
            job_id,
        ))))
    }

    async fn lookup_function_by_name(&self, name: &str) -> Result<Function> {
        let selector = super::function::FunctionLookupSelector::by_name(name)?;
        super::function::lookup_function(&self.client, selector).await
    }

    async fn lookup_function_by_id(&self, function_id: &FunctionId) -> Result<Function> {
        let selector = super::function::FunctionLookupSelector::by_function_id(function_id);
        super::function::lookup_function(&self.client, selector).await
    }

    async fn remove_function_name(&self, name: &str, current: &Function) -> Result<()> {
        super::function::remove_function_name(&self.client, name, current).await
    }

    async fn revoke_function(&self, function: &Function) -> Result<()> {
        super::function::revoke_function(&self.client, function).await
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
        // Admit schema before scan_as_stream, request/body/header construction, or HTTP.
        reject_caller_authored_generated_column_schema(request.data.schema().as_ref())?;

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
            let describe_body = rsp.text().await.ok();
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
            // This describe already carries the schema, so hand it to the table
            // instead of making the first schema read fetch it again. A version or
            // branch pin applied after this invalidates the cache.
            if let Some(body) = &describe_body {
                table.seed_schema(body);
            }
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, OnceLock};

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use lance_namespace_impls::{DynamicContextProvider, OperationInfo};

    use crate::connection::ConnectBuilder;
    use crate::{
        Connection, Error,
        database::CreateTableMode,
        error::FunctionErrorCode,
        function::{
            Function, FunctionCapability, FunctionDefinition, FunctionId, FunctionOutput,
            FunctionParameter, FunctionSignature, PythonFunctionDefinition,
            RegisterFunctionJobSpec,
        },
        job::JobResult,
        remote::{
            ARROW_STREAM_CONTENT_TYPE, ClientConfig, HeaderProvider, JSON_CONTENT_TYPE, RetryConfig,
        },
    };
    use serde_json::{Value, json};

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
    async fn test_open_table_seeds_the_schema_from_its_describe() {
        let describe_calls = Arc::new(AtomicUsize::new(0));
        let counted = describe_calls.clone();
        let conn = Connection::new_with_handler(move |request| {
            assert_eq!(request.url().path(), "/v1/table/table1/describe/");
            counted.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"version": 1, "schema": {"fields": [
                        {"name": "id", "type": {"type": "int64"}, "nullable": false}
                    ]}}"#
                        .to_string(),
                )
                .unwrap()
        });

        let table = conn.open_table("table1").execute().await.unwrap();
        let schema = table.schema().await.unwrap();

        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(describe_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_open_table_survives_a_describe_body_it_cannot_parse() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.url().path(), "/v1/table/table1/describe/");
            http::Response::builder()
                .status(200)
                .body(r#"{"table": "table1"}"#.to_string())
                .unwrap()
        });

        let table = conn.open_table("table1").execute().await.unwrap();

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

    #[tokio::test]
    async fn test_list_jobs_paginates() {
        let page = Arc::new(AtomicUsize::new(0));
        let conn = Connection::new_with_handler(move |request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/jobs/list");
            let body: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            match page.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    assert!(body.get("page_token").is_none());
                    http::Response::builder()
                        .status(200)
                        .body(
                            r#"{"jobs": [{"job_id": "job-1", "table": "t1", "job_type": "create_index", "state": "in_progress", "created_at_millis": 1000}], "page_token": "next"}"#,
                        )
                        .unwrap()
                }
                _ => {
                    assert_eq!(body["page_token"], "next");
                    http::Response::builder()
                        .status(200)
                        .body(
                            r#"{"jobs": [{"job_id": "job-2", "table": "t2", "job_type": "create_index", "state": "succeeded", "created_at_millis": 2000}, {"job_id": "job-3", "table": "t3", "job_type": "create_index", "state": "timed_out", "created_at_millis": 3000}]}"#,
                        )
                        .unwrap()
                }
            }
        });
        let jobs = conn.list_jobs().await.unwrap();
        assert_eq!(jobs.len(), 3);
        assert_eq!(jobs[0].job_id, "job-1");
        assert_eq!(jobs[0].table, "t1");
        assert_eq!(jobs[0].state, "running");
        assert_eq!(jobs[1].job_id, "job-2");
        assert_eq!(jobs[1].state, "finished");
        assert_eq!(jobs[1].created_at_millis, 2000);
        assert_eq!(jobs[2].job_id, "job-3");
        assert_eq!(jobs[2].state, "failed");
    }

    #[tokio::test]
    async fn test_get_job() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/jobs/describe");
            let body: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            assert_eq!(body["job_id"], "job-1");
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"job_id": "job-1", "job_type": "create_index", "job_state": "FAILED", "creation_ms": 1000, "spec": {"column": "vec"}, "failure": {"phase": "execute", "message": "worker died", "retryable": true}}"#,
                )
                .unwrap()
        });
        let job = conn.get_job("job-1").await.unwrap().unwrap();
        assert_eq!(job.job_id, "job-1");
        assert_eq!(job.job_type, "create_index");
        assert_eq!(job.state, "failed");
        assert_eq!(job.creation_ms, 1000);
        assert_eq!(job.spec["column"], "vec");
        let failure = job.failure.unwrap();
        assert_eq!(failure.phase.as_deref(), Some("execute"));
        assert_eq!(failure.message.as_deref(), Some("worker died"));
        assert_eq!(failure.retryable, Some(true));
    }

    /// Typed get_job keeps requiring an echoed response job_id even when the
    /// rest of a create_index DONE describe looks valid.
    #[tokio::test]
    async fn test_get_job_omitted_job_id_is_http() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.url().path(), "/v1/jobs/describe");
            http::Response::builder()
                .status(200)
                .body(r#"{"job_type":"create_index","job_state":"DONE","creation_ms":1,"spec":{}}"#)
                .unwrap()
        });
        let err = conn
            .get_job("job-1")
            .await
            .expect_err("get_job must require response job_id");
        match err {
            Error::Http { .. } => {}
            other => panic!("expected Error::Http, got {other:?}"),
        }
    }

    /// Documented lowercase describe/list aliases stay stable on get_job.
    #[tokio::test]
    async fn test_get_job_normalizes_documented_state_aliases() {
        let cases = [
            ("in_progress", "running"),
            ("done", "finished"),
            ("succeeded", "finished"),
            ("failed", "failed"),
            ("timed_out", "failed"),
            ("cancelled", "cancelled"),
            ("canceled", "cancelled"),
            ("IN_PROGRESS", "running"),
            ("DONE", "finished"),
            ("FAILED", "failed"),
            ("CANCELLED", "cancelled"),
            ("TIMED_OUT", "failed"),
        ];
        for (wire_state, expected_client_state) in cases {
            let body = format!(
                r#"{{"job_id":"job-alias","job_type":"create_index","job_state":"{wire_state}","creation_ms":1,"spec":{{}}}}"#
            );
            let conn = Connection::new_with_handler(move |_| {
                http::Response::builder()
                    .status(200)
                    .body(body.clone())
                    .unwrap()
            });
            let job = conn
                .get_job("job-alias")
                .await
                .unwrap_or_else(|err| panic!("alias {wire_state} must describe: {err:?}"))
                .expect("job must exist");
            assert_eq!(
                job.state, expected_client_state,
                "get_job alias {wire_state} must normalize to {expected_client_state}"
            );
            assert_eq!(job.job_id, "job-alias");
        }
    }

    #[tokio::test]
    async fn test_get_job_missing_is_none() {
        let conn = Connection::new_with_handler(|_| {
            http::Response::builder()
                .status(404)
                .body("no such job")
                .unwrap()
        });
        assert!(conn.get_job("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_cancel_job() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.url().path(), "/v1/jobs/cancel");
            http::Response::builder()
                .status(200)
                .body(r#"{"job_id": "job-1"}"#)
                .unwrap()
        });
        assert!(conn.cancel_job("job-1").await.unwrap());

        let conn = Connection::new_with_handler(|_| {
            http::Response::builder()
                .status(404)
                .body("no such job")
                .unwrap()
        });
        assert!(!conn.cancel_job("nope").await.unwrap());
    }

    #[tokio::test]
    async fn test_job_history_parses_arrow_stream() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "state",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::StringArray::from(vec![
                "created", "done",
            ]))],
        )
        .unwrap();
        let mut body = Vec::new();
        {
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut body, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        let conn = Connection::new_with_handler(move |request| {
            assert_eq!(request.url().path(), "/v1/jobs/query_events");
            let req_body: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            assert_eq!(req_body["job_id"], "job-1");
            http::Response::builder()
                .status(200)
                .body(body.clone())
                .unwrap()
        });
        let batches = conn.job_history(Some("job-1")).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_conn_job_waits_to_done() {
        let polls = Arc::new(AtomicUsize::new(0));
        let polls_ref = polls.clone();
        let conn = Connection::new_with_handler(move |request| {
            assert_eq!(request.url().path(), "/v1/jobs/describe");
            let state = if polls_ref.fetch_add(1, Ordering::SeqCst) == 0 {
                "IN_PROGRESS"
            } else {
                "DONE"
            };
            http::Response::builder()
                .status(200)
                .body(format!(
                    r#"{{"job_id": "job-1", "job_type": "create_index", "job_state": "{}", "creation_ms": 1}}"#,
                    state
                ))
                .unwrap()
        });
        let job = conn.job("job-1").unwrap();
        assert_eq!(job.id(), Some("job-1"));
        assert_eq!(job.status().await.unwrap(), "running");
        job.wait().await.unwrap();
        assert_eq!(job.status().await.unwrap(), "finished");
        assert!(polls.load(Ordering::SeqCst) >= 3);
    }

    #[tokio::test]
    async fn test_get_job_decodes_known_failure_error_code() {
        use crate::error::FunctionErrorCode;

        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.url().path(), "/v1/jobs/describe");
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"job_id":"job-1","job_type":"create_index","job_state":"FAILED","creation_ms":1000,"spec":{},"failure":{"error_code":"name_or_function_not_found","phase":"validate","message":"looks like definition_validation_failure","retryable":false}}"#,
                )
                .unwrap()
        });
        let job = conn.get_job("job-1").await.unwrap().unwrap();
        let failure = job.failure.expect("failure payload present");
        match &failure.error_code {
            Some(code) => {
                assert_eq!(code, &FunctionErrorCode::NameOrFunctionNotFound);
                assert_ne!(code, &FunctionErrorCode::DefinitionValidationFailure);
            }
            None => panic!("known error_code must be decoded by get_job"),
        }
        assert_eq!(failure.phase.as_deref(), Some("validate"));
        assert_eq!(failure.retryable, Some(false));
    }

    #[tokio::test]
    async fn test_get_job_preserves_unrecognized_failure_error_code() {
        use crate::error::FunctionErrorCode;

        let conn = Connection::new_with_handler(|_| {
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"job_id":"job-1","job_type":"create_index","job_state":"FAILED","creation_ms":1000,"spec":{},"failure":{"error_code":"enterprise_future_category_xyz","phase":"execute","message":"new category","retryable":true}}"#,
                )
                .unwrap()
        });
        let job = conn.get_job("job-1").await.unwrap().unwrap();
        let failure = job.failure.expect("failure payload present");
        match &failure.error_code {
            Some(FunctionErrorCode::Unrecognized(raw)) => {
                assert_eq!(raw, "enterprise_future_category_xyz");
            }
            Some(other) => panic!("unknown error_code must stay Unrecognized, got {other:?}"),
            None => panic!("unknown error_code must not be dropped by get_job"),
        }
    }

    #[tokio::test]
    async fn test_get_job_allows_older_failure_payload_without_error_code() {
        let conn = Connection::new_with_handler(|_| {
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"job_id":"job-1","job_type":"create_index","job_state":"FAILED","creation_ms":1000,"spec":{},"failure":{"phase":"execute","message":"stale_or_conflicting_input in logs","retryable":true}}"#,
                )
                .unwrap()
        });
        let job = conn.get_job("job-1").await.unwrap().unwrap();
        let failure = job.failure.expect("failure payload present");
        assert!(
            failure.error_code.is_none(),
            "older get_job payloads without error_code must not invent a category: {failure:?}"
        );
        assert_eq!(failure.phase.as_deref(), Some("execute"));
        assert_eq!(failure.retryable, Some(true));
    }

    #[tokio::test]
    async fn test_conn_job_wait_decodes_failure_error_code_without_transport_override() {
        use crate::error::FunctionErrorCode;

        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.url().path(), "/v1/jobs/describe");
            // Transport is HTTP 200 with FAILED state; category comes only from error_code.
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"job_id":"job-1","job_type":"create_index","job_state":"FAILED","creation_ms":1,"failure":{"error_code":"unsupported_runtime_or_capability","phase":"dispatch","message":"revoked_function in transport logs","retryable":false}}"#,
                )
                .unwrap()
        });
        let err = conn
            .job("job-1")
            .unwrap()
            .wait()
            .await
            .expect_err("FAILED must surface JobFailed");
        match err {
            Error::JobFailed { failure, .. } => match &failure.error_code {
                Some(code) => {
                    assert_eq!(code, &FunctionErrorCode::UnsupportedRuntimeOrCapability);
                    assert_ne!(code, &FunctionErrorCode::RevokedFunction);
                }
                None => panic!("Database job wait must decode failure.error_code"),
            },
            other => panic!("expected Error::JobFailed, got {other:?}"),
        }
    }

    /// Optional JSON object field for deterministic `/v1/jobs/describe` fixtures.
    #[derive(Clone)]
    enum JsonField {
        Absent,
        Null,
        Present(Value),
    }

    fn sample_description_function() -> Function {
        let id = FunctionId::try_new("fn.exact.remote-job-description").expect("valid FunctionId");
        let signature = FunctionSignature::try_new(
            vec![
                FunctionParameter::new("x", DataType::Int32),
                FunctionParameter::new("label", DataType::Utf8),
            ],
            FunctionOutput::new(DataType::Int32, true),
        )
        .expect("valid FunctionSignature");
        Function::new(id, signature)
    }

    fn assert_exact_function(actual: &Function, expected: &Function) {
        assert_eq!(actual.id(), expected.id());
        assert_eq!(actual.signature(), expected.signature());
    }

    fn job_result_none_wire() -> Value {
        serde_json::to_value(JobResult::None).expect("serialize JobResult::None wire")
    }

    fn job_result_function_wire(function: &Function) -> Value {
        serde_json::to_value(JobResult::Function(function.clone()))
            .expect("serialize JobResult::Function wire")
    }

    fn describe_body(
        job_id: &str,
        job_state: &str,
        job_type: JsonField,
        result: JsonField,
    ) -> String {
        let mut body = json!({
            "job_id": job_id,
            "job_state": job_state,
            "creation_ms": 1,
            "spec": {},
        });
        let object = body
            .as_object_mut()
            .expect("describe body must be a JSON object");
        match job_type {
            JsonField::Absent => {}
            JsonField::Null => {
                object.insert("job_type".into(), Value::Null);
            }
            JsonField::Present(value) => {
                object.insert("job_type".into(), value);
            }
        }
        match result {
            JsonField::Absent => {}
            JsonField::Null => {
                object.insert("result".into(), Value::Null);
            }
            JsonField::Present(value) => {
                object.insert("result".into(), value);
            }
        }
        body.to_string()
    }

    fn conn_with_describe_body(body: String) -> Connection {
        Connection::new_with_handler(move |request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/jobs/describe");
            http::Response::builder()
                .status(200)
                .body(body.clone())
                .unwrap()
        })
    }

    /// register_function DONE Function is shared by get_job and job(id).wait().
    #[tokio::test]
    async fn remote_job_description_result_register_function_done_matches_wait() {
        let expected = sample_description_function();
        let body = describe_body(
            "job-register",
            "DONE",
            JsonField::Present(Value::String("register_function".into())),
            JsonField::Present(job_result_function_wire(&expected)),
        );
        let conn = conn_with_describe_body(body);

        let description = conn
            .get_job("job-register")
            .await
            .expect("valid register_function describe must succeed")
            .expect("job must exist");
        let from_get = description
            .result
            .as_ref()
            .and_then(JobResult::function)
            .expect("register_function success must be Some(Function)");
        assert_exact_function(from_get, &expected);

        let waited = conn
            .job("job-register")
            .expect("job handle")
            .wait()
            .await
            .expect("wait over the same fixture must succeed");
        let from_wait = waited
            .function()
            .expect("wait must return the exact Function");
        assert_exact_function(from_wait, &expected);
    }

    /// Known no-result DONE: omitted/null stay None; explicit None is Some(None).
    #[tokio::test]
    async fn remote_job_description_result_known_no_result_omission_vs_explicit_none() {
        for result_field in [JsonField::Absent, JsonField::Null] {
            let body = describe_body(
                "job-create-index",
                "DONE",
                JsonField::Present(Value::String("create_index".into())),
                result_field,
            );
            let description = conn_with_describe_body(body)
                .get_job("job-create-index")
                .await
                .expect("known no-result describe must succeed")
                .expect("job must exist");
            assert_eq!(description.result, None);
        }

        let body = describe_body(
            "job-create-index-explicit",
            "DONE",
            JsonField::Present(Value::String("create_index".into())),
            JsonField::Present(job_result_none_wire()),
        );
        let description = conn_with_describe_body(body)
            .get_job("job-create-index-explicit")
            .await
            .expect("explicit None describe must succeed")
            .expect("job must exist");
        assert_eq!(description.result, Some(JobResult::None));
    }

    /// Unknown nonterminal and ordinary FAILED/CANCELLED stay describable with no result.
    #[tokio::test]
    async fn remote_job_description_result_nonterminal_unknown_and_failed_cancelled_without_result()
    {
        let unknown_running = describe_body(
            "job-future-running",
            "IN_PROGRESS",
            JsonField::Present(Value::String("future_job_type_xyz".into())),
            JsonField::Absent,
        );
        let description = conn_with_describe_body(unknown_running)
            .get_job("job-future-running")
            .await
            .expect("unknown nonterminal without result must remain describable")
            .expect("job must exist");
        assert_eq!(description.job_type, "future_job_type_xyz");
        assert_eq!(description.state, "running");
        assert_eq!(description.result, None);

        for (job_id, job_state, client_state) in [
            ("job-failed", "FAILED", "failed"),
            ("job-cancelled", "CANCELLED", "cancelled"),
        ] {
            let body = describe_body(
                job_id,
                job_state,
                JsonField::Present(Value::String("create_index".into())),
                JsonField::Absent,
            );
            let description = conn_with_describe_body(body)
                .get_job(job_id)
                .await
                .expect("FAILED/CANCELLED without result must remain describable")
                .expect("job must exist");
            assert_eq!(description.state, client_state);
            assert_eq!(
                description.result, None,
                "must not invent a result for {job_state}"
            );
        }
    }

    /// get_job rejects the same strict invalid describe shapes as RemoteJob::wait.
    #[tokio::test]
    async fn remote_job_description_result_strict_invalid_cases_are_http() {
        let function = sample_description_function();
        let function_wire = job_result_function_wire(&function);
        let none_wire = job_result_none_wire();

        let mut unknown_kind = none_wire.clone();
        unknown_kind["kind"] = Value::String("artifact".into());

        let mut unknown_version = none_wire.clone();
        unknown_version["format_version"] = Value::from(2);

        let mut unknown_outer_field = none_wire.clone();
        unknown_outer_field
            .as_object_mut()
            .unwrap()
            .insert("unexpected_field".into(), Value::Bool(true));

        let mut empty_function_id = function_wire.clone();
        empty_function_id["function"]["id"] = Value::String("".into());

        let cases = [
            (
                "register_missing",
                "DONE",
                JsonField::Present(Value::String("register_function".into())),
                JsonField::Absent,
            ),
            (
                "register_null",
                "DONE",
                JsonField::Present(Value::String("register_function".into())),
                JsonField::Null,
            ),
            (
                "register_explicit_none",
                "DONE",
                JsonField::Present(Value::String("register_function".into())),
                JsonField::Present(none_wire.clone()),
            ),
            (
                "known_no_result_with_function",
                "DONE",
                JsonField::Present(Value::String("create_index".into())),
                JsonField::Present(function_wire.clone()),
            ),
            (
                "unknown_done_missing",
                "DONE",
                JsonField::Present(Value::String("future_job_type_xyz".into())),
                JsonField::Absent,
            ),
            (
                "unknown_done_explicit_none",
                "DONE",
                JsonField::Present(Value::String("future_job_type_xyz".into())),
                JsonField::Present(none_wire.clone()),
            ),
            (
                "unknown_done_explicit_function",
                "DONE",
                JsonField::Present(Value::String("future_job_type_xyz".into())),
                JsonField::Present(function_wire.clone()),
            ),
            (
                "malformed_unknown_kind",
                "DONE",
                JsonField::Present(Value::String("register_function".into())),
                JsonField::Present(unknown_kind),
            ),
            (
                "malformed_unknown_version",
                "DONE",
                JsonField::Present(Value::String("register_function".into())),
                JsonField::Present(unknown_version),
            ),
            (
                "malformed_unknown_outer_field",
                "DONE",
                JsonField::Present(Value::String("register_function".into())),
                JsonField::Present(unknown_outer_field),
            ),
            (
                "malformed_empty_function_id",
                "DONE",
                JsonField::Present(Value::String("register_function".into())),
                JsonField::Present(empty_function_id),
            ),
            (
                "failed_carrying_function",
                "FAILED",
                JsonField::Present(Value::String("register_function".into())),
                JsonField::Present(function_wire),
            ),
            (
                "cancelled_carrying_none",
                "CANCELLED",
                JsonField::Present(Value::String("create_index".into())),
                JsonField::Present(none_wire),
            ),
        ];

        let mut unexpected = Vec::new();
        for (job_id, job_state, job_type, result_field) in cases {
            let body = describe_body(job_id, job_state, job_type, result_field);
            match conn_with_describe_body(body).get_job(job_id).await {
                Err(Error::Http { .. }) => {}
                Ok(Some(description)) => unexpected.push(format!(
                    "{job_id}: Ok(Some(result={:?}))",
                    description.result
                )),
                Ok(None) => unexpected.push(format!("{job_id}: Ok(None)")),
                Err(other) => unexpected.push(format!("{job_id}: Err({other:?})")),
            }
        }
        assert!(
            unexpected.is_empty(),
            "strict invalid get_job cases must be Error::Http: {unexpected:?}"
        );
    }

    /// Unknown informational outer fields are tolerated for a valid Function description.
    #[tokio::test]
    async fn remote_job_description_result_unknown_outer_fields_tolerated() {
        let expected = sample_description_function();
        let body = json!({
            "job_id": "job-register-extra",
            "job_state": "DONE",
            "job_type": "register_function",
            "creation_ms": 42,
            "spec": {"ignored": true},
            "result": job_result_function_wire(&expected),
            "server_note": "informational-only",
            "extra_admin_field": 7,
        })
        .to_string();
        let description = conn_with_describe_body(body)
            .get_job("job-register-extra")
            .await
            .expect("unknown outer fields must not block description")
            .expect("job must exist");
        assert_eq!(description.job_id, "job-register-extra");
        assert_eq!(description.creation_ms, 42);
        let function = description
            .result
            .as_ref()
            .and_then(JobResult::function)
            .expect("expected Some(Function)");
        assert_exact_function(function, &expected);
    }

    /// Existing description fields and stable failure error_code stay intact with absent result.
    #[tokio::test]
    async fn remote_job_description_result_existing_fields_intact_when_result_absent() {
        let body = json!({
            "job_id": "job-1",
            "job_type": "create_index",
            "job_state": "FAILED",
            "creation_ms": 1000,
            "spec": {"column": "vec"},
            "failure": {
                "error_code": "name_or_function_not_found",
                "phase": "validate",
                "message": "looks like definition_validation_failure",
                "retryable": false
            }
        })
        .to_string();
        let description = conn_with_describe_body(body)
            .get_job("job-1")
            .await
            .expect("failure description without result must succeed")
            .expect("job must exist");
        assert_eq!(description.job_id, "job-1");
        assert_eq!(description.job_type, "create_index");
        assert_eq!(description.state, "failed");
        assert_eq!(description.creation_ms, 1000);
        assert_eq!(description.spec["column"], "vec");
        assert_eq!(description.result, None);
        let failure = description.failure.expect("failure payload present");
        match &failure.error_code {
            Some(code) => {
                assert_eq!(code, &FunctionErrorCode::NameOrFunctionNotFound);
                assert_ne!(code, &FunctionErrorCode::DefinitionValidationFailure);
            }
            None => panic!("known error_code must remain decoded while result is absent"),
        }
        assert_eq!(failure.phase.as_deref(), Some("validate"));
        assert_eq!(failure.retryable, Some(false));
    }

    /// Fixture wires round-trip through the pinned public JobResult serde.
    #[test]
    fn remote_job_description_result_fixture_wires_match_job_result_serde() {
        let none_wire = job_result_none_wire();
        let none: JobResult =
            serde_json::from_value(none_wire.clone()).expect("None wire must decode");
        assert_eq!(none, JobResult::None);
        assert_eq!(
            serde_json::to_value(JobResult::None).expect("serialize None"),
            none_wire
        );

        let expected = sample_description_function();
        let function_wire = job_result_function_wire(&expected);
        let decoded: JobResult =
            serde_json::from_value(function_wire.clone()).expect("Function wire must decode");
        match decoded {
            JobResult::Function(function) => assert_exact_function(&function, &expected),
            JobResult::None => panic!("Function fixture must not decode as None"),
        }
        assert_eq!(
            serde_json::to_value(JobResult::Function(expected)).expect("serialize Function"),
            function_wire
        );
    }

    // -------------------------------------------------------------------------
    // RegisterFunctionJobSpec remote submit transport (RED until register_function)
    // -------------------------------------------------------------------------

    const REGISTER_SOURCE_MARKER: &str = "def normalize(text, limit):\n    return text[:limit]  # SENSITIVE_REGISTER_SOURCE_MARKER\n";
    const REGISTER_SECRET_MARKER: &str = "secret://team/register-function-privacy-token";

    fn sample_register_function_job_spec() -> RegisterFunctionJobSpec {
        let signature = FunctionSignature::try_new(
            vec![
                FunctionParameter::new("text", DataType::Utf8),
                FunctionParameter::new("limit", DataType::Int32),
            ],
            FunctionOutput::new(DataType::Utf8, true),
        )
        .expect("valid FunctionSignature");
        let python = PythonFunctionDefinition::try_new(
            "normalize_mod",
            "normalize",
            REGISTER_SOURCE_MARKER,
            "3.12",
            vec!["Unidecode==1.3.8".to_string()],
        )
        .expect("valid PythonFunctionDefinition");
        let capabilities = vec![
            FunctionCapability::try_network("https://api.example.com").expect("network capability"),
            FunctionCapability::try_secret(REGISTER_SECRET_MARKER, "API_TOKEN")
                .expect("secret capability"),
        ];
        let definition = FunctionDefinition::try_new(signature, python, capabilities)
            .expect("valid FunctionDefinition");
        RegisterFunctionJobSpec::try_new("text.normalize", definition, None)
            .expect("valid RegisterFunctionJobSpec")
    }

    fn register_error_chain_text(err: &Error) -> String {
        let mut text = format!("{err}\n{err:?}");
        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
        while let Some(e) = current {
            text.push('\n');
            text.push_str(&e.to_string());
            text.push('\n');
            text.push_str(&format!("{e:?}"));
            current = e.source();
        }
        text
    }

    fn assert_register_markers_absent(err: &Error) {
        let text = register_error_chain_text(err);
        assert!(
            !text.contains(REGISTER_SOURCE_MARKER),
            "Python source marker must be absent from error/debug/source chain: {text}"
        );
        assert!(
            !text.contains(REGISTER_SECRET_MARKER),
            "secret reference marker must be absent from error/debug/source chain: {text}"
        );
    }

    fn assert_register_request(
        request: &reqwest::Request,
        expected_spec: &RegisterFunctionJobSpec,
    ) {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/functions/register");
        let body = request
            .body()
            .and_then(|b| b.as_bytes())
            .expect("register request must carry a JSON body");
        let actual: Value = serde_json::from_slice(body).expect("register body must be JSON");
        let expected =
            serde_json::to_value(expected_spec).expect("serialize RegisterFunctionJobSpec");
        assert_eq!(
            actual, expected,
            "POST body must be the exact RegisterFunctionJobSpec wire"
        );
        assert!(
            actual
                .to_string()
                .contains("SENSITIVE_REGISTER_SOURCE_MARKER"),
            "trusted request body must include full Python source"
        );
        assert_eq!(
            actual["definition"]["capabilities"][1]["reference"],
            Value::String(REGISTER_SECRET_MARKER.into()),
            "trusted request body must include secret reference"
        );
    }

    /// Successful submit uses exact path/method/body and projects a non-empty Job id.
    #[tokio::test]
    async fn register_function_submit_posts_exact_spec_and_returns_remote_job() {
        let spec = sample_register_function_job_spec();
        let expected_body = serde_json::to_value(&spec).expect("serialize spec");
        let conn = Connection::new_with_handler(move |request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/functions/register");
            let body = request.body().unwrap().as_bytes().unwrap();
            let actual: Value = serde_json::from_slice(body).unwrap();
            assert_eq!(actual, expected_body);
            http::Response::builder()
                .status(200)
                .body(r#"{"job_id":"job-register-transport-1","server_extra":{"ok":true}}"#)
                .unwrap()
        });

        let job = conn
            .register_function(spec)
            .await
            .expect("register_function submit must succeed");
        assert_eq!(
            job.id(),
            Some("job-register-transport-1"),
            "successful submit must project the non-empty job_id onto the unified remote Job"
        );
    }

    /// One retry keeps the SDK-generated request id and exact body before success.
    #[tokio::test]
    async fn register_function_submit_retry_preserves_request_id_and_body() {
        let spec = sample_register_function_job_spec();
        let expected_body = serde_json::to_value(&spec).expect("serialize spec");
        let seen_request_id = Arc::new(OnceLock::new());
        let seen_request_id_ref = seen_request_id.clone();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();

        let expected_spec = sample_register_function_job_spec();
        let conn = Connection::new_with_handler_and_config(
            move |request| {
                assert_register_request(&request, &expected_spec);
                assert_eq!(
                    serde_json::from_slice::<Value>(request.body().unwrap().as_bytes().unwrap())
                        .unwrap(),
                    expected_body
                );

                let request_id = request.headers()["x-request-id"]
                    .to_str()
                    .unwrap()
                    .to_string();
                assert!(!request_id.is_empty(), "SDK must generate a request id");
                let seen = seen_request_id_ref.get_or_init(|| request_id.clone());
                assert_eq!(
                    &request_id, seen,
                    "request id must be identical across retries"
                );

                let n = attempts_ref.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    http::Response::builder()
                        .status(500)
                        .body("transient register failure")
                        .unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"job_id":"job-register-retry-1"}"#)
                        .unwrap()
                }
            },
            ClientConfig {
                retry_config: RetryConfig {
                    retries: Some(2),
                    backoff_factor: Some(0.0),
                    backoff_jitter: Some(0.0),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let job = conn
            .register_function(spec)
            .await
            .expect("register_function must succeed after one retry");
        assert_eq!(job.id(), Some("job-register-retry-1"));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert!(seen_request_id.get().is_some());
    }

    /// Missing/null/empty/wrong-type/malformed job_id fail closed as Error::Http.
    #[tokio::test]
    async fn register_function_submit_invalid_job_id_is_http_without_markers() {
        let cases: Vec<(&str, String)> = vec![
            ("missing", r#"{"server_extra":true}"#.to_string()),
            ("null", r#"{"job_id":null}"#.to_string()),
            ("empty", r#"{"job_id":""}"#.to_string()),
            ("wrong_type", r#"{"job_id":123}"#.to_string()),
            ("malformed", "not-json".to_string()),
        ];

        let mut unexpected = Vec::new();
        for (label, response_body) in cases {
            let spec = sample_register_function_job_spec();
            let expected_body = serde_json::to_value(&spec).expect("serialize spec");
            let body_for_handler = response_body.clone();
            let conn = Connection::new_with_handler(move |request| {
                assert_eq!(request.url().path(), "/v1/functions/register");
                let actual: Value =
                    serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
                assert_eq!(actual, expected_body);
                http::Response::builder()
                    .status(200)
                    .body(body_for_handler.clone())
                    .unwrap()
            });

            match conn.register_function(spec).await {
                Err(err @ Error::Http { .. }) => assert_register_markers_absent(&err),
                other => unexpected.push(format!("{label}: {other:?}")),
            }
        }
        assert!(
            unexpected.is_empty(),
            "invalid job_id shapes must fail closed as Error::Http: {unexpected:?}"
        );
    }

    /// Non-retry 4xx and exhausted 5xx bodies that echo markers stay out of error text.
    #[tokio::test]
    async fn register_function_submit_error_bodies_omit_sensitive_markers() {
        let echoed =
            format!("register failed with {REGISTER_SOURCE_MARKER} and {REGISTER_SECRET_MARKER}");

        // Non-retryable 4xx
        {
            let spec = sample_register_function_job_spec();
            let body = echoed.clone();
            let conn = Connection::new_with_handler(move |request| {
                assert_eq!(request.url().path(), "/v1/functions/register");
                http::Response::builder()
                    .status(400)
                    .body(body.clone())
                    .unwrap()
            });
            let err = conn
                .register_function(spec)
                .await
                .expect_err("4xx register submit must fail");
            assert!(
                matches!(err, Error::Http { .. }),
                "non-retry 4xx must surface as Error::Http, got {err:?}"
            );
            assert_register_markers_absent(&err);
        }

        // Exhausted retryable 5xx
        {
            let spec = sample_register_function_job_spec();
            let body = echoed.clone();
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_ref = attempts.clone();
            let conn = Connection::new_with_handler_and_config(
                move |request| {
                    attempts_ref.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(request.url().path(), "/v1/functions/register");
                    http::Response::builder()
                        .status(500)
                        .body(body.clone())
                        .unwrap()
                },
                ClientConfig {
                    retry_config: RetryConfig {
                        // RetryCounter treats `retries` as max request failures, so
                        // retries=2 yields exactly two transport attempts before Error::Retry.
                        retries: Some(2),
                        backoff_factor: Some(0.0),
                        backoff_jitter: Some(0.0),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            );
            let err = conn
                .register_function(spec)
                .await
                .expect_err("exhausted 5xx register submit must fail");
            assert!(
                matches!(err, Error::Retry { .. }),
                "exhausted 5xx must surface as Error::Retry, got {err:?}"
            );
            assert_eq!(
                attempts.load(Ordering::SeqCst),
                2,
                "RetryCounter max request failures=2 must make exactly two transport attempts"
            );
            assert_register_markers_absent(&err);
        }
    }

    /// Local databases reject registration without mutating database state.
    #[tokio::test]
    async fn register_function_local_database_returns_not_supported_without_mutation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let conn = ConnectBuilder::new(dir.path().to_str().unwrap())
            .execute()
            .await
            .expect("local connect");
        let before = conn
            .table_names()
            .execute()
            .await
            .expect("table_names before");
        assert!(before.is_empty());

        let err = conn
            .register_function(sample_register_function_job_spec())
            .await
            .expect_err("local register_function must be unsupported");
        assert!(
            matches!(err, Error::NotSupported { .. }),
            "expected NotSupported, got {err:?}"
        );

        let after = conn
            .table_names()
            .execute()
            .await
            .expect("table_names after");
        assert_eq!(before, after, "unsupported register must not mutate tables");
    }

    /// Submit then existing /v1/jobs/describe returns the exact Function (no name lookup).
    #[tokio::test]
    async fn register_function_submit_then_describe_returns_exact_function() {
        let expected = sample_description_function();
        let function_wire = job_result_function_wire(&expected);
        let describe_body = describe_body(
            "job-register-wait-1",
            "DONE",
            JsonField::Present(Value::String("register_function".into())),
            JsonField::Present(function_wire),
        );
        let spec = sample_register_function_job_spec();
        let expected_spec_body = serde_json::to_value(&spec).expect("serialize spec");
        let paths = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let paths_ref = paths.clone();

        let conn = Connection::new_with_handler(move |request| {
            let path = request.url().path().to_string();
            paths_ref.lock().unwrap().push(path.clone());
            match path.as_str() {
                "/v1/functions/register" => {
                    assert_eq!(request.method(), &reqwest::Method::POST);
                    let actual: Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    assert_eq!(actual, expected_spec_body);
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"job_id":"job-register-wait-1"}"#.to_string())
                        .unwrap()
                }
                "/v1/jobs/describe" => {
                    assert_eq!(request.method(), &reqwest::Method::POST);
                    let body: Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    assert_eq!(body["job_id"], "job-register-wait-1");
                    assert!(
                        body.get("name").is_none(),
                        "describe must not perform a second name lookup: {body}"
                    );
                    http::Response::builder()
                        .status(200)
                        .body(describe_body.clone())
                        .unwrap()
                }
                other => panic!("unexpected path for register+wait flow: {other}"),
            }
        });

        let job = conn
            .register_function(spec)
            .await
            .expect("register_function submit must return a Job");
        assert_eq!(job.id(), Some("job-register-wait-1"));

        let waited = job.wait().await.expect("wait via /v1/jobs/describe");
        let function = waited
            .function()
            .expect("register_function success must be JobResult::Function");
        assert_exact_function(function, &expected);

        let seen = paths.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![
                "/v1/functions/register".to_string(),
                "/v1/jobs/describe".to_string(),
            ],
            "flow must be submit then describe only, with no Function name lookup"
        );
    }

    // -------------------------------------------------------------------------
    // Function lookup transport (RED until lookup_function_by_{name,id})
    // -------------------------------------------------------------------------

    const LOOKUP_CATALOG_NAME: &str = "text.normalize.lookup-name";
    const LOOKUP_FUNCTION_ID: &str = "fn.exact.lookup-handle";
    const LOOKUP_SERVER_MESSAGE_MARKER: &str =
        "SERVER_LOOKUP_DIAGNOSTIC_MARKER name=text.normalize.lookup-name id=fn.exact.lookup-handle";

    fn sample_lookup_function() -> Function {
        let id = FunctionId::try_new(LOOKUP_FUNCTION_ID).expect("valid FunctionId");
        let signature = FunctionSignature::try_new(
            vec![
                FunctionParameter::new("text", DataType::Utf8),
                FunctionParameter::new("limit", DataType::Int32),
            ],
            FunctionOutput::new(DataType::Utf8, true),
        )
        .expect("valid FunctionSignature");
        Function::new(id, signature)
    }

    fn lookup_function_wire(function: &Function) -> Value {
        serde_json::to_value(function).expect("serialize Function wire")
    }

    fn lookup_success_body(function: &Function, extra_outer: Option<Value>) -> String {
        let mut body = json!({
            "function": lookup_function_wire(function),
        });
        if let Some(Value::Object(extra)) = extra_outer {
            let object = body.as_object_mut().expect("lookup success must be object");
            for (k, v) in extra {
                object.insert(k, v);
            }
        }
        body.to_string()
    }

    fn lookup_error_chain_text(err: &Error) -> String {
        let mut text = format!("{err}\n{err:?}");
        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
        while let Some(e) = current {
            text.push('\n');
            text.push_str(&e.to_string());
            text.push('\n');
            text.push_str(&format!("{e:?}"));
            current = e.source();
        }
        text
    }

    fn assert_lookup_payload_free(err: &Error) {
        let text = lookup_error_chain_text(err);
        assert!(
            !text.contains(LOOKUP_SERVER_MESSAGE_MARKER),
            "server diagnostic marker must be absent from error/debug/source chain: {text}"
        );
        assert!(
            !text.contains(LOOKUP_CATALOG_NAME),
            "catalog name must be absent from error/debug/source chain: {text}"
        );
        assert!(
            !text.contains(LOOKUP_FUNCTION_ID),
            "FunctionId must be absent from error/debug/source chain: {text}"
        );
        assert!(
            !text.contains("SENSITIVE_LOOKUP_BODY_MARKER"),
            "non-success/malformed body marker must be absent from error/debug/source chain: {text}"
        );
    }

    fn assert_lookup_name_request(request: &reqwest::Request, expected_name: &str) {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/functions/lookup");
        let body = request
            .body()
            .and_then(|b| b.as_bytes())
            .expect("lookup request must carry a JSON body");
        let actual: Value = serde_json::from_slice(body).expect("lookup body must be JSON");
        assert_eq!(
            actual,
            json!({ "name": expected_name }),
            "name lookup body must be exactly {{\"name\":...}}"
        );
        assert!(
            actual.get("function_id").is_none(),
            "name lookup must not send function_id: {actual}"
        );
    }

    fn assert_lookup_id_request(request: &reqwest::Request, expected_id: &str) {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/functions/lookup");
        let body = request
            .body()
            .and_then(|b| b.as_bytes())
            .expect("lookup request must carry a JSON body");
        let actual: Value = serde_json::from_slice(body).expect("lookup body must be JSON");
        assert_eq!(
            actual,
            json!({ "function_id": expected_id }),
            "id lookup body must be exactly {{\"function_id\":...}}"
        );
        assert!(
            actual.get("name").is_none(),
            "id lookup must not send name: {actual}"
        );
    }

    /// Name lookup uses exact path/body and returns the immutable Function value.
    #[tokio::test]
    async fn lookup_function_by_name_posts_exact_body_and_returns_function_without_name() {
        let expected = sample_lookup_function();
        let body = lookup_success_body(&expected, None);
        let conn = Connection::new_with_handler(move |request| {
            assert_lookup_name_request(&request, LOOKUP_CATALOG_NAME);
            http::Response::builder()
                .status(200)
                .body(body.clone())
                .unwrap()
        });

        let function = conn
            .lookup_function_by_name(LOOKUP_CATALOG_NAME)
            .await
            .expect("name lookup must succeed");
        assert_exact_function(&function, &expected);

        let debug = format!("{function:?}");
        let wire = serde_json::to_value(&function).expect("Function serializes");
        assert!(
            !debug.contains(LOOKUP_CATALOG_NAME),
            "returned Function debug must not carry the catalog name: {debug}"
        );
        assert!(
            wire.get("name").is_none(),
            "returned Function wire must not include a name field: {wire}"
        );
        assert_eq!(function.id().as_str(), LOOKUP_FUNCTION_ID);
    }

    /// Exact-ID lookup uses exact path/body and is independent of catalog name.
    #[tokio::test]
    async fn lookup_function_by_id_posts_exact_body_and_returns_function() {
        let expected = sample_lookup_function();
        let body = lookup_success_body(&expected, None);
        let id = FunctionId::try_new(LOOKUP_FUNCTION_ID).expect("valid FunctionId");
        let conn = Connection::new_with_handler(move |request| {
            assert_lookup_id_request(&request, LOOKUP_FUNCTION_ID);
            http::Response::builder()
                .status(200)
                .body(body.clone())
                .unwrap()
        });

        let function = conn
            .lookup_function_by_id(&id)
            .await
            .expect("id lookup must succeed");
        assert_exact_function(&function, &expected);
        let debug = format!("{function:?}");
        assert!(
            !debug.contains(LOOKUP_CATALOG_NAME),
            "id lookup Function must not invent a catalog name: {debug}"
        );
    }

    /// Unknown outer success fields are accepted; Function decoding stays strict.
    #[tokio::test]
    async fn lookup_function_accepts_additive_outer_success_fields() {
        let expected = sample_lookup_function();
        let body = lookup_success_body(
            &expected,
            Some(json!({
                "server_extra": {"ok": true},
                "request_echo_name": LOOKUP_CATALOG_NAME,
            })),
        );
        let conn = Connection::new_with_handler(move |request| {
            assert_lookup_name_request(&request, LOOKUP_CATALOG_NAME);
            http::Response::builder()
                .status(200)
                .body(body.clone())
                .unwrap()
        });

        let function = conn
            .lookup_function_by_name(LOOKUP_CATALOG_NAME)
            .await
            .expect("additive outer success must decode");
        assert_exact_function(&function, &expected);
    }

    /// Empty selectors fail closed before any HTTP request is issued.
    #[tokio::test]
    async fn lookup_function_empty_selectors_fail_before_transport() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();
        let conn = Connection::new_with_handler(move |_request| -> http::Response<String> {
            attempts_ref.fetch_add(1, Ordering::SeqCst);
            panic!("empty selector must not issue an HTTP request");
        });

        let err = conn
            .lookup_function_by_name("")
            .await
            .expect_err("empty name must fail before transport");
        assert!(
            matches!(err, Error::InvalidInput { .. }),
            "empty name must be InvalidInput, got {err:?}"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            0,
            "empty name must not touch transport"
        );

        // Empty FunctionId cannot be constructed; lookup by id is therefore
        // unreachable with an empty selector. Keep the contract explicit.
        let empty_id = FunctionId::try_new("").expect_err("empty FunctionId rejected");
        assert!(
            matches!(empty_id, Error::InvalidInput { .. }),
            "empty FunctionId must be InvalidInput, got {empty_id:?}"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            0,
            "empty FunctionId construction must not touch transport"
        );
    }

    /// Known explicit not-found code becomes Error::Function; status/message ignored.
    #[tokio::test]
    async fn lookup_function_explicit_not_found_code_is_function_error() {
        let body = json!({
            "error_code": "name_or_function_not_found",
            "message": LOOKUP_SERVER_MESSAGE_MARKER,
            "looks_like": "definition_validation_failure",
        })
        .to_string();
        let conn = Connection::new_with_handler(move |request| {
            assert_lookup_name_request(&request, LOOKUP_CATALOG_NAME);
            http::Response::builder()
                .status(404)
                .body(body.clone())
                .unwrap()
        });

        let err = conn
            .lookup_function_by_name(LOOKUP_CATALOG_NAME)
            .await
            .expect_err("missing name must fail");
        match &err {
            Error::Function { code, message } => {
                assert_eq!(code.as_str(), "name_or_function_not_found");
                assert_ne!(code.as_str(), "definition_validation_failure");
                assert!(
                    !message.contains(LOOKUP_SERVER_MESSAGE_MARKER),
                    "Function error message must be sanitized, got {message}"
                );
                assert!(
                    !message.contains(LOOKUP_CATALOG_NAME),
                    "Function error message must not echo the catalog name, got {message}"
                );
            }
            other => panic!("expected Error::Function, got {other:?}"),
        }
        assert_lookup_payload_free(&err);
    }

    /// Unknown nonempty explicit code is preserved; HTTP status does not override it.
    #[tokio::test]
    async fn lookup_function_preserves_unknown_explicit_code_despite_status_and_message() {
        let raw = "enterprise_future_lookup_category_xyz";
        let body = json!({
            "error_code": raw,
            "message": format!(
                "{LOOKUP_SERVER_MESSAGE_MARKER} revoked_function name_or_function_not_found"
            ),
        })
        .to_string();
        let conn = Connection::new_with_handler(move |request| {
            assert_lookup_id_request(&request, LOOKUP_FUNCTION_ID);
            http::Response::builder()
                .status(409)
                .body(body.clone())
                .unwrap()
        });
        let id = FunctionId::try_new(LOOKUP_FUNCTION_ID).expect("valid FunctionId");

        let err = conn
            .lookup_function_by_id(&id)
            .await
            .expect_err("unknown explicit code must surface");
        match &err {
            Error::Function { code, message } => {
                assert_eq!(code.as_str(), raw);
                assert!(
                    matches!(code, FunctionErrorCode::Unrecognized(_)),
                    "unknown code must stay Unrecognized, got {code:?}"
                );
                assert!(
                    !message.contains(LOOKUP_SERVER_MESSAGE_MARKER),
                    "diagnostic message must be sanitized"
                );
                assert_ne!(code.as_str(), "revoked_function");
                assert_ne!(code.as_str(), "name_or_function_not_found");
            }
            other => panic!("expected Error::Function, got {other:?}"),
        }
        assert_lookup_payload_free(&err);
    }

    /// Missing/empty/wrong-type error_code on non-success stays payload-free Http.
    #[tokio::test]
    async fn lookup_function_missing_or_invalid_error_code_is_payload_free_http() {
        let cases: Vec<(&str, u16, String)> = vec![
            (
                "missing_code_404",
                404,
                json!({
                    "message": LOOKUP_SERVER_MESSAGE_MARKER,
                    "SENSITIVE_LOOKUP_BODY_MARKER": true,
                })
                .to_string(),
            ),
            (
                "empty_code",
                400,
                json!({
                    "error_code": "",
                    "message": LOOKUP_SERVER_MESSAGE_MARKER,
                    "SENSITIVE_LOOKUP_BODY_MARKER": true,
                })
                .to_string(),
            ),
            (
                "wrong_type_code",
                400,
                json!({
                    "error_code": 123,
                    "message": LOOKUP_SERVER_MESSAGE_MARKER,
                    "SENSITIVE_LOOKUP_BODY_MARKER": true,
                })
                .to_string(),
            ),
            (
                "null_code",
                404,
                json!({
                    "error_code": null,
                    "message": LOOKUP_SERVER_MESSAGE_MARKER,
                    "SENSITIVE_LOOKUP_BODY_MARKER": true,
                })
                .to_string(),
            ),
            (
                "non_json",
                404,
                format!("not-json {LOOKUP_SERVER_MESSAGE_MARKER} SENSITIVE_LOOKUP_BODY_MARKER"),
            ),
        ];

        let mut unexpected = Vec::new();
        for (label, status, response_body) in cases {
            let body_for_handler = response_body.clone();
            let conn = Connection::new_with_handler(move |request| {
                assert_lookup_name_request(&request, LOOKUP_CATALOG_NAME);
                http::Response::builder()
                    .status(status)
                    .body(body_for_handler.clone())
                    .unwrap()
            });
            match conn.lookup_function_by_name(LOOKUP_CATALOG_NAME).await {
                Err(err @ Error::Http { .. }) => assert_lookup_payload_free(&err),
                Err(Error::Function { .. }) => unexpected.push(format!(
                    "{label}: must not invent Error::Function without explicit code"
                )),
                other => unexpected.push(format!("{label}: {other:?}")),
            }
        }
        assert!(
            unexpected.is_empty(),
            "invalid/missing error_code must stay payload-free Http: {unexpected:?}"
        );
    }

    /// Malformed/missing/invalid success payloads stay payload-free Http.
    #[tokio::test]
    async fn lookup_function_invalid_success_payload_is_payload_free_http() {
        let cases: Vec<(&str, String)> = vec![
            (
                "missing_function",
                json!({
                    "server_extra": true,
                    "SENSITIVE_LOOKUP_BODY_MARKER": LOOKUP_SERVER_MESSAGE_MARKER,
                })
                .to_string(),
            ),
            (
                "null_function",
                json!({
                    "function": null,
                    "SENSITIVE_LOOKUP_BODY_MARKER": LOOKUP_SERVER_MESSAGE_MARKER,
                })
                .to_string(),
            ),
            (
                "wrong_type_function",
                json!({
                    "function": "not-an-object",
                    "SENSITIVE_LOOKUP_BODY_MARKER": LOOKUP_SERVER_MESSAGE_MARKER,
                })
                .to_string(),
            ),
            (
                "invalid_function_unknown_field",
                json!({
                    "function": {
                        "format_version": 1,
                        "id": LOOKUP_FUNCTION_ID,
                        "signature": {
                            "parameters": [],
                            "output": {
                                "data_type_ipc": lookup_function_wire(&sample_lookup_function())
                                    ["signature"]["output"]["data_type_ipc"].clone(),
                                "nullable": true
                            }
                        },
                        "name": LOOKUP_CATALOG_NAME,
                        "SENSITIVE_LOOKUP_BODY_MARKER": true
                    }
                })
                .to_string(),
            ),
            (
                "malformed_json",
                format!("not-json {LOOKUP_SERVER_MESSAGE_MARKER} SENSITIVE_LOOKUP_BODY_MARKER"),
            ),
        ];

        let mut unexpected = Vec::new();
        for (label, response_body) in cases {
            let body_for_handler = response_body.clone();
            let conn = Connection::new_with_handler(move |request| {
                assert_lookup_name_request(&request, LOOKUP_CATALOG_NAME);
                http::Response::builder()
                    .status(200)
                    .body(body_for_handler.clone())
                    .unwrap()
            });
            match conn.lookup_function_by_name(LOOKUP_CATALOG_NAME).await {
                Err(err @ Error::Http { .. }) => assert_lookup_payload_free(&err),
                other => unexpected.push(format!("{label}: {other:?}")),
            }
        }
        assert!(
            unexpected.is_empty(),
            "invalid success payloads must fail closed as payload-free Http: {unexpected:?}"
        );
    }

    /// Local databases reject both lookup seams without mutating state.
    #[tokio::test]
    async fn lookup_function_local_database_returns_not_supported_without_mutation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let conn = ConnectBuilder::new(dir.path().to_str().unwrap())
            .execute()
            .await
            .expect("local connect");
        let before = conn
            .table_names()
            .execute()
            .await
            .expect("table_names before");
        assert!(before.is_empty());

        let err_name = conn
            .lookup_function_by_name(LOOKUP_CATALOG_NAME)
            .await
            .expect_err("local name lookup must be unsupported");
        assert!(
            matches!(err_name, Error::NotSupported { .. }),
            "expected NotSupported for name lookup, got {err_name:?}"
        );

        let id = FunctionId::try_new(LOOKUP_FUNCTION_ID).expect("valid FunctionId");
        let err_id = conn
            .lookup_function_by_id(&id)
            .await
            .expect_err("local id lookup must be unsupported");
        assert!(
            matches!(err_id, Error::NotSupported { .. }),
            "expected NotSupported for id lookup, got {err_id:?}"
        );

        let after = conn
            .table_names()
            .execute()
            .await
            .expect("table_names after");
        assert_eq!(before, after, "unsupported lookup must not mutate tables");
    }

    /// Empty name is a public Connection invariant: InvalidInput on local too.
    #[tokio::test]
    async fn lookup_function_local_empty_name_is_invalid_input_without_mutation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let conn = ConnectBuilder::new(dir.path().to_str().unwrap())
            .execute()
            .await
            .expect("local connect");
        let before = conn
            .table_names()
            .execute()
            .await
            .expect("table_names before");
        assert!(before.is_empty());

        let err = conn
            .lookup_function_by_name("")
            .await
            .expect_err("empty name must fail before backend dispatch");
        assert!(
            matches!(err, Error::InvalidInput { .. }),
            "empty name must be InvalidInput on local Connection, got {err:?}"
        );

        let after = conn
            .table_names()
            .execute()
            .await
            .expect("table_names after");
        assert_eq!(before, after, "empty-name rejection must not mutate tables");
    }

    /// Database trait seam must reject empty names as InvalidInput (not NotSupported).
    ///
    /// `Connection::database()` exposes `Arc<dyn Database>`; callers that bypass
    /// Connection prevalidation must still get backend-independent InvalidInput.
    #[tokio::test]
    async fn lookup_function_local_database_trait_empty_name_is_invalid_input_without_mutation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let conn = ConnectBuilder::new(dir.path().to_str().unwrap())
            .execute()
            .await
            .expect("local connect");
        let before = conn
            .table_names()
            .execute()
            .await
            .expect("table_names before");
        assert!(before.is_empty());

        let err = conn
            .database()
            .lookup_function_by_name("")
            .await
            .expect_err("empty name must fail on Database trait default");
        assert!(
            matches!(err, Error::InvalidInput { .. }),
            "empty name via Database trait must be InvalidInput, got {err:?}"
        );

        let after = conn
            .table_names()
            .execute()
            .await
            .expect("table_names after");
        assert_eq!(
            before, after,
            "Database-trait empty-name rejection must not mutate tables"
        );
    }

    /// One retry keeps the SDK-generated request id and exact body before success.
    #[tokio::test]
    async fn lookup_function_retry_preserves_request_id_and_body() {
        let expected = sample_lookup_function();
        let success_body = lookup_success_body(&expected, None);
        let seen_request_id = Arc::new(OnceLock::new());
        let seen_request_id_ref = seen_request_id.clone();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();

        let conn = Connection::new_with_handler_and_config(
            move |request| {
                assert_lookup_name_request(&request, LOOKUP_CATALOG_NAME);
                let request_id = request.headers()["x-request-id"]
                    .to_str()
                    .unwrap()
                    .to_string();
                assert!(!request_id.is_empty(), "SDK must generate a request id");
                let seen = seen_request_id_ref.get_or_init(|| request_id.clone());
                assert_eq!(
                    &request_id, seen,
                    "request id must be identical across retries"
                );

                let n = attempts_ref.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    http::Response::builder()
                        .status(500)
                        .body(format!(
                            "{LOOKUP_SERVER_MESSAGE_MARKER} SENSITIVE_LOOKUP_BODY_MARKER"
                        ))
                        .unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(success_body.clone())
                        .unwrap()
                }
            },
            ClientConfig {
                retry_config: RetryConfig {
                    retries: Some(2),
                    backoff_factor: Some(0.0),
                    backoff_jitter: Some(0.0),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let function = conn
            .lookup_function_by_name(LOOKUP_CATALOG_NAME)
            .await
            .expect("lookup must succeed after one retry");
        assert_exact_function(&function, &expected);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert!(seen_request_id.get().is_some());
    }

    /// Response-body read failures consume the read budget, keep request id/body.
    #[tokio::test]
    async fn lookup_function_retries_response_body_read_failure_with_read_budget() {
        let expected = sample_lookup_function();
        let success_body = lookup_success_body(&expected, None);
        let seen_request_id = Arc::new(OnceLock::new());
        let seen_request_id_ref = seen_request_id.clone();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();

        let conn = Connection::new_with_handler_and_config(
            move |request| {
                assert_lookup_name_request(&request, LOOKUP_CATALOG_NAME);
                let request_id = request.headers()["x-request-id"]
                    .to_str()
                    .unwrap()
                    .to_string();
                assert!(!request_id.is_empty(), "SDK must generate a request id");
                let seen = seen_request_id_ref.get_or_init(|| request_id.clone());
                assert_eq!(
                    &request_id, seen,
                    "request id must be identical across retries"
                );

                let n = attempts_ref.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    let stream = futures::stream::once(async {
                        Err::<bytes::Bytes, _>(std::io::Error::other(
                            "simulated lookup response body read failure",
                        ))
                    });
                    http::Response::builder()
                        .status(200)
                        .body(reqwest::Body::wrap_stream(stream))
                        .unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(reqwest::Body::from(success_body.clone()))
                        .unwrap()
                }
            },
            ClientConfig {
                // retries=1 would exhaust immediately if body-read were misclassified
                // as a request failure; read_retries=2 allows one read failure then success.
                retry_config: RetryConfig {
                    retries: Some(1),
                    read_retries: Some(2),
                    connect_retries: Some(1),
                    backoff_factor: Some(0.0),
                    backoff_jitter: Some(0.0),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let function = conn
            .lookup_function_by_name(LOOKUP_CATALOG_NAME)
            .await
            .expect("lookup must succeed after one response-body read retry");
        assert_exact_function(&function, &expected);
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "body-read failure must consume read budget and retry once"
        );
        assert!(seen_request_id.get().is_some());
    }

    /// Nonretryable client/header errors must not be repeated.
    #[tokio::test]
    async fn lookup_function_nonretryable_client_error_is_not_repeated() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_ref = calls.clone();

        #[derive(Debug)]
        struct CountingErrorHeaderProvider {
            calls: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl HeaderProvider for CountingErrorHeaderProvider {
            async fn get_headers(&self) -> crate::Result<HashMap<String, String>> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Err(Error::Runtime {
                    message: "Failed to fetch auth token".to_string(),
                })
            }
        }

        let conn = Connection::new_with_handler_and_config(
            move |_request| -> http::Response<&'static str> {
                panic!("lookup must not reach transport when header provider fails");
            },
            ClientConfig {
                header_provider: Some(Arc::new(CountingErrorHeaderProvider { calls: calls_ref })
                    as Arc<dyn HeaderProvider>),
                retry_config: RetryConfig {
                    retries: Some(3),
                    connect_retries: Some(3),
                    read_retries: Some(3),
                    backoff_factor: Some(0.0),
                    backoff_jitter: Some(0.0),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let err = conn
            .lookup_function_by_name(LOOKUP_CATALOG_NAME)
            .await
            .expect_err("header provider failure must surface");
        match err {
            Error::Runtime { message } => {
                assert_eq!(message, "Failed to fetch auth token");
            }
            other => panic!("expected Runtime from header provider, got {other:?}"),
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "nonretryable client/header error must not be repeated"
        );
    }

    // -------------------------------------------------------------------------
    // Conditional Function name removal
    //
    // Direct synchronous catalog CAS via POST /v1/functions/remove. Not a Job
    // and not physical Function deletion. Caller supplies an observed immutable
    // Function handle; only current.id is authority on the wire.
    // -------------------------------------------------------------------------

    const REMOVE_CATALOG_NAME: &str = "text.normalize.remove-name";
    const REMOVE_FUNCTION_ID: &str = "fn.exact.remove-handle";
    const REMOVE_SERVER_MESSAGE_MARKER: &str =
        "SERVER_REMOVE_DIAGNOSTIC_MARKER name=text.normalize.remove-name id=fn.exact.remove-handle";

    fn sample_remove_function() -> Function {
        let id = FunctionId::try_new(REMOVE_FUNCTION_ID).expect("valid FunctionId");
        let signature = FunctionSignature::try_new(
            vec![
                FunctionParameter::new("text", DataType::Utf8),
                FunctionParameter::new("limit", DataType::Int32),
            ],
            FunctionOutput::new(DataType::Utf8, true),
        )
        .expect("valid FunctionSignature");
        Function::new(id, signature)
    }

    fn remove_error_chain_text(err: &Error) -> String {
        let mut text = format!("{err}\n{err:?}");
        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
        while let Some(e) = current {
            text.push('\n');
            text.push_str(&e.to_string());
            text.push('\n');
            text.push_str(&format!("{e:?}"));
            current = e.source();
        }
        text
    }

    fn assert_remove_payload_free(err: &Error) {
        let text = remove_error_chain_text(err);
        assert!(
            !text.contains(REMOVE_SERVER_MESSAGE_MARKER),
            "server diagnostic marker must be absent from error/debug/source chain: {text}"
        );
        assert!(
            !text.contains(REMOVE_CATALOG_NAME),
            "catalog name must be absent from error/debug/source chain: {text}"
        );
        assert!(
            !text.contains(REMOVE_FUNCTION_ID),
            "FunctionId must be absent from error/debug/source chain: {text}"
        );
        assert!(
            !text.contains("SENSITIVE_REMOVE_BODY_MARKER"),
            "non-success/malformed body marker must be absent from error/debug/source chain: {text}"
        );
    }

    fn assert_remove_request(
        request: &reqwest::Request,
        expected_name: &str,
        expected_function_id: &str,
    ) {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/functions/remove");
        assert!(
            request.url().query().is_none(),
            "remove selectors must stay out of the URL query: {}",
            request.url()
        );
        let body = request
            .body()
            .and_then(|b| b.as_bytes())
            .expect("remove request must carry a JSON body");
        let actual: Value = serde_json::from_slice(body).expect("remove body must be JSON");
        assert_eq!(
            actual,
            json!({
                "name": expected_name,
                "expected_current_function_id": expected_function_id,
            }),
            "remove body must be exactly {{\"name\":...,\"expected_current_function_id\":...}}"
        );
        let object = actual.as_object().expect("remove body must be an object");
        assert_eq!(
            object.len(),
            2,
            "remove body must not carry format_version or extra user fields: {actual}"
        );
        assert!(
            object.get("format_version").is_none(),
            "remove body must not include format_version: {actual}"
        );
        assert!(
            object.get("function_id").is_none(),
            "remove body must use expected_current_function_id, not function_id: {actual}"
        );
        assert!(
            object.get("current").is_none() && object.get("function").is_none(),
            "remove must not send a Function record: {actual}"
        );
        assert!(
            object.get("job_id").is_none() && object.get("idempotency_key").is_none(),
            "remove is not a Job and must not send user idempotency keys: {actual}"
        );
    }

    /// Exact path/body and 204 success use only the observed Function id.
    #[tokio::test]
    async fn remove_function_name_posts_exact_body_and_succeeds_on_204() {
        let current = sample_remove_function();
        let expected_id = current.id().as_str().to_string();
        let conn = Connection::new_with_handler(move |request| {
            assert_remove_request(&request, REMOVE_CATALOG_NAME, &expected_id);
            // Illegal body on 204 must be ignored; success is status-driven only.
            http::Response::builder()
                .status(204)
                .body(format!(
                    "{{\"SENSITIVE_REMOVE_BODY_MARKER\":true,\"message\":{REMOVE_SERVER_MESSAGE_MARKER:?}}}"
                ))
                .unwrap()
        });

        conn.remove_function_name(REMOVE_CATALOG_NAME, &current)
            .await
            .expect("HTTP 204 must complete the CAS");
    }

    /// One configured 5xx retry then 204 keeps identical request id/body.
    #[tokio::test]
    async fn remove_function_name_retry_preserves_request_id_and_body() {
        let current = sample_remove_function();
        let expected_id = current.id().as_str().to_string();
        let seen_request_id = Arc::new(OnceLock::new());
        let seen_request_id_ref = seen_request_id.clone();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();

        let conn = Connection::new_with_handler_and_config(
            move |request| {
                assert_remove_request(&request, REMOVE_CATALOG_NAME, &expected_id);
                let request_id = request.headers()["x-request-id"]
                    .to_str()
                    .unwrap()
                    .to_string();
                assert!(!request_id.is_empty(), "SDK must generate a request id");
                let seen = seen_request_id_ref.get_or_init(|| request_id.clone());
                assert_eq!(
                    &request_id, seen,
                    "request id must be identical across retries"
                );

                let n = attempts_ref.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    http::Response::builder()
                        .status(500)
                        .body(format!(
                            "{REMOVE_SERVER_MESSAGE_MARKER} SENSITIVE_REMOVE_BODY_MARKER"
                        ))
                        .unwrap()
                } else {
                    http::Response::builder()
                        .status(204)
                        .body(String::new())
                        .unwrap()
                }
            },
            ClientConfig {
                retry_config: RetryConfig {
                    retries: Some(2),
                    backoff_factor: Some(0.0),
                    backoff_jitter: Some(0.0),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        conn.remove_function_name(REMOVE_CATALOG_NAME, &current)
            .await
            .expect("remove must succeed after one retry");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "one 5xx then 204 must be exactly two attempts"
        );
        assert!(seen_request_id.get().is_some());
    }

    /// Exhausted always-retryable 5xx without explicit error_code surfaces Error::Retry.
    ///
    /// retries=2 is max request failures: exactly two identical attempts, then
    /// Retry with request_failures == max_request_failures == 2, zero connect/read
    /// failures, the retryable status retained, and a payload-free source chain.
    #[tokio::test]
    async fn remove_function_name_exhausted_retryable_5xx_returns_retry_with_request_counters() {
        let current = sample_remove_function();
        let expected_id = current.id().as_str().to_string();
        let seen_request_id = Arc::new(OnceLock::new());
        let seen_request_id_ref = seen_request_id.clone();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();
        let body = format!("{REMOVE_SERVER_MESSAGE_MARKER} SENSITIVE_REMOVE_BODY_MARKER");

        let conn = Connection::new_with_handler_and_config(
            move |request| {
                assert_remove_request(&request, REMOVE_CATALOG_NAME, &expected_id);
                let request_id = request.headers()["x-request-id"]
                    .to_str()
                    .unwrap()
                    .to_string();
                assert!(!request_id.is_empty(), "SDK must generate a request id");
                let seen = seen_request_id_ref.get_or_init(|| request_id.clone());
                assert_eq!(
                    &request_id, seen,
                    "request id must be identical across exhausted retries"
                );

                attempts_ref.fetch_add(1, Ordering::SeqCst);
                http::Response::builder()
                    .status(500)
                    .body(body.clone())
                    .unwrap()
            },
            ClientConfig {
                retry_config: RetryConfig {
                    // RetryCounter treats `retries` as max request failures, so
                    // retries=2 yields exactly two transport attempts before Error::Retry.
                    retries: Some(2),
                    backoff_factor: Some(0.0),
                    backoff_jitter: Some(0.0),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let err = conn
            .remove_function_name(REMOVE_CATALOG_NAME, &current)
            .await
            .expect_err("exhausted retryable 5xx must fail");
        match &err {
            Error::Retry {
                request_failures,
                max_request_failures,
                connect_failures,
                read_failures,
                status_code,
                ..
            } => {
                assert_eq!(*request_failures, 2);
                assert_eq!(*max_request_failures, 2);
                assert_eq!(
                    request_failures, max_request_failures,
                    "request budget must be fully exhausted"
                );
                assert_eq!(*connect_failures, 0, "5xx must not consume connect budget");
                assert_eq!(*read_failures, 0, "5xx must not consume read budget");
                assert_eq!(
                    status_code.map(|s| s.as_u16()),
                    Some(500),
                    "retryable status must be retained on Error::Retry"
                );
            }
            other => panic!("exhausted 5xx must surface as Error::Retry, got {other:?}"),
        }
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "retries=2 must make exactly two identical attempts"
        );
        assert!(seen_request_id.get().is_some());
        assert_remove_payload_free(&err);
    }

    /// Explicit name_conflict on a retryable status is terminal Error::Function.
    #[tokio::test]
    async fn remove_function_name_explicit_name_conflict_is_terminal_on_retryable_status() {
        let current = sample_remove_function();
        let expected_id = current.id().as_str().to_string();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();
        let body = json!({
            "error_code": "name_conflict",
            "message": format!(
                "{REMOVE_SERVER_MESSAGE_MARKER} looks_like revoked_function"
            ),
            "SENSITIVE_REMOVE_BODY_MARKER": true,
        })
        .to_string();

        let conn = Connection::new_with_handler_and_config(
            move |request| {
                assert_remove_request(&request, REMOVE_CATALOG_NAME, &expected_id);
                attempts_ref.fetch_add(1, Ordering::SeqCst);
                http::Response::builder()
                    .status(503)
                    .body(body.clone())
                    .unwrap()
            },
            ClientConfig {
                retry_config: RetryConfig {
                    retries: Some(3),
                    backoff_factor: Some(0.0),
                    backoff_jitter: Some(0.0),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let err = conn
            .remove_function_name(REMOVE_CATALOG_NAME, &current)
            .await
            .expect_err("explicit name_conflict must fail");
        match &err {
            Error::Function { code, message } => {
                assert_eq!(code.as_str(), "name_conflict");
                assert!(
                    matches!(code, FunctionErrorCode::NameConflict),
                    "expected NameConflict, got {code:?}"
                );
                assert_ne!(code.as_str(), "revoked_function");
                assert!(
                    !message.contains(REMOVE_SERVER_MESSAGE_MARKER),
                    "Function error message must be sanitized, got {message}"
                );
                assert!(
                    !message.contains(REMOVE_CATALOG_NAME),
                    "Function error message must not echo the catalog name, got {message}"
                );
                assert!(
                    !message.contains(REMOVE_FUNCTION_ID),
                    "Function error message must not echo the FunctionId, got {message}"
                );
            }
            other => panic!("expected Error::Function, got {other:?}"),
        }
        assert_remove_payload_free(&err);
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "explicit semantic code must not consume request retries"
        );
    }

    /// Unknown nonempty explicit code is preserved; status/message do not override.
    #[tokio::test]
    async fn remove_function_name_preserves_unknown_explicit_code_despite_status_and_message() {
        let current = sample_remove_function();
        let expected_id = current.id().as_str().to_string();
        let raw = "enterprise_future_remove_category_xyz";
        let body = json!({
            "error_code": raw,
            "message": format!(
                "{REMOVE_SERVER_MESSAGE_MARKER} name_conflict revoked_function"
            ),
            "SENSITIVE_REMOVE_BODY_MARKER": true,
        })
        .to_string();
        let conn = Connection::new_with_handler(move |request| {
            assert_remove_request(&request, REMOVE_CATALOG_NAME, &expected_id);
            http::Response::builder()
                .status(409)
                .body(body.clone())
                .unwrap()
        });

        let err = conn
            .remove_function_name(REMOVE_CATALOG_NAME, &current)
            .await
            .expect_err("unknown explicit code must surface");
        match &err {
            Error::Function { code, message } => {
                assert_eq!(code.as_str(), raw);
                assert!(
                    matches!(code, FunctionErrorCode::Unrecognized(_)),
                    "unknown code must stay Unrecognized, got {code:?}"
                );
                assert_ne!(code.as_str(), "name_conflict");
                assert_ne!(code.as_str(), "revoked_function");
                assert!(
                    !message.contains(REMOVE_SERVER_MESSAGE_MARKER),
                    "diagnostic message must be sanitized"
                );
            }
            other => panic!("expected Error::Function, got {other:?}"),
        }
        assert_remove_payload_free(&err);
    }

    /// Missing/empty/null/wrong-type/malformed error_code stays payload-free Http.
    #[tokio::test]
    async fn remove_function_name_missing_or_invalid_error_code_is_payload_free_http() {
        let cases: Vec<(&str, u16, String)> = vec![
            (
                "missing_code_404",
                404,
                json!({
                    "message": REMOVE_SERVER_MESSAGE_MARKER,
                    "SENSITIVE_REMOVE_BODY_MARKER": true,
                })
                .to_string(),
            ),
            (
                "empty_code",
                400,
                json!({
                    "error_code": "",
                    "message": REMOVE_SERVER_MESSAGE_MARKER,
                    "SENSITIVE_REMOVE_BODY_MARKER": true,
                })
                .to_string(),
            ),
            (
                "wrong_type_code",
                400,
                json!({
                    "error_code": 123,
                    "message": REMOVE_SERVER_MESSAGE_MARKER,
                    "SENSITIVE_REMOVE_BODY_MARKER": true,
                })
                .to_string(),
            ),
            (
                // Non-retryable status: invalid/null error_code must stay immediate Http.
                // Exhausted retryable 5xx is covered by
                // remove_function_name_exhausted_retryable_5xx_returns_retry_with_request_counters.
                "null_code",
                400,
                json!({
                    "error_code": null,
                    "message": REMOVE_SERVER_MESSAGE_MARKER,
                    "SENSITIVE_REMOVE_BODY_MARKER": true,
                })
                .to_string(),
            ),
            (
                // Non-retryable status: this case proves invalid-code -> Http only.
                // Exhausted retryable 5xx without error_code is covered separately
                // by remove_function_name_exhausted_retryable_5xx_returns_retry_with_request_counters.
                "non_json",
                400,
                format!("not-json {REMOVE_SERVER_MESSAGE_MARKER} SENSITIVE_REMOVE_BODY_MARKER"),
            ),
        ];

        let mut unexpected = Vec::new();
        for (label, status, response_body) in cases {
            let current = sample_remove_function();
            let expected_id = current.id().as_str().to_string();
            let body_for_handler = response_body.clone();
            let conn = Connection::new_with_handler(move |request| {
                assert_remove_request(&request, REMOVE_CATALOG_NAME, &expected_id);
                http::Response::builder()
                    .status(status)
                    .body(body_for_handler.clone())
                    .unwrap()
            });
            match conn
                .remove_function_name(REMOVE_CATALOG_NAME, &current)
                .await
            {
                Err(err @ Error::Http { .. }) => assert_remove_payload_free(&err),
                Err(Error::Function { .. }) => unexpected.push(format!(
                    "{label}: must not invent Error::Function without explicit code"
                )),
                other => unexpected.push(format!("{label}: {other:?}")),
            }
        }
        assert!(
            unexpected.is_empty(),
            "invalid/missing error_code must stay payload-free Http: {unexpected:?}"
        );
    }

    /// 200/202 are payload-free protocol Http failures, never CAS success.
    #[tokio::test]
    async fn remove_function_name_other_2xx_are_payload_free_http_failures() {
        let cases: Vec<(&str, u16, String)> = vec![
            (
                "200_with_body",
                200,
                json!({
                    "ok": true,
                    "message": REMOVE_SERVER_MESSAGE_MARKER,
                    "SENSITIVE_REMOVE_BODY_MARKER": true,
                    "job_id": "must-not-infer-job",
                })
                .to_string(),
            ),
            (
                "202_empty",
                202,
                format!("{REMOVE_SERVER_MESSAGE_MARKER} SENSITIVE_REMOVE_BODY_MARKER"),
            ),
            ("200_empty", 200, String::new()),
        ];

        let mut unexpected = Vec::new();
        for (label, status, response_body) in cases {
            let current = sample_remove_function();
            let expected_id = current.id().as_str().to_string();
            let body_for_handler = response_body.clone();
            let conn = Connection::new_with_handler(move |request| {
                assert_remove_request(&request, REMOVE_CATALOG_NAME, &expected_id);
                http::Response::builder()
                    .status(status)
                    .body(body_for_handler.clone())
                    .unwrap()
            });
            match conn
                .remove_function_name(REMOVE_CATALOG_NAME, &current)
                .await
            {
                Ok(()) => {
                    unexpected.push(format!("{label}: must not treat non-204 2xx as success"))
                }
                Err(err @ Error::Http { .. }) => assert_remove_payload_free(&err),
                Err(Error::Function { .. }) => unexpected.push(format!(
                    "{label}: must not invent Error::Function from 2xx body"
                )),
                other => unexpected.push(format!("{label}: {other:?}")),
            }
        }
        assert!(
            unexpected.is_empty(),
            "200/202 must be payload-free Http failures: {unexpected:?}"
        );
    }

    /// Non-204 success must fail from status alone without reading the body.
    ///
    /// A protocol-invalid HTTP 200 whose body stream fails if read must return
    /// payload-free Error::Http with status 200 on exactly one attempt, even when
    /// read/request retry budgets are configured above one. Body must not be
    /// read and no retry budget may be consumed.
    #[tokio::test]
    async fn remove_function_name_non_204_success_does_not_read_failing_body() {
        let current = sample_remove_function();
        let expected_id = current.id().as_str().to_string();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();

        let conn = Connection::new_with_handler_and_config(
            move |request| {
                assert_remove_request(&request, REMOVE_CATALOG_NAME, &expected_id);
                attempts_ref.fetch_add(1, Ordering::SeqCst);
                let stream = futures::stream::once(async {
                    Err::<bytes::Bytes, _>(std::io::Error::other(
                        "simulated remove response body read failure SENSITIVE_REMOVE_BODY_MARKER",
                    ))
                });
                http::Response::builder()
                    .status(200)
                    .body(reqwest::Body::wrap_stream(stream))
                    .unwrap()
            },
            ClientConfig {
                // Budgets above one must not be consumed: status 200 is terminal
                // protocol Http before any body read/retry classification.
                retry_config: RetryConfig {
                    retries: Some(3),
                    read_retries: Some(3),
                    connect_retries: Some(3),
                    backoff_factor: Some(0.0),
                    backoff_jitter: Some(0.0),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let err = conn
            .remove_function_name(REMOVE_CATALOG_NAME, &current)
            .await
            .expect_err("non-204 success must be protocol Http");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "invalid 2xx must not read the body or consume retry budget"
        );
        match &err {
            Error::Http { status_code, .. } => {
                assert_eq!(
                    status_code.map(|s| s.as_u16()),
                    Some(200),
                    "protocol Http must retain status 200 from the response alone"
                );
            }
            other => panic!("expected payload-free Error::Http, got {other:?}"),
        }
        assert_remove_payload_free(&err);
    }

    /// Empty name is InvalidInput before backend through Connection.
    #[tokio::test]
    async fn remove_function_name_empty_name_fails_before_transport() {
        let current = sample_remove_function();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();
        let conn = Connection::new_with_handler(move |_request| -> http::Response<String> {
            attempts_ref.fetch_add(1, Ordering::SeqCst);
            panic!("empty name must not issue an HTTP request");
        });

        let err = conn
            .remove_function_name("", &current)
            .await
            .expect_err("empty name must fail before transport");
        assert!(
            matches!(err, Error::InvalidInput { .. }),
            "empty name must be InvalidInput, got {err:?}"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            0,
            "empty name must not touch transport"
        );
        assert_remove_payload_free(&err);
    }

    /// Database trait seam must reject empty names as InvalidInput (not NotSupported).
    #[tokio::test]
    async fn remove_function_name_database_trait_empty_name_is_invalid_input_without_mutation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let conn = ConnectBuilder::new(dir.path().to_str().unwrap())
            .execute()
            .await
            .expect("local connect");
        let before = conn
            .table_names()
            .execute()
            .await
            .expect("table_names before");
        assert!(before.is_empty());

        let current = sample_remove_function();
        let err = conn
            .database()
            .remove_function_name("", &current)
            .await
            .expect_err("empty name must fail on Database trait default");
        assert!(
            matches!(err, Error::InvalidInput { .. }),
            "empty name via Database trait must be InvalidInput, got {err:?}"
        );

        let after = conn
            .table_names()
            .execute()
            .await
            .expect("table_names after");
        assert_eq!(
            before, after,
            "Database-trait empty-name rejection must not mutate tables"
        );
    }

    /// Valid local removal is NotSupported and does not mutate tables.
    #[tokio::test]
    async fn remove_function_name_local_database_returns_not_supported_without_mutation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let conn = ConnectBuilder::new(dir.path().to_str().unwrap())
            .execute()
            .await
            .expect("local connect");
        let before = conn
            .table_names()
            .execute()
            .await
            .expect("table_names before");
        assert!(before.is_empty());

        let current = sample_remove_function();
        let err = conn
            .remove_function_name(REMOVE_CATALOG_NAME, &current)
            .await
            .expect_err("local remove_function_name must be unsupported");
        assert!(
            matches!(err, Error::NotSupported { .. }),
            "expected NotSupported for local remove, got {err:?}"
        );

        let after = conn
            .table_names()
            .execute()
            .await
            .expect("table_names after");
        assert_eq!(before, after, "unsupported remove must not mutate tables");
    }

    /// Empty name on local Connection is InvalidInput before NotSupported.
    #[tokio::test]
    async fn remove_function_name_local_empty_name_is_invalid_input_without_mutation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let conn = ConnectBuilder::new(dir.path().to_str().unwrap())
            .execute()
            .await
            .expect("local connect");
        let before = conn
            .table_names()
            .execute()
            .await
            .expect("table_names before");
        assert!(before.is_empty());

        let current = sample_remove_function();
        let err = conn
            .remove_function_name("", &current)
            .await
            .expect_err("empty name must fail before backend dispatch");
        assert!(
            matches!(err, Error::InvalidInput { .. }),
            "empty name must be InvalidInput on local Connection, got {err:?}"
        );

        let after = conn
            .table_names()
            .execute()
            .await
            .expect("table_names after");
        assert_eq!(before, after, "empty-name rejection must not mutate tables");
    }

    // -------------------------------------------------------------------------
    // Exact Function revocation
    //
    // Direct administrator catalog set-bit via POST /v1/functions/revoke.
    // Targets an exact Function ID. Not name removal, physical deletion,
    // Function mutation, Job, or generated-column mutation.
    // -------------------------------------------------------------------------

    const REVOKE_FUNCTION_ID: &str = "fn.exact.revoke-handle";
    const REVOKE_SERVER_MESSAGE_MARKER: &str =
        "SERVER_REVOKE_DIAGNOSTIC_MARKER id=fn.exact.revoke-handle name=text.normalize.revoke-name";
    const REVOKE_CATALOG_NAME_MARKER: &str = "text.normalize.revoke-name";

    fn sample_revoke_function() -> Function {
        let id = FunctionId::try_new(REVOKE_FUNCTION_ID).expect("valid FunctionId");
        let signature = FunctionSignature::try_new(
            vec![
                FunctionParameter::new("text", DataType::Utf8),
                FunctionParameter::new("limit", DataType::Int32),
            ],
            FunctionOutput::new(DataType::Utf8, true),
        )
        .expect("valid FunctionSignature");
        Function::new(id, signature)
    }

    fn revoke_error_chain_text(err: &Error) -> String {
        let mut text = format!("{err}\n{err:?}");
        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
        while let Some(e) = current {
            text.push('\n');
            text.push_str(&e.to_string());
            text.push('\n');
            text.push_str(&format!("{e:?}"));
            current = e.source();
        }
        text
    }

    fn assert_revoke_payload_free(err: &Error) {
        let text = revoke_error_chain_text(err);
        assert!(
            !text.contains(REVOKE_SERVER_MESSAGE_MARKER),
            "server diagnostic marker must be absent from error/debug/source chain: {text}"
        );
        assert!(
            !text.contains(REVOKE_CATALOG_NAME_MARKER),
            "catalog name marker must be absent from error/debug/source chain: {text}"
        );
        assert!(
            !text.contains(REVOKE_FUNCTION_ID),
            "FunctionId must be absent from error/debug/source chain: {text}"
        );
        assert!(
            !text.contains("SENSITIVE_REVOKE_BODY_MARKER"),
            "non-success/malformed body marker must be absent from error/debug/source chain: {text}"
        );
    }

    fn assert_revoke_request(request: &reqwest::Request, expected_function_id: &str) {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/functions/revoke");
        assert!(
            request.url().query().is_none(),
            "revoke selectors must stay out of the URL query: {}",
            request.url()
        );
        let request_id = request.headers()["x-request-id"]
            .to_str()
            .expect("x-request-id must be present");
        assert!(
            !request_id.is_empty(),
            "SDK must generate a nonempty request id"
        );
        let body = request
            .body()
            .and_then(|b| b.as_bytes())
            .expect("revoke request must carry a JSON body");
        let actual: Value = serde_json::from_slice(body).expect("revoke body must be JSON");
        assert_eq!(
            actual,
            json!({
                "function_id": expected_function_id,
            }),
            "revoke body must be exactly {{\"function_id\":...}}"
        );
        let object = actual.as_object().expect("revoke body must be an object");
        assert_eq!(
            object.len(),
            1,
            "revoke body must not carry extra fields: {actual}"
        );
        assert!(
            object.get("name").is_none(),
            "revoke must not send a catalog name: {actual}"
        );
        assert!(
            object.get("expected_current_function_id").is_none(),
            "revoke is not CAS remove and must not send expected_current_function_id: {actual}"
        );
        assert!(
            object.get("current").is_none() && object.get("function").is_none(),
            "revoke must not send a Function record: {actual}"
        );
        assert!(
            object.get("signature").is_none() && object.get("format_version").is_none(),
            "revoke must not send signature or format_version: {actual}"
        );
        assert!(
            object.get("job_id").is_none() && object.get("idempotency_key").is_none(),
            "revoke is not a Job and must not send user idempotency keys: {actual}"
        );
        assert!(
            object.get("user_version").is_none()
                && object.get("reason").is_none()
                && object.get("expiry").is_none()
                && object.get("force").is_none(),
            "revoke must not send reason/expiry/force/user-version fields: {actual}"
        );
        assert!(
            !request.url().path().contains("remove"),
            "revoke must not use the remove path: {}",
            request.url().path()
        );
    }

    /// Exact path/body/request id and 204 success ignore an illegal body.
    #[tokio::test]
    async fn revoke_function_posts_exact_body_and_succeeds_on_204() {
        let function = sample_revoke_function();
        let before = function.clone();
        let expected_id = function.id().as_str().to_string();
        let conn = Connection::new_with_handler(move |request| {
            assert_revoke_request(&request, &expected_id);
            // Illegal body on 204 must be ignored; success is status-driven only.
            http::Response::builder()
                .status(204)
                .body(format!(
                    "{{\"SENSITIVE_REVOKE_BODY_MARKER\":true,\"message\":{REVOKE_SERVER_MESSAGE_MARKER:?},\"name\":{REVOKE_CATALOG_NAME_MARKER:?}}}"
                ))
                .unwrap()
        });

        conn.revoke_function(&function)
            .await
            .expect("HTTP 204 must complete revocation");
        assert_exact_function(&function, &before);
    }

    /// Repeated logical revoke calls that each receive 204 both succeed.
    ///
    /// Idempotent public outcome only: each logical call may generate its own
    /// internal request id; tests must not assert cross-call id equality.
    #[tokio::test]
    async fn revoke_function_repeated_204_is_idempotent() {
        let function = sample_revoke_function();
        let before = function.clone();
        let expected_id = function.id().as_str().to_string();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();

        let conn = Connection::new_with_handler(move |request| {
            assert_revoke_request(&request, &expected_id);
            attempts_ref.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(204)
                .body(String::new())
                .unwrap()
        });

        conn.revoke_function(&function)
            .await
            .expect("first revoke must succeed on 204");
        conn.revoke_function(&function)
            .await
            .expect("second revoke of an already-revoked Function must also succeed on 204");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "two logical revoke calls must issue two exact requests"
        );
        assert_exact_function(&function, &before);
    }

    /// One configured 5xx retry then 204 keeps identical request id/body.
    #[tokio::test]
    async fn revoke_function_retry_preserves_request_id_and_body() {
        let function = sample_revoke_function();
        let before = function.clone();
        let expected_id = function.id().as_str().to_string();
        let seen_request_id = Arc::new(OnceLock::new());
        let seen_request_id_ref = seen_request_id.clone();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();

        let conn = Connection::new_with_handler_and_config(
            move |request| {
                assert_revoke_request(&request, &expected_id);
                let request_id = request.headers()["x-request-id"]
                    .to_str()
                    .unwrap()
                    .to_string();
                let seen = seen_request_id_ref.get_or_init(|| request_id.clone());
                assert_eq!(
                    &request_id, seen,
                    "request id must be identical across retries within one logical call"
                );

                let n = attempts_ref.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    http::Response::builder()
                        .status(500)
                        .body(format!(
                            "{REVOKE_SERVER_MESSAGE_MARKER} SENSITIVE_REVOKE_BODY_MARKER"
                        ))
                        .unwrap()
                } else {
                    http::Response::builder()
                        .status(204)
                        .body(String::new())
                        .unwrap()
                }
            },
            ClientConfig {
                retry_config: RetryConfig {
                    retries: Some(2),
                    backoff_factor: Some(0.0),
                    backoff_jitter: Some(0.0),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        conn.revoke_function(&function)
            .await
            .expect("revoke must succeed after one retry");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "one 5xx then 204 must be exactly two attempts"
        );
        assert!(seen_request_id.get().is_some());
        assert_exact_function(&function, &before);
    }

    /// Exhausted always-retryable 5xx without explicit error_code surfaces Error::Retry.
    ///
    /// retries=2 is max request failures: exactly two identical attempts, then
    /// Retry with request_failures == max_request_failures == 2, zero connect/read
    /// failures, the retryable status retained, and a payload-free source chain.
    #[tokio::test]
    async fn revoke_function_exhausted_retryable_5xx_returns_retry_with_request_counters() {
        let function = sample_revoke_function();
        let expected_id = function.id().as_str().to_string();
        let seen_request_id = Arc::new(OnceLock::new());
        let seen_request_id_ref = seen_request_id.clone();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();
        let body = format!("{REVOKE_SERVER_MESSAGE_MARKER} SENSITIVE_REVOKE_BODY_MARKER");

        let conn = Connection::new_with_handler_and_config(
            move |request| {
                assert_revoke_request(&request, &expected_id);
                let request_id = request.headers()["x-request-id"]
                    .to_str()
                    .unwrap()
                    .to_string();
                let seen = seen_request_id_ref.get_or_init(|| request_id.clone());
                assert_eq!(
                    &request_id, seen,
                    "request id must be identical across exhausted retries"
                );

                attempts_ref.fetch_add(1, Ordering::SeqCst);
                http::Response::builder()
                    .status(500)
                    .body(body.clone())
                    .unwrap()
            },
            ClientConfig {
                retry_config: RetryConfig {
                    // RetryCounter treats `retries` as max request failures, so
                    // retries=2 yields exactly two transport attempts before Error::Retry.
                    retries: Some(2),
                    backoff_factor: Some(0.0),
                    backoff_jitter: Some(0.0),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let err = conn
            .revoke_function(&function)
            .await
            .expect_err("exhausted retryable 5xx must fail");
        match &err {
            Error::Retry {
                request_failures,
                max_request_failures,
                connect_failures,
                read_failures,
                status_code,
                ..
            } => {
                assert_eq!(*request_failures, 2);
                assert_eq!(*max_request_failures, 2);
                assert_eq!(
                    request_failures, max_request_failures,
                    "request budget must be fully exhausted"
                );
                assert_eq!(*connect_failures, 0, "5xx must not consume connect budget");
                assert_eq!(*read_failures, 0, "5xx must not consume read budget");
                assert_eq!(
                    status_code.map(|s| s.as_u16()),
                    Some(500),
                    "retryable status must be retained on Error::Retry"
                );
            }
            other => panic!("exhausted 5xx must surface as Error::Retry, got {other:?}"),
        }
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "retries=2 must make exactly two identical attempts"
        );
        assert!(seen_request_id.get().is_some());
        assert_revoke_payload_free(&err);
    }

    /// Explicit name_or_function_not_found on a retryable status is terminal.
    #[tokio::test]
    async fn revoke_function_explicit_name_or_function_not_found_is_terminal_on_retryable_status() {
        let function = sample_revoke_function();
        let expected_id = function.id().as_str().to_string();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();
        let body = json!({
            "error_code": "name_or_function_not_found",
            "message": format!(
                "{REVOKE_SERVER_MESSAGE_MARKER} looks_like revoked_function name_conflict"
            ),
            "SENSITIVE_REVOKE_BODY_MARKER": true,
        })
        .to_string();

        let conn = Connection::new_with_handler_and_config(
            move |request| {
                assert_revoke_request(&request, &expected_id);
                attempts_ref.fetch_add(1, Ordering::SeqCst);
                http::Response::builder()
                    .status(503)
                    .body(body.clone())
                    .unwrap()
            },
            ClientConfig {
                retry_config: RetryConfig {
                    retries: Some(3),
                    backoff_factor: Some(0.0),
                    backoff_jitter: Some(0.0),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let err = conn
            .revoke_function(&function)
            .await
            .expect_err("explicit name_or_function_not_found must fail");
        match &err {
            Error::Function { code, message } => {
                assert_eq!(code.as_str(), "name_or_function_not_found");
                assert!(
                    matches!(code, FunctionErrorCode::NameOrFunctionNotFound),
                    "expected NameOrFunctionNotFound, got {code:?}"
                );
                assert_ne!(code.as_str(), "revoked_function");
                assert_ne!(code.as_str(), "name_conflict");
                assert!(
                    !message.contains(REVOKE_SERVER_MESSAGE_MARKER),
                    "Function error message must be sanitized, got {message}"
                );
                assert!(
                    !message.contains(REVOKE_FUNCTION_ID),
                    "Function error message must not echo the FunctionId, got {message}"
                );
                assert!(
                    !message.contains(REVOKE_CATALOG_NAME_MARKER),
                    "Function error message must not echo a catalog name, got {message}"
                );
            }
            other => panic!("expected Error::Function, got {other:?}"),
        }
        assert_revoke_payload_free(&err);
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "explicit semantic code must not consume request retries"
        );
    }

    /// Unknown nonempty explicit code is preserved; status/message do not override.
    #[tokio::test]
    async fn revoke_function_preserves_unknown_explicit_code_despite_status_and_message() {
        let function = sample_revoke_function();
        let expected_id = function.id().as_str().to_string();
        let raw = "enterprise_future_revoke_category_xyz";
        let body = json!({
            "error_code": raw,
            "message": format!(
                "{REVOKE_SERVER_MESSAGE_MARKER} name_or_function_not_found revoked_function"
            ),
            "SENSITIVE_REVOKE_BODY_MARKER": true,
        })
        .to_string();
        let conn = Connection::new_with_handler(move |request| {
            assert_revoke_request(&request, &expected_id);
            http::Response::builder()
                .status(409)
                .body(body.clone())
                .unwrap()
        });

        let err = conn
            .revoke_function(&function)
            .await
            .expect_err("unknown explicit code must surface");
        match &err {
            Error::Function { code, message } => {
                assert_eq!(code.as_str(), raw);
                assert!(
                    matches!(code, FunctionErrorCode::Unrecognized(_)),
                    "unknown code must stay Unrecognized, got {code:?}"
                );
                assert_ne!(code.as_str(), "name_or_function_not_found");
                assert_ne!(code.as_str(), "revoked_function");
                assert!(
                    !message.contains(REVOKE_SERVER_MESSAGE_MARKER),
                    "diagnostic message must be sanitized"
                );
            }
            other => panic!("expected Error::Function, got {other:?}"),
        }
        assert_revoke_payload_free(&err);
    }

    /// Missing/empty/null/wrong-type/malformed error_code stays payload-free Http.
    #[tokio::test]
    async fn revoke_function_missing_or_invalid_error_code_is_payload_free_http() {
        let cases: Vec<(&str, u16, String)> = vec![
            (
                "missing_code_404",
                404,
                json!({
                    "message": REVOKE_SERVER_MESSAGE_MARKER,
                    "SENSITIVE_REVOKE_BODY_MARKER": true,
                })
                .to_string(),
            ),
            (
                "empty_code",
                400,
                json!({
                    "error_code": "",
                    "message": REVOKE_SERVER_MESSAGE_MARKER,
                    "SENSITIVE_REVOKE_BODY_MARKER": true,
                })
                .to_string(),
            ),
            (
                "wrong_type_code",
                400,
                json!({
                    "error_code": 123,
                    "message": REVOKE_SERVER_MESSAGE_MARKER,
                    "SENSITIVE_REVOKE_BODY_MARKER": true,
                })
                .to_string(),
            ),
            (
                // Non-retryable status: invalid/null error_code must stay immediate Http.
                // Exhausted retryable 5xx is covered by
                // revoke_function_exhausted_retryable_5xx_returns_retry_with_request_counters.
                "null_code",
                400,
                json!({
                    "error_code": null,
                    "message": REVOKE_SERVER_MESSAGE_MARKER,
                    "SENSITIVE_REVOKE_BODY_MARKER": true,
                })
                .to_string(),
            ),
            (
                // Non-retryable status: this case proves invalid-code -> Http only.
                // Exhausted retryable 5xx without error_code is covered separately
                // by revoke_function_exhausted_retryable_5xx_returns_retry_with_request_counters.
                "non_json",
                400,
                format!("not-json {REVOKE_SERVER_MESSAGE_MARKER} SENSITIVE_REVOKE_BODY_MARKER"),
            ),
        ];

        let mut unexpected = Vec::new();
        for (label, status, response_body) in cases {
            let function = sample_revoke_function();
            let expected_id = function.id().as_str().to_string();
            let body_for_handler = response_body.clone();
            let conn = Connection::new_with_handler(move |request| {
                assert_revoke_request(&request, &expected_id);
                http::Response::builder()
                    .status(status)
                    .body(body_for_handler.clone())
                    .unwrap()
            });
            match conn.revoke_function(&function).await {
                Err(err @ Error::Http { .. }) => assert_revoke_payload_free(&err),
                Err(Error::Function { .. }) => unexpected.push(format!(
                    "{label}: must not invent Error::Function without explicit code"
                )),
                other => unexpected.push(format!("{label}: {other:?}")),
            }
        }
        assert!(
            unexpected.is_empty(),
            "invalid/missing error_code must stay payload-free Http: {unexpected:?}"
        );
    }

    /// 200/202 are payload-free protocol Http failures, never revoke success.
    #[tokio::test]
    async fn revoke_function_other_2xx_are_payload_free_http_failures() {
        let cases: Vec<(&str, u16, String)> = vec![
            (
                "200_with_body",
                200,
                json!({
                    "ok": true,
                    "message": REVOKE_SERVER_MESSAGE_MARKER,
                    "SENSITIVE_REVOKE_BODY_MARKER": true,
                    "job_id": "must-not-infer-job",
                    "name": REVOKE_CATALOG_NAME_MARKER,
                })
                .to_string(),
            ),
            (
                "202_empty",
                202,
                format!("{REVOKE_SERVER_MESSAGE_MARKER} SENSITIVE_REVOKE_BODY_MARKER"),
            ),
            ("200_empty", 200, String::new()),
        ];

        let mut unexpected = Vec::new();
        for (label, status, response_body) in cases {
            let function = sample_revoke_function();
            let expected_id = function.id().as_str().to_string();
            let body_for_handler = response_body.clone();
            let conn = Connection::new_with_handler(move |request| {
                assert_revoke_request(&request, &expected_id);
                http::Response::builder()
                    .status(status)
                    .body(body_for_handler.clone())
                    .unwrap()
            });
            match conn.revoke_function(&function).await {
                Ok(()) => {
                    unexpected.push(format!("{label}: must not treat non-204 2xx as success"))
                }
                Err(err @ Error::Http { .. }) => assert_revoke_payload_free(&err),
                Err(Error::Function { .. }) => unexpected.push(format!(
                    "{label}: must not invent Error::Function from 2xx body"
                )),
                other => unexpected.push(format!("{label}: {other:?}")),
            }
        }
        assert!(
            unexpected.is_empty(),
            "200/202 must be payload-free Http failures: {unexpected:?}"
        );
    }

    /// Non-204 success must fail from status alone without reading the body.
    ///
    /// A protocol-invalid HTTP 200 whose body stream fails if read must return
    /// payload-free Error::Http with status 200 on exactly one attempt, even when
    /// read/request retry budgets are configured above one. Body must not be
    /// read and no retry budget may be consumed.
    #[tokio::test]
    async fn revoke_function_non_204_success_does_not_read_failing_body() {
        let function = sample_revoke_function();
        let expected_id = function.id().as_str().to_string();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_ref = attempts.clone();

        let conn = Connection::new_with_handler_and_config(
            move |request| {
                assert_revoke_request(&request, &expected_id);
                attempts_ref.fetch_add(1, Ordering::SeqCst);
                let stream = futures::stream::once(async {
                    Err::<bytes::Bytes, _>(std::io::Error::other(
                        "simulated revoke response body read failure SENSITIVE_REVOKE_BODY_MARKER",
                    ))
                });
                http::Response::builder()
                    .status(200)
                    .body(reqwest::Body::wrap_stream(stream))
                    .unwrap()
            },
            ClientConfig {
                // Budgets above one must not be consumed: status 200 is terminal
                // protocol Http before any body read/retry classification.
                retry_config: RetryConfig {
                    retries: Some(3),
                    read_retries: Some(3),
                    connect_retries: Some(3),
                    backoff_factor: Some(0.0),
                    backoff_jitter: Some(0.0),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let err = conn
            .revoke_function(&function)
            .await
            .expect_err("non-204 success must be protocol Http");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "invalid 2xx must not read the body or consume retry budget"
        );
        match &err {
            Error::Http { status_code, .. } => {
                assert_eq!(
                    status_code.map(|s| s.as_u16()),
                    Some(200),
                    "protocol Http must retain status 200 from the response alone"
                );
            }
            other => panic!("expected payload-free Error::Http, got {other:?}"),
        }
        assert_revoke_payload_free(&err);
    }

    /// Valid local Connection revocation is NotSupported and does not mutate tables.
    #[tokio::test]
    async fn revoke_function_local_connection_returns_not_supported_without_mutation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let conn = ConnectBuilder::new(dir.path().to_str().unwrap())
            .execute()
            .await
            .expect("local connect");
        let before = conn
            .table_names()
            .execute()
            .await
            .expect("table_names before");
        assert!(before.is_empty());

        let function = sample_revoke_function();
        let handle_before = function.clone();
        let err = conn
            .revoke_function(&function)
            .await
            .expect_err("local revoke_function must be unsupported");
        assert!(
            matches!(err, Error::NotSupported { .. }),
            "expected NotSupported for local revoke, got {err:?}"
        );
        assert_exact_function(&function, &handle_before);

        let after = conn
            .table_names()
            .execute()
            .await
            .expect("table_names after");
        assert_eq!(before, after, "unsupported revoke must not mutate tables");
    }

    /// Database trait seam must return NotSupported without table mutation.
    #[tokio::test]
    async fn revoke_function_database_trait_local_returns_not_supported_without_mutation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let conn = ConnectBuilder::new(dir.path().to_str().unwrap())
            .execute()
            .await
            .expect("local connect");
        let before = conn
            .table_names()
            .execute()
            .await
            .expect("table_names before");
        assert!(before.is_empty());

        let function = sample_revoke_function();
        let handle_before = function.clone();
        let err = conn
            .database()
            .revoke_function(&function)
            .await
            .expect_err("local Database::revoke_function must be unsupported");
        assert!(
            matches!(err, Error::NotSupported { .. }),
            "expected NotSupported for Database trait revoke, got {err:?}"
        );
        assert_exact_function(&function, &handle_before);

        let after = conn
            .table_names()
            .execute()
            .await
            .expect("table_names after");
        assert_eq!(
            before, after,
            "Database-trait unsupported revoke must not mutate tables"
        );
    }
}
