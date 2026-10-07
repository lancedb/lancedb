// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use lance_io::object_store::StorageOptions;
use lance_namespace_impls::{DynamicContextProvider, OperationInfo};
use moka::future::Cache;
use reqwest::Response;
use reqwest::header::CONTENT_TYPE;

use lance_namespace::models::{
    CreateNamespaceRequest, CreateNamespaceResponse, DescribeNamespaceRequest,
    DescribeNamespaceResponse, DropNamespaceRequest, DropNamespaceResponse, JsonArrowSchema,
    ListNamespacesRequest, ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
};

use crate::Error;
use crate::database::{
    CloneTableRequest, CreateTableMode, CreateTableRequest, Database, DatabaseOptions, JobInfo,
    OpenTableRequest, ReadConsistency, TableNamesRequest,
};
use crate::error::Result;
use crate::function::{
    FunctionArtifactRequest, FunctionRegistrationRequest, FunctionSignature, FunctionVersion,
    PythonRuntimeSpec,
};
use crate::job::Job;
use crate::materialized_view::CreateMaterializedViewRequest;
use crate::remote::job::{PauseJobResponse, RemoteJob, ResumeJobResponse, job_state_to_client};
use crate::remote::util::stream_as_body;
use crate::secrets::SecretBinding;
use crate::secrets::SecretInfo;
use crate::table::BaseTable;
use crate::utils::{reject_relative_segment, validate_table_name};
use crate::view::ViewDescription;

use super::client::{
    ClientConfig, HeaderProvider, HttpSend, ID_DELIMITER, RequestResultExt, RestfulLanceDbClient,
    Sender,
};
use super::sql::SqlClient;
use super::table::RemoteTable;
use super::util::parse_server_version;
use super::{ARROW_STREAM_CONTENT_TYPE, extract_job_id};

fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
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

    pub fn support_blobs(&self) -> bool {
        self.0 >= semver::Version::new(0, 5, 0)
    }

    pub fn support_fts_document_granularity(&self) -> bool {
        self.0 >= semver::Version::new(0, 6, 0)
    }
}

pub const OPT_REMOTE_PREFIX: &str = "remote_database_";
pub const OPT_REMOTE_API_KEY: &str = "remote_database_api_key";
pub const OPT_REMOTE_REGION: &str = "remote_database_region";
pub const OPT_REMOTE_HOST_OVERRIDE: &str = "remote_database_host_override";
pub const OPT_REMOTE_SQL_HOST_OVERRIDE: &str = "remote_database_sql_host_override";
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
    // Cache existence and server capabilities, not mutable per-handle table state.
    table_cache: Cache<String, ServerVersion>,
    uri: String,
    /// Headers to pass to the namespace client for authentication
    namespace_headers: HashMap<String, String>,
    namespace_context_provider: Option<Arc<dyn DynamicContextProvider>>,
    /// TLS configuration for mTLS support
    tls_config: Option<super::client::TlsConfig>,
    sql_client: Option<SqlClient>,
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

pub struct RemoteHostOverrides {
    pub rest: Option<String>,
    pub sql: Option<String>,
}

impl RemoteDatabase {
    pub(crate) fn try_new(
        uri: &str,
        api_key: &str,
        region: &str,
        host_overrides: RemoteHostOverrides,
        client_config: ClientConfig,
        options: RemoteOptions,
        read_consistency_interval: Option<std::time::Duration>,
    ) -> Result<Self> {
        let parsed = super::client::parse_db_url(uri)?;
        Self::try_new_with_identity(
            uri,
            api_key,
            region,
            host_overrides,
            client_config,
            options,
            read_consistency_interval,
            parsed,
        )
    }

    pub(crate) fn for_catalog(
        endpoint: &str,
        name: Option<&str>,
        options: &super::catalog::RemoteCatalogOptions,
    ) -> Result<Self> {
        let scope = super::catalog::ScopedHeaderProvider {
            provider: options.client_config.header_provider.clone(),
            database: name.map(str::to_string),
        };
        let mut config = options.client_config.clone();
        scope.apply(&mut config.extra_headers);
        config.header_provider = Some(Arc::new(scope));
        let uri = name
            .map(|name| format!("db://{}", urlencoding::encode(name)))
            .unwrap_or_else(|| endpoint.to_string());
        let mut db = Self::try_new_with_identity(
            &uri,
            options.api_key.as_deref().unwrap_or(""),
            "us-east-1",
            RemoteHostOverrides {
                rest: Some(endpoint.to_string()),
                sql: options.sql_host_override.clone(),
            },
            config,
            RemoteOptions::default(),
            options.read_consistency_interval,
            super::client::ParsedDbUrl {
                db_name: name.unwrap_or("").to_string(),
                db_prefix: None,
            },
        )?;
        if name.is_none() {
            db.sql_client = None;
        }
        Ok(db)
    }

    #[allow(clippy::too_many_arguments)]
    fn try_new_with_identity(
        uri: &str,
        api_key: &str,
        region: &str,
        host_overrides: RemoteHostOverrides,
        client_config: ClientConfig,
        options: RemoteOptions,
        read_consistency_interval: Option<std::time::Duration>,
        parsed: super::client::ParsedDbUrl,
    ) -> Result<Self> {
        let sql_client = SqlClient::new(
            parsed.db_name.clone(),
            parsed.db_prefix.clone(),
            api_key.to_string(),
            host_overrides.rest.clone(),
            host_overrides.sql,
            client_config.clone(),
        );
        let header_map = RestfulLanceDbClient::<Sender>::default_headers(
            api_key,
            region,
            &parsed.db_name,
            host_overrides.rest.is_some() && !parsed.db_name.is_empty(),
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
            host_overrides.rest,
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
            sql_client: Some(sql_client),
        })
    }
}

impl<S: HttpSend> RemoteDatabase<S> {
    /// Post a request whose body carries a credential.
    ///
    /// Shared by the create and alter verbs, which declare their own request
    /// types: the two mean different things to the service and are free to
    /// diverge, so what they share is the posting and not the shape.
    ///
    /// The value is a request field and never a path segment or query
    /// parameter, which keeps it out of access logs and proxy traces.
    async fn post_secret_write<T: serde::Serialize>(&self, route: &str, body: &T) -> Result<()> {
        let req = self.client.post(route).json(body);
        // This call is what says the body is a credential. Nothing downstream
        // can tell from the bytes, and a route list in the transport would have
        // to be kept in step with endpoints declared here.
        let (request_id, response) = self.client.send_suppressing_body(req).await?;
        self.client.check_response(&request_id, response).await?;
        Ok(())
    }

    async fn submit_drop_table(
        &self,
        name: &str,
        namespace_path: &[String],
    ) -> Result<(String, Response)> {
        let identifier = build_table_identifier(name, namespace_path)?;
        let cache_key = build_cache_key(name, namespace_path);
        let req = self.client.post(&format!("/v1/table/{}/drop/", identifier));
        let (request_id, resp) = self.client.send(req).await?;
        let resp = self.client.check_response(&request_id, resp).await?;
        self.table_cache.remove(&cache_key).await;
        Ok((request_id, resp))
    }

    /// Collect the tables of a namespace in name order, for `table_names`.
    ///
    /// `table_names` promises name order and resumes after a table name, but the namespace
    /// route's `page_token` is opaque -- it belongs to the store the listing walks, and a
    /// token this client invented would resume from the wrong place. So the whole namespace is
    /// walked by handing each response's token straight back, and the name semantics are
    /// applied here. Constructing no token is what makes this work against a server on either
    /// side of the change: it only ever repeats what the server said.
    ///
    /// This is the cost `table_names` already paid -- the server used to enumerate and sort the
    /// namespace on every request -- and it is why `list_tables` replaces it.
    async fn table_names_in_namespace(
        &self,
        request: &TableNamesRequest,
    ) -> Result<(Vec<String>, ServerVersion)> {
        let namespace_id = build_namespace_identifier(&request.namespace_path)?;
        let path = format!("/v1/namespace/{}/table/list", namespace_id);

        let mut names = Vec::new();
        // Every page reports the same server, so keep the first page's version.
        let mut version: Option<ServerVersion> = None;
        let mut page_token: Option<String> = None;
        loop {
            let mut req = self.client.get(&path);
            if let Some(ref token) = page_token {
                req = req.query(&[("page_token", token)]);
            }
            let (request_id, rsp) = self.client.send_with_retry(req, None, true).await?;
            let rsp = self.client.check_response(&request_id, rsp).await?;
            if version.is_none() {
                version = Some(parse_server_version(&request_id, &rsp)?);
            }
            let response: ListTablesResponse = rsp.json().await.err_to_http(request_id)?;
            names.extend(response.tables);
            // An empty token is the end of the listing, not a token to send back: a server
            // that reads an empty token as "start from the beginning" would hand back the
            // first page again.
            match response.page_token.filter(|token| !token.is_empty()) {
                // A server that repeated a token would never finish; treat that as the end
                // rather than looping on it.
                Some(token) if Some(&token) != page_token.as_ref() => page_token = Some(token),
                _ => break,
            }
        }

        names.sort();
        if let Some(ref start_after) = request.start_after {
            names.retain(|name| name > start_after);
        }
        if let Some(limit) = request.limit {
            names.truncate(limit as usize);
        }
        Ok((names, version.unwrap_or_default()))
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
                sql_client: None,
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
                sql_client: None,
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

/// The path segment addressing one object: its namespace path and its name.
///
/// One builder for tables, Secrets, Functions and materialized views: the
/// identifier grammar belongs to the namespace spec, not to an object type. An
/// empty path addresses an object with no namespace.
///
/// Components are checked for addressability, not a character set. The name's
/// grammar is the caller's, so a table reports [`Error::InvalidTableName`], a
/// Function admits names a table may not, and a catalog database carries the
/// `/` that [`RemoteCatalog`] allows.
///
/// [`RemoteCatalog`]: super::catalog::RemoteCatalog
fn build_object_identifier(what: &str, name: &str, namespace: &[String]) -> Result<String> {
    for segment in namespace {
        reject_unaddressable_component("namespace segment", segment)?;
    }
    reject_unaddressable_component(what, name)?;
    Ok(join_identifier(
        namespace.iter().map(String::as_str).chain([name]),
    ))
}

/// What a component may not be if the join is to survive being split back
/// apart: empty, a segment URL parsing resolves away, or the delimiter itself.
///
/// Each erases a boundary no encoding of the joined form recovers. `["prod",
/// ""]` joins to `prod$`, which reads back as `["prod"]`, so a drop reaches the
/// parent of the namespace the caller named.
///
/// Not a character set: per-component percent-encoding makes the wider set
/// safe, since a `/` in a name reaches the service as `%2F`, still one
/// segment.
fn reject_unaddressable_component(what: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::InvalidInput {
            message: format!(
                "{what} must not be empty: the identifier would carry two delimiters in a row, \
                 and splitting it back apart would name a different object"
            ),
        });
    }
    reject_relative_segment(what, value)?;
    if value.contains(ID_DELIMITER) {
        return Err(Error::InvalidInput {
            message: format!(
                "{what} '{value}' contains the identifier delimiter '{ID_DELIMITER}', so the \
                 namespace path and the name it joins could not be told apart"
            ),
        });
    }
    Ok(())
}

/// The path segment addressing one table. A wrapper for the error type:
/// callers match on [`Error::InvalidTableName`].
fn build_table_identifier(name: &str, namespace: &[String]) -> Result<String> {
    validate_table_name(name)?;
    build_object_identifier("table name", name, namespace)
}

/// Join components into the `{id}` a route addresses: each percent-encoded,
/// then joined by the delimiter.
///
/// Per component rather than over the joined string, so the delimiter stays a
/// delimiter and nothing inside a component can end the path segment.
///
/// A second line, not the first: a component from the object charset is all
/// unreserved and encodes to itself, so the route reads as the caller wrote it.
/// It does not cover `.` and `..`, which are unreserved too and resolve away
/// after decoding -- [`build_object_identifier`] refuses those.
fn join_identifier<'a>(components: impl Iterator<Item = &'a str>) -> String {
    components
        .map(|component| urlencoding::encode(component).into_owned())
        .collect::<Vec<_>>()
        .join(ID_DELIMITER)
}

/// The path segment addressing one namespace.
fn build_namespace_identifier(namespace: &[String]) -> Result<String> {
    for segment in namespace {
        reject_unaddressable_component("namespace segment", segment)?;
    }
    if namespace.is_empty() {
        // According to the namespace spec, use delimiter to represent root namespace
        return Ok(ID_DELIMITER.to_string());
    }
    Ok(join_identifier(namespace.iter().map(String::as_str)))
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

#[derive(serde::Deserialize)]
struct RemoteListedFunctionVersion {
    definition: FunctionVersion,
}

#[derive(serde::Deserialize)]
struct RemoteListFunctionsResponse {
    #[serde(default)]
    functions: Vec<RemoteListedFunctionVersion>,
    #[serde(default)]
    page_token: Option<String>,
}

#[derive(serde::Deserialize)]
struct RemoteDropFunctionResponse {
    dropped: bool,
}

/// The create body: every field of [`FunctionRegistrationRequest`] except the
/// name, which is the path identifier.
///
/// A struct rather than a literal listing the fields, so that the compiler
/// decides what reaches the service. A field the registration request grows is
/// a build error here until it is handled; a literal would simply not send it.
#[derive(serde::Serialize)]
struct RemoteCreateFunctionRequest<'a> {
    artifact: &'a FunctionArtifactRequest,
    signature: &'a FunctionSignature,
    runtime: &'a PythonRuntimeSpec,
    /// Absent when the Function binds nothing, so such a client sends what a
    /// client without bindings sends.
    ///
    /// A service that does not know the field ignores it: registration
    /// succeeds, the returned version carries no bindings, and the Function
    /// fails at execution with the variable unset. [`ServerVersion`] is how
    /// this codebase refuses a feature the service is too old for; it is held
    /// per table, so gating a database-level call is follow-up work.
    ///
    /// [`ServerVersion`]: super::db::ServerVersion
    #[serde(skip_serializing_if = "<[SecretBinding]>::is_empty")]
    secret_bindings: &'a [SecretBinding],
}

/// Create a Secret under a name the database does not yet hold.
///
/// Declared separately from the alter request although the two are identical
/// today: they are different operations to the service -- one refuses an
/// existing name, the other requires it -- and either may grow a field the
/// other has no meaning for.
///
/// The name and its namespace are the path identifier, so neither appears here.
#[derive(serde::Serialize)]
struct RemoteCreateSecretRequest<'a> {
    value: &'a str,
}

/// Replace the credential behind a Secret the database already holds.
#[derive(serde::Serialize)]
struct RemoteAlterSecretRequest<'a> {
    value: &'a str,
}

#[derive(serde::Deserialize)]
struct RemoteListSecretsResponse {
    #[serde(default)]
    secrets: Vec<RemoteListedSecret>,
    #[serde(default)]
    page_token: Option<String>,
}

/// An object rather than a bare name so a later listing can carry a Secret's
/// type or last-updated time without breaking this one.
#[derive(serde::Deserialize)]
struct RemoteListedSecret {
    name: String,
}

/// Define a view from a query. The name and its namespace are the path
/// identifier, so neither appears here.
#[derive(serde::Serialize)]
struct RemoteCreateViewRequest<'a> {
    query: &'a str,
}

/// What the service reports about one view. The schema arrives as the
/// namespace spec's JSON encoding, which is what `describe_table` uses too.
#[derive(serde::Deserialize)]
struct RemoteViewDescription {
    name: String,
    #[serde(default)]
    namespace: Vec<String>,
    query: String,
    default_database: String,
    /// A path, like `namespace`: the root is the absent field rather than a
    /// spelling of its own.
    #[serde(default)]
    default_namespace: Vec<String>,
    schema: JsonArrowSchema,
}

impl RemoteViewDescription {
    fn into_description(self, request_id: String) -> Result<ViewDescription> {
        let schema =
            lance_namespace::schema::convert_json_arrow_schema(&self.schema).map_err(|source| {
                Error::Http {
                    source: format!("View '{}' has an undecodable schema: {source}", self.name)
                        .into(),
                    request_id,
                    status_code: None,
                }
            })?;
        Ok(ViewDescription {
            name: self.name,
            namespace_path: self.namespace,
            query: self.query,
            default_database: self.default_database,
            default_namespace_path: self.default_namespace,
            schema: Arc::new(schema),
        })
    }
}

#[derive(serde::Deserialize)]
struct RemoteListViewsResponse {
    #[serde(default)]
    views: Vec<String>,
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

    async fn create_materialized_view_async(
        &self,
        request: CreateMaterializedViewRequest,
    ) -> Result<Job> {
        if request.with_no_data {
            return Err(Error::NotSupported {
                message: "remote materialized views are always populated by their SQL job"
                    .to_string(),
            });
        }
        let client = self.sql_client.clone().ok_or_else(|| Error::NotSupported {
            message: "SQL is unavailable for this remote database client".to_string(),
        })?;
        let namespace = request.namespace_path;
        let statement = format!(
            "CREATE MATERIALIZED VIEW {} AS {}",
            quote_sql_identifier(&request.name),
            request.query
        );
        Ok(client.submit_as_job(statement, namespace, || async { Ok(()) }))
    }

    async fn drop_materialized_view_async(
        &self,
        name: &str,
        namespace_path: &[String],
    ) -> Result<Job> {
        let identifier = build_table_identifier(name, namespace_path)?;
        let request = self
            .client
            .post(&format!("/v1/materialized_view/{identifier}/drop"));
        let (request_id, response) = self.client.send(request).await?;
        let response = self.client.check_response(&request_id, response).await?;
        let status = response.status();
        let body = response.text().await.err_to_http(request_id.clone())?;
        let job = match status {
            StatusCode::OK => Job::new_done(),
            StatusCode::ACCEPTED => {
                let job_id = extract_job_id(&body).ok_or_else(|| Error::Http {
                    source: "materialized-view drop response did not contain a valid job_id".into(),
                    request_id,
                    status_code: Some(status),
                })?;
                Job::new(Box::new(RemoteJob::new(self.client.clone(), job_id)))
            }
            _ => {
                return Err(Error::Http {
                    source: "materialized-view drop must return 200 OK or 202 Accepted".into(),
                    request_id,
                    status_code: Some(status),
                });
            }
        };
        self.table_cache
            .remove(&build_cache_key(name, namespace_path))
            .await;
        Ok(job)
    }

    async fn list_materialized_views(&self, namespace_path: &[String]) -> Result<Vec<String>> {
        #[derive(serde::Deserialize)]
        struct ListMaterializedViewsResponse {
            #[serde(default)]
            views: Vec<String>,
            #[serde(default)]
            page_token: Option<String>,
        }

        let namespace_id = build_namespace_identifier(namespace_path)?;
        let path = format!("/v1/namespace/{namespace_id}/materialized_view/list");
        let mut views = Vec::new();
        let mut page_token: Option<String> = None;
        let mut seen_page_tokens = HashSet::new();
        loop {
            let mut req = self.client.get(&path);
            if let Some(token) = &page_token {
                req = req.query(&[("page_token", token)]);
            }
            let (request_id, response) = self.client.send(req).await?;
            let response = self.client.check_response(&request_id, response).await?;
            let status = response.status();
            let response: ListMaterializedViewsResponse =
                response.json().await.err_to_http(request_id.clone())?;
            views.extend(response.views);
            let Some(next_page_token) = response.page_token.filter(|token| !token.is_empty())
            else {
                break;
            };
            if !seen_page_tokens.insert(next_page_token.clone()) {
                return Err(Error::Http {
                    source: "Materialized-view listing response repeated a page_token".into(),
                    request_id,
                    status_code: Some(status),
                });
            }
            page_token = Some(next_page_token);
        }
        Ok(views)
    }

    async fn create_function_async(
        &self,
        request: FunctionRegistrationRequest,
        namespace_path: &[String],
    ) -> Result<Job<FunctionVersion>> {
        let function_id = build_object_identifier("Function name", &request.name, namespace_path)?;
        let req = self
            .client
            .post(&format!("/v1/function/{function_id}/create"))
            .json(&RemoteCreateFunctionRequest {
                artifact: &request.artifact,
                signature: &request.signature,
                runtime: &request.runtime,
                secret_bindings: &request.secret_bindings,
            });
        let (request_id, response) = self.client.send(req).await?;
        let response = self.client.check_response(&request_id, response).await?;
        let status = response.status();
        let body = response.text().await.err_to_http(request_id.clone())?;
        let job_id = extract_job_id(&body).ok_or_else(|| Error::Http {
            source: "Function registration response did not contain a valid job_id".into(),
            request_id,
            status_code: Some(status),
        })?;
        Ok(Job::new_typed(Box::new(RemoteJob::new(
            self.client.clone(),
            job_id,
        ))))
    }

    async fn get_function(
        &self,
        name: &str,
        version: &str,
        namespace_path: &[String],
    ) -> Result<FunctionVersion> {
        let function_id = build_object_identifier("Function name", name, namespace_path)?;
        let req = self
            .client
            .post(&format!("/v1/function/{function_id}/describe"))
            .json(&serde_json::json!({
                "version": version,
            }));
        let (request_id, response) = self.client.send(req).await?;
        let response = self.client.check_response(&request_id, response).await?;
        response.json().await.err_to_http(request_id)
    }

    async fn list_functions(&self, namespace_path: &[String]) -> Result<Vec<FunctionVersion>> {
        let namespace_id = build_namespace_identifier(namespace_path)?;
        let path = format!("/v1/namespace/{namespace_id}/function/list");
        let mut functions = Vec::new();
        let mut page_token: Option<String> = None;
        let mut seen_page_tokens = HashSet::new();
        loop {
            let mut req = self
                .client
                .get(&path)
                .query(&[("include_definition", true)]);
            if let Some(token) = &page_token {
                req = req.query(&[("page_token", token)]);
            }
            let (request_id, response) = self.client.send(req).await?;
            let response = self.client.check_response(&request_id, response).await?;
            let status = response.status();
            let response: RemoteListFunctionsResponse =
                response.json().await.err_to_http(request_id.clone())?;
            functions.extend(
                response
                    .functions
                    .into_iter()
                    .map(|listed| listed.definition),
            );
            let Some(next_page_token) = response.page_token.filter(|token| !token.is_empty())
            else {
                break;
            };
            if !seen_page_tokens.insert(next_page_token.clone()) {
                return Err(Error::Http {
                    source: "Function listing response repeated a page_token".into(),
                    request_id,
                    status_code: Some(status),
                });
            }
            page_token = Some(next_page_token);
        }
        Ok(functions)
    }

    async fn drop_function(
        &self,
        name: &str,
        version: &str,
        namespace_path: &[String],
    ) -> Result<bool> {
        Ok(self
            .drop_function_async(name, version, namespace_path)
            .await?
            .0)
    }

    async fn drop_function_async(
        &self,
        name: &str,
        version: &str,
        namespace_path: &[String],
    ) -> Result<(bool, Job)> {
        let function_id = build_object_identifier("Function name", name, namespace_path)?;
        let req = self
            .client
            .post(&format!("/v1/function/{function_id}/drop"))
            .json(&serde_json::json!({
                "version": version,
            }));
        let (request_id, response) = self.client.send(req).await?;
        let response = self.client.check_response(&request_id, response).await?;
        let status = response.status();
        let body = response.text().await.err_to_http(request_id.clone())?;
        let dropped: RemoteDropFunctionResponse =
            serde_json::from_str(&body).map_err(|source| Error::Http {
                source: Box::new(source),
                request_id: request_id.clone(),
                status_code: Some(status),
            })?;
        let job = match status {
            StatusCode::OK => Job::new_done(),
            StatusCode::ACCEPTED => {
                let job_id = extract_job_id(&body).ok_or_else(|| Error::Http {
                    source: "asynchronous Function drop response did not contain a valid job_id"
                        .into(),
                    request_id,
                    status_code: Some(status),
                })?;
                Job::new(Box::new(RemoteJob::new(self.client.clone(), job_id)))
            }
            _ => {
                return Err(Error::Http {
                    source: "Function drop must return 200 OK or 202 Accepted".into(),
                    request_id,
                    status_code: Some(status),
                });
            }
        };
        Ok((dropped.dropped, job))
    }

    async fn create_secret(
        &self,
        name: &str,
        value: &str,
        namespace_path: &[String],
    ) -> Result<()> {
        let secret_id = build_object_identifier("Secret name", name, namespace_path)?;
        self.post_secret_write(
            &format!("/v1/secret/{secret_id}/create"),
            &RemoteCreateSecretRequest { value },
        )
        .await
    }

    async fn alter_secret(&self, name: &str, value: &str, namespace_path: &[String]) -> Result<()> {
        let secret_id = build_object_identifier("Secret name", name, namespace_path)?;
        self.post_secret_write(
            &format!("/v1/secret/{secret_id}/alter"),
            &RemoteAlterSecretRequest { value },
        )
        .await
    }

    async fn list_secrets(&self, namespace_path: &[String]) -> Result<Vec<String>> {
        let namespace_id = build_namespace_identifier(namespace_path)?;
        let path = format!("/v1/namespace/{namespace_id}/secret/list");
        let mut names = Vec::new();
        let mut page_token: Option<String> = None;
        let mut seen_page_tokens = HashSet::new();
        loop {
            let mut req = self.client.get(&path);
            if let Some(token) = &page_token {
                req = req.query(&[("page_token", token)]);
            }
            let (request_id, response) = self.client.send(req).await?;
            let response = self.client.check_response(&request_id, response).await?;
            let status = response.status();
            let response: RemoteListSecretsResponse =
                response.json().await.err_to_http(request_id.clone())?;
            names.extend(response.secrets.into_iter().map(|secret| secret.name));
            let Some(next_page_token) = response.page_token.filter(|token| !token.is_empty())
            else {
                break;
            };
            if !seen_page_tokens.insert(next_page_token.clone()) {
                return Err(Error::Http {
                    source: "Secret listing response repeated a page_token".into(),
                    request_id,
                    status_code: Some(status),
                });
            }
            page_token = Some(next_page_token);
        }
        Ok(names)
    }

    async fn drop_secret(&self, name: &str, namespace_path: &[String]) -> Result<()> {
        let secret_id = build_object_identifier("Secret name", name, namespace_path)?;
        let req = self.client.post(&format!("/v1/secret/{secret_id}/drop"));
        let (request_id, response) = self.client.send(req).await?;
        self.client.check_response(&request_id, response).await?;
        Ok(())
    }

    async fn describe_secret(&self, name: &str, namespace_path: &[String]) -> Result<SecretInfo> {
        let secret_id = build_object_identifier("Secret name", name, namespace_path)?;
        let req = self
            .client
            .post(&format!("/v1/secret/{secret_id}/describe"));
        let (request_id, response) = self.client.send(req).await?;
        let response = self.client.check_response(&request_id, response).await?;
        response.json().await.err_to_http(request_id)
    }

    async fn create_view(
        &self,
        name: &str,
        query: &str,
        namespace_path: &[String],
    ) -> Result<ViewDescription> {
        let view_id = build_object_identifier("View name", name, namespace_path)?;
        let req = self
            .client
            .post(&format!("/v1/view/{view_id}/create"))
            .json(&RemoteCreateViewRequest { query });
        let (request_id, response) = self.client.send(req).await?;
        let response = self.client.check_response(&request_id, response).await?;
        let description: RemoteViewDescription =
            response.json().await.err_to_http(request_id.clone())?;
        description.into_description(request_id)
    }

    async fn describe_view(
        &self,
        name: &str,
        namespace_path: &[String],
    ) -> Result<ViewDescription> {
        let view_id = build_object_identifier("View name", name, namespace_path)?;
        let req = self.client.post(&format!("/v1/view/{view_id}/describe"));
        let (request_id, response) = self.client.send(req).await?;
        let response = self.client.check_response(&request_id, response).await?;
        let description: RemoteViewDescription =
            response.json().await.err_to_http(request_id.clone())?;
        description.into_description(request_id)
    }

    async fn drop_view(&self, name: &str, namespace_path: &[String]) -> Result<()> {
        self.drop_view_async(name, namespace_path)
            .await?
            .wait()
            .await
    }

    async fn drop_view_async(&self, name: &str, namespace_path: &[String]) -> Result<Job> {
        let view_id = build_object_identifier("View name", name, namespace_path)?;
        let req = self.client.post(&format!("/v1/view/{view_id}/drop"));
        let (request_id, response) = self.client.send(req).await?;
        let response = self.client.check_response(&request_id, response).await?;
        let status = response.status();
        let body = response.text().await.err_to_http(request_id.clone())?;
        match status {
            // Nothing was bound to the name, so nothing is being deleted.
            StatusCode::OK => Ok(Job::new_done()),
            StatusCode::ACCEPTED => {
                let job_id = extract_job_id(&body).ok_or_else(|| Error::Http {
                    source: "view drop response did not contain a valid job_id".into(),
                    request_id,
                    status_code: Some(status),
                })?;
                Ok(Job::new(Box::new(RemoteJob::new(
                    self.client.clone(),
                    job_id,
                ))))
            }
            _ => Err(Error::Http {
                source: "view drop must return 200 OK or 202 Accepted".into(),
                request_id,
                status_code: Some(status),
            }),
        }
    }

    async fn list_views(&self, namespace_path: &[String]) -> Result<Vec<String>> {
        let namespace_id = build_namespace_identifier(namespace_path)?;
        let path = format!("/v1/namespace/{namespace_id}/view/list");
        let mut views = Vec::new();
        let mut page_token: Option<String> = None;
        let mut seen_page_tokens = HashSet::new();
        loop {
            let mut req = self.client.get(&path);
            if let Some(token) = &page_token {
                req = req.query(&[("page_token", token)]);
            }
            let (request_id, response) = self.client.send(req).await?;
            let response = self.client.check_response(&request_id, response).await?;
            let status = response.status();
            let response: RemoteListViewsResponse =
                response.json().await.err_to_http(request_id.clone())?;
            views.extend(response.views);
            let Some(next_page_token) = response.page_token.filter(|token| !token.is_empty())
            else {
                break;
            };
            if !seen_page_tokens.insert(next_page_token.clone()) {
                return Err(Error::Http {
                    source: "View listing response repeated a page_token".into(),
                    request_id,
                    status_code: Some(status),
                });
            }
            page_token = Some(next_page_token);
        }
        Ok(views)
    }

    async fn open_job(&self, job_id: &str) -> Result<Job> {
        let handle = super::job::RemoteJob::new(self.client.clone(), job_id.to_string());
        match crate::job::JobHandle::describe(&handle).await {
            Ok(description) => Ok(Job::opened(Box::new(handle), description)),
            Err(Error::Http {
                status_code: Some(StatusCode::NOT_FOUND),
                ..
            }) => Err(Error::JobNotFound {
                job_id: job_id.to_string(),
            }),
            Err(err) => Err(err),
        }
    }

    async fn list_jobs(&self) -> Result<Vec<JobInfo>> {
        let mut out = Vec::new();
        let mut page_token: Option<String> = None;
        let mut seen_page_tokens = HashSet::new();
        for page in 0..MAX_LIST_JOBS_PAGES {
            let mut body = serde_json::json!({});
            if let Some(token) = &page_token {
                body["page_token"] = serde_json::Value::String(token.clone());
            }
            let req = self.client.post("/v1/jobs/list").json(&body);
            let (request_id, rsp) = self.client.send(req).await?;
            let rsp = self.client.check_response(&request_id, rsp).await?;
            let status = rsp.status();
            let body: RemoteListJobsResponse = rsp.json().await.err_to_http(request_id.clone())?;
            out.extend(body.jobs.into_iter().map(|row| JobInfo {
                job_id: row.job_id,
                table: row.table,
                job_type: row.job_type,
                state: job_state_to_client(&row.state),
                created_at_millis: row.created_at_millis,
            }));
            let Some(next_page_token) = body.page_token.filter(|token| !token.is_empty()) else {
                break;
            };
            if !seen_page_tokens.insert(next_page_token.clone()) {
                return Err(Error::Http {
                    source: "Job listing response repeated a page_token".into(),
                    request_id,
                    status_code: Some(status),
                });
            }
            page_token = Some(next_page_token);
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

    async fn pause_job(&self, job_id: &str) -> Result<crate::database::PauseJobStatus> {
        let req = self
            .client
            .post("/v1/jobs/pause")
            .json(&serde_json::json!({ "job_id": job_id }));
        let (request_id, rsp) = self.client.send(req).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let body: PauseJobResponse = rsp.json().await.err_to_http(request_id)?;
        Ok(if body.paused {
            crate::database::PauseJobStatus::Pausing
        } else if body.committing {
            crate::database::PauseJobStatus::Committing
        } else {
            crate::database::PauseJobStatus::AlreadyPaused
        })
    }

    async fn resume_job(&self, job_id: &str) -> Result<crate::database::ResumeJobStatus> {
        let req = self
            .client
            .post("/v1/jobs/resume")
            .json(&serde_json::json!({ "job_id": job_id }));
        let (request_id, rsp) = self.client.send(req).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let body: ResumeJobResponse = rsp.json().await.err_to_http(request_id)?;
        Ok(if body.resumed {
            crate::database::ResumeJobStatus::Resumed
        } else if body.still_pausing {
            crate::database::ResumeJobStatus::StillPausing
        } else {
            crate::database::ResumeJobStatus::NotPaused
        })
    }

    async fn execute_query_async(
        &self,
        query: &str,
        default_namespace_path: &[String],
    ) -> Result<crate::sql::Query> {
        let client = self
            .sql_client
            .as_ref()
            .ok_or_else(|| Error::NotSupported {
                message: "SQL is unavailable for this remote database client".to_string(),
            })?;
        client.submit(query, default_namespace_path).await
    }

    async fn describe_query(&self, query_id: uuid::Uuid) -> Result<crate::sql::QueryDescription> {
        let client = self
            .sql_client
            .as_ref()
            .ok_or_else(|| Error::NotSupported {
                message: "SQL is unavailable for this remote database client".to_string(),
            })?;
        client.describe(query_id).await
    }

    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>> {
        let (tables, version) = if request.namespace_path.is_empty() {
            // The flat route resumes after a table name and orders by name, which is exactly
            // what `start_after` means, so the server does the paging.
            let mut req = self.client.get("/v1/table/");
            if let Some(limit) = request.limit {
                req = req.query(&[("limit", limit)]);
            }
            if let Some(ref start_after) = request.start_after {
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
            (tables, version)
        } else {
            self.table_names_in_namespace(&request).await?
        };

        for table in &tables {
            build_table_identifier(table, &request.namespace_path)?;
            let cache_key = build_cache_key(table, &request.namespace_path);
            self.table_cache.insert(cache_key, version.clone()).await;
        }
        Ok(tables)
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        let namespace_parts = request.id.as_deref().unwrap_or(&[]);
        let namespace_id = build_namespace_identifier(namespace_parts)?;
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
            build_table_identifier(table, &namespace_vec)?;
            let cache_key = build_cache_key(table, &namespace_vec);
            self.table_cache.insert(cache_key, version.clone()).await;
        }

        Ok(response)
    }

    async fn create_table(&self, mut request: CreateTableRequest) -> Result<Arc<dyn BaseTable>> {
        let data_schema = request.data.schema();
        let body = stream_as_body(request.data.scan_as_stream())?;

        let identifier = build_table_identifier(&request.name, &request.namespace_path)?;
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
                        let table = self.open_table(req).await?;
                        let table_schema = table.schema().await?;

                        if table_schema.as_ref() != data_schema.as_ref() {
                            return Err(Error::Schema {
                                message: "Provided schema does not match existing table schema"
                                    .to_string(),
                            });
                        }

                        Ok(table)
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
        let table_identifier = build_table_identifier(&request.name, &request.namespace_path)?;
        let cache_key = build_cache_key(&request.name, &request.namespace_path);
        let table = Arc::new(RemoteTable::new_with_sql_client(
            self.client.clone(),
            request.name.clone(),
            request.namespace_path.clone(),
            table_identifier,
            version.clone(),
            self.sql_client.clone(),
        ));
        self.table_cache.insert(cache_key, version).await;

        Ok(table)
    }

    async fn clone_table(&self, request: CloneTableRequest) -> Result<Arc<dyn BaseTable>> {
        let table_identifier =
            build_table_identifier(&request.target_table_name, &request.target_namespace_path)?;

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
        let table = Arc::new(RemoteTable::new_with_sql_client(
            self.client.clone(),
            request.target_table_name.clone(),
            request.target_namespace_path.clone(),
            table_identifier,
            version.clone(),
            self.sql_client.clone(),
        ));
        self.table_cache.insert(cache_key, version).await;

        Ok(table)
    }

    async fn open_table(&self, request: OpenTableRequest) -> Result<Arc<dyn BaseTable>> {
        let identifier = build_table_identifier(&request.name, &request.namespace_path)?;
        let cache_key = build_cache_key(&request.name, &request.namespace_path);

        // Every open gets its own checkout, schema cache, and freshness state.
        if let Some(version) = self.table_cache.get(&cache_key).await {
            Ok(Arc::new(RemoteTable::new_with_sql_client(
                self.client.clone(),
                request.name,
                request.namespace_path,
                identifier,
                version,
                self.sql_client.clone(),
            )))
        } else {
            // Describe the table to confirm it exists before moving on.
            let req = self
                .client
                .post(&format!("/v1/table/{}/describe/", identifier));
            let (request_id, rsp) = self.client.send_with_retry(req, None, true).await?;
            let rsp =
                RemoteTable::<S>::handle_table_not_found(&request.name, rsp, &request_id).await?;
            let rsp = self.client.check_response(&request_id, rsp).await?;
            let version = parse_server_version(&request_id, &rsp)?;
            let describe_body = rsp.text().await.ok();
            let table = Arc::new(RemoteTable::new_with_sql_client(
                self.client.clone(),
                request.name.clone(),
                request.namespace_path.clone(),
                identifier,
                version.clone(),
                self.sql_client.clone(),
            ));
            // This describe already carries the schema, so hand it to the table
            // instead of making the first schema read fetch it again. A version or
            // branch pin applied after this invalidates the cache.
            if let Some(body) = &describe_body {
                table.seed_schema(body);
            }
            self.table_cache.insert(cache_key, version).await;
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
        let current_identifier = build_table_identifier(current_name, cur_namespace_path)?;
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
        self.submit_drop_table(name, namespace_path)
            .await
            .map(|_| ())
    }

    async fn drop_table_async(&self, name: &str, namespace_path: &[String]) -> Result<Job> {
        let (request_id, response) = self.submit_drop_table(name, namespace_path).await?;
        let status = response.status();
        let body = response.text().await.err_to_http(request_id.clone())?;
        let job_id = extract_job_id(&body);
        Ok(match job_id {
            Some(job_id) => Job::new(Box::new(RemoteJob::new(self.client.clone(), job_id))),
            None if status == StatusCode::ACCEPTED => {
                return Err(Error::Http {
                    source: "asynchronous drop-table response did not contain a valid job_id"
                        .into(),
                    request_id,
                    status_code: Some(status),
                });
            }
            None => Job::new_done(),
        })
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
        let namespace_id = build_namespace_identifier(namespace_parts)?;
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
        let namespace_id = build_namespace_identifier(namespace_parts)?;
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
            mode: request.mode,
            properties: request.properties,
        };

        req = req.json(&body);
        let (request_id, resp) = self.client.send(req).await?;
        let resp = self.client.check_response(&request_id, resp).await?;

        if resp.status() == StatusCode::NO_CONTENT {
            return Ok(CreateNamespaceResponse::default());
        }
        resp.json().await.err_to_http(request_id)
    }

    async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<DropNamespaceResponse> {
        let namespace_parts = request.id.as_deref().unwrap_or(&[]);
        let namespace_id = build_namespace_identifier(namespace_parts)?;
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
            mode: request.mode,
            behavior: request.behavior,
        };

        req = req.json(&body);
        let (request_id, resp) = self.client.send(req).await?;
        let resp = self.client.check_response(&request_id, resp).await?;

        if resp.status() == StatusCode::NO_CONTENT {
            return Ok(DropNamespaceResponse::default());
        }
        resp.json().await.err_to_http(request_id)
    }

    async fn describe_namespace(
        &self,
        request: DescribeNamespaceRequest,
    ) -> Result<DescribeNamespaceResponse> {
        let namespace_parts = request.id.as_deref().unwrap_or(&[]);
        let namespace_id = build_namespace_identifier(namespace_parts)?;
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
            .delimiter(ID_DELIMITER)
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
        properties.insert("delimiter".to_string(), ID_DELIMITER.to_string());
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
mod tests;
