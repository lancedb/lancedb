// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! REST server adapter for Lance Namespace
//!
//! This module provides a REST API server that wraps any `LanceNamespace` implementation,
//! allowing it to be accessed via HTTP. The server implements the Lance REST Namespace
//! specification.

use std::sync::Arc;

use axum::{
    Json, Router, ServiceExt,
    body::Bytes,
    extract::{FromRequest, Path, Query, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, de::DeserializeOwned};
use tokio::sync::watch;
use tower::Layer;
use tower_http::normalize_path::NormalizePathLayer;
use tower_http::trace::TraceLayer;

use lance_core::{Error, Result};
use lance_namespace::LanceNamespace;
use lance_namespace::error::NamespaceError;
use lance_namespace::models::*;

/// Configuration for the REST server
#[derive(Debug, Clone)]
pub struct RestAdapterConfig {
    /// Host address to bind to
    pub host: String,
    /// Port to listen on
    pub port: u16,
}

impl Default for RestAdapterConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 2333,
        }
    }
}

/// REST server adapter that wraps a Lance Namespace implementation
pub struct RestAdapter {
    backend: Arc<dyn LanceNamespace>,
    config: RestAdapterConfig,
}

impl RestAdapter {
    /// Create a new REST server with the given backend namespace
    pub fn new(backend: Arc<dyn LanceNamespace>, config: RestAdapterConfig) -> Self {
        Self { backend, config }
    }

    /// Build the Axum router with all REST API routes
    fn router(&self) -> Router {
        Router::new()
            // Namespace operations
            .route("/v1/namespace/:id/create", post(create_namespace))
            .route("/v1/namespace/:id/list", get(list_namespaces))
            .route("/v1/namespace/:id/describe", post(describe_namespace))
            .route("/v1/namespace/:id/drop", post(drop_namespace))
            .route("/v1/namespace/:id/exists", post(namespace_exists))
            .route("/v1/namespace/:id/table/list", get(list_tables))
            // Table metadata operations
            .route("/v1/table/:id/register", post(register_table))
            .route("/v1/table/:id/describe", post(describe_table))
            .route("/v1/table/:id/exists", post(table_exists))
            .route("/v1/table/:id/drop", post(drop_table))
            .route("/v1/table/:id/deregister", post(deregister_table))
            .route("/v1/table/:id/rename", post(rename_table))
            .route("/v1/table/:id/restore", post(restore_table))
            .route("/v1/table/:id/version/list", post(list_table_versions))
            .route("/v1/table/:id/version/create", post(create_table_version))
            .route(
                "/v1/table/:id/version/describe",
                post(describe_table_version),
            )
            .route(
                "/v1/table/:id/version/delete",
                post(batch_delete_table_versions),
            )
            .route("/v1/table/:id/stats", get(get_table_stats))
            // Table data operations
            .route("/v1/table/:id/create", post(create_table))
            .route("/v1/table/:id/declare", post(declare_table))
            .route("/v1/table/:id/insert", post(insert_into_table))
            .route("/v1/table/:id/merge_insert", post(merge_insert_into_table))
            .route("/v1/table/:id/update", post(update_table))
            .route("/v1/table/:id/delete", post(delete_from_table))
            .route("/v1/table/:id/query", post(query_table))
            .route("/v1/table/:id/count_rows", get(count_table_rows))
            // Index operations
            .route("/v1/table/:id/create_index", post(create_table_index))
            .route(
                "/v1/table/:id/create_scalar_index",
                post(create_table_scalar_index),
            )
            .route("/v1/table/:id/index/list", post(list_table_indices))
            .route(
                "/v1/table/:id/index/:index_name/stats",
                get(describe_table_index_stats),
            )
            .route(
                "/v1/table/:id/index/:index_name/drop",
                post(drop_table_index),
            )
            // Schema operations
            .route("/v1/table/:id/add_columns", post(alter_table_add_columns))
            .route(
                "/v1/table/:id/alter_columns",
                post(alter_table_alter_columns),
            )
            .route("/v1/table/:id/drop_columns", post(alter_table_drop_columns))
            .route(
                "/v1/table/:id/backfill_column",
                post(alter_table_backfill_columns),
            )
            .route("/v1/table/:id/refresh", post(refresh_materialized_view))
            .route(
                "/v1/materialized_view/:id/refresh",
                post(refresh_materialized_view),
            )
            .route(
                "/v1/materialized_view/:id/create",
                post(create_materialized_view),
            )
            .route(
                "/v1/table/:id/schema_metadata/update",
                post(update_table_schema_metadata),
            )
            // Tag operations
            .route("/v1/table/:id/tags/list", get(list_table_tags))
            .route("/v1/table/:id/tags/version", post(get_table_tag_version))
            .route("/v1/table/:id/tags/create", post(create_table_tag))
            .route("/v1/table/:id/tags/delete", post(delete_table_tag))
            .route("/v1/table/:id/tags/update", post(update_table_tag))
            // Branch operations
            .route("/v1/table/:id/branches/create", post(create_table_branch))
            .route("/v1/table/:id/branches/list", post(list_table_branches))
            .route("/v1/table/:id/branches/delete", post(delete_table_branch))
            // Query plan operations
            .route("/v1/table/:id/explain_plan", post(explain_table_query_plan))
            .route("/v1/table/:id/analyze_plan", post(analyze_table_query_plan))
            // Transaction operations
            .route("/v1/transaction/:id/describe", post(describe_transaction))
            .route("/v1/transaction/:id/alter", post(alter_transaction))
            // Global table operations
            .route("/v1/table", get(list_all_tables))
            .layer(TraceLayer::new_for_http())
            .with_state(self.backend.clone())
    }

    /// Start the REST server in the background and return a handle for shutdown.
    ///
    /// This method binds to the configured address and spawns a background task
    /// to handle requests. The returned handle can be used to gracefully shut down
    /// the server.
    ///
    /// Returns an error immediately if the server fails to bind to the address.
    /// If port 0 is specified, the OS will assign an available ephemeral port.
    /// The actual port can be retrieved from the returned handle via `port()`.
    pub async fn start(self) -> Result<RestAdapterHandle> {
        let addr = format!("{}:{}", self.config.host, self.config.port);

        let listener = tokio::net::TcpListener::bind(&addr).await.map_err(|e| {
            log::error!("RestAdapter::start() failed to bind to {}: {}", addr, e);
            Error::from(NamespaceError::Internal {
                message: format!("Failed to bind to {}: {:?}", addr, e),
            })
        })?;

        // Get the actual port (important when port 0 was specified)
        let actual_port = listener.local_addr().map(|a| a.port()).unwrap_or(0);

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let router = self.router();
        let app = NormalizePathLayer::trim_trailing_slash().layer(router);

        tokio::spawn(async move {
            let result = axum::serve(listener, ServiceExt::<Request>::into_make_service(app))
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.changed().await;
                })
                .await;

            if let Err(e) = result {
                log::error!("RestAdapter: server error: {}", e);
            }

            // Signal that server has shut down
            let _ = done_tx.send(());
        });

        Ok(RestAdapterHandle {
            shutdown_tx,
            done_rx: std::sync::Mutex::new(Some(done_rx)),
            port: actual_port,
        })
    }
}

/// Handle for controlling a running REST adapter server.
///
/// Use this handle to gracefully shut down the server when it's no longer needed.
pub struct RestAdapterHandle {
    shutdown_tx: watch::Sender<bool>,
    done_rx: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    port: u16,
}

impl RestAdapterHandle {
    /// Get the actual port the server is listening on.
    /// This is useful when port 0 was specified to get an OS-assigned port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Gracefully shut down the server and wait for it to complete.
    ///
    /// This signals the server to stop accepting new connections, waits for
    /// existing connections to complete, and blocks until the server has
    /// fully shut down.
    pub fn shutdown(&self) {
        // Send shutdown signal
        let _ = self.shutdown_tx.send(true);

        // Wait for server to complete
        if let Some(done_rx) = self.done_rx.lock().unwrap().take() {
            // Use a new runtime to block on the oneshot receiver
            // This is needed because shutdown() is called from sync context
            let _ = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let _ = rt.block_on(done_rx);
            })
            .join();
        }
    }
}

// ============================================================================
// Query Parameters and Extractors
// ============================================================================

/// Optional JSON body extractor that allows empty request bodies.
/// Similar to sophon's MaybeJson - returns None if body is empty.
struct MaybeJson<T>(Option<T>);

impl<S, T> FromRequest<S> for MaybeJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send + 'static,
{
    type Rejection = Response;

    fn from_request<'life0, 'async_trait>(
        req: Request,
        state: &'life0 S,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<Self, Self::Rejection>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let bytes = Bytes::from_request(req, state)
                .await
                .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()).into_response())?;

            if bytes.is_empty() {
                return Ok(Self(None));
            }

            match serde_json::from_slice(&bytes) {
                Ok(value) => Ok(Self(Some(value))),
                Err(e) => Err((StatusCode::BAD_REQUEST, e.to_string()).into_response()),
            }
        })
    }
}

#[derive(Debug, Deserialize)]
struct DelimiterQuery {
    delimiter: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PaginationQuery {
    delimiter: Option<String>,
    page_token: Option<String>,
    limit: Option<i32>,
    include_declared: Option<bool>,
    descending: Option<bool>,
    branch: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DescribeTableQuery {
    delimiter: Option<String>,
    with_table_uri: Option<bool>,
    load_detailed_metadata: Option<bool>,
    check_declared: Option<bool>,
}

// ============================================================================
// Error Conversion
// ============================================================================

/// Map a NamespaceError error code to an HTTP status code.
fn error_code_to_status(code: u32) -> StatusCode {
    match lance_namespace::error::ErrorCode::from_u32(code) {
        Some(lance_namespace::error::ErrorCode::NamespaceNotFound)
        | Some(lance_namespace::error::ErrorCode::TableNotFound)
        | Some(lance_namespace::error::ErrorCode::TableIndexNotFound)
        | Some(lance_namespace::error::ErrorCode::TableTagNotFound)
        | Some(lance_namespace::error::ErrorCode::TransactionNotFound)
        | Some(lance_namespace::error::ErrorCode::TableVersionNotFound)
        | Some(lance_namespace::error::ErrorCode::TableColumnNotFound)
        | Some(lance_namespace::error::ErrorCode::TableBranchNotFound) => StatusCode::NOT_FOUND,
        Some(lance_namespace::error::ErrorCode::NamespaceAlreadyExists)
        | Some(lance_namespace::error::ErrorCode::TableAlreadyExists)
        | Some(lance_namespace::error::ErrorCode::TableIndexAlreadyExists)
        | Some(lance_namespace::error::ErrorCode::TableTagAlreadyExists)
        | Some(lance_namespace::error::ErrorCode::TableBranchAlreadyExists)
        | Some(lance_namespace::error::ErrorCode::ConcurrentModification) => StatusCode::CONFLICT,
        Some(lance_namespace::error::ErrorCode::NamespaceNotEmpty)
        | Some(lance_namespace::error::ErrorCode::InvalidTableState) => StatusCode::CONFLICT,
        Some(lance_namespace::error::ErrorCode::InvalidInput)
        | Some(lance_namespace::error::ErrorCode::TableSchemaValidationError) => {
            StatusCode::BAD_REQUEST
        }
        Some(lance_namespace::error::ErrorCode::Unsupported) => StatusCode::NOT_ACCEPTABLE,
        Some(lance_namespace::error::ErrorCode::PermissionDenied) => StatusCode::FORBIDDEN,
        Some(lance_namespace::error::ErrorCode::Unauthenticated) => StatusCode::UNAUTHORIZED,
        Some(lance_namespace::error::ErrorCode::ServiceUnavailable) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        Some(lance_namespace::error::ErrorCode::Throttling) => StatusCode::TOO_MANY_REQUESTS,
        Some(lance_namespace::error::ErrorCode::Internal) | None => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Convert Lance errors to HTTP responses using the spec's `ErrorResponse` model.
fn error_to_response(err: Error) -> Response {
    match err {
        Error::Namespace { source, .. } => {
            if let Some(ns_err) = source.downcast_ref::<NamespaceError>() {
                let code = ns_err.code().as_u32();
                let status = error_code_to_status(code);
                let mut resp = ErrorResponse::new(code as i32);
                resp.error = Some(ns_err.message().to_string());
                (status, Json(resp)).into_response()
            } else {
                let mut resp = ErrorResponse::new(18);
                resp.error = Some(source.to_string());
                (StatusCode::INTERNAL_SERVER_ERROR, Json(resp)).into_response()
            }
        }
        _ => {
            let mut resp = ErrorResponse::new(18);
            resp.error = Some(err.to_string());
            (StatusCode::INTERNAL_SERVER_ERROR, Json(resp)).into_response()
        }
    }
}

// ============================================================================
// Namespace Operation Handlers
// ============================================================================

async fn create_namespace(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<CreateNamespaceRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.create_namespace(request).await {
        Ok(response) => (StatusCode::CREATED, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn list_namespaces(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> Response {
    let request = ListNamespacesRequest {
        id: Some(parse_id(&id, params.delimiter.as_deref())),
        page_token: params.page_token,
        limit: params.limit,
        identity: extract_identity(&headers),
        ..Default::default()
    };

    match backend.list_namespaces(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn describe_namespace(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<DescribeNamespaceRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.describe_namespace(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn drop_namespace(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<DropNamespaceRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.drop_namespace(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn namespace_exists(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<NamespaceExistsRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.namespace_exists(request).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => error_to_response(e),
    }
}

// ============================================================================
// Table Metadata Operation Handlers
// ============================================================================

async fn list_tables(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> Response {
    let request = ListTablesRequest {
        id: Some(parse_id(&id, params.delimiter.as_deref())),
        page_token: params.page_token,
        limit: params.limit,
        include_declared: params.include_declared,
        identity: extract_identity(&headers),
        ..Default::default()
    };

    match backend.list_tables(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn register_table(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<RegisterTableRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.register_table(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn describe_table(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DescribeTableQuery>,
    Json(mut request): Json<DescribeTableRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);
    if params.with_table_uri.is_some() {
        request.with_table_uri = params.with_table_uri;
    }
    if params.load_detailed_metadata.is_some() {
        request.load_detailed_metadata = params.load_detailed_metadata;
    }
    if params.check_declared.is_some() {
        request.check_declared = params.check_declared;
    }

    match backend.describe_table(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn table_exists(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<TableExistsRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.table_exists(request).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn drop_table(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
) -> Response {
    let request = DropTableRequest {
        id: Some(parse_id(&id, params.delimiter.as_deref())),
        identity: extract_identity(&headers),
        ..Default::default()
    };

    match backend.drop_table(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn deregister_table(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<DeregisterTableRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.deregister_table(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

// ============================================================================
// Table Data Operation Handlers
// ============================================================================

#[derive(Debug, Deserialize)]
struct CreateTableQuery {
    delimiter: Option<String>,
    mode: Option<String>,
    properties: Option<String>,
    storage_options: Option<String>,
}

fn parse_json_query_param<T: serde::de::DeserializeOwned>(
    raw: Option<&str>,
    operation: &str,
    param_name: &str,
) -> std::result::Result<Option<T>, Box<Response>> {
    match raw {
        Some(raw) => serde_json::from_str(raw).map(Some).map_err(|e| {
            let response = (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": {
                        "message": format!(
                            "Failed to parse {} {} query parameter as JSON: {}",
                            operation, param_name, e
                        )
                    }
                })),
            )
                .into_response();
            Box::new(response)
        }),
        None => Ok(None),
    }
}

async fn create_table(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<CreateTableQuery>,
    body: Bytes,
) -> Response {
    let properties =
        match parse_json_query_param(params.properties.as_deref(), "create_table", "properties") {
            Ok(properties) => properties,
            Err(response) => return *response,
        };
    let storage_options = match parse_json_query_param(
        params.storage_options.as_deref(),
        "create_table",
        "storage_options",
    ) {
        Ok(storage_options) => storage_options,
        Err(response) => return *response,
    };
    let request = CreateTableRequest {
        id: Some(parse_id(&id, params.delimiter.as_deref())),
        mode: params.mode.clone(),
        properties,
        storage_options,
        identity: extract_identity(&headers),
        ..Default::default()
    };

    match backend.create_table(request, body).await {
        Ok(response) => (StatusCode::CREATED, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn declare_table(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<DeclareTableRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.declare_table(request).await {
        Ok(response) => (StatusCode::CREATED, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

#[derive(Debug, Deserialize)]
struct InsertQuery {
    delimiter: Option<String>,
    mode: Option<String>,
}

async fn insert_into_table(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<InsertQuery>,
    body: Bytes,
) -> Response {
    let request = InsertIntoTableRequest {
        id: Some(parse_id(&id, params.delimiter.as_deref())),
        mode: params.mode.clone(),
        identity: extract_identity(&headers),
        ..Default::default()
    };

    match backend.insert_into_table(request, body).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

#[derive(Debug, Deserialize)]
struct MergeInsertQuery {
    delimiter: Option<String>,
    on: Option<String>,
    when_matched_update_all: Option<bool>,
    when_matched_update_all_filt: Option<String>,
    when_not_matched_insert_all: Option<bool>,
    when_not_matched_by_source_delete: Option<bool>,
    when_not_matched_by_source_delete_filt: Option<String>,
    timeout: Option<String>,
    use_index: Option<bool>,
}

async fn merge_insert_into_table(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<MergeInsertQuery>,
    body: Bytes,
) -> Response {
    let request = MergeInsertIntoTableRequest {
        id: Some(parse_id(&id, params.delimiter.as_deref())),
        on: params.on,
        when_matched_update_all: params.when_matched_update_all,
        when_matched_update_all_filt: params.when_matched_update_all_filt,
        when_not_matched_insert_all: params.when_not_matched_insert_all,
        when_not_matched_by_source_delete: params.when_not_matched_by_source_delete,
        when_not_matched_by_source_delete_filt: params.when_not_matched_by_source_delete_filt,
        timeout: params.timeout,
        use_index: params.use_index,
        identity: extract_identity(&headers),
        ..Default::default()
    };

    match backend.merge_insert_into_table(request, body).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn update_table(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<UpdateTableRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.update_table(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn delete_from_table(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<DeleteFromTableRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.delete_from_table(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn query_table(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<QueryTableRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.query_table(request).await {
        Ok(bytes) => (StatusCode::OK, bytes).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn count_table_rows(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
) -> Response {
    let request = CountTableRowsRequest {
        id: Some(parse_id(&id, params.delimiter.as_deref())),
        version: None,
        predicate: None,
        identity: extract_identity(&headers),
        ..Default::default()
    };

    match backend.count_table_rows(request).await {
        Ok(count) => (StatusCode::OK, Json(serde_json::json!({ "count": count }))).into_response(),
        Err(e) => error_to_response(e),
    }
}

// ============================================================================
// Table Management Operation Handlers
// ============================================================================

async fn rename_table(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<RenameTableRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.rename_table(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn restore_table(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<RestoreTableRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.restore_table(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn list_table_versions(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> Response {
    let request = ListTableVersionsRequest {
        id: Some(parse_id(&id, params.delimiter.as_deref())),
        page_token: params.page_token,
        limit: params.limit,
        descending: params.descending,
        branch: params.branch,
        identity: extract_identity(&headers),
        ..Default::default()
    };

    match backend.list_table_versions(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn create_table_version(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(body): Json<CreateTableVersionRequest>,
) -> Response {
    let request = CreateTableVersionRequest {
        id: Some(parse_id(&id, params.delimiter.as_deref())),
        identity: extract_identity(&headers),
        version: body.version,
        manifest_path: body.manifest_path,
        manifest_size: body.manifest_size,
        e_tag: body.e_tag,
        metadata: body.metadata,
        branch: body.branch,
        ..Default::default()
    };

    match backend.create_table_version(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn describe_table_version(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<DelimiterQuery>,
    Json(body): Json<DescribeTableVersionRequest>,
) -> Response {
    let request = DescribeTableVersionRequest {
        id: Some(parse_id(&id, query.delimiter.as_deref())),
        version: body.version,
        branch: body.branch,
        identity: extract_identity(&headers),
        ..Default::default()
    };

    match backend.describe_table_version(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn batch_delete_table_versions(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(body): Json<BatchDeleteTableVersionsRequest>,
) -> Response {
    let request = BatchDeleteTableVersionsRequest {
        id: Some(parse_id(&id, params.delimiter.as_deref())),
        identity: extract_identity(&headers),
        ranges: body.ranges,
        branch: body.branch,
        ..Default::default()
    };

    match backend.batch_delete_table_versions(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn get_table_stats(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
) -> Response {
    let request = GetTableStatsRequest {
        id: Some(parse_id(&id, params.delimiter.as_deref())),
        identity: extract_identity(&headers),
        ..Default::default()
    };

    match backend.get_table_stats(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn list_all_tables(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Query(params): Query<PaginationQuery>,
) -> Response {
    let request = ListTablesRequest {
        id: None,
        page_token: params.page_token,
        limit: params.limit,
        include_declared: params.include_declared,
        identity: extract_identity(&headers),
        ..Default::default()
    };

    match backend.list_all_tables(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

// ============================================================================
// Index Operation Handlers
// ============================================================================

async fn create_table_index(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<CreateTableIndexRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.create_table_index(request).await {
        Ok(response) => (StatusCode::CREATED, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn create_table_scalar_index(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<CreateTableIndexRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.create_table_scalar_index(request).await {
        Ok(response) => (StatusCode::CREATED, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn list_table_indices(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    MaybeJson(body): MaybeJson<ListTableIndicesRequest>,
) -> Response {
    let mut request = body.unwrap_or_default();
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.list_table_indices(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

#[derive(Debug, Deserialize)]
struct IndexPathParams {
    id: String,
    index_name: String,
}

async fn describe_table_index_stats(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(params): Path<IndexPathParams>,
    Query(query): Query<DelimiterQuery>,
) -> Response {
    let request = DescribeTableIndexStatsRequest {
        id: Some(parse_id(&params.id, query.delimiter.as_deref())),
        version: None,
        index_name: Some(params.index_name),
        identity: extract_identity(&headers),
        ..Default::default()
    };

    match backend.describe_table_index_stats(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn drop_table_index(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(params): Path<IndexPathParams>,
    Query(query): Query<DelimiterQuery>,
) -> Response {
    let request = DropTableIndexRequest {
        id: Some(parse_id(&params.id, query.delimiter.as_deref())),
        index_name: Some(params.index_name),
        identity: extract_identity(&headers),
        ..Default::default()
    };

    match backend.drop_table_index(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

// ============================================================================
// Schema Operation Handlers
// ============================================================================

async fn alter_table_add_columns(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<AlterTableAddColumnsRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.alter_table_add_columns(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn alter_table_alter_columns(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<AlterTableAlterColumnsRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.alter_table_alter_columns(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn alter_table_backfill_columns(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<AlterTableBackfillColumnsRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.alter_table_backfill_columns(request).await {
        Ok(response) => (StatusCode::ACCEPTED, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn refresh_materialized_view(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<RefreshMaterializedViewRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.refresh_materialized_view(request).await {
        Ok(response) => (StatusCode::ACCEPTED, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn create_materialized_view(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<CreateMaterializedViewRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.create_materialized_view(request).await {
        Ok(response) => (StatusCode::CREATED, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn alter_table_drop_columns(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<AlterTableDropColumnsRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.alter_table_drop_columns(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn update_table_schema_metadata(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<UpdateTableSchemaMetadataRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.update_table_schema_metadata(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

// ============================================================================
// Tag Operation Handlers
// ============================================================================

async fn list_table_tags(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> Response {
    let request = ListTableTagsRequest {
        id: Some(parse_id(&id, params.delimiter.as_deref())),
        page_token: params.page_token,
        limit: params.limit,
        identity: extract_identity(&headers),
        ..Default::default()
    };

    match backend.list_table_tags(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn get_table_tag_version(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<GetTableTagVersionRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.get_table_tag_version(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn create_table_tag(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<CreateTableTagRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.create_table_tag(request).await {
        Ok(response) => (StatusCode::CREATED, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn delete_table_tag(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<DeleteTableTagRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.delete_table_tag(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn update_table_tag(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<UpdateTableTagRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.update_table_tag(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

// ============================================================================
// Branch Operation Handlers
// ============================================================================

async fn create_table_branch(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<CreateTableBranchRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.create_table_branch(request).await {
        Ok(response) => (StatusCode::CREATED, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn list_table_branches(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> Response {
    let request = ListTableBranchesRequest {
        id: Some(parse_id(&id, params.delimiter.as_deref())),
        page_token: params.page_token,
        limit: params.limit,
        identity: extract_identity(&headers),
        ..Default::default()
    };

    match backend.list_table_branches(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn delete_table_branch(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<DeleteTableBranchRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.delete_table_branch(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

// ============================================================================
// Query Plan Operation Handlers
// ============================================================================

async fn explain_table_query_plan(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<ExplainTableQueryPlanRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.explain_table_query_plan(request).await {
        Ok(plan) => (StatusCode::OK, plan).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn analyze_table_query_plan(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<DelimiterQuery>,
    Json(mut request): Json<AnalyzeTableQueryPlanRequest>,
) -> Response {
    request.id = Some(parse_id(&id, params.delimiter.as_deref()));
    request.identity = extract_identity(&headers);

    match backend.analyze_table_query_plan(request).await {
        Ok(plan) => (StatusCode::OK, plan).into_response(),
        Err(e) => error_to_response(e),
    }
}

// ============================================================================
// Transaction Operation Handlers
// ============================================================================

async fn describe_transaction(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(_params): Query<DelimiterQuery>,
    Json(mut request): Json<DescribeTransactionRequest>,
) -> Response {
    // The path id is the transaction identifier
    // The request.id in body is the table ID (namespace path)
    // For the trait, we set request.id to include both table ID and transaction ID
    // by appending the transaction ID to the table ID path
    if let Some(ref mut table_id) = request.id {
        table_id.push(id);
    } else {
        request.id = Some(vec![id]);
    }
    request.identity = extract_identity(&headers);

    match backend.describe_transaction(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn alter_transaction(
    State(backend): State<Arc<dyn LanceNamespace>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(_params): Query<DelimiterQuery>,
    Json(mut request): Json<AlterTransactionRequest>,
) -> Response {
    // The path id is the transaction identifier
    // Append it to the table ID path in the request
    if let Some(ref mut table_id) = request.id {
        table_id.push(id);
    } else {
        request.id = Some(vec![id]);
    }
    request.identity = extract_identity(&headers);

    match backend.alter_transaction(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => error_to_response(e),
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Parse object ID from path string using delimiter
fn parse_id(id_str: &str, delimiter: Option<&str>) -> Vec<String> {
    let delimiter = delimiter.unwrap_or("$");

    // Special case: if ID equals delimiter, it represents root namespace (empty vec)
    if id_str == delimiter {
        return vec![];
    }

    id_str
        .split(delimiter)
        .filter(|s| !s.is_empty()) // Filter out empty strings from split
        .map(|s| s.to_string())
        .collect()
}

/// Extract identity information from HTTP headers
///
/// Extracts `x-api-key` and `Authorization` (Bearer token) headers and returns
/// an Identity object if either is present.
fn extract_identity(headers: &HeaderMap) -> Option<Box<Identity>> {
    let api_key = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let auth_token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            // Extract token from "Bearer <token>" format
            s.strip_prefix("Bearer ")
                .or_else(|| s.strip_prefix("bearer "))
                .map(|t| t.to_string())
        });

    if api_key.is_some() || auth_token.is_some() {
        Some(Box::new(Identity {
            api_key,
            auth_token,
        }))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_id_default_delimiter() {
        let id = parse_id("ns1$ns2$table", None);
        assert_eq!(id, vec!["ns1", "ns2", "table"]);
    }

    #[test]
    fn test_parse_id_custom_delimiter() {
        let id = parse_id("ns1/ns2/table", Some("/"));
        assert_eq!(id, vec!["ns1", "ns2", "table"]);
    }

    #[test]
    fn test_parse_id_single_part() {
        let id = parse_id("table", None);
        assert_eq!(id, vec!["table"]);
    }

    #[test]
    fn test_parse_id_root_namespace() {
        // When ID equals delimiter, it represents root namespace
        let id = parse_id("$", None);
        assert_eq!(id, Vec::<String>::new());

        let id = parse_id("/", Some("/"));
        assert_eq!(id, Vec::<String>::new());
    }

    #[test]
    fn test_parse_id_filters_empty() {
        // Filter out empty strings from split results
        let id = parse_id("$$table$$", None);
        assert_eq!(id, vec!["table"]);
    }

    // ============================================================================
    // Integration Tests
    // ============================================================================

    #[cfg(feature = "rest")]
    mod integration {
        use super::super::*;
        use crate::{DirectoryNamespaceBuilder, RestNamespaceBuilder};
        use std::sync::Arc;
        use tempfile::TempDir;

        /// Test fixture that manages server lifecycle
        struct RestServerFixture {
            _temp_dir: TempDir,
            namespace: crate::RestNamespace,
            server_handle: RestAdapterHandle,
        }

        impl RestServerFixture {
            async fn new() -> Self {
                Self::build(false).await
            }

            /// Like [`Self::new`], with managed versioning (table version
            /// tracking) enabled on the backend.
            async fn new_managed() -> Self {
                Self::build(true).await
            }

            async fn build(managed_versioning: bool) -> Self {
                let temp_dir = TempDir::new().unwrap();
                let temp_path = temp_dir.path().to_str().unwrap().to_string();

                // Create DirectoryNamespace backend with manifest enabled
                let mut builder = DirectoryNamespaceBuilder::new(&temp_path).manifest_enabled(true);
                if managed_versioning {
                    builder = builder.table_version_tracking_enabled(true);
                }
                let backend = builder.build().await.unwrap();
                let backend = Arc::new(backend);

                // Start REST server with port 0 (OS assigns available port)
                let config = RestAdapterConfig {
                    port: 0,
                    ..Default::default()
                };

                let server = RestAdapter::new(backend.clone(), config);
                let server_handle = server.start().await.unwrap();

                // Get the actual port assigned by OS
                let actual_port = server_handle.port();

                // Create RestNamespace client
                let server_url = format!("http://127.0.0.1:{}", actual_port);
                let namespace = RestNamespaceBuilder::new(&server_url)
                    .delimiter("$")
                    .build();

                Self {
                    _temp_dir: temp_dir,
                    namespace,
                    server_handle,
                }
            }
        }

        impl Drop for RestServerFixture {
            fn drop(&mut self) {
                self.server_handle.shutdown();
            }
        }

        /// Helper to create Arrow IPC data for testing
        fn create_test_arrow_data() -> Bytes {
            use arrow::array::{Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::ipc::writer::StreamWriter;
            use arrow::record_batch::RecordBatch;

            let schema = Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
            ]);

            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
                ],
            )
            .unwrap();

            let mut buffer = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema()).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }

            Bytes::from(buffer)
        }

        /// Helper to create Arrow IPC data with vector column for testing vector index
        fn create_test_vector_data(num_rows: usize, dim: i32) -> Bytes {
            use arrow::array::{FixedSizeListArray, Float32Array, Int32Array};
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::ipc::writer::StreamWriter;
            use arrow::record_batch::RecordBatch;

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "vector",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        dim,
                    ),
                    true,
                ),
            ]));

            let ids: Vec<i32> = (0..num_rows as i32).collect();
            let vector_values: Vec<f32> = (0..(num_rows * dim as usize))
                .map(|i| (i as f32) * 0.01)
                .collect();

            let vector_field = Arc::new(Field::new("item", DataType::Float32, true));
            let vectors = FixedSizeListArray::try_new(
                vector_field,
                dim,
                Arc::new(Float32Array::from(vector_values)),
                None,
            )
            .unwrap();

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(ids)), Arc::new(vectors)],
            )
            .unwrap();

            let mut buffer = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }

            Bytes::from(buffer)
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_trailing_slash_handling() {
            let fixture = RestServerFixture::new().await;
            let port = fixture.server_handle.port();

            // Create a namespace using the normal API (without trailing slash)
            let create_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_req)
                .await
                .unwrap();

            // Test that a request with trailing slash works (using direct HTTP)
            let client = reqwest::Client::new();

            // Test POST endpoint with trailing slash
            let response = client
                .post(format!(
                    "http://127.0.0.1:{}/v1/namespace/test_namespace/exists/",
                    port
                ))
                .json(&serde_json::json!({}))
                .send()
                .await
                .unwrap();

            assert_eq!(
                response.status(),
                204,
                "POST request with trailing slash should succeed with 204 No Content"
            );

            // Test GET endpoint with trailing slash
            let response = client
                .get(format!(
                    "http://127.0.0.1:{}/v1/namespace/test_namespace/list/",
                    port
                ))
                .send()
                .await
                .unwrap();

            assert!(
                response.status().is_success(),
                "GET request with trailing slash should succeed, got status: {}",
                response.status()
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_create_and_list_child_namespaces() {
            let fixture = RestServerFixture::new().await;

            // Create child namespaces
            for i in 1..=3 {
                let create_req = CreateNamespaceRequest {
                    id: Some(vec![format!("namespace{}", i)]),
                    properties: None,
                    mode: None,
                    ..Default::default()
                };
                let result = fixture.namespace.create_namespace(create_req).await;
                assert!(result.is_ok(), "Failed to create namespace{}", i);
            }

            // List child namespaces
            let list_req = ListNamespacesRequest {
                id: Some(vec![]),
                page_token: None,
                limit: None,
                ..Default::default()
            };
            let result = fixture.namespace.list_namespaces(list_req).await;
            assert!(result.is_ok());
            let namespaces = result.unwrap();
            assert_eq!(namespaces.namespaces.len(), 3);
            assert!(namespaces.namespaces.contains(&"namespace1".to_string()));
            assert!(namespaces.namespaces.contains(&"namespace2".to_string()));
            assert!(namespaces.namespaces.contains(&"namespace3".to_string()));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_nested_namespace_hierarchy() {
            let fixture = RestServerFixture::new().await;

            // Create parent namespace
            let create_req = CreateNamespaceRequest {
                id: Some(vec!["parent".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_req)
                .await
                .unwrap();

            // Create nested child namespaces
            let create_req = CreateNamespaceRequest {
                id: Some(vec!["parent".to_string(), "child1".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_req)
                .await
                .unwrap();

            let create_req = CreateNamespaceRequest {
                id: Some(vec!["parent".to_string(), "child2".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_req)
                .await
                .unwrap();

            // List children of parent
            let list_req = ListNamespacesRequest {
                id: Some(vec!["parent".to_string()]),
                page_token: None,
                limit: None,
                ..Default::default()
            };
            let result = fixture.namespace.list_namespaces(list_req).await;
            assert!(result.is_ok());
            let children = result.unwrap().namespaces;
            assert_eq!(children.len(), 2);
            assert!(children.contains(&"child1".to_string()));
            assert!(children.contains(&"child2".to_string()));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_create_table_in_child_namespace() {
            let fixture = RestServerFixture::new().await;
            let table_data = create_test_arrow_data();

            // Create child namespace first
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Create table in child namespace
            let create_table_req = CreateTableRequest {
                id: Some(vec!["test_namespace".to_string(), "test_table".to_string()]),
                mode: Some("Create".to_string()),
                ..Default::default()
            };

            let result = fixture
                .namespace
                .create_table(create_table_req, table_data)
                .await;

            assert!(
                result.is_ok(),
                "Failed to create table in child namespace: {:?}",
                result.err()
            );

            // Check response details
            let response = result.unwrap();
            assert!(
                response.location.is_some(),
                "Response should include location"
            );
            assert!(
                response.location.unwrap().contains("test_table"),
                "Location should contain table name"
            );
            assert_eq!(
                response.version,
                Some(1),
                "Initial table version should be 1"
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_list_tables_in_child_namespace() {
            let fixture = RestServerFixture::new().await;
            let table_data = create_test_arrow_data();

            // Create child namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Create multiple tables in the namespace
            for i in 1..=3 {
                let create_table_req = CreateTableRequest {
                    id: Some(vec!["test_namespace".to_string(), format!("table{}", i)]),
                    mode: Some("Create".to_string()),
                    ..Default::default()
                };
                fixture
                    .namespace
                    .create_table(create_table_req, table_data.clone())
                    .await
                    .unwrap();
            }

            // List tables in the namespace
            let list_req = ListTablesRequest {
                id: Some(vec!["test_namespace".to_string()]),
                page_token: None,
                limit: None,
                ..Default::default()
            };
            let result = fixture.namespace.list_tables(list_req).await;
            assert!(result.is_ok());
            let tables = result.unwrap();
            assert_eq!(tables.tables.len(), 3);
            assert!(tables.tables.contains(&"table1".to_string()));
            assert!(tables.tables.contains(&"table2".to_string()));
            assert!(tables.tables.contains(&"table3".to_string()));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_table_exists_in_child_namespace() {
            let fixture = RestServerFixture::new().await;
            let table_data = create_test_arrow_data();

            // Create child namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Create table
            let create_table_req = CreateTableRequest {
                id: Some(vec!["test_namespace".to_string(), "test_table".to_string()]),
                mode: Some("Create".to_string()),
                ..Default::default()
            };
            fixture
                .namespace
                .create_table(create_table_req, table_data)
                .await
                .unwrap();

            // Check table exists
            let mut exists_req = TableExistsRequest::new();
            exists_req.id = Some(vec!["test_namespace".to_string(), "test_table".to_string()]);
            let result = fixture.namespace.table_exists(exists_req).await;
            assert!(result.is_ok(), "Table should exist in child namespace");
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_declared_table_exists_in_child_namespace() {
            let fixture = RestServerFixture::new().await;

            // Create child namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Declare table
            let declare_req = DeclareTableRequest {
                id: Some(vec!["test_namespace".to_string(), "test_table".to_string()]),
                ..Default::default()
            };
            fixture.namespace.declare_table(declare_req).await.unwrap();

            // Check table exists
            let mut exists_req = TableExistsRequest::new();
            exists_req.id = Some(vec!["test_namespace".to_string(), "test_table".to_string()]);
            let result = fixture.namespace.table_exists(exists_req).await;
            assert!(
                result.is_ok(),
                "Declared table should exist in child namespace"
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_describe_table_in_child_namespace() {
            let fixture = RestServerFixture::new().await;
            let table_data = create_test_arrow_data();

            // Create child namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Create table
            let create_table_req = CreateTableRequest {
                id: Some(vec!["test_namespace".to_string(), "test_table".to_string()]),
                mode: Some("Create".to_string()),
                ..Default::default()
            };
            fixture
                .namespace
                .create_table(create_table_req, table_data)
                .await
                .unwrap();

            // Describe the table
            let mut describe_req = DescribeTableRequest::new();
            describe_req.id = Some(vec!["test_namespace".to_string(), "test_table".to_string()]);
            let result = fixture.namespace.describe_table(describe_req).await;

            assert!(
                result.is_ok(),
                "Failed to describe table in child namespace: {:?}",
                result.err()
            );
            let response = result.unwrap();

            // Check location
            assert!(
                response.location.is_some(),
                "Response should include location"
            );
            let location = response.location.unwrap();
            assert!(
                location.contains("test_table"),
                "Location should contain table name"
            );

            // Check version (might be None for empty datasets in some implementations)
            // When version is present, it should be 1 for the first version
            if let Some(version) = response.version {
                assert_eq!(version, 1, "First table version should be 1");
            }

            // Check schema (if available)
            if let Some(schema) = response.schema {
                assert_eq!(schema.fields.len(), 2, "Schema should have 2 fields");

                // Verify field names and types
                let field_names: Vec<&str> =
                    schema.fields.iter().map(|f| f.name.as_str()).collect();
                assert!(field_names.contains(&"id"), "Schema should have 'id' field");
                assert!(
                    field_names.contains(&"name"),
                    "Schema should have 'name' field"
                );

                let id_field = schema.fields.iter().find(|f| f.name == "id").unwrap();
                assert_eq!(
                    id_field.r#type.r#type.to_lowercase(),
                    "int32",
                    "id field should be int32"
                );
                assert!(!id_field.nullable, "id field should be non-nullable");

                let name_field = schema.fields.iter().find(|f| f.name == "name").unwrap();
                assert_eq!(
                    name_field.r#type.r#type.to_lowercase(),
                    "utf8",
                    "name field should be utf8"
                );
                assert!(!name_field.nullable, "name field should be non-nullable");
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_drop_table_in_child_namespace() {
            let fixture = RestServerFixture::new().await;
            let table_data = create_test_arrow_data();

            // Create child namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Create table
            let create_table_req = CreateTableRequest {
                id: Some(vec!["test_namespace".to_string(), "test_table".to_string()]),
                mode: Some("Create".to_string()),
                ..Default::default()
            };
            fixture
                .namespace
                .create_table(create_table_req, table_data)
                .await
                .unwrap();

            // Drop the table
            let drop_req = DropTableRequest {
                id: Some(vec!["test_namespace".to_string(), "test_table".to_string()]),
                ..Default::default()
            };
            let result = fixture.namespace.drop_table(drop_req).await;
            assert!(
                result.is_ok(),
                "Failed to drop table in child namespace: {:?}",
                result.err()
            );

            // Verify table no longer exists
            let mut exists_req = TableExistsRequest::new();
            exists_req.id = Some(vec!["test_namespace".to_string(), "test_table".to_string()]);
            let result = fixture.namespace.table_exists(exists_req).await;
            assert!(result.is_err(), "Table should not exist after drop");
            // After drop, accessing the table should fail
            // (error message varies depending on implementation details)
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_describe_declared_table_in_child_namespace() {
            let fixture = RestServerFixture::new().await;

            // Create child namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Declare table
            let declare_req = DeclareTableRequest {
                id: Some(vec!["test_namespace".to_string(), "test_table".to_string()]),
                ..Default::default()
            };
            fixture.namespace.declare_table(declare_req).await.unwrap();

            // Describe the declared table
            let mut describe_req = DescribeTableRequest::new();
            describe_req.id = Some(vec!["test_namespace".to_string(), "test_table".to_string()]);
            let result = fixture.namespace.describe_table(describe_req).await;

            assert!(
                result.is_ok(),
                "Failed to describe declared table in child namespace: {:?}",
                result.err()
            );
            let response = result.unwrap();

            // Check location
            assert!(
                response.location.is_some(),
                "Response should include location"
            );
            let location = response.location.unwrap();
            assert!(
                location.contains("test_table"),
                "Location should contain table name"
            );
            assert_eq!(response.is_only_declared, None);

            let mut describe_req = DescribeTableRequest::new();
            describe_req.id = Some(vec!["test_namespace".to_string(), "test_table".to_string()]);
            describe_req.check_declared = Some(true);
            let response = fixture
                .namespace
                .describe_table(describe_req)
                .await
                .expect("Should describe declared table with check_declared");
            assert_eq!(response.is_only_declared, Some(true));

            // Declared tables don't have a version until data is written
            // (version is None for declared tables)

            // Declared tables don't have a schema initially
            // (schema is None until data is added)
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_drop_declared_table_in_child_namespace() {
            let fixture = RestServerFixture::new().await;

            // Create child namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Declare table
            let declare_req = DeclareTableRequest {
                id: Some(vec!["test_namespace".to_string(), "test_table".to_string()]),
                ..Default::default()
            };
            fixture.namespace.declare_table(declare_req).await.unwrap();

            // Drop the empty table
            let drop_req = DropTableRequest {
                id: Some(vec!["test_namespace".to_string(), "test_table".to_string()]),
                ..Default::default()
            };
            let result = fixture.namespace.drop_table(drop_req).await;
            assert!(
                result.is_ok(),
                "Failed to drop empty table in child namespace: {:?}",
                result.err()
            );

            // Verify table no longer exists
            let mut exists_req = TableExistsRequest::new();
            exists_req.id = Some(vec!["test_namespace".to_string(), "test_table".to_string()]);
            let result = fixture.namespace.table_exists(exists_req).await;
            assert!(
                result.is_err(),
                "Declared table should not exist after drop"
            );
            // After drop, accessing the table should fail
            // (error message varies depending on implementation details)
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_deeply_nested_namespace_with_declared_table() {
            let fixture = RestServerFixture::new().await;

            // Create deeply nested namespace hierarchy
            let create_req = CreateNamespaceRequest {
                id: Some(vec!["level1".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_req)
                .await
                .unwrap();

            let create_req = CreateNamespaceRequest {
                id: Some(vec!["level1".to_string(), "level2".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_req)
                .await
                .unwrap();

            let create_req = CreateNamespaceRequest {
                id: Some(vec![
                    "level1".to_string(),
                    "level2".to_string(),
                    "level3".to_string(),
                ]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_req)
                .await
                .unwrap();

            // Declare table in deeply nested namespace
            let declare_req = DeclareTableRequest {
                id: Some(vec![
                    "level1".to_string(),
                    "level2".to_string(),
                    "level3".to_string(),
                    "deep_table".to_string(),
                ]),
                ..Default::default()
            };

            let result = fixture.namespace.declare_table(declare_req).await;

            assert!(
                result.is_ok(),
                "Failed to declare table in deeply nested namespace"
            );

            // Verify table exists
            let mut exists_req = TableExistsRequest::new();
            exists_req.id = Some(vec![
                "level1".to_string(),
                "level2".to_string(),
                "level3".to_string(),
                "deep_table".to_string(),
            ]);
            let result = fixture.namespace.table_exists(exists_req).await;
            assert!(
                result.is_ok(),
                "Declared table should exist in deeply nested namespace"
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_deeply_nested_namespace_with_table() {
            let fixture = RestServerFixture::new().await;
            let table_data = create_test_arrow_data();

            // Create deeply nested namespace hierarchy
            let create_req = CreateNamespaceRequest {
                id: Some(vec!["level1".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_req)
                .await
                .unwrap();

            let create_req = CreateNamespaceRequest {
                id: Some(vec!["level1".to_string(), "level2".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_req)
                .await
                .unwrap();

            let create_req = CreateNamespaceRequest {
                id: Some(vec![
                    "level1".to_string(),
                    "level2".to_string(),
                    "level3".to_string(),
                ]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_req)
                .await
                .unwrap();

            // Create table in deeply nested namespace
            let create_table_req = CreateTableRequest {
                id: Some(vec![
                    "level1".to_string(),
                    "level2".to_string(),
                    "level3".to_string(),
                    "deep_table".to_string(),
                ]),
                mode: Some("Create".to_string()),
                ..Default::default()
            };

            let result = fixture
                .namespace
                .create_table(create_table_req, table_data)
                .await;

            assert!(
                result.is_ok(),
                "Failed to create table in deeply nested namespace"
            );

            // Verify table exists
            let mut exists_req = TableExistsRequest::new();
            exists_req.id = Some(vec![
                "level1".to_string(),
                "level2".to_string(),
                "level3".to_string(),
                "deep_table".to_string(),
            ]);
            let result = fixture.namespace.table_exists(exists_req).await;
            assert!(
                result.is_ok(),
                "Table should exist in deeply nested namespace"
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_namespace_isolation() {
            let fixture = RestServerFixture::new().await;
            let table_data = create_test_arrow_data();

            // Create two sibling namespaces
            let create_req = CreateNamespaceRequest {
                id: Some(vec!["namespace1".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_req)
                .await
                .unwrap();

            let create_req = CreateNamespaceRequest {
                id: Some(vec!["namespace2".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_req)
                .await
                .unwrap();

            // Create table with same name in both namespaces
            let create_table_req = CreateTableRequest {
                id: Some(vec!["namespace1".to_string(), "shared_table".to_string()]),
                mode: Some("Create".to_string()),
                ..Default::default()
            };
            fixture
                .namespace
                .create_table(create_table_req, table_data.clone())
                .await
                .unwrap();

            let create_table_req = CreateTableRequest {
                id: Some(vec!["namespace2".to_string(), "shared_table".to_string()]),
                mode: Some("Create".to_string()),
                ..Default::default()
            };
            fixture
                .namespace
                .create_table(create_table_req, table_data)
                .await
                .unwrap();

            // Drop table in namespace1
            let drop_req = DropTableRequest {
                id: Some(vec!["namespace1".to_string(), "shared_table".to_string()]),
                ..Default::default()
            };
            fixture.namespace.drop_table(drop_req).await.unwrap();

            // Verify namespace1 table is gone but namespace2 table still exists
            let mut exists_req = TableExistsRequest::new();
            exists_req.id = Some(vec!["namespace1".to_string(), "shared_table".to_string()]);
            let result = fixture.namespace.table_exists(exists_req).await;
            assert!(
                result.is_err(),
                "Table in namespace1 should not exist after drop"
            );
            // After drop, accessing the table should fail
            // (error message varies depending on implementation details)

            let mut exists_req = TableExistsRequest::new();
            exists_req.id = Some(vec!["namespace2".to_string(), "shared_table".to_string()]);
            assert!(fixture.namespace.table_exists(exists_req).await.is_ok());
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_drop_namespace_with_tables_fails() {
            let fixture = RestServerFixture::new().await;
            let table_data = create_test_arrow_data();

            // Create namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Create table in namespace
            let create_table_req = CreateTableRequest {
                id: Some(vec!["test_namespace".to_string(), "test_table".to_string()]),
                mode: Some("Create".to_string()),
                ..Default::default()
            };
            fixture
                .namespace
                .create_table(create_table_req, table_data)
                .await
                .unwrap();

            // Try to drop namespace with table - should fail
            let mut drop_req = DropNamespaceRequest::new();
            drop_req.id = Some(vec!["test_namespace".to_string()]);
            let result = fixture.namespace.drop_namespace(drop_req).await;
            assert!(
                result.is_err(),
                "Should not be able to drop namespace with tables"
            );
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("not empty"),
                "Error should contain 'not empty', got: {}",
                err_msg
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_drop_empty_child_namespace() {
            let fixture = RestServerFixture::new().await;

            // Create namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Drop empty namespace - should succeed
            let mut drop_req = DropNamespaceRequest::new();
            drop_req.id = Some(vec!["test_namespace".to_string()]);
            let result = fixture.namespace.drop_namespace(drop_req).await;
            assert!(
                result.is_ok(),
                "Should be able to drop empty child namespace"
            );

            // Verify namespace no longer exists
            let exists_req = NamespaceExistsRequest {
                id: Some(vec!["test_namespace".to_string()]),
                ..Default::default()
            };
            let result = fixture.namespace.namespace_exists(exists_req).await;
            assert!(result.is_err(), "Namespace should not exist after drop");
            // After drop, namespace should not be found
            // (error message varies depending on implementation details)
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_namespace_with_properties() {
            let fixture = RestServerFixture::new().await;

            // Create namespace with properties
            let mut properties = std::collections::HashMap::new();
            properties.insert("owner".to_string(), "test_user".to_string());
            properties.insert("environment".to_string(), "production".to_string());

            let create_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: Some(properties.clone()),
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_req)
                .await
                .unwrap();

            // Describe namespace and verify properties
            let describe_req = DescribeNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                ..Default::default()
            };
            let result = fixture.namespace.describe_namespace(describe_req).await;
            assert!(result.is_ok());
            let response = result.unwrap();
            assert!(response.properties.is_some());
            let props = response.properties.unwrap();
            assert_eq!(props.get("owner"), Some(&"test_user".to_string()));
            assert_eq!(props.get("environment"), Some(&"production".to_string()));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_root_namespace_operations() {
            let fixture = RestServerFixture::new().await;

            // Root namespace should always exist
            let exists_req = NamespaceExistsRequest {
                id: Some(vec![]),
                ..Default::default()
            };
            let result = fixture.namespace.namespace_exists(exists_req).await;
            assert!(result.is_ok(), "Root namespace should exist");

            // Cannot create root namespace
            let create_req = CreateNamespaceRequest {
                id: Some(vec![]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            let result = fixture.namespace.create_namespace(create_req).await;
            assert!(result.is_err(), "Cannot create root namespace");
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("already exists") && err_msg.contains("root namespace"),
                "Error should contain 'already exists' and 'root namespace', got: {}",
                err_msg
            );

            // Cannot drop root namespace
            let mut drop_req = DropNamespaceRequest::new();
            drop_req.id = Some(vec![]);
            let result = fixture.namespace.drop_namespace(drop_req).await;
            assert!(result.is_err(), "Cannot drop root namespace");
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("Root namespace cannot be dropped"),
                "Error should be 'Root namespace cannot be dropped', got: {}",
                err_msg
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_register_table() {
            let fixture = RestServerFixture::new().await;
            let table_data = create_test_arrow_data();

            // Create child namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Create a physical table using create_table
            let create_table_req = CreateTableRequest {
                id: Some(vec![
                    "test_namespace".to_string(),
                    "physical_table".to_string(),
                ]),
                mode: Some("Create".to_string()),
                ..Default::default()
            };
            fixture
                .namespace
                .create_table(create_table_req, table_data)
                .await
                .unwrap();

            // Register another table pointing to a relative path
            let register_req = RegisterTableRequest {
                id: Some(vec![
                    "test_namespace".to_string(),
                    "registered_table".to_string(),
                ]),
                location: "test_namespace$physical_table.lance".to_string(),
                mode: None,
                properties: None,
                ..Default::default()
            };

            let result = fixture.namespace.register_table(register_req).await;
            assert!(
                result.is_ok(),
                "Failed to register table: {:?}",
                result.err()
            );

            let response = result.unwrap();
            assert_eq!(
                response.location,
                Some("test_namespace$physical_table.lance".to_string())
            );

            // Verify registered table exists
            let mut exists_req = TableExistsRequest::new();
            exists_req.id = Some(vec![
                "test_namespace".to_string(),
                "registered_table".to_string(),
            ]);
            let result = fixture.namespace.table_exists(exists_req).await;
            assert!(result.is_ok(), "Registered table should exist");
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_register_table_rejects_absolute_uri() {
            let fixture = RestServerFixture::new().await;

            // Create child namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Try to register with absolute URI - should fail
            let register_req = RegisterTableRequest {
                id: Some(vec!["test_namespace".to_string(), "bad_table".to_string()]),
                location: "s3://bucket/table.lance".to_string(),
                mode: None,
                properties: None,
                ..Default::default()
            };

            let result = fixture.namespace.register_table(register_req).await;
            assert!(result.is_err(), "Should reject absolute URI");
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("Absolute URIs are not allowed"),
                "Error should mention absolute URIs, got: {}",
                err_msg
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_register_table_rejects_path_traversal() {
            let fixture = RestServerFixture::new().await;

            // Create child namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Try to register with path traversal - should fail
            let register_req = RegisterTableRequest {
                id: Some(vec!["test_namespace".to_string(), "bad_table".to_string()]),
                location: "../outside/table.lance".to_string(),
                mode: None,
                properties: None,
                ..Default::default()
            };

            let result = fixture.namespace.register_table(register_req).await;
            assert!(result.is_err(), "Should reject path traversal");
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("Path traversal is not allowed"),
                "Error should mention path traversal, got: {}",
                err_msg
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_deregister_table() {
            let fixture = RestServerFixture::new().await;
            let table_data = create_test_arrow_data();

            // Create child namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Create a table
            let create_table_req = CreateTableRequest {
                id: Some(vec!["test_namespace".to_string(), "test_table".to_string()]),
                mode: Some("Create".to_string()),
                ..Default::default()
            };
            fixture
                .namespace
                .create_table(create_table_req, table_data)
                .await
                .unwrap();

            // Verify table exists
            let mut exists_req = TableExistsRequest::new();
            exists_req.id = Some(vec!["test_namespace".to_string(), "test_table".to_string()]);
            assert!(
                fixture
                    .namespace
                    .table_exists(exists_req.clone())
                    .await
                    .is_ok()
            );

            // Deregister the table
            let deregister_req = DeregisterTableRequest {
                id: Some(vec!["test_namespace".to_string(), "test_table".to_string()]),
                ..Default::default()
            };
            let result = fixture.namespace.deregister_table(deregister_req).await;
            assert!(
                result.is_ok(),
                "Failed to deregister table: {:?}",
                result.err()
            );

            let response = result.unwrap();

            // Should return exact location and id
            assert!(
                response.location.is_some(),
                "Deregister response should include location"
            );
            let location = response.location.unwrap();
            assert!(
                location.ends_with("test_namespace$test_table"),
                "Location should end with test_namespace$test_table, got: {}",
                location
            );
            assert_eq!(
                response.id,
                Some(vec!["test_namespace".to_string(), "test_table".to_string()])
            );

            // Verify physical data still exists at the location
            let dataset = lance::Dataset::open(&location).await;
            assert!(
                dataset.is_ok(),
                "Physical table data should still exist at {}",
                location
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_register_deregister_round_trip() {
            let fixture = RestServerFixture::new().await;
            let table_data = create_test_arrow_data();

            // Create child namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_namespace".to_string()]),
                properties: None,
                mode: None,
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Create a physical table
            let create_table_req = CreateTableRequest {
                id: Some(vec![
                    "test_namespace".to_string(),
                    "original_table".to_string(),
                ]),
                mode: Some("Create".to_string()),
                ..Default::default()
            };
            let create_response = fixture
                .namespace
                .create_table(create_table_req, table_data.clone())
                .await
                .unwrap();

            // Deregister it
            let deregister_req = DeregisterTableRequest {
                id: Some(vec![
                    "test_namespace".to_string(),
                    "original_table".to_string(),
                ]),
                ..Default::default()
            };
            fixture
                .namespace
                .deregister_table(deregister_req)
                .await
                .unwrap();

            // Re-register with a different name
            let location = create_response
                .location
                .as_ref()
                .and_then(|loc| loc.strip_prefix(fixture.namespace.endpoint()))
                .unwrap_or(create_response.location.as_ref().unwrap())
                .to_string();

            let relative_location = location
                .split('/')
                .next_back()
                .unwrap_or(&location)
                .to_string();

            let register_req = RegisterTableRequest {
                id: Some(vec![
                    "test_namespace".to_string(),
                    "renamed_table".to_string(),
                ]),
                location: relative_location.clone(),
                mode: None,
                properties: None,
                ..Default::default()
            };

            let register_response = fixture
                .namespace
                .register_table(register_req)
                .await
                .expect("Failed to re-register table with new name");

            // Should return the exact location we registered
            assert_eq!(register_response.location, Some(relative_location.clone()));

            // Verify new table exists
            let mut exists_req = TableExistsRequest::new();
            exists_req.id = Some(vec![
                "test_namespace".to_string(),
                "renamed_table".to_string(),
            ]);
            let result = fixture.namespace.table_exists(exists_req).await;
            assert!(result.is_ok(), "Re-registered table should exist");

            // Verify both tables point to the same physical location
            let mut describe_req = DescribeTableRequest::new();
            describe_req.id = Some(vec![
                "test_namespace".to_string(),
                "renamed_table".to_string(),
            ]);
            let describe_response = fixture
                .namespace
                .describe_table(describe_req)
                .await
                .expect("Should be able to describe renamed table");

            // Location should end with the physical table path (same as original)
            assert!(
                describe_response
                    .location
                    .as_ref()
                    .map(|loc| loc.ends_with(&relative_location))
                    .unwrap_or(false),
                "Renamed table should point to original physical location {}, got: {:?}",
                relative_location,
                describe_response.location
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_namespace_write() {
            use arrow::array::Int32Array;
            use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
            use arrow::record_batch::{RecordBatch, RecordBatchIterator};
            use lance::dataset::{Dataset, WriteMode, WriteParams};
            use lance_namespace::LanceNamespace;

            let fixture = RestServerFixture::new().await;
            let namespace = Arc::new(fixture.namespace.clone()) as Arc<dyn LanceNamespace>;

            // Use child namespace instead of root
            let table_id = vec!["test_ns".to_string(), "test_table".to_string()];
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("a", DataType::Int32, false),
                ArrowField::new("b", DataType::Int32, false),
            ]));

            // Test 1: CREATE mode
            let data1 = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(Int32Array::from(vec![10, 20, 30])),
                ],
            )
            .unwrap();

            let reader1 = RecordBatchIterator::new(vec![data1].into_iter().map(Ok), schema.clone());
            let dataset =
                Dataset::write_into_namespace(reader1, namespace.clone(), table_id.clone(), None)
                    .await
                    .unwrap();

            assert_eq!(dataset.count_rows(None).await.unwrap(), 3);
            assert_eq!(dataset.version().version, 1);

            // Test 2: APPEND mode
            let data2 = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![4, 5])),
                    Arc::new(Int32Array::from(vec![40, 50])),
                ],
            )
            .unwrap();

            let params_append = WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            };

            let reader2 = RecordBatchIterator::new(vec![data2].into_iter().map(Ok), schema.clone());
            let dataset = Dataset::write_into_namespace(
                reader2,
                namespace.clone(),
                table_id.clone(),
                Some(params_append),
            )
            .await
            .unwrap();

            assert_eq!(dataset.count_rows(None).await.unwrap(), 5);
            assert_eq!(dataset.version().version, 2);

            // Test 3: OVERWRITE mode
            let data3 = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![100, 200])),
                    Arc::new(Int32Array::from(vec![1000, 2000])),
                ],
            )
            .unwrap();

            let params_overwrite = WriteParams {
                mode: WriteMode::Overwrite,
                ..Default::default()
            };

            let reader3 = RecordBatchIterator::new(vec![data3].into_iter().map(Ok), schema.clone());
            let dataset = Dataset::write_into_namespace(
                reader3,
                namespace.clone(),
                table_id.clone(),
                Some(params_overwrite),
            )
            .await
            .unwrap();

            assert_eq!(dataset.count_rows(None).await.unwrap(), 2);
            assert_eq!(dataset.version().version, 3);

            // Verify old data was replaced
            let result = dataset.scan().try_into_batch().await.unwrap();
            let a_col = result
                .column_by_name("a")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(a_col.values(), &[100, 200]);
        }

        // ============================================================================
        // DynamicContextProvider Integration Test
        // ============================================================================

        use crate::context::{DynamicContextProvider, OperationInfo};
        use std::collections::HashMap;

        /// Test context provider that adds custom headers to every request.
        #[derive(Debug)]
        struct TestDynamicContextProvider {
            headers: HashMap<String, String>,
        }

        impl DynamicContextProvider for TestDynamicContextProvider {
            fn provide_context(&self, _info: &OperationInfo) -> HashMap<String, String> {
                self.headers.clone()
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_rest_namespace_with_context_provider() {
            let temp_dir = TempDir::new().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap().to_string();

            // Create DirectoryNamespace backend with manifest enabled
            let backend = DirectoryNamespaceBuilder::new(&temp_path)
                .manifest_enabled(true)
                .build()
                .await
                .unwrap();
            let backend = Arc::new(backend);

            // Start REST server
            let config = RestAdapterConfig {
                port: 0,
                ..Default::default()
            };

            let server = RestAdapter::new(backend.clone(), config);
            let server_handle = server.start().await.unwrap();
            let actual_port = server_handle.port();

            // Create context provider that adds custom headers
            let mut context_headers = HashMap::new();
            context_headers.insert(
                "headers.X-Custom-Auth".to_string(),
                "test-auth-token".to_string(),
            );
            context_headers.insert(
                "headers.X-Request-Source".to_string(),
                "integration-test".to_string(),
            );

            let provider = Arc::new(TestDynamicContextProvider {
                headers: context_headers,
            });

            // Create RestNamespace client with context provider and base headers
            let server_url = format!("http://127.0.0.1:{}", actual_port);
            let namespace = RestNamespaceBuilder::new(&server_url)
                .delimiter("$")
                .header("X-Base-Header", "base-value")
                .context_provider(provider)
                .build();

            // Create a namespace - should work with context provider
            let create_req = CreateNamespaceRequest {
                id: Some(vec!["context_test_ns".to_string()]),
                properties: None,
                mode: None,
                identity: None,
                context: None,
            };
            let result = namespace.create_namespace(create_req).await;
            assert!(result.is_ok(), "Failed to create namespace: {:?}", result);

            // List namespaces - should also work
            let list_req = ListNamespacesRequest {
                id: Some(vec![]),
                limit: Some(10),
                page_token: None,
                identity: None,
                context: None,
            };
            let result = namespace.list_namespaces(list_req).await;
            assert!(result.is_ok(), "Failed to list namespaces: {:?}", result);
            let response = result.unwrap();
            assert!(
                response.namespaces.contains(&"context_test_ns".to_string()),
                "Namespace not found in list"
            );

            // Create a table - should work with context provider
            let table_data = create_test_arrow_data();
            let create_table_req = CreateTableRequest {
                id: Some(vec![
                    "context_test_ns".to_string(),
                    "test_table".to_string(),
                ]),
                mode: Some("create".to_string()),
                ..Default::default()
            };
            let result = namespace.create_table(create_table_req, table_data).await;
            assert!(result.is_ok(), "Failed to create table: {:?}", result);

            // Describe the table - should work with context provider
            let describe_req = DescribeTableRequest {
                id: Some(vec![
                    "context_test_ns".to_string(),
                    "test_table".to_string(),
                ]),
                ..Default::default()
            };
            let result = namespace.describe_table(describe_req).await;
            assert!(result.is_ok(), "Failed to describe table: {:?}", result);

            // Cleanup
            server_handle.shutdown();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_list_table_versions_with_descending() {
            let fixture = RestServerFixture::new().await;
            let table_data = create_test_arrow_data();

            // Create namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["version_test_ns".to_string()]),
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Create table
            let create_table_req = CreateTableRequest {
                id: Some(vec![
                    "version_test_ns".to_string(),
                    "version_table".to_string(),
                ]),
                mode: Some("create".to_string()),
                ..Default::default()
            };
            fixture
                .namespace
                .create_table(create_table_req, table_data)
                .await
                .unwrap();

            // List table versions (ascending by default)
            let list_req = ListTableVersionsRequest {
                id: Some(vec![
                    "version_test_ns".to_string(),
                    "version_table".to_string(),
                ]),
                descending: None,
                ..Default::default()
            };
            let result = fixture.namespace.list_table_versions(list_req).await;
            assert!(
                result.is_ok(),
                "Failed to list table versions: {:?}",
                result
            );
            let versions = result.unwrap();
            assert!(
                !versions.versions.is_empty(),
                "Should have at least one version"
            );

            // List table versions with descending=true
            let list_req = ListTableVersionsRequest {
                id: Some(vec![
                    "version_test_ns".to_string(),
                    "version_table".to_string(),
                ]),
                descending: Some(true),
                ..Default::default()
            };
            let result = fixture.namespace.list_table_versions(list_req).await;
            assert!(
                result.is_ok(),
                "Failed to list table versions with descending: {:?}",
                result
            );

            // List table versions with descending=false
            let list_req = ListTableVersionsRequest {
                id: Some(vec![
                    "version_test_ns".to_string(),
                    "version_table".to_string(),
                ]),
                descending: Some(false),
                ..Default::default()
            };
            let result = fixture.namespace.list_table_versions(list_req).await;
            assert!(
                result.is_ok(),
                "Failed to list table versions with ascending: {:?}",
                result
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_branch_param_forwarded_end_to_end() {
            let fixture = RestServerFixture::new().await;

            fixture
                .namespace
                .create_namespace(CreateNamespaceRequest {
                    id: Some(vec!["branch_fwd_ns".to_string()]),
                    ..Default::default()
                })
                .await
                .unwrap();
            fixture
                .namespace
                .create_table(
                    CreateTableRequest {
                        id: Some(vec![
                            "branch_fwd_ns".to_string(),
                            "branch_fwd_table".to_string(),
                        ]),
                        mode: Some("create".to_string()),
                        ..Default::default()
                    },
                    create_test_arrow_data(),
                )
                .await
                .unwrap();

            let id = vec!["branch_fwd_ns".to_string(), "branch_fwd_table".to_string()];

            // Control: no branch succeeds (resolves the main chain).
            assert!(
                fixture
                    .namespace
                    .list_table_versions(ListTableVersionsRequest {
                        id: Some(id.clone()),
                        ..Default::default()
                    })
                    .await
                    .is_ok()
            );

            // list forwards branch as a query param; a bogus branch 404s at the backend.
            assert!(
                fixture
                    .namespace
                    .list_table_versions(ListTableVersionsRequest {
                        id: Some(id.clone()),
                        branch: Some("ghost".to_string()),
                        ..Default::default()
                    })
                    .await
                    .is_err(),
                "branch must be forwarded as a query param and honored by the backend"
            );

            // describe carries branch in the request body; a bogus branch likewise 404s.
            assert!(
                fixture
                    .namespace
                    .describe_table_version(DescribeTableVersionRequest {
                        id: Some(id.clone()),
                        branch: Some("ghost".to_string()),
                        ..Default::default()
                    })
                    .await
                    .is_err(),
                "branch must be forwarded in the request body and honored by the backend"
            );

            fixture.server_handle.shutdown();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_branch_crud_end_to_end() {
            let fixture = RestServerFixture::new().await;

            fixture
                .namespace
                .create_namespace(CreateNamespaceRequest {
                    id: Some(vec!["branch_crud_ns".to_string()]),
                    ..Default::default()
                })
                .await
                .unwrap();
            fixture
                .namespace
                .create_table(
                    CreateTableRequest {
                        id: Some(vec![
                            "branch_crud_ns".to_string(),
                            "branch_crud_table".to_string(),
                        ]),
                        mode: Some("create".to_string()),
                        ..Default::default()
                    },
                    create_test_arrow_data(),
                )
                .await
                .unwrap();

            let id = vec![
                "branch_crud_ns".to_string(),
                "branch_crud_table".to_string(),
            ];

            // create -> list shows it (client -> server -> directory backend)
            fixture
                .namespace
                .create_table_branch(CreateTableBranchRequest {
                    id: Some(id.clone()),
                    name: "dev".to_string(),
                    ..Default::default()
                })
                .await
                .unwrap();

            let listed = fixture
                .namespace
                .list_table_branches(ListTableBranchesRequest {
                    id: Some(id.clone()),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert!(
                listed.branches.contains_key("dev"),
                "created branch should appear in list: {:?}",
                listed.branches
            );

            // duplicate create -> 409 Conflict
            let port = fixture.server_handle.port();
            let client = reqwest::Client::new();
            let table_path = "branch_crud_ns%24branch_crud_table";
            let resp = client
                .post(format!(
                    "http://127.0.0.1:{}/v1/table/{}/branches/create",
                    port, table_path
                ))
                .query(&[("delimiter", "$")])
                .json(&serde_json::json!({ "name": "dev" }))
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                409,
                "duplicate branch create should map to 409, got {}",
                resp.status()
            );

            // delete -> list no longer shows it
            fixture
                .namespace
                .delete_table_branch(DeleteTableBranchRequest {
                    id: Some(id.clone()),
                    name: "dev".to_string(),
                    ..Default::default()
                })
                .await
                .unwrap();

            let listed = fixture
                .namespace
                .list_table_branches(ListTableBranchesRequest {
                    id: Some(id.clone()),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert!(
                !listed.branches.contains_key("dev"),
                "deleted branch must not appear in list: {:?}",
                listed.branches
            );

            // delete missing -> 404 Not Found (raw HTTP validates TableBranchNotFound -> 404).
            let resp = client
                .post(format!(
                    "http://127.0.0.1:{}/v1/table/{}/branches/delete",
                    port, table_path
                ))
                .query(&[("delimiter", "$")])
                .json(&serde_json::json!({ "name": "dev" }))
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                404,
                "deleting a missing branch should map to 404, got {}",
                resp.status()
            );

            fixture.server_handle.shutdown();
        }

        /// The managed (manifest-tracked) branch flow over REST: create a
        /// managed table and a branch through the RestNamespace client, open
        /// the branch via `from_namespace(...).with_branch`, commit on it,
        /// check out across branches at an overlapping version number, and
        /// round-trip a branch-pointing tag. Mirrors the DirectoryNamespace
        /// e2e to prove the REST layer forwards everything the managed commit
        /// store needs.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_namespace_managed_branch_e2e() {
            use arrow::array::Int32Array;
            use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
            use arrow::record_batch::{RecordBatch, RecordBatchIterator};
            use futures::TryStreamExt;
            use lance::dataset::builder::DatasetBuilder;
            use lance::dataset::refs::Ref;
            use lance::dataset::{Dataset, WriteMode, WriteParams};
            use lance_namespace::LanceNamespace;

            async fn scan_ids(ds: &Dataset) -> Vec<i32> {
                let batches: Vec<RecordBatch> = ds
                    .scan()
                    .try_into_stream()
                    .await
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                let mut ids: Vec<i32> = batches
                    .iter()
                    .flat_map(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .values()
                            .to_vec()
                    })
                    .collect();
                ids.sort();
                ids
            }

            let fixture = RestServerFixture::new_managed().await;
            let namespace = Arc::new(fixture.namespace.clone()) as Arc<dyn LanceNamespace>;
            let table_id = vec!["mb_table".to_string()];

            let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "id",
                DataType::Int32,
                false,
            )]));
            let batch = |seed: i32| {
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![seed]))])
                    .unwrap()
            };

            // Managed main: v1 (id=1), v2 (id=2).
            let mut main_ds = Dataset::write_into_namespace(
                RecordBatchIterator::new(vec![Ok(batch(1))], schema.clone()),
                namespace.clone(),
                table_id.clone(),
                Some(WriteParams {
                    mode: WriteMode::Create,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
            main_ds
                .append(
                    RecordBatchIterator::new(vec![Ok(batch(2))], schema.clone()),
                    None,
                )
                .await
                .unwrap();

            // The REST layer must surface managed versioning for the deferred
            // commit handler to engage.
            let described = namespace
                .describe_table(DescribeTableRequest {
                    id: Some(table_id.clone()),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(
                described.managed_versioning,
                Some(true),
                "managed_versioning must survive the REST round trip"
            );
            let main_chain_len = |ns: Arc<dyn LanceNamespace>, table_id: Vec<String>| async move {
                ns.list_table_versions(ListTableVersionsRequest {
                    id: Some(table_id),
                    ..Default::default()
                })
                .await
                .unwrap()
                .versions
                .len()
            };
            assert_eq!(main_chain_len(namespace.clone(), table_id.clone()).await, 2);

            // Branch via the REST client, then open and commit on it.
            namespace
                .create_table_branch(CreateTableBranchRequest {
                    id: Some(table_id.clone()),
                    name: "exp".to_string(),
                    ..Default::default()
                })
                .await
                .unwrap();
            let mut branch_ds = DatasetBuilder::from_namespace(namespace.clone(), table_id.clone())
                .await
                .unwrap()
                .with_branch("exp", None)
                .load()
                .await
                .unwrap();
            assert_eq!(branch_ds.manifest().branch.as_deref(), Some("exp"));
            assert!(
                branch_ds
                    .branch_location()
                    .path
                    .as_ref()
                    .ends_with("tree/exp"),
                "the branch dataset must be rooted at the branch chain"
            );
            branch_ds
                .append(
                    RecordBatchIterator::new(vec![Ok(batch(3))], schema.clone()),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(branch_ds.manifest().branch.as_deref(), Some("exp"));
            assert_eq!(scan_ids(&branch_ds).await, vec![1, 2, 3]);
            assert_eq!(
                main_chain_len(namespace.clone(), table_id.clone()).await,
                2,
                "a branch commit must not advance main's chain"
            );

            // Cross-branch checkout at an overlapping version number must land
            // on the branch chain (branch numbering continues from the fork
            // point, so both chains have this version).
            let overlap_version = branch_ds.version().version;
            while main_ds.version().version < overlap_version {
                main_ds
                    .append(
                        RecordBatchIterator::new(vec![Ok(batch(100))], schema.clone()),
                        None,
                    )
                    .await
                    .unwrap();
            }
            let on_branch = main_ds
                .checkout_version(Ref::Version(Some("exp".to_string()), Some(overlap_version)))
                .await
                .unwrap();
            assert_eq!(on_branch.manifest().branch.as_deref(), Some("exp"));
            assert_eq!(scan_ids(&on_branch).await, vec![1, 2, 3]);
            let on_branch_latest = main_ds.checkout_branch("exp").await.unwrap();
            assert_eq!(on_branch_latest.manifest().branch.as_deref(), Some("exp"));

            // Branch-pointing tag round trip through the builder.
            main_ds
                .tags()
                .create("exp-tag", ("exp", Some(overlap_version)))
                .await
                .unwrap();
            let tag_open = DatasetBuilder::from_namespace(namespace.clone(), table_id.clone())
                .await
                .unwrap()
                .with_tag("exp-tag")
                .load()
                .await
                .unwrap();
            assert_eq!(tag_open.manifest().branch.as_deref(), Some("exp"));
            assert_eq!(tag_open.version().version, overlap_version);
            assert_eq!(scan_ids(&tag_open).await, vec![1, 2, 3]);

            fixture.server_handle.shutdown();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_list_branches_bodyless_post() {
            let fixture = RestServerFixture::new().await;

            fixture
                .namespace
                .create_namespace(CreateNamespaceRequest {
                    id: Some(vec!["list_post_ns".to_string()]),
                    ..Default::default()
                })
                .await
                .unwrap();
            fixture
                .namespace
                .create_table(
                    CreateTableRequest {
                        id: Some(vec![
                            "list_post_ns".to_string(),
                            "list_post_table".to_string(),
                        ]),
                        mode: Some("create".to_string()),
                        ..Default::default()
                    },
                    create_test_arrow_data(),
                )
                .await
                .unwrap();
            fixture
                .namespace
                .create_table_branch(CreateTableBranchRequest {
                    id: Some(vec![
                        "list_post_ns".to_string(),
                        "list_post_table".to_string(),
                    ]),
                    name: "dev".to_string(),
                    ..Default::default()
                })
                .await
                .unwrap();

            let port = fixture.server_handle.port();
            let client = reqwest::Client::new();
            let resp = client
                .post(format!(
                    "http://127.0.0.1:{}/v1/table/list_post_ns%24list_post_table/branches/list",
                    port
                ))
                .query(&[("delimiter", "$")])
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                200,
                "bodyless list POST should succeed, got {}",
                resp.status()
            );
            let body: ListTableBranchesResponse = resp.json().await.unwrap();
            assert!(
                body.branches.contains_key("dev"),
                "bodyless list should return the branch, got: {:?}",
                body.branches
            );

            fixture.server_handle.shutdown();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_describe_table_version() {
            let fixture = RestServerFixture::new().await;
            let table_data = create_test_arrow_data();

            // Create namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["describe_version_ns".to_string()]),
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Create table
            let create_table_req = CreateTableRequest {
                id: Some(vec![
                    "describe_version_ns".to_string(),
                    "describe_version_table".to_string(),
                ]),
                mode: Some("create".to_string()),
                ..Default::default()
            };
            fixture
                .namespace
                .create_table(create_table_req, table_data)
                .await
                .unwrap();

            // Describe table version with specific version number
            let describe_req = DescribeTableVersionRequest {
                id: Some(vec![
                    "describe_version_ns".to_string(),
                    "describe_version_table".to_string(),
                ]),
                version: Some(1),
                ..Default::default()
            };
            let result = fixture.namespace.describe_table_version(describe_req).await;
            assert!(
                result.is_ok(),
                "Failed to describe table version 1: {:?}",
                result
            );
            let version_info = result.unwrap();
            assert_eq!(version_info.version.version, 1);

            // Describe table version with None (latest)
            let describe_req = DescribeTableVersionRequest {
                id: Some(vec![
                    "describe_version_ns".to_string(),
                    "describe_version_table".to_string(),
                ]),
                version: None,
                ..Default::default()
            };
            let result = fixture.namespace.describe_table_version(describe_req).await;
            assert!(
                result.is_ok(),
                "Failed to describe latest table version: {:?}",
                result
            );
            let version_info = result.unwrap();
            assert_eq!(
                version_info.version.version, 1,
                "Latest version should be 1"
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_create_and_list_table_index() {
            let fixture = RestServerFixture::new().await;
            let table_data = create_test_arrow_data();

            // Create namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["index_test_ns".to_string()]),
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Create table
            let create_table_req = CreateTableRequest {
                id: Some(vec![
                    "index_test_ns".to_string(),
                    "index_test_table".to_string(),
                ]),
                mode: Some("Create".to_string()),
                ..Default::default()
            };
            fixture
                .namespace
                .create_table(create_table_req, table_data)
                .await
                .unwrap();

            // Create scalar index on 'id' column
            let create_index_req = CreateTableIndexRequest {
                id: Some(vec![
                    "index_test_ns".to_string(),
                    "index_test_table".to_string(),
                ]),
                column: "id".to_string(),
                index_type: "BTREE".to_string(),
                name: Some("id_idx".to_string()),
                ..Default::default()
            };
            let result = fixture.namespace.create_table_index(create_index_req).await;
            assert!(result.is_ok(), "Failed to create index: {:?}", result.err());

            // List indices
            let list_indices_req = ListTableIndicesRequest {
                id: Some(vec![
                    "index_test_ns".to_string(),
                    "index_test_table".to_string(),
                ]),
                ..Default::default()
            };
            let result = fixture.namespace.list_table_indices(list_indices_req).await;
            assert!(result.is_ok(), "Failed to list indices: {:?}", result.err());
            let indices = result.unwrap();
            assert_eq!(indices.indexes.len(), 1, "Should have exactly one index");
            assert_eq!(
                indices.indexes[0].index_name, "id_idx",
                "Index name should match"
            );
            assert_eq!(
                indices.indexes[0].columns,
                vec!["id"],
                "Index column should be 'id'"
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_create_vector_index() {
            let fixture = RestServerFixture::new().await;
            // Create 256 rows with 8-dimensional vectors for vector index
            let table_data = create_test_vector_data(256, 8);

            // Create namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["vector_index_ns".to_string()]),
                ..Default::default()
            };
            fixture
                .namespace
                .create_namespace(create_ns_req)
                .await
                .unwrap();

            // Create table with vector data
            let create_table_req = CreateTableRequest {
                id: Some(vec![
                    "vector_index_ns".to_string(),
                    "vector_table".to_string(),
                ]),
                mode: Some("Create".to_string()),
                ..Default::default()
            };
            fixture
                .namespace
                .create_table(create_table_req, table_data)
                .await
                .unwrap();

            // Create vector index on 'vector' column using IVF_FLAT
            let mut create_index_req =
                CreateTableIndexRequest::new("vector".to_string(), "IVF_FLAT".to_string());
            create_index_req.id = Some(vec![
                "vector_index_ns".to_string(),
                "vector_table".to_string(),
            ]);
            create_index_req.name = Some("vector_idx".to_string());
            create_index_req.distance_type = Some("l2".to_string());
            let result = fixture.namespace.create_table_index(create_index_req).await;
            assert!(
                result.is_ok(),
                "Failed to create vector index: {:?}",
                result.err()
            );

            // List indices to verify
            let list_indices_req = ListTableIndicesRequest {
                id: Some(vec![
                    "vector_index_ns".to_string(),
                    "vector_table".to_string(),
                ]),
                ..Default::default()
            };
            let result = fixture.namespace.list_table_indices(list_indices_req).await;
            assert!(result.is_ok(), "Failed to list indices: {:?}", result.err());
            let indices = result.unwrap();
            assert_eq!(indices.indexes.len(), 1, "Should have exactly one index");
            assert_eq!(
                indices.indexes[0].index_name, "vector_idx",
                "Index name should match"
            );
            assert_eq!(
                indices.indexes[0].columns,
                vec!["vector"],
                "Index column should be 'vector'"
            );
        }
    }
}
