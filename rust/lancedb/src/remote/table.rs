// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

pub mod insert;

use self::insert::RemoteInsertExec;
use crate::expr::expr_to_sql_string;
use crate::table::write_progress::FinishOnDrop;

use super::ARROW_STREAM_CONTENT_TYPE;
use super::client::RequestResultExt;
use super::client::{HttpSend, RestfulLanceDbClient, Sender};
use super::db::ServerVersion;
use crate::data::scannable::{PeekedScannable, Scannable, estimate_write_partitions};
use crate::index::Index;
use crate::index::IndexStatistics;
use crate::index::waiter::wait_for_index;
use crate::query::{QueryFilter, QueryRequest, Select, VectorQueryRequest};
use crate::table::AddColumnsResult;
use crate::table::AddResult;
use crate::table::DeleteResult;
use crate::table::DropColumnsResult;
use crate::table::LsmWriteSpec;
use crate::table::MergeResult;
use crate::table::Tags;
use crate::table::UpdateResult;
use crate::table::merge::MergeFilter;
use crate::table::query::create_multi_vector_plan;
use crate::table::{AlterColumnsResult, FieldMetadataUpdate, UpdateFieldMetadataResult};
use crate::table::{AnyQuery, Filter, Predicate, PreprocessingOutput, TableStatistics};
use crate::utils::background_cache::BackgroundCache;
use crate::utils::{
    resolve_arrow_field_path, supported_btree_data_type, supported_vector_data_type,
};
use crate::{DistanceType, Error};
use crate::{
    error::Result,
    index::{IndexBuilder, IndexConfig},
    query::{AnalyzePlanDistributedMetrics, QueryExecutionOptions},
    table::{
        AddDataBuilder, BaseTable, OptimizeAction, OptimizeStats, TableDefinition, UpdateBuilder,
        merge::MergeInsertBuilder,
    },
};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_ipc::reader::FileReader;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion_common::DataFusionError;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use futures::{StreamExt, TryStreamExt};
use http::header::CONTENT_TYPE;
use http::{HeaderName, StatusCode};
use lance::arrow::json::{JsonDataType, JsonSchema};
use lance::dataset::refs::TagContents;
use lance::dataset::scanner::DatasetRecordBatchStream;
use lance::dataset::{ColumnAlteration, NewColumnTransform, Version};
use lance_datafusion::exec::{OneShotExec, execute_plan};
use reqwest::{RequestBuilder, Response};
use serde::{Deserialize, Serialize};
use serde_json::Number;
use std::collections::HashMap;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

const REQUEST_TIMEOUT_HEADER: HeaderName = HeaderName::from_static("x-request-timeout-ms");
const MIN_VERSION_HEADER: HeaderName = HeaderName::from_static("x-lancedb-min-version");
const MIN_TIMESTAMP_HEADER: HeaderName = HeaderName::from_static("x-lancedb-min-timestamp");
const MIN_READ_VERSION_HEADER: HeaderName = HeaderName::from_static("x-lancedb-min-read-version");
const VERSION_HEADER: HeaderName = HeaderName::from_static("x-lancedb-version");
const METRIC_TYPE_KEY: &str = "metric_type";
const INDEX_TYPE_KEY: &str = "index_type";
const SCHEMA_CACHE_TTL: Duration = Duration::from_secs(30);
const SCHEMA_CACHE_REFRESH_WINDOW: Duration = Duration::from_secs(5);

/// Per-table state driving the freshness headers (`x-lancedb-min-version`,
/// `x-lancedb-min-timestamp`, and `x-lancedb-min-read-version`) sent on read
/// requests.
#[derive(Debug, Default, Clone, Copy)]
struct FreshnessState {
    /// Provides read-your-write within a single handle: writes that return a
    /// version update this, and reads send it as `x-lancedb-min-version`.
    min_version: Option<u64>,
    /// Highest dataset version observed in a *read* response on this handle.
    /// Reads send it as `x-lancedb-min-read-version` so a load-balanced query
    /// node whose cache is behind this version must refresh before serving,
    /// giving monotonic reads across nodes regardless of which one the load
    /// balancer routes to. Sourced only from reads (always committed dataset
    /// versions), never from writes (which may return WAL entry ids), so it is
    /// unaffected by the WAL/version mismatch that retired `min_version`.
    min_read_version: Option<u64>,
    /// Wall-clock time captured at the last [`BaseTable::checkout_latest`]
    /// call. Subsequent reads send
    /// `max(baseline, now - read_consistency_interval)` as
    /// `x-lancedb-min-timestamp`.
    ///
    /// Without this, `checkout_latest()` would have no effect on subsequent
    /// reads when `read_consistency_interval` is unset (the default): a
    /// server-side cache could still serve a snapshot older than the moment
    /// the user explicitly asked for "latest". The baseline forces the
    /// server to skip any cache entry older than the checkout time, so the
    /// `checkout_latest()` signal is preserved across reads on the same
    /// handle regardless of the configured consistency interval.
    checkout_baseline: Option<SystemTime>,
}

/// Snapshot of the headers that should be attached to a single read request.
#[derive(Debug, Default, Clone, Copy)]
struct FreshnessHeaders {
    min_version: Option<u64>,
    min_timestamp: Option<SystemTime>,
    min_read_version: Option<u64>,
}

impl FreshnessHeaders {
    fn apply(self, mut request: RequestBuilder) -> RequestBuilder {
        if let Some(v) = self.min_version {
            request = request.header(MIN_VERSION_HEADER, v.to_string());
        }
        if let Some(ts) = self.min_timestamp {
            let dt: chrono::DateTime<chrono::Utc> = ts.into();
            request = request.header(MIN_TIMESTAMP_HEADER, dt.to_rfc3339());
        }
        if let Some(v) = self.min_read_version {
            request = request.header(MIN_READ_VERSION_HEADER, v.to_string());
        }
        request
    }
}

fn compute_min_timestamp(
    state: &FreshnessState,
    interval: Option<Duration>,
    now: SystemTime,
) -> Option<SystemTime> {
    let interval_based = match interval {
        None => None,
        Some(d) if d.is_zero() => Some(now),
        Some(d) => Some(now.checked_sub(d).unwrap_or(now)),
    };
    match (interval_based, state.checkout_baseline) {
        (None, None) => None,
        (Some(t), None) | (None, Some(t)) => Some(t),
        (Some(a), Some(b)) => Some(a.max(b)),
    }
}

/// Normalize a branch selector: trim whitespace and treat `""` or `"main"` as
/// the (absent) main branch, matching the server's convention.
fn normalize_branch(branch: Option<String>) -> Option<String> {
    branch
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && value != "main")
}

pub struct RemoteTags<'a, S: HttpSend = Sender> {
    inner: &'a RemoteTable<S>,
}

#[async_trait]
impl<S: HttpSend + 'static> Tags for RemoteTags<'_, S> {
    async fn list(&self) -> Result<HashMap<String, TagContents>> {
        let request = self
            .inner
            .post_read(&format!("/v1/table/{}/tags/list/", self.inner.identifier));
        let (request_id, response) = self.inner.send(request, true).await?;
        let response = self
            .inner
            .check_table_response(&request_id, response)
            .await?;

        match response.text().await {
            Ok(body) => {
                // Explicitly tell serde_json what type we want to deserialize into
                let tags_map: HashMap<String, TagContents> =
                    serde_json::from_str(&body).map_err(|e| Error::Http {
                        source: format!("Failed to parse tags list: {}", e).into(),
                        request_id,
                        status_code: None,
                    })?;

                Ok(tags_map)
            }
            Err(err) => {
                let status_code = err.status();
                Err(Error::Http {
                    source: Box::new(err),
                    request_id,
                    status_code,
                })
            }
        }
    }

    async fn get_version(&self, tag: &str) -> Result<u64> {
        let request = self.inner.post_read(&format!(
            "/v1/table/{}/tags/version/",
            self.inner.identifier
        ));
        self.inner
            .resolve_tag_version_with_request(tag, request)
            .await
    }

    async fn create(&mut self, tag: &str, version: u64) -> Result<()> {
        let mut body = serde_json::json!({
            "tag": tag,
            "version": version
        });
        self.inner.apply_branch_body(&mut body);
        let request = self
            .inner
            .client
            .post(&format!("/v1/table/{}/tags/create/", self.inner.identifier))
            .json(&body);

        let (request_id, response) = self.inner.send(request, true).await?;
        self.inner
            .check_table_response(&request_id, response)
            .await?;
        Ok(())
    }

    async fn delete(&mut self, tag: &str) -> Result<()> {
        let request = self
            .inner
            .client
            .post(&format!("/v1/table/{}/tags/delete/", self.inner.identifier))
            .json(&serde_json::json!({ "tag": tag }));

        let (request_id, response) = self.inner.send(request, true).await?;
        self.inner
            .check_table_response(&request_id, response)
            .await?;
        Ok(())
    }

    async fn update(&mut self, tag: &str, version: u64) -> Result<()> {
        let mut body = serde_json::json!({
            "tag": tag,
            "version": version
        });
        self.inner.apply_branch_body(&mut body);
        let request = self
            .inner
            .client
            .post(&format!("/v1/table/{}/tags/update/", self.inner.identifier))
            .json(&body);

        let (request_id, response) = self.inner.send(request, true).await?;
        self.inner
            .check_table_response(&request_id, response)
            .await?;
        Ok(())
    }
}

pub struct RemoteTable<S: HttpSend = Sender> {
    client: RestfulLanceDbClient<S>,
    name: String,
    namespace: Vec<String>,
    identifier: String,
    server_version: ServerVersion,

    version: RwLock<Option<u64>>,
    location: RwLock<Option<String>>,
    schema_cache: BackgroundCache<SchemaRef, Error>,
    freshness: Mutex<FreshnessState>,
    /// The branch this handle is scoped to, or `None` for the main branch.
    /// Stamped onto every branch-accepting request so reads and writes resolve
    /// on the branch's own version chain rather than main's.
    branch: Option<String>,
}

impl<S: HttpSend> std::fmt::Debug for RemoteTable<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteTable")
            .field("name", &self.name)
            .field("identifier", &self.identifier)
            .finish_non_exhaustive()
    }
}

impl<S: HttpSend> RemoteTable<S> {
    pub fn new(
        client: RestfulLanceDbClient<S>,
        name: String,
        namespace: Vec<String>,
        identifier: String,
        server_version: ServerVersion,
    ) -> Self {
        Self {
            client,
            name,
            namespace,
            identifier,
            server_version,
            version: RwLock::new(None),
            location: RwLock::new(None),
            schema_cache: BackgroundCache::new(SCHEMA_CACHE_TTL, SCHEMA_CACHE_REFRESH_WINDOW),
            freshness: Mutex::new(FreshnessState::default()),
            branch: None,
        }
    }

    /// Return a new handle scoped to `branch`, sharing the client but with fresh
    /// caches and version/freshness state (the branch tracks its own latest).
    /// Mirrors `NativeTable`'s handle-per-branch model.
    fn with_branch(&self, branch: Option<String>) -> Self {
        Self {
            client: self.client.clone(),
            name: self.name.clone(),
            namespace: self.namespace.clone(),
            identifier: self.identifier.clone(),
            server_version: self.server_version.clone(),
            version: RwLock::new(None),
            location: RwLock::new(None),
            schema_cache: BackgroundCache::new(SCHEMA_CACHE_TTL, SCHEMA_CACHE_REFRESH_WINDOW),
            freshness: Mutex::new(FreshnessState::default()),
            branch,
        }
    }

    /// Stamp the branch onto a request as a `?branch=` query param (used for
    /// Arrow-body / query-only ops). `None` (main) leaves the request unchanged,
    /// keeping it byte-identical to the non-branch path.
    fn apply_branch_query(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.branch {
            Some(branch) => request.query(&[("branch", branch.as_str())]),
            None => request,
        }
    }

    /// Stamp the branch into a JSON request body under `"branch"` (used for JSON
    /// ops). `None` (main) leaves the body unchanged.
    fn apply_branch_body(&self, body: &mut serde_json::Value) {
        if let Some(branch) = &self.branch {
            body["branch"] = serde_json::Value::String(branch.clone());
        }
    }

    async fn describe(&self) -> Result<TableDescription> {
        let version = self.current_version().await;
        self.describe_version(version).await
    }

    async fn describe_version(&self, version: Option<u64>) -> Result<TableDescription> {
        let request = self.post_read(&format!("/v1/table/{}/describe/", self.identifier));
        self.describe_with_request(request, version).await
    }

    async fn resolve_tag_version_with_request(
        &self,
        tag: &str,
        request: RequestBuilder,
    ) -> Result<u64> {
        let request = request.json(&serde_json::json!({ "tag": tag }));

        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;

        match response.text().await {
            Ok(body) => {
                let value: serde_json::Value =
                    serde_json::from_str(&body).map_err(|e| Error::Http {
                        source: format!("Failed to parse tag version: {}", e).into(),
                        request_id: request_id.clone(),
                        status_code: None,
                    })?;

                value
                    .get("version")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| Error::Http {
                        source: format!("Invalid tag version response: {}", body).into(),
                        request_id,
                        status_code: None,
                    })
            }
            Err(err) => {
                let status_code = err.status();
                Err(Error::Http {
                    source: Box::new(err),
                    request_id,
                    status_code,
                })
            }
        }
    }

    /// Resolve a tag to its `(branch, version)` coordinate via the `tags/version`
    /// endpoint, since the `/branches/create` contract accepts no `from_tag`.
    async fn resolve_tag_ref(&self, tag: &str) -> Result<(Option<String>, u64)> {
        let request = self
            .client
            .post(&format!("/v1/table/{}/tags/version/", self.identifier))
            .json(&serde_json::json!({ "tag": tag }));
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let value: serde_json::Value = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse tag version: {}", e).into(),
            request_id: request_id.clone(),
            status_code: None,
        })?;
        let version = value
            .get("version")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| Error::Http {
                source: format!("Invalid tag version response: {}", body).into(),
                request_id,
                status_code: None,
            })?;
        let branch = value
            .get("branch")
            .and_then(|v| v.as_str())
            .map(String::from);
        Ok((normalize_branch(branch), version))
    }

    async fn describe_with_request(
        &self,
        request: RequestBuilder,
        version: Option<u64>,
    ) -> Result<TableDescription> {
        let mut body = serde_json::json!({ "version": version });
        self.apply_branch_body(&mut body);
        let request = request.json(&body);

        let (request_id, response) = self.send(request, true).await?;

        let response = self.check_table_response(&request_id, response).await?;

        match response.text().await {
            Ok(body) => serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to parse table description: {}", e).into(),
                request_id,
                status_code: None,
            }),
            Err(err) => {
                let status_code = err.status();
                Err(Error::Http {
                    source: Box::new(err),
                    request_id,
                    status_code,
                })
            }
        }
    }

    fn reader_as_body(data: Box<dyn RecordBatchReader + Send>) -> Result<reqwest::Body> {
        // TODO: Once Phalanx supports compression, we should use it here.
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(
            Vec::new(),
            &RecordBatchReader::schema(&*data),
        )?;

        //  Mutex is just here to make it sync. We shouldn't have any contention.
        let mut data = Mutex::new(data);
        let body_iter = std::iter::from_fn(move || match data.get_mut().unwrap().next() {
            Some(Ok(batch)) => {
                writer.write(&batch).ok()?;
                let buffer = std::mem::take(writer.get_mut());
                Some(Ok(buffer))
            }
            Some(Err(e)) => Some(Err(e)),
            None => {
                writer.finish().ok()?;
                let buffer = std::mem::take(writer.get_mut());
                Some(Ok(buffer))
            }
        });
        let body_stream = futures::stream::iter(body_iter);
        Ok(reqwest::Body::wrap_stream(body_stream))
    }

    /// Buffer the reader into memory
    async fn buffer_reader<R: RecordBatchReader + ?Sized>(
        reader: &mut R,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let schema = reader.schema();
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch?);
        }
        Ok((schema, batches))
    }

    /// Create a new RecordBatchReader from buffered data
    fn make_reader(schema: SchemaRef, batches: Vec<RecordBatch>) -> impl RecordBatchReader {
        let iter = batches.into_iter().map(Ok);
        RecordBatchIterator::new(iter, schema)
    }

    async fn send(&self, req: RequestBuilder, with_retry: bool) -> Result<(String, Response)> {
        let res = if with_retry {
            self.client.send_with_retry(req, None, true).await?
        } else {
            self.client.send(req).await?
        };
        Ok(res)
    }

    /// Send the request with streaming body.
    /// This will use retries if with_retry is set and the number of configured retries is > 0.
    /// If retries are enabled, the stream will be buffered into memory.
    async fn send_streaming(
        &self,
        req: RequestBuilder,
        mut data: Box<dyn RecordBatchReader + Send>,
        with_retry: bool,
    ) -> Result<(String, Response)> {
        if !with_retry || self.client.retry_config.retries == 0 {
            let body = Self::reader_as_body(data)?;
            return self.client.send(req.body(body)).await;
        }

        // to support retries, buffer into memory and clone the batches on each retry
        let (schema, batches) = Self::buffer_reader(&mut *data).await?;
        let make_body = Box::new(move || {
            let reader = Self::make_reader(schema.clone(), batches.clone());
            Self::reader_as_body(Box::new(reader))
        });
        let res = self
            .client
            .send_with_retry(req, Some(make_body), false)
            .await?;

        Ok(res)
    }

    pub(super) async fn handle_table_not_found(
        table_name: &str,
        response: reqwest::Response,
        request_id: &str,
    ) -> Result<reqwest::Response> {
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            let body = response.text().await.ok().unwrap_or_default();
            let request_error = Error::Http {
                source: body.into(),
                request_id: request_id.into(),
                status_code: Some(status),
            };
            return Err(Error::TableNotFound {
                name: table_name.to_string(),
                source: Box::new(request_error),
            });
        }
        Ok(response)
    }

    /// Check if a status code should trigger schema cache invalidation
    fn should_invalidate_cache_for_status(status: StatusCode) -> bool {
        // Only invalidate for errors that could be schema-related
        // Don't invalidate for auth errors (401, 403) or temporary failures (503, 502)
        matches!(
            status,
            StatusCode::BAD_REQUEST // 400 - could be schema mismatch
            | StatusCode::NOT_FOUND // 404 - table might have been recreated
            | StatusCode::UNPROCESSABLE_ENTITY // 422 - schema validation error
            | StatusCode::INTERNAL_SERVER_ERROR // 500 - could be schema issue on server
        )
    }

    async fn check_table_response(
        &self,
        request_id: &str,
        response: reqwest::Response,
    ) -> Result<reqwest::Response> {
        let status = response.status();
        let not_found_result = Self::handle_table_not_found(&self.name, response, request_id).await;

        // Check if we should invalidate cache for 404 errors
        if not_found_result.is_err() && Self::should_invalidate_cache_for_status(status) {
            self.invalidate_schema_cache();
        }

        let response = not_found_result?;
        let result = self.client.check_response(request_id, response).await;

        // Invalidate schema cache on errors that could be schema-related
        if result.is_err() && Self::should_invalidate_cache_for_status(status) {
            self.invalidate_schema_cache();
        }

        result
    }

    async fn read_arrow_stream(
        &self,
        request_id: &str,
        response: reqwest::Response,
    ) -> Result<SendableRecordBatchStream> {
        let response = self.check_table_response(request_id, response).await?;

        // There isn't a way to actually stream this data yet. I have an upstream issue:
        // https://github.com/apache/arrow-rs/issues/6420
        let body = response.bytes().await.err_to_http(request_id.into())?;
        let reader = FileReader::try_new(Cursor::new(body), None)?;
        let schema = reader.schema();
        let stream = futures::stream::iter(reader).map_err(DataFusionError::from);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn apply_query_params(
        &self,
        body: &mut serde_json::Value,
        params: &QueryRequest,
    ) -> Result<()> {
        params.check_filter()?;
        body["prefilter"] = params.prefilter.into();
        if let Some(offset) = params.offset {
            body["offset"] = serde_json::Value::Number(serde_json::Number::from(offset));
        }

        // Server requires k.
        // use isize::MAX as usize to avoid overflow: https://github.com/lancedb/lancedb/issues/2211
        let limit = params.limit.unwrap_or(isize::MAX as usize);
        body["k"] = serde_json::Value::Number(serde_json::Number::from(limit));

        if let Some(filter) = &params.filter {
            let filter_sql = match filter {
                QueryFilter::Sql(sql) => sql.clone(),
                QueryFilter::Datafusion(expr) => expr_to_sql_string(expr)?,
                QueryFilter::Substrait(_) => {
                    return Err(Error::NotSupported {
                        message: "Substrait filters are not supported for remote queries"
                            .to_string(),
                    });
                }
            };
            body["filter"] = serde_json::Value::String(filter_sql);
        }

        match &params.select {
            Select::All => {}
            Select::Columns(columns) => {
                body["columns"] = serde_json::Value::Array(
                    columns
                        .iter()
                        .map(|s| serde_json::Value::String(s.clone()))
                        .collect(),
                );
            }
            Select::Dynamic(pairs) => {
                let alias_map =
                    serde_json::Map::from_iter(pairs.iter().map(|(name, expr)| {
                        (name.clone(), serde_json::Value::String(expr.clone()))
                    }));
                body["columns"] = alias_map.into();
            }
            Select::Expr(pairs) => {
                let alias_map: Result<serde_json::Map<String, serde_json::Value>> = pairs
                    .iter()
                    .map(|(name, expr)| {
                        expr_to_sql_string(expr)
                            .map(|sql| (name.clone(), serde_json::Value::String(sql)))
                    })
                    .collect();
                body["columns"] = alias_map?.into();
            }
        }

        if params.fast_search {
            body["fast_search"] = serde_json::Value::Bool(true);
        }

        if params.with_row_id {
            body["with_row_id"] = serde_json::Value::Bool(true);
        }

        if let Some(full_text_search) = &params.full_text_search {
            if full_text_search.wand_factor.is_some() {
                return Err(Error::NotSupported {
                    message: "Wand factor is not yet supported in LanceDB Cloud".into(),
                });
            }

            if self.server_version.support_structural_fts() {
                body["full_text_query"] = serde_json::json!({
                    "query": full_text_search.query.clone(),
                });
            } else {
                body["full_text_query"] = serde_json::json!({
                    "columns": full_text_search.columns().into_iter().collect::<Vec<_>>(),
                    "query": full_text_search.query.query(),
                })
            }
        }

        if let Some(order_by) = &params.order_by {
            body["order_by"] = serde_json::Value::Array(
                order_by
                    .iter()
                    .map(|o| {
                        serde_json::json!({
                            "column_name": o.column_name,
                            "ascending": o.ascending,
                            "nulls_first": o.nulls_first,
                        })
                    })
                    .collect(),
            );
        }

        Ok(())
    }

    fn apply_vector_query_params(
        &self,
        mut body: serde_json::Value,
        query: &VectorQueryRequest,
    ) -> Result<Vec<serde_json::Value>> {
        self.apply_query_params(&mut body, &query.base)?;

        // Apply general parameters, before we dispatch based on number of query vectors.
        if let Some(distance_type) = query.distance_type {
            body["distance_type"] = serde_json::json!(distance_type);
        }
        if let Some(approx_mode) = query.approx_mode {
            body["approx_mode"] = serde_json::json!(approx_mode);
        }
        // In 0.23.1 we migrated from `nprobes` to `minimum_nprobes` and `maximum_nprobes`.
        // Old client / new server: since minimum_nprobes is missing, fallback to nprobes
        // New client / old server: old server will only see nprobes, make sure to set both
        //                          nprobes and minimum_nprobes
        // New client / new server: since minimum_nprobes is present, server can ignore nprobes
        body["nprobes"] = query.minimum_nprobes.into();
        body["minimum_nprobes"] = query.minimum_nprobes.into();
        if let Some(maximum_nprobes) = query.maximum_nprobes {
            body["maximum_nprobes"] = maximum_nprobes.into();
        } else {
            body["maximum_nprobes"] = serde_json::Value::Number(Number::from_u128(0).unwrap())
        }
        body["lower_bound"] = query.lower_bound.into();
        body["upper_bound"] = query.upper_bound.into();
        body["ef"] = query.ef.into();
        body["refine_factor"] = query.refine_factor.into();
        if let Some(vector_column) = query.column.as_ref() {
            body["vector_column"] = serde_json::Value::String(vector_column.clone());
        }
        if !query.use_index {
            body["bypass_vector_index"] = serde_json::Value::Bool(true);
        }

        fn vector_to_json(vector: &arrow_array::ArrayRef) -> Result<serde_json::Value> {
            match vector.data_type() {
                DataType::Float32 => {
                    let array = vector
                        .as_any()
                        .downcast_ref::<arrow_array::Float32Array>()
                        .unwrap();
                    Ok(serde_json::Value::Array(
                        array
                            .values()
                            .iter()
                            .map(|v| {
                                serde_json::Value::Number(
                                    serde_json::Number::from_f64(*v as f64).unwrap(),
                                )
                            })
                            .collect(),
                    ))
                }
                _ => Err(Error::InvalidInput {
                    message: "VectorQuery vector must be of type Float32".into(),
                }),
            }
        }

        let bodies = match query.query_vector.len() {
            0 => {
                // Server takes empty vector, not null or undefined.
                body["vector"] = serde_json::Value::Array(Vec::new());
                vec![body]
            }
            1 => {
                body["vector"] = vector_to_json(&query.query_vector[0])?;
                vec![body]
            }
            _ => {
                if self.server_version.support_multivector() {
                    let vectors = query
                        .query_vector
                        .iter()
                        .map(vector_to_json)
                        .collect::<Result<Vec<_>>>()?;
                    body["vector"] = serde_json::Value::Array(vectors);
                    vec![body]
                } else {
                    // Server does not support multiple vectors in a single query.
                    // We need to send multiple requests.
                    let mut bodies = Vec::with_capacity(query.query_vector.len());
                    for vector in &query.query_vector {
                        let mut body = body.clone();
                        body["vector"] = vector_to_json(vector)?;
                        bodies.push(body);
                    }
                    bodies
                }
            }
        };

        Ok(bodies)
    }

    async fn create_multipart_write(&self) -> Result<String> {
        let request = self.apply_branch_query(self.client.post(&format!(
            "/v1/table/{}/multipart_write/create",
            self.identifier
        )));
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let parsed: serde_json::Value = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse multipart create response: {}", e).into(),
            request_id,
            status_code: None,
        })?;
        parsed["upload_id"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| Error::Http {
                source: "Missing upload_id in multipart create response".into(),
                request_id: String::new(),
                status_code: None,
            })
    }

    async fn complete_multipart_write(&self, upload_id: &str) -> Result<AddResult> {
        let request = self.apply_branch_query(
            self.client
                .post(&format!(
                    "/v1/table/{}/multipart_write/complete",
                    self.identifier
                ))
                .query(&[("upload_id", upload_id)]),
        );
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let parsed: serde_json::Value = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse multipart complete response: {}", e).into(),
            request_id,
            status_code: None,
        })?;
        let version = parsed["version"].as_u64().ok_or_else(|| Error::Http {
            source: "Missing version in multipart complete response".into(),
            request_id: String::new(),
            status_code: None,
        })?;
        Ok(AddResult { version })
    }

    async fn abort_multipart_write(&self, upload_id: &str) -> Result<()> {
        let request = self.apply_branch_query(
            self.client
                .post(&format!(
                    "/v1/table/{}/multipart_write/abort",
                    self.identifier
                ))
                .query(&[("upload_id", upload_id)]),
        );
        let (request_id, response) = self.send(request, true).await?;
        self.check_table_response(&request_id, response).await?;
        Ok(())
    }

    async fn check_mutable(&self) -> Result<()> {
        let read_guard = self.version.read().await;
        match *read_guard {
            None => Ok(()),
            Some(version) => Err(Error::NotSupported {
                message: format!(
                    "Cannot mutate table reference fixed at version {}. Call checkout_latest() to get a mutable table reference.",
                    version
                ),
            }),
        }
    }

    async fn current_version(&self) -> Option<u64> {
        let read_guard = self.version.read().await;
        *read_guard
    }

    /// Snapshot the freshness headers to attach to a single read request.
    /// Computed at call time so that retries reuse the same snapshot.
    fn snapshot_freshness_headers(&self) -> FreshnessHeaders {
        let state = *self.freshness.lock().unwrap();
        FreshnessHeaders {
            min_version: state.min_version,
            min_timestamp: compute_min_timestamp(
                &state,
                self.client.read_consistency_interval,
                SystemTime::now(),
            ),
            min_read_version: state.min_read_version,
        }
    }

    /// Build a POST request and attach the read-freshness headers
    /// (`x-lancedb-min-version`, `x-lancedb-min-timestamp`).
    fn post_read(&self, uri: &str) -> RequestBuilder {
        self.snapshot_freshness_headers()
            .apply(self.client.post(uri))
    }

    /// Record a version returned by a write so subsequent reads can request at
    /// least that version via `x-lancedb-min-version`. A returned `0` from a
    /// backward-compatible old server is ignored.
    fn track_write_version(&self, version: u64) {
        if version == 0 {
            return;
        }
        let mut state = self.freshness.lock().unwrap();
        state.min_version = Some(state.min_version.map_or(version, |v| v.max(version)));
    }

    /// Record a dataset version observed in a *read* response so subsequent
    /// reads request at least this version via `x-lancedb-min-read-version`,
    /// giving monotonic reads across load-balanced query nodes. A returned `0`
    /// (or absent header from an old server) is ignored.
    fn track_read_version(&self, version: u64) {
        if version == 0 {
            return;
        }
        let mut state = self.freshness.lock().unwrap();
        state.min_read_version = Some(state.min_read_version.map_or(version, |v| v.max(version)));
    }

    /// Parse the `x-lancedb-version` response header (the dataset version a read
    /// reflects) and fold it into the read-version watermark.
    fn track_read_version_from_headers(&self, headers: &reqwest::header::HeaderMap) {
        if let Some(version) = headers
            .get(&VERSION_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
        {
            self.track_read_version(version);
        }
    }

    async fn execute_query(
        &self,
        query: &AnyQuery,
        options: &QueryExecutionOptions,
    ) -> Result<Vec<Pin<Box<dyn RecordBatchStream + Send>>>> {
        let mut request = self.post_read(&format!("/v1/table/{}/query/", self.identifier));

        if let Some(timeout) = options.timeout {
            // Also send to server, so it can abort the query if it takes too long.
            // (If it doesn't fit into u64, it's not worth sending anyways.)
            if let Ok(timeout_ms) = u64::try_from(timeout.as_millis()) {
                request = request.header(REQUEST_TIMEOUT_HEADER, timeout_ms);
            }
        }

        let query_bodies = self.prepare_query_bodies(query).await?;
        let requests: Vec<reqwest::RequestBuilder> = query_bodies
            .into_iter()
            .map(|body| request.try_clone().unwrap().json(&body))
            .collect();

        let futures = requests.into_iter().map(|req| async move {
            let (request_id, response) = self.send(req, true).await?;
            self.track_read_version_from_headers(response.headers());
            self.read_arrow_stream(&request_id, response).await
        });
        let streams = futures::future::try_join_all(futures);

        if let Some(timeout) = options.timeout {
            let timeout_future = tokio::time::sleep(timeout);
            tokio::pin!(timeout_future);
            tokio::pin!(streams);
            tokio::select! {
                _ = &mut timeout_future => {
                    Err(Error::Other {
                        message: format!("Query timeout after {} ms", timeout.as_millis()),
                        source: None,
                    })
                }
                result = &mut streams => {
                    Ok(result?)
                }
            }
        } else {
            Ok(streams.await?)
        }
    }

    async fn prepare_query_bodies(&self, query: &AnyQuery) -> Result<Vec<serde_json::Value>> {
        let version = self.current_version().await;
        let mut base_body = serde_json::json!({ "version": version });
        self.apply_branch_body(&mut base_body);

        match query {
            AnyQuery::Query(query) => {
                let mut body = base_body.clone();
                self.apply_query_params(&mut body, query)?;
                // Empty vector can be passed if no vector search is performed.
                body["vector"] = serde_json::Value::Array(Vec::new());
                Ok(vec![body])
            }
            AnyQuery::VectorQuery(query) => self.apply_vector_query_params(base_body, query),
        }
    }

    fn invalidate_schema_cache(&self) {
        self.schema_cache.invalidate();
    }

    fn handle_error_invalidation(&self, error: &Error) {
        let status_code = match error {
            Error::Http { status_code, .. } => *status_code,
            Error::Retry { status_code, .. } => *status_code,
            _ => None,
        };
        if let Some(status_code) = status_code
            && Self::should_invalidate_cache_for_status(status_code)
        {
            self.invalidate_schema_cache();
        }
    }
}

#[derive(Deserialize)]
struct TableDescription {
    version: u64,
    schema: JsonSchema,
    location: Option<String>,
}

/// Extract an Error from Arc<Error>, reconstructing if the Arc is shared.
/// This is needed because `Shared` futures cache results internally, so
/// `Arc::try_unwrap` typically fails.
fn unwrap_shared_error(arc: Arc<Error>) -> Error {
    match Arc::try_unwrap(arc) {
        Ok(err) => err,
        Err(arc) => match &*arc {
            Error::TableNotFound { name, source } => Error::TableNotFound {
                name: name.clone(),
                source: source.to_string().into(),
            },
            _ => Error::Runtime {
                message: arc.to_string(),
            },
        },
    }
}

async fn fetch_schema<S: HttpSend>(
    client: &RestfulLanceDbClient<S>,
    identifier: &str,
    table_name: &str,
    version: Option<u64>,
    branch: Option<String>,
    freshness_headers: FreshnessHeaders,
) -> Result<SchemaRef> {
    let mut body = serde_json::json!({ "version": version });
    if let Some(branch) = &branch {
        body["branch"] = serde_json::Value::String(branch.clone());
    }
    let request = freshness_headers
        .apply(client.post(&format!("/v1/table/{}/describe/", identifier)))
        .json(&body);

    let (request_id, response) = client.send_with_retry(request, None, true).await?;

    if response.status() == StatusCode::NOT_FOUND {
        let body = response.text().await.ok().unwrap_or_default();
        return Err(Error::TableNotFound {
            name: table_name.to_string(),
            source: Box::new(Error::Http {
                source: body.into(),
                request_id,
                status_code: Some(StatusCode::NOT_FOUND),
            }),
        });
    }

    let response = client.check_response(&request_id, response).await?;
    let body = response.text().await.map_err(|e| {
        let status_code = e.status();
        Error::Http {
            source: Box::new(e),
            request_id: request_id.clone(),
            status_code,
        }
    })?;

    let description: TableDescription = serde_json::from_str(&body).map_err(|e| Error::Http {
        source: format!("Failed to parse table description: {}", e).into(),
        request_id,
        status_code: None,
    })?;

    let arrow_schema: arrow_schema::Schema = description.schema.try_into()?;
    Ok(Arc::new(arrow_schema))
}

impl<S: HttpSend> std::fmt::Display for RemoteTable<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RemoteTable({})", self.identifier)
    }
}

#[cfg(all(test, feature = "remote"))]
mod test_utils {
    use super::*;
    use crate::remote::ClientConfig;
    use crate::remote::client::test_utils::client_with_handler;
    use crate::remote::client::test_utils::{
        MockSender, client_with_handler_and_config, client_with_handler_and_interval,
    };

    impl RemoteTable<MockSender> {
        pub fn new_mock<F, T>(name: String, handler: F, version: Option<semver::Version>) -> Self
        where
            F: Fn(reqwest::Request) -> http::Response<T> + Send + Sync + 'static,
            T: Into<reqwest::Body>,
        {
            let client = client_with_handler(handler);
            Self {
                client,
                name: name.clone(),
                namespace: vec![],
                identifier: name,
                server_version: version.map(ServerVersion).unwrap_or_default(),
                version: RwLock::new(None),
                location: RwLock::new(None),
                schema_cache: BackgroundCache::new(SCHEMA_CACHE_TTL, SCHEMA_CACHE_REFRESH_WINDOW),
                freshness: Mutex::new(FreshnessState::default()),
                branch: None,
            }
        }

        pub fn new_mock_with_consistency_interval<F, T>(
            name: String,
            handler: F,
            read_consistency_interval: Option<Duration>,
        ) -> Self
        where
            F: Fn(reqwest::Request) -> http::Response<T> + Send + Sync + 'static,
            T: Into<reqwest::Body>,
        {
            let client = client_with_handler_and_interval(handler, read_consistency_interval);
            Self {
                client,
                name: name.clone(),
                namespace: vec![],
                identifier: name,
                server_version: ServerVersion::default(),
                version: RwLock::new(None),
                location: RwLock::new(None),
                schema_cache: BackgroundCache::new(SCHEMA_CACHE_TTL, SCHEMA_CACHE_REFRESH_WINDOW),
                freshness: Mutex::new(FreshnessState::default()),
                branch: None,
            }
        }

        pub fn new_mock_with_config<F, T>(name: String, handler: F, config: ClientConfig) -> Self
        where
            F: Fn(reqwest::Request) -> http::Response<T> + Send + Sync + 'static,
            T: Into<reqwest::Body>,
        {
            Self::new_mock_with_version_and_config(name, handler, None, config)
        }

        pub fn new_mock_with_version_and_config<F, T>(
            name: String,
            handler: F,
            version: Option<semver::Version>,
            config: ClientConfig,
        ) -> Self
        where
            F: Fn(reqwest::Request) -> http::Response<T> + Send + Sync + 'static,
            T: Into<reqwest::Body>,
        {
            let client = client_with_handler_and_config(handler, config);
            Self {
                client,
                name: name.clone(),
                namespace: vec![],
                identifier: name,
                server_version: version.map(ServerVersion).unwrap_or_default(),
                version: RwLock::new(None),
                location: RwLock::new(None),
                schema_cache: BackgroundCache::new(SCHEMA_CACHE_TTL, SCHEMA_CACHE_REFRESH_WINDOW),
                freshness: Mutex::new(FreshnessState::default()),
                branch: None,
            }
        }
    }
}

impl<S: HttpSend + 'static> RemoteTable<S> {
    fn is_retryable_write_error(&self, err: &Error) -> bool {
        match err {
            Error::Http {
                source,
                status_code,
                ..
            } => {
                // Don't retry read errors (is_body/is_decode): the
                // server may have committed the write already, and
                // without an idempotency key we'd duplicate data.
                source
                    .downcast_ref::<reqwest::Error>()
                    .is_some_and(|e| e.is_connect())
                    || status_code.is_some_and(|s| self.client.retry_config.statuses.contains(&s))
            }
            // send_with_retry exhausted its internal retries on a retryable
            // status. The outer loop can still retry the whole operation with
            // a fresh session.
            Error::Retry { status_code, .. } => {
                status_code.is_some_and(|s| self.client.retry_config.statuses.contains(&s))
            }
            _ => false,
        }
    }

    async fn add_single_partition(&self, output: PreprocessingOutput) -> Result<AddResult> {
        use crate::remote::retry::RetryCounter;

        let _guard = output.tracker.as_ref().map(|t| t.track_task());

        let mut insert: Arc<dyn ExecutionPlan> = Arc::new(RemoteInsertExec::new(
            self.name.clone(),
            self.identifier.clone(),
            self.client.clone(),
            output.plan,
            output.overwrite,
            output.tracker.clone(),
            self.branch.clone(),
        ));

        let mut retry_counter =
            RetryCounter::new(&self.client.retry_config, uuid::Uuid::new_v4().to_string());

        loop {
            let stream = execute_plan(insert.clone(), Default::default())?;
            let result: Result<Vec<_>> = stream.try_collect().await.map_err(Error::from);

            match result {
                Ok(_) => {
                    let add_result = (insert.as_ref() as &dyn std::any::Any)
                        .downcast_ref::<RemoteInsertExec<S>>()
                        .and_then(|i| i.add_result())
                        .unwrap_or(AddResult { version: 0 });

                    if output.overwrite {
                        self.invalidate_schema_cache();
                    }
                    self.track_write_version(add_result.version);

                    return Ok(add_result);
                }
                Err(err) if output.rescannable && self.is_retryable_write_error(&err) => {
                    retry_counter.increment_from_error(err)?;
                    tokio::time::sleep(retry_counter.next_sleep_time()).await;
                    insert = insert.reset_state()?;
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn add_multipart(
        &self,
        output: PreprocessingOutput,
        num_partitions: usize,
    ) -> Result<AddResult> {
        use crate::remote::retry::RetryCounter;

        let mut retry_counter =
            RetryCounter::new(&self.client.retry_config, uuid::Uuid::new_v4().to_string());

        loop {
            let upload_id = self.create_multipart_write().await?;

            let result = self
                .execute_multipart_inserts(&upload_id, &output, num_partitions)
                .await;

            match result {
                Ok(()) => match self.complete_multipart_write(&upload_id).await {
                    Ok(result) => {
                        if output.overwrite {
                            self.invalidate_schema_cache();
                        }
                        self.track_write_version(result.version);
                        return Ok(result);
                    }
                    Err(e) => {
                        if let Err(abort_err) = self.abort_multipart_write(&upload_id).await {
                            log::warn!(
                                "Failed to abort multipart write {}: {}",
                                upload_id,
                                abort_err
                            );
                        }
                        if output.rescannable && self.is_retryable_write_error(&e) {
                            retry_counter.increment_from_error(e)?;
                            tokio::time::sleep(retry_counter.next_sleep_time()).await;
                            continue;
                        }
                        return Err(e);
                    }
                },
                Err(e) => {
                    if let Err(abort_err) = self.abort_multipart_write(&upload_id).await {
                        log::warn!(
                            "Failed to abort multipart write {}: {}",
                            upload_id,
                            abort_err
                        );
                    }
                    if output.rescannable && self.is_retryable_write_error(&e) {
                        retry_counter.increment_from_error(e)?;
                        tokio::time::sleep(retry_counter.next_sleep_time()).await;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn execute_multipart_inserts(
        &self,
        upload_id: &str,
        output: &PreprocessingOutput,
        num_partitions: usize,
    ) -> Result<()> {
        debug_assert!(
            output.rescannable,
            "multipart inserts require rescannable input for retry support"
        );

        let plan = Arc::new(
            datafusion_physical_plan::repartition::RepartitionExec::try_new(
                output.plan.clone(),
                datafusion_physical_plan::Partitioning::RoundRobinBatch(num_partitions),
            )?,
        ) as Arc<dyn ExecutionPlan>;

        let insert = Arc::new(RemoteInsertExec::new_multipart(
            self.name.clone(),
            self.identifier.clone(),
            self.client.clone(),
            plan,
            output.overwrite,
            upload_id.to_string(),
            output.tracker.clone(),
            self.branch.clone(),
            self.client.max_bytes_per_request(),
            self.client.max_request_duration(),
        ));

        let task_ctx = Arc::new(datafusion_execution::TaskContext::default());
        let tracker = output.tracker.clone();
        let mut join_set = tokio::task::JoinSet::new();
        for partition in 0..num_partitions {
            let exec = insert.clone();
            let ctx = task_ctx.clone();
            let tracker = tracker.clone();
            join_set.spawn(async move {
                let _guard = tracker.as_ref().map(|t| t.track_task());
                let mut stream = exec
                    .execute(partition, ctx)
                    .map_err(|e| -> Error { e.into() })?;
                while let Some(batch) = stream.next().await {
                    batch.map_err(|e| -> Error { e.into() })?;
                }
                Ok::<_, Error>(())
            });
        }

        // JoinSet aborts all remaining tasks when dropped, so if we return
        // early on error the orphaned tasks are automatically cancelled.
        while let Some(result) = join_set.join_next().await {
            result.map_err(|e| Error::Runtime {
                message: format!("Insert task panicked: {}", e),
            })??;
        }

        Ok(())
    }
}

/// Deserialize an index's `created_at` field.
///
/// The server returns this as an RFC 3339 string (e.g. `"2026-06-18T21:37:36.637Z"`),
/// but older deployments sent a unix timestamp in milliseconds. Accept both so the
/// client works against any server version.
fn deserialize_created_at<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<DateTime<Utc>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum CreatedAt {
        Rfc3339(String),
        Millis(i64),
    }

    match Option::<CreatedAt>::deserialize(deserializer)? {
        None => Ok(None),
        Some(CreatedAt::Rfc3339(s)) => DateTime::parse_from_rfc3339(&s)
            .map(|dt| Some(dt.with_timezone(&Utc)))
            .map_err(D::Error::custom),
        Some(CreatedAt::Millis(ms)) => Ok(DateTime::from_timestamp_millis(ms)),
    }
}

impl<S: HttpSend + 'static> RemoteTable<S> {
    /// Parse the response from `/index/list/` into `IndexConfig` entries.
    ///
    /// When the server returns `index_type` inline, all enriched fields are
    /// used directly and no further requests are made. When `index_type` is
    /// absent (legacy servers), a `/index/{name}/stats/` call is made for each
    /// index to retrieve the type.
    async fn parse_index_list_response(
        &self,
        body: &str,
        request_id: &str,
        schema: &SchemaRef,
    ) -> Result<Vec<IndexConfig>> {
        use crate::index::IndexType;

        #[derive(Deserialize)]
        struct ListIndicesResponse {
            indexes: Vec<IndexListEntry>,
        }

        #[derive(Deserialize)]
        struct IndexListEntry {
            index_name: String,
            columns: Vec<String>,
            // Present on enriched responses; absent on legacy servers.
            // Used as the sentinel to decide whether to skip the stats call.
            index_type: Option<IndexType>,
            index_uuid: Option<String>,
            #[serde(default, deserialize_with = "deserialize_created_at")]
            created_at: Option<DateTime<Utc>>,
            num_indexed_rows: Option<u64>,
            num_unindexed_rows: Option<u64>,
            size_bytes: Option<u64>,
            num_segments: Option<u32>,
            index_version: Option<i32>,
            index_details: Option<String>,
            type_url: Option<String>,
        }

        let response: ListIndicesResponse =
            serde_json::from_str(body).map_err(|err| Error::Http {
                source: format!(
                    "Failed to parse list_indices response: {}, body: {}",
                    err, body
                )
                .into(),
                request_id: request_id.to_string(),
                status_code: None,
            })?;

        let mut futures = Vec::with_capacity(response.indexes.len());
        for entry in response.indexes {
            let columns = entry
                .columns
                .iter()
                .map(|column| {
                    resolve_arrow_field_path(schema, column)
                        .map(|(canonical_column, _)| canonical_column)
                })
                .collect::<Result<Vec<_>>>()?;

            let future = async move {
                if let Some(index_type) = entry.index_type {
                    // Enriched response: all fields available, no stats call needed.
                    Ok(Some(IndexConfig {
                        name: entry.index_name,
                        index_type,
                        columns,
                        index_uuid: entry.index_uuid,
                        type_url: entry.type_url,
                        created_at: entry.created_at,
                        num_indexed_rows: entry.num_indexed_rows,
                        num_unindexed_rows: entry.num_unindexed_rows,
                        size_bytes: entry.size_bytes,
                        num_segments: entry.num_segments,
                        index_version: entry.index_version,
                        index_details: entry.index_details,
                    }))
                } else {
                    // Legacy response: fetch index type via stats endpoint.
                    match self.index_stats(&entry.index_name).await {
                        Ok(Some(stats)) => Ok(Some(IndexConfig {
                            name: entry.index_name,
                            index_type: stats.index_type,
                            columns,
                            index_uuid: None,
                            type_url: None,
                            created_at: None,
                            num_indexed_rows: None,
                            num_unindexed_rows: None,
                            size_bytes: None,
                            num_segments: None,
                            index_version: None,
                            index_details: None,
                        })),
                        Ok(None) => Ok(None), // Index deleted since we listed it.
                        Err(e) => Err(e),
                    }
                }
            };
            futures.push(future);
        }

        let results = futures::future::try_join_all(futures).await?;
        Ok(results.into_iter().flatten().collect())
    }
}

#[async_trait]
impl<S: HttpSend> BaseTable for RemoteTable<S> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }

    fn namespace(&self) -> &[String] {
        &self.namespace
    }

    fn id(&self) -> &str {
        &self.identifier
    }
    async fn version(&self) -> Result<u64> {
        self.describe().await.map(|desc| desc.version)
    }
    async fn checkout(&self, version: u64) -> Result<()> {
        // Validate the version exists. The describe is sent without freshness
        // headers so a stale `min_version` from a previous write doesn't ride
        // along on an explicit time-travel request.
        let request = self
            .client
            .post(&format!("/v1/table/{}/describe/", self.identifier));
        self.describe_with_request(request, Some(version))
            .await
            .map_err(|e| match e {
                // try to map the error to a more user-friendly error telling them
                // specifically that the version does not exist
                Error::TableNotFound { name, source } => Error::TableNotFound {
                    name: format!("{} (version: {})", name, version),
                    source,
                },
                e => e,
            })?;

        let mut write_guard = self.version.write().await;
        *write_guard = Some(version);
        drop(write_guard);

        // Explicit time-travel: drop any read-your-write / freshness
        // constraints so the user sees exactly the requested version.
        *self.freshness.lock().unwrap() = FreshnessState::default();

        // Invalidate schema cache since we're switching versions
        self.invalidate_schema_cache();

        Ok(())
    }
    async fn checkout_latest(&self) -> Result<()> {
        let mut write_guard = self.version.write().await;
        *write_guard = None;
        drop(write_guard);

        // Drop any per-handle read/write tracking; subsequent reads use the
        // baseline timestamp captured now to guarantee freshness.
        *self.freshness.lock().unwrap() = FreshnessState {
            min_version: None,
            checkout_baseline: Some(SystemTime::now()),
            min_read_version: None,
        };

        // Invalidate schema cache since we're switching versions
        self.invalidate_schema_cache();

        Ok(())
    }
    async fn restore(&self) -> Result<()> {
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/restore/", self.identifier));
        let version = self.current_version().await;
        let mut body = serde_json::json!({ "version": version });
        self.apply_branch_body(&mut body);
        request = request.json(&body);

        let (request_id, response) = self.send(request, true).await?;
        self.check_table_response(&request_id, response).await?;
        self.checkout_latest().await?;
        Ok(())
    }

    async fn list_versions(&self) -> Result<Vec<Version>> {
        let request = self.apply_branch_query(
            self.post_read(&format!("/v1/table/{}/version/list/", self.identifier)),
        );
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;

        #[derive(Deserialize)]
        struct ListVersionsResponse {
            versions: Vec<Version>,
        }

        let body = response.text().await.err_to_http(request_id.clone())?;
        let body: ListVersionsResponse =
            serde_json::from_str(&body).map_err(|err| Error::Http {
                source: format!(
                    "Failed to parse list_versions response: {}, body: {}",
                    err, body
                )
                .into(),
                request_id,
                status_code: None,
            })?;

        Ok(body.versions)
    }

    async fn schema(&self) -> Result<SchemaRef> {
        if let Some(schema) = self.schema_cache.try_get() {
            return Ok(schema);
        }

        let version = self.current_version().await;
        let client = self.client.clone();
        let identifier = self.identifier.clone();
        let table_name = self.name.clone();
        let branch = self.branch.clone();
        let freshness_headers = self.snapshot_freshness_headers();

        self.schema_cache
            .get(move || async move {
                fetch_schema(
                    &client,
                    &identifier,
                    &table_name,
                    version,
                    branch,
                    freshness_headers,
                )
                .await
            })
            .await
            .map_err(unwrap_shared_error)
    }

    async fn create_branch(
        &self,
        name: &str,
        from: lance::dataset::refs::Ref,
    ) -> Result<Arc<dyn BaseTable>> {
        use lance::dataset::refs::Ref;

        if name.trim().is_empty() {
            return Err(Error::InvalidInput {
                message: "branch name must be a non-empty string".into(),
            });
        }

        // Translate the source ref into the `from_branch` / `from_version` the
        // `/branches/create` contract accepts (it has no `from_tag`).
        let (from_branch, from_version) = match from {
            Ref::Version(branch, version) => (normalize_branch(branch), version),
            Ref::VersionNumber(version) => (normalize_branch(self.branch.clone()), Some(version)),
            Ref::Tag(tag) => {
                let (branch, version) = self.resolve_tag_ref(&tag).await?;
                (branch, Some(version))
            }
        };

        let mut body = serde_json::json!({ "name": name });
        if let Some(from_branch) = &from_branch {
            body["from_branch"] = serde_json::Value::String(from_branch.clone());
        }
        if let Some(from_version) = from_version {
            body["from_version"] = serde_json::json!(from_version);
        }

        let request = self
            .client
            .post(&format!("/v1/table/{}/branches/create/", self.identifier))
            .json(&body);

        // Send without retry so the expected 409 (branch already exists) is
        // surfaced as a response we can map, rather than being retried.
        let (request_id, response) = self.send(request, false).await?;
        match response.status() {
            StatusCode::CONFLICT => {
                return Err(Error::TableAlreadyExists {
                    name: format!("{} (branch: {})", self.name, name),
                });
            }
            StatusCode::BAD_REQUEST => {
                let body = response.text().await.unwrap_or_default();
                return Err(Error::InvalidInput {
                    message: format!("invalid create_branch request: {}", body),
                });
            }
            StatusCode::NOT_FOUND => {
                // 404 covers both a missing table and a missing source ref; name
                // the source coordinate so the error isn't misattributed to the table.
                let body = response.text().await.unwrap_or_default();
                let source_desc = match (&from_branch, from_version) {
                    (Some(b), Some(v)) => format!(" (source: branch '{b}' version {v})"),
                    (Some(b), None) => format!(" (source: branch '{b}')"),
                    (None, Some(v)) => format!(" (source: version {v})"),
                    (None, None) => String::new(),
                };
                return Err(Error::TableNotFound {
                    name: format!("{}{}", self.name, source_desc),
                    source: Box::new(Error::Http {
                        source: body.into(),
                        request_id,
                        status_code: Some(StatusCode::NOT_FOUND),
                    }),
                });
            }
            _ => {}
        }
        self.check_table_response(&request_id, response).await?;

        Ok(Arc::new(self.with_branch(Some(name.to_string()))))
    }

    async fn checkout_branch(&self, name: &str) -> Result<Arc<dyn BaseTable>> {
        // `main` / empty normalizes to the main-branch handle.
        let Some(branch) = normalize_branch(Some(name.to_string())) else {
            return Ok(Arc::new(self.with_branch(None)));
        };

        // Validate via listing -- the cheapest check that distinguishes a missing
        // branch from a missing table.
        let branches = self.list_branches().await?;
        if !branches.contains_key(&branch) {
            return Err(Error::TableNotFound {
                name: format!("{} (branch: {})", self.name, branch),
                source: format!("branch '{}' does not exist", branch).into(),
            });
        }

        Ok(Arc::new(self.with_branch(Some(branch))))
    }

    async fn list_branches(&self) -> Result<HashMap<String, lance::dataset::refs::BranchContents>> {
        use lance::dataset::refs::BranchContents;

        let request = self.post_read(&format!("/v1/table/{}/branches/list/", self.identifier));
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        #[derive(Deserialize)]
        struct ListBranchesResponse {
            branches: HashMap<String, BranchContents>,
        }

        let parsed: ListBranchesResponse =
            serde_json::from_str(&body).map_err(|err| Error::Http {
                source: format!(
                    "Failed to parse list_branches response: {}, body: {}",
                    err, body
                )
                .into(),
                request_id,
                status_code: None,
            })?;

        Ok(parsed.branches)
    }

    async fn delete_branch(&self, name: &str) -> Result<()> {
        if name.trim().is_empty() {
            return Err(Error::InvalidInput {
                message: "branch name must be a non-empty string".into(),
            });
        }
        let request = self
            .client
            .post(&format!("/v1/table/{}/branches/delete/", self.identifier))
            .json(&serde_json::json!({ "name": name }));
        let (request_id, response) = self.send(request, true).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::TableNotFound {
                name: format!("{} (branch: {})", self.name, name),
                source: format!("branch '{}' does not exist", name).into(),
            });
        }
        self.check_table_response(&request_id, response).await?;
        Ok(())
    }

    fn current_branch(&self) -> Option<String> {
        self.branch.clone()
    }

    async fn count_rows(&self, filter: Option<Filter>) -> Result<usize> {
        let mut request = self.post_read(&format!("/v1/table/{}/count_rows/", self.identifier));

        let version = self.current_version().await;

        let mut body = if let Some(filter) = filter {
            let filter_sql = match filter {
                Filter::Sql(sql) => sql.clone(),
                Filter::Datafusion(expr) => expr_to_sql_string(&expr)?,
            };
            serde_json::json!({ "predicate": filter_sql, "version": version })
        } else {
            serde_json::json!({ "version": version })
        };
        self.apply_branch_body(&mut body);
        request = request.json(&body);

        let (request_id, response) = match self.send(request, true).await {
            Ok((id, resp)) => {
                // check_table_response now handles error-based invalidation
                let response = self.check_table_response(&id, resp).await?;
                (id, response)
            }
            Err(e) => {
                self.handle_error_invalidation(&e);
                return Err(e);
            }
        };

        self.track_read_version_from_headers(response.headers());
        let body = response.text().await.err_to_http(request_id.clone())?;

        serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse row count: {}", e).into(),
            request_id,
            status_code: None,
        })
    }
    async fn add(&self, mut add: AddDataBuilder) -> Result<AddResult> {
        self.check_mutable().await?;

        let table_schema = self.schema().await?;
        let table_def = TableDefinition::try_from_rich_schema(table_schema.clone())?;

        let num_partitions = if let Some(parallelism) = add.write_parallelism {
            if parallelism > 1 && self.server_version.support_multipart_write() {
                parallelism
            } else {
                1
            }
        } else if self.server_version.support_multipart_write() {
            // Peek at the first batch to estimate write partitions, same as NativeTable.
            let mut peeked = PeekedScannable::new(add.data);
            let n = if let Some(first_batch) = peeked.peek().await {
                let max_partitions = lance_core::utils::tokio::get_num_compute_intensive_cpus();
                estimate_write_partitions(
                    first_batch.get_array_memory_size(),
                    first_batch.num_rows(),
                    peeked.num_rows(),
                    max_partitions,
                )
            } else {
                1
            };
            add.data = Box::new(peeked);
            n
        } else {
            1
        };

        let output = add.into_plan(&table_schema, &table_def)?;

        if let Some(ref t) = output.tracker {
            t.set_total_tasks(num_partitions);
        }
        let _finish = FinishOnDrop(output.tracker.clone());

        if num_partitions > 1 {
            self.add_multipart(output, num_partitions).await
        } else {
            self.add_single_partition(output).await
        }
    }

    async fn create_plan(
        &self,
        query: &AnyQuery,
        options: QueryExecutionOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let streams = self.execute_query(query, &options).await?;
        if streams.len() == 1 {
            let stream = streams.into_iter().next().unwrap();
            Ok(Arc::new(OneShotExec::new(stream)))
        } else {
            let stream_execs = streams
                .into_iter()
                .map(|stream| Arc::new(OneShotExec::new(stream)) as Arc<dyn ExecutionPlan>)
                .collect();
            create_multi_vector_plan(stream_execs)
        }
    }

    async fn query(
        &self,
        query: &AnyQuery,
        options: QueryExecutionOptions,
    ) -> Result<DatasetRecordBatchStream> {
        let streams = self.execute_query(query, &options).await?;

        if streams.len() == 1 {
            Ok(DatasetRecordBatchStream::new(
                streams.into_iter().next().unwrap(),
            ))
        } else {
            let stream_execs = streams
                .into_iter()
                .map(|stream| Arc::new(OneShotExec::new(stream)) as Arc<dyn ExecutionPlan>)
                .collect();
            let plan = create_multi_vector_plan(stream_execs)?;

            Ok(DatasetRecordBatchStream::new(execute_plan(
                plan,
                Default::default(),
            )?))
        }
    }

    async fn explain_plan(&self, query: &AnyQuery, verbose: bool) -> Result<String> {
        let base_request = self.post_read(&format!("/v1/table/{}/explain_plan/", self.identifier));

        let query_bodies = self.prepare_query_bodies(query).await?;
        let requests: Vec<reqwest::RequestBuilder> = query_bodies
            .into_iter()
            .map(|query_body| {
                let explain_request = serde_json::json!({
                    "verbose": verbose,
                    "query": query_body
                });

                base_request.try_clone().unwrap().json(&explain_request)
            })
            .collect::<Vec<_>>();

        let futures = requests.into_iter().map(|req| async move {
            let (request_id, response) = self.send(req, true).await?;
            let response = self.check_table_response(&request_id, response).await?;
            let body = response.text().await.err_to_http(request_id.clone())?;

            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to parse explain plan: {}", e).into(),
                request_id,
                status_code: None,
            })
        });

        let plan_texts = futures::future::try_join_all(futures).await?;
        let final_plan = if plan_texts.len() > 1 {
            plan_texts
                .into_iter()
                .enumerate()
                .map(|(i, plan)| format!("--- Plan #{} ---\n{}", i + 1, plan))
                .collect::<Vec<_>>()
                .join("\n\n")
        } else {
            plan_texts.into_iter().next().unwrap_or_default()
        };

        Ok(final_plan)
    }

    async fn analyze_plan(
        &self,
        query: &AnyQuery,
        options: QueryExecutionOptions,
    ) -> Result<String> {
        let mut request = self.post_read(&format!("/v1/table/{}/analyze_plan/", self.identifier));

        if options.analyze_plan_distributed_metrics != AnalyzePlanDistributedMetrics::Aggregate {
            request = request.query(&[(
                "distributed_metrics",
                options.analyze_plan_distributed_metrics.as_query_param(),
            )]);
        }

        let query_bodies = self.prepare_query_bodies(query).await?;
        let requests: Vec<reqwest::RequestBuilder> = query_bodies
            .into_iter()
            .map(|body| request.try_clone().unwrap().json(&body))
            .collect();

        let futures = requests.into_iter().map(|req| async move {
            let (request_id, response) = self.send(req, true).await?;
            let response = self.check_table_response(&request_id, response).await?;
            let body = response.text().await.err_to_http(request_id.clone())?;

            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to execute analyze plan: {}", e).into(),
                request_id,
                status_code: None,
            })
        });

        let analyze_result_texts = futures::future::try_join_all(futures).await?;
        let final_analyze = if analyze_result_texts.len() > 1 {
            analyze_result_texts
                .into_iter()
                .enumerate()
                .map(|(i, plan)| format!("--- Query #{} ---\n{}", i + 1, plan))
                .collect::<Vec<_>>()
                .join("\n\n")
        } else {
            analyze_result_texts.into_iter().next().unwrap_or_default()
        };

        Ok(final_analyze)
    }

    async fn update(&self, update: UpdateBuilder) -> Result<UpdateResult> {
        self.check_mutable().await?;
        let request = self
            .client
            .post(&format!("/v1/table/{}/update/", self.identifier));

        let mut updates = Vec::new();
        for (column, expression) in update.columns {
            updates.push(vec![column, expression]);
        }

        let mut body = serde_json::json!({
            "updates": updates,
            "predicate": update.filter,
        });
        self.apply_branch_body(&mut body);
        let request = request.json(&body);

        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(UpdateResult {
                rows_updated: 0,
                version: 0,
            });
        }

        let update_response: UpdateResult =
            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to parse update response: {}", e).into(),
                request_id,
                status_code: None,
            })?;

        self.track_write_version(update_response.version);
        Ok(update_response)
    }

    async fn delete(&self, predicate: Predicate<'_>) -> Result<DeleteResult> {
        self.check_mutable().await?;
        let predicate_sql = match predicate {
            Predicate::String(s) => s.to_string(),
            Predicate::Expr(expr) => expr_to_sql_string(expr)?,
        };
        let mut body = serde_json::json!({ "predicate": predicate_sql });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!("/v1/table/{}/delete/", self.identifier))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(DeleteResult {
                num_deleted_rows: 0,
                version: 0,
            });
        }
        let delete_response: DeleteResult =
            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to parse delete response: {}", e).into(),
                request_id,
                status_code: None,
            })?;
        self.track_write_version(delete_response.version);
        Ok(delete_response)
    }

    async fn create_index(&self, mut index: IndexBuilder) -> Result<()> {
        self.check_mutable().await?;
        let request = self
            .client
            .post(&format!("/v1/table/{}/create_index/", self.identifier));

        let column = match index.columns.len() {
            0 => {
                return Err(Error::InvalidInput {
                    message: "No columns specified".into(),
                });
            }
            1 => index.columns.pop().unwrap(),
            _ => {
                return Err(Error::NotSupported {
                    message: "Indices over multiple columns not yet supported".into(),
                });
            }
        };
        let schema = self.schema().await?;
        let (canonical_column, field) = resolve_arrow_field_path(&schema, &column)?;
        let mut body = serde_json::json!({
            "column": canonical_column
        });

        // Add name parameter if provided (for backwards compatibility, only include if Some)
        if let Some(ref name) = index.name {
            body["name"] = serde_json::Value::String(name.clone());
        }

        // Warn if train=false is specified since it's not meaningful
        if !index.train {
            log::warn!(
                "train=false has no effect remote tables. The index will be created empty and automatically populated in the background."
            );
        }

        fn to_json(params: &impl serde::Serialize) -> crate::Result<serde_json::Value> {
            serde_json::to_value(params).map_err(|e| Error::InvalidInput {
                message: format!("failed to serialize index params {:?}", e),
            })
        }

        // Map each Index variant to its wire type name and serializable params.
        // Auto is special-cased since it needs schema inspection.
        let (index_type_str, params) = match &index.index {
            Index::IvfFlat(p) => ("IVF_FLAT", Some(to_json(p)?)),
            Index::IvfPq(p) => ("IVF_PQ", Some(to_json(p)?)),
            Index::IvfSq(p) => ("IVF_SQ", Some(to_json(p)?)),
            Index::IvfHnswSq(p) => ("IVF_HNSW_SQ", Some(to_json(p)?)),
            Index::IvfHnswFlat(p) => ("IVF_HNSW_FLAT", Some(to_json(p)?)),
            Index::IvfRq(p) => ("IVF_RQ", Some(to_json(p)?)),
            Index::BTree(p) => ("BTREE", Some(to_json(p)?)),
            Index::Bitmap(p) => ("BITMAP", Some(to_json(p)?)),
            Index::LabelList(p) => ("LABEL_LIST", Some(to_json(p)?)),
            Index::Fm(p) => ("FM", Some(to_json(p)?)),
            Index::FTS(p) => ("FTS", Some(to_json(p)?)),
            Index::Auto => {
                if supported_vector_data_type(field.data_type()) {
                    body[METRIC_TYPE_KEY] =
                        serde_json::Value::String(DistanceType::L2.to_string().to_lowercase());
                    ("IVF_PQ", None)
                } else if supported_btree_data_type(field.data_type()) {
                    ("BTREE", None)
                } else {
                    return Err(Error::NotSupported {
                        message: format!(
                            "there are no indices supported for the field `{}` with the data type {}",
                            field.name(),
                            field.data_type()
                        ),
                    });
                }
            }
            _ => {
                return Err(Error::NotSupported {
                    message: "Index type not supported".into(),
                });
            }
        };

        body[INDEX_TYPE_KEY] = index_type_str.into();
        if let Some(params) = params {
            for (key, value) in params.as_object().expect("params should be a JSON object") {
                body[key] = value.clone();
            }
        }
        self.apply_branch_body(&mut body);

        let request = request.json(&body);

        let (request_id, response) = self.send(request, true).await?;

        self.check_table_response(&request_id, response).await?;

        if let Some(wait_timeout) = index.wait_timeout {
            let index_name = index.name.unwrap_or_else(|| format!("{}_idx", column));
            self.wait_for_index(&[&index_name], wait_timeout).await?;
        }

        Ok(())
    }

    /// Poll until the columns are fully indexed. Will return Error::Timeout if the columns
    /// are not fully indexed within the timeout.
    async fn wait_for_index(&self, index_names: &[&str], timeout: Duration) -> Result<()> {
        wait_for_index(self, index_names, timeout).await
    }

    async fn merge_insert(
        &self,
        params: MergeInsertBuilder,
        new_data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<MergeResult> {
        self.check_mutable().await?;

        let timeout = params.timeout;

        let query = MergeInsertRequest::try_from(params)?;
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/merge_insert/", self.identifier))
            .query(&query)
            .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE);
        request = self.apply_branch_query(request);

        if let Some(timeout) = timeout {
            // (If it doesn't fit into u64, it's not worth sending anyways.)
            if let Ok(timeout_ms) = u64::try_from(timeout.as_millis()) {
                request = request.header(REQUEST_TIMEOUT_HEADER, timeout_ms);
            }
        }

        let (request_id, response) = self.send_streaming(request, new_data, true).await?;

        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(MergeResult {
                version: 0,
                num_deleted_rows: 0,
                num_inserted_rows: 0,
                num_updated_rows: 0,
                num_attempts: 0,
                num_rows: 0,
            });
        }

        let merge_insert_response: MergeResult =
            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to parse merge_insert response: {}", e).into(),
                request_id,
                status_code: None,
            })?;

        self.track_write_version(merge_insert_response.version);
        Ok(merge_insert_response)
    }

    async fn set_unenforced_primary_key(&self, _columns: &[&str]) -> Result<()> {
        Err(Error::NotSupported {
            message: "set_unenforced_primary_key is not supported on LanceDB cloud.".into(),
        })
    }

    async fn set_lsm_write_spec(&self, spec: LsmWriteSpec) -> Result<()> {
        self.check_mutable().await?;

        // Map the spec onto the server's request DTO. `sharding` is internally
        // tagged on `mode` to mirror sophon's `Sharding` enum; `maintained_indexes`
        // and `writer_config_defaults` are sent verbatim (an empty list means "no
        // maintained indexes", not "default to all").
        let sharding = match &spec {
            LsmWriteSpec::Bucket {
                column,
                num_buckets,
                ..
            } => serde_json::json!({
                "mode": "bucket",
                "column": column,
                "num_buckets": num_buckets,
            }),
            LsmWriteSpec::Identity { column, .. } => serde_json::json!({
                "mode": "identity",
                "column": column,
            }),
            LsmWriteSpec::Unsharded { .. } => serde_json::json!({ "mode": "unsharded" }),
        };
        let body = serde_json::json!({
            "sharding": sharding,
            "maintained_indexes": spec.maintained_indexes(),
            "writer_config_defaults": spec.writer_config_defaults(),
        });

        let request = self
            .client
            .post(&format!(
                "/v1/table/{}/set_lsm_write_spec/",
                self.identifier
            ))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        self.check_table_response(&request_id, response).await?;
        Ok(())
    }

    async fn unset_lsm_write_spec(&self) -> Result<()> {
        self.check_mutable().await?;
        let request = self.client.post(&format!(
            "/v1/table/{}/unset_lsm_write_spec/",
            self.identifier
        ));
        let (request_id, response) = self.send(request, true).await?;
        self.check_table_response(&request_id, response).await?;
        Ok(())
    }

    async fn get_lsm_write_spec(&self) -> Result<Option<LsmWriteSpec>> {
        // Read counterpart to set/unset, resolved server-side against HEAD. The
        // server reads the spec from the `__lance_mem_wal` system index (shard
        // column mapped from its Lance field id against the current schema) and
        // re-encodes it into the same sophon-owned shape the set endpoint
        // accepts — no lance/lancedb types cross the wire. `lsm_write_spec` is
        // null when the LSM write path is not enabled for the table.
        let request = self.post_read(&format!(
            "/v1/table/{}/get_lsm_write_spec/",
            self.identifier
        ));
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        // Mirror of sophon's `Sharding` (internally tagged on `mode`) and
        // `LsmWriteSpecBody` / `GetLsmWriteSpecResponse`.
        #[derive(Deserialize)]
        #[serde(tag = "mode", rename_all = "snake_case")]
        enum Sharding {
            Unsharded,
            Bucket { column: String, num_buckets: u32 },
            Identity { column: String },
        }
        #[derive(Deserialize)]
        struct LsmWriteSpecBody {
            sharding: Sharding,
            #[serde(default)]
            maintained_indexes: Vec<String>,
            #[serde(default)]
            writer_config_defaults: std::collections::HashMap<String, String>,
        }
        #[derive(Deserialize)]
        struct GetLsmWriteSpecResponse {
            lsm_write_spec: Option<LsmWriteSpecBody>,
        }

        let parsed: GetLsmWriteSpecResponse =
            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to parse get_lsm_write_spec response: {}", e).into(),
                request_id,
                status_code: None,
            })?;

        let Some(body) = parsed.lsm_write_spec else {
            // The LSM write path is not enabled for this table.
            return Ok(None);
        };

        let spec = match body.sharding {
            Sharding::Bucket {
                column,
                num_buckets,
            } => LsmWriteSpec::bucket(column, num_buckets),
            Sharding::Identity { column } => LsmWriteSpec::identity(column),
            Sharding::Unsharded => LsmWriteSpec::unsharded(),
        }
        .with_maintained_indexes(body.maintained_indexes)
        .with_writer_config_defaults(body.writer_config_defaults);

        Ok(Some(spec))
    }

    async fn tags(&self) -> Result<Box<dyn Tags + '_>> {
        Ok(Box::new(RemoteTags { inner: self }))
    }
    async fn checkout_tag(&self, tag: &str) -> Result<()> {
        // Resolve the tag without attaching freshness headers; a stale
        // `min_version` from a previous write should not ride along on an
        // explicit time-travel request.
        let request = self
            .client
            .post(&format!("/v1/table/{}/tags/version/", self.identifier));
        let version = self.resolve_tag_version_with_request(tag, request).await?;

        let mut write_guard = self.version.write().await;
        *write_guard = Some(version);
        drop(write_guard);

        // Explicit time-travel: drop any read-your-write / freshness
        // constraints so the user sees exactly the tagged version.
        *self.freshness.lock().unwrap() = FreshnessState::default();

        // Invalidate schema cache since we're switching versions
        self.invalidate_schema_cache();

        Ok(())
    }
    async fn optimize(&self, _action: OptimizeAction) -> Result<OptimizeStats> {
        self.check_mutable().await?;
        Err(Error::NotSupported {
            message: "optimize is not supported on LanceDB cloud.".into(),
        })
    }
    async fn add_columns(
        &self,
        transforms: NewColumnTransform,
        _read_columns: Option<Vec<String>>,
    ) -> Result<AddColumnsResult> {
        self.check_mutable().await?;
        match transforms {
            NewColumnTransform::SqlExpressions(expressions) => {
                let body = expressions
                    .into_iter()
                    .map(|(name, expression)| {
                        serde_json::json!({
                            "name": name,
                            "expression": expression,
                        })
                    })
                    .collect::<Vec<_>>();
                let mut body = serde_json::json!({ "new_columns": body });
                self.apply_branch_body(&mut body);
                let request = self
                    .client
                    .post(&format!("/v1/table/{}/add_columns/", self.identifier))
                    .json(&body);
                let (request_id, response) = self.send(request, true).await?;
                let response = self.check_table_response(&request_id, response).await?;
                let body = response.text().await.err_to_http(request_id.clone())?;

                if body.trim().is_empty() {
                    // Backward compatible with old servers
                    return Ok(AddColumnsResult { version: 0 });
                }

                let result: AddColumnsResult =
                    serde_json::from_str(&body).map_err(|e| Error::Http {
                        source: format!("Failed to parse add_columns response: {}", e).into(),
                        request_id,
                        status_code: None,
                    })?;

                self.invalidate_schema_cache();
                self.track_write_version(result.version);

                Ok(result)
            }
            _ => {
                return Err(Error::NotSupported {
                    message: "Only SQL expressions are supported for adding columns".into(),
                });
            }
        }
    }

    async fn alter_columns(&self, alterations: &[ColumnAlteration]) -> Result<AlterColumnsResult> {
        self.check_mutable().await?;
        let body = alterations
            .iter()
            .map(|alteration| {
                let mut value = serde_json::json!({
                    "path": alteration.path,
                });
                if let Some(rename) = &alteration.rename {
                    value["rename"] = serde_json::Value::String(rename.clone());
                }
                if let Some(data_type) = &alteration.data_type {
                    let json_data_type = JsonDataType::try_from(data_type).unwrap();
                    let json_data_type = serde_json::to_value(&json_data_type).unwrap();
                    value["data_type"] = json_data_type;
                }
                if let Some(nullable) = &alteration.nullable {
                    value["nullable"] = serde_json::Value::Bool(*nullable);
                }
                value
            })
            .collect::<Vec<_>>();
        let mut body = serde_json::json!({ "alterations": body });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!("/v1/table/{}/alter_columns/", self.identifier))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(AlterColumnsResult { version: 0 });
        }

        let result: AlterColumnsResult = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse alter_columns response: {}", e).into(),
            request_id,
            status_code: None,
        })?;

        self.invalidate_schema_cache();
        self.track_write_version(result.version);

        Ok(result)
    }

    async fn update_field_metadata(
        &self,
        updates: &[FieldMetadataUpdate],
    ) -> Result<UpdateFieldMetadataResult> {
        self.check_mutable().await?;
        let mut body = serde_json::json!({ "updates": updates });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!(
                "/v1/table/{}/update_field_metadata/",
                self.identifier
            ))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        let result: UpdateFieldMetadataResult =
            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to parse update_field_metadata response: {}", e).into(),
                request_id,
                status_code: None,
            })?;

        self.invalidate_schema_cache();
        self.track_write_version(result.version);
        Ok(result)
    }

    async fn drop_columns(&self, columns: &[&str]) -> Result<DropColumnsResult> {
        self.check_mutable().await?;
        let mut body = serde_json::json!({ "columns": columns });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!("/v1/table/{}/drop_columns/", self.identifier))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(DropColumnsResult { version: 0 });
        }

        let result: DropColumnsResult = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse drop_columns response: {}", e).into(),
            request_id,
            status_code: None,
        })?;

        self.invalidate_schema_cache();
        self.track_write_version(result.version);

        Ok(result)
    }

    async fn list_indices(&self) -> Result<Vec<IndexConfig>> {
        let mut request = self.post_read(&format!("/v1/table/{}/index/list/", self.identifier));
        let version = self.current_version().await;
        let mut body = serde_json::json!({ "version": version });
        self.apply_branch_body(&mut body);
        request = request.json(&body);

        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let schema = self.schema().await?;

        self.parse_index_list_response(&body, &request_id, &schema)
            .await
    }

    async fn index_stats(&self, index_name: &str) -> Result<Option<IndexStatistics>> {
        let mut request = self.post_read(&format!(
            "/v1/table/{}/index/{}/stats/",
            self.identifier, index_name
        ));
        let version = self.current_version().await;
        let mut body = serde_json::json!({ "version": version });
        self.apply_branch_body(&mut body);
        request = request.json(&body);

        let (request_id, response) = self.send(request, true).await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let response = self.check_table_response(&request_id, response).await?;

        let body = response.text().await.err_to_http(request_id.clone())?;

        let stats = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse index statistics: {}", e).into(),
            request_id,
            status_code: None,
        })?;

        Ok(Some(stats))
    }

    async fn drop_index(&self, index_name: &str) -> Result<()> {
        let request = self.apply_branch_query(self.client.post(&format!(
            "/v1/table/{}/index/{}/drop/",
            self.identifier, index_name
        )));
        let (request_id, response) = self.send(request, true).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::IndexNotFound {
                name: index_name.to_string(),
            });
        };
        self.client.check_response(&request_id, response).await?;
        Ok(())
    }

    async fn prewarm_index(&self, index_name: &str) -> Result<()> {
        let request = self.client.post(&format!(
            "/v1/table/{}/index/{}/prewarm/",
            self.identifier, index_name
        ));
        let (request_id, response) = self.send(request, true).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::IndexNotFound {
                name: index_name.to_string(),
            });
        }
        self.check_table_response(&request_id, response).await?;
        Ok(())
    }

    async fn prewarm_data(&self, columns: Option<Vec<String>>) -> Result<()> {
        let mut request = self.client.post(&format!(
            "/v1/table/{}/page_cache/prewarm/",
            self.identifier
        ));
        let body = serde_json::json!({
            "columns": columns.unwrap_or_default(),
        });
        request = request.json(&body);
        let (request_id, response) = self.send(request, true).await?;
        self.check_table_response(&request_id, response).await?;
        Ok(())
    }

    async fn table_definition(&self) -> Result<TableDefinition> {
        let schema = self.schema().await?;
        TableDefinition::try_from_rich_schema(schema)
    }
    async fn uri(&self) -> Result<String> {
        // Check if we already have the location cached
        {
            let location = self.location.read().await;
            if let Some(ref loc) = *location {
                return Ok(loc.clone());
            }
        }

        // Fetch from server via describe
        let description = self.describe().await?;
        let location = description.location.ok_or_else(|| Error::NotSupported {
            message: "Table URI not supported by the server".into(),
        })?;

        // Cache the location for future use
        {
            let mut cached_location = self.location.write().await;
            *cached_location = Some(location.clone());
        }

        Ok(location)
    }

    async fn storage_options(&self) -> Option<HashMap<String, String>> {
        None
    }

    async fn initial_storage_options(&self) -> Option<HashMap<String, String>> {
        None
    }

    async fn latest_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
        Ok(None)
    }

    async fn stats(&self) -> Result<TableStatistics> {
        let mut request = self.post_read(&format!("/v1/table/{}/stats/", self.identifier));
        if let Some(branch) = &self.branch {
            request = request.json(&serde_json::json!({ "branch": branch }));
        }
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        let stats = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse table statistics: {}", e).into(),
            request_id,
            status_code: None,
        })?;
        Ok(stats)
    }

    async fn create_insert_exec(
        &self,
        input: Arc<dyn ExecutionPlan>,
        write_params: lance::dataset::WriteParams,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let overwrite = matches!(write_params.mode, lance::dataset::WriteMode::Overwrite);
        Ok(Arc::new(insert::RemoteInsertExec::new(
            self.name.clone(),
            self.identifier.clone(),
            self.client.clone(),
            input,
            overwrite,
            None,
            self.branch.clone(),
        )))
    }
}

#[derive(Serialize)]
struct MergeInsertRequest {
    on: String,
    when_matched_update_all: bool,
    when_matched_update_all_filt: Option<String>,
    when_not_matched_insert_all: bool,
    when_not_matched_by_source_delete: bool,
    when_not_matched_by_source_delete_filt: Option<String>,
    // For backwards compatibility, only serialize use_index when it's false
    // (the default is true)
    #[serde(skip_serializing_if = "is_true")]
    use_index: bool,
}

fn is_true(b: &bool) -> bool {
    *b
}

impl TryFrom<MergeInsertBuilder> for MergeInsertRequest {
    type Error = Error;

    fn try_from(value: MergeInsertBuilder) -> Result<Self> {
        if value.on.is_empty() {
            return Err(Error::InvalidInput {
                message: "MergeInsertBuilder missing required 'on' field".into(),
            });
        } else if value.on.len() > 1 {
            return Err(Error::NotSupported {
                message: "MergeInsertBuilder only supports a single 'on' column".into(),
            });
        }
        let on = value.on[0].clone();

        let when_matched_update_all_filt = match value.when_matched_update_all_filt {
            Some(MergeFilter::Sql(sql)) => Some(sql),
            Some(MergeFilter::Expr(_)) => {
                return Err(Error::NotSupported {
                    message: "DataFusion expressions are not supported on remote tables".into(),
                });
            }
            None => None,
        };

        let when_not_matched_by_source_delete_filt =
            match value.when_not_matched_by_source_delete_filt {
                Some(MergeFilter::Sql(sql)) => Some(sql),
                Some(MergeFilter::Expr(_)) => {
                    return Err(Error::NotSupported {
                        message: "DataFusion expressions are not supported on remote tables".into(),
                    });
                }
                None => None,
            };

        Ok(Self {
            on,
            when_matched_update_all: value.when_matched_update_all,
            when_matched_update_all_filt,
            when_not_matched_insert_all: value.when_not_matched_insert_all,
            when_not_matched_by_source_delete: value.when_not_matched_by_source_delete,
            when_not_matched_by_source_delete_filt,
            // Only serialize use_index when it's false for backwards compatibility
            use_index: value.use_index,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use std::{collections::HashMap, pin::Pin};

    use super::*;

    use crate::remote::client::{ClientConfig, RetryConfig};
    use crate::table::{AddDataMode, FieldMetadataUpdate, FtsToken};

    use arrow::{array::AsArray, compute::concat_batches, datatypes::Int32Type};
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, record_batch};
    use arrow_schema::{DataType, Field, Schema};
    use chrono::{DateTime, Utc};
    use futures::{StreamExt, TryFutureExt, future::BoxFuture};
    use lance_index::scalar::inverted::query::MatchQuery;
    use lance_index::scalar::{FullTextSearchQuery, InvertedIndexParams};
    use reqwest::Body;
    use rstest::rstest;
    use serde_json::json;

    use crate::index::vector::{
        IvfFlatIndexBuilder, IvfHnswFlatIndexBuilder, IvfHnswSqIndexBuilder, IvfRqIndexBuilder,
        IvfSqIndexBuilder,
    };
    use crate::remote::JSON_CONTENT_TYPE;
    use crate::remote::db::DEFAULT_SERVER_VERSION;
    use crate::utils::background_cache::clock;
    use crate::{
        DistanceType, Error, Table,
        index::{Index, IndexStatistics, IndexType, vector::IvfPqIndexBuilder},
        query::{
            AnalyzePlanDistributedMetrics, ColumnOrdering, ExecutableQuery, QueryBase,
            QueryExecutionOptions,
        },
        remote::ARROW_FILE_CONTENT_TYPE,
    };

    #[tokio::test]
    async fn test_not_found() {
        let table = Table::new_with_handler("my_table", |_| {
            http::Response::builder()
                .status(404)
                .body("table my_table not found")
                .unwrap()
        });

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let example_data_for_add = || batch.clone();
        let example_data_for_merge = || -> Box<dyn RecordBatchReader + Send> {
            Box::new(RecordBatchIterator::new(
                [Ok(batch.clone())],
                batch.schema(),
            ))
        };

        // All endpoints should translate 404 to TableNotFound.
        let results: Vec<BoxFuture<'_, Result<()>>> = vec![
            Box::pin(table.version().map_ok(|_| ())),
            Box::pin(table.schema().map_ok(|_| ())),
            Box::pin(table.count_rows(None).map_ok(|_| ())),
            Box::pin(table.update().column("a", "a + 1").execute().map_ok(|_| ())),
            Box::pin(table.add(example_data_for_add()).execute().map_ok(|_| ())),
            Box::pin(
                table
                    .merge_insert(&["test"])
                    .execute(example_data_for_merge())
                    .map_ok(|_| ()),
            ),
            Box::pin(table.delete("false").map_ok(|_| ())),
            Box::pin(
                table
                    .add_columns(
                        NewColumnTransform::SqlExpressions(vec![("x".into(), "y".into())]),
                        None,
                    )
                    .map_ok(|_| ()),
            ),
            Box::pin(async {
                let alterations = vec![ColumnAlteration::new("x".into()).rename("y".into())];
                table.alter_columns(&alterations).await.map(|_| ())
            }),
            Box::pin(table.drop_columns(&["a"]).map_ok(|_| ())),
            // TODO: other endpoints.
        ];

        for result in results {
            let result = result.await;
            assert!(result.is_err());
            assert!(
                matches!(&result, &Err(Error::TableNotFound { ref name, .. }) if name == "my_table")
            );
            let full_error_report = snafu::Report::from_error(result.unwrap_err()).to_string();
            assert!(full_error_report.contains("table my_table not found"));
        }
    }

    #[tokio::test]
    async fn test_version() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/describe/");

            http::Response::builder()
                .status(200)
                .body(r#"{"version": 42, "schema": { "fields": [] }}"#)
                .unwrap()
        });

        let version = table.version().await.unwrap();
        assert_eq!(version, 42);
    }

    #[tokio::test]
    async fn test_schema() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/describe/");

            http::Response::builder()
                .status(200)
                .body(
                    r#"{"version": 42, "schema": {"fields": [
                    {"name": "a", "type": { "type": "int32" }, "nullable": false},
                    {"name": "b", "type": { "type": "string" }, "nullable": true}
                ], "metadata": {"key": "value"}}}"#,
                )
                .unwrap()
        });

        let expected = Arc::new(
            Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, true),
            ])
            .with_metadata([("key".into(), "value".into())].into()),
        );

        let schema = table.schema().await.unwrap();
        assert_eq!(schema, expected);
    }

    #[tokio::test]
    async fn test_count_rows() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/count_rows/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );
            assert_eq!(
                request.body().unwrap().as_bytes().unwrap(),
                br#"{"version":null}"#
            );

            http::Response::builder().status(200).body("42").unwrap()
        });

        let count = table.count_rows(None).await.unwrap();
        assert_eq!(count, 42);

        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/count_rows/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );
            assert_eq!(
                request.body().unwrap().as_bytes().unwrap(),
                br#"{"predicate":"a > 10","version":null}"#
            );

            http::Response::builder().status(200).body("42").unwrap()
        });

        let count = table.count_rows(Some("a > 10".into())).await.unwrap();
        assert_eq!(count, 42);
    }

    async fn collect_body(body: Body) -> Vec<u8> {
        use http_body::Body;
        let mut body = body;
        let mut data = Vec::new();
        let mut body_pin = Pin::new(&mut body);
        futures::stream::poll_fn(|cx| body_pin.as_mut().poll_frame(cx))
            .for_each(|frame| {
                data.extend_from_slice(frame.unwrap().data_ref().unwrap());
                futures::future::ready(())
            })
            .await;
        data
    }

    fn write_ipc_stream(data: &RecordBatch) -> Vec<u8> {
        let mut body = Vec::new();
        {
            let options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(Some(arrow_ipc::CompressionType::LZ4_FRAME))
                .expect("Failed to create IPC write options");
            let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
                &mut body,
                &data.schema(),
                options,
            )
            .expect("Failed to create writer");
            writer.write(data).expect("Failed to write data");
            writer.finish().expect("Failed to finish");
        }
        body
    }

    fn write_ipc_file(data: &RecordBatch) -> Vec<u8> {
        let mut body = Vec::new();
        {
            let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut body, &data.schema())
                .expect("Failed to create writer");
            writer.write(data).expect("Failed to write data");
            writer.finish().expect("Failed to finish");
        }
        body
    }

    /// Build a JSON describe response for the given schema.
    fn describe_response(schema: &Schema) -> String {
        let json_schema = JsonSchema::try_from(schema).unwrap();
        serde_json::to_string(&json!({
            "version": 1,
            "schema": json_schema,
        }))
        .unwrap()
    }

    fn nested_index_schema() -> Schema {
        let vector_type =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 8);
        Schema::new(vec![
            Field::new("rowId", DataType::Int32, false),
            Field::new("row-id", DataType::Int32, false),
            Field::new("userId", DataType::Int32, false),
            Field::new(
                "metadata",
                DataType::Struct(vec![Field::new("user_id", DataType::Int32, false)].into()),
                false,
            ),
            Field::new(
                "MetaData",
                DataType::Struct(vec![Field::new("userId", DataType::Int32, false)].into()),
                false,
            ),
            Field::new(
                "image",
                DataType::Struct(vec![Field::new("embedding", vector_type, false)].into()),
                false,
            ),
            Field::new(
                "payload",
                DataType::Struct(vec![Field::new("text", DataType::Utf8, false)].into()),
                false,
            ),
            Field::new(
                "meta-data",
                DataType::Struct(vec![Field::new("user-id", DataType::Int32, false)].into()),
                false,
            ),
            Field::new(
                "literal",
                DataType::Struct(vec![Field::new("a.b", DataType::Int32, false)].into()),
                false,
            ),
        ])
    }

    #[rstest]
    #[case("", 0)]
    #[case("{}", 0)]
    #[case(r#"{"request_id": "test-request-id"}"#, 0)]
    #[case(r#"{"version": 43}"#, 43)]
    #[tokio::test]
    async fn test_add_append(#[case] response_body: &str, #[case] expected_version: u64) {
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        // Clone response_body to give it 'static lifetime for the closure
        let response_body = response_body.to_string();

        let describe_body = describe_response(&data.schema());
        let (sender, receiver) = std::sync::mpsc::channel();
        let table =
            Table::new_with_handler("my_table", move |mut request| match request.url().path() {
                "/v1/table/my_table/describe/" => http::Response::builder()
                    .status(200)
                    .body(describe_body.clone())
                    .unwrap(),
                "/v1/table/my_table/insert/" => {
                    assert_eq!(request.method(), "POST");
                    assert!(
                        request
                            .url()
                            .query_pairs()
                            .filter(|(k, _)| k == "mode")
                            .all(|(_, v)| v == "append")
                    );
                    assert_eq!(
                        request.headers().get("Content-Type").unwrap(),
                        ARROW_STREAM_CONTENT_TYPE
                    );
                    let mut body_out = reqwest::Body::from(Vec::new());
                    std::mem::swap(request.body_mut().as_mut().unwrap(), &mut body_out);
                    sender.send(body_out).unwrap();
                    http::Response::builder()
                        .status(200)
                        .body(response_body.clone())
                        .unwrap()
                }
                path => panic!("Unexpected request path: {}", path),
            });
        let result = table.add(data.clone()).execute().await.unwrap();

        // Check version matches expected value
        assert_eq!(result.version, expected_version);

        let body = receiver.recv().unwrap();
        let body = collect_body(body).await;
        let expected_body = write_ipc_stream(&data);
        assert_eq!(&body, &expected_body);
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_add_overwrite(#[case] old_server: bool) {
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let describe_body = describe_response(&data.schema());
        let (sender, receiver) = std::sync::mpsc::channel();
        let table =
            Table::new_with_handler("my_table", move |mut request| match request.url().path() {
                "/v1/table/my_table/describe/" => http::Response::builder()
                    .status(200)
                    .body(describe_body.clone())
                    .unwrap(),
                "/v1/table/my_table/insert/" => {
                    assert_eq!(request.method(), "POST");
                    assert_eq!(
                        request
                            .url()
                            .query_pairs()
                            .find(|(k, _)| k == "mode")
                            .map(|kv| kv.1)
                            .as_deref(),
                        Some("overwrite"),
                        "Expected mode=overwrite"
                    );

                    assert_eq!(
                        request.headers().get("Content-Type").unwrap(),
                        ARROW_STREAM_CONTENT_TYPE
                    );

                    let mut body_out = reqwest::Body::from(Vec::new());
                    std::mem::swap(request.body_mut().as_mut().unwrap(), &mut body_out);
                    sender.send(body_out).unwrap();

                    if old_server {
                        http::Response::builder()
                            .status(200)
                            .body("".to_string())
                            .unwrap()
                    } else {
                        http::Response::builder()
                            .status(200)
                            .body(r#"{"version": 43}"#.to_string())
                            .unwrap()
                    }
                }
                path => panic!("Unexpected request path: {}", path),
            });

        let result = table
            .add(data.clone())
            .mode(AddDataMode::Overwrite)
            .execute()
            .await
            .unwrap();

        assert_eq!(result.version, if old_server { 0 } else { 43 });

        let body = receiver.recv().unwrap();
        let body = collect_body(body).await;
        let expected_body = write_ipc_stream(&data);
        assert_eq!(&body, &expected_body);
    }

    #[tokio::test]
    async fn test_add_preprocessing() {
        use crate::table::NaNVectorBehavior;
        use arrow_array::{FixedSizeListArray, Float32Array, Int64Array};

        // The table schema: {id: Int64, vec: FixedSizeList<Float32>[3]}
        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
                false,
            ),
        ]);
        let json_schema = JsonSchema::try_from(&table_schema).unwrap();
        let describe_body = serde_json::to_string(&json!({
            "version": 1,
            "schema": json_schema,
        }))
        .unwrap();

        // ---- Part 1: NaN vectors should be rejected by default ----
        let nan_data = RecordBatch::try_new(
            Arc::new(table_schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(
                    FixedSizeListArray::try_new(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        3,
                        Arc::new(Float32Array::from(vec![1.0, f32::NAN, 3.0])),
                        None,
                    )
                    .unwrap(),
                ),
            ],
        )
        .unwrap();

        let describe_body_clone = describe_body.clone();
        let table =
            Table::new_with_handler("my_table", move |request| match request.url().path() {
                "/v1/table/my_table/describe/" => http::Response::builder()
                    .status(200)
                    .body(describe_body_clone.clone())
                    .unwrap(),
                "/v1/table/my_table/insert/" => http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 2}"#.to_string())
                    .unwrap(),
                path => panic!("Unexpected path: {path}"),
            });

        let result = table.add(nan_data).execute().await;
        assert!(result.is_err(), "NaN vectors should be rejected by default");
        assert!(
            result.unwrap_err().to_string().contains("NaN"),
            "error should mention NaN"
        );

        // ---- Part 2: With Keep, should handle casting and missing columns ----
        // Input: {id: Int32 (needs cast to Int64), vec: FixedSizeList<Float32>[3] with NaN}
        // Table expects Int64 for id; NaN should be kept.
        let input_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
                false,
            ),
        ]);
        let cast_data = RecordBatch::try_new(
            Arc::new(input_schema),
            vec![
                Arc::new(Int32Array::from(vec![42])),
                Arc::new(
                    FixedSizeListArray::try_new(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        3,
                        Arc::new(Float32Array::from(vec![1.0, f32::NAN, 3.0])),
                        None,
                    )
                    .unwrap(),
                ),
            ],
        )
        .unwrap();

        let (sender, receiver) = std::sync::mpsc::channel();
        let table =
            Table::new_with_handler("my_table", move |mut request| match request.url().path() {
                "/v1/table/my_table/describe/" => http::Response::builder()
                    .status(200)
                    .body(describe_body.clone())
                    .unwrap(),
                "/v1/table/my_table/insert/" => {
                    let mut body_out = reqwest::Body::from(Vec::new());
                    std::mem::swap(request.body_mut().as_mut().unwrap(), &mut body_out);
                    sender.send(body_out).unwrap();
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 2}"#.to_string())
                        .unwrap()
                }
                path => panic!("Unexpected path: {path}"),
            });

        table
            .add(cast_data)
            .on_nan_vectors(NaNVectorBehavior::Keep)
            .execute()
            .await
            .unwrap();

        // Verify the data sent to the server was cast to the table schema.
        let body = receiver.recv().unwrap();
        let body = collect_body(body).await;
        let cursor = std::io::Cursor::new(body);
        let mut reader = arrow_ipc::reader::StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        let ids: &Int64Array = batch.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(ids.value(0), 42);
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_update(#[case] old_server: bool) {
        let table = Table::new_with_handler("my_table", move |request| {
            if request.url().path() == "/v1/table/my_table/update/" {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    JSON_CONTENT_TYPE
                );

                if let Some(body) = request.body().unwrap().as_bytes() {
                    let body = std::str::from_utf8(body).unwrap();
                    let value: serde_json::Value = serde_json::from_str(body).unwrap();
                    let updates = value.get("updates").unwrap().as_array().unwrap();
                    assert!(updates.len() == 2);

                    let col_name = updates[0][0].as_str().unwrap();
                    let expression = updates[0][1].as_str().unwrap();
                    assert_eq!(col_name, "a");
                    assert_eq!(expression, "a + 1");

                    let col_name = updates[1][0].as_str().unwrap();
                    let expression = updates[1][1].as_str().unwrap();
                    assert_eq!(col_name, "b");
                    assert_eq!(expression, "b - 1");

                    let only_if = value.get("predicate").unwrap().as_str().unwrap();
                    assert_eq!(only_if, "b > 10");
                }

                if old_server {
                    http::Response::builder().status(200).body("").unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"rows_updated": 5, "version": 43}"#)
                        .unwrap()
                }
            } else {
                panic!("Unexpected request path: {}", request.url().path());
            }
        });

        let result = table
            .update()
            .column("a", "a + 1")
            .column("b", "b - 1")
            .only_if("b > 10")
            .execute()
            .await
            .unwrap();

        assert_eq!(result.version, if old_server { 0 } else { 43 });
        assert_eq!(result.rows_updated, if old_server { 0 } else { 5 });
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_alter_columns(#[case] old_server: bool) {
        let table = Table::new_with_handler("my_table", move |request| {
            if request.url().path() == "/v1/table/my_table/alter_columns/" {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    JSON_CONTENT_TYPE
                );

                let body = request.body().unwrap().as_bytes().unwrap();
                let body = std::str::from_utf8(body).unwrap();
                let value: serde_json::Value = serde_json::from_str(body).unwrap();
                let alterations = value.get("alterations").unwrap().as_array().unwrap();
                assert!(alterations.len() == 2);

                let path = alterations[0]["path"].as_str().unwrap();
                let data_type = alterations[0]["data_type"]["type"].as_str().unwrap();
                assert_eq!(path, "b.c");
                assert_eq!(data_type, "int32");

                let path = alterations[1]["path"].as_str().unwrap();
                let nullable = alterations[1]["nullable"].as_bool().unwrap();
                let rename = alterations[1]["rename"].as_str().unwrap();
                assert_eq!(path, "x");
                assert!(nullable);
                assert_eq!(rename, "y");

                if old_server {
                    http::Response::builder().status(200).body("{}").unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 43}"#)
                        .unwrap()
                }
            } else {
                panic!("Unexpected request path: {}", request.url().path());
            }
        });

        let result = table
            .alter_columns(&[
                ColumnAlteration::new("b.c".into()).cast_to(DataType::Int32),
                ColumnAlteration::new("x".into())
                    .rename("y".into())
                    .set_nullable(true),
            ])
            .await
            .unwrap();

        assert_eq!(result.version, if old_server { 0 } else { 43 });
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_merge_insert(#[case] old_server: bool) {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let data: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            [Ok(batch.clone())],
            batch.schema(),
        ));

        let table = Table::new_with_handler("my_table", move |request| {
            if request.url().path() == "/v1/table/my_table/merge_insert/" {
                assert_eq!(request.method(), "POST");

                let params = request.url().query_pairs().collect::<HashMap<_, _>>();
                assert_eq!(params["on"], "some_col");
                assert_eq!(params["when_matched_update_all"], "false");
                assert_eq!(params["when_not_matched_insert_all"], "false");
                assert_eq!(params["when_not_matched_by_source_delete"], "false");
                assert!(!params.contains_key("when_matched_update_all_filt"));
                assert!(!params.contains_key("when_not_matched_by_source_delete_filt"));
                assert!(!params.contains_key("use_index"));

                if old_server {
                    http::Response::builder().status(200).body("{}").unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 43, "num_deleted_rows": 0, "num_inserted_rows": 3, "num_updated_rows": 0}"#)
                        .unwrap()
                }
            } else {
                panic!("Unexpected request path: {}", request.url().path());
            }
        });

        let result = table
            .merge_insert(&["some_col"])
            .execute(data)
            .await
            .unwrap();

        assert_eq!(result.version, if old_server { 0 } else { 43 });
        if !old_server {
            assert_eq!(result.num_deleted_rows, 0);
            assert_eq!(result.num_inserted_rows, 3);
            assert_eq!(result.num_updated_rows, 0);
        }
    }

    #[tokio::test]
    async fn test_merge_insert_retries_on_409() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let data: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            [Ok(batch.clone())],
            batch.schema(),
        ));

        // Default parameters
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/merge_insert/");

            let params = request.url().query_pairs().collect::<HashMap<_, _>>();
            assert_eq!(params["on"], "some_col");
            assert_eq!(params["when_matched_update_all"], "false");
            assert_eq!(params["when_not_matched_insert_all"], "false");
            assert_eq!(params["when_not_matched_by_source_delete"], "false");
            assert!(!params.contains_key("when_matched_update_all_filt"));
            assert!(!params.contains_key("when_not_matched_by_source_delete_filt"));

            http::Response::builder().status(409).body("").unwrap()
        });

        let e = table
            .merge_insert(&["some_col"])
            .execute(data)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("Hit retry limit"));
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_delete(#[case] old_server: bool) {
        let table = Table::new_with_handler("my_table", move |request| {
            if request.url().path() == "/v1/table/my_table/delete/" {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    JSON_CONTENT_TYPE
                );

                let body = request.body().unwrap().as_bytes().unwrap();
                let body: serde_json::Value = serde_json::from_slice(body).unwrap();
                let predicate = body.get("predicate").unwrap().as_str().unwrap();
                assert_eq!(predicate, "id in (1, 2, 3)");

                if old_server {
                    http::Response::builder().status(200).body("{}").unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 43}"#)
                        .unwrap()
                }
            } else {
                panic!("Unexpected request path: {}", request.url().path());
            }
        });

        let result = table.delete("id in (1, 2, 3)").await.unwrap();
        assert_eq!(result.version, if old_server { 0 } else { 43 });
    }

    #[tokio::test]
    async fn test_delete_expr() {
        use datafusion_expr::{col, lit};

        let table = Table::new_with_handler("my_table", move |request| {
            if request.url().path() == "/v1/table/my_table/delete/" {
                assert_eq!(request.method(), "POST");

                let body = request.body().unwrap().as_bytes().unwrap();
                let body: serde_json::Value = serde_json::from_slice(body).unwrap();
                assert!(body.get("predicate").unwrap().is_string());

                http::Response::builder()
                    .status(200)
                    .body(r#"{"num_deleted_rows": 4, "version": 2}"#)
                    .unwrap()
            } else {
                panic!("Unexpected request path: {}", request.url().path());
            }
        });

        let expr = col("id").gt(lit(5));
        let result = table.delete(&expr).await.unwrap();
        assert_eq!(result.num_deleted_rows, 4);
        assert_eq!(result.version, 2);
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_drop_columns(#[case] old_server: bool) {
        let table = Table::new_with_handler("my_table", move |request| {
            if request.url().path() == "/v1/table/my_table/drop_columns/" {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    JSON_CONTENT_TYPE
                );

                let body = request.body().unwrap().as_bytes().unwrap();
                let body = std::str::from_utf8(body).unwrap();
                let value: serde_json::Value = serde_json::from_str(body).unwrap();
                let columns = value.get("columns").unwrap().as_array().unwrap();
                assert!(columns.len() == 2);

                let col1 = columns[0].as_str().unwrap();
                let col2 = columns[1].as_str().unwrap();
                assert_eq!(col1, "a");
                assert_eq!(col2, "b");

                if old_server {
                    http::Response::builder().status(200).body("{}").unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 43}"#)
                        .unwrap()
                }
            } else {
                panic!("Unexpected request path: {}", request.url().path());
            }
        });

        let result = table.drop_columns(&["a", "b"]).await.unwrap();
        assert_eq!(result.version, if old_server { 0 } else { 43 });
    }

    #[tokio::test]
    async fn test_query_plain() {
        let expected_data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let expected_data_ref = expected_data.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/query/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let expected_body = serde_json::json!({
                "k": isize::MAX as usize,
                "prefilter": true,
                "vector": [], // Empty vector means no vector query.
                "version": null,
            });
            assert_eq!(body, expected_body);

            let response_body = write_ipc_file(&expected_data_ref);
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(response_body)
                .unwrap()
        });

        let data = table
            .query()
            .execute()
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].as_ref().unwrap(), &expected_data);
    }

    #[tokio::test]
    async fn test_query_vector_default_values() {
        let expected_data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let expected_data_ref = expected_data.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/query/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let mut expected_body = serde_json::json!({
                "prefilter": true,
                "nprobes": 20,
                "minimum_nprobes": 20,
                "maximum_nprobes": 20,
                "lower_bound": Option::<f32>::None,
                "upper_bound": Option::<f32>::None,
                "k": 10,
                "ef": Option::<usize>::None,
                "refine_factor": null,
                "version": null,
            });
            // Pass vector separately to make sure it matches f32 precision.
            expected_body["vector"] = vec![0.1f32, 0.2, 0.3].into();
            assert_eq!(body, expected_body);

            let response_body = write_ipc_file(&expected_data_ref);
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(response_body)
                .unwrap()
        });

        let data = table
            .query()
            .nearest_to(vec![0.1, 0.2, 0.3])
            .unwrap()
            .execute()
            .await;
        let data = data.unwrap().collect::<Vec<_>>().await;
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].as_ref().unwrap(), &expected_data);
    }

    #[tokio::test]
    async fn test_query_vector_approx_mode_sent_when_set() {
        let expected_data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let expected_data_ref = expected_data.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/query/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let mut expected_body = serde_json::json!({
                "prefilter": true,
                "nprobes": 20,
                "minimum_nprobes": 20,
                "maximum_nprobes": 20,
                "approx_mode": "accurate",
                "lower_bound": Option::<f32>::None,
                "upper_bound": Option::<f32>::None,
                "k": 10,
                "ef": Option::<usize>::None,
                "refine_factor": null,
                "version": null,
            });
            expected_body["vector"] = vec![0.1f32, 0.2, 0.3].into();
            assert_eq!(body, expected_body);

            let response_body = write_ipc_file(&expected_data_ref);
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(response_body)
                .unwrap()
        });

        let data = table
            .query()
            .nearest_to(vec![0.1, 0.2, 0.3])
            .unwrap()
            .approx_mode(crate::ApproxMode::Accurate)
            .execute()
            .await;
        let data = data.unwrap().collect::<Vec<_>>().await;
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].as_ref().unwrap(), &expected_data);
    }

    #[tokio::test]
    async fn test_query_fts_default_values() {
        let expected_data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let expected_data_ref = expected_data.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/query/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let expected_body = serde_json::json!({
                "full_text_query": {
                    "columns": [],
                    "query": "test",
                },
                "prefilter": true,
                "version": null,
                "k": 10,
                "vector": [],
            });
            assert_eq!(body, expected_body);

            let response_body = write_ipc_file(&expected_data_ref);
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(response_body)
                .unwrap()
        });

        let data = table
            .query()
            .full_text_search(FullTextSearchQuery::new("test".to_owned()))
            .execute()
            .await;
        let data = data.unwrap().collect::<Vec<_>>().await;
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].as_ref().unwrap(), &expected_data);
    }

    #[tokio::test]
    async fn test_query_vector_all_params() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/query/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let mut expected_body = serde_json::json!({
                "vector_column": "my_vector",
                "prefilter": false,
                "k": 42,
                "offset": 10,
                "distance_type": "cosine",
                "bypass_vector_index": true,
                "columns": ["a", "b"],
                "order_by": [
                    {
                        "column_name": "score",
                        "ascending": false,
                        "nulls_first": true,
                    },
                    {
                        "column_name": "id",
                        "ascending": true,
                        "nulls_first": false,
                    }
                ],
                "nprobes": 12,
                "minimum_nprobes": 12,
                "maximum_nprobes": 12,
                "lower_bound": Option::<f32>::None,
                "upper_bound": Option::<f32>::None,
                "ef": Option::<usize>::None,
                "refine_factor": 2,
                "version": null,
            });
            // Pass vector separately to make sure it matches f32 precision.
            expected_body["vector"] = vec![0.1f32, 0.2, 0.3].into();
            assert_eq!(body, expected_body);

            let data = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let response_body = write_ipc_file(&data);
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(response_body)
                .unwrap()
        });

        let _ = table
            .query()
            .limit(42)
            .offset(10)
            .select(Select::columns(&["a", "b"]))
            .order_by(Some(vec![
                ColumnOrdering::desc_nulls_first("score".to_string()),
                ColumnOrdering::asc_nulls_last("id".to_string()),
            ]))
            .nearest_to(vec![0.1, 0.2, 0.3])
            .unwrap()
            .column("my_vector")
            .postfilter()
            .distance_type(crate::DistanceType::Cosine)
            .nprobes(12)
            .refine_factor(2)
            .bypass_vector_index()
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_query_vector_nested_field_path() {
        let expected_data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let expected_data_ref = expected_data.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/query/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let mut expected_body = serde_json::json!({
                "vector_column": "image.embedding",
                "prefilter": true,
                "k": 10,
                "nprobes": 20,
                "minimum_nprobes": 20,
                "maximum_nprobes": 20,
                "lower_bound": Option::<f32>::None,
                "upper_bound": Option::<f32>::None,
                "ef": Option::<usize>::None,
                "refine_factor": Option::<u32>::None,
                "version": null,
            });
            expected_body["vector"] = vec![0.1f32, 0.2, 0.3].into();
            assert_eq!(body, expected_body);

            let response_body = write_ipc_file(&expected_data_ref);
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(response_body)
                .unwrap()
        });

        let _ = table
            .query()
            .nearest_to(vec![0.1, 0.2, 0.3])
            .unwrap()
            .column("image.embedding")
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_query_fts() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/query/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let expected_body = serde_json::json!({
                "full_text_query": {
                    "columns": ["a", "b"],
                    "query": "hello world",
                },
                "k": 10,
                "vector": [],
                "with_row_id": true,
                "prefilter": true,
                "version": null
            });
            let expected_body_2 = serde_json::json!({
                "full_text_query": {
                    "columns": ["b","a"],
                    "query": "hello world",
                },
                "k": 10,
                "vector": [],
                "with_row_id": true,
                "prefilter": true,
                "version": null
            });
            assert!(body == expected_body || body == expected_body_2);

            let data = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let response_body = write_ipc_file(&data);
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(response_body)
                .unwrap()
        });

        let _ = table
            .query()
            .full_text_search(
                FullTextSearchQuery::new("hello world".into())
                    .with_columns(&["a".into(), "b".into()])
                    .unwrap(),
            )
            .with_row_id()
            .limit(10)
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_analyze_plan_distributed_metrics_query_param() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/analyze_plan/");
            assert_eq!(
                request
                    .url()
                    .query_pairs()
                    .find(|(k, _)| k == "distributed_metrics"),
                Some(("distributed_metrics".into(), "per_worker".into()))
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(body["k"], serde_json::json!(1));

            http::Response::builder()
                .status(200)
                .body(r#""analyzed plan""#)
                .unwrap()
        });

        let result = table
            .query()
            .limit(1)
            .analyze_plan_with_options(QueryExecutionOptions {
                analyze_plan_distributed_metrics: AnalyzePlanDistributedMetrics::PerWorker,
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(result, "analyzed plan");
    }

    #[tokio::test]
    async fn test_query_structured_fts() {
        let table =
            Table::new_with_handler_version("my_table", semver::Version::new(0, 3, 0), |request| {
                assert_eq!(request.method(), "POST");
                assert_eq!(request.url().path(), "/v1/table/my_table/query/");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    JSON_CONTENT_TYPE
                );

                let body = request.body().unwrap().as_bytes().unwrap();
                let body: serde_json::Value = serde_json::from_slice(body).unwrap();
                let expected_body = serde_json::json!({
                    "full_text_query": {
                        "query": {
                            "match": {
                                "terms": "hello world",
                                "column": "payload.text",
                                "boost": 1.0,
                                "fuzziness": 0,
                                "max_expansions": 50,
                                "operator": "Or",
                                "prefix_length": 0,
                            },
                        }
                    },
                    "k": 10,
                    "vector": [],
                    "with_row_id": true,
                    "prefilter": true,
                    "version": null
                });
                assert_eq!(body, expected_body);

                let data = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                    vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
                )
                .unwrap();
                let response_body = write_ipc_file(&data);
                http::Response::builder()
                    .status(200)
                    .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                    .body(response_body)
                    .unwrap()
            });

        let _ = table
            .query()
            .full_text_search(FullTextSearchQuery::new_query(
                MatchQuery::new("hello world".to_owned())
                    .with_column(Some("payload.text".to_owned()))
                    .into(),
            ))
            .with_row_id()
            .limit(10)
            .execute()
            .await
            .unwrap();
    }

    #[rstest]
    #[case(DEFAULT_SERVER_VERSION.clone())]
    #[case(semver::Version::new(0, 2, 0))]
    #[tokio::test]
    async fn test_batch_queries(#[case] version: semver::Version) {
        let table = Table::new_with_handler_version("my_table", version.clone(), move |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/query/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );
            let body: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            let query_vectors = body["vector"].as_array().unwrap();
            let version = ServerVersion(version.clone());
            let data = if version.support_multivector() {
                assert_eq!(query_vectors.len(), 2);
                assert_eq!(query_vectors[0].as_array().unwrap().len(), 3);
                assert_eq!(query_vectors[1].as_array().unwrap().len(), 3);
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("a", DataType::Int32, false),
                        Field::new("query_index", DataType::Int32, false),
                    ])),
                    vec![
                        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                        Arc::new(Int32Array::from(vec![0, 0, 0, 1, 1, 1])),
                    ],
                )
                .unwrap()
            } else {
                // it's single flat vector, so here the length is dim
                assert_eq!(query_vectors.len(), 3);
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                    vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
                )
                .unwrap()
            };

            let response_body = write_ipc_file(&data);
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(response_body)
                .unwrap()
        });

        let query = table
            .query()
            .nearest_to(vec![0.1, 0.2, 0.3])
            .unwrap()
            .add_query_vector(vec![0.4, 0.5, 0.6])
            .unwrap();

        let results = query
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let results = concat_batches(&results[0].schema(), &results).unwrap();

        let query_index = results["query_index"].as_primitive::<Int32Type>();
        // We don't guarantee order.
        assert!(query_index.values().contains(&0));
        assert!(query_index.values().contains(&1));
    }

    #[tokio::test]
    async fn test_create_index() {
        let cases = [
            (
                "IVF_FLAT",
                json!({
                    "metric_type": "hamming",
                    "sample_rate": 256,
                    "max_iterations": 50,
                }),
                Index::IvfFlat(IvfFlatIndexBuilder::default().distance_type(DistanceType::Hamming)),
            ),
            (
                "IVF_FLAT",
                json!({
                    "metric_type": "hamming",
                    "num_partitions": 128,
                    "sample_rate": 256,
                    "max_iterations": 50,
                }),
                Index::IvfFlat(
                    IvfFlatIndexBuilder::default()
                        .distance_type(DistanceType::Hamming)
                        .num_partitions(128),
                ),
            ),
            (
                "IVF_PQ",
                json!({
                    "metric_type": "l2",
                    "sample_rate": 256,
                    "max_iterations": 50,
                }),
                Index::IvfPq(Default::default()),
            ),
            (
                "IVF_PQ",
                json!({
                    "metric_type": "cosine",
                    "num_partitions": 128,
                    "num_bits": 4,
                    "sample_rate": 256,
                    "max_iterations": 50,
                }),
                Index::IvfPq(
                    IvfPqIndexBuilder::default()
                        .distance_type(DistanceType::Cosine)
                        .num_partitions(128)
                        .num_bits(4),
                ),
            ),
            (
                "IVF_PQ",
                json!({
                    "metric_type": "l2",
                    "num_sub_vectors": 16,
                    "sample_rate": 512,
                    "max_iterations": 100,
                }),
                Index::IvfPq(
                    IvfPqIndexBuilder::default()
                        .num_sub_vectors(16)
                        .sample_rate(512)
                        .max_iterations(100),
                ),
            ),
            (
                "IVF_HNSW_SQ",
                json!({
                    "metric_type": "l2",
                    "sample_rate": 256,
                    "max_iterations": 50,
                    "m": 20,
                    "ef_construction": 300,
                }),
                Index::IvfHnswSq(Default::default()),
            ),
            (
                "IVF_HNSW_SQ",
                json!({
                    "metric_type": "l2",
                    "num_partitions": 128,
                    "sample_rate": 256,
                    "max_iterations": 50,
                    "m": 40,
                    "ef_construction": 500,
                }),
                Index::IvfHnswSq(
                    IvfHnswSqIndexBuilder::default()
                        .distance_type(DistanceType::L2)
                        .num_partitions(128)
                        .num_edges(40)
                        .ef_construction(500),
                ),
            ),
            (
                "IVF_HNSW_FLAT",
                json!({
                    "metric_type": "l2",
                    "sample_rate": 256,
                    "max_iterations": 50,
                    "m": 20,
                    "ef_construction": 300,
                }),
                Index::IvfHnswFlat(Default::default()),
            ),
            (
                "IVF_HNSW_FLAT",
                json!({
                    "metric_type": "cosine",
                    "num_partitions": 64,
                    "sample_rate": 256,
                    "max_iterations": 50,
                    "m": 40,
                    "ef_construction": 500,
                }),
                Index::IvfHnswFlat(
                    IvfHnswFlatIndexBuilder::default()
                        .distance_type(DistanceType::Cosine)
                        .num_partitions(64)
                        .num_edges(40)
                        .ef_construction(500),
                ),
            ),
            (
                "IVF_SQ",
                json!({
                    "metric_type": "l2",
                    "sample_rate": 256,
                    "max_iterations": 50,
                }),
                Index::IvfSq(Default::default()),
            ),
            (
                "IVF_SQ",
                json!({
                    "metric_type": "cosine",
                    "num_partitions": 64,
                    "sample_rate": 256,
                    "max_iterations": 50,
                }),
                Index::IvfSq(
                    IvfSqIndexBuilder::default()
                        .distance_type(DistanceType::Cosine)
                        .num_partitions(64),
                ),
            ),
            (
                "IVF_RQ",
                json!({
                    "metric_type": "l2",
                    "sample_rate": 256,
                    "max_iterations": 50,
                }),
                Index::IvfRq(Default::default()),
            ),
            (
                "IVF_RQ",
                json!({
                    "metric_type": "cosine",
                    "num_partitions": 64,
                    "num_bits": 8,
                    "sample_rate": 256,
                    "max_iterations": 50,
                }),
                Index::IvfRq(
                    IvfRqIndexBuilder::default()
                        .distance_type(DistanceType::Cosine)
                        .num_partitions(64)
                        .num_bits(8),
                ),
            ),
            // HNSW_PQ isn't yet supported on SaaS
            ("BTREE", json!({}), Index::BTree(Default::default())),
            ("BITMAP", json!({}), Index::Bitmap(Default::default())),
            (
                "LABEL_LIST",
                json!({}),
                Index::LabelList(Default::default()),
            ),
            (
                "FTS",
                serde_json::to_value(InvertedIndexParams::default()).unwrap(),
                Index::FTS(Default::default()),
            ),
        ];

        for (index_type, expected_body, index) in cases {
            let table = Table::new_with_handler("my_table", move |request| {
                assert_eq!(request.method(), "POST");
                match request.url().path() {
                    "/v1/table/my_table/describe/" => {
                        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
                        http::Response::builder()
                            .status(200)
                            .body(describe_response(&schema))
                            .unwrap()
                    }
                    "/v1/table/my_table/create_index/" => {
                        assert_eq!(
                            request.headers().get("Content-Type").unwrap(),
                            JSON_CONTENT_TYPE
                        );
                        let body = request.body().unwrap().as_bytes().unwrap();
                        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
                        let mut expected_body = expected_body.clone();
                        expected_body["column"] = "a".into();
                        expected_body[INDEX_TYPE_KEY] = index_type.into();

                        assert_eq!(body, expected_body);

                        http::Response::builder()
                            .status(200)
                            .body("{}".to_string())
                            .unwrap()
                    }
                    path => panic!("Unexpected path: {}", path),
                }
            });

            table.create_index(&["a"], index).execute().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_create_index_nested_field_paths() {
        let schema = nested_index_schema();
        let expected_requests = Arc::new(vec![
            json!({
                "column": "rowId",
                "index_type": "BTREE",
            }),
            json!({
                "column": "`row-id`",
                "index_type": "BTREE",
            }),
            json!({
                "column": "userId",
                "index_type": "BTREE",
            }),
            json!({
                "column": "MetaData.userId",
                "index_type": "BTREE",
            }),
            json!({
                "column": "metadata.user_id",
                "index_type": "BTREE",
            }),
            json!({
                "column": "image.embedding",
                "index_type": "IVF_PQ",
                "metric_type": "l2",
            }),
            {
                let mut body = serde_json::to_value(InvertedIndexParams::default()).unwrap();
                body["column"] = "payload.text".into();
                body["index_type"] = "FTS".into();
                body
            },
            json!({
                "column": "`meta-data`.`user-id`",
                "index_type": "BTREE",
            }),
            json!({
                "column": "literal.`a.b`",
                "index_type": "BTREE",
            }),
        ]);
        let request_idx = Arc::new(AtomicUsize::new(0));
        let table = Table::new_with_handler("my_table", {
            let schema = schema.clone();
            let expected_requests = expected_requests.clone();
            let request_idx = request_idx.clone();
            move |request| {
                assert_eq!(request.method(), "POST");
                match request.url().path() {
                    "/v1/table/my_table/describe/" => http::Response::builder()
                        .status(200)
                        .body(describe_response(&schema))
                        .unwrap(),
                    "/v1/table/my_table/create_index/" => {
                        assert_eq!(
                            request.headers().get("Content-Type").unwrap(),
                            JSON_CONTENT_TYPE
                        );
                        let idx = request_idx.fetch_add(1, Ordering::SeqCst);
                        let body = request.body().unwrap().as_bytes().unwrap();
                        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
                        assert_eq!(body, expected_requests[idx]);
                        http::Response::builder()
                            .status(200)
                            .body("{}".to_string())
                            .unwrap()
                    }
                    path => panic!("Unexpected path: {}", path),
                }
            }
        });

        table
            .create_index(&["rowId"], Index::BTree(Default::default()))
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["`ROW-ID`"], Index::BTree(Default::default()))
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["userId"], Index::BTree(Default::default()))
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["MetaData.userId"], Index::BTree(Default::default()))
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["Metadata.USER_ID"], Index::BTree(Default::default()))
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["Image.Embedding"], Index::Auto)
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["Payload.Text"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["`META-DATA`.`USER-ID`"], Index::BTree(Default::default()))
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["literal.`A.B`"], Index::BTree(Default::default()))
            .execute()
            .await
            .unwrap();

        assert_eq!(request_idx.load(Ordering::SeqCst), expected_requests.len());
    }

    #[tokio::test]
    async fn test_list_indices() {
        let schema = Schema::new(vec![
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 8),
                false,
            ),
            Field::new(
                "metadata",
                DataType::Struct(vec![Field::new("my.column", DataType::Utf8, true)].into()),
                false,
            ),
        ]);
        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.method(), "POST");

            let response_body = match request.url().path() {
                "/v1/table/my_table/describe/" => {
                    return http::Response::builder()
                        .status(200)
                        .body(describe_response(&schema))
                        .unwrap();
                }
                "/v1/table/my_table/index/list/" => {
                    serde_json::json!({
                        "indexes": [
                            {
                                "index_name": "vector_idx",
                                "index_uuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                                "columns": ["vector"],
                                "index_status": "done",
                            },
                            {
                                "index_name": "my_idx",
                                "index_uuid": "34255f64-5717-4562-b3fc-2c963f66afa6",
                                "columns": ["metadata.`my.column`"],
                                "index_status": "done",
                            },
                        ]
                    })
                }
                "/v1/table/my_table/index/vector_idx/stats/" => {
                    serde_json::json!({
                        "num_indexed_rows": 100000,
                        "num_unindexed_rows": 0,
                        "index_type": "IVF_PQ",
                        "distance_type": "l2"
                    })
                }
                "/v1/table/my_table/index/my_idx/stats/" => {
                    serde_json::json!({
                        "num_indexed_rows": 100000,
                        "num_unindexed_rows": 0,
                        "index_type": "LABEL_LIST"
                    })
                }
                path => panic!("Unexpected path: {}", path),
            };
            http::Response::builder()
                .status(200)
                .body(serde_json::to_string(&response_body).unwrap())
                .unwrap()
        });

        let indices = table.list_indices().await.unwrap();
        let expected = vec![
            IndexConfig {
                name: "vector_idx".into(),
                index_type: IndexType::IvfPq,
                columns: vec!["vector".into()],
                index_uuid: None,
                type_url: None,
                created_at: None,
                num_indexed_rows: None,
                num_unindexed_rows: None,
                size_bytes: None,
                num_segments: None,
                index_version: None,
                index_details: None,
            },
            IndexConfig {
                name: "my_idx".into(),
                index_type: IndexType::LabelList,
                columns: vec!["metadata.`my.column`".into()],
                index_uuid: None,
                type_url: None,
                created_at: None,
                num_indexed_rows: None,
                num_unindexed_rows: None,
                size_bytes: None,
                num_segments: None,
                index_version: None,
                index_details: None,
            },
        ];
        assert_eq!(indices, expected);
    }

    #[tokio::test]
    async fn test_list_indices_nested_field_paths() {
        let schema = nested_index_schema();
        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.method(), "POST");

            let response_body = match request.url().path() {
                "/v1/table/my_table/describe/" => {
                    return http::Response::builder()
                        .status(200)
                        .body(describe_response(&schema))
                        .unwrap();
                }
                "/v1/table/my_table/index/list/" => {
                    serde_json::json!({
                        "indexes": [
                            {
                                "index_name": "row_id_idx",
                                "index_uuid": "00000000-0000-0000-0000-000000000001",
                                "columns": ["rowId"],
                                "index_status": "done",
                            },
                            {
                                "index_name": "row_dash_id_idx",
                                "index_uuid": "00000000-0000-0000-0000-000000000002",
                                "columns": ["`ROW-ID`"],
                                "index_status": "done",
                            },
                            {
                                "index_name": "user_id_idx",
                                "index_uuid": "00000000-0000-0000-0000-000000000003",
                                "columns": ["userId"],
                                "index_status": "done",
                            },
                            {
                                "index_name": "mixed_case_metadata_user_id_idx",
                                "index_uuid": "00000000-0000-0000-0000-000000000004",
                                "columns": ["MetaData.userId"],
                                "index_status": "done",
                            },
                            {
                                "index_name": "metadata_user_id_idx",
                                "index_uuid": "00000000-0000-0000-0000-000000000005",
                                "columns": ["Metadata.USER_ID"],
                                "index_status": "done",
                            },
                            {
                                "index_name": "image_embedding_idx",
                                "index_uuid": "00000000-0000-0000-0000-000000000006",
                                "columns": ["Image.Embedding"],
                                "index_status": "done",
                            },
                            {
                                "index_name": "payload_text_idx",
                                "index_uuid": "00000000-0000-0000-0000-000000000007",
                                "columns": ["Payload.Text"],
                                "index_status": "done",
                            },
                            {
                                "index_name": "meta_data_user_id_idx",
                                "index_uuid": "00000000-0000-0000-0000-000000000008",
                                "columns": ["`META-DATA`.`USER-ID`"],
                                "index_status": "done",
                            },
                            {
                                "index_name": "literal_dot_idx",
                                "index_uuid": "00000000-0000-0000-0000-000000000009",
                                "columns": ["literal.`A.B`"],
                                "index_status": "done",
                            },
                        ]
                    })
                }
                "/v1/table/my_table/index/row_id_idx/stats/"
                | "/v1/table/my_table/index/row_dash_id_idx/stats/"
                | "/v1/table/my_table/index/user_id_idx/stats/"
                | "/v1/table/my_table/index/mixed_case_metadata_user_id_idx/stats/"
                | "/v1/table/my_table/index/metadata_user_id_idx/stats/"
                | "/v1/table/my_table/index/meta_data_user_id_idx/stats/"
                | "/v1/table/my_table/index/literal_dot_idx/stats/" => {
                    serde_json::json!({
                        "num_indexed_rows": 100000,
                        "num_unindexed_rows": 0,
                        "index_type": "BTREE"
                    })
                }
                "/v1/table/my_table/index/image_embedding_idx/stats/" => {
                    serde_json::json!({
                        "num_indexed_rows": 100000,
                        "num_unindexed_rows": 0,
                        "index_type": "IVF_PQ",
                        "distance_type": "l2"
                    })
                }
                "/v1/table/my_table/index/payload_text_idx/stats/" => {
                    serde_json::json!({
                        "num_indexed_rows": 100000,
                        "num_unindexed_rows": 0,
                        "index_type": "FTS"
                    })
                }
                path => panic!("Unexpected path: {}", path),
            };
            http::Response::builder()
                .status(200)
                .body(serde_json::to_string(&response_body).unwrap())
                .unwrap()
        });

        let indices = table.list_indices().await.unwrap();
        // The remote path leaves the rich metadata fields None until the server
        // wires them through. See https://github.com/lancedb/lancedb/issues/3494
        let expected: Vec<IndexConfig> = [
            ("row_id_idx", IndexType::BTree, "rowId"),
            ("row_dash_id_idx", IndexType::BTree, "`row-id`"),
            ("user_id_idx", IndexType::BTree, "userId"),
            (
                "mixed_case_metadata_user_id_idx",
                IndexType::BTree,
                "MetaData.userId",
            ),
            ("metadata_user_id_idx", IndexType::BTree, "metadata.user_id"),
            ("image_embedding_idx", IndexType::IvfPq, "image.embedding"),
            ("payload_text_idx", IndexType::FTS, "payload.text"),
            (
                "meta_data_user_id_idx",
                IndexType::BTree,
                "`meta-data`.`user-id`",
            ),
            ("literal_dot_idx", IndexType::BTree, "literal.`a.b`"),
        ]
        .into_iter()
        .map(|(name, index_type, column)| IndexConfig {
            name: name.into(),
            index_type,
            columns: vec![column.into()],
            index_uuid: None,
            type_url: None,
            created_at: None,
            num_indexed_rows: None,
            num_unindexed_rows: None,
            size_bytes: None,
            num_segments: None,
            index_version: None,
            index_details: None,
        })
        .collect();
        assert_eq!(indices, expected);
    }

    /// Verifies that when the server returns `index_type` in the list response,
    /// `list_indices` uses all enriched fields directly and does **not** make a
    /// per-index `/index/{name}/stats/` call.
    #[tokio::test]
    async fn test_list_indices_enriched() {
        let schema = Schema::new(vec![
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 8),
                false,
            ),
            Field::new("text", DataType::Utf8, false),
        ]);
        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.method(), "POST");
            match request.url().path() {
                "/v1/table/my_table/describe/" => http::Response::builder()
                    .status(200)
                    .body(describe_response(&schema))
                    .unwrap(),
                "/v1/table/my_table/index/list/" => {
                    let body = serde_json::json!({
                        "indexes": [
                            {
                                "index_name": "vector_idx",
                                "index_uuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                                "columns": ["vector"],
                                "index_type": "IVF_PQ",
                                "index_status": "done",
                                "num_indexed_rows": 1000,
                                "num_unindexed_rows": 50,
                                "size_bytes": 204800,
                                "num_segments": 2,
                                "index_version": 1,
                                "index_details": "{\"num_partitions\":16}",
                                "created_at": "2026-06-18T21:37:36.637Z",
                                "type_url": "type.googleapis.com/lance.index.vector.IvfPq",
                            },
                            {
                                "index_name": "text_idx",
                                "index_uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                                "columns": ["text"],
                                "index_type": "FTS",
                                "index_status": "done",
                                "num_indexed_rows": 1000,
                                "num_unindexed_rows": 0,
                                "size_bytes": 8192,
                                "num_segments": 1,
                            },
                        ]
                    });
                    http::Response::builder()
                        .status(200)
                        .body(serde_json::to_string(&body).unwrap())
                        .unwrap()
                }
                // stats endpoint must NOT be called for enriched responses
                path => panic!("Unexpected path (stats should not be called): {}", path),
            }
        });

        let indices = table.list_indices().await.unwrap();
        assert_eq!(indices.len(), 2);

        let vec_idx = &indices[0];
        assert_eq!(vec_idx.name, "vector_idx");
        assert_eq!(vec_idx.index_type, IndexType::IvfPq);
        assert_eq!(vec_idx.columns, vec!["vector".to_string()]);
        assert_eq!(
            vec_idx.index_uuid,
            Some("3fa85f64-5717-4562-b3fc-2c963f66afa6".to_string())
        );
        assert_eq!(vec_idx.num_indexed_rows, Some(1000));
        assert_eq!(vec_idx.num_unindexed_rows, Some(50));
        assert_eq!(vec_idx.size_bytes, Some(204800));
        assert_eq!(vec_idx.num_segments, Some(2));
        assert_eq!(vec_idx.index_version, Some(1));
        assert_eq!(
            vec_idx.index_details,
            Some("{\"num_partitions\":16}".to_string())
        );
        assert_eq!(
            vec_idx.type_url,
            Some("type.googleapis.com/lance.index.vector.IvfPq".to_string())
        );
        assert_eq!(
            vec_idx.created_at,
            Some("2026-06-18T21:37:36.637Z".parse::<DateTime<Utc>>().unwrap())
        );

        let text_idx = &indices[1];
        assert_eq!(text_idx.name, "text_idx");
        assert_eq!(text_idx.index_type, IndexType::FTS);
        assert_eq!(text_idx.columns, vec!["text".to_string()]);
        assert_eq!(
            text_idx.index_uuid,
            Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".to_string())
        );
        assert_eq!(text_idx.num_indexed_rows, Some(1000));
        assert_eq!(text_idx.num_unindexed_rows, Some(0));
        assert_eq!(text_idx.size_bytes, Some(8192));
        assert_eq!(text_idx.num_segments, Some(1));
        // optional fields not present in the response are None
        assert_eq!(text_idx.index_version, None);
        assert_eq!(text_idx.index_details, None);
        assert_eq!(text_idx.type_url, None);
        assert_eq!(text_idx.created_at, None);
    }

    #[tokio::test]
    async fn test_tokenize_uses_remote_index_details() {
        let schema = Schema::new(vec![Field::new("text", DataType::Utf8, false)]);
        let index_details = serde_json::json!({
            "base_tokenizer": "icu",
            "language": "English",
            "with_position": false,
            "max_token_length": 40,
            "lower_case": true,
            "stem": false,
            "remove_stop_words": false,
            "ascii_folding": true,
        })
        .to_string();
        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.method(), "POST");
            match request.url().path() {
                "/v1/table/my_table/describe/" => http::Response::builder()
                    .status(200)
                    .body(describe_response(&schema))
                    .unwrap(),
                "/v1/table/my_table/index/list/" => {
                    let body = serde_json::json!({
                        "indexes": [
                            {
                                "index_name": "text_idx",
                                "columns": ["text"],
                                "index_type": "FTS",
                                "index_details": index_details,
                            },
                        ]
                    });
                    http::Response::builder()
                        .status(200)
                        .body(serde_json::to_string(&body).unwrap())
                        .unwrap()
                }
                path => panic!("Unexpected path: {}", path),
            }
        });

        let tokens = table
            .tokenize("Hello, こんにちは世界!", "text_idx")
            .await
            .unwrap();

        assert_eq!(
            tokens,
            vec![
                FtsToken {
                    text: "hello".to_string(),
                    position: 0,
                },
                FtsToken {
                    text: "こんにちは".to_string(),
                    position: 1,
                },
                FtsToken {
                    text: "世界".to_string(),
                    position: 2,
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_tokenize_requires_existing_index_name() {
        let schema = Schema::new(vec![Field::new("text", DataType::Utf8, false)]);
        let table = Table::new_with_handler("my_table", move |request| -> http::Response<String> {
            assert_eq!(request.method(), "POST");
            match request.url().path() {
                "/v1/table/my_table/describe/" => http::Response::builder()
                    .status(200)
                    .body(describe_response(&schema))
                    .unwrap(),
                "/v1/table/my_table/index/list/" => {
                    let body = serde_json::json!({ "indexes": [] });
                    http::Response::builder()
                        .status(200)
                        .body(serde_json::to_string(&body).unwrap())
                        .unwrap()
                }
                path => panic!("Unexpected path: {}", path),
            }
        });

        let err = table.tokenize("hello", "text_idx").await.unwrap_err();
        assert!(matches!(
            err,
            Error::InvalidInput { message }
                if message.contains("No index named 'text_idx'")
        ));
    }

    #[tokio::test]
    async fn test_tokenize_with_column_remote_requires_index_details() {
        let schema = Schema::new(vec![Field::new("text", DataType::Utf8, false)]);
        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.method(), "POST");
            match request.url().path() {
                "/v1/table/my_table/describe/" => http::Response::builder()
                    .status(200)
                    .body(describe_response(&schema))
                    .unwrap(),
                "/v1/table/my_table/index/list/" => {
                    let body = serde_json::json!({
                        "indexes": [
                            {
                                "index_name": "text_idx",
                                "columns": ["text"],
                                "index_type": "FTS",
                            },
                        ]
                    });
                    http::Response::builder()
                        .status(200)
                        .body(serde_json::to_string(&body).unwrap())
                        .unwrap()
                }
                path => panic!("Unexpected path: {}", path),
            }
        });

        let err = table
            .tokenize_with_column("hello", "text")
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::InvalidInput { message }
                if message.contains("does not include tokenizer details")
        ));
    }

    #[test]
    fn test_deserialize_created_at() {
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(default, deserialize_with = "deserialize_created_at")]
            created_at: Option<DateTime<Utc>>,
        }

        // RFC 3339 string (current server format).
        let w: Wrapper =
            serde_json::from_str(r#"{"created_at": "2026-06-18T21:37:36.637Z"}"#).unwrap();
        assert_eq!(
            w.created_at,
            Some("2026-06-18T21:37:36.637Z".parse::<DateTime<Utc>>().unwrap())
        );

        // Unix milliseconds (legacy server format).
        let w: Wrapper = serde_json::from_str(r#"{"created_at": 1700000000000}"#).unwrap();
        assert_eq!(w.created_at, DateTime::from_timestamp_millis(1700000000000));

        // Null and missing both yield None.
        let w: Wrapper = serde_json::from_str(r#"{"created_at": null}"#).unwrap();
        assert_eq!(w.created_at, None);
        let w: Wrapper = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(w.created_at, None);

        // A malformed string is rejected rather than silently dropped to None.
        assert!(serde_json::from_str::<Wrapper>(r#"{"created_at": "not-a-date"}"#).is_err());
    }

    #[tokio::test]
    async fn test_list_versions() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/version/list/");

            let version1 = lance::dataset::Version {
                version: 1,
                timestamp: "2024-01-01T00:00:00Z".parse().unwrap(),
                metadata: Default::default(),
            };
            let version2 = lance::dataset::Version {
                version: 2,
                timestamp: "2024-02-01T00:00:00Z".parse().unwrap(),
                metadata: Default::default(),
            };
            let response_body = serde_json::json!({
                "versions": [
                    version1,
                    version2,
                ]
            });
            let response_body = serde_json::to_string(&response_body).unwrap();

            http::Response::builder()
                .status(200)
                .body(response_body)
                .unwrap()
        });

        let versions = table.list_versions().await.unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version, 1);
        assert_eq!(
            versions[0].timestamp,
            "2024-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(versions[1].version, 2);
        assert_eq!(
            versions[1].timestamp,
            "2024-02-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        // assert_eq!(versions, expected);
    }

    #[tokio::test]
    async fn test_index_stats() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/index/my_index/stats/"
            );

            let response_body = serde_json::json!({
              "num_indexed_rows": 100000,
              "num_unindexed_rows": 0,
              "index_type": "IVF_PQ",
              "distance_type": "l2"
            });
            let response_body = serde_json::to_string(&response_body).unwrap();

            http::Response::builder()
                .status(200)
                .body(response_body)
                .unwrap()
        });
        let indices = table.index_stats("my_index").await.unwrap().unwrap();
        let expected = IndexStatistics {
            num_indexed_rows: 100000,
            num_unindexed_rows: 0,
            index_type: IndexType::IvfPq,
            distance_type: Some(DistanceType::L2),
            num_indices: None,
        };
        assert_eq!(indices, expected);

        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/index/my_index/stats/"
            );

            http::Response::builder().status(404).body("").unwrap()
        });
        let indices = table.index_stats("my_index").await.unwrap();
        assert!(indices.is_none());
    }

    #[tokio::test]
    async fn test_passes_version() {
        let table = Table::new_with_handler("my_table", |request| {
            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let version = body
                .as_object()
                .unwrap()
                .get("version")
                .unwrap()
                .as_u64()
                .unwrap();
            assert_eq!(version, 42);

            let response_body = match request.url().path() {
                "/v1/table/my_table/describe/" => {
                    serde_json::json!({
                        "version": 42,
                        "schema": { "fields": [] }
                    })
                }
                "/v1/table/my_table/index/list/" => {
                    serde_json::json!({
                        "indexes": []
                    })
                }
                "/v1/table/my_table/index/my_idx/stats/" => {
                    serde_json::json!({
                        "num_indexed_rows": 100000,
                        "num_unindexed_rows": 0,
                        "index_type": "IVF_PQ",
                        "distance_type": "l2"
                    })
                }
                "/v1/table/my_table/count_rows/" => {
                    serde_json::json!(1000)
                }
                "/v1/table/my_table/query/" => {
                    let expected_data = RecordBatch::try_new(
                        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
                    )
                    .unwrap();
                    let expected_data_ref = expected_data.clone();
                    let response_body = write_ipc_file(&expected_data_ref);
                    return http::Response::builder()
                        .status(200)
                        .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                        .body(response_body)
                        .unwrap();
                }

                path => panic!("Unexpected path: {}", path),
            };

            http::Response::builder()
                .status(200)
                .body(
                    serde_json::to_string(&response_body)
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                )
                .unwrap()
        });

        table.checkout(42).await.unwrap();

        // ensure that version is passed to the /describe endpoint
        let version = table.version().await.unwrap();
        assert_eq!(version, 42);

        // ensure it's passed to other read API calls
        table.list_indices().await.unwrap();
        table.index_stats("my_idx").await.unwrap();
        table.count_rows(None).await.unwrap();
        table
            .query()
            .nearest_to(vec![0.1, 0.2, 0.3])
            .unwrap()
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_fails_if_checkout_version_doesnt_exist() {
        let table = Table::new_with_handler("my_table", |request| {
            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let version = body
                .as_object()
                .unwrap()
                .get("version")
                .unwrap()
                .as_u64()
                .unwrap();
            if version != 42 {
                return http::Response::builder()
                    .status(404)
                    .body(format!("Table my_table (version: {}) not found", version))
                    .unwrap();
            }

            let response_body = match request.url().path() {
                "/v1/table/my_table/describe/" => {
                    serde_json::json!({
                        "version": 42,
                        "schema": { "fields": [] }
                    })
                }
                _ => panic!("Unexpected path"),
            };

            http::Response::builder()
                .status(200)
                .body(serde_json::to_string(&response_body).unwrap())
                .unwrap()
        });

        let res = table.checkout(43).await;
        println!("{:?}", res);
        assert!(
            matches!(res, Err(Error::TableNotFound { name, .. }) if name == "my_table (version: 43)")
        );
    }

    #[tokio::test]
    async fn test_timetravel_immutable() {
        let table = Table::new_with_handler::<String>("my_table", |request| {
            let response_body = match request.url().path() {
                "/v1/table/my_table/describe/" => {
                    serde_json::json!({
                        "version": 42,
                        "schema": { "fields": [] }
                    })
                }
                _ => panic!("Should not have made a request: {:?}", request),
            };

            http::Response::builder()
                .status(200)
                .body(serde_json::to_string(&response_body).unwrap())
                .unwrap()
        });

        table.checkout(42).await.unwrap();

        // Ensure that all mutable operations fail.
        let res = table
            .update()
            .column("a", "a + 1")
            .column("b", "b - 1")
            .only_if("b > 10")
            .execute()
            .await;
        assert!(matches!(res, Err(Error::NotSupported { .. })));

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let data: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            [Ok(batch.clone())],
            batch.schema(),
        ));
        let res = table.merge_insert(&["some_col"]).execute(data).await;
        assert!(matches!(res, Err(Error::NotSupported { .. })));

        let res = table.delete("id in (1, 2, 3)").await;
        assert!(matches!(res, Err(Error::NotSupported { .. })));

        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let res = table.add(data.clone()).execute().await;
        assert!(matches!(res, Err(Error::NotSupported { .. })));

        let res = table
            .create_index(&["a"], Index::IvfPq(Default::default()))
            .execute()
            .await;
        assert!(matches!(res, Err(Error::NotSupported { .. })));
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_add_columns(#[case] old_server: bool) {
        let table = Table::new_with_handler("my_table", move |request| {
            if request.url().path() == "/v1/table/my_table/add_columns/" {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    JSON_CONTENT_TYPE
                );

                let body = request.body().unwrap().as_bytes().unwrap();
                let body = std::str::from_utf8(body).unwrap();
                let value: serde_json::Value = serde_json::from_str(body).unwrap();
                let new_columns = value.get("new_columns").unwrap().as_array().unwrap();
                assert!(new_columns.len() == 2);

                let col_name = new_columns[0]["name"].as_str().unwrap();
                let expression = new_columns[0]["expression"].as_str().unwrap();
                assert_eq!(col_name, "b");
                assert_eq!(expression, "a + 1");

                let col_name = new_columns[1]["name"].as_str().unwrap();
                let expression = new_columns[1]["expression"].as_str().unwrap();
                assert_eq!(col_name, "x");
                assert_eq!(expression, "cast(NULL as int32)");

                if old_server {
                    http::Response::builder().status(200).body("{}").unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 43}"#)
                        .unwrap()
                }
            } else {
                panic!("Unexpected request path: {}", request.url().path());
            }
        });

        let result = table
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![
                    ("b".into(), "a + 1".into()),
                    ("x".into(), "cast(NULL as int32)".into()),
                ]),
                None,
            )
            .await
            .unwrap();

        assert_eq!(result.version, if old_server { 0 } else { 43 });
    }

    #[tokio::test]
    async fn test_prewarm_index() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/index/my_index/prewarm/"
            );
            http::Response::builder().status(200).body("{}").unwrap()
        });
        table.prewarm_index("my_index").await.unwrap();
    }

    #[tokio::test]
    async fn test_prewarm_index_not_found() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/index/my_index/prewarm/"
            );
            http::Response::builder().status(404).body("{}").unwrap()
        });
        let e = table.prewarm_index("my_index").await.unwrap_err();
        assert!(matches!(e, Error::IndexNotFound { .. }));
    }

    #[tokio::test]
    async fn test_prewarm_data() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/page_cache/prewarm/"
            );
            http::Response::builder().status(200).body("{}").unwrap()
        });
        table.prewarm_data(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_prewarm_data_with_columns() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/page_cache/prewarm/"
            );
            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(body["columns"], serde_json::json!(["col_a", "col_b"]));
            http::Response::builder().status(200).body("{}").unwrap()
        });
        table
            .prewarm_data(Some(vec!["col_a".into(), "col_b".into()]))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_drop_index() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/index/my_index/drop/"
            );
            http::Response::builder().status(200).body("{}").unwrap()
        });
        table.drop_index("my_index").await.unwrap();
    }

    #[tokio::test]
    async fn test_drop_index_not_exists() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/index/my_index/drop/"
            );
            http::Response::builder().status(404).body("{}").unwrap()
        });

        // Assert that the error is IndexNotFound
        let e = table.drop_index("my_index").await.unwrap_err();
        assert!(matches!(e, Error::IndexNotFound { .. }));
    }

    #[tokio::test]
    async fn test_set_lsm_write_spec_unsharded() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/set_lsm_write_spec/"
            );
            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(body["sharding"], serde_json::json!({ "mode": "unsharded" }));
            assert_eq!(body["maintained_indexes"], serde_json::json!(["id_idx"]));
            assert_eq!(
                body["writer_config_defaults"],
                serde_json::json!({ "max_memtable_rows": "1000" })
            );
            http::Response::builder()
                .status(200)
                .body(r#"{"maintained_indexes":["id_idx"]}"#)
                .unwrap()
        });
        let spec = crate::table::LsmWriteSpec::unsharded()
            .with_maintained_indexes(["id_idx"])
            .with_writer_config_defaults([("max_memtable_rows", "1000")]);
        table.set_lsm_write_spec(spec).await.unwrap();
    }

    #[tokio::test]
    async fn test_set_lsm_write_spec_bucket() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/set_lsm_write_spec/"
            );
            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(
                body["sharding"],
                serde_json::json!({ "mode": "bucket", "column": "id", "num_buckets": 16 })
            );
            assert_eq!(body["maintained_indexes"], serde_json::json!([]));
            http::Response::builder().status(200).body("{}").unwrap()
        });
        table
            .set_lsm_write_spec(crate::table::LsmWriteSpec::bucket("id", 16))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_set_lsm_write_spec_identity() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/set_lsm_write_spec/"
            );
            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(
                body["sharding"],
                serde_json::json!({ "mode": "identity", "column": "tenant" })
            );
            http::Response::builder().status(200).body("{}").unwrap()
        });
        table
            .set_lsm_write_spec(crate::table::LsmWriteSpec::identity("tenant"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_unset_lsm_write_spec() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/unset_lsm_write_spec/"
            );
            http::Response::builder().status(200).body("{}").unwrap()
        });
        table.unset_lsm_write_spec().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_lsm_write_spec() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/get_lsm_write_spec/"
            );

            // The server resolves the spec and re-encodes it into the same
            // sophon-owned shape the set endpoint accepts (`Sharding` internally
            // tagged on `mode`, wrapped in `lsm_write_spec`).
            let response = serde_json::json!({
                "lsm_write_spec": {
                    "sharding": { "mode": "bucket", "column": "id", "num_buckets": 4 },
                    "maintained_indexes": ["id_idx"],
                    "writer_config_defaults": { "durable_write": "false" },
                }
            });
            http::Response::builder()
                .status(200)
                .body(response.to_string())
                .unwrap()
        });

        let spec = table
            .get_lsm_write_spec()
            .await
            .unwrap()
            .expect("a spec should be reported");
        match spec {
            crate::table::LsmWriteSpec::Bucket {
                column,
                num_buckets,
                maintained_indexes,
                writer_config_defaults,
            } => {
                assert_eq!(column, "id");
                assert_eq!(num_buckets, 4);
                assert_eq!(maintained_indexes, vec!["id_idx".to_string()]);
                assert_eq!(
                    writer_config_defaults
                        .get("durable_write")
                        .map(String::as_str),
                    Some("false")
                );
            }
            other => panic!("expected a bucket spec, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_get_lsm_write_spec_absent() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/get_lsm_write_spec/"
            );
            // Null spec → the LSM write path is not enabled.
            let response = serde_json::json!({ "lsm_write_spec": null });
            http::Response::builder()
                .status(200)
                .body(response.to_string())
                .unwrap()
        });
        assert!(table.get_lsm_write_spec().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_wait_for_index() {
        let table = _make_table_with_indices(0);
        table
            .wait_for_index(&["vector_idx", "my_idx"], Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_index_timeout() {
        let table = _make_table_with_indices(100);
        let e = table
            .wait_for_index(&["vector_idx", "my_idx"], Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "Timeout error: timed out waiting for indices: [\"vector_idx\", \"my_idx\"] after 1s"
        );
    }

    #[tokio::test]
    async fn test_wait_for_index_timeout_never_created() {
        let table = _make_table_with_indices(0);
        let e = table
            .wait_for_index(&["doesnt_exist_idx"], Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "Timeout error: timed out waiting for indices: [\"doesnt_exist_idx\"] after 1s"
        );
    }

    fn _make_table_with_indices(unindexed_rows: usize) -> Table {
        Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.method(), "POST");

            let response_body = match request.url().path() {
                "/v1/table/my_table/describe/" => {
                    let schema = Schema::new(vec![
                        Field::new(
                            "vector",
                            DataType::FixedSizeList(
                                Arc::new(Field::new("item", DataType::Float32, true)),
                                8,
                            ),
                            false,
                        ),
                        Field::new("my_column", DataType::Utf8, false),
                    ]);
                    serde_json::from_str::<serde_json::Value>(&describe_response(&schema)).unwrap()
                }
                "/v1/table/my_table/index/list/" => {
                    serde_json::json!({
                        "indexes": [
                            {
                                "index_name": "vector_idx",
                                "index_uuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                                "columns": ["vector"],
                                "index_status": "done",
                            },
                            {
                                "index_name": "my_idx",
                                "index_uuid": "34255f64-5717-4562-b3fc-2c963f66afa6",
                                "columns": ["my_column"],
                                "index_status": "done",
                            },
                        ]
                    })
                }
                "/v1/table/my_table/index/vector_idx/stats/" => {
                    serde_json::json!({
                        "num_indexed_rows": 100000,
                        "num_unindexed_rows": unindexed_rows,
                        "index_type": "IVF_PQ",
                        "distance_type": "l2"
                    })
                }
                "/v1/table/my_table/index/my_idx/stats/" => {
                    serde_json::json!({
                        "num_indexed_rows": 100000,
                        "num_unindexed_rows": unindexed_rows,
                        "index_type": "LABEL_LIST"
                    })
                }
                _path => {
                    serde_json::json!(None::<String>)
                }
            };
            let body = serde_json::to_string(&response_body).unwrap();
            let status = if body == "null" { 404 } else { 200 };
            http::Response::builder().status(status).body(body).unwrap()
        })
    }

    #[tokio::test]
    async fn test_table_with_namespace_identifier() {
        // Test that a table created with namespace uses the correct identifier in API calls
        let table = Table::new_with_handler("ns1$ns2$table1", |request| {
            assert_eq!(request.method(), "POST");
            // All API calls should use the full identifier in the path
            assert_eq!(request.url().path(), "/v1/table/ns1$ns2$table1/describe/");

            http::Response::builder()
                .status(200)
                .body(r#"{"version": 1, "schema": { "fields": [] }}"#)
                .unwrap()
        });

        // The name() method should return just the base name, not the full identifier
        assert_eq!(table.name(), "ns1$ns2$table1");

        // API operations should work correctly
        let version = table.version().await.unwrap();
        assert_eq!(version, 1);
    }

    #[tokio::test]
    async fn test_query_with_namespace() {
        let table = Table::new_with_handler("analytics$events", |request| {
            match request.url().path() {
                "/v1/table/analytics$events/query/" => {
                    assert_eq!(request.method(), "POST");

                    // Return empty arrow stream
                    let data = RecordBatch::try_new(
                        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
                        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
                    )
                    .unwrap();
                    let body = write_ipc_file(&data);

                    http::Response::builder()
                        .status(200)
                        .header("Content-Type", ARROW_FILE_CONTENT_TYPE)
                        .body(body)
                        .unwrap()
                }
                _ => {
                    panic!("Unexpected path: {}", request.url().path());
                }
            }
        });

        let results = table.query().execute().await.unwrap();
        let batches = results.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_add_data_with_namespace() {
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let describe_body = describe_response(&data.schema());
        let (sender, receiver) = std::sync::mpsc::channel();
        let table = Table::new_with_handler("prod$metrics", move |mut request| {
            match request.url().path() {
                "/v1/table/prod$metrics/describe/" => http::Response::builder()
                    .status(200)
                    .body(describe_body.clone())
                    .unwrap(),
                "/v1/table/prod$metrics/insert/" => {
                    assert_eq!(request.method(), "POST");
                    assert_eq!(
                        request.headers().get("Content-Type").unwrap(),
                        ARROW_STREAM_CONTENT_TYPE
                    );
                    let mut body_out = reqwest::Body::from(Vec::new());
                    std::mem::swap(request.body_mut().as_mut().unwrap(), &mut body_out);
                    sender.send(body_out).unwrap();
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 2}"#.to_string())
                        .unwrap()
                }
                path => panic!("Unexpected request path: {}", path),
            }
        });

        let result = table.add(data.clone()).execute().await.unwrap();

        assert_eq!(result.version, 2);

        let body = receiver.recv().unwrap();
        let body = collect_body(body).await;
        let expected_body = write_ipc_stream(&data);
        assert_eq!(&body, &expected_body);
    }

    #[tokio::test]
    async fn test_create_index_with_namespace() {
        let table = Table::new_with_handler("dev$users", |request| {
            match request.url().path() {
                "/v1/table/dev$users/create_index/" => {
                    assert_eq!(request.method(), "POST");
                    assert_eq!(
                        request.headers().get("Content-Type").unwrap(),
                        JSON_CONTENT_TYPE
                    );

                    // Verify the request body contains the column name
                    if let Some(body) = request.body().unwrap().as_bytes() {
                        let body = std::str::from_utf8(body).unwrap();
                        let value: serde_json::Value = serde_json::from_str(body).unwrap();
                        assert_eq!(value["column"], "embedding");
                        assert_eq!(value["index_type"], "IVF_PQ");
                    }

                    http::Response::builder()
                        .status(200)
                        .body("".to_string())
                        .unwrap()
                }
                "/v1/table/dev$users/describe/" => {
                    let schema = Schema::new(vec![Field::new(
                        "embedding",
                        DataType::FixedSizeList(
                            Arc::new(Field::new("item", DataType::Float32, true)),
                            8,
                        ),
                        false,
                    )]);
                    http::Response::builder()
                        .status(200)
                        .body(describe_response(&schema))
                        .unwrap()
                }
                _ => {
                    panic!("Unexpected path: {}", request.url().path());
                }
            }
        });

        table
            .create_index(&["embedding"], Index::IvfPq(IvfPqIndexBuilder::default()))
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_drop_columns_with_namespace() {
        let table = Table::new_with_handler("test$schema_ops", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/test$schema_ops/drop_columns/"
            );
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            if let Some(body) = request.body().unwrap().as_bytes() {
                let body = std::str::from_utf8(body).unwrap();
                let value: serde_json::Value = serde_json::from_str(body).unwrap();
                let columns = value["columns"].as_array().unwrap();
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0], "old_col1");
                assert_eq!(columns[1], "old_col2");
            }

            http::Response::builder()
                .status(200)
                .body(r#"{"version": 5}"#)
                .unwrap()
        });

        let result = table.drop_columns(&["old_col1", "old_col2"]).await.unwrap();
        assert_eq!(result.version, 5);
    }

    #[tokio::test]
    async fn test_uri() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/describe/");

            http::Response::builder()
                .status(200)
                .body(r#"{"version": 1, "schema": {"fields": []}, "location": "s3://bucket/path/to/table"}"#)
                .unwrap()
        });

        let uri = table.uri().await.unwrap();
        assert_eq!(uri, "s3://bucket/path/to/table");
    }

    #[tokio::test]
    async fn test_uri_missing_location() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/describe/");

            // Server returns response without location field
            http::Response::builder()
                .status(200)
                .body(r#"{"version": 1, "schema": {"fields": []}}"#)
                .unwrap()
        });

        let result = table.uri().await;
        assert!(result.is_err());
        assert!(matches!(&result, Err(Error::NotSupported { .. })));
    }

    #[tokio::test]
    async fn test_uri_caching() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.url().path(), "/v1/table/my_table/describe/");
            call_count_clone.fetch_add(1, Ordering::SeqCst);

            http::Response::builder()
                .status(200)
                .body(
                    r#"{"version": 1, "schema": {"fields": []}, "location": "gs://bucket/table"}"#,
                )
                .unwrap()
        });

        // First call should fetch from server
        let uri1 = table.uri().await.unwrap();
        assert_eq!(uri1, "gs://bucket/table");
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Second call should use cached value
        let uri2 = table.uri().await.unwrap();
        assert_eq!(uri2, "gs://bucket/table");
        assert_eq!(call_count.load(Ordering::SeqCst), 1); // Still 1, no new call
    }

    /// Test that schema is fetched once and cached for subsequent calls
    #[tokio::test]
    async fn test_schema_caching() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.url().path(), "/v1/table/my_table/describe/");
            call_count_clone.fetch_add(1, Ordering::SeqCst);

            http::Response::builder()
                .status(200)
                .body(
                    r#"{"version": 1, "schema": {"fields": [
                        {"name": "a", "type": { "type": "int32" }, "nullable": false}
                    ]}}"#,
                )
                .unwrap()
        });

        // First call should fetch from server
        let schema1 = table.schema().await.unwrap();
        assert_eq!(schema1.fields().len(), 1);
        assert_eq!(schema1.field(0).name(), "a");
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Second call should use cached value
        let schema2 = table.schema().await.unwrap();
        assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
        assert_eq!(call_count.load(Ordering::SeqCst), 1); // Still 1, no new call

        // Third call should still use cached value
        let schema3 = table.schema().await.unwrap();
        assert_eq!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
        assert_eq!(call_count.load(Ordering::SeqCst), 1); // Still 1, no new call
    }

    /// Test that schema cache expires after 30 seconds TTL
    #[tokio::test]
    async fn test_schema_cache_invalidation_after_ttl() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.url().path(), "/v1/table/my_table/describe/");
            call_count_clone.fetch_add(1, Ordering::SeqCst);

            http::Response::builder()
                .status(200)
                .body(
                    r#"{"version": 1, "schema": {"fields": [
                        {"name": "a", "type": { "type": "int32" }, "nullable": false}
                    ]}}"#,
                )
                .unwrap()
        });

        // First call should fetch from server
        let _schema1 = table.schema().await.unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Second call should use cached value (within TTL)
        let schema2 = table.schema().await.unwrap();
        assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&_schema1));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Advance mock time past TTL (no real wait)
        clock::advance_by(Duration::from_secs(31));

        // Third call should re-fetch from server (TTL expired)
        let schema3 = table.schema().await.unwrap();
        assert_ne!(Arc::as_ptr(&schema3), Arc::as_ptr(&_schema1));
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    /// Test that schema cache is invalidated after schema-changing operations
    #[rstest]
    #[case("overwrite")]
    #[case("add_columns")]
    #[case("drop_columns")]
    #[case("alter_columns")]
    #[tokio::test]
    async fn test_schema_cache_invalidation_after_operation(#[case] operation: &str) {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            let path = request.url().path();

            if path == "/v1/table/my_table/describe/" {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
                http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false},
                            {"name": "b", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                    )
                    .unwrap()
            } else if path == "/v1/table/my_table/insert/"
                || path == "/v1/table/my_table/add_columns/"
                || path == "/v1/table/my_table/drop_columns/"
                || path == "/v1/table/my_table/alter_columns/"
            {
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 2}"#)
                    .unwrap()
            } else {
                http::Response::builder()
                    .status(404)
                    .body("not found")
                    .unwrap()
            }
        });

        // First schema call should fetch from server
        let schema1 = table.schema().await.unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Second schema call should use cached value
        let schema2 = table.schema().await.unwrap();
        assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Perform the schema-changing operation
        match operation {
            "overwrite" => {
                let data = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
                let _ = table.add(data).mode(AddDataMode::Overwrite).execute().await;
            }
            "add_columns" => {
                let _ = table
                    .add_columns(
                        NewColumnTransform::SqlExpressions(vec![("c".into(), "a + 1".into())]),
                        None,
                    )
                    .await;
            }
            "drop_columns" => {
                let _ = table.drop_columns(&["b"]).await;
            }
            "alter_columns" => {
                let alterations = vec![ColumnAlteration::new("a".into()).rename("new_a".into())];
                let _ = table.alter_columns(&alterations).await;
            }
            _ => panic!("Unknown operation: {}", operation),
        }

        // Schema call after operation should re-fetch from server (cache invalidated)
        let schema3 = table.schema().await.unwrap();
        assert_ne!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    /// Test that schema cache is invalidated when server returns certain error codes
    #[rstest]
    #[case(400, true)] // 400 Bad Request should invalidate cache
    #[case(401, false)] // 401 Unauthorized should NOT invalidate cache
    #[case(403, false)] // 403 Forbidden should NOT invalidate cache
    #[case(404, true)] // 404 Not Found should invalidate (table might be recreated)
    #[case(500, true)] // 500 Internal Server Error should invalidate cache
    #[case(503, false)] // 503 Service Unavailable should NOT invalidate cache
    #[tokio::test]
    async fn test_schema_cache_invalidation_on_errors(
        #[case] error_status: u16,
        #[case] should_invalidate: bool,
    ) {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            let path = request.url().path();
            let current_count = call_count_clone.load(Ordering::SeqCst);

            if path == "/v1/table/my_table/describe/" {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
                http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                    )
                    .unwrap()
            } else if path == "/v1/table/my_table/count_rows/" {
                // Return error on first count_rows call
                if current_count == 1 {
                    http::Response::builder()
                        .status(error_status)
                        .body("error")
                        .unwrap()
                } else {
                    http::Response::builder().status(200).body("10").unwrap()
                }
            } else {
                http::Response::builder()
                    .status(404)
                    .body("not found")
                    .unwrap()
            }
        });

        // First schema call should fetch from server
        let schema1 = table.schema().await.unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Second schema call should use cached value
        let schema2 = table.schema().await.unwrap();
        assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Perform operation that returns error
        let result = table.count_rows(None).await;
        assert!(result.is_err());

        // Schema call after error - check if cache was invalidated
        let schema3 = table.schema().await.unwrap();
        if should_invalidate {
            assert_eq!(
                call_count.load(Ordering::SeqCst),
                2,
                "Cache should be invalidated for {} error",
                error_status
            );
            assert_ne!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
        } else {
            assert_eq!(
                call_count.load(Ordering::SeqCst),
                1,
                "Cache should NOT be invalidated for {} error",
                error_status
            );
            assert_eq!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
        }
    }

    /// Test that schema cache is invalidated after checkout
    #[tokio::test]
    async fn test_schema_cache_invalidation_on_checkout() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            let path = request.url().path();
            call_count_clone.fetch_add(1, Ordering::SeqCst);
            let count = call_count_clone.load(Ordering::SeqCst);

            if path == "/v1/table/my_table/describe/" {
                // Return different schemas for different calls
                if count <= 2 {
                    // First schema call and checkout validation
                    http::Response::builder()
                        .status(200)
                        .body(
                            r#"{"version": 1, "schema": {"fields": [
                                {"name": "a", "type": { "type": "int32" }, "nullable": false}
                            ]}}"#,
                        )
                        .unwrap()
                } else {
                    // After checkout
                    http::Response::builder()
                        .status(200)
                        .body(
                            r#"{"version": 2, "schema": {"fields": [
                                {"name": "a", "type": { "type": "int32" }, "nullable": false},
                                {"name": "b", "type": { "type": "int32" }, "nullable": false}
                            ]}}"#,
                        )
                        .unwrap()
                }
            } else {
                http::Response::builder()
                    .status(404)
                    .body("not found")
                    .unwrap()
            }
        });

        // First schema call
        let schema1 = table.schema().await.unwrap();
        assert_eq!(schema1.fields().len(), 1);

        // Second schema call should use cached value (no new call)
        let call_count_before = call_count.load(Ordering::SeqCst);
        let schema2 = table.schema().await.unwrap();
        assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
        assert_eq!(call_count.load(Ordering::SeqCst), call_count_before);

        // Checkout to version 2 (makes a describe call to validate)
        let _ = table.checkout(2).await;

        // Schema call after checkout should re-fetch (cache was invalidated)
        let schema3 = table.schema().await.unwrap();
        assert_eq!(schema3.fields().len(), 2);
        assert_ne!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
    }

    /// Test that schema cache is invalidated after checkout_latest
    #[tokio::test]
    async fn test_schema_cache_invalidation_on_checkout_latest() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            let path = request.url().path();

            if path == "/v1/table/my_table/describe/" {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
                http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                    )
                    .unwrap()
            } else {
                http::Response::builder()
                    .status(404)
                    .body("not found")
                    .unwrap()
            }
        });

        // First schema call
        let schema1 = table.schema().await.unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Second schema call should use cached value
        let schema2 = table.schema().await.unwrap();
        assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Checkout latest
        let _ = table.checkout_latest().await;

        // Schema call after checkout_latest should re-fetch (cache was invalidated)
        let schema3 = table.schema().await.unwrap();
        assert_ne!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    /// Test that schema cache is invalidated after checkout_tag
    #[tokio::test]
    async fn test_schema_cache_invalidation_on_checkout_tag() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            let path = request.url().path();

            if path == "/v1/table/my_table/describe/" {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
                http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                    )
                    .unwrap()
            } else if path == "/v1/table/my_table/tags/list/" {
                http::Response::builder()
                    .status(200)
                    .body(r#"{"tags": {"v2": {"version": 2}}}"#)
                    .unwrap()
            } else if path == "/v1/table/my_table/tags/version/" {
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 2}"#)
                    .unwrap()
            } else {
                http::Response::builder()
                    .status(404)
                    .body("not found")
                    .unwrap()
            }
        });

        // First schema call
        let schema1 = table.schema().await.unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Second schema call should use cached value
        let schema2 = table.schema().await.unwrap();
        assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Checkout tag
        table
            .checkout_tag("v2")
            .await
            .expect("checkout_tag should succeed");

        // Schema call after checkout_tag should re-fetch (cache was invalidated)
        let schema3 = table.schema().await.unwrap();
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            2,
            "Cache should have been invalidated and re-fetched"
        );
        assert_ne!(
            Arc::as_ptr(&schema3),
            Arc::as_ptr(&schema1),
            "Should be different Arc instances"
        );
    }

    /// Test that restore invalidates cache (via checkout_latest)
    #[tokio::test]
    async fn test_schema_cache_invalidation_on_restore() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            let path = request.url().path();

            if path == "/v1/table/my_table/describe/" {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
                http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                    )
                    .unwrap()
            } else if path == "/v1/table/my_table/restore/" {
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 1}"#)
                    .unwrap()
            } else {
                http::Response::builder()
                    .status(404)
                    .body("not found")
                    .unwrap()
            }
        });

        // First schema call
        let schema1 = table.schema().await.unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Second schema call uses cache
        let schema2 = table.schema().await.unwrap();
        assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Restore operation
        let _ = table.restore().await;

        // Schema call after restore should re-fetch (cache invalidated)
        let schema3 = table.schema().await.unwrap();
        assert_ne!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    /// Test that centralized error handling invalidates cache on query errors
    #[tokio::test]
    async fn test_centralized_error_invalidation_on_query() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            let path = request.url().path();
            let current_count = call_count_clone.load(Ordering::SeqCst);

            if path == "/v1/table/my_table/describe/" {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
                http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                    )
                    .unwrap()
            } else if path == "/v1/table/my_table/query/" {
                // Return 400 error on first query (could be schema mismatch)
                if current_count == 1 {
                    http::Response::builder()
                        .status(400)
                        .body("Bad request")
                        .unwrap()
                } else {
                    // Return empty result for successful query
                    http::Response::builder()
                        .status(200)
                        .header("content-type", "application/vnd.apache.arrow.stream")
                        .body("")
                        .unwrap()
                }
            } else {
                http::Response::builder()
                    .status(404)
                    .body("not found")
                    .unwrap()
            }
        });

        // First schema call
        let schema1 = table.schema().await.unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Second schema call uses cache
        let schema2 = table.schema().await.unwrap();
        assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Query that returns 400 error
        let result = table.query().execute().await;
        assert!(result.is_err());

        // Schema call after error should re-fetch (cache invalidated by centralized handler)
        let schema3 = table.schema().await.unwrap();
        assert_ne!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    /// Test that concurrent schema() calls with an empty cache only trigger one fetch.
    #[tokio::test]
    async fn test_concurrent_schema_calls_single_fetch() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let table = Arc::new(Table::new_with_handler("my_table", move |request| {
            let path = request.url().path();
            if path == "/v1/table/my_table/describe/" {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
                http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                    )
                    .unwrap()
            } else {
                panic!("Unexpected request: {}", path);
            }
        }));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let table = table.clone();
            handles.push(tokio::spawn(async move { table.schema().await.unwrap() }));
        }

        let schemas: Vec<SchemaRef> = futures::future::try_join_all(handles).await.unwrap();

        // All callers should get the same Arc
        for schema in &schemas {
            assert_eq!(Arc::as_ptr(schema), Arc::as_ptr(&schemas[0]));
        }
        // Only one describe call should have been made
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    /// Test that a background refresh is triggered in the refresh window and
    /// returns the cached value immediately.
    #[tokio::test]
    async fn test_background_refresh_triggers_in_window() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            let path = request.url().path();
            if path == "/v1/table/my_table/describe/" {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                if count == 0 {
                    http::Response::builder()
                        .status(200)
                        .body(
                            r#"{"version": 1, "schema": {"fields": [
                                {"name": "a", "type": { "type": "int32" }, "nullable": false}
                            ]}}"#,
                        )
                        .unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(
                            r#"{"version": 2, "schema": {"fields": [
                                {"name": "a", "type": { "type": "int32" }, "nullable": false},
                                {"name": "b", "type": { "type": "string" }, "nullable": true}
                            ]}}"#,
                        )
                        .unwrap()
                }
            } else {
                panic!("Unexpected request: {}", path);
            }
        });

        // Populate cache and trigger peek transition to Current state
        let schema1 = table.schema().await.unwrap();
        assert_eq!(schema1.fields().len(), 1);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        // Second call transitions cache from Refreshing to Current via peek()
        let schema2 = table.schema().await.unwrap();
        assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));

        // Advance into refresh window (TTL=30s, window=5s, so 26s is in window)
        clock::advance_by(Duration::from_secs(26));

        // This call enters the refresh window: returns cached value and creates
        // a background shared future (Refreshing state with previous).
        let schema3 = table.schema().await.unwrap();
        assert_eq!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
        // Only the initial fetch so far
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Advance past TTL so the previous value expires. This forces the next
        // schema() to Wait on the in-flight shared future, driving it to completion.
        clock::advance_by(Duration::from_secs(30));

        let schema4 = table.schema().await.unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
        assert_eq!(schema4.fields().len(), 2);
        assert_ne!(Arc::as_ptr(&schema4), Arc::as_ptr(&schema1));
    }

    /// Test that multiple calls during the refresh window don't trigger
    /// duplicate background refreshes.
    #[tokio::test]
    async fn test_no_duplicate_background_refreshes() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            let path = request.url().path();
            if path == "/v1/table/my_table/describe/" {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
                http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                    )
                    .unwrap()
            } else {
                panic!("Unexpected request: {}", path);
            }
        });

        // Populate cache and transition to Current state
        let schema1 = table.schema().await.unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        let _ = table.schema().await.unwrap(); // peek transition

        // Advance into refresh window
        clock::advance_by(Duration::from_secs(26));

        // Multiple rapid calls should all return cached. The first one enters
        // the refresh window and starts a background fetch (Refreshing state).
        // Subsequent calls see Refreshing with a valid previous and return it.
        for _ in 0..5 {
            let schema = table.schema().await.unwrap();
            assert_eq!(Arc::as_ptr(&schema), Arc::as_ptr(&schema1));
        }

        // Advance past TTL and drive the shared future to completion
        clock::advance_by(Duration::from_secs(30));
        let _ = table.schema().await.unwrap();

        // Only one additional describe call (the background refresh),
        // not five separate ones
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    /// Test that if a background refresh fails, the previously cached value
    /// is preserved and still returned.
    #[tokio::test]
    async fn test_background_refresh_error_preserves_cache() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            let path = request.url().path();
            if path == "/v1/table/my_table/describe/" {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                if count == 0 {
                    // First call succeeds
                    http::Response::builder()
                        .status(200)
                        .body(
                            r#"{"version": 1, "schema": {"fields": [
                                {"name": "a", "type": { "type": "int32" }, "nullable": false}
                            ]}}"#,
                        )
                        .unwrap()
                } else {
                    // Subsequent calls fail (422 is not retried)
                    http::Response::builder()
                        .status(422)
                        .body("Unprocessable Entity")
                        .unwrap()
                }
            } else {
                panic!("Unexpected request: {}", path);
            }
        });

        // Populate cache and transition to Current state
        let schema1 = table.schema().await.unwrap();
        assert_eq!(schema1.fields().len(), 1);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        let _ = table.schema().await.unwrap(); // peek transition

        // Advance into refresh window
        clock::advance_by(Duration::from_secs(26));

        // Trigger background refresh (returns cached value). The background
        // fetch will fail but the previous value should be preserved.
        let schema2 = table.schema().await.unwrap();
        assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));

        // Still in the refresh window: the previous value is valid,
        // so calling schema() should still return it.
        let schema3 = table.schema().await.unwrap();
        assert_eq!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));

        // Advance past TTL. The shared future will be driven and fail.
        // The peek() error path should revert to the previous cached value.
        clock::advance_by(Duration::from_secs(30));

        // After the error, the previous is restored but its timestamp is old,
        // so the next call triggers a new fetch which also fails.
        let result = table.schema().await;
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
        // The error from the failed fetch should be propagated
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_add_insert_fails() {
        // Verify that an HTTP error from the insert endpoint is properly
        // surfaced with the status code intact. Use 400 (non-retryable).
        let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
        let describe_body = describe_response(&batch.schema());

        let table =
            Table::new_with_handler("my_table", move |request| match request.url().path() {
                "/v1/table/my_table/describe/" => http::Response::builder()
                    .status(200)
                    .body(describe_body.clone())
                    .unwrap(),
                "/v1/table/my_table/insert/" => http::Response::builder()
                    .status(400)
                    .body("bad request".to_string())
                    .unwrap(),
                path => panic!("Unexpected request path: {}", path),
            });

        let result = table.add(batch).execute().await;
        let err = result.unwrap_err();
        match &err {
            Error::Http { status_code, .. } => {
                assert_eq!(*status_code, Some(reqwest::StatusCode::BAD_REQUEST));
            }
            other => panic!("Expected Http error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_add_retries_on_retryable_status() {
        // Verify that rescannable data retries on retryable status codes (e.g. 502)
        // and eventually succeeds.
        let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
        let describe_body = describe_response(&batch.schema());

        let attempt = Arc::new(AtomicUsize::new(0));
        let attempt_clone = attempt.clone();

        let table =
            Table::new_with_handler("my_table", move |request| match request.url().path() {
                "/v1/table/my_table/describe/" => http::Response::builder()
                    .status(200)
                    .body(describe_body.clone())
                    .unwrap(),
                "/v1/table/my_table/insert/" => {
                    let n = attempt_clone.fetch_add(1, Ordering::SeqCst);
                    if n < 2 {
                        http::Response::builder()
                            .status(502)
                            .body("bad gateway".to_string())
                            .unwrap()
                    } else {
                        http::Response::builder()
                            .status(200)
                            .body(r#"{"version": 3}"#.to_string())
                            .unwrap()
                    }
                }
                path => panic!("Unexpected request path: {}", path),
            });

        let result = table.add(batch).execute().await.unwrap();
        assert_eq!(result.version, 3);
        assert_eq!(attempt.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_query_with_datafusion_filter() {
        use datafusion_expr::{col, lit};

        let expected_data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let expected_data_ref = expected_data.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/query/");

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();

            // The Datafusion expression should be serialized to SQL
            let filter = body.get("filter").expect("filter should be present");
            let filter_str = filter.as_str().expect("filter should be a string");
            // col("x") > lit(10) AND col("status") = lit("active")
            assert!(
                filter_str.contains("x") && filter_str.contains("10"),
                "Filter should contain 'x' and '10', got: {}",
                filter_str
            );
            assert!(
                filter_str.contains("status") && filter_str.contains("active"),
                "Filter should contain 'status' and 'active', got: {}",
                filter_str
            );

            let response_body = write_ipc_file(&expected_data_ref);
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(response_body)
                .unwrap()
        });

        // Use only_if_expr with a Datafusion expression
        let expr = col("x").gt(lit(10)).and(col("status").eq(lit("active")));
        let data = table
            .query()
            .only_if_expr(expr)
            .execute()
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].as_ref().unwrap(), &expected_data);
    }

    fn schema_json() -> &'static str {
        r#"{"fields": [{"name": "id", "type": {"type": "int32"}, "nullable": true}]}"#
    }

    fn simple_describe_response() -> http::Response<String> {
        http::Response::builder()
            .status(200)
            .body(format!(r#"{{"version": 1, "schema": {}}}"#, schema_json()))
            .unwrap()
    }

    #[tokio::test]
    async fn test_multipart_write_happy_path() {
        use std::sync::Mutex;

        let create_count = Arc::new(AtomicUsize::new(0));
        let insert_count = Arc::new(AtomicUsize::new(0));
        let complete_count = Arc::new(AtomicUsize::new(0));
        let abort_count = Arc::new(AtomicUsize::new(0));
        let upload_ids = Arc::new(Mutex::new(Vec::<String>::new()));

        let create_count_c = create_count.clone();
        let insert_count_c = insert_count.clone();
        let complete_count_c = complete_count.clone();
        let abort_count_c = abort_count.clone();
        let upload_ids_c = upload_ids.clone();

        let table = Table::new_with_handler_version(
            "my_table",
            semver::Version::new(0, 4, 0),
            move |request| {
                let path = request.url().path();
                let query = request.url().query().unwrap_or("");

                if path == "/v1/table/my_table/describe/" {
                    return simple_describe_response();
                }

                if path == "/v1/table/my_table/multipart_write/create" {
                    create_count_c.fetch_add(1, Ordering::SeqCst);
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"upload_id": "test-upload-123"}"#.to_string())
                        .unwrap();
                }

                if path == "/v1/table/my_table/insert/" {
                    insert_count_c.fetch_add(1, Ordering::SeqCst);
                    let uid = url::form_urlencoded::parse(query.as_bytes())
                        .find(|(k, _)| k == "upload_id")
                        .map(|(_, v)| v.to_string());
                    upload_ids_c
                        .lock()
                        .unwrap()
                        .push(uid.expect("missing upload_id on insert"));
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 1}"#.to_string())
                        .unwrap();
                }

                if path == "/v1/table/my_table/multipart_write/complete" {
                    complete_count_c.fetch_add(1, Ordering::SeqCst);
                    let uid = url::form_urlencoded::parse(query.as_bytes())
                        .find(|(k, _)| k == "upload_id")
                        .map(|(_, v)| v.to_string());
                    upload_ids_c
                        .lock()
                        .unwrap()
                        .push(uid.expect("missing upload_id on complete"));
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 5}"#.to_string())
                        .unwrap();
                }

                if path == "/v1/table/my_table/multipart_write/abort" {
                    abort_count_c.fetch_add(1, Ordering::SeqCst);
                    return http::Response::builder()
                        .status(200)
                        .body(String::new())
                        .unwrap();
                }

                panic!("Unexpected request path: {}", path);
            },
        );

        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let result = table
            .add(vec![batch])
            .write_parallelism(2)
            .execute()
            .await
            .unwrap();

        assert_eq!(result.version, 5);
        assert_eq!(create_count.load(Ordering::SeqCst), 1);
        assert!(
            insert_count.load(Ordering::SeqCst) > 1,
            "Expected multiple insert calls, got {}",
            insert_count.load(Ordering::SeqCst)
        );
        assert_eq!(complete_count.load(Ordering::SeqCst), 1);
        assert_eq!(abort_count.load(Ordering::SeqCst), 0);

        let ids = upload_ids.lock().unwrap();
        assert!(
            ids.iter().all(|id| id == "test-upload-123"),
            "All requests should use the same upload_id, got: {:?}",
            *ids
        );
    }

    #[tokio::test]
    async fn test_multipart_write_progress() {
        let callback_count = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let last_total_tasks = Arc::new(AtomicUsize::new(0));
        let seen_done = Arc::new(std::sync::Mutex::new(false));

        let cb_count = callback_count.clone();
        let cb_active = max_active.clone();
        let cb_total = last_total_tasks.clone();
        let cb_done = seen_done.clone();

        let table = Table::new_with_handler_version(
            "my_table",
            semver::Version::new(0, 4, 0),
            move |request| {
                let path = request.url().path();

                if path == "/v1/table/my_table/describe/" {
                    return simple_describe_response();
                }
                if path == "/v1/table/my_table/multipart_write/create" {
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"upload_id": "prog-upload"}"#.to_string())
                        .unwrap();
                }
                if path == "/v1/table/my_table/insert/" {
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 1}"#.to_string())
                        .unwrap();
                }
                if path == "/v1/table/my_table/multipart_write/complete" {
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 3}"#.to_string())
                        .unwrap();
                }
                panic!("Unexpected request path: {}", path);
            },
        );

        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        table
            .add(vec![batch])
            .write_parallelism(2)
            .progress(move |p| {
                cb_count.fetch_add(1, Ordering::SeqCst);
                cb_active.fetch_max(p.active_tasks(), Ordering::SeqCst);
                cb_total.store(p.total_tasks(), Ordering::SeqCst);
                if p.done() {
                    *cb_done.lock().unwrap() = true;
                }
            })
            .execute()
            .await
            .unwrap();

        assert!(
            callback_count.load(Ordering::SeqCst) >= 1,
            "expected at least one progress callback"
        );
        assert!(*seen_done.lock().unwrap(), "must see done=true");
        assert_eq!(last_total_tasks.load(Ordering::SeqCst), 2);
        assert!(
            max_active.load(Ordering::SeqCst) >= 1,
            "expected at least one active task"
        );
    }

    #[tokio::test]
    async fn test_multipart_write_fallback_old_server() {
        let insert_count = Arc::new(AtomicUsize::new(0));
        let create_count = Arc::new(AtomicUsize::new(0));

        let insert_count_c = insert_count.clone();
        let create_count_c = create_count.clone();

        // Server version 0.3.0 does not support multipart writes
        let table = Table::new_with_handler_version(
            "my_table",
            semver::Version::new(0, 3, 0),
            move |request| {
                let path = request.url().path();

                if path == "/v1/table/my_table/describe/" {
                    return simple_describe_response();
                }

                if path.contains("multipart_write") {
                    create_count_c.fetch_add(1, Ordering::SeqCst);
                    panic!("Should not call multipart write endpoints on old server");
                }

                if path == "/v1/table/my_table/insert/" {
                    let query = request.url().query().unwrap_or("");
                    assert!(
                        !query.contains("upload_id"),
                        "Should not have upload_id for old server"
                    );
                    insert_count_c.fetch_add(1, Ordering::SeqCst);
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 2}"#.to_string())
                        .unwrap();
                }

                panic!("Unexpected request path: {}", path);
            },
        );

        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let result = table
            .add(vec![batch])
            .write_parallelism(2)
            .execute()
            .await
            .unwrap();

        assert_eq!(result.version, 2);
        assert_eq!(create_count.load(Ordering::SeqCst), 0);
        assert_eq!(insert_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_multipart_write_small_data_single_partition() {
        let insert_count = Arc::new(AtomicUsize::new(0));
        let create_count = Arc::new(AtomicUsize::new(0));

        let insert_count_c = insert_count.clone();
        let create_count_c = create_count.clone();

        let table = Table::new_with_handler_version(
            "my_table",
            semver::Version::new(0, 4, 0),
            move |request| {
                let path = request.url().path();

                if path == "/v1/table/my_table/describe/" {
                    return simple_describe_response();
                }

                if path.contains("multipart_write") {
                    create_count_c.fetch_add(1, Ordering::SeqCst);
                    panic!("Should not call multipart write endpoints for small data");
                }

                if path == "/v1/table/my_table/insert/" {
                    let query = request.url().query().unwrap_or("");
                    assert!(
                        !query.contains("upload_id"),
                        "Should not have upload_id for small data"
                    );
                    insert_count_c.fetch_add(1, Ordering::SeqCst);
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 2}"#.to_string())
                        .unwrap();
                }

                panic!("Unexpected request path: {}", path);
            },
        );

        // Small data: only 3 rows
        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let result = table.add(vec![batch]).execute().await.unwrap();

        assert_eq!(result.version, 2);
        assert_eq!(create_count.load(Ordering::SeqCst), 0);
        assert_eq!(insert_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_multipart_write_abort_on_insert_failure() {
        let create_count = Arc::new(AtomicUsize::new(0));
        let insert_count = Arc::new(AtomicUsize::new(0));
        let complete_count = Arc::new(AtomicUsize::new(0));
        let abort_count = Arc::new(AtomicUsize::new(0));

        let create_count_c = create_count.clone();
        let insert_count_c = insert_count.clone();
        let complete_count_c = complete_count.clone();
        let abort_count_c = abort_count.clone();

        let table = Table::new_with_handler_version(
            "my_table",
            semver::Version::new(0, 4, 0),
            move |request| {
                let path = request.url().path();

                if path == "/v1/table/my_table/describe/" {
                    return simple_describe_response();
                }

                if path == "/v1/table/my_table/multipart_write/create" {
                    create_count_c.fetch_add(1, Ordering::SeqCst);
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"upload_id": "test-upload-456"}"#.to_string())
                        .unwrap();
                }

                if path == "/v1/table/my_table/insert/" {
                    let count = insert_count_c.fetch_add(1, Ordering::SeqCst);
                    // Fail on the first insert with non-retryable status
                    if count == 0 {
                        return http::Response::builder()
                            .status(400)
                            .body("Bad Request".to_string())
                            .unwrap();
                    }
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 1}"#.to_string())
                        .unwrap();
                }

                if path == "/v1/table/my_table/multipart_write/complete" {
                    complete_count_c.fetch_add(1, Ordering::SeqCst);
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 5}"#.to_string())
                        .unwrap();
                }

                if path == "/v1/table/my_table/multipart_write/abort" {
                    abort_count_c.fetch_add(1, Ordering::SeqCst);
                    return http::Response::builder()
                        .status(200)
                        .body(String::new())
                        .unwrap();
                }

                panic!("Unexpected request path: {}", path);
            },
        );

        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let result = table.add(vec![batch]).write_parallelism(2).execute().await;

        assert!(result.is_err());
        assert_eq!(create_count.load(Ordering::SeqCst), 1);
        assert_eq!(complete_count.load(Ordering::SeqCst), 0);
        assert_eq!(abort_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_multipart_write_abort_on_complete_failure() {
        let abort_count = Arc::new(AtomicUsize::new(0));
        let abort_count_c = abort_count.clone();

        let table = Table::new_with_handler_version(
            "my_table",
            semver::Version::new(0, 4, 0),
            move |request| {
                let path = request.url().path();

                if path == "/v1/table/my_table/describe/" {
                    return simple_describe_response();
                }

                if path == "/v1/table/my_table/multipart_write/create" {
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"upload_id": "test-upload-789"}"#.to_string())
                        .unwrap();
                }

                if path == "/v1/table/my_table/insert/" {
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 1}"#.to_string())
                        .unwrap();
                }

                if path == "/v1/table/my_table/multipart_write/complete" {
                    return http::Response::builder()
                        .status(400)
                        .body("Bad Request".to_string())
                        .unwrap();
                }

                if path == "/v1/table/my_table/multipart_write/abort" {
                    abort_count_c.fetch_add(1, Ordering::SeqCst);
                    return http::Response::builder()
                        .status(200)
                        .body(String::new())
                        .unwrap();
                }

                panic!("Unexpected request path: {}", path);
            },
        );

        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let result = table.add(vec![batch]).write_parallelism(2).execute().await;

        assert!(result.is_err());
        assert_eq!(abort_count.load(Ordering::SeqCst), 1);
    }

    fn retry_config_no_backoff() -> ClientConfig {
        ClientConfig {
            retry_config: RetryConfig {
                retries: Some(3),
                connect_retries: Some(3),
                read_retries: Some(3),
                backoff_factor: Some(0.0),
                backoff_jitter: Some(0.0),
                statuses: Some(vec![502, 503]),
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_multipart_write_retry_on_partition_failure() {
        // All inserts for the first upload session return 503 (retryable).
        // After exhausting internal retries, the outer loop retries with a
        // new session and succeeds.
        let create_count = Arc::new(AtomicUsize::new(0));
        let complete_count = Arc::new(AtomicUsize::new(0));
        let abort_count = Arc::new(AtomicUsize::new(0));

        let create_count_c = create_count.clone();
        let complete_count_c = complete_count.clone();
        let abort_count_c = abort_count.clone();

        let table = Table::new_with_handler_version_and_config(
            "my_table",
            semver::Version::new(0, 4, 0),
            move |request| {
                let path = request.url().path();
                let query = request.url().query().unwrap_or("");

                if path == "/v1/table/my_table/describe/" {
                    return simple_describe_response();
                }

                if path == "/v1/table/my_table/multipart_write/create" {
                    let n = create_count_c.fetch_add(1, Ordering::SeqCst);
                    let body = format!(r#"{{"upload_id": "upload-{}"}}"#, n + 1);
                    return http::Response::builder().status(200).body(body).unwrap();
                }

                if path == "/v1/table/my_table/insert/" {
                    // Fail all inserts for the first session
                    if query.contains("upload_id=upload-1") {
                        return http::Response::builder()
                            .status(503)
                            .body("Service Unavailable".to_string())
                            .unwrap();
                    }
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 1}"#.to_string())
                        .unwrap();
                }

                if path == "/v1/table/my_table/multipart_write/complete" {
                    complete_count_c.fetch_add(1, Ordering::SeqCst);
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 7}"#.to_string())
                        .unwrap();
                }

                if path == "/v1/table/my_table/multipart_write/abort" {
                    abort_count_c.fetch_add(1, Ordering::SeqCst);
                    return http::Response::builder()
                        .status(200)
                        .body(String::new())
                        .unwrap();
                }

                panic!("Unexpected request path: {}", path);
            },
            retry_config_no_backoff(),
        );

        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let result = table
            .add(vec![batch])
            .write_parallelism(2)
            .execute()
            .await
            .unwrap();

        assert_eq!(result.version, 7);
        assert_eq!(create_count.load(Ordering::SeqCst), 2);
        assert_eq!(abort_count.load(Ordering::SeqCst), 1);
        assert_eq!(complete_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_multipart_write_retry_on_complete_failure() {
        // Complete returns 503 for the first session, succeeds for the second.
        let create_count = Arc::new(AtomicUsize::new(0));
        let abort_count = Arc::new(AtomicUsize::new(0));

        let create_count_c = create_count.clone();
        let abort_count_c = abort_count.clone();

        let table = Table::new_with_handler_version_and_config(
            "my_table",
            semver::Version::new(0, 4, 0),
            move |request| {
                let path = request.url().path();
                let query = request.url().query().unwrap_or("");

                if path == "/v1/table/my_table/describe/" {
                    return simple_describe_response();
                }

                if path == "/v1/table/my_table/multipart_write/create" {
                    let n = create_count_c.fetch_add(1, Ordering::SeqCst);
                    let body = format!(r#"{{"upload_id": "upload-{}"}}"#, n + 1);
                    return http::Response::builder().status(200).body(body).unwrap();
                }

                if path == "/v1/table/my_table/insert/" {
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 1}"#.to_string())
                        .unwrap();
                }

                if path == "/v1/table/my_table/multipart_write/complete" {
                    // Fail complete for first session
                    if query.contains("upload_id=upload-1") {
                        return http::Response::builder()
                            .status(503)
                            .body("Service Unavailable".to_string())
                            .unwrap();
                    }
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 9}"#.to_string())
                        .unwrap();
                }

                if path == "/v1/table/my_table/multipart_write/abort" {
                    abort_count_c.fetch_add(1, Ordering::SeqCst);
                    return http::Response::builder()
                        .status(200)
                        .body(String::new())
                        .unwrap();
                }

                panic!("Unexpected request path: {}", path);
            },
            retry_config_no_backoff(),
        );

        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let result = table
            .add(vec![batch])
            .write_parallelism(2)
            .execute()
            .await
            .unwrap();

        assert_eq!(result.version, 9);
        assert_eq!(create_count.load(Ordering::SeqCst), 2);
        assert_eq!(abort_count.load(Ordering::SeqCst), 1);
    }

    // ---- Read freshness header tests ------------------------------------

    #[test]
    fn test_compute_min_timestamp_combines_baseline_and_interval() {
        let now = SystemTime::now();
        let baseline = now - Duration::from_secs(60);

        // No interval, no baseline -> no header.
        assert_eq!(
            compute_min_timestamp(&FreshnessState::default(), None, now),
            None
        );

        // Baseline only -> baseline.
        let state = FreshnessState {
            min_version: None,
            checkout_baseline: Some(baseline),
            min_read_version: None,
        };
        assert_eq!(compute_min_timestamp(&state, None, now), Some(baseline));

        // ZERO interval, no baseline -> now.
        assert_eq!(
            compute_min_timestamp(&FreshnessState::default(), Some(Duration::ZERO), now),
            Some(now)
        );

        // Positive interval, no baseline -> now - interval.
        assert_eq!(
            compute_min_timestamp(
                &FreshnessState::default(),
                Some(Duration::from_secs(10)),
                now
            ),
            Some(now - Duration::from_secs(10))
        );

        // Both: pick the more-recent (i.e. tighter) constraint.
        // baseline = now-60, now-interval = now-10. now-10 is newer.
        let state = FreshnessState {
            min_version: None,
            checkout_baseline: Some(baseline),
            min_read_version: None,
        };
        assert_eq!(
            compute_min_timestamp(&state, Some(Duration::from_secs(10)), now),
            Some(now - Duration::from_secs(10))
        );

        // Both, baseline newer: pick baseline.
        let recent_baseline = now - Duration::from_secs(5);
        let state = FreshnessState {
            min_version: None,
            checkout_baseline: Some(recent_baseline),
            min_read_version: None,
        };
        assert_eq!(
            compute_min_timestamp(&state, Some(Duration::from_secs(60)), now),
            Some(recent_baseline)
        );
    }

    /// Allowed slop when comparing a header timestamp against a locally
    /// captured wall-clock bound. Tests run fast enough that 1s is plenty.
    const FRESHNESS_TOLERANCE: Duration = Duration::from_secs(1);

    fn capturing_handler<F>(
        body_for: F,
    ) -> (
        impl Fn(reqwest::Request) -> http::Response<String> + Clone + Send + Sync + 'static,
        Arc<std::sync::Mutex<Option<http::HeaderMap>>>,
    )
    where
        F: Fn(&str) -> String + Clone + Send + Sync + 'static,
    {
        let captured = Arc::new(std::sync::Mutex::new(None));
        let captured_c = captured.clone();
        let handler = move |request: reqwest::Request| {
            *captured_c.lock().unwrap() = Some(request.headers().clone());
            let path = request.url().path().to_string();
            http::Response::builder()
                .status(200)
                .body(body_for(&path))
                .unwrap()
        };
        (handler, captured)
    }

    fn parse_min_timestamp(headers: &http::HeaderMap) -> SystemTime {
        let value = headers
            .get("x-lancedb-min-timestamp")
            .expect("expected x-lancedb-min-timestamp header")
            .to_str()
            .unwrap();
        chrono::DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&chrono::Utc)
            .into()
    }

    #[tokio::test]
    async fn test_freshness_default_sends_no_headers() {
        let (handler, captured) = capturing_handler(|_| "42".to_string());
        let table = Table::new_with_handler("my_table", handler);

        let _ = table.count_rows(None).await.unwrap();

        let headers = captured.lock().unwrap().clone().unwrap();
        assert!(!headers.contains_key("x-lancedb-min-timestamp"));
        assert!(!headers.contains_key("x-lancedb-min-version"));
    }

    #[tokio::test]
    async fn test_freshness_zero_interval_sends_now() {
        let (handler, captured) = capturing_handler(|_| "42".to_string());
        let table =
            Table::new_with_handler_and_interval("my_table", handler, Some(Duration::from_secs(0)));

        let before = SystemTime::now();
        table.count_rows(None).await.unwrap();
        let after = SystemTime::now();

        let headers = captured.lock().unwrap().clone().unwrap();
        let sent = parse_min_timestamp(&headers);
        assert!(
            sent >= before - FRESHNESS_TOLERANCE && sent <= after + FRESHNESS_TOLERANCE,
            "expected timestamp roughly equal to wall clock"
        );
        assert!(!headers.contains_key("x-lancedb-min-version"));
    }

    #[tokio::test]
    async fn test_freshness_positive_interval_sends_now_minus_interval() {
        let (handler, captured) = capturing_handler(|_| "42".to_string());
        let interval = Duration::from_secs(30);
        let table = Table::new_with_handler_and_interval("my_table", handler, Some(interval));

        let before = SystemTime::now();
        table.count_rows(None).await.unwrap();
        let after = SystemTime::now();

        let headers = captured.lock().unwrap().clone().unwrap();
        let sent = parse_min_timestamp(&headers);
        assert!(
            sent >= before - interval - FRESHNESS_TOLERANCE
                && sent <= after - interval + FRESHNESS_TOLERANCE,
            "expected timestamp roughly equal to now - interval"
        );
    }

    #[tokio::test]
    async fn test_freshness_checkout_latest_sets_baseline() {
        let (handler, captured) = capturing_handler(|path| match path {
            "/v1/table/my_table/count_rows/" => "42".to_string(),
            _ => panic!("unexpected path: {}", path),
        });
        // No interval — only the baseline should drive the timestamp.
        let table = Table::new_with_handler_and_interval("my_table", handler, None);

        let before_checkout = SystemTime::now();
        table.checkout_latest().await.unwrap();
        let after_checkout = SystemTime::now();

        table.count_rows(None).await.unwrap();

        let headers = captured.lock().unwrap().clone().unwrap();
        let sent = parse_min_timestamp(&headers);
        assert!(
            sent >= before_checkout - FRESHNESS_TOLERANCE
                && sent <= after_checkout + FRESHNESS_TOLERANCE,
            "expected timestamp captured at checkout_latest() time"
        );
        assert!(!headers.contains_key("x-lancedb-min-version"));
    }

    #[tokio::test]
    async fn test_freshness_min_version_tracked_after_write() {
        let (handler, captured) = capturing_handler(|path| match path {
            "/v1/table/my_table/update/" => r#"{"rows_updated":1,"version":7}"#.to_string(),
            "/v1/table/my_table/count_rows/" => "42".to_string(),
            _ => panic!("unexpected path: {}", path),
        });
        let table = Table::new_with_handler("my_table", handler);

        let _ = table.update().column("a", "a + 1").execute().await.unwrap();
        // Update headers also pass through captured; reset by reading after.
        table.count_rows(None).await.unwrap();

        let headers = captured.lock().unwrap().clone().unwrap();
        assert_eq!(
            headers
                .get("x-lancedb-min-version")
                .unwrap()
                .to_str()
                .unwrap(),
            "7"
        );
    }

    /// A handler that records every request's headers and answers each read with
    /// an `x-lancedb-version` response header taken from `versions` (by call
    /// index, saturating at the last entry). An empty string means "no header".
    fn read_version_handler(
        versions: &'static [&'static str],
    ) -> (
        impl Fn(reqwest::Request) -> http::Response<String> + Clone + Send + Sync + 'static,
        Arc<std::sync::Mutex<Vec<http::HeaderMap>>>,
    ) {
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let requests_c = requests.clone();
        let call = Arc::new(AtomicUsize::new(0));
        let handler = move |request: reqwest::Request| {
            requests_c.lock().unwrap().push(request.headers().clone());
            let i = call.fetch_add(1, Ordering::SeqCst).min(versions.len() - 1);
            let mut builder = http::Response::builder().status(200);
            if !versions[i].is_empty() {
                builder = builder.header("x-lancedb-version", versions[i]);
            }
            builder.body("42".to_string()).unwrap()
        };
        (handler, requests)
    }

    #[tokio::test]
    async fn test_read_version_watermark_tracked_and_sent() {
        let (handler, requests) = read_version_handler(&["100", "100"]);
        let table = Table::new_with_handler("my_table", handler);

        // First read has no watermark yet; the response advertises version 100,
        // so the second read must floor the server at 100.
        table.count_rows(None).await.unwrap();
        table.count_rows(None).await.unwrap();

        let reqs = requests.lock().unwrap();
        assert!(!reqs[0].contains_key("x-lancedb-min-read-version"));
        assert_eq!(
            reqs[1]
                .get("x-lancedb-min-read-version")
                .unwrap()
                .to_str()
                .unwrap(),
            "100"
        );
    }

    #[tokio::test]
    async fn test_read_version_watermark_keeps_max() {
        // Server reports 100 then a stale 50; the watermark must not regress.
        let (handler, requests) = read_version_handler(&["100", "50", "50"]);
        let table = Table::new_with_handler("my_table", handler);

        table.count_rows(None).await.unwrap();
        table.count_rows(None).await.unwrap();
        table.count_rows(None).await.unwrap();

        let reqs = requests.lock().unwrap();
        assert_eq!(
            reqs[2]
                .get("x-lancedb-min-read-version")
                .unwrap()
                .to_str()
                .unwrap(),
            "100"
        );
    }

    #[tokio::test]
    async fn test_read_version_absent_header_no_watermark() {
        // An old server that doesn't return the version header leaves the
        // watermark unset, preserving backward compatibility.
        let (handler, requests) = read_version_handler(&[""]);
        let table = Table::new_with_handler("my_table", handler);

        table.count_rows(None).await.unwrap();
        table.count_rows(None).await.unwrap();

        let reqs = requests.lock().unwrap();
        assert!(!reqs[1].contains_key("x-lancedb-min-read-version"));
    }

    #[tokio::test]
    async fn test_read_version_watermark_reset_on_checkout_latest() {
        let (handler, requests) = read_version_handler(&["100", "100"]);
        let table = Table::new_with_handler("my_table", handler);

        table.count_rows(None).await.unwrap();
        table.checkout_latest().await.unwrap();
        table.count_rows(None).await.unwrap();

        // The read after checkout_latest starts from a clean slate.
        let reqs = requests.lock().unwrap();
        assert!(
            !reqs
                .last()
                .unwrap()
                .contains_key("x-lancedb-min-read-version")
        );
    }

    /// Like `capturing_handler`, but keeps a per-path snapshot of the headers
    /// from every request so tests can assert on a specific endpoint.
    #[allow(clippy::type_complexity)]
    fn path_capturing_handler<F>(
        body_for: F,
    ) -> (
        impl Fn(reqwest::Request) -> http::Response<String> + Clone + Send + Sync + 'static,
        Arc<std::sync::Mutex<HashMap<String, http::HeaderMap>>>,
    )
    where
        F: Fn(&str) -> String + Clone + Send + Sync + 'static,
    {
        let captured: Arc<std::sync::Mutex<HashMap<String, http::HeaderMap>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
        let captured_c = captured.clone();
        let handler = move |request: reqwest::Request| {
            let path = request.url().path().to_string();
            captured_c
                .lock()
                .unwrap()
                .insert(path.clone(), request.headers().clone());
            http::Response::builder()
                .status(200)
                .body(body_for(&path))
                .unwrap()
        };
        (handler, captured)
    }

    #[tokio::test]
    async fn test_freshness_checkout_validation_sends_no_min_version() {
        // After a write bumps min_version, calling checkout(v) must not let
        // that stale header ride along on the validating /describe/ request.
        let (handler, captured) = path_capturing_handler(|path| match path {
            "/v1/table/my_table/update/" => r#"{"rows_updated":1,"version":7}"#.to_string(),
            "/v1/table/my_table/describe/" => r#"{"version":5,"schema":{"fields":[]}}"#.to_string(),
            _ => panic!("unexpected path: {}", path),
        });
        let table = Table::new_with_handler("my_table", handler);

        table.update().column("a", "a + 1").execute().await.unwrap();
        table.checkout(5).await.unwrap();

        let captured = captured.lock().unwrap();
        let describe_headers = captured
            .get("/v1/table/my_table/describe/")
            .expect("describe should have been called by checkout(v)");
        assert!(
            !describe_headers.contains_key("x-lancedb-min-version"),
            "checkout(v) describe must not carry stale min_version",
        );
        assert!(!describe_headers.contains_key("x-lancedb-min-timestamp"));
    }

    #[tokio::test]
    async fn test_freshness_checkout_tag_resolve_sends_no_min_version() {
        // Same invariant for checkout_tag: the tag-resolve request must not
        // pick up a stale min_version from a prior write.
        let (handler, captured) = path_capturing_handler(|path| match path {
            "/v1/table/my_table/update/" => r#"{"rows_updated":1,"version":7}"#.to_string(),
            "/v1/table/my_table/tags/version/" => r#"{"version":5}"#.to_string(),
            _ => panic!("unexpected path: {}", path),
        });
        let table = Table::new_with_handler("my_table", handler);

        table.update().column("a", "a + 1").execute().await.unwrap();
        table.checkout_tag("v_initial").await.unwrap();

        let captured = captured.lock().unwrap();
        let resolve_headers = captured
            .get("/v1/table/my_table/tags/version/")
            .expect("tags/version should have been called by checkout_tag");
        assert!(
            !resolve_headers.contains_key("x-lancedb-min-version"),
            "checkout_tag resolve must not carry stale min_version",
        );
        assert!(!resolve_headers.contains_key("x-lancedb-min-timestamp"));
    }

    #[tokio::test]
    async fn test_freshness_checkout_clears_min_version() {
        let (handler, captured) = capturing_handler(|path| match path {
            "/v1/table/my_table/update/" => r#"{"rows_updated":1,"version":7}"#.to_string(),
            // checkout(5) needs to describe version 5 first
            "/v1/table/my_table/describe/" => r#"{"version":5,"schema":{"fields":[]}}"#.to_string(),
            "/v1/table/my_table/count_rows/" => "42".to_string(),
            _ => panic!("unexpected path: {}", path),
        });
        let table = Table::new_with_handler("my_table", handler);

        table.update().column("a", "a + 1").execute().await.unwrap();
        table.checkout(5).await.unwrap();
        table.count_rows(None).await.unwrap();

        let headers = captured.lock().unwrap().clone().unwrap();
        assert!(!headers.contains_key("x-lancedb-min-version"));
        assert!(!headers.contains_key("x-lancedb-min-timestamp"));
    }

    #[tokio::test]
    async fn test_update_field_metadata() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/update_field_metadata/"
            );
            http::Response::builder()
                .status(200)
                .body(r#"{"version": 7, "fields": {"category": {"unit": "label"}}}"#)
                .unwrap()
        });

        let result = table
            .update_field_metadata(&[FieldMetadataUpdate::new("category").set("unit", "label")])
            .await
            .unwrap();
        assert_eq!(result.version, 7);
    }

    // ----- Branch support -----

    /// Parse a request's in-memory JSON body. Only valid for JSON-body ops
    /// (not Arrow-stream inserts, whose body is a stream).
    fn request_body_json(request: &reqwest::Request) -> serde_json::Value {
        let bytes = request
            .body()
            .expect("request has a body")
            .as_bytes()
            .expect("body is in-memory");
        serde_json::from_slice(bytes).expect("body is valid JSON")
    }

    #[tokio::test]
    async fn test_create_branch_default_source() {
        use lance::dataset::refs::Ref;
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/branches/create/");
            let body = request_body_json(&request);
            assert_eq!(body["name"], "exp");
            assert!(
                body.get("from_branch").is_none(),
                "a main source omits from_branch"
            );
            assert!(
                body.get("from_version").is_none(),
                "a latest source omits from_version"
            );
            http::Response::builder().status(200).body("{}").unwrap()
        });
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        assert_eq!(branch.current_branch(), Some("exp".to_string()));
        assert_eq!(table.current_branch(), None);
    }

    #[tokio::test]
    async fn test_create_branch_from_branch_and_version() {
        use lance::dataset::refs::Ref;
        let table = Table::new_with_handler("my_table", |request| {
            let body = request_body_json(&request);
            assert_eq!(body["name"], "exp");
            assert_eq!(body["from_branch"], "base");
            assert_eq!(body["from_version"], 3);
            http::Response::builder().status(200).body("{}").unwrap()
        });
        table
            .create_branch("exp", Ref::Version(Some("base".into()), Some(3)))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_branch_from_main_normalizes_to_none() {
        use lance::dataset::refs::Ref;
        let table = Table::new_with_handler("my_table", |request| {
            let body = request_body_json(&request);
            assert!(
                body.get("from_branch").is_none(),
                "\"main\" normalizes to an absent from_branch"
            );
            assert_eq!(body["from_version"], 7);
            http::Response::builder().status(200).body("{}").unwrap()
        });
        table
            .create_branch("exp", Ref::Version(Some("main".into()), Some(7)))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_branch_from_version_number_on_main() {
        use lance::dataset::refs::Ref;
        // A bare version number on a main handle resolves to (main, version).
        let table = Table::new_with_handler("my_table", |request| {
            let body = request_body_json(&request);
            assert!(body.get("from_branch").is_none());
            assert_eq!(body["from_version"], 5);
            http::Response::builder().status(200).body("{}").unwrap()
        });
        table
            .create_branch("exp", Ref::VersionNumber(5))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_branch_from_tag_resolves_via_tags_endpoint() {
        use lance::dataset::refs::Ref;
        // A tag source has no from_tag in the create contract; it is resolved to
        // its (branch, version) via the tags/version endpoint first.
        let table = Table::new_with_handler("my_table", |request| match request.url().path() {
            "/v1/table/my_table/tags/version/" => {
                assert_eq!(request_body_json(&request)["tag"], "t");
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version":3,"branch":"base"}"#.to_string())
                    .unwrap()
            }
            "/v1/table/my_table/branches/create/" => {
                let body = request_body_json(&request);
                assert_eq!(body["name"], "exp");
                assert_eq!(body["from_branch"], "base");
                assert_eq!(body["from_version"], 3);
                http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap()
            }
            path => panic!("unexpected request path: {path}"),
        });
        table
            .create_branch("exp", Ref::Tag("t".into()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_branch_from_tag_on_main_normalizes() {
        use lance::dataset::refs::Ref;
        // A tag resolving to the main branch collapses from_branch to absent.
        let table = Table::new_with_handler("my_table", |request| match request.url().path() {
            "/v1/table/my_table/tags/version/" => http::Response::builder()
                .status(200)
                .body(r#"{"version":4,"branch":"main"}"#.to_string())
                .unwrap(),
            "/v1/table/my_table/branches/create/" => {
                let body = request_body_json(&request);
                assert!(
                    body.get("from_branch").is_none(),
                    "a resolved \"main\" normalizes to an absent from_branch"
                );
                assert_eq!(body["from_version"], 4);
                http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap()
            }
            path => panic!("unexpected request path: {path}"),
        });
        table
            .create_branch("exp", Ref::Tag("t".into()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_branch_invalid_request_maps_to_invalid_input() {
        use lance::dataset::refs::Ref;
        let table = Table::new_with_handler("my_table", |_| {
            http::Response::builder()
                .status(400)
                .body("unsafe branch name")
                .unwrap()
        });
        let err = table
            .create_branch("../evil", Ref::Version(None, None))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn test_create_branch_conflict_maps_to_already_exists() {
        use lance::dataset::refs::Ref;
        let table = Table::new_with_handler("my_table", |_| {
            http::Response::builder()
                .status(409)
                .body("branch already exists")
                .unwrap()
        });
        let err = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::TableAlreadyExists { .. }),
            "409 should map to AlreadyExists, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_create_branch_empty_name_rejected_client_side() {
        use lance::dataset::refs::Ref;
        // The empty name is rejected before any request is sent.
        let table = Table::new_with_handler("my_table", |request| -> http::Response<String> {
            panic!("unexpected request: {}", request.url().path())
        });
        let err = table
            .create_branch("", Ref::Version(None, None))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn test_list_branches() {
        use lance::dataset::refs::BranchIdentifier;
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/branches/list/");
            // A branch forked off main: the server omits `parentBranch` entirely
            // (skip_serializing_if), not `null`, so this mirrors the real wire.
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"branches":{"exp":{"parentVersion":2,"createAt":1234,"manifestSize":4096}}}"#,
                )
                .unwrap()
        });
        let branches = table.list_branches().await.unwrap();
        let exp = branches.get("exp").expect("exp present");
        assert_eq!(exp.parent_version, 2);
        assert_eq!(exp.create_at, 1234);
        assert_eq!(exp.manifest_size, 4096);
        assert_eq!(exp.parent_branch, None);
        assert!(exp.metadata.is_empty());
        // The server omits the internal lineage token; it defaults to the sentinel.
        assert_eq!(
            exp.identifier,
            BranchIdentifier::missing_identifier_sentinel()
        );
    }

    #[tokio::test]
    async fn test_delete_branch() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/branches/delete/");
            let body = request_body_json(&request);
            assert_eq!(body["name"], "exp");
            http::Response::builder().status(200).body("{}").unwrap()
        });
        table.delete_branch("exp").await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_branch_not_found() {
        let table = Table::new_with_handler("my_table", |_| {
            http::Response::builder()
                .status(404)
                .body("no such branch")
                .unwrap()
        });
        let err = table.delete_branch("ghost").await.unwrap_err();
        assert!(matches!(err, Error::TableNotFound { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn test_checkout_branch_validates_via_list() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.url().path(), "/v1/table/my_table/branches/list/");
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"branches":{"exp":{"parentBranch":null,"parentVersion":1,"createAt":1,"manifestSize":1}}}"#,
                )
                .unwrap()
        });
        let branch = table.checkout_branch("exp", None).await.unwrap();
        assert_eq!(branch.current_branch(), Some("exp".to_string()));
    }

    #[tokio::test]
    async fn test_checkout_branch_missing() {
        let table = Table::new_with_handler("my_table", |_| {
            http::Response::builder()
                .status(200)
                .body(r#"{"branches":{}}"#)
                .unwrap()
        });
        let err = table.checkout_branch("ghost", None).await.unwrap_err();
        assert!(matches!(err, Error::TableNotFound { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn test_checkout_main_returns_main_handle() {
        // "main" yields a main-scoped handle without any validation request.
        let table = Table::new_with_handler("my_table", |request| -> http::Response<String> {
            panic!("unexpected request: {}", request.url().path())
        });
        let main = table.checkout_branch("main", None).await.unwrap();
        assert_eq!(main.current_branch(), None);
    }

    #[tokio::test]
    async fn test_branch_count_rows_carries_branch_in_body() {
        use lance::dataset::refs::Ref;
        let table = Table::new_with_handler("my_table", |request| match request.url().path() {
            "/v1/table/my_table/branches/create/" => http::Response::builder()
                .status(200)
                .body("{}".to_string())
                .unwrap(),
            "/v1/table/my_table/count_rows/" => {
                let body = request_body_json(&request);
                assert_eq!(body["branch"], "exp");
                http::Response::builder()
                    .status(200)
                    .body("7".to_string())
                    .unwrap()
            }
            path => panic!("unexpected request path: {path}"),
        });
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        assert_eq!(branch.count_rows(None).await.unwrap(), 7);
    }

    #[tokio::test]
    async fn test_main_handle_omits_branch_in_body() {
        // A main handle must not send a branch field (byte-compatible with
        // pre-branch servers and the existing wire format).
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.url().path(), "/v1/table/my_table/count_rows/");
            let body = request_body_json(&request);
            assert!(
                body.get("branch").is_none(),
                "main handle must not send a branch field"
            );
            http::Response::builder().status(200).body("0").unwrap()
        });
        table.count_rows(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_branch_update_and_delete_carry_branch() {
        use lance::dataset::refs::Ref;
        let table = Table::new_with_handler("my_table", |request| match request.url().path() {
            "/v1/table/my_table/branches/create/" => http::Response::builder()
                .status(200)
                .body("{}".to_string())
                .unwrap(),
            "/v1/table/my_table/update/" => {
                assert_eq!(request_body_json(&request)["branch"], "exp");
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version":5}"#.to_string())
                    .unwrap()
            }
            "/v1/table/my_table/delete/" => {
                assert_eq!(request_body_json(&request)["branch"], "exp");
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version":6,"num_deleted_rows":1}"#.to_string())
                    .unwrap()
            }
            path => panic!("unexpected request path: {path}"),
        });
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        branch
            .update()
            .column("a", "a + 1")
            .execute()
            .await
            .unwrap();
        branch.delete("a > 1").await.unwrap();
    }

    #[tokio::test]
    async fn test_branch_list_indices_carries_branch_in_body() {
        use lance::dataset::refs::Ref;
        // list_indices posts to index/list and then fetches the schema (describe)
        // to resolve column names; both must carry the branch.
        let describe_body =
            describe_response(&Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let table =
            Table::new_with_handler("my_table", move |request| match request.url().path() {
                "/v1/table/my_table/branches/create/" => http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap(),
                "/v1/table/my_table/index/list/" => {
                    assert_eq!(request_body_json(&request)["branch"], "exp");
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"indexes":[]}"#.to_string())
                        .unwrap()
                }
                "/v1/table/my_table/describe/" => {
                    assert_eq!(request_body_json(&request)["branch"], "exp");
                    http::Response::builder()
                        .status(200)
                        .body(describe_body.clone())
                        .unwrap()
                }
                path => panic!("unexpected request path: {path}"),
            });
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        assert!(branch.list_indices().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_branch_list_versions_carries_query_param() {
        use lance::dataset::refs::Ref;
        let table = Table::new_with_handler("my_table", |request| match request.url().path() {
            "/v1/table/my_table/branches/create/" => http::Response::builder()
                .status(200)
                .body("{}".to_string())
                .unwrap(),
            "/v1/table/my_table/version/list/" => {
                assert_eq!(
                    request
                        .url()
                        .query_pairs()
                        .find(|(k, _)| k == "branch")
                        .map(|(_, v)| v.into_owned()),
                    Some("exp".to_string()),
                    "version/list must carry ?branch=exp"
                );
                http::Response::builder()
                    .status(200)
                    .body(r#"{"versions":[]}"#.to_string())
                    .unwrap()
            }
            path => panic!("unexpected request path: {path}"),
        });
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        assert!(branch.list_versions().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_branch_drop_index_carries_query_param() {
        use lance::dataset::refs::Ref;
        let table = Table::new_with_handler("my_table", |request| match request.url().path() {
            "/v1/table/my_table/branches/create/" => http::Response::builder()
                .status(200)
                .body("{}".to_string())
                .unwrap(),
            "/v1/table/my_table/index/my_idx/drop/" => {
                assert_eq!(
                    request
                        .url()
                        .query_pairs()
                        .find(|(k, _)| k == "branch")
                        .map(|(_, v)| v.into_owned()),
                    Some("exp".to_string())
                );
                http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap()
            }
            path => panic!("unexpected request path: {path}"),
        });
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        branch.drop_index("my_idx").await.unwrap();
    }

    #[tokio::test]
    async fn test_branch_insert_carries_query_param() {
        use lance::dataset::refs::Ref;
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let describe_body = describe_response(&data.schema());
        let table =
            Table::new_with_handler("my_table", move |request| match request.url().path() {
                "/v1/table/my_table/branches/create/" => http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap(),
                "/v1/table/my_table/describe/" => {
                    // schema() fetch on the branch handle carries branch in the body.
                    assert_eq!(request_body_json(&request)["branch"], "exp");
                    http::Response::builder()
                        .status(200)
                        .body(describe_body.clone())
                        .unwrap()
                }
                "/v1/table/my_table/insert/" => {
                    assert_eq!(
                        request
                            .url()
                            .query_pairs()
                            .find(|(k, _)| k == "branch")
                            .map(|(_, v)| v.into_owned()),
                        Some("exp".to_string()),
                        "insert must carry ?branch=exp"
                    );
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version":2}"#.to_string())
                        .unwrap()
                }
                path => panic!("unexpected request path: {path}"),
            });
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        branch.add(data.clone()).execute().await.unwrap();
    }

    #[tokio::test]
    async fn test_branch_tag_create_carries_branch_in_body() {
        use lance::dataset::refs::Ref;
        let table = Table::new_with_handler("my_table", |request| match request.url().path() {
            "/v1/table/my_table/branches/create/" => http::Response::builder()
                .status(200)
                .body("{}".to_string())
                .unwrap(),
            "/v1/table/my_table/tags/create/" => {
                let body = request_body_json(&request);
                assert_eq!(body["branch"], "exp");
                assert_eq!(body["tag"], "v1");
                http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap()
            }
            path => panic!("unexpected request path: {path}"),
        });
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        branch.tags().await.unwrap().create("v1", 1).await.unwrap();
    }

    #[tokio::test]
    async fn test_checkout_branch_version_forwards_branch() {
        // Branch versions overlap main's, so the version must resolve on the
        // branch's own chain -- the validating describe and reads carry both.
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let describe_body = describe_response(&schema);
        let table = Table::new_with_handler("my_table", move |request| {
            match request.url().path() {
                "/v1/table/my_table/branches/list/" => http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"branches":{"exp":{"parentBranch":null,"parentVersion":1,"createAt":1,"manifestSize":1}}}"#
                            .to_string(),
                    )
                    .unwrap(),
                "/v1/table/my_table/describe/" => {
                    let body = request_body_json(&request);
                    assert_eq!(body["branch"], "exp", "checkout validate carries branch");
                    assert_eq!(body["version"], 2, "checkout validate carries version");
                    http::Response::builder().status(200).body(describe_body.clone()).unwrap()
                }
                "/v1/table/my_table/count_rows/" => {
                    let body = request_body_json(&request);
                    assert_eq!(body["branch"], "exp");
                    assert_eq!(body["version"], 2, "overlapping version resolves on the branch chain");
                    http::Response::builder().status(200).body("3".to_string()).unwrap()
                }
                path => panic!("unexpected request path: {path}"),
            }
        });
        let branch = table.checkout_branch("exp", Some(2)).await.unwrap();
        assert_eq!(branch.current_branch(), Some("exp".to_string()));
        assert_eq!(branch.count_rows(None).await.unwrap(), 3);
    }

    fn branch_query_param(request: &reqwest::Request) -> Option<String> {
        request
            .url()
            .query_pairs()
            .find(|(k, _)| k == "branch")
            .map(|(_, v)| v.into_owned())
    }

    #[tokio::test]
    async fn test_branch_query_carries_branch_in_body() {
        use lance::dataset::refs::Ref;
        // /query/ is the hot read path; the branch must ride in the JSON body.
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let data_ref = data.clone();
        let table =
            Table::new_with_handler("my_table", move |request| match request.url().path() {
                "/v1/table/my_table/branches/create/" => http::Response::builder()
                    .status(200)
                    .body(b"{}".to_vec())
                    .unwrap(),
                "/v1/table/my_table/query/" => {
                    assert_eq!(request_body_json(&request)["branch"], "exp");
                    http::Response::builder()
                        .status(200)
                        .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                        .body(write_ipc_file(&data_ref))
                        .unwrap()
                }
                path => panic!("unexpected request path: {path}"),
            });
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        let rows: usize = branch
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 3);
    }

    #[tokio::test]
    async fn test_branch_merge_insert_carries_query_param() {
        use lance::dataset::refs::Ref;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let data: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            [Ok(batch.clone())],
            batch.schema(),
        ));
        let table = Table::new_with_handler("my_table", move |request| {
            match request.url().path() {
                "/v1/table/my_table/branches/create/" => http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap(),
                "/v1/table/my_table/merge_insert/" => {
                    assert_eq!(
                        branch_query_param(&request).as_deref(),
                        Some("exp"),
                        "merge_insert must carry ?branch=exp"
                    );
                    http::Response::builder()
                        .status(200)
                        .body(
                            r#"{"version":2,"num_deleted_rows":0,"num_inserted_rows":3,"num_updated_rows":0}"#
                                .to_string(),
                        )
                        .unwrap()
                }
                path => panic!("unexpected request path: {path}"),
            }
        });
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        branch.merge_insert(&["id"]).execute(data).await.unwrap();
    }

    #[tokio::test]
    async fn test_branch_multipart_write_carries_query_param() {
        use lance::dataset::refs::Ref;
        // The multipart path (create -> insert parts -> complete) must carry
        // ?branch= on every leg; an old server version forces it.
        let table = Table::new_with_handler_version(
            "my_table",
            semver::Version::new(0, 4, 0),
            move |request| match request.url().path() {
                "/v1/table/my_table/branches/create/" => http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap(),
                "/v1/table/my_table/describe/" => simple_describe_response(),
                "/v1/table/my_table/multipart_write/create" => {
                    assert_eq!(branch_query_param(&request).as_deref(), Some("exp"));
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"upload_id": "u1"}"#.to_string())
                        .unwrap()
                }
                "/v1/table/my_table/insert/" => {
                    assert_eq!(
                        branch_query_param(&request).as_deref(),
                        Some("exp"),
                        "multipart insert must carry ?branch=exp"
                    );
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 1}"#.to_string())
                        .unwrap()
                }
                "/v1/table/my_table/multipart_write/complete" => {
                    assert_eq!(branch_query_param(&request).as_deref(), Some("exp"));
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 5}"#.to_string())
                        .unwrap()
                }
                path => panic!("unexpected request path: {path}"),
            },
        );
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        branch
            .add(vec![batch])
            .write_parallelism(2)
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_branch_restore_carries_branch_in_body() {
        use lance::dataset::refs::Ref;
        let table =
            Table::new_with_handler("my_table", move |request| match request.url().path() {
                "/v1/table/my_table/branches/create/" => http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap(),
                "/v1/table/my_table/restore/" => {
                    assert_eq!(request_body_json(&request)["branch"], "exp");
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version":1}"#.to_string())
                        .unwrap()
                }
                path => panic!("unexpected request path: {path}"),
            });
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        branch.restore().await.unwrap();
    }

    #[tokio::test]
    async fn test_branch_create_index_carries_branch_in_body() {
        use lance::dataset::refs::Ref;
        // create_index fetches the schema (describe) to resolve the column and
        // then posts create_index; both must carry the branch in the body.
        let describe_body =
            describe_response(&Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let table =
            Table::new_with_handler("my_table", move |request| match request.url().path() {
                "/v1/table/my_table/branches/create/" => http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap(),
                "/v1/table/my_table/describe/" => {
                    assert_eq!(request_body_json(&request)["branch"], "exp");
                    http::Response::builder()
                        .status(200)
                        .body(describe_body.clone())
                        .unwrap()
                }
                "/v1/table/my_table/create_index/" => {
                    assert_eq!(request_body_json(&request)["branch"], "exp");
                    http::Response::builder()
                        .status(200)
                        .body("{}".to_string())
                        .unwrap()
                }
                path => panic!("unexpected request path: {path}"),
            });
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        branch
            .create_index(&["a"], Index::BTree(Default::default()))
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_branch_column_ops_carry_branch_in_body() {
        use lance::dataset::refs::Ref;
        // add_columns / alter_columns / drop_columns all stamp the branch into
        // the JSON body via apply_branch_body.
        let table =
            Table::new_with_handler("my_table", move |request| match request.url().path() {
                "/v1/table/my_table/branches/create/" => http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap(),
                "/v1/table/my_table/add_columns/"
                | "/v1/table/my_table/alter_columns/"
                | "/v1/table/my_table/drop_columns/" => {
                    assert_eq!(
                        request_body_json(&request)["branch"],
                        "exp",
                        "{} must carry the branch",
                        request.url().path()
                    );
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version":43}"#.to_string())
                        .unwrap()
                }
                path => panic!("unexpected request path: {path}"),
            });
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        branch
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![("b".into(), "a + 1".into())]),
                None,
            )
            .await
            .unwrap();
        branch
            .alter_columns(&[ColumnAlteration::new("a".into()).rename("b".into())])
            .await
            .unwrap();
        branch.drop_columns(&["a"]).await.unwrap();
    }

    #[tokio::test]
    async fn test_branch_update_field_metadata_carries_branch_in_body() {
        use lance::dataset::refs::Ref;
        let table =
            Table::new_with_handler("my_table", move |request| match request.url().path() {
                "/v1/table/my_table/branches/create/" => http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap(),
                "/v1/table/my_table/update_field_metadata/" => {
                    assert_eq!(request_body_json(&request)["branch"], "exp");
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version":7,"fields":{}}"#.to_string())
                        .unwrap()
                }
                path => panic!("unexpected request path: {path}"),
            });
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        branch
            .update_field_metadata(&[FieldMetadataUpdate::new("category").set("unit", "label")])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_branch_index_stats_carries_branch_in_body() {
        use lance::dataset::refs::Ref;
        let table = Table::new_with_handler("my_table", move |request| {
            match request.url().path() {
                "/v1/table/my_table/branches/create/" => http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap(),
                "/v1/table/my_table/index/my_index/stats/" => {
                    assert_eq!(request_body_json(&request)["branch"], "exp");
                    http::Response::builder()
                        .status(200)
                        .body(
                            r#"{"num_indexed_rows":1,"num_unindexed_rows":0,"index_type":"IVF_PQ","distance_type":"l2"}"#
                                .to_string(),
                        )
                        .unwrap()
                }
                path => panic!("unexpected request path: {path}"),
            }
        });
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        assert!(branch.index_stats("my_index").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_branch_stats_attaches_body_while_main_omits_it() {
        use lance::dataset::refs::Ref;
        // stats has a bespoke conditional body: a main handle stays a bodyless
        // POST, while a branch handle attaches {"branch": ...}.
        let stats_body = r#"{"total_bytes":1,"num_rows":3,"num_indices":0,"fragment_stats":{"num_fragments":1,"num_small_fragments":0,"lengths":{"min":3,"max":3,"mean":3,"p25":3,"p50":3,"p75":3,"p99":3}}}"#;
        let table =
            Table::new_with_handler("my_table", move |request| match request.url().path() {
                "/v1/table/my_table/branches/create/" => http::Response::builder()
                    .status(200)
                    .body(stats_body.to_string())
                    .unwrap(),
                "/v1/table/my_table/stats/" => {
                    match request.body() {
                        // main handle: byte-identical to the pre-branch wire format.
                        None => {}
                        // branch handle: branch travels in the body.
                        Some(_) => assert_eq!(request_body_json(&request)["branch"], "exp"),
                    }
                    http::Response::builder()
                        .status(200)
                        .body(stats_body.to_string())
                        .unwrap()
                }
                path => panic!("unexpected request path: {path}"),
            });
        table.stats().await.unwrap();
        let branch = table
            .create_branch("exp", Ref::Version(None, None))
            .await
            .unwrap();
        branch.stats().await.unwrap();
    }
}
