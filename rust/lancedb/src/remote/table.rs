// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

pub mod blobs;
pub mod insert;

use self::insert::{RemoteWriteExec, WriteOp};
use super::client::RequestResultExt;
use super::client::{HttpSend, RestfulLanceDbClient, Sender};
use super::db::ServerVersion;
use super::sql::SqlClient;
use super::{ARROW_FILE_CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE, extract_job_id};
use crate::blob::BlobFile;
use crate::data::scannable::{PeekedScannable, Scannable, estimate_write_partitions};
use crate::expr::expr_to_sql_string;
use crate::index::Index;
use crate::index::IndexStatistics;
use crate::index::scalar::FtsQuery;
use crate::index::waiter::wait_for_index;
use crate::job::Job;
use crate::materialized_view::{
    MaterializedViewDefinition, MaterializedViewInfo, RefreshMaterializedViewResult,
};
use crate::query::wal_fusion::PkFusionMemory; // WAL-PK-FUSION: delete.
use crate::query::{QueryFilter, QueryRequest, Select, VectorQueryRequest};
use crate::remote::job::RemoteJob;
use crate::table::AddColumnsResult;
use crate::table::AddResult;
use crate::table::BranchDiff;
use crate::table::CherryPickResult;
use crate::table::DeleteResult;
use crate::table::DropColumnsResult;
use crate::table::LsmStats;
use crate::table::LsmWriteSpec;
use crate::table::MergeResult;
use crate::table::Tags;
use crate::table::UpdateResult;
use crate::table::lsm_stats::GetLsmStatsResponse;
use crate::table::merge::MergeFilter;
use crate::table::query::create_multi_vector_plan;
use crate::table::write_progress::FinishOnDrop;
use crate::table::{
    AlterColumnsResult, FieldMetadataUpdate, RefreshColumnResult, UpdateFieldMetadataResult,
};
use crate::table::{AnyQuery, Filter, Predicate, PreprocessingOutput, TableStatistics};
use crate::utils::background_cache::BackgroundCache;
use crate::utils::{
    MaxBatchLengthStream, TimeoutStream, public_fts_field_path_by_id, resolve_arrow_field_path,
    resolve_arrow_fts_field_path, supported_btree_data_type, supported_vector_data_type,
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
use arrow_array::{LargeBinaryArray, RecordBatch, RecordBatchReader};
use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_schema::{ArrowError, DataType, SchemaRef};
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
use std::collections::{HashMap, HashSet};
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
const SCHEMA_SELECTOR_CHANGED: &str = "table selector changed while fetching schema";

fn fts_query_requires_document_granularity_support(query: &FtsQuery) -> bool {
    match query {
        // Combined-fields queries do not expose a document granularity option.
        FtsQuery::CombinedFields(_) => false,
        FtsQuery::Match(query) => query
            .document_granularity
            .is_some_and(|granularity| granularity.is_list_element()),
        FtsQuery::Phrase(query) => query
            .document_granularity
            .is_some_and(|granularity| granularity.is_list_element()),
        FtsQuery::Boost(query) => {
            fts_query_requires_document_granularity_support(&query.positive)
                || fts_query_requires_document_granularity_support(&query.negative)
        }
        FtsQuery::MultiMatch(query) => query.match_queries.iter().any(|query| {
            query
                .document_granularity
                .is_some_and(|granularity| granularity.is_list_element())
        }),
        FtsQuery::Boolean(query) => query
            .must
            .iter()
            .chain(&query.should)
            .chain(&query.must_not)
            .any(fts_query_requires_document_granularity_support),
    }
}

/// Per-table state driving the freshness headers (`x-lancedb-min-version`,
/// `x-lancedb-min-timestamp`, and `x-lancedb-min-read-version`) sent on table
/// requests.
#[derive(Debug, Default, Clone, Copy)]
struct FreshnessState {
    /// Identifies the handle timeline that produced this state. Explicit
    /// checkout operations advance the generation so responses from older
    /// in-flight requests cannot repopulate the new timeline's constraints.
    generation: u64,
    /// Exact-version, tag, and snapshot handles must not carry latest-timeline
    /// constraints. Their request body already selects the precise version.
    pinned: bool,
    /// Provides read-your-write within a single handle: writes that return a
    /// version update this, and reads send it as `x-lancedb-min-version`.
    min_version: Option<u64>,
    /// Highest committed dataset version advertised by a successful table
    /// response on this handle. Later requests send it as
    /// `x-lancedb-min-read-version` so a load-balanced query
    /// node whose cache is behind this version must refresh before serving,
    /// giving monotonic reads across nodes regardless of which one the load
    /// balancer routes to. Unlike write result bodies, this is sourced only
    /// from the server's committed dataset-version response header or other
    /// typed dataset-version fields, so WAL entry ids cannot enter it.
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

/// Snapshot of the headers that should be attached to a single table request.
#[derive(Debug, Default, Clone, Copy)]
struct FreshnessHeaders {
    generation: u64,
    min_version: Option<u64>,
    min_timestamp: Option<SystemTime>,
    min_read_version: Option<u64>,
}

#[derive(Debug, Default, Clone, Copy)]
struct ReadSnapshot {
    version: Option<u64>,
    freshness_state: FreshnessState,
    freshness: FreshnessHeaders,
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

    fn observe_version(self, freshness: &Mutex<FreshnessState>, version: u64) {
        track_read_version_for_generation(freshness, self.generation, version);
    }

    fn observe_headers(
        self,
        freshness: &Mutex<FreshnessState>,
        headers: &reqwest::header::HeaderMap,
    ) {
        if let Some(version) = headers
            .get(&VERSION_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
        {
            self.observe_version(freshness, version);
        }
    }

    fn update_if_current(
        self,
        freshness: &Mutex<FreshnessState>,
        update: impl FnOnce(&mut FreshnessState),
    ) {
        let mut state = freshness.lock().unwrap();
        if state.generation == self.generation {
            update(&mut state);
        }
    }

    fn is_current(self, freshness: &Mutex<FreshnessState>) -> bool {
        freshness.lock().unwrap().generation == self.generation
    }
}

fn track_read_version(freshness: &Mutex<FreshnessState>, version: u64) {
    if version == 0 {
        return;
    }
    let mut state = freshness.lock().unwrap();
    state.min_read_version = Some(state.min_read_version.map_or(version, |v| v.max(version)));
}

fn track_read_version_for_generation(
    freshness: &Mutex<FreshnessState>,
    generation: u64,
    version: u64,
) {
    if version == 0 {
        return;
    }
    let mut state = freshness.lock().unwrap();
    if state.generation == generation {
        state.min_read_version = Some(state.min_read_version.map_or(version, |v| v.max(version)));
    }
}

/// A backfill job whose successful wait establishes a read-freshness
/// baseline on the submitting handle, so a later read cannot be served
/// from a cache older than the completed fill. A handle pinned by checkout
/// at completion keeps its time-travel view instead.
struct FreshnessJob<S: HttpSend> {
    inner: RemoteJob<S>,
    freshness: Arc<Mutex<FreshnessState>>,
    version: Arc<RwLock<Option<u64>>>,
    tracked_result: TrackedJobResult,
    freshness_request: FreshnessHeaders,
}

#[derive(Clone, Copy)]
enum TrackedJobResult {
    None,
    RefreshColumn,
}

#[async_trait]
impl<S: HttpSend> crate::job::JobHandle for FreshnessJob<S> {
    fn id(&self) -> Option<&str> {
        crate::job::JobHandle::id(&self.inner)
    }

    async fn status(&self) -> Result<String> {
        crate::job::JobHandle::status(&self.inner).await
    }

    async fn describe(&self) -> Result<crate::database::JobDescription> {
        crate::job::JobHandle::describe(&self.inner).await
    }

    async fn events(
        &self,
        request: crate::job::JobEventsRequest,
    ) -> Result<Vec<arrow_array::RecordBatch>> {
        crate::job::JobHandle::events(&self.inner, request).await
    }

    async fn wait(&self) -> Result<crate::job::TerminalResult> {
        let result = crate::job::JobHandle::wait(&self.inner).await?;
        let version = self.version.read().await;
        if version.is_none() {
            let result_version = match self.tracked_result {
                TrackedJobResult::None => None,
                TrackedJobResult::RefreshColumn => result.value().and_then(|value| {
                    serde_json::from_value::<crate::function::RefreshColumnResult>(value.clone())
                        .ok()
                        .map(|result| {
                            result
                                .published_version
                                .map_or(result.source_version, |version| {
                                    version.max(result.source_version)
                                })
                        })
                }),
            }
            .filter(|version| *version != 0);
            if let Some(version) = result_version {
                self.freshness_request
                    .observe_version(&self.freshness, version);
            } else {
                self.freshness_request
                    .update_if_current(&self.freshness, |state| {
                        state.checkout_baseline = Some(SystemTime::now());
                    });
            }
        }
        Ok(result)
    }

    async fn cancel(&self) -> Result<()> {
        crate::job::JobHandle::cancel(&self.inner).await
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

fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn freshness_headers_snapshot(
    freshness: &Mutex<FreshnessState>,
    interval: Option<Duration>,
) -> FreshnessHeaders {
    freshness_state_snapshot(freshness, interval).1
}

fn freshness_state_snapshot(
    freshness: &Mutex<FreshnessState>,
    interval: Option<Duration>,
) -> (FreshnessState, FreshnessHeaders) {
    let state = *freshness.lock().unwrap();
    if state.pinned {
        return (
            state,
            FreshnessHeaders {
                generation: state.generation,
                ..FreshnessHeaders::default()
            },
        );
    }
    (
        state,
        FreshnessHeaders {
            generation: state.generation,
            min_version: state.min_version,
            min_timestamp: compute_min_timestamp(&state, interval, SystemTime::now()),
            min_read_version: state.min_read_version,
        },
    )
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
            .client
            .post(&format!("/v1/table/{}/tags/list/", self.inner.identifier));
        let (request_id, response) = self.inner.send_unfenced(request, true).await?;
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
        let request = self.inner.client.post(&format!(
            "/v1/table/{}/tags/version/",
            self.inner.identifier
        ));
        self.inner
            .resolve_tag_version_with_request(tag, request, false)
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

        let (request_id, response) = self.inner.send_unfenced(request, true).await?;
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
    sql_client: Option<SqlClient>,

    version: Arc<RwLock<Option<u64>>>,
    location: RwLock<Option<String>>,
    schema_cache: BackgroundCache<SchemaRef, Error>,
    // WAL-PK-FUSION: delete, with its initializers below.
    wal_pk_fusion: PkFusionMemory,
    freshness: Arc<Mutex<FreshnessState>>,
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
    async fn submit_create_index(&self, mut index: IndexBuilder) -> Result<Option<String>> {
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
        if matches!(
            &index.index,
            Index::FTS(params) if params.get_document_granularity().is_list_element()
        ) && !self.server_version.support_fts_document_granularity()
        {
            return Err(Error::NotSupported {
                message: "FTS document granularity requires remote server version 0.6.0 or later"
                    .into(),
            });
        }
        let schema = self.schema().await?;
        let (canonical_column, field) = match &index.index {
            Index::FTS(_) => resolve_arrow_fts_field_path(&schema, &column)?,
            _ => resolve_arrow_field_path(&schema, &column)?,
        };
        let mut body = serde_json::json!({
            "column": canonical_column
        });

        if !index.replace {
            body["replace"] = false.into();
        }

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
            Index::ZoneMap(p) => ("ZONEMAP", Some(to_json(p)?)),
            Index::NGram(p) => ("NGRAM", Some(to_json(p)?)),
            Index::BloomFilter(p) => ("BLOOM_FILTER", Some(to_json(p)?)),
            Index::RTree(p) => ("RTREE", Some(to_json(p)?)),
            Index::FTS(p) => {
                let mut params = to_json(p)?;
                if p.get_document_granularity().is_list_element() {
                    params["document_granularity"] = "list_element".into();
                }
                ("FTS", Some(params))
            }
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

        let response = self.check_table_response(&request_id, response).await?;
        let job_id = response
            .text()
            .await
            .ok()
            .and_then(|body| extract_job_id(&body));

        if let Some(wait_timeout) = index.wait_timeout {
            let index_name = index.name.unwrap_or_else(|| format!("{}_idx", column));
            self.wait_for_index(&[&index_name], wait_timeout).await?;
        }

        Ok(job_id)
    }

    pub(super) fn new_with_sql_client(
        client: RestfulLanceDbClient<S>,
        name: String,
        namespace: Vec<String>,
        identifier: String,
        server_version: ServerVersion,
        sql_client: Option<SqlClient>,
    ) -> Self {
        Self {
            client,
            name,
            namespace,
            identifier,
            server_version,
            sql_client,
            version: Arc::new(RwLock::new(None)),
            location: RwLock::new(None),
            schema_cache: BackgroundCache::new(SCHEMA_CACHE_TTL, SCHEMA_CACHE_REFRESH_WINDOW),
            wal_pk_fusion: PkFusionMemory::default(),
            freshness: Arc::new(Mutex::new(FreshnessState::default())),
            branch: None,
        }
    }

    /// Seed the schema cache from a `describe` body the caller already fetched.
    ///
    /// Best effort. `open_table` succeeds today without reading this body, so a
    /// body we cannot parse leaves the cache empty and the next schema read
    /// fetches it again through the path that reports a real error.
    pub(crate) fn seed_schema(&self, describe_body: &str) {
        let Ok(description) = serde_json::from_str::<TableDescription>(describe_body) else {
            return;
        };
        self.track_read_version(description.version);
        if let Ok(schema) = arrow_schema::Schema::try_from(description.schema) {
            self.schema_cache.seed(Arc::new(schema));
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
            sql_client: self.sql_client.clone(),
            version: Arc::new(RwLock::new(None)),
            location: RwLock::new(None),
            schema_cache: BackgroundCache::new(SCHEMA_CACHE_TTL, SCHEMA_CACHE_REFRESH_WINDOW),
            wal_pk_fusion: PkFusionMemory::default(),
            freshness: Arc::new(Mutex::new(FreshnessState::default())),
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
        self.describe_read_snapshot(self.snapshot_read_state().await)
            .await
    }

    async fn describe_read_snapshot(
        &self,
        read_snapshot: ReadSnapshot,
    ) -> Result<TableDescription> {
        let request = self
            .client
            .post(&format!("/v1/table/{}/describe/", self.identifier));
        self.describe_with_request(
            request,
            read_snapshot.version,
            Some(read_snapshot.freshness),
        )
        .await
    }

    async fn schema_read_snapshot(&self, read_snapshot: ReadSnapshot) -> Result<SchemaRef> {
        if read_snapshot.freshness.is_current(&self.freshness)
            && let Some(schema) = self.schema_cache.try_get()
            && read_snapshot.freshness.is_current(&self.freshness)
        {
            return Ok(schema);
        }

        let description = self.describe_read_snapshot(read_snapshot).await?;
        Ok(Arc::new(description.schema.try_into()?))
    }

    async fn resolve_tag_version_with_request(
        &self,
        tag: &str,
        request: RequestBuilder,
        fenced: bool,
    ) -> Result<u64> {
        let request = request.json(&serde_json::json!({ "tag": tag }));

        let (request_id, response) = if fenced {
            self.send(request, true).await?
        } else {
            self.send_unfenced(request, true).await?
        };
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
        let (request_id, response) = self.send_unfenced(request, true).await?;
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
        freshness_request: Option<FreshnessHeaders>,
    ) -> Result<TableDescription> {
        let mut body = serde_json::json!({ "version": version });
        self.apply_branch_body(&mut body);
        let request = request.json(&body);

        let (request_id, response) = if let Some(freshness_request) = freshness_request {
            self.send_with_freshness(request, true, freshness_request)
                .await?
        } else {
            self.send_unfenced(request, true).await?
        };

        let response = self.check_table_response(&request_id, response).await?;

        match response.text().await {
            Ok(body) => {
                let description: TableDescription =
                    serde_json::from_str(&body).map_err(|e| Error::Http {
                        source: format!("Failed to parse table description: {}", e).into(),
                        request_id,
                        status_code: None,
                    })?;
                if let Some(freshness_request) = freshness_request {
                    freshness_request.observe_version(&self.freshness, description.version);
                }
                Ok(description)
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

    async fn send(&self, req: RequestBuilder, with_retry: bool) -> Result<(String, Response)> {
        let freshness_request = self.snapshot_freshness_headers();
        self.send_with_freshness(req, with_retry, freshness_request)
            .await
    }

    async fn send_with_freshness(
        &self,
        req: RequestBuilder,
        with_retry: bool,
        freshness_request: FreshnessHeaders,
    ) -> Result<(String, Response)> {
        let req = freshness_request.apply(req);
        let res = if with_retry {
            self.client.send_with_retry(req, None, true).await?
        } else {
            self.client.send(req).await?
        };
        if res.1.status().is_success() {
            freshness_request.observe_headers(&self.freshness, res.1.headers());
        }
        Ok(res)
    }

    async fn send_unfenced(
        &self,
        req: RequestBuilder,
        with_retry: bool,
    ) -> Result<(String, Response)> {
        if with_retry {
            self.client.send_with_retry(req, None, true).await
        } else {
            self.client.send(req).await
        }
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

    async fn read_arrow_response(
        &self,
        request_id: &str,
        response: reqwest::Response,
    ) -> Result<SendableRecordBatchStream> {
        let response = self.check_table_response(request_id, response).await?;

        // The header has to be read before the body, which consumes the response.
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let framing = resolve_arrow_ipc_framing(content_type.as_deref(), request_id)?;

        // Buffer the whole body. File framing keeps its footer at the end, so /query
        // cannot decode incrementally. Stream framing could, via
        // arrow_ipc::reader::StreamDecoder, but fetch_blobs concatenates every batch
        // before returning, so no caller would see data sooner.
        let body = response.bytes().await.err_to_http(request_id.into())?;
        type IpcBatchIterator =
            Box<dyn Iterator<Item = std::result::Result<RecordBatch, ArrowError>> + Send>;
        let (schema, batches): (SchemaRef, IpcBatchIterator) = match framing {
            ArrowIpcFraming::Stream => {
                let reader = StreamReader::try_new(Cursor::new(body), None)?;
                (reader.schema(), Box::new(reader))
            }
            ArrowIpcFraming::File => {
                let reader = FileReader::try_new(Cursor::new(body), None)?;
                (reader.schema(), Box::new(reader))
            }
        };
        let stream = futures::stream::iter(batches).map_err(DataFusionError::from);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn apply_query_params(
        &self,
        body: &mut serde_json::Value,
        params: &QueryRequest,
    ) -> Result<()> {
        params.check_filter()?;
        body["prefilter"] = params.prefilter.into();
        // Only forward use_lsm when explicitly set; a server that predates it
        // ignores the field and routes as it would by default.
        if let Some(use_lsm) = params.use_lsm {
            body["use_lsm"] = serde_json::Value::Bool(use_lsm);
        }
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

            let requires_document_granularity_support =
                fts_query_requires_document_granularity_support(&full_text_search.query);
            if requires_document_granularity_support
                && !self.server_version.support_fts_document_granularity()
            {
                return Err(Error::NotSupported {
                    message:
                        "FTS document granularity requires remote server version 0.6.0 or later"
                            .into(),
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
                                serde_json::Number::from_f64(*v as f64)
                                    .map(serde_json::Value::Number)
                                    .ok_or_else(|| Error::InvalidInput {
                                        message: "query vector must contain only finite values"
                                            .into(),
                                    })
                            })
                            .collect::<Result<Vec<_>>>()?,
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

    async fn snapshot_read_state(&self) -> ReadSnapshot {
        let version = self.version.read().await;
        let (freshness_state, freshness) =
            freshness_state_snapshot(&self.freshness, self.client.read_consistency_interval);
        ReadSnapshot {
            version: *version,
            freshness_state,
            freshness,
        }
    }

    /// Snapshot the freshness headers to attach to a single table request.
    /// Computed at call time so that retries reuse the same snapshot.
    fn snapshot_freshness_headers(&self) -> FreshnessHeaders {
        freshness_headers_snapshot(&self.freshness, self.client.read_consistency_interval)
    }

    fn reset_freshness(&self, checkout_baseline: Option<SystemTime>, pinned: bool) {
        let mut state = self.freshness.lock().unwrap();
        let generation = state.generation.wrapping_add(1);
        *state = FreshnessState {
            generation,
            pinned,
            checkout_baseline,
            ..FreshnessState::default()
        };
    }

    /// Send an LSM operator request with the transport retry layer **off**.
    ///
    /// Retry policy on these routes belongs to the checkpoint loop, which
    /// reads the status and can tell contention from a lost claim. Leaving the
    /// transport layer on would re-ask on its own schedule first, and surface
    /// an `Error::Retry` whose status the loop would then have to unwrap.
    async fn send_lsm_route(&self, request: RequestBuilder) -> Result<(String, reqwest::Response)> {
        let (request_id, response) = self.send(request, false).await?;
        let response = self.check_table_response(&request_id, response).await?;
        Ok((request_id, response))
    }

    /// Record a version returned by a write so subsequent reads can request at
    /// least that version via `x-lancedb-min-version`. A returned `0` from a
    /// backward-compatible old server is ignored.
    fn track_write_version(&self, freshness_request: FreshnessHeaders, version: u64) {
        if version == 0 {
            return;
        }
        freshness_request.update_if_current(&self.freshness, |state| {
            state.min_version = Some(state.min_version.map_or(version, |v| v.max(version)));
        });
    }

    /// Record a committed dataset version observed in a table response so
    /// subsequent requests ask for at least this version via
    /// `x-lancedb-min-read-version`,
    /// giving monotonic reads across load-balanced query nodes. A returned `0`
    /// (or absent header from an old server) is ignored.
    fn track_read_version(&self, version: u64) {
        track_read_version(&self.freshness, version);
    }

    async fn execute_query(
        &self,
        query: &AnyQuery,
        options: &QueryExecutionOptions,
    ) -> Result<Vec<Pin<Box<dyn RecordBatchStream + Send>>>> {
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/query/", self.identifier));

        if let Some(timeout) = options.timeout {
            // Also send to server, so it can abort the query if it takes too long.
            // (If it doesn't fit into u64, it's not worth sending anyways.)
            if let Ok(timeout_ms) = u64::try_from(timeout.as_millis()) {
                request = request.header(REQUEST_TIMEOUT_HEADER, timeout_ms);
            }
        }

        let read_snapshot = self.snapshot_read_state().await;
        let query_bodies = self.prepare_query_bodies(query, read_snapshot.version)?;
        let requests: Vec<reqwest::RequestBuilder> = query_bodies
            .into_iter()
            .map(|body| request.try_clone().unwrap().json(&body))
            .collect();

        let futures = requests.into_iter().map(|req| async move {
            let (request_id, response) = self
                .send_with_freshness(req, true, read_snapshot.freshness)
                .await?;
            self.read_arrow_response(&request_id, response).await
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

    fn prepare_query_bodies(
        &self,
        query: &AnyQuery,
        version: Option<u64>,
    ) -> Result<Vec<serde_json::Value>> {
        let query = query.canonicalized()?;
        let mut base_body = serde_json::json!({ "version": version });
        self.apply_branch_body(&mut base_body);

        match &query {
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

/// How a response body frames its Arrow IPC payload. `/query` answers with file framing
/// and `fetch_blobs` with stream framing, so the reader is picked per response.
enum ArrowIpcFraming {
    File,
    Stream,
}

/// A response with no `Content-Type` uses file framing, preserving this helper's
/// behavior before `fetch_blobs` introduced stream responses.
fn resolve_arrow_ipc_framing(
    content_type: Option<&str>,
    request_id: &str,
) -> Result<ArrowIpcFraming> {
    let Some(media_type) = content_type.map(base_media_type) else {
        return Ok(ArrowIpcFraming::File);
    };
    if media_type.eq_ignore_ascii_case(ARROW_STREAM_CONTENT_TYPE) {
        return Ok(ArrowIpcFraming::Stream);
    }
    if media_type.eq_ignore_ascii_case(ARROW_FILE_CONTENT_TYPE) {
        return Ok(ArrowIpcFraming::File);
    }
    Err(Error::Http {
        source: format!(
            "Expected an Arrow IPC response with Content-Type '{ARROW_STREAM_CONTENT_TYPE}' \
            or '{ARROW_FILE_CONTENT_TYPE}', got '{media_type}'"
        )
        .into(),
        request_id: request_id.into(),
        status_code: None,
    })
}

/// An Arrow IPC stream carrying `schema` and no batches.
///
/// The body of an all-null `add_columns`: the server reads the fields to
/// add straight off the schema message, so nothing but the field list is
/// worth sending.
fn write_ipc_schema(schema: &arrow_schema::Schema) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut body, schema)?;
        writer.finish()?;
    }
    Ok(body)
}

/// Strip media-type parameters before matching against the Arrow content types.
fn base_media_type(content_type: &str) -> &str {
    match content_type.split_once(';') {
        Some((media_type, _parameters)) => media_type.trim(),
        None => content_type.trim(),
    }
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
    read_snapshot: ReadSnapshot,
    branch: Option<String>,
    freshness: Arc<Mutex<FreshnessState>>,
) -> Result<SchemaRef> {
    let mut body = serde_json::json!({ "version": read_snapshot.version });
    if let Some(branch) = &branch {
        body["branch"] = serde_json::Value::String(branch.clone());
    }
    let freshness_headers = read_snapshot.freshness;
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
    freshness_headers.observe_headers(&freshness, response.headers());
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
    freshness_headers.observe_version(&freshness, description.version);
    if !freshness_headers.is_current(&freshness) {
        return Err(Error::Runtime {
            message: SCHEMA_SELECTOR_CHANGED.to_string(),
        });
    }

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
                sql_client: None,
                version: Arc::new(RwLock::new(None)),
                location: RwLock::new(None),
                schema_cache: BackgroundCache::new(SCHEMA_CACHE_TTL, SCHEMA_CACHE_REFRESH_WINDOW),
                wal_pk_fusion: PkFusionMemory::default(),
                freshness: Arc::new(Mutex::new(FreshnessState::default())),
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
                sql_client: None,
                version: Arc::new(RwLock::new(None)),
                location: RwLock::new(None),
                schema_cache: BackgroundCache::new(SCHEMA_CACHE_TTL, SCHEMA_CACHE_REFRESH_WINDOW),
                wal_pk_fusion: PkFusionMemory::default(),
                freshness: Arc::new(Mutex::new(FreshnessState::default())),
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
                sql_client: None,
                version: Arc::new(RwLock::new(None)),
                location: RwLock::new(None),
                schema_cache: BackgroundCache::new(SCHEMA_CACHE_TTL, SCHEMA_CACHE_REFRESH_WINDOW),
                wal_pk_fusion: PkFusionMemory::default(),
                freshness: Arc::new(Mutex::new(FreshnessState::default())),
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
        let freshness_request = self.snapshot_freshness_headers();

        let mut insert: Arc<dyn ExecutionPlan> = Arc::new(
            RemoteWriteExec::new(
                self.name.clone(),
                self.identifier.clone(),
                self.client.clone(),
                output.plan,
                WriteOp::Insert {
                    overwrite: output.overwrite,
                },
                output.tracker.clone(),
                self.branch.clone(),
            )
            .with_freshness(
                self.freshness.clone(),
                self.client.read_consistency_interval,
            ),
        );

        let mut retry_counter =
            RetryCounter::new(&self.client.retry_config, uuid::Uuid::new_v4().to_string());

        loop {
            let stream = execute_plan(insert.clone(), Default::default())?;
            let result: Result<Vec<_>> = stream.try_collect().await.map_err(Error::from);

            match result {
                Ok(_) => {
                    let add_result = (insert.as_ref() as &dyn std::any::Any)
                        .downcast_ref::<RemoteWriteExec<S>>()
                        .and_then(|i| i.add_result())
                        .unwrap_or(AddResult { version: 0 });

                    if output.overwrite {
                        self.invalidate_schema_cache();
                    }
                    self.track_write_version(freshness_request, add_result.version);

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
            let freshness_request = self.snapshot_freshness_headers();
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
                        self.track_write_version(freshness_request, result.version);
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

        let insert = Arc::new(
            RemoteWriteExec::new_multipart(
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
            )
            .with_freshness(
                self.freshness.clone(),
                self.client.read_consistency_interval,
            ),
        );

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
    async fn index_stats_read_snapshot(
        &self,
        index_name: &str,
        read_snapshot: ReadSnapshot,
    ) -> Result<Option<IndexStatistics>> {
        let encoded_name = urlencoding::encode(index_name);
        let mut body = serde_json::json!({ "version": read_snapshot.version });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!(
                "/v1/table/{}/index/{encoded_name}/stats/",
                self.identifier
            ))
            .json(&body);

        let (request_id, response) = self
            .send_with_freshness(request, true, read_snapshot.freshness)
            .await?;
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
        read_snapshot: ReadSnapshot,
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
                    match self
                        .index_stats_read_snapshot(&entry.index_name, read_snapshot)
                        .await
                    {
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
        let mut indices: Vec<IndexConfig> = results.into_iter().flatten().collect();
        let lance_schema = lance_core::datatypes::Schema::try_from(schema.as_ref())?;
        for index in &mut indices {
            if index.index_type == IndexType::FTS {
                // The wire format uses physical paths for schema resolution. Match
                // native tables by exposing list-transparent paths to callers.
                for column in &mut index.columns {
                    let field_id = lance_schema.field_id(column)?;
                    *column = public_fts_field_path_by_id(&lance_schema, field_id)?;
                }
            }
        }
        Ok(indices)
    }
}

#[async_trait]
impl<S: HttpSend> BaseTable for RemoteTable<S> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn analyze_plan_is_remote(&self) -> bool {
        true
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
    async fn materialized_view_info(&self) -> Result<MaterializedViewInfo> {
        #[derive(Deserialize)]
        struct DescribeMaterializedViewResponse {
            query: String,
        }

        let request = self.client.post(&format!(
            "/v1/materialized_view/{}/describe",
            self.identifier
        ));
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let response: DescribeMaterializedViewResponse =
            response.json().await.err_to_http(request_id)?;
        let parsed_definition = MaterializedViewDefinition::from_sql(&response.query).ok();
        Ok(MaterializedViewInfo {
            definition_sql: response.query,
            parsed_definition,
        })
    }

    async fn refresh_materialized_view_async(
        &self,
        source_version: Option<u64>,
    ) -> Result<Job<RefreshMaterializedViewResult>> {
        self.check_mutable().await?;
        if source_version.is_some() {
            return Err(Error::NotSupported {
                message: "source-version pinning is available only for local materialized views"
                    .to_string(),
            });
        }
        let sql_client = self.sql_client.clone().ok_or_else(|| Error::NotSupported {
            message: "SQL is unavailable for this remote table client".to_string(),
        })?;
        let statement = format!(
            "REFRESH MATERIALIZED VIEW {}",
            quote_sql_identifier(&self.name)
        );
        let namespace = self.namespace.clone();
        let client = self.client.clone();
        let name = self.name.clone();
        let identifier = self.identifier.clone();
        let server_version = self.server_version.clone();
        let freshness = self.freshness.clone();
        let freshness_request = self.snapshot_freshness_headers();
        let job_sql_client = sql_client.clone();
        Ok(
            sql_client.submit_as_job(statement, namespace.clone(), move || async move {
                let table = Self::new_with_sql_client(
                    client,
                    name,
                    namespace,
                    identifier,
                    server_version,
                    Some(job_sql_client),
                );
                table.checkout_latest().await?;
                let version = table.version().await?;
                let rows_written =
                    u64::try_from(table.count_rows(None).await?).map_err(|_| Error::Runtime {
                        message: "materialized-view row count exceeds u64".to_string(),
                    })?;
                freshness_request.observe_version(&freshness, version);
                Ok(RefreshMaterializedViewResult {
                    mode: crate::materialized_view::RefreshMode::Rebuild,
                    rows_written,
                    source_version: 0,
                    version,
                })
            }),
        )
    }
    async fn query_snapshot(&self) -> Result<Arc<dyn BaseTable>> {
        let description = self.describe().await?;
        let TableDescription {
            version,
            schema,
            location,
        } = description;
        let schema = Arc::new(arrow_schema::Schema::try_from(schema)?);
        let snapshot = self.with_branch(self.branch.clone());
        *snapshot.version.write().await = Some(version);
        *snapshot.location.write().await = location;
        snapshot.schema_cache.seed(schema);
        Ok(Arc::new(snapshot))
    }
    async fn version(&self) -> Result<u64> {
        self.describe().await.map(|desc| desc.version)
    }

    async fn checkout_current(&self) -> Result<Arc<dyn BaseTable>> {
        let description = self.describe().await?;
        let TableDescription {
            version,
            schema,
            location,
        } = description;
        let schema = Arc::new(arrow_schema::Schema::try_from(schema)?);
        let snapshot = self.with_branch(self.branch.clone());
        *snapshot.version.write().await = Some(version);
        *snapshot.location.write().await = location;
        snapshot.schema_cache.seed(schema);
        Ok(Arc::new(snapshot))
    }

    async fn checkout(&self, version: u64) -> Result<()> {
        // Validate the version exists. The describe is sent without freshness
        // headers so a stale `min_version` from a previous write doesn't ride
        // along on an explicit time-travel request.
        let request = self
            .client
            .post(&format!("/v1/table/{}/describe/", self.identifier));
        self.describe_with_request(request, Some(version), None)
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
        // Commit the selector and its freshness mode while holding the selector
        // write lock, with no cancellation point between the two updates.
        self.reset_freshness(None, true);
        *write_guard = Some(version);
        self.invalidate_schema_cache();
        drop(write_guard);

        Ok(())
    }
    async fn checkout_latest(&self) -> Result<()> {
        let mut write_guard = self.version.write().await;
        // Drop any per-handle read/write tracking; subsequent reads use the
        // baseline timestamp captured now to guarantee freshness.
        self.reset_freshness(Some(SystemTime::now()), false);
        *write_guard = None;
        self.invalidate_schema_cache();
        drop(write_guard);

        Ok(())
    }
    async fn snapshot_at_current_version(&self) -> Result<Option<Arc<dyn BaseTable>>> {
        // A checked-out handle already names its snapshot. Otherwise resolve
        // latest exactly once before creating the independent pinned handle.
        let read_snapshot = self.snapshot_read_state().await;
        let version = match read_snapshot.version {
            Some(version) => version,
            None => self.describe_read_snapshot(read_snapshot).await?.version,
        };

        let snapshot = self.with_branch(self.branch.clone());
        *snapshot.version.write().await = Some(version);
        snapshot.reset_freshness(None, true);
        Ok(Some(Arc::new(snapshot)))
    }
    async fn restore(&self) -> Result<()> {
        let read_snapshot = self.snapshot_read_state().await;
        let version = read_snapshot.version.ok_or_else(|| Error::InvalidInput {
            message: "you must run checkout before running restore".to_string(),
        })?;
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/restore/", self.identifier));
        let mut body = serde_json::json!({ "version": version });
        self.apply_branch_body(&mut body);
        request = request.json(&body);

        let (request_id, response) = self
            .send_with_freshness(request, true, read_snapshot.freshness)
            .await?;
        self.check_table_response(&request_id, response).await?;
        self.checkout_latest().await?;
        Ok(())
    }

    async fn list_versions(&self) -> Result<Vec<Version>> {
        let request = self.apply_branch_query(
            self.client
                .post(&format!("/v1/table/{}/version/list/", self.identifier)),
        );
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;

        // Servers report the creation time either as an RFC 3339 `timestamp`
        // (direct-table path) or as `timestamp_millis` in milliseconds since
        // epoch (namespace-backed path), and may omit `metadata`.
        #[derive(Deserialize)]
        struct VersionEntry {
            version: u64,
            timestamp: Option<DateTime<Utc>>,
            timestamp_millis: Option<i64>,
            #[serde(default)]
            metadata: std::collections::BTreeMap<String, String>,
        }

        #[derive(Deserialize)]
        struct ListVersionsResponse {
            versions: Vec<VersionEntry>,
        }

        let body = response.text().await.err_to_http(request_id.clone())?;
        let body: ListVersionsResponse =
            serde_json::from_str(&body).map_err(|err| Error::Http {
                source: format!(
                    "Failed to parse list_versions response: {}, body: {}",
                    err, body
                )
                .into(),
                request_id: request_id.clone(),
                status_code: None,
            })?;

        body.versions
            .into_iter()
            .map(|entry| {
                let timestamp = entry
                    .timestamp
                    .or_else(|| {
                        entry
                            .timestamp_millis
                            .and_then(DateTime::<Utc>::from_timestamp_millis)
                    })
                    .ok_or_else(|| Error::Http {
                        source: format!(
                            "list_versions response for version {} has neither a valid \
                             `timestamp` nor `timestamp_millis` field",
                            entry.version
                        )
                        .into(),
                        request_id: request_id.clone(),
                        status_code: None,
                    })?;
                Ok(Version {
                    version: entry.version,
                    timestamp,
                    metadata: entry.metadata,
                })
            })
            .collect()
    }

    async fn schema(&self) -> Result<SchemaRef> {
        loop {
            let read_snapshot = self.snapshot_read_state().await;
            if let Some(schema) = self.schema_cache.try_get() {
                return Ok(schema);
            }

            let client = self.client.clone();
            let identifier = self.identifier.clone();
            let table_name = self.name.clone();
            let branch = self.branch.clone();
            let freshness = self.freshness.clone();

            match self
                .schema_cache
                .get(move || async move {
                    fetch_schema(
                        &client,
                        &identifier,
                        &table_name,
                        read_snapshot,
                        branch,
                        freshness,
                    )
                    .await
                })
                .await
            {
                Ok(schema) => return Ok(schema),
                Err(error)
                    if matches!(
                        &*error,
                        Error::Runtime { message } if message == SCHEMA_SELECTOR_CHANGED
                    ) => {}
                Err(error) => return Err(unwrap_shared_error(error)),
            }
        }
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
        let (request_id, response) = self.send_unfenced(request, false).await?;
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

        let request = self
            .client
            .post(&format!("/v1/table/{}/branches/list/", self.identifier));
        let (request_id, response) = self.send_unfenced(request, true).await?;
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

    async fn diff_branch(&self, from_branch: &str) -> Result<BranchDiff> {
        if from_branch.trim().is_empty() {
            return Err(Error::InvalidInput {
                message: "Branch name cannot be empty.".into(),
            });
        }
        let request = self
            .client
            .post(&format!("/v1/table/{}/branches/diff/", self.identifier))
            .json(&serde_json::json!({ "from_branch": from_branch }));
        let (request_id, response) = self.send_unfenced(request, true).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::TableNotFound {
                name: format!("{} (branch: {})", self.name, from_branch),
                source: format!("branch '{}' does not exist", from_branch).into(),
            });
        }
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        serde_json::from_str(&body).map_err(|err| Error::Http {
            source: format!(
                "Failed to parse diff_branch response: {}, body: {}",
                err, body
            )
            .into(),
            request_id,
            status_code: None,
        })
    }

    async fn cherry_pick(&self, from_branch: &str, dry_run: bool) -> Result<CherryPickResult> {
        if from_branch.trim().is_empty() {
            return Err(Error::InvalidInput {
                message: "Branch name cannot be empty.".into(),
            });
        }
        let read_snapshot = self.snapshot_read_state().await;
        let target_freshness = if self.branch.is_none() && read_snapshot.version.is_none() {
            Some(read_snapshot.freshness)
        } else {
            None
        };
        let request = self
            .client
            .post(&format!(
                "/v1/table/{}/branches/cherry_pick/",
                self.identifier
            ))
            .json(&serde_json::json!({
                "from_branch": from_branch,
                "dry_run": dry_run,
            }));
        // No retry. HTTP 409 is CherryPickStatus::Failed with a body, not a transport error.
        let (request_id, response) = self.send_unfenced(request, false).await?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Err(Error::TableNotFound {
                name: format!("{} (branch: {})", self.name, from_branch),
                source: format!("branch '{}' does not exist", from_branch).into(),
            });
        }
        // 200 and 409 both carry CherryPickResult.
        if status != StatusCode::OK && status != StatusCode::CONFLICT {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Http {
                source: format!("unexpected status {status} from cherry_pick: {body}").into(),
                request_id,
                status_code: Some(status),
            });
        }
        let body = response.text().await.err_to_http(request_id.clone())?;
        let result: CherryPickResult = serde_json::from_str(&body).map_err(|err| Error::Http {
            source: format!(
                "Failed to parse cherry_pick response: {}, body: {}",
                err, body
            )
            .into(),
            request_id,
            status_code: Some(status),
        })?;
        if !dry_run
            && status == StatusCode::OK
            && result.status == crate::table::CherryPickStatus::CherryPicked
            && let (Some(freshness), Some(version)) = (target_freshness, result.main_version_after)
        {
            freshness.observe_version(&self.freshness, version);
        }
        Ok(result)
    }

    fn current_branch(&self) -> Option<String> {
        self.branch.clone()
    }

    async fn count_rows(&self, filter: Option<Filter>) -> Result<usize> {
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/count_rows/", self.identifier));

        let read_snapshot = self.snapshot_read_state().await;

        let mut body = if let Some(filter) = filter {
            let filter_sql = match filter {
                Filter::Sql(sql) => crate::expr::canonicalize_sql_predicate(&sql)?,
                Filter::Datafusion(expr) => expr_to_sql_string(&expr)?,
            };
            serde_json::json!({ "predicate": filter_sql, "version": read_snapshot.version })
        } else {
            serde_json::json!({ "version": read_snapshot.version })
        };
        self.apply_branch_body(&mut body);
        request = request.json(&body);

        let (request_id, response) = match self
            .send_with_freshness(request, true, read_snapshot.freshness)
            .await
        {
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

        let body = response.text().await.err_to_http(request_id.clone())?;

        serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse row count: {}", e).into(),
            request_id,
            status_code: None,
        })
    }
    async fn add(&self, mut add: AddDataBuilder) -> Result<AddResult> {
        self.check_mutable().await?;

        if add.allow_external_blob_outside_bases {
            return Err(Error::NotSupported {
                message: "allow_external_blob_outside_bases is only supported on local tables"
                    .to_string(),
            });
        }
        // String blob values still coerce to the uri child in into_plan.
        // Remote and local share that input shape.

        let table_schema = self.schema().await?;
        crate::table::computed_columns::ensure_supported_function_metadata(table_schema.as_ref())?;
        let table_def = TableDefinition::try_from_rich_schema(table_schema.clone())?;

        let num_partitions = if self.server_version.support_multipart_write() {
            // Peek at the first batch to estimate write partitions (same as
            // NativeTable) and, regardless of `write_parallelism`, to detect a
            // fully empty input. A multipart write creates its upload session
            // before any partition executes; if the input turns out to have no
            // batches at all, no partition ever stages a part (see
            // `send_multipart_chunked`), so completing the write has nothing to
            // commit and e.g. `mode=overwrite` would be silently dropped. Route
            // empty input through the single-request path instead, which always
            // sends one schema-only request.
            let mut peeked = PeekedScannable::new(add.data);
            let n = match peeked.peek().await {
                Some(first_batch) => match add.write_parallelism {
                    Some(parallelism) if parallelism > 1 => parallelism,
                    Some(_) => 1,
                    None => {
                        let max_partitions =
                            lance_core::utils::tokio::get_num_compute_intensive_cpus();
                        estimate_write_partitions(
                            first_batch.get_array_memory_size(),
                            first_batch.num_rows(),
                            peeked.num_rows(),
                            max_partitions,
                        )
                    }
                },
                None => 1,
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
        if let AnyQuery::Query(request) = query
            && let Some(offsets) = &request.take_offsets
        {
            return crate::query::create_take_offsets_plan(self, request, offsets, options, false)
                .await;
        }

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
        if let AnyQuery::Query(request) = query
            && let Some(offsets) = &request.take_offsets
        {
            let plan = crate::query::create_take_offsets_plan(
                self,
                request,
                offsets,
                options.clone(),
                false,
            )
            .await?;
            let inner = execute_plan(plan, Default::default())?;
            let inner = MaxBatchLengthStream::new_boxed(inner, options.max_batch_length as usize);
            let inner = if let Some(timeout) = options.timeout {
                TimeoutStream::new_boxed(inner, timeout)
            } else {
                inner
            };
            return Ok(DatasetRecordBatchStream::new(inner));
        }

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

    async fn blob_columns(&self) -> Result<Vec<String>> {
        self.blob_columns_impl().await
    }

    async fn fetch_blobs(&self, column: &str, row_ids: &[u64]) -> Result<LargeBinaryArray> {
        self.fetch_blobs_impl(column, row_ids).await
    }

    async fn fetch_blob_files(
        &self,
        column: &str,
        row_ids: &[u64],
    ) -> Result<Vec<Option<BlobFile>>> {
        self.fetch_blob_files_impl(column, row_ids).await
    }

    async fn explain_plan(&self, query: &AnyQuery, verbose: bool) -> Result<String> {
        if let AnyQuery::Query(request) = query
            && let Some(offsets) = &request.take_offsets
        {
            return crate::query::explain_take_offsets_plan(self, request, offsets, verbose).await;
        }

        let base_request = self
            .client
            .post(&format!("/v1/table/{}/explain_plan/", self.identifier));

        let read_snapshot = self.snapshot_read_state().await;
        let query_bodies = self.prepare_query_bodies(query, read_snapshot.version)?;
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
            let (request_id, response) = self
                .send_with_freshness(req, true, read_snapshot.freshness)
                .await?;
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
        let prepared_query = if let AnyQuery::Query(request) = query
            && request.take_offsets.is_some()
        {
            Some(AnyQuery::Query(
                crate::query::prepare_take_offsets_request(self, request).await?,
            ))
        } else {
            None
        };
        let query = prepared_query.as_ref().unwrap_or(query);

        let mut request = self
            .client
            .post(&format!("/v1/table/{}/analyze_plan/", self.identifier));

        if options.analyze_plan_distributed_metrics != AnalyzePlanDistributedMetrics::Aggregate {
            request = request.query(&[(
                "distributed_metrics",
                options.analyze_plan_distributed_metrics.as_query_param(),
            )]);
        }

        let read_snapshot = self.snapshot_read_state().await;
        let query_bodies = self.prepare_query_bodies(query, read_snapshot.version)?;
        let requests: Vec<reqwest::RequestBuilder> = query_bodies
            .into_iter()
            .map(|body| request.try_clone().unwrap().json(&body))
            .collect();

        let futures = requests.into_iter().map(|req| async move {
            let (request_id, response) = self
                .send_with_freshness(req, true, read_snapshot.freshness)
                .await?;
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

    async fn update(&self, mut update: UpdateBuilder) -> Result<UpdateResult> {
        update.canonicalize_filter()?;
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

        let freshness_request = self.snapshot_freshness_headers();
        let (request_id, response) = self
            .send_with_freshness(request, true, freshness_request)
            .await?;
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

        self.track_write_version(freshness_request, update_response.version);
        Ok(update_response)
    }

    async fn delete(&self, predicate: Predicate<'_>) -> Result<DeleteResult> {
        self.check_mutable().await?;
        let predicate_sql = match predicate {
            Predicate::String(s) => crate::expr::canonicalize_sql_predicate(s)?,
            Predicate::Expr(expr) => expr_to_sql_string(expr)?,
        };
        let mut body = serde_json::json!({ "predicate": predicate_sql });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!("/v1/table/{}/delete/", self.identifier))
            .json(&body);
        let freshness_request = self.snapshot_freshness_headers();
        let (request_id, response) = self
            .send_with_freshness(request, true, freshness_request)
            .await?;
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
        self.track_write_version(freshness_request, delete_response.version);
        Ok(delete_response)
    }

    async fn create_index(&self, index: IndexBuilder) -> Result<()> {
        self.submit_create_index(index).await.map(|_| ())
    }

    async fn create_index_async(&self, index: IndexBuilder) -> Result<Job> {
        Ok(match self.submit_create_index(index).await? {
            Some(job_id) => Job::new(Box::new(FreshnessJob {
                inner: RemoteJob::new(self.client.clone(), job_id),
                freshness: self.freshness.clone(),
                version: self.version.clone(),
                tracked_result: TrackedJobResult::None,
                freshness_request: self.snapshot_freshness_headers(),
            })),
            None => Job::new_done(),
        })
    }

    /// Poll until the columns are fully indexed. Will return Error::Timeout if the columns
    /// are not fully indexed within the timeout.
    async fn wait_for_index(&self, index_names: &[&str], timeout: Duration) -> Result<()> {
        wait_for_index(self, index_names, timeout).await
    }

    async fn merge_insert(
        &self,
        mut params: MergeInsertBuilder,
        new_data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<MergeResult> {
        params.canonicalize_filters()?;
        self.check_mutable().await?;

        let timeout = params.timeout;
        let query = MergeInsertRequest::try_from(params)?;

        // Drive merge_insert through the same RemoteWriteExec streaming path as
        // add(). This routes the request body through the error side-channel so
        // an input stream error (e.g. NaN rejection) surfaces with its original
        // message instead of the masked HTTP error Hyper produces when a request
        // body stream fails under HTTP2 (issue #2339). The branch, request
        // timeout header, and merge query params are all applied inside the exec.
        //
        // The public merge_insert API only accepts a `RecordBatchReader`, which
        // is not rescannable and so could not be retried directly. To preserve
        // the previous retry-on-retryable-status behaviour, buffer the reader
        // into memory first: a `Vec<RecordBatch>` is rescannable, so the outer
        // loop can re-execute the plan (and re-stream the body) on each retry.
        // This mirrors the old `send_streaming(with_retry=true)` path, which
        // likewise buffered the reader to support retries.
        let schema = RecordBatchReader::schema(new_data.as_ref());
        let mut batches = new_data.collect::<std::result::Result<Vec<_>, _>>()?;
        // An empty reader still carries a schema. Keep it in an empty batch so
        // the buffered source remains scannable and can be replayed on retries.
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(schema));
        }
        let source: Box<dyn Scannable> = Box::new(batches);
        let rescannable = source.rescannable();
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(crate::table::datafusion::scannable_exec::ScannableExec::new(source, None));
        let freshness_request = self.snapshot_freshness_headers();

        let mut merge: Arc<dyn ExecutionPlan> = Arc::new(
            RemoteWriteExec::new(
                self.name.clone(),
                self.identifier.clone(),
                self.client.clone(),
                input,
                WriteOp::MergeInsert { query, timeout },
                None,
                self.branch.clone(),
            )
            .with_freshness(
                self.freshness.clone(),
                self.client.read_consistency_interval,
            ),
        );

        let mut retry_counter = crate::remote::retry::RetryCounter::new(
            &self.client.retry_config,
            uuid::Uuid::new_v4().to_string(),
        );

        loop {
            let stream = execute_plan(merge.clone(), Default::default())?;
            let result: Result<Vec<_>> = stream.try_collect().await.map_err(Error::from);

            match result {
                Ok(_) => {
                    let merge_result = (merge.as_ref() as &dyn std::any::Any)
                        .downcast_ref::<RemoteWriteExec<S>>()
                        .and_then(|m| m.merge_result())
                        .unwrap_or_default();

                    self.track_write_version(freshness_request, merge_result.version);
                    return Ok(merge_result);
                }
                Err(err) if rescannable && self.is_retryable_write_error(&err) => {
                    retry_counter.increment_from_error(err)?;
                    tokio::time::sleep(retry_counter.next_sleep_time()).await;
                    merge = merge.reset_state()?;
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn set_unenforced_primary_key(&self, _columns: &[&str]) -> Result<()> {
        Err(Error::NotSupported {
            message: "set_unenforced_primary_key is not supported on LanceDB cloud.".into(),
        })
    }

    async fn flush_lsm(&self) -> Result<()> {
        let request = self
            .client
            .post(&format!("/v1/table/{}/flush_lsm/", self.identifier));
        self.send_lsm_route(request).await?;
        Ok(())
    }

    async fn compact_lsm(&self) -> Result<()> {
        let request = self
            .client
            .post(&format!("/v1/table/{}/compact_lsm/", self.identifier));
        self.send_lsm_route(request).await?;
        Ok(())
    }

    async fn get_lsm_stats(&self, include_generation_rows: bool) -> Result<Option<LsmStats>> {
        // Read-semantics POST, like `get_lsm_write_spec`.
        let request = self
            .client
            .post(&format!("/v1/table/{}/get_lsm_stats/", self.identifier))
            .json(&serde_json::json!({
                "include_generation_rows": include_generation_rows,
            }));
        let (request_id, response) = self.send_lsm_route(request).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let parsed: GetLsmStatsResponse = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse get_lsm_stats response: {e}").into(),
            request_id,
            status_code: None,
        })?;
        // `null` — and only — when the table has no LSM write path.
        Ok(parsed.lsm_stats)
    }

    async fn set_lsm_write_spec(&self, spec: LsmWriteSpec) -> Result<()> {
        self.check_mutable().await?;

        // Map the spec onto the server's request DTO. `sharding` is internally
        // tagged on `mode` to mirror sophon's `Sharding` enum. A null
        // `maintained_indexes` asks the server to resolve every maintainable
        // index at HEAD; a list is verbatim, an empty one meaning none.
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
        self.wal_pk_fusion.forget(); // WAL-PK-FUSION: delete.
        Ok(())
    }

    async fn get_lsm_write_spec(&self) -> Result<Option<LsmWriteSpec>> {
        // Read counterpart to set/unset, resolved server-side against HEAD. The
        // server reads the spec from the `__lance_mem_wal` system index (shard
        // column mapped from its Lance field id against the current schema) and
        // re-encodes it into the same sophon-owned shape the set endpoint
        // accepts — no lance/lancedb types cross the wire. `lsm_write_spec` is
        // null when the LSM write path is not enabled for the table.
        let request = self.client.post(&format!(
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
            /// `null` selects every index the table has; `[]` selects none.
            #[serde(default)]
            maintained_indexes: Option<Vec<String>>,
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

    // WAL-PK-FUSION: delete both.
    fn hybrid_pk_fusion_learned(&self) -> bool {
        self.wal_pk_fusion.learned()
    }

    fn note_hybrid_pk_fusion(&self) {
        self.wal_pk_fusion.note();
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
        let version = self
            .resolve_tag_version_with_request(tag, request, false)
            .await?;

        let mut write_guard = self.version.write().await;
        // Commit the selector and its freshness mode while holding the selector
        // write lock, with no cancellation point between the two updates.
        self.reset_freshness(None, true);
        *write_guard = Some(version);
        self.invalidate_schema_cache();
        drop(write_guard);

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
        crate::table::computed_columns::ensure_not_function_bound(
            self.schema().await?.as_ref(),
            "schema evolution",
            crate::table::schema_evolution::new_column_names(&transforms),
        )?;
        let path = format!("/v1/table/{}/add_columns/", self.identifier);
        let request = match transforms {
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
                self.client.post(&path).json(&body)
            }
            // Every field becomes a column that reads null for existing
            // rows. The schema goes over the wire as Arrow IPC rather than
            // a JSON description of the types: that is what the server
            // takes, and it is the only encoding that round-trips a field
            // whole, including decimal precision and a timestamp's unit and
            // timezone. With no JSON envelope the branch rides the query
            // string, as it does for the other binary-bodied endpoints.
            NewColumnTransform::AllNulls(schema) => {
                let body = write_ipc_schema(schema.as_ref())?;
                self.apply_branch_query(
                    self.client
                        .post(&path)
                        .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
                        .body(body),
                )
            }
            _ => {
                return Err(Error::NotSupported {
                    message: "Only SQL expressions and all-null column schemas are supported \
                              for adding columns"
                        .into(),
                });
            }
        };

        let freshness_request = self.snapshot_freshness_headers();
        let (request_id, response) = self
            .send_with_freshness(request, true, freshness_request)
            .await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(AddColumnsResult { version: 0 });
        }

        let result: AddColumnsResult = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse add_columns response: {}", e).into(),
            request_id,
            status_code: None,
        })?;

        self.invalidate_schema_cache();
        self.track_write_version(freshness_request, result.version);

        Ok(result)
    }

    async fn add_computed_columns(&self, columns: &[(String, String)]) -> Result<AddColumnsResult> {
        self.check_mutable().await?;
        crate::table::computed_columns::ensure_not_function_bound(
            self.schema().await?.as_ref(),
            "schema evolution",
            columns.iter().map(|(name, _)| name),
        )?;
        // The server plans the declaration against its table schema, including
        // Blob v2 semantics inherited by a direct field projection.
        let entries = columns
            .iter()
            .map(
                |(name, expression)| lance_namespace::models::AddColumnsEntry {
                    name: name.clone(),
                    computed: Some(Some(expression.clone())),
                    ..Default::default()
                },
            )
            .collect::<Vec<_>>();
        let mut body = serde_json::json!({ "new_columns": entries });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!("/v1/table/{}/add_columns/", self.identifier))
            .json(&body);
        let freshness_request = self.snapshot_freshness_headers();
        let (request_id, response) = self
            .send_with_freshness(request, true, freshness_request)
            .await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(AddColumnsResult { version: 0 });
        }

        let result: AddColumnsResult = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse add_columns response: {}", e).into(),
            request_id,
            status_code: None,
        })?;

        self.invalidate_schema_cache();
        self.track_write_version(freshness_request, result.version);

        Ok(result)
    }

    async fn add_function_columns(
        &self,
        application: &crate::function::FunctionApplication,
        output_name: Option<&str>,
    ) -> Result<AddColumnsResult> {
        self.check_mutable().await?;
        let schema = self.schema().await?;
        let plan = crate::table::computed_columns::plan_function_application(
            schema.as_ref(),
            application,
            output_name,
        )?;
        let new_columns = plan
            .outputs
            .iter()
            .map(|output| {
                serde_json::json!({
                    "name": output.output_name,
                    "all_null": true,
                })
            })
            .collect::<Vec<_>>();
        let mut body = serde_json::json!({
            "new_columns": new_columns,
            "function": {
                "application": plan.application,
                "binding_metadata_version": plan.binding_metadata_version,
                "input_bindings": plan.input_bindings,
                "input_schema": plan.input_schema,
                "output_schema": plan.output_schema,
                "outputs": plan.outputs,
            },
        });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!("/v1/table/{}/add_columns/", self.identifier))
            .json(&body);
        let freshness_request = self.snapshot_freshness_headers();
        let (request_id, response) = self
            .send_with_freshness(request, true, freshness_request)
            .await?;
        // A Function declaration can return 404 for the Function rather than the table.
        let response = self
            .client
            .check_response(&request_id, response)
            .await
            .inspect_err(|error| self.handle_error_invalidation(error))?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            return Ok(AddColumnsResult { version: 0 });
        }

        let result: AddColumnsResult = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse add Function columns response: {e}").into(),
            request_id,
            status_code: None,
        })?;

        self.invalidate_schema_cache();
        self.track_write_version(freshness_request, result.version);
        Ok(result)
    }

    async fn refresh_column(&self, _column: &str) -> Result<RefreshColumnResult> {
        // The server runs a refresh as a job and does not report a fill
        // count, so the blocking form has no honest result to return.
        Err(Error::NotSupported {
            message: "a remote refresh runs as a server job; use refresh_column_async and \
                      wait on the returned handle"
                .into(),
        })
    }

    async fn refresh_column_async(
        &self,
        column: &str,
    ) -> Result<Job<crate::function::RefreshColumnResult>> {
        self.check_mutable().await?;
        let mut body = serde_json::json!({ "column": column });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!("/v1/table/{}/backfill_column", self.identifier))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        // Preserve dependency errors: a deleted bound Function also returns 404.
        let response = self
            .client
            .check_response(&request_id, response)
            .await
            .inspect_err(|error| self.handle_error_invalidation(error))?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        #[derive(serde::Deserialize)]
        struct BackfillResponse {
            job_id: String,
        }
        let response: BackfillResponse = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse backfill_column response: {}", e).into(),
            request_id,
            status_code: None,
        })?;

        Ok(Job::new_typed(Box::new(FreshnessJob {
            inner: RemoteJob::new(self.client.clone(), response.job_id),
            freshness: self.freshness.clone(),
            version: self.version.clone(),
            tracked_result: TrackedJobResult::RefreshColumn,
            freshness_request: self.snapshot_freshness_headers(),
        })))
    }

    async fn function_errors(
        &self,
        request: &crate::function::FunctionErrorsRequest,
    ) -> Result<crate::function::FunctionErrors> {
        let mut body = serde_json::json!({});
        if let Some(job_id) = &request.job_id {
            body["job_id"] = serde_json::json!(job_id);
        }
        if let Some(column) = &request.column {
            body["column"] = serde_json::json!(column);
        }
        if let Some(limit) = request.limit {
            body["limit"] = serde_json::json!(limit);
        }
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!("/v1/table/{}/errors", self.identifier))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse errors response: {}", e).into(),
            request_id,
            status_code: None,
        })
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
        let freshness_request = self.snapshot_freshness_headers();
        let (request_id, response) = self
            .send_with_freshness(request, true, freshness_request)
            .await?;
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
        self.track_write_version(freshness_request, result.version);

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
        let freshness_request = self.snapshot_freshness_headers();
        let (request_id, response) = self
            .send_with_freshness(request, true, freshness_request)
            .await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        let result: UpdateFieldMetadataResult =
            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to parse update_field_metadata response: {}", e).into(),
                request_id,
                status_code: None,
            })?;

        self.invalidate_schema_cache();
        self.track_write_version(freshness_request, result.version);
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
        let freshness_request = self.snapshot_freshness_headers();
        let (request_id, response) = self
            .send_with_freshness(request, true, freshness_request)
            .await?;
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
        self.track_write_version(freshness_request, result.version);

        Ok(result)
    }

    async fn list_indices(&self) -> Result<Vec<IndexConfig>> {
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/index/list/", self.identifier));
        let read_snapshot = self.snapshot_read_state().await;
        let mut body = serde_json::json!({ "version": read_snapshot.version });
        self.apply_branch_body(&mut body);
        request = request.json(&body);

        let (request_id, response) = self
            .send_with_freshness(request, true, read_snapshot.freshness)
            .await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let schema = self.schema_read_snapshot(read_snapshot).await?;

        self.parse_index_list_response(&body, &request_id, &schema, read_snapshot)
            .await
    }

    async fn index_stats(&self, index_name: &str) -> Result<Option<IndexStatistics>> {
        self.index_stats_read_snapshot(index_name, self.snapshot_read_state().await)
            .await
    }

    async fn drop_index(&self, index_name: &str) -> Result<()> {
        let encoded_name = urlencoding::encode(index_name);
        let request = self.apply_branch_query(self.client.post(&format!(
            "/v1/table/{}/index/{encoded_name}/drop/",
            self.identifier
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
        let encoded_name = urlencoding::encode(index_name);
        let request = self.client.post(&format!(
            "/v1/table/{}/index/{encoded_name}/prewarm/",
            self.identifier
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
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/stats/", self.identifier));
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
        Ok(Arc::new(
            insert::RemoteWriteExec::new(
                self.name.clone(),
                self.identifier.clone(),
                self.client.clone(),
                input,
                WriteOp::Insert { overwrite },
                None,
                self.branch.clone(),
            )
            .with_freshness(
                self.freshness.clone(),
                self.client.read_consistency_interval,
            ),
        ))
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct MergeInsertRequest {
    // Sent as one repeated `on` query parameter per column, which is how the
    // namespace spec encodes an array-valued `on`. serde_urlencoded (which
    // reqwest's `query()` uses) cannot serialize a sequence nested in a struct,
    // so this field is emitted separately by [`Self::on_query_params`].
    #[serde(skip_serializing)]
    on: Vec<String>,
    when_matched_update_all: bool,
    when_matched_update_all_filt: Option<String>,
    when_not_matched_insert_all: bool,
    when_not_matched_by_source_delete: bool,
    when_not_matched_by_source_delete_filt: Option<String>,
    // For backwards compatibility, only serialize use_index when it's false
    // (the default is true)
    #[serde(skip_serializing_if = "is_true")]
    use_index: bool,
    // Only serialize use_lsm when explicitly set (Some); a server that predates
    // it ignores the field and routes as it would by default.
    #[serde(skip_serializing_if = "Option::is_none")]
    use_lsm: Option<bool>,
}

impl MergeInsertRequest {
    /// The `on` columns as repeated query parameters: `?on=a&on=b`.
    ///
    /// A single column serializes to `?on=a`, exactly what clients sent before
    /// `on` became a list, so a server that predates composite keys sees no
    /// change from a single-column caller.
    pub(crate) fn on_query_params(&self) -> Vec<(&str, &str)> {
        self.on.iter().map(|col| ("on", col.as_str())).collect()
    }
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
        }
        // The server rejects a repeated column with a 400; catching it here
        // names the offending column and costs no round trip.
        let mut seen = HashSet::with_capacity(value.on.len());
        if let Some(dup) = value.on.iter().find(|col| !seen.insert(*col)) {
            return Err(Error::InvalidInput {
                message: format!("MergeInsertBuilder 'on' column '{dup}' is repeated"),
            });
        }

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
            on: value.on,
            when_matched_update_all: value.when_matched_update_all,
            when_matched_update_all_filt,
            when_not_matched_insert_all: value.when_not_matched_insert_all,
            when_not_matched_by_source_delete: value.when_not_matched_by_source_delete,
            when_not_matched_by_source_delete_filt,
            // Only serialize use_index when it's false for backwards compatibility
            use_index: value.use_index,
            use_lsm: value.use_lsm,
        })
    }
}

#[cfg(test)]
mod tests;
