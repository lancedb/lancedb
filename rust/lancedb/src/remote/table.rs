// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

pub mod blobs;
mod branches;
mod columns;
mod describe;
mod freshness;
mod indices;
pub mod insert;
mod lsm;
mod merge;
mod requests;
mod tags;
mod write;

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

use describe::*;
use freshness::*;
pub use merge::*;
pub use tags::*;

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

fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Normalize a branch selector: trim whitespace and treat `""` or `"main"` as
/// the (absent) main branch, matching the server's convention.
fn normalize_branch(branch: Option<String>) -> Option<String> {
    branch
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && value != "main")
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
        self.create_branch_impl(name, from).await
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
        self.list_branches_impl().await
    }

    async fn delete_branch(&self, name: &str) -> Result<()> {
        self.delete_branch_impl(name).await
    }

    async fn diff_branch(&self, from_branch: &str) -> Result<BranchDiff> {
        self.diff_branch_impl(from_branch).await
    }

    async fn cherry_pick(&self, from_branch: &str, dry_run: bool) -> Result<CherryPickResult> {
        self.cherry_pick_impl(from_branch, dry_run).await
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
        self.get_lsm_stats_impl(include_generation_rows).await
    }

    async fn set_lsm_write_spec(&self, spec: LsmWriteSpec) -> Result<()> {
        self.set_lsm_write_spec_impl(spec).await
    }

    async fn unset_lsm_write_spec(&self) -> Result<()> {
        self.unset_lsm_write_spec_impl().await
    }

    async fn get_lsm_write_spec(&self) -> Result<Option<LsmWriteSpec>> {
        self.get_lsm_write_spec_impl().await
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
        self.add_columns_impl(transforms, _read_columns).await
    }

    async fn add_computed_columns(&self, columns: &[(String, String)]) -> Result<AddColumnsResult> {
        self.add_computed_columns_impl(columns).await
    }

    async fn add_function_columns(
        &self,
        application: &crate::function::FunctionApplication,
        output_name: Option<&str>,
    ) -> Result<AddColumnsResult> {
        self.add_function_columns_impl(application, output_name)
            .await
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
        self.refresh_column_async_impl(column).await
    }

    async fn function_errors(
        &self,
        request: &crate::function::FunctionErrorsRequest,
    ) -> Result<crate::function::FunctionErrors> {
        self.function_errors_impl(request).await
    }

    async fn alter_columns(&self, alterations: &[ColumnAlteration]) -> Result<AlterColumnsResult> {
        self.alter_columns_impl(alterations).await
    }

    async fn update_field_metadata(
        &self,
        updates: &[FieldMetadataUpdate],
    ) -> Result<UpdateFieldMetadataResult> {
        self.update_field_metadata_impl(updates).await
    }

    async fn drop_columns(&self, columns: &[&str]) -> Result<DropColumnsResult> {
        self.drop_columns_impl(columns).await
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

#[cfg(test)]
mod tests;
