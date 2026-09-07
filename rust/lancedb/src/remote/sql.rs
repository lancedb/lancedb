// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use arrow_flight::{
    Action, CancelFlightInfoRequest, CancelFlightInfoResult, CancelStatus, FlightClient,
    FlightDescriptor, FlightEndpoint, FlightInfo, PollInfo,
};
use arrow_schema::{Schema, SchemaRef};
use futures::TryStreamExt;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use prost::Message;
use tokio::sync::{Mutex, Notify, OnceCell, mpsc};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use uuid::Uuid;

use crate::arrow::{SendableRecordBatchStream, SimpleRecordBatchStream};
use crate::error::{Error, Result};
use crate::remote::client::{ClientConfig, TlsConfig};
use crate::remote::retry::ResolvedRetryConfig;
use crate::sql::{Query, QueryDescription, QueryHandle, QueryStatus};

const DEFAULT_SQL_PORT: u16 = 10025;
const DEFAULT_SQL_TLS_PORT: u16 = 10026;
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(300);
const STATUS_POLL_TIMEOUT: Duration = Duration::from_secs(1);
const MIN_POLL_INTERVAL: Duration = Duration::from_millis(50);
const MAX_SQL_MESSAGE_SIZE: usize = 1024 * 1024 * 1024;
const TERMINAL_QUERY_RETENTION: Duration = Duration::from_secs(300);
const ABANDONED_QUERY_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone)]
pub(super) struct SqlClient {
    inner: Arc<SqlClientInner>,
    queries: Arc<QueryRegistry>,
}

struct SqlClientInner {
    database: String,
    database_prefix: Option<String>,
    api_key: String,
    host_override: Option<String>,
    sql_host_override: Option<String>,
    client_config: ClientConfig,
    client: Arc<OnceCell<SqlConnection>>,
}

struct SqlConnection {
    // FlightClient does not expose its transport. Cancellation retains the channel so it can
    // install a per-call interceptor that records whether a request was dispatched.
    channel: Channel,
    client: FlightServiceClient<Channel>,
}

struct ResultEndpointStream {
    stream: FlightRecordBatchStream,
    request_id: String,
    read_timeout: Duration,
}

struct PreparedSqlResult {
    schema: SchemaRef,
    next_endpoint: usize,
    endpoint_stream: Option<ResultEndpointStream>,
    buffered_batch: Option<RecordBatch>,
}

impl ResultEndpointStream {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        tokio::time::timeout(self.read_timeout, self.stream.try_next())
            .await
            .map_err(|_| sql_error(&self.request_id, "SQL result read timed out"))?
            .map_err(|err| sql_error(&self.request_id, err))
    }
}

enum CancelOutcome {
    Status(CancelStatus),
    NotFound(String),
}

struct CancelAttempt {
    dispatched: Arc<AtomicBool>,
    unresolved: Arc<AtomicBool>,
    resolved: bool,
}

struct ResultStartGuard<'a> {
    started: &'a AtomicBool,
    committed: bool,
}

impl<'a> ResultStartGuard<'a> {
    fn new(started: &'a AtomicBool) -> Self {
        Self {
            started,
            committed: false,
        }
    }

    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for ResultStartGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.started.store(false, Ordering::SeqCst);
        }
    }
}

impl CancelAttempt {
    fn new(dispatched: Arc<AtomicBool>, unresolved: Arc<AtomicBool>) -> Self {
        Self {
            dispatched,
            unresolved,
            resolved: false,
        }
    }

    fn resolve(&mut self) {
        self.resolved = true;
    }
}

impl Drop for CancelAttempt {
    fn drop(&mut self) {
        if !self.resolved && self.dispatched.load(Ordering::SeqCst) {
            self.unresolved.store(true, Ordering::SeqCst);
        }
    }
}

impl std::fmt::Debug for SqlClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqlClient")
            .field("database", &self.inner.database)
            .field("database_prefix", &self.inner.database_prefix)
            .field("api_key", &"<redacted>")
            .field("host_override", &self.inner.host_override)
            .field("sql_host_override", &self.inner.sql_host_override)
            .field("client_config", &"<redacted>")
            .field("initialized", &self.inner.client.get().is_some())
            .finish()
    }
}

impl SqlClient {
    pub(super) fn new(
        database: String,
        database_prefix: Option<String>,
        api_key: String,
        host_override: Option<String>,
        sql_host_override: Option<String>,
        client_config: ClientConfig,
    ) -> Self {
        Self {
            inner: Arc::new(SqlClientInner {
                database,
                database_prefix,
                api_key,
                host_override,
                sql_host_override,
                client_config,
                client: Arc::new(OnceCell::new()),
            }),
            queries: Arc::new(QueryRegistry::new()),
        }
    }

    pub(super) async fn submit(
        &self,
        query: &str,
        default_namespace_path: &[String],
    ) -> Result<Query> {
        let timeout = self.inner.overall_timeout()?;
        with_overall_timeout(timeout, "SQL query submission", async {
            validate_namespace_path(default_namespace_path)?;
            let command = CommandStatementQuery {
                query: query.to_string(),
                transaction_id: None,
            };
            let descriptor = FlightDescriptor::new_cmd(command.as_any().encode_to_vec());
            let poll_info = self.inner.poll(descriptor, default_namespace_path).await?;
            let query_id = Uuid::now_v7();
            let query = Arc::new(RemoteQuery::new(
                query_id,
                self.inner.clone(),
                default_namespace_path.to_vec(),
                poll_info,
            )?);
            self.queries.insert(query_id, query.clone());
            Ok(Query::new(Arc::new(RemoteQueryHandle::new(query))))
        })
        .await
    }

    pub(super) async fn describe(&self, query_id: Uuid) -> Result<QueryDescription> {
        let query = self
            .queries
            .get(query_id)
            .ok_or_else(|| Error::InvalidInput {
                message: "Unknown or expired SQL query id for this connection".to_string(),
            })?;
        query.describe().await
    }

    #[cfg(test)]
    async fn initialized_client_count(&self) -> usize {
        usize::from(self.inner.client.get().is_some())
    }
}

impl SqlClientInner {
    fn overall_timeout(&self) -> Result<Option<Duration>> {
        resolve_timeout(
            self.client_config.timeout_config.timeout,
            "LANCE_CLIENT_TIMEOUT",
            None,
        )
    }

    async fn poll(
        &self,
        descriptor: FlightDescriptor,
        default_namespace_path: &[String],
    ) -> Result<PollInfo> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let read_timeout = resolve_timeout(
            self.client_config.timeout_config.read_timeout,
            "LANCE_CLIENT_READ_TIMEOUT",
            Some(DEFAULT_READ_TIMEOUT),
        )?
        .unwrap();
        let mut client = self
            .client_with_headers(default_namespace_path, &request_id)
            .await?;
        tokio::time::timeout(read_timeout, client.poll_flight_info(descriptor))
            .await
            .map_err(|_| sql_error(&request_id, "SQL query poll timed out"))?
            .map_err(|err| sql_error(&request_id, err))
    }

    async fn poll_status(
        &self,
        descriptor: FlightDescriptor,
        default_namespace_path: &[String],
    ) -> Result<Option<PollInfo>> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let mut client = self
            .client_with_headers(default_namespace_path, &request_id)
            .await
            .map_err(|err| sql_error(&request_id, err))?;
        match tokio::time::timeout(STATUS_POLL_TIMEOUT, client.poll_flight_info(descriptor)).await {
            Ok(result) => result.map(Some).map_err(|err| sql_error(&request_id, err)),
            Err(_) => Ok(None),
        }
    }

    async fn poll_continuation(
        &self,
        descriptor: FlightDescriptor,
        default_namespace_path: &[String],
    ) -> Result<PollInfo> {
        let read_timeout = resolve_timeout(
            self.client_config.timeout_config.read_timeout,
            "LANCE_CLIENT_READ_TIMEOUT",
            Some(DEFAULT_READ_TIMEOUT),
        )?
        .unwrap();
        let retry_config = ResolvedRetryConfig::try_from(self.client_config.retry_config.clone())?;
        let mut retry_count = 0_u8;
        loop {
            let started = Instant::now();
            let request_id = uuid::Uuid::new_v4().to_string();
            let mut client = self
                .client_with_headers(default_namespace_path, &request_id)
                .await?;
            let result =
                tokio::time::timeout(read_timeout, client.poll_flight_info(descriptor.clone()))
                    .await;
            let poll_info = match result {
                Err(_) if retry_count < retry_config.read_retries => {
                    retry_count += 1;
                    tokio::time::sleep(poll_retry_delay(&retry_config, retry_count)).await;
                    continue;
                }
                Err(_) => return Err(sql_error(&request_id, "SQL query poll timed out")),
                Ok(Err(FlightError::Tonic(status)))
                    if matches!(
                        status.code(),
                        tonic::Code::DeadlineExceeded | tonic::Code::Unavailable
                    ) && retry_count < retry_config.read_retries =>
                {
                    retry_count += 1;
                    tokio::time::sleep(poll_retry_delay(&retry_config, retry_count)).await;
                    continue;
                }
                Ok(Err(error)) => return Err(sql_error(&request_id, error)),
                Ok(Ok(poll_info)) => poll_info,
            };
            if let Some(delay) = MIN_POLL_INTERVAL.checked_sub(started.elapsed()) {
                tokio::time::sleep(delay).await;
            }
            return Ok(poll_info);
        }
    }

    async fn open_result_endpoint(
        &self,
        endpoint: FlightEndpoint,
        default_namespace_path: &[String],
    ) -> Result<ResultEndpointStream> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let read_timeout = resolve_timeout(
            self.client_config.timeout_config.read_timeout,
            "LANCE_CLIENT_READ_TIMEOUT",
            Some(DEFAULT_READ_TIMEOUT),
        )?
        .unwrap();
        let ticket = endpoint.ticket.ok_or_else(|| {
            sql_error(&request_id, "SQL result endpoint did not include a ticket")
        })?;
        let mut endpoint_client = self
            .client_with_headers(default_namespace_path, &request_id)
            .await?;
        let stream = tokio::time::timeout(read_timeout, endpoint_client.do_get(ticket))
            .await
            .map_err(|_| sql_error(&request_id, "SQL result fetch timed out"))?
            .map_err(|err| sql_error(&request_id, err))?;
        Ok(ResultEndpointStream {
            stream,
            request_id,
            read_timeout,
        })
    }

    async fn cancel(
        &self,
        info: FlightInfo,
        default_namespace_path: &[String],
        unresolved_attempt: Arc<AtomicBool>,
    ) -> Result<CancelOutcome> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let read_timeout = resolve_timeout(
            self.client_config.timeout_config.read_timeout,
            "LANCE_CLIENT_READ_TIMEOUT",
            Some(DEFAULT_READ_TIMEOUT),
        )?
        .unwrap();
        let connection = self.connection(&request_id).await?;
        let headers = self.headers(default_namespace_path, &request_id).await?;
        let metadata = client_with_headers(connection.client.clone(), &headers)?
            .metadata()
            .clone();
        let dispatched = Arc::new(AtomicBool::new(false));
        let mut attempt = CancelAttempt::new(dispatched.clone(), unresolved_attempt);
        let mut client = FlightServiceClient::with_interceptor(
            connection.channel.clone(),
            move |request: tonic::Request<()>| {
                dispatched.store(true, Ordering::SeqCst);
                Ok(request)
            },
        )
        .max_decoding_message_size(MAX_SQL_MESSAGE_SIZE);
        let action = Action::new(
            "CancelFlightInfo",
            CancelFlightInfoRequest::new(info).encode_to_vec(),
        );
        let mut request = tonic::Request::new(action);
        *request.metadata_mut() = metadata;
        let result = tokio::time::timeout(read_timeout, async {
            let response = client
                .do_action(request)
                .await
                .map_err(|status| FlightError::Tonic(Box::new(status)))?;
            let response = response
                .into_inner()
                .message()
                .await
                .map_err(|status| FlightError::Tonic(Box::new(status)))?
                .ok_or_else(|| {
                    FlightError::protocol("Received no response for cancel_flight_info call")
                })?;
            CancelFlightInfoResult::decode(response.body)
                .map_err(|err| FlightError::DecodeError(err.to_string()))
        })
        .await
        .map_err(|_| sql_error(&request_id, "SQL query cancellation timed out"))?;
        let result = match result {
            Ok(result) => result,
            Err(FlightError::Tonic(status)) if status.code() == tonic::Code::NotFound => {
                attempt.resolve();
                return Ok(CancelOutcome::NotFound(request_id));
            }
            Err(FlightError::Tonic(status)) if !cancellation_status_is_ambiguous(status.code()) => {
                attempt.resolve();
                return Err(sql_error(&request_id, status));
            }
            Err(error) => return Err(sql_error(&request_id, error)),
        };
        let status = CancelStatus::try_from(result.status)
            .map_err(|_| sql_error(&request_id, "SQL query returned an invalid cancel status"))?;
        if status != CancelStatus::Unspecified {
            attempt.resolve();
        }
        Ok(CancelOutcome::Status(status))
    }

    async fn client_with_headers(
        &self,
        default_namespace_path: &[String],
        request_id: &str,
    ) -> Result<FlightClient> {
        let connection = self.connection(request_id).await?;
        let headers = self.headers(default_namespace_path, request_id).await?;
        client_with_headers(connection.client.clone(), &headers)
    }

    async fn connection(&self, request_id: &str) -> Result<&SqlConnection> {
        self.client
            .get_or_try_init(|| async {
                let target = resolve_sql_host_override(
                    self.host_override.as_deref(),
                    self.sql_host_override.as_deref(),
                )?;
                let channel = connect_channel(&target, &self.client_config, request_id).await?;
                let client = FlightServiceClient::new(channel.clone())
                    .max_decoding_message_size(MAX_SQL_MESSAGE_SIZE);
                Ok::<_, Error>(SqlConnection { channel, client })
            })
            .await
    }

    async fn headers(
        &self,
        default_namespace_path: &[String],
        request_id: &str,
    ) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        merge_headers(&mut headers, &self.client_config.extra_headers)?;
        if let Some(provider) = &self.client_config.header_provider {
            merge_headers(&mut headers, &provider.get_headers().await?)?;
        }

        let has_authorization = headers.contains_key("authorization");
        let has_api_key = headers.contains_key("x-api-key");
        if has_authorization && has_api_key {
            return Err(Error::InvalidInput {
                message: "SQL accepts either authorization or x-api-key, not both".to_string(),
            });
        }
        if !has_authorization && !has_api_key {
            if self.api_key.is_empty() {
                return Err(Error::InvalidInput {
                    message: "SQL authentication credentials are required".to_string(),
                });
            }
            insert_header(&mut headers, "x-api-key", &self.api_key)?;
        }

        insert_header(&mut headers, "database", &self.database)?;
        if let Some(database_prefix) = &self.database_prefix {
            insert_header(&mut headers, "x-lancedb-database-prefix", database_prefix)?;
        }
        let namespace_path = if default_namespace_path.is_empty() {
            "public".to_string()
        } else {
            default_namespace_path.join("$")
        };
        insert_header(&mut headers, "namespace-path", &namespace_path)?;
        insert_header(&mut headers, "x-request-id", request_id)?;
        if let Some(user_id) = self.client_config.resolve_user_id() {
            insert_header(&mut headers, "x-lancedb-user-id", &user_id)?;
        }
        Ok(headers)
    }
}

struct QueryRegistry {
    queries: StdMutex<HashMap<Uuid, Arc<RemoteQuery>>>,
}

impl QueryRegistry {
    fn new() -> Self {
        Self {
            queries: StdMutex::new(HashMap::new()),
        }
    }

    fn insert(&self, id: Uuid, query: Arc<RemoteQuery>) {
        self.remove_expired();
        self.queries.lock().unwrap().insert(id, query);
    }

    fn get(&self, id: Uuid) -> Option<Arc<RemoteQuery>> {
        self.remove_expired();
        let query = self.queries.lock().unwrap().get(&id).cloned();
        if let Some(query) = &query {
            query.touch();
        }
        query
    }

    fn remove_expired(&self) {
        self.queries
            .lock()
            .unwrap()
            .retain(|_, query| !query.registry_expired(Arc::strong_count(query) == 1));
    }
}

struct RemoteQuery {
    id: Uuid,
    client: Arc<SqlClientInner>,
    default_namespace_path: Vec<String>,
    state: Mutex<PollInfo>,
    poll_gate: Mutex<()>,
    cancel_gate: Mutex<()>,
    state_changed: Notify,
    cancelled: Notify,
    expires_at: StdMutex<Option<chrono::DateTime<chrono::Utc>>>,
    terminal_at: OnceLock<Instant>,
    last_accessed: StdMutex<Instant>,
    lifecycle: StdMutex<QueryLifecycle>,
    cancel_request_uncertain: Arc<AtomicBool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryLifecycle {
    Running,
    Ready,
    Cancelling,
    Completed,
    Cancelled,
}

impl RemoteQuery {
    fn new(
        id: Uuid,
        client: Arc<SqlClientInner>,
        default_namespace_path: Vec<String>,
        poll_info: PollInfo,
    ) -> Result<Self> {
        let expires_at = query_expiration(&poll_info)?;
        let terminal_at = OnceLock::new();
        let lifecycle = if poll_info.flight_descriptor.is_none() {
            let _ = terminal_at.set(Instant::now());
            QueryLifecycle::Ready
        } else {
            QueryLifecycle::Running
        };
        Ok(Self {
            id,
            client,
            default_namespace_path,
            state: Mutex::new(poll_info),
            poll_gate: Mutex::new(()),
            cancel_gate: Mutex::new(()),
            state_changed: Notify::new(),
            cancelled: Notify::new(),
            expires_at: StdMutex::new(expires_at),
            terminal_at,
            last_accessed: StdMutex::new(Instant::now()),
            lifecycle: StdMutex::new(lifecycle),
            cancel_request_uncertain: Arc::new(AtomicBool::new(false)),
        })
    }

    fn registry_expired(&self, abandoned: bool) -> bool {
        if let Some(finished) = self.terminal_at.get() {
            return finished.elapsed() >= TERMINAL_QUERY_RETENTION;
        }
        self.expires_at
            .lock()
            .unwrap()
            .is_some_and(|expires_at| expires_at <= chrono::Utc::now())
            || (abandoned
                && self.last_accessed.lock().unwrap().elapsed() >= ABANDONED_QUERY_RETENTION)
    }

    fn mark_terminal(&self) {
        let _ = self.terminal_at.set(Instant::now());
    }

    fn mark_ready(&self) {
        let mut lifecycle = self.lifecycle.lock().unwrap();
        if *lifecycle == QueryLifecycle::Running {
            *lifecycle = QueryLifecycle::Ready;
        }
        drop(lifecycle);
        self.mark_terminal();
    }

    fn mark_cancelled(&self) -> bool {
        self.cancel_request_uncertain.store(false, Ordering::SeqCst);
        let mut lifecycle = self.lifecycle.lock().unwrap();
        if matches!(
            *lifecycle,
            QueryLifecycle::Cancelled | QueryLifecycle::Completed
        ) {
            return false;
        }
        *lifecycle = QueryLifecycle::Cancelled;
        drop(lifecycle);
        self.mark_terminal();
        self.cancelled.notify_waiters();
        self.state_changed.notify_waiters();
        true
    }

    fn mark_cancelling(&self) {
        self.cancel_request_uncertain.store(false, Ordering::SeqCst);
        let mut lifecycle = self.lifecycle.lock().unwrap();
        if matches!(*lifecycle, QueryLifecycle::Running | QueryLifecycle::Ready) {
            *lifecycle = QueryLifecycle::Cancelling;
            drop(lifecycle);
            self.cancelled.notify_waiters();
            self.state_changed.notify_waiters();
        }
    }

    async fn restore_after_rejected_cancellation(&self) {
        self.cancel_request_uncertain.store(false, Ordering::SeqCst);
        let running = self.state.lock().await.flight_descriptor.is_some();
        let mut lifecycle = self.lifecycle.lock().unwrap();
        if *lifecycle == QueryLifecycle::Cancelling {
            *lifecycle = if running {
                QueryLifecycle::Running
            } else {
                QueryLifecycle::Ready
            };
            drop(lifecycle);
            self.state_changed.notify_waiters();
        }
    }

    fn mark_result_completed(&self) -> Result<()> {
        let mut lifecycle = self.lifecycle.lock().unwrap();
        if matches!(
            *lifecycle,
            QueryLifecycle::Cancelling | QueryLifecycle::Cancelled
        ) {
            return Err(self.cancelled_error());
        }
        *lifecycle = QueryLifecycle::Completed;
        Ok(())
    }

    fn lifecycle(&self) -> QueryLifecycle {
        *self.lifecycle.lock().unwrap()
    }

    fn is_cancellation_requested(&self) -> bool {
        matches!(
            self.lifecycle(),
            QueryLifecycle::Cancelling | QueryLifecycle::Cancelled
        )
    }

    fn cancelled_error(&self) -> Error {
        Error::JobCancelled {
            job_id: Some(self.id.to_string()),
        }
    }

    async fn wait_for_cancellation(&self) {
        loop {
            let cancelled = self.cancelled.notified();
            if self.is_cancellation_requested() {
                return;
            }
            cancelled.await;
        }
    }

    fn touch(&self) {
        *self.last_accessed.lock().unwrap() = Instant::now();
    }

    async fn poll_next_state(&self, descriptor: FlightDescriptor) -> Result<PollInfo> {
        self.touch();
        if self.is_cancellation_requested() {
            return Err(self.cancelled_error());
        }
        let _poll_guard = tokio::select! {
            biased;
            _ = self.wait_for_cancellation() => return Err(self.cancelled_error()),
            poll_guard = self.poll_gate.lock() => poll_guard,
        };
        let latest = self.state.lock().await.clone();
        if latest.flight_descriptor.as_ref() != Some(&descriptor) {
            return Ok(latest);
        }
        let updated = tokio::select! {
            biased;
            _ = self.wait_for_cancellation() => return Err(self.cancelled_error()),
            result = self.client.poll_continuation(
                descriptor.clone(),
                &self.default_namespace_path,
            ) => match result {
                Err(_) if self.is_cancellation_requested() => {
                    return Err(self.cancelled_error());
                }
                result => result?,
            },
        };
        self.update_state(&descriptor, updated).await
    }

    async fn prepare_result(self: &Arc<Self>) -> Result<PreparedSqlResult> {
        loop {
            if self.is_cancellation_requested() {
                return Err(self.cancelled_error());
            }
            let state = self.state.lock().await.clone();
            if let Some(info) = state.info {
                if let Some(endpoint) = info.endpoint.first().cloned() {
                    let mut endpoint_stream = tokio::select! {
                        biased;
                        _ = self.wait_for_cancellation() => return Err(self.cancelled_error()),
                        result = self.client.open_result_endpoint(
                            endpoint,
                            &self.default_namespace_path,
                        ) => result?,
                    };
                    let buffered_batch = tokio::select! {
                        biased;
                        _ = self.wait_for_cancellation() => return Err(self.cancelled_error()),
                        result = endpoint_stream.next_batch() => result?,
                    };
                    let schema = buffered_batch
                        .as_ref()
                        .map(RecordBatch::schema)
                        .or_else(|| endpoint_stream.stream.schema().cloned())
                        .ok_or_else(|| Error::Runtime {
                            message: "SQL result endpoint did not include a schema".to_string(),
                        })?;
                    return Ok(PreparedSqlResult {
                        schema,
                        next_endpoint: 1,
                        endpoint_stream: buffered_batch.is_some().then_some(endpoint_stream),
                        buffered_batch,
                    });
                }
                if state.flight_descriptor.is_none() {
                    let schema = if info.schema.is_empty() {
                        Arc::new(Schema::empty())
                    } else {
                        let request_id = uuid::Uuid::new_v4().to_string();
                        Arc::new(
                            info.try_decode_schema()
                                .map_err(|err| sql_error(&request_id, err))?,
                        )
                    };
                    return Ok(PreparedSqlResult {
                        schema,
                        next_endpoint: 0,
                        endpoint_stream: None,
                        buffered_batch: None,
                    });
                }
            } else if state.flight_descriptor.is_none() {
                return Err(Error::Runtime {
                    message: "Completed SQL query did not include result information".to_string(),
                });
            }
            let descriptor = state.flight_descriptor.ok_or_else(|| Error::Runtime {
                message: "Completed SQL query did not include result information".to_string(),
            })?;
            self.poll_next_state(descriptor).await?;
        }
    }

    async fn run_result_stream(
        self: Arc<Self>,
        mut prepared: PreparedSqlResult,
        sender: mpsc::Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        if let Some(batch) = prepared.buffered_batch.take()
            && !self
                .send_result_batch(&sender, &prepared.schema, batch)
                .await?
        {
            return Ok(());
        }
        loop {
            if self.is_cancellation_requested() {
                return Err(self.cancelled_error());
            }
            if let Some(endpoint_stream) = prepared.endpoint_stream.as_mut() {
                let batch = tokio::select! {
                    biased;
                    _ = sender.closed() => return Ok(()),
                    _ = self.wait_for_cancellation() => return Err(self.cancelled_error()),
                    result = endpoint_stream.next_batch() => result?,
                };
                if let Some(batch) = batch {
                    if !self
                        .send_result_batch(&sender, &prepared.schema, batch)
                        .await?
                    {
                        return Ok(());
                    }
                } else {
                    prepared.endpoint_stream = None;
                }
                continue;
            }

            let state = self.state.lock().await.clone();
            let endpoints = state
                .info
                .as_ref()
                .map(|info| info.endpoint.as_slice())
                .unwrap_or_default();
            if prepared.next_endpoint > endpoints.len() {
                return Err(Error::Runtime {
                    message: "SQL service removed a previously advertised result endpoint"
                        .to_string(),
                });
            }
            if let Some(endpoint) = endpoints.get(prepared.next_endpoint).cloned() {
                prepared.next_endpoint += 1;
                prepared.endpoint_stream = Some(tokio::select! {
                    biased;
                    _ = sender.closed() => return Ok(()),
                    _ = self.wait_for_cancellation() => return Err(self.cancelled_error()),
                    result = self.client.open_result_endpoint(
                        endpoint,
                        &self.default_namespace_path,
                    ) => result?,
                });
                continue;
            }
            if let Some(descriptor) = state.flight_descriptor {
                tokio::select! {
                    biased;
                    _ = sender.closed() => return Ok(()),
                    _ = self.wait_for_cancellation() => return Err(self.cancelled_error()),
                    result = self.poll_next_state(descriptor) => result?,
                };
                continue;
            }
            self.mark_result_completed()?;
            return Ok(());
        }
    }

    async fn send_result_batch(
        &self,
        sender: &mpsc::Sender<Result<RecordBatch>>,
        schema: &SchemaRef,
        batch: RecordBatch,
    ) -> Result<bool> {
        if batch.schema().as_ref() != schema.as_ref() {
            return Err(Error::Runtime {
                message: "SQL result endpoint returned a different schema".to_string(),
            });
        }
        tokio::select! {
            biased;
            _ = self.wait_for_cancellation() => Err(self.cancelled_error()),
            result = sender.send(Ok(batch)) => Ok(result.is_ok()),
        }
    }

    async fn update_state(
        &self,
        descriptor: &FlightDescriptor,
        updated: PollInfo,
    ) -> Result<PollInfo> {
        self.touch();
        let expires_at = query_expiration(&updated)?;
        let mut state = self.state.lock().await;
        if state.flight_descriptor.as_ref() == Some(descriptor) {
            if updated.flight_descriptor.is_none() {
                self.mark_ready();
            }
            *self.expires_at.lock().unwrap() = expires_at;
            *state = updated;
            self.state_changed.notify_waiters();
        }
        Ok(state.clone())
    }
}

impl RemoteQuery {
    async fn describe(&self) -> Result<QueryDescription> {
        let timeout = self.client.overall_timeout()?;
        with_overall_timeout(timeout, "SQL query description", self.describe_inner()).await
    }

    async fn describe_inner(&self) -> Result<QueryDescription> {
        self.touch();
        if self.is_cancellation_requested() {
            let state = self.state.lock().await.clone();
            return query_description(self.id, &state, self.lifecycle());
        }
        let state = self.state.lock().await.clone();
        let state = if let Some(descriptor) = state.flight_descriptor.clone() {
            let poll_guard = tokio::select! {
                biased;
                _ = self.wait_for_cancellation() => return query_description(
                    self.id,
                    &state,
                    self.lifecycle(),
                ),
                poll_guard = tokio::time::timeout(
                    STATUS_POLL_TIMEOUT,
                    self.poll_gate.lock(),
                ) => poll_guard,
            };
            let Ok(_poll_guard) = poll_guard else {
                return query_description(self.id, &state, self.lifecycle());
            };
            let latest = self.state.lock().await.clone();
            if latest.flight_descriptor.as_ref() != Some(&descriptor) {
                latest
            } else {
                let updated = tokio::select! {
                    biased;
                    _ = self.wait_for_cancellation() => return query_description(
                        self.id,
                        &latest,
                        self.lifecycle(),
                    ),
                    result = self.client.poll_status(
                        descriptor.clone(),
                        &self.default_namespace_path,
                    ) => match result {
                        Err(_) if self.is_cancellation_requested() => return query_description(
                            self.id,
                            &latest,
                            self.lifecycle(),
                        ),
                        result => result?,
                    },
                };
                if let Some(updated) = updated {
                    self.update_state(&descriptor, updated).await?
                } else {
                    latest
                }
            }
        } else {
            state
        };
        query_description(self.id, &state, self.lifecycle())
    }

    async fn cancel(&self) -> Result<()> {
        let timeout = self.client.overall_timeout()?;
        with_overall_timeout(timeout, "SQL query cancellation", self.cancel_inner()).await
    }

    async fn cancel_inner(&self) -> Result<()> {
        self.touch();
        let _cancel_guard = self.cancel_gate.lock().await;
        if matches!(
            self.lifecycle(),
            QueryLifecycle::Cancelled | QueryLifecycle::Completed
        ) {
            return Ok(());
        }
        loop {
            let notified = self.state_changed.notified();
            let state = self.state.lock().await.clone();
            if let Some(info) = state.info {
                let previously_uncertain = self.cancel_request_uncertain.load(Ordering::SeqCst);
                let outcome = match self
                    .client
                    .cancel(
                        info,
                        &self.default_namespace_path,
                        self.cancel_request_uncertain.clone(),
                    )
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(_)
                        if matches!(
                            self.lifecycle(),
                            QueryLifecycle::Cancelled | QueryLifecycle::Completed
                        ) =>
                    {
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                };
                if matches!(
                    self.lifecycle(),
                    QueryLifecycle::Cancelled | QueryLifecycle::Completed
                ) {
                    return Ok(());
                }
                let status = match outcome {
                    CancelOutcome::Status(status) => status,
                    CancelOutcome::NotFound(_)
                        if self.lifecycle() == QueryLifecycle::Cancelling =>
                    {
                        self.mark_cancelled();
                        return Ok(());
                    }
                    CancelOutcome::NotFound(request_id) => {
                        let message = if previously_uncertain {
                            "SQL query cancellation outcome is unknown because a prior request may have reached the service and the target was not found on retry"
                        } else {
                            "SQL query cancellation target was not found"
                        };
                        return Err(sql_error(&request_id, message));
                    }
                };
                return match status {
                    CancelStatus::Cancelled => {
                        self.mark_cancelled();
                        Ok(())
                    }
                    CancelStatus::Cancelling => {
                        self.mark_cancelling();
                        Ok(())
                    }
                    CancelStatus::NotCancellable => {
                        self.restore_after_rejected_cancellation().await;
                        Err(Error::NotSupported {
                            message: "The SQL query is not cancellable".to_string(),
                        })
                    }
                    CancelStatus::Unspecified => Err(Error::Runtime {
                        message: "The SQL service returned an unspecified cancellation status"
                            .to_string(),
                    }),
                };
            }
            let Some(descriptor) = state.flight_descriptor else {
                return Ok(());
            };

            tokio::select! {
                poll_guard = self.poll_gate.lock() => {
                    let _poll_guard = poll_guard;
                    if self.state.lock().await.flight_descriptor.as_ref() != Some(&descriptor) {
                        continue;
                    }
                    let updated = self.client.poll_continuation(
                        descriptor.clone(),
                        &self.default_namespace_path,
                    ).await?;
                    self.update_state(&descriptor, updated).await?;
                }
                _ = notified => {}
            }
        }
    }
}

struct RemoteQueryHandle {
    query: Arc<RemoteQuery>,
    result_started: AtomicBool,
}

impl RemoteQueryHandle {
    fn new(query: Arc<RemoteQuery>) -> Self {
        Self {
            query,
            result_started: AtomicBool::new(false),
        }
    }
}

#[async_trait::async_trait]
impl QueryHandle for RemoteQueryHandle {
    fn id(&self) -> Uuid {
        self.query.touch();
        self.query.id
    }

    async fn describe(&self) -> Result<QueryDescription> {
        self.query.describe().await
    }

    async fn reader(&self) -> Result<SendableRecordBatchStream> {
        let timeout = self.query.client.overall_timeout()?;
        if self
            .result_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(Error::Runtime {
                message: "SQL query results can only be consumed once".to_string(),
            });
        }
        let result_start = ResultStartGuard::new(&self.result_started);
        let started = Instant::now();
        let prepared =
            with_overall_timeout(timeout, "SQL query result", self.query.prepare_result()).await?;
        let remaining_timeout = timeout.map(|timeout| timeout.saturating_sub(started.elapsed()));
        let schema = prepared.schema.clone();
        let (sender, receiver) = mpsc::channel(2);
        let error_sender = sender.clone();
        let query = self.query.clone();
        tokio::spawn(async move {
            let result = with_overall_timeout(
                remaining_timeout,
                "SQL query result",
                query.run_result_stream(prepared, sender),
            )
            .await;
            if let Err(error) = result {
                let _ = error_sender.send(Err(error)).await;
            }
        });
        let stream = futures::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        });
        result_start.commit();
        Ok(Box::pin(SimpleRecordBatchStream::new(stream, schema)))
    }

    async fn cancel(&self) -> Result<()> {
        self.query.cancel().await
    }
}

fn query_description(
    id: Uuid,
    poll_info: &PollInfo,
    lifecycle: QueryLifecycle,
) -> Result<QueryDescription> {
    let expires_at = query_expiration(poll_info)?;
    Ok(QueryDescription {
        id,
        status: match lifecycle {
            QueryLifecycle::Cancelling => QueryStatus::Cancelling,
            QueryLifecycle::Cancelled => QueryStatus::Cancelled,
            QueryLifecycle::Running if poll_info.flight_descriptor.is_some() => {
                QueryStatus::Running
            }
            QueryLifecycle::Running | QueryLifecycle::Ready | QueryLifecycle::Completed => {
                QueryStatus::Finished
            }
        },
        progress: poll_info.progress,
        expires_at,
    })
}

fn query_expiration(poll_info: &PollInfo) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    poll_info
        .expiration_time
        .as_ref()
        .map(|timestamp| {
            u32::try_from(timestamp.nanos)
                .ok()
                .and_then(|nanos| chrono::DateTime::from_timestamp(timestamp.seconds, nanos))
                .ok_or_else(|| Error::Runtime {
                    message: "SQL service returned an invalid query expiration time".to_string(),
                })
        })
        .transpose()
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SqlTarget {
    uri: String,
    tls: bool,
}

fn resolve_sql_host_override(
    host_override: Option<&str>,
    sql_host_override: Option<&str>,
) -> Result<SqlTarget> {
    if let Some(uri) = sql_host_override {
        return normalize_sql_host_override(uri);
    }
    let host_override = host_override.ok_or_else(|| Error::InvalidInput {
        message: "sql_host_override is required when the SQL service endpoint cannot be derived from host_override".to_string(),
    })?;
    let parsed = url::Url::parse(host_override).map_err(|err| Error::InvalidInput {
        message: format!("Invalid host_override: {err}"),
    })?;
    if parsed.scheme() != "http" {
        return Err(Error::InvalidInput {
            message: "sql_host_override is required for TLS or non-HTTP host overrides".to_string(),
        });
    }
    validate_endpoint_url(&parsed, "host_override")?;
    let port = match parsed.port().or(explicit_port(host_override)) {
        Some(u16::MAX) => {
            return Err(Error::InvalidInput {
                message: "sql_host_override is required when host_override uses port 65535"
                    .to_string(),
            });
        }
        Some(port) => port + 1,
        None => DEFAULT_SQL_PORT,
    };
    Ok(SqlTarget {
        uri: endpoint_uri("http", parsed.host_str().unwrap(), port),
        tls: false,
    })
}

fn normalize_sql_host_override(uri: &str) -> Result<SqlTarget> {
    let parsed = url::Url::parse(uri).map_err(|err| Error::InvalidInput {
        message: format!("Invalid sql_host_override: {err}"),
    })?;
    validate_endpoint_url(&parsed, "sql_host_override")?;
    let tls = match parsed.scheme().to_ascii_lowercase().as_str() {
        "grpc" | "grpc+tcp" | "http" => false,
        "grpc+tls" | "grpcs" | "https" => true,
        _ => {
            return Err(Error::InvalidInput {
                message:
                    "sql_host_override must use grpc, grpc+tcp, grpc+tls, grpcs, http, or https"
                        .to_string(),
            });
        }
    };
    let port = parsed.port().or(explicit_port(uri)).unwrap_or(if tls {
        DEFAULT_SQL_TLS_PORT
    } else {
        DEFAULT_SQL_PORT
    });
    if port == 0 {
        return Err(Error::InvalidInput {
            message: "sql_host_override port must be greater than zero".to_string(),
        });
    }
    Ok(SqlTarget {
        uri: endpoint_uri(
            if tls { "https" } else { "http" },
            parsed.host_str().unwrap(),
            port,
        ),
        tls,
    })
}

fn explicit_port(uri: &str) -> Option<u16> {
    let authority = uri.split_once("://")?.1.split(['/', '?', '#']).next()?;
    let suffix = if authority.starts_with('[') {
        authority.split_once(']')?.1.strip_prefix(':')?
    } else {
        authority.rsplit_once(':')?.1
    };
    suffix.parse().ok()
}

fn validate_endpoint_url(parsed: &url::Url, name: &str) -> Result<()> {
    if parsed.host_str().is_none() {
        return Err(Error::InvalidInput {
            message: format!("{name} must include a hostname"),
        });
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(Error::InvalidInput {
            message: format!("{name} must not include user information"),
        });
    }
    if !matches!(parsed.path(), "" | "/") || parsed.query().is_some() || parsed.fragment().is_some()
    {
        return Err(Error::InvalidInput {
            message: format!("{name} must not include a path, query, or fragment"),
        });
    }
    Ok(())
}

fn endpoint_uri(scheme: &str, host: &str, port: u16) -> String {
    if host.contains(':') {
        let host = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host);
        format!("{scheme}://[{host}]:{port}")
    } else {
        format!("{scheme}://{host}:{port}")
    }
}

async fn connect_channel(
    target: &SqlTarget,
    config: &ClientConfig,
    request_id: &str,
) -> Result<Channel> {
    let connect_timeout = resolve_timeout(
        config.timeout_config.connect_timeout,
        "LANCE_CLIENT_CONNECT_TIMEOUT",
        Some(DEFAULT_CONNECT_TIMEOUT),
    )?
    .unwrap();
    let mut endpoint = Endpoint::from_shared(target.uri.clone())
        .map_err(|err| sql_error(request_id, err))?
        .connect_timeout(connect_timeout);
    if target.tls {
        endpoint = endpoint
            .tls_config(tls_config(config.tls_config.as_ref())?)
            .map_err(|err| sql_error(request_id, err))?;
    }
    tokio::time::timeout(connect_timeout, endpoint.connect())
        .await
        .map_err(|_| sql_error(request_id, "SQL connection timed out"))?
        .map_err(|err| sql_error(request_id, err))
}

fn tls_config(config: Option<&TlsConfig>) -> Result<ClientTlsConfig> {
    let mut tls = ClientTlsConfig::new().with_enabled_roots();
    if let Some(config) = config {
        if !config.assert_hostname {
            return Err(Error::InvalidInput {
                message: "SQL cannot disable TLS hostname verification".to_string(),
            });
        }
        if let Some(path) = &config.ssl_ca_cert {
            let pem = fs::read(path).map_err(|err| Error::InvalidInput {
                message: format!("Failed to read SQL CA certificate {path}: {err}"),
            })?;
            tls = tls.ca_certificate(Certificate::from_pem(pem));
        }
        match (&config.cert_file, &config.key_file) {
            (Some(cert), Some(key)) => {
                let cert_pem = fs::read(cert).map_err(|err| Error::InvalidInput {
                    message: format!("Failed to read SQL client certificate {cert}: {err}"),
                })?;
                let key_pem = fs::read(key).map_err(|err| Error::InvalidInput {
                    message: format!("Failed to read SQL client key {key}: {err}"),
                })?;
                tls = tls.identity(Identity::from_pem(cert_pem, key_pem));
            }
            (None, None) => {}
            _ => {
                return Err(Error::InvalidInput {
                    message: "SQL mTLS requires both cert_file and key_file".to_string(),
                });
            }
        }
    }
    Ok(tls)
}

fn client_with_headers(
    client: FlightServiceClient<Channel>,
    headers: &HeaderMap,
) -> Result<FlightClient> {
    let mut client = FlightClient::new_from_inner(client);
    for (key, value) in headers {
        let value = value.to_str().map_err(|err| Error::InvalidInput {
            message: format!("Invalid SQL metadata value for {key:?}: {err}"),
        })?;
        client
            .add_header(key.as_str(), value)
            .map_err(|err| Error::InvalidInput {
                message: format!("Invalid SQL metadata header {key:?}: {err}"),
            })?;
    }
    Ok(client)
}

fn merge_headers(destination: &mut HeaderMap, source: &HashMap<String, String>) -> Result<()> {
    for (key, value) in source {
        insert_header(destination, key, value)?;
    }
    Ok(())
}

fn insert_header(headers: &mut HeaderMap, key: &str, value: &str) -> Result<()> {
    let key = HeaderName::from_bytes(key.as_bytes()).map_err(|err| Error::InvalidInput {
        message: format!("Invalid SQL metadata key {key:?}: {err}"),
    })?;
    let value = HeaderValue::try_from(value).map_err(|err| Error::InvalidInput {
        message: format!("Invalid SQL metadata value for {key:?}: {err}"),
    })?;
    headers.insert(key, value);
    Ok(())
}

fn validate_namespace_path(path: &[String]) -> Result<()> {
    for component in path {
        if component.is_empty()
            || !component.is_ascii()
            || component.contains('$')
            || component.bytes().any(|byte| !(0x20..=0x7e).contains(&byte))
        {
            return Err(Error::InvalidInput {
                message: "default_namespace_path components must be non-empty printable ASCII strings without '$'".to_string(),
            });
        }
    }
    Ok(())
}

fn poll_retry_delay(config: &ResolvedRetryConfig, retry_count: u8) -> Duration {
    let exponent = i32::from(retry_count.saturating_sub(1).min(16));
    let backoff = config.backoff_factor * 2.0_f32.powi(exponent);
    let jitter = rand::random::<f32>() * config.backoff_jitter;
    Duration::from_secs_f32((backoff + jitter).clamp(MIN_POLL_INTERVAL.as_secs_f32(), 60.0))
}

fn cancellation_status_is_ambiguous(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Cancelled
            | tonic::Code::Unknown
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Internal
            | tonic::Code::Unavailable
            | tonic::Code::DataLoss
    )
}

fn resolve_timeout(
    configured: Option<Duration>,
    env_name: &str,
    default: Option<Duration>,
) -> Result<Option<Duration>> {
    if configured.is_some() {
        return Ok(configured);
    }
    match std::env::var(env_name) {
        Ok(value) => value
            .parse::<u64>()
            .map(Duration::from_secs)
            .map(Some)
            .map_err(|_| Error::InvalidInput {
                message: format!("Invalid value for {env_name} environment variable: {value:?}"),
            }),
        Err(_) => Ok(default),
    }
}

async fn with_overall_timeout<T>(
    timeout: Option<Duration>,
    operation: &str,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    match timeout {
        Some(timeout) => {
            tokio::time::timeout(timeout, future)
                .await
                .map_err(|_| Error::Runtime {
                    message: format!("{operation} timed out"),
                })?
        }
        None => future.await,
    }
}

fn sql_error(request_id: &str, error: impl std::fmt::Display) -> Error {
    Error::Runtime {
        message: format!("SQL error (request_id={request_id}): {error}"),
    }
}

#[cfg(test)]
#[path = "sql_test.rs"]
mod tests;
