// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use arrow_flight::{
    Action, CancelFlightInfoRequest, CancelFlightInfoResult, CancelStatus, FlightClient,
    FlightDescriptor, FlightInfo, PollInfo,
};
use futures::TryStreamExt;
use prost::Message;
use tokio::sync::{Mutex, Notify, OnceCell, OwnedSemaphorePermit, Semaphore};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::error::{Error, Result};
use crate::remote::client::{ClientConfig, TlsConfig};
use crate::remote::retry::ResolvedRetryConfig;
use crate::sql::{Query, QueryDescription, QueryHandle};

const DEFAULT_SQL_PORT: u16 = 10025;
const DEFAULT_SQL_TLS_PORT: u16 = 10026;
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(300);
const STATUS_POLL_TIMEOUT: Duration = Duration::from_secs(1);
const MIN_POLL_INTERVAL: Duration = Duration::from_millis(50);
const MAX_SQL_MESSAGE_SIZE: usize = 1024 * 1024 * 1024;
const QUERY_CACHE_CAPACITY: u64 = 10_000;
const TERMINAL_QUERY_RETENTION: Duration = Duration::from_secs(300);
const ABANDONED_QUERY_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const QUERY_ID_PREFIX: &str = "lq1_";

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
    channel: Channel,
    client: FlightServiceClient<Channel>,
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
            queries: Arc::new(QueryRegistry::new(QUERY_CACHE_CAPACITY)),
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
            let permit = self.queries.reserve()?;
            let command = CommandStatementQuery {
                query: query.to_string(),
                transaction_id: None,
            };
            let descriptor = FlightDescriptor::new_cmd(command.as_any().encode_to_vec());
            let poll_info = self.inner.poll(descriptor, default_namespace_path).await?;
            let query_id = format!("{QUERY_ID_PREFIX}{}", uuid::Uuid::new_v4().simple());
            let query = Arc::new(RemoteQuery::new(
                query_id.clone(),
                self.inner.clone(),
                default_namespace_path.to_vec(),
                poll_info,
            )?);
            self.queries.insert(query_id, query.clone(), permit);
            Ok(Query::new(Arc::new(RemoteQueryHandle::new(query))))
        })
        .await
    }

    pub(super) async fn describe(&self, query_id: &str) -> Result<QueryDescription> {
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

    async fn fetch_result(
        &self,
        info: FlightInfo,
        default_namespace_path: &[String],
    ) -> Result<Vec<RecordBatch>> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let read_timeout = resolve_timeout(
            self.client_config.timeout_config.read_timeout,
            "LANCE_CLIENT_READ_TIMEOUT",
            Some(DEFAULT_READ_TIMEOUT),
        )?
        .unwrap();
        let mut result_schema = if info.schema.is_empty() {
            None
        } else {
            Some(std::sync::Arc::new(
                info.clone()
                    .try_decode_schema()
                    .map_err(|err| sql_error(&request_id, err))?,
            ))
        };

        let mut batches = Vec::new();
        for endpoint in info.endpoint {
            let ticket = endpoint.ticket.clone().ok_or_else(|| {
                sql_error(&request_id, "SQL result endpoint did not include a ticket")
            })?;
            let mut endpoint_client = self
                .client_with_headers(default_namespace_path, &request_id)
                .await?;
            let mut stream = tokio::time::timeout(read_timeout, endpoint_client.do_get(ticket))
                .await
                .map_err(|_| sql_error(&request_id, "SQL result fetch timed out"))?
                .map_err(|err| sql_error(&request_id, err))?;
            loop {
                let next = tokio::time::timeout(read_timeout, stream.try_next())
                    .await
                    .map_err(|_| sql_error(&request_id, "SQL result read timed out"))?
                    .map_err(|err| sql_error(&request_id, err))?;
                match next {
                    Some(batch) => batches.push(batch),
                    None => break,
                }
            }
            if result_schema.is_none() {
                result_schema = stream.schema().cloned();
            }
        }
        if batches.is_empty()
            && let Some(schema) = result_schema
        {
            batches.push(RecordBatch::new_empty(schema));
        }
        Ok(batches)
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
            Ok(result) => {
                attempt.resolve();
                result
            }
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
        CancelStatus::try_from(result.status)
            .map(CancelOutcome::Status)
            .map_err(|_| sql_error(&request_id, "SQL query returned an invalid cancel status"))
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
    ) -> Result<HashMap<String, String>> {
        let mut headers = HashMap::new();
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

struct RegisteredQuery {
    query: Arc<RemoteQuery>,
    _permit: OwnedSemaphorePermit,
}

struct QueryRegistry {
    queries: StdMutex<HashMap<String, RegisteredQuery>>,
    capacity: Arc<Semaphore>,
    max_capacity: u64,
}

impl QueryRegistry {
    fn new(capacity: u64) -> Self {
        Self {
            queries: StdMutex::new(HashMap::new()),
            capacity: Arc::new(Semaphore::new(capacity as usize)),
            max_capacity: capacity,
        }
    }

    fn reserve(&self) -> Result<OwnedSemaphorePermit> {
        self.remove_expired();
        if let Ok(permit) = self.capacity.clone().try_acquire_owned() {
            return Ok(permit);
        }
        if let Some(permit) = self
            .remove_oldest_terminal()
            .or_else(|| self.remove_oldest_abandoned())
        {
            return Ok(permit);
        }
        self.capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| self.capacity_error())
    }

    fn insert(&self, id: String, query: Arc<RemoteQuery>, permit: OwnedSemaphorePermit) {
        self.queries.lock().unwrap().insert(
            id,
            RegisteredQuery {
                query,
                _permit: permit,
            },
        );
    }

    fn get(&self, id: &str) -> Option<Arc<RemoteQuery>> {
        self.remove_expired();
        let query = self
            .queries
            .lock()
            .unwrap()
            .get(id)
            .map(|entry| entry.query.clone());
        if let Some(query) = &query {
            query.touch();
        }
        query
    }

    fn remove_expired(&self) {
        self.queries.lock().unwrap().retain(|_, entry| {
            !entry
                .query
                .registry_expired(Arc::strong_count(&entry.query) == 1)
        });
    }

    fn remove_oldest_terminal(&self) -> Option<OwnedSemaphorePermit> {
        let mut queries = self.queries.lock().unwrap();
        let oldest = queries
            .iter()
            .filter_map(|(id, entry)| entry.query.terminal_at.get().map(|at| (id.clone(), *at)))
            .min_by_key(|(_, at)| *at)
            .map(|(id, _)| id);
        oldest
            .and_then(|id| queries.remove(&id))
            .map(|entry| entry._permit)
    }

    fn remove_oldest_abandoned(&self) -> Option<OwnedSemaphorePermit> {
        let mut queries = self.queries.lock().unwrap();
        let oldest = queries
            .iter()
            .filter(|(_, entry)| {
                entry.query.terminal_at.get().is_none() && Arc::strong_count(&entry.query) == 1
            })
            .map(|(id, entry)| (id.clone(), *entry.query.last_accessed.lock().unwrap()))
            .min_by_key(|(_, last_accessed)| *last_accessed)
            .map(|(id, _)| id);
        oldest
            .and_then(|id| queries.remove(&id))
            .map(|entry| entry._permit)
    }

    fn capacity_error(&self) -> Error {
        Error::Runtime {
            message: format!(
                "This connection already retains {} active SQL queries",
                self.max_capacity
            ),
        }
    }
}

struct RemoteQuery {
    id: String,
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
        id: String,
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
            job_id: Some(self.id.clone()),
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

    async fn poll_until_finished(&self) -> Result<FlightInfo> {
        self.touch();
        loop {
            if self.is_cancellation_requested() {
                return Err(self.cancelled_error());
            }
            let state = self.state.lock().await.clone();
            let Some(descriptor) = state.flight_descriptor else {
                if self.is_cancellation_requested() {
                    return Err(self.cancelled_error());
                }
                return state.info.ok_or_else(|| Error::Runtime {
                    message: "Completed SQL query did not include result information".to_string(),
                });
            };
            let _poll_guard = tokio::select! {
                biased;
                _ = self.wait_for_cancellation() => return Err(self.cancelled_error()),
                poll_guard = self.poll_gate.lock() => poll_guard,
            };
            if self.state.lock().await.flight_descriptor.as_ref() != Some(&descriptor) {
                continue;
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
            self.update_state(&descriptor, updated).await?;
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
            return query_description(&self.id, &state, self.lifecycle());
        }
        let state = self.state.lock().await.clone();
        let state = if let Some(descriptor) = state.flight_descriptor.clone() {
            let poll_guard = tokio::select! {
                biased;
                _ = self.wait_for_cancellation() => return query_description(
                    &self.id,
                    &state,
                    self.lifecycle(),
                ),
                poll_guard = tokio::time::timeout(
                    STATUS_POLL_TIMEOUT,
                    self.poll_gate.lock(),
                ) => poll_guard,
            };
            let Ok(_poll_guard) = poll_guard else {
                return query_description(&self.id, &state, self.lifecycle());
            };
            let latest = self.state.lock().await.clone();
            if latest.flight_descriptor.as_ref() != Some(&descriptor) {
                latest
            } else {
                let updated = tokio::select! {
                    biased;
                    _ = self.wait_for_cancellation() => return query_description(
                        &self.id,
                        &latest,
                        self.lifecycle(),
                    ),
                    result = self.client.poll_status(
                        descriptor.clone(),
                        &self.default_namespace_path,
                    ) => match result {
                        Err(_) if self.is_cancellation_requested() => return query_description(
                            &self.id,
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
        query_description(&self.id, &state, self.lifecycle())
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
                        if self.lifecycle() == QueryLifecycle::Cancelling
                            || previously_uncertain =>
                    {
                        self.mark_cancelled();
                        return Ok(());
                    }
                    CancelOutcome::NotFound(request_id) => {
                        return Err(sql_error(
                            &request_id,
                            "SQL query cancellation target was not found",
                        ));
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
    result: OnceCell<Vec<RecordBatch>>,
}

impl RemoteQueryHandle {
    fn new(query: Arc<RemoteQuery>) -> Self {
        Self {
            query,
            result: OnceCell::new(),
        }
    }
}

#[async_trait::async_trait]
impl QueryHandle for RemoteQueryHandle {
    fn id(&self) -> &str {
        self.query.touch();
        &self.query.id
    }

    async fn describe(&self) -> Result<QueryDescription> {
        self.query.describe().await
    }

    async fn result(&self) -> Result<Vec<RecordBatch>> {
        let timeout = self.query.client.overall_timeout()?;
        with_overall_timeout(timeout, "SQL query result", async {
            let batches = self
                .result
                .get_or_try_init(|| async {
                    let info = self.query.poll_until_finished().await?;
                    let batches = tokio::select! {
                        biased;
                        _ = self.query.wait_for_cancellation() => {
                            return Err(self.query.cancelled_error());
                        }
                        result = self.query.client.fetch_result(
                            info,
                            &self.query.default_namespace_path,
                        ) => match result {
                            Err(_) if self.query.is_cancellation_requested() => {
                                return Err(self.query.cancelled_error());
                            }
                            result => result?,
                        },
                    };
                    self.query.mark_result_completed()?;
                    Ok(batches)
                })
                .await?;
            self.query.mark_result_completed()?;
            Ok(batches.clone())
        })
        .await
    }

    async fn cancel(&self) -> Result<()> {
        self.query.cancel().await
    }
}

fn query_description(
    id: &str,
    poll_info: &PollInfo,
    lifecycle: QueryLifecycle,
) -> Result<QueryDescription> {
    let expires_at = query_expiration(poll_info)?;
    Ok(QueryDescription {
        id: id.to_string(),
        status: match lifecycle {
            QueryLifecycle::Cancelling => "cancelling",
            QueryLifecycle::Cancelled => "cancelled",
            QueryLifecycle::Running if poll_info.flight_descriptor.is_some() => "running",
            QueryLifecycle::Running | QueryLifecycle::Ready | QueryLifecycle::Completed => {
                "finished"
            }
        }
        .to_string(),
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
    headers: &HashMap<String, String>,
) -> Result<FlightClient> {
    let mut client = FlightClient::new_from_inner(client);
    for (key, value) in headers {
        client
            .add_header(key, value)
            .map_err(|err| Error::InvalidInput {
                message: format!("Invalid SQL metadata header {key:?}: {err}"),
            })?;
    }
    Ok(client)
}

fn merge_headers(
    destination: &mut HashMap<String, String>,
    source: &HashMap<String, String>,
) -> Result<()> {
    for (key, value) in source {
        insert_header(destination, key, value)?;
    }
    Ok(())
}

fn insert_header(headers: &mut HashMap<String, String>, key: &str, value: &str) -> Result<()> {
    let key = key.to_ascii_lowercase();
    let valid_key = !key.is_empty()
        && key.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"-_.".contains(&byte)
        });
    if !valid_key {
        return Err(Error::InvalidInput {
            message: format!("Invalid SQL metadata key: {key:?}"),
        });
    }
    if !value.is_ascii() || value.bytes().any(|byte| !(0x20..=0x7e).contains(&byte)) {
        return Err(Error::InvalidInput {
            message: format!("SQL metadata must be printable ASCII: {key:?}"),
        });
    }
    headers.insert(key, value.to_string());
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
mod tests {
    use std::sync::atomic::AtomicUsize;

    use arrow_array::{Int64Array, StringArray};
    use arrow_flight::encode::FlightDataEncoderBuilder;
    use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
    use arrow_flight::sql::{Any, CommandStatementQuery};
    use arrow_flight::{
        Action, ActionType, CancelFlightInfoResult, Criteria, Empty, FlightData, FlightEndpoint,
        FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    };
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;
    use futures::stream::BoxStream;
    use tonic::{Request, Response, Status, Streaming};

    use super::*;
    use crate::remote::client::HeaderProvider;

    #[derive(Debug, Default)]
    struct DelayedHeaderProvider {
        delay_next: AtomicBool,
    }

    #[async_trait::async_trait]
    impl HeaderProvider for DelayedHeaderProvider {
        async fn get_headers(&self) -> Result<HashMap<String, String>> {
            if self.delay_next.swap(false, Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1_100)).await;
            }
            Ok(HashMap::new())
        }
    }

    fn assert_overall_timeout<T>(result: Result<T>, operation: &str) {
        match result {
            Err(Error::Runtime { message }) => {
                assert_eq!(message, format!("SQL query {operation} timed out"));
            }
            _ => panic!("SQL query {operation} did not honor the overall timeout"),
        }
    }

    #[derive(Debug)]
    struct CapturedHeaders {
        database: String,
        namespace_path: String,
        request_id: String,
        api_key: String,
        database_prefix: String,
    }

    #[derive(Clone)]
    struct TestSqlService {
        query_count: Arc<AtomicUsize>,
        do_get_count: Arc<AtomicUsize>,
        cancel_count: Arc<AtomicUsize>,
        cancel_denied_count: Arc<AtomicUsize>,
        cancel_timeout_count: Arc<AtomicUsize>,
        cancelling_response_count: Arc<AtomicUsize>,
        first_continuation_count: Arc<AtomicUsize>,
        transient_poll_failures: Arc<AtomicUsize>,
        headers: Arc<std::sync::Mutex<Vec<CapturedHeaders>>>,
        result: RecordBatch,
        large_result: RecordBatch,
    }

    impl Default for TestSqlService {
        fn default() -> Self {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]));
            let result =
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42_i64]))])
                    .unwrap();
            let large_schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )]));
            let large_result = RecordBatch::try_new(
                large_schema,
                vec![Arc::new(StringArray::from(vec![
                    "x".repeat(5 * 1024 * 1024),
                ]))],
            )
            .unwrap();
            Self {
                query_count: Arc::new(AtomicUsize::new(0)),
                do_get_count: Arc::new(AtomicUsize::new(0)),
                cancel_count: Arc::new(AtomicUsize::new(0)),
                cancel_denied_count: Arc::new(AtomicUsize::new(0)),
                cancel_timeout_count: Arc::new(AtomicUsize::new(0)),
                cancelling_response_count: Arc::new(AtomicUsize::new(0)),
                first_continuation_count: Arc::new(AtomicUsize::new(0)),
                transient_poll_failures: Arc::new(AtomicUsize::new(0)),
                headers: Arc::new(std::sync::Mutex::new(Vec::new())),
                result,
                large_result,
            }
        }
    }

    #[tonic::async_trait]
    impl FlightService for TestSqlService {
        type HandshakeStream = BoxStream<'static, std::result::Result<HandshakeResponse, Status>>;
        type ListFlightsStream = BoxStream<'static, std::result::Result<FlightInfo, Status>>;
        type DoGetStream = BoxStream<'static, std::result::Result<FlightData, Status>>;
        type DoPutStream = BoxStream<'static, std::result::Result<PutResult, Status>>;
        type DoActionStream = BoxStream<'static, std::result::Result<arrow_flight::Result, Status>>;
        type ListActionsStream = BoxStream<'static, std::result::Result<ActionType, Status>>;
        type DoExchangeStream = BoxStream<'static, std::result::Result<FlightData, Status>>;

        async fn handshake(
            &self,
            _request: Request<Streaming<HandshakeRequest>>,
        ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
            Err(Status::unimplemented("handshake"))
        }

        async fn list_flights(
            &self,
            _request: Request<Criteria>,
        ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
            Err(Status::unimplemented("list_flights"))
        }

        async fn get_flight_info(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> std::result::Result<Response<FlightInfo>, Status> {
            Err(Status::unimplemented("get_flight_info"))
        }

        async fn poll_flight_info(
            &self,
            request: Request<arrow_flight::FlightDescriptor>,
        ) -> std::result::Result<Response<PollInfo>, Status> {
            let metadata = request.metadata();
            let header = |name| {
                metadata
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .unwrap()
                    .to_string()
            };
            self.headers.lock().unwrap().push(CapturedHeaders {
                database: header("database"),
                namespace_path: header("namespace-path"),
                request_id: header("x-request-id"),
                api_key: header("x-api-key"),
                database_prefix: header("x-lancedb-database-prefix"),
            });

            let command = Any::decode(request.get_ref().cmd.as_ref())
                .ok()
                .and_then(|any| any.unpack::<CommandStatementQuery>().ok().flatten());
            let (query, stage) = if let Some(command) = command {
                self.query_count.fetch_add(1, Ordering::SeqCst);
                (command.query, 0_u8)
            } else {
                let continuation = std::str::from_utf8(request.get_ref().cmd.as_ref())
                    .map_err(|_| Status::invalid_argument("invalid continuation"))?;
                let mut parts = continuation.splitn(3, ':');
                if parts.next() != Some("poll") {
                    return Err(Status::invalid_argument("invalid continuation"));
                }
                let stage = parts
                    .next()
                    .and_then(|stage| stage.parse().ok())
                    .ok_or_else(|| Status::invalid_argument("invalid continuation"))?;
                if stage == 1 {
                    self.first_continuation_count.fetch_add(1, Ordering::SeqCst);
                }
                let query = parts
                    .next()
                    .ok_or_else(|| Status::invalid_argument("invalid continuation"))?;
                (query.to_string(), stage)
            };
            if (query == "SELECT slow" || query == "SELECT cancelling") && stage > 0 {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            if query == "SELECT no info" && stage == 1 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if stage == 1
                && (query == "SELECT fail"
                    || (query == "SELECT retry"
                        && self.transient_poll_failures.fetch_add(1, Ordering::SeqCst) == 0))
            {
                return Err(Status::unavailable("transient polling failure"));
            }
            let complete = if query == "SELECT no info" {
                stage >= 2
            } else {
                stage >= 1
            };

            let mut info = FlightInfo::new().with_endpoint(
                FlightEndpoint::new()
                    .with_ticket(Ticket::new(query.clone()))
                    .with_location("grpc://127.0.0.1:1"),
            );
            if query != "SELECT empty" {
                let schema = if query == "SELECT large message" {
                    self.large_result.schema_ref()
                } else {
                    self.result.schema_ref()
                };
                info = info.try_with_schema(schema).unwrap();
            }
            Ok(Response::new(PollInfo {
                info: (query != "SELECT no info" || stage > 0).then_some(info),
                flight_descriptor: (!complete)
                    .then(|| FlightDescriptor::new_cmd(format!("poll:{}:{query}", stage + 1))),
                progress: Some(if complete { 1.0 } else { 0.25 }),
                expiration_time: None,
            }))
        }

        async fn get_schema(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> std::result::Result<Response<SchemaResult>, Status> {
            Err(Status::unimplemented("get_schema"))
        }

        async fn do_get(
            &self,
            request: Request<Ticket>,
        ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
            self.do_get_count.fetch_add(1, Ordering::SeqCst);
            let ticket = request.get_ref().ticket.as_ref();
            let empty = ticket == b"SELECT empty";
            let slow = ticket == b"SELECT slow get";
            let large = ticket == b"SELECT large message";
            let result = if large {
                self.large_result.clone()
            } else {
                self.result.clone()
            };
            let schema = result.schema();
            let input = futures::stream::once(async move {
                if slow {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                (!empty).then_some(Ok(result))
            })
            .filter_map(futures::future::ready);
            let mut encoder = FlightDataEncoderBuilder::new().with_schema(schema);
            if large {
                encoder = encoder.with_max_flight_data_size(8 * 1024 * 1024);
            }
            let stream = encoder.build(input).map_err(Status::from);
            Ok(Response::new(Box::pin(stream)))
        }

        async fn do_put(
            &self,
            _request: Request<Streaming<FlightData>>,
        ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
            Err(Status::unimplemented("do_put"))
        }

        async fn do_action(
            &self,
            request: Request<Action>,
        ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
            if request.get_ref().r#type != "CancelFlightInfo" {
                return Err(Status::invalid_argument("unexpected action"));
            }
            self.cancel_count.fetch_add(1, Ordering::SeqCst);
            let cancel_request = CancelFlightInfoRequest::decode(request.get_ref().body.clone())
                .map_err(|_| Status::invalid_argument("invalid cancellation request"))?;
            let query = cancel_request
                .info
                .and_then(|info| info.endpoint.into_iter().next())
                .and_then(|endpoint| endpoint.ticket)
                .and_then(|ticket| String::from_utf8(ticket.ticket.to_vec()).ok())
                .ok_or_else(|| Status::invalid_argument("cancellation request had no ticket"))?;
            if query == "SELECT cancel race" {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            if query == "SELECT cancel timeout" {
                if self.cancel_timeout_count.fetch_add(1, Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                } else {
                    return Err(Status::not_found("query cancellation completed"));
                }
            }
            if query == "SELECT cancel missing" {
                return Err(Status::not_found("query was not found"));
            }
            if query == "SELECT cancel denied" {
                if self.cancel_denied_count.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(Status::permission_denied("cancellation is not allowed"));
                }
                return Err(Status::not_found("query was not found"));
            }
            let status = if query == "SELECT cancelling" {
                if self
                    .cancelling_response_count
                    .fetch_add(1, Ordering::SeqCst)
                    == 0
                {
                    CancelStatus::Cancelling
                } else {
                    return Err(Status::not_found("query cancellation completed"));
                }
            } else if query == "SELECT cancel race" {
                CancelStatus::NotCancellable
            } else {
                CancelStatus::Cancelled
            };
            let response = arrow_flight::Result {
                body: CancelFlightInfoResult::new(status).encode_to_vec().into(),
            };
            Ok(Response::new(Box::pin(futures::stream::iter([Ok(
                response,
            )]))))
        }

        async fn list_actions(
            &self,
            _request: Request<Empty>,
        ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
            Err(Status::unimplemented("list_actions"))
        }

        async fn do_exchange(
            &self,
            _request: Request<Streaming<FlightData>>,
        ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
            Err(Status::unimplemented("do_exchange"))
        }
    }

    #[tokio::test]
    async fn submits_polls_fetches_cancels_and_reuses_client() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let service = TestSqlService::default();
        let query_count = service.query_count.clone();
        let do_get_count = service.do_get_count.clone();
        let cancel_count = service.cancel_count.clone();
        let first_continuation_count = service.first_continuation_count.clone();
        let headers = service.headers.clone();
        let expected = service.result.clone();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_shutdown(address, async {
                    let _ = shutdown_rx.await;
                }),
        );
        let mut ready = false;
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(address).await.is_ok() {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(ready, "SQL test server did not start");

        let mut client_config = ClientConfig::default();
        client_config.retry_config.read_retries = Some(1);
        client_config.retry_config.backoff_factor = Some(0.0);
        client_config.retry_config.backoff_jitter = Some(0.0);
        client_config
            .extra_headers
            .insert("x-static-secret".to_string(), "static-secret".to_string());
        let header_provider = Arc::new(DelayedHeaderProvider::default());
        client_config.header_provider = Some(header_provider.clone());
        let client = SqlClient::new(
            "analytics".to_string(),
            Some("tenant/production".to_string()),
            "test-key".to_string(),
            None,
            Some(format!("grpc://{address}")),
            client_config,
        );
        assert_eq!(client.initialized_client_count().await, 0);
        assert!(!format!("{client:?}").contains("test-key"));
        assert!(!format!("{client:?}").contains("static-secret"));

        let mut timeout_client_config = ClientConfig::default();
        timeout_client_config.timeout_config.timeout = Some(Duration::from_millis(50));
        let timeout_header_provider = Arc::new(DelayedHeaderProvider::default());
        timeout_client_config.header_provider = Some(timeout_header_provider.clone());
        let timeout_client = SqlClient::new(
            "analytics".to_string(),
            Some("tenant/production".to_string()),
            "test-key".to_string(),
            None,
            Some(format!("grpc://{address}")),
            timeout_client_config,
        );
        timeout_header_provider
            .delay_next
            .store(true, Ordering::SeqCst);
        assert_overall_timeout(
            timeout_client
                .submit("SELECT overall timeout", &["public".to_string()])
                .await,
            "submission",
        );
        let timeout_query = timeout_client
            .submit("SELECT overall timeout", &["public".to_string()])
            .await
            .unwrap();
        timeout_header_provider
            .delay_next
            .store(true, Ordering::SeqCst);
        assert_overall_timeout(
            timeout_client.describe(timeout_query.id()).await,
            "description",
        );
        timeout_header_provider
            .delay_next
            .store(true, Ordering::SeqCst);
        assert_overall_timeout(timeout_query.result().await, "result");
        timeout_header_provider
            .delay_next
            .store(true, Ordering::SeqCst);
        assert_overall_timeout(timeout_query.cancel().await, "cancellation");
        timeout_query.cancel().await.unwrap();

        let pre_dispatch_timeout = timeout_client
            .submit("SELECT cancel missing", &["public".to_string()])
            .await
            .unwrap();
        timeout_header_provider
            .delay_next
            .store(true, Ordering::SeqCst);
        assert_overall_timeout(pre_dispatch_timeout.cancel().await, "cancellation");
        assert!(pre_dispatch_timeout.cancel().await.is_err());
        assert_ne!(
            pre_dispatch_timeout.describe().await.unwrap().status,
            "cancelled"
        );

        let rejected_cancel = timeout_client
            .submit("SELECT cancel denied", &["public".to_string()])
            .await
            .unwrap();
        assert!(rejected_cancel.cancel().await.is_err());
        assert!(rejected_cancel.cancel().await.is_err());
        assert_ne!(
            rejected_cancel.describe().await.unwrap().status,
            "cancelled"
        );

        let uncertain_cancel = timeout_client
            .submit("SELECT cancel timeout", &["public".to_string()])
            .await
            .unwrap();
        assert_overall_timeout(uncertain_cancel.cancel().await, "cancellation");
        uncertain_cancel.cancel().await.unwrap();
        assert_eq!(
            uncertain_cancel.describe().await.unwrap().status,
            "cancelled"
        );
        assert!(matches!(
            uncertain_cancel.result().await,
            Err(Error::JobCancelled { .. })
        ));

        let first = client
            .submit("SELECT 'super-secret'", &["public".to_string()])
            .await
            .unwrap();
        let id_suffix = first.id().strip_prefix(QUERY_ID_PREFIX).unwrap();
        assert_eq!(id_suffix.len(), 32);
        assert!(id_suffix.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(!first.id().contains("super-secret"));
        header_provider.delay_next.store(true, Ordering::SeqCst);
        let describe_started = Instant::now();
        let first_description = client.describe(first.id()).await.unwrap();
        assert!(describe_started.elapsed() >= Duration::from_millis(1_100));
        assert_eq!(first_description.status, "finished");
        assert_eq!(first_description.progress, Some(1.0));
        let first_result = first.result().await.unwrap();
        let first_result_again = first.result().await.unwrap();

        let staged = client
            .submit("SELECT no info", &["public".to_string()])
            .await
            .unwrap();
        let staged_running = client.describe(staged.id()).await.unwrap();
        assert_eq!(staged_running.status, "running");
        let staged_finished = client.describe(staged.id()).await.unwrap();
        assert_eq!(staged_finished.status, "finished");

        let empty = client
            .submit("SELECT empty", &["public".to_string()])
            .await
            .unwrap();
        let empty_result = empty.result().await.unwrap();

        let large = client
            .submit("SELECT large message", &["public".to_string()])
            .await
            .unwrap();
        let large_result = large.result().await.unwrap();
        assert_eq!(large_result.len(), 1);
        assert_eq!(large_result[0].num_rows(), 1);
        assert_eq!(
            large_result[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .len(),
            5 * 1024 * 1024,
        );

        let cancelled = client
            .submit(
                "SELECT cancelled",
                &["events".to_string(), "raw".to_string()],
            )
            .await
            .unwrap();
        cancelled.cancel().await.unwrap();
        assert_eq!(cancelled.describe().await.unwrap().status, "cancelled");
        assert!(matches!(
            cancelled.result().await,
            Err(Error::JobCancelled { .. })
        ));

        let slow = Arc::new(
            client
                .submit("SELECT slow", &["public".to_string()])
                .await
                .unwrap(),
        );
        let result_task = {
            let slow = slow.clone();
            tokio::spawn(async move { slow.result().await })
        };
        tokio::time::sleep(Duration::from_millis(25)).await;
        tokio::time::timeout(Duration::from_millis(150), slow.cancel())
            .await
            .expect("cancellation must not wait for result polling")
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(150), result_task)
                .await
                .expect("cancellation must wake result polling")
                .unwrap(),
            Err(Error::JobCancelled { .. })
        ));
        let cancel_count_after_slow = cancel_count.load(Ordering::SeqCst);
        slow.cancel().await.unwrap();
        assert_eq!(
            cancel_count.load(Ordering::SeqCst),
            cancel_count_after_slow,
            "a confirmed cancellation must not be sent again",
        );

        let slow_get = Arc::new(
            client
                .submit("SELECT slow get", &["public".to_string()])
                .await
                .unwrap(),
        );
        let do_get_count_before_slow = do_get_count.load(Ordering::SeqCst);
        let slow_get_result_task = {
            let slow_get = slow_get.clone();
            tokio::spawn(async move { slow_get.result().await })
        };
        while do_get_count.load(Ordering::SeqCst) == do_get_count_before_slow {
            tokio::task::yield_now().await;
        }
        slow_get.cancel().await.unwrap();
        assert_eq!(slow_get.describe().await.unwrap().status, "cancelled");
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(150), slow_get_result_task)
                .await
                .expect("cancellation must wake result fetching")
                .unwrap(),
            Err(Error::JobCancelled { .. })
        ));
        assert!(matches!(
            slow_get.result().await,
            Err(Error::JobCancelled { .. })
        ));

        let restored = Arc::new(
            RemoteQuery::new(
                "restored".to_string(),
                client.inner.clone(),
                vec!["public".to_string()],
                PollInfo {
                    flight_descriptor: Some(FlightDescriptor::new_cmd("restored")),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let mut restored_waiter = {
            let restored = restored.clone();
            tokio::spawn(async move { restored.wait_for_cancellation().await })
        };
        tokio::task::yield_now().await;
        restored.mark_cancelling();
        restored.restore_after_rejected_cancellation().await;
        assert_eq!(restored.lifecycle(), QueryLifecycle::Running);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut restored_waiter)
                .await
                .is_err(),
            "a stale cancellation notification must not complete the waiter",
        );
        restored.mark_cancelling();
        tokio::time::timeout(Duration::from_millis(100), restored_waiter)
            .await
            .expect("a current cancellation must complete the waiter")
            .unwrap();

        let cancelling = Arc::new(
            client
                .submit("SELECT cancelling", &["public".to_string()])
                .await
                .unwrap(),
        );
        let cancelling_result_task = {
            let cancelling = cancelling.clone();
            tokio::spawn(async move { cancelling.result().await })
        };
        tokio::time::sleep(Duration::from_millis(25)).await;
        cancelling.cancel().await.unwrap();
        assert_eq!(cancelling.describe().await.unwrap().status, "cancelling");
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(150), cancelling_result_task)
                .await
                .expect("an accepted cancellation must wake result polling")
                .unwrap(),
            Err(Error::JobCancelled { .. })
        ));
        cancelling.cancel().await.unwrap();
        assert_eq!(cancelling.describe().await.unwrap().status, "cancelled");

        let cancel_race = Arc::new(
            client
                .submit("SELECT cancel race", &["public".to_string()])
                .await
                .unwrap(),
        );
        let cancel_count_before_race = cancel_count.load(Ordering::SeqCst);
        let cancel_race_task = {
            let cancel_race = cancel_race.clone();
            tokio::spawn(async move { cancel_race.cancel().await })
        };
        while cancel_count.load(Ordering::SeqCst) == cancel_count_before_race {
            tokio::task::yield_now().await;
        }
        let cancel_race_result = cancel_race.result().await.unwrap();
        tokio::time::timeout(Duration::from_millis(500), cancel_race_task)
            .await
            .expect("completed result must make in-flight cancellation a no-op")
            .unwrap()
            .unwrap();
        assert_eq!(cancel_race.describe().await.unwrap().status, "finished");
        assert_eq!(cancel_race.result().await.unwrap(), cancel_race_result);

        let no_info = Arc::new(
            client
                .submit("SELECT no info", &["public".to_string()])
                .await
                .unwrap(),
        );
        let continuation_count_before = first_continuation_count.load(Ordering::SeqCst);
        let no_info_result_task = {
            let no_info = no_info.clone();
            tokio::spawn(async move { no_info.result().await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        tokio::time::timeout(Duration::from_secs(1), no_info.cancel())
            .await
            .expect("cancellation should wait for cancellable query information")
            .unwrap();
        assert!(matches!(
            no_info_result_task.await.unwrap(),
            Err(Error::JobCancelled { .. })
        ));
        assert_eq!(
            first_continuation_count.load(Ordering::SeqCst),
            continuation_count_before + 1,
            "result and cancel must share one continuation poll",
        );

        let retried = client
            .submit("SELECT retry", &["public".to_string()])
            .await
            .unwrap();
        assert_eq!(retried.result().await.unwrap(), vec![expected.clone()]);

        let failed = client
            .submit("SELECT fail", &["public".to_string()])
            .await
            .unwrap();
        assert!(failed.result().await.is_err());

        let active_registry = QueryRegistry::new(1);
        let active_permit = active_registry.reserve().unwrap();
        let active_query = Arc::new(
            RemoteQuery::new(
                "active".to_string(),
                client.inner.clone(),
                vec!["public".to_string()],
                PollInfo {
                    flight_descriptor: Some(FlightDescriptor::new_cmd("active")),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        active_registry.insert("active".to_string(), active_query.clone(), active_permit);
        assert!(active_registry.reserve().is_err());
        assert!(Arc::ptr_eq(
            &active_registry.get("active").unwrap(),
            &active_query,
        ));

        let expired_registry = QueryRegistry::new(1);
        let expired_permit = expired_registry.reserve().unwrap();
        let expired_query = Arc::new(
            RemoteQuery::new(
                "expired".to_string(),
                client.inner.clone(),
                vec!["public".to_string()],
                PollInfo {
                    flight_descriptor: Some(FlightDescriptor::new_cmd("expired")),
                    expiration_time: Some(Default::default()),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        expired_registry.insert("expired".to_string(), expired_query, expired_permit);
        assert!(expired_registry.reserve().is_ok());
        assert!(expired_registry.get("expired").is_none());

        let terminal_registry = QueryRegistry::new(1);
        let terminal_permit = terminal_registry.reserve().unwrap();
        let terminal_query = Arc::new(
            RemoteQuery::new(
                "terminal".to_string(),
                client.inner.clone(),
                vec!["public".to_string()],
                PollInfo {
                    flight_descriptor: None,
                    expiration_time: Some(Default::default()),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        terminal_registry.insert("terminal".to_string(), terminal_query, terminal_permit);
        assert!(terminal_registry.get("terminal").is_some());
        assert!(terminal_registry.reserve().is_ok());
        assert!(terminal_registry.get("terminal").is_none());

        let transitioned_registry = QueryRegistry::new(1);
        let transitioned_permit = transitioned_registry.reserve().unwrap();
        let transition_descriptor = FlightDescriptor::new_cmd("transitioned");
        let transitioned_query = Arc::new(
            RemoteQuery::new(
                "transitioned".to_string(),
                client.inner.clone(),
                vec!["public".to_string()],
                PollInfo {
                    flight_descriptor: Some(transition_descriptor.clone()),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        transitioned_query
            .update_state(
                &transition_descriptor,
                PollInfo {
                    flight_descriptor: None,
                    expiration_time: Some(Default::default()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        transitioned_registry.insert(
            "transitioned".to_string(),
            transitioned_query,
            transitioned_permit,
        );
        assert!(transitioned_registry.get("transitioned").is_some());

        let abandoned_registry = QueryRegistry::new(1);
        let abandoned_permit = abandoned_registry.reserve().unwrap();
        let abandoned_query = Arc::new(
            RemoteQuery::new(
                "abandoned".to_string(),
                client.inner.clone(),
                vec!["public".to_string()],
                PollInfo {
                    flight_descriptor: Some(FlightDescriptor::new_cmd("abandoned")),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        abandoned_registry.insert(
            "abandoned".to_string(),
            abandoned_query.clone(),
            abandoned_permit,
        );
        drop(abandoned_query);
        assert!(abandoned_registry.reserve().is_ok());
        assert!(abandoned_registry.get("abandoned").is_none());

        let stale_registry = QueryRegistry::new(1);
        let stale_permit = stale_registry.reserve().unwrap();
        let stale_query = Arc::new(
            RemoteQuery::new(
                "stale".to_string(),
                client.inner.clone(),
                vec!["public".to_string()],
                PollInfo {
                    flight_descriptor: Some(FlightDescriptor::new_cmd("stale")),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        *stale_query.last_accessed.lock().unwrap() = Instant::now() - ABANDONED_QUERY_RETENTION;
        stale_registry.insert("stale".to_string(), stale_query.clone(), stale_permit);
        drop(stale_query);
        assert!(stale_registry.get("stale").is_none());

        let concurrent_registry = Arc::new(QueryRegistry::new(2));
        for id in ["terminal-one", "terminal-two"] {
            let permit = concurrent_registry.reserve().unwrap();
            let query = Arc::new(
                RemoteQuery::new(
                    id.to_string(),
                    client.inner.clone(),
                    vec!["public".to_string()],
                    PollInfo::default(),
                )
                .unwrap(),
            );
            concurrent_registry.insert(id.to_string(), query, permit);
        }
        let reservation_barrier = Arc::new(std::sync::Barrier::new(3));
        let reserve = |registry: Arc<QueryRegistry>, barrier: Arc<std::sync::Barrier>| {
            tokio::task::spawn_blocking(move || {
                barrier.wait();
                registry.reserve()
            })
        };
        let reservation_one = reserve(concurrent_registry.clone(), reservation_barrier.clone());
        let reservation_two = reserve(concurrent_registry.clone(), reservation_barrier.clone());
        reservation_barrier.wait();
        let (permit_one, permit_two) = tokio::join!(reservation_one, reservation_two);
        let permit_one = permit_one.unwrap().unwrap();
        let permit_two = permit_two.unwrap().unwrap();
        assert_eq!(concurrent_registry.capacity.available_permits(), 0);
        drop((permit_one, permit_two));
        assert_eq!(concurrent_registry.capacity.available_permits(), 2);

        assert_eq!(client.initialized_client_count().await, 1);
        assert_eq!(query_count.load(Ordering::SeqCst), 16);
        assert_eq!(do_get_count.load(Ordering::SeqCst), 6);
        assert_eq!(cancel_count.load(Ordering::SeqCst), 13);
        assert_eq!(first_result, vec![expected.clone()]);
        assert_eq!(first_result_again, vec![expected.clone()]);
        assert_eq!(empty_result.len(), 1);
        assert_eq!(empty_result[0].schema(), expected.schema());
        assert_eq!(empty_result[0].num_rows(), 0);
        assert!(client.describe("invalid").await.is_err());
        {
            let headers = headers.lock().unwrap();
            assert_eq!(headers[0].database, "analytics");
            assert_eq!(headers[0].namespace_path, "public");
            assert_eq!(headers[0].api_key, "test-key");
            assert_eq!(headers[0].database_prefix, "tenant/production");
            assert!(
                headers
                    .iter()
                    .any(|header| header.namespace_path == "events$raw")
            );
            assert!(
                headers
                    .windows(2)
                    .all(|headers| headers[0].request_id != headers[1].request_id)
            );
        }
        let _ = shutdown_tx.send(());
        server.await.unwrap().unwrap();
    }

    #[test]
    fn normalizes_supported_uris() {
        assert_eq!(
            normalize_sql_host_override("grpc://localhost").unwrap(),
            SqlTarget {
                uri: "http://localhost:10025".to_string(),
                tls: false,
            }
        );
        assert_eq!(
            normalize_sql_host_override("grpcs://example.com").unwrap(),
            SqlTarget {
                uri: "https://example.com:10026".to_string(),
                tls: true,
            }
        );
        assert_eq!(
            normalize_sql_host_override("grpc://[::1]:10025").unwrap(),
            SqlTarget {
                uri: "http://[::1]:10025".to_string(),
                tls: false,
            }
        );
        assert_eq!(
            normalize_sql_host_override("https://example.com:443").unwrap(),
            SqlTarget {
                uri: "https://example.com:443".to_string(),
                tls: true,
            }
        );
    }

    #[test]
    fn derives_plaintext_endpoint_from_host_override() {
        assert_eq!(
            resolve_sql_host_override(Some("http://localhost:10024"), None).unwrap(),
            SqlTarget {
                uri: "http://localhost:10025".to_string(),
                tls: false,
            }
        );
        assert_eq!(
            resolve_sql_host_override(Some("http://localhost:80"), None).unwrap(),
            SqlTarget {
                uri: "http://localhost:81".to_string(),
                tls: false,
            }
        );
    }

    #[test]
    fn rejects_unsafe_or_ambiguous_endpoints() {
        assert!(normalize_sql_host_override("ftp://localhost").is_err());
        assert!(normalize_sql_host_override("grpc://user@localhost").is_err());
        assert!(normalize_sql_host_override("grpc://localhost/path").is_err());
        assert!(resolve_sql_host_override(Some("https://localhost"), None).is_err());
    }

    #[test]
    fn validates_namespace_components() {
        assert!(validate_namespace_path(&[]).is_ok());
        assert!(validate_namespace_path(&["events".into(), "raw".into()]).is_ok());
        assert!(validate_namespace_path(&["events$raw".into()]).is_err());
        assert!(validate_namespace_path(&["".into()]).is_err());
        assert!(validate_namespace_path(&["café".into()]).is_err());
    }
}
