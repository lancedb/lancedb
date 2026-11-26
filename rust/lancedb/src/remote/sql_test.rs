// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::atomic::AtomicUsize;

use arrow_array::builder::{ListBuilder, StringBuilder, StringDictionaryBuilder};
use arrow_array::types::{Float32Type, Int32Type};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, Float32Array, Int64Array, StringArray};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::{Any, CommandStatementQuery};
use arrow_flight::{
    Action, ActionType, CancelFlightInfoResult, Criteria, Empty, FlightData, FlightEndpoint,
    FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_schema::{DataType, Field, Schema};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status, Streaming};

use super::stream::{PROTOCOL_HEADER, PROTOCOL_VERSION, QueryResponse};
use super::*;
use crate::Connection;
use crate::database::Database;
use crate::embeddings::MemoryRegistry;
use crate::materialized_view::CreateMaterializedViewRequest;
use crate::remote::RemoteCatalogOptions;
use crate::remote::client::HeaderProvider;
use crate::remote::client::test_utils::client_with_handler;
use crate::remote::db::RemoteDatabase;
use crate::remote::table::RemoteTable;
use crate::storage_integration::{CreateStorageIntegrationRequest, SecretReference};
use crate::table::BaseTable;

#[test]
fn server_info_survives_flight_error_and_job_wrapping() {
    for with_job in [false, true] {
        let mut status = tonic::Status::invalid_argument("invalid query");
        status
            .metadata_mut()
            .insert("x-lancedb-error-code", "invalid_query".parse().unwrap());
        let build = serde_json::json!({
            "release": "1.2.3", "git_sha": "0123456789ab", "build_date": "2024-01-01T00:00:00Z"
        })
        .to_string();
        status.metadata_mut().insert_bin(
            "x-lancedb-server-build-bin",
            tonic::metadata::MetadataValue::from_bytes(build.as_bytes()),
        );
        if with_job {
            status
                .metadata_mut()
                .insert(SQL_JOB_ID_METADATA_KEY, "job-original".parse().unwrap());
        }
        let error = sql_status_error("request-original", &status);
        let text = error.to_string();
        assert!(text.contains("invalid query"), "{text}");
        assert!(text.contains("1.2.3"), "{text}");
        assert!(text.contains("0123456789ab"), "{text}");
        assert!(text.contains("2024-01-01T00:00:00Z"), "{text}");
        assert_eq!(
            error.remote_error_metadata().unwrap().error_code.as_deref(),
            Some("invalid_query")
        );
        match error {
            Error::JobFailed { job_id, .. } if with_job => {
                assert_eq!(job_id.as_deref(), Some("job-original"))
            }
            Error::Sql {
                grpc_code,
                request_id,
                ..
            } if !with_job => {
                assert_eq!(grpc_code, tonic::Code::InvalidArgument as i32);
                assert_eq!(request_id, "request-original");
            }
            other => panic!("unexpected error classification: {other}"),
        }
    }
}

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

async fn collect_result(query: &Query) -> Result<Vec<RecordBatch>> {
    query.reader().await?.try_collect().await
}

#[derive(Debug)]
struct CapturedHeaders {
    database: String,
    namespace_path: String,
    request_id: String,
    api_key: String,
    database_prefix: String,
    materialized_view_job_handle: Option<String>,
    grpc_timeout: Option<String>,
}

/// Every exchange's statement and the parameter row it arrived with.
type ReceivedExchanges = Arc<std::sync::Mutex<Vec<(String, Option<RecordBatch>)>>>;

#[derive(Clone)]
struct TestSqlService {
    adaptive: bool,
    defer: bool,
    invalid_decision: bool,
    query_count: Arc<AtomicUsize>,
    statements: Arc<std::sync::Mutex<Vec<String>>>,
    do_get_count: Arc<AtomicUsize>,
    cancel_count: Arc<AtomicUsize>,
    cancel_denied_count: Arc<AtomicUsize>,
    cancel_timeout_count: Arc<AtomicUsize>,
    cancel_unspecified_count: Arc<AtomicUsize>,
    cancelling_response_count: Arc<AtomicUsize>,
    incremental_finished: Arc<AtomicBool>,
    first_continuation_count: Arc<AtomicUsize>,
    transient_poll_failures: Arc<AtomicUsize>,
    headers: Arc<std::sync::Mutex<Vec<CapturedHeaders>>>,
    result_deadlines: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    gate_refresh_release: Arc<Notify>,
    exchanges: ReceivedExchanges,
    result_overrides: Arc<std::sync::Mutex<HashMap<String, RecordBatch>>>,
    materialized_view_job_handle_supported: bool,
    optimize_error: Option<Status>,
    prewarm_error: Option<Status>,
    prewarm_azure_supported: bool,
    /// How many exchange responses have been dropped, finished or not.
    exchange_drops: Arc<AtomicUsize>,
    result: RecordBatch,
    result_tail: Vec<std::result::Result<RecordBatch, Status>>,
    result_frames: Option<Vec<FlightData>>,
    large_result: RecordBatch,
    dictionary_result: RecordBatch,
}

/// Counts a response stream being dropped, which is how a server sees a call
/// end -- by the client cancelling it or by the rows running out.
struct DropSignal(Arc<AtomicUsize>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl TestSqlService {
    fn capture_headers(&self, metadata: &MetadataMap) {
        let header = |name| {
            metadata
                .get(name)
                .and_then(|value| value.to_str().ok())
                .unwrap()
                .to_string()
        };
        self.headers.lock().unwrap().push(CapturedHeaders {
            grpc_timeout: metadata
                .get("grpc-timeout")
                .map(|value| value.to_str().unwrap().to_owned()),
            database: header("database"),
            namespace_path: header("namespace-path"),
            request_id: header("x-request-id"),
            api_key: header("x-api-key"),
            database_prefix: metadata
                .get("x-lancedb-database-prefix")
                .map(|value| value.to_str().unwrap().to_string())
                .unwrap_or_default(),
            materialized_view_job_handle: metadata
                .get("x-lancedb-mv-job-handle")
                .map(|value| value.to_str().unwrap().to_string()),
        });
    }

    fn result_for_query(&self, query: &str) -> RecordBatch {
        if query == "SELECT large message" {
            self.large_result.clone()
        } else if query == "SELECT dictionary" {
            self.dictionary_result.clone()
        } else {
            self.result_overrides
                .lock()
                .unwrap()
                .get(query)
                .cloned()
                .unwrap_or_else(|| self.result.clone())
        }
    }

    fn adaptive_response(
        &self,
        query: &str,
        rows: BoxStream<'static, std::result::Result<FlightData, Status>>,
    ) -> Response<BoxStream<'static, std::result::Result<FlightData, Status>>> {
        if !self.adaptive {
            return Response::new(rows);
        }
        let poll_info = self.defer.then(|| {
            let mut info = FlightInfo::new();
            if !query.starts_with("SELECT durable job") {
                info = info.with_endpoint(
                    FlightEndpoint::new().with_ticket(Ticket::new(query.to_string())),
                );
            }
            if query.starts_with("SELECT durable job") {
                info = info
                    .try_with_schema(&self.result_for_query(query).schema())
                    .unwrap();
            }
            if self.materialized_view_job_handle_supported {
                info.app_metadata = b"early-v1".to_vec().into();
            }
            PollInfo {
                info: Some(info),
                flight_descriptor: Some(FlightDescriptor::new_cmd(format!("poll:1:{query}"))),
                progress: Some(0.25),
                expiration_time: None,
            }
        });
        let first = FlightData {
            app_metadata: if self.invalid_decision {
                b"invalid protobuf".to_vec().into()
            } else {
                Any {
                    type_url: "type.googleapis.com/lancedb.sql.QueryResponse".to_string(),
                    value: QueryResponse { poll_info }.encode_to_vec().into(),
                }
                .encode_to_vec()
                .into()
            },
            ..Default::default()
        };
        let rest = if self.defer {
            futures::stream::empty().boxed()
        } else {
            rows
        };
        let mut response = Response::new(
            futures::stream::once(async { Ok(first) })
                .chain(rest)
                .boxed(),
        );
        response
            .metadata_mut()
            .insert(PROTOCOL_HEADER, PROTOCOL_VERSION.parse().unwrap());
        if query.starts_with("SELECT direct durable job") {
            let job_id = query.rsplit(' ').next().unwrap();
            response.metadata_mut().insert(
                SQL_JOB_ID_METADATA_KEY,
                format!("job-{job_id}").parse().unwrap(),
            );
        }
        response
    }
}

impl Default for TestSqlService {
    fn default() -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let result =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42_i64]))]).unwrap();
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
        let mut dictionary_builder = StringDictionaryBuilder::<Int32Type>::new();
        dictionary_builder.append("dictionary value").unwrap();
        let dictionary = dictionary_builder.finish();
        let dictionary_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            dictionary.data_type().clone(),
            false,
        )]));
        let dictionary_result =
            RecordBatch::try_new(dictionary_schema, vec![Arc::new(dictionary)]).unwrap();
        Self {
            adaptive: false,
            defer: false,
            invalid_decision: false,
            query_count: Arc::new(AtomicUsize::new(0)),
            statements: Arc::new(std::sync::Mutex::new(Vec::new())),
            do_get_count: Arc::new(AtomicUsize::new(0)),
            cancel_count: Arc::new(AtomicUsize::new(0)),
            cancel_denied_count: Arc::new(AtomicUsize::new(0)),
            cancel_timeout_count: Arc::new(AtomicUsize::new(0)),
            cancel_unspecified_count: Arc::new(AtomicUsize::new(0)),
            cancelling_response_count: Arc::new(AtomicUsize::new(0)),
            incremental_finished: Arc::new(AtomicBool::new(false)),
            first_continuation_count: Arc::new(AtomicUsize::new(0)),
            transient_poll_failures: Arc::new(AtomicUsize::new(0)),
            headers: Arc::new(std::sync::Mutex::new(Vec::new())),
            result_deadlines: Arc::new(std::sync::Mutex::new(Vec::new())),
            gate_refresh_release: Arc::new(Notify::new()),
            exchanges: Arc::new(std::sync::Mutex::new(Vec::new())),
            result_overrides: Arc::new(std::sync::Mutex::new(HashMap::new())),
            materialized_view_job_handle_supported: false,
            optimize_error: None,
            prewarm_error: None,
            prewarm_azure_supported: false,
            exchange_drops: Arc::new(AtomicUsize::new(0)),
            result,
            result_tail: vec![],
            result_frames: None,
            large_result,
            dictionary_result,
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
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        self.capture_headers(request.metadata());
        assert!(
            request
                .metadata()
                .get("x-azure-storage-account-name")
                .is_some()
        );
        let schema = if self.prewarm_azure_supported {
            self.result
                .schema()
                .as_ref()
                .clone()
                .with_metadata(HashMap::from([(
                    "lancedb.prewarm.azure_storage_account_name".into(),
                    "true".into(),
                )]))
        } else {
            self.result.schema().as_ref().clone()
        };
        Ok(Response::new(
            FlightInfo::new().try_with_schema(&schema).unwrap(),
        ))
    }

    async fn poll_flight_info(
        &self,
        request: Request<arrow_flight::FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        self.capture_headers(request.metadata());

        let command = Any::decode(request.get_ref().cmd.as_ref())
            .ok()
            .and_then(|any| any.unpack::<CommandStatementQuery>().ok().flatten());
        let (query, stage) = if let Some(command) = command {
            self.query_count.fetch_add(1, Ordering::SeqCst);
            self.statements.lock().unwrap().push(command.query.clone());
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
        if query == "SELECT rejected" {
            return Err(Status::invalid_argument("invalid statement"));
        }
        if (query == "SELECT slow" || query == "SELECT cancelling") && stage > 0 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        if query == "SELECT no info" && stage == 1 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if query == "SELECT incremental" && stage == 1 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            self.incremental_finished.store(true, Ordering::SeqCst);
        }
        if stage == 1
            && (query == "SELECT fail"
                || (query == "SELECT retry"
                    && self.transient_poll_failures.fetch_add(1, Ordering::SeqCst) == 0))
        {
            return Err(Status::unavailable("transient polling failure"));
        }
        if query == "SELECT function failure" && stage == 1 {
            let mut status = Status::failed_precondition("Function 'frames' raised ValueError");
            status
                .metadata_mut()
                .insert("x-lancedb-sql-job-id", "job-1".parse().unwrap());
            return Err(status);
        }
        if query == "SELECT durable job failure" && stage == 1 {
            let mut status = Status::internal("durable SQL job failed");
            status
                .metadata_mut()
                .insert("x-lancedb-sql-job-id", "job-1".parse().unwrap());
            return Err(status);
        }
        let complete = if query == "SELECT no info" || query.starts_with("SELECT durable job") {
            stage >= 2
        } else {
            stage >= 1
        };

        let first_ticket = if query == "SELECT incremental" {
            format!("{query}:first")
        } else {
            query.clone()
        };
        let mut info = FlightInfo::new();
        if !(query.starts_with("SELECT durable job") && !complete) {
            info = info.with_endpoint(
                FlightEndpoint::new()
                    .with_ticket(Ticket::new(first_ticket))
                    .with_location("grpc://127.0.0.1:1"),
            );
        }
        if query == "SELECT incremental" && stage > 0 {
            info = info.with_endpoint(
                FlightEndpoint::new()
                    .with_ticket(Ticket::new(format!("{query}:second")))
                    .with_location("grpc://127.0.0.1:1"),
            );
        }
        if query != "SELECT empty" {
            let schema = self.result_for_query(&query).schema();
            info = info.try_with_schema(&schema).unwrap();
        }
        let mut response = Response::new(PollInfo {
            info: (query != "SELECT no info" || stage > 0).then_some(info),
            flight_descriptor: (!complete)
                .then(|| FlightDescriptor::new_cmd(format!("poll:{}:{query}", stage + 1))),
            progress: Some(if complete { 1.0 } else { 0.25 }),
            expiration_time: None,
        });
        if query.starts_with("SELECT durable job") && stage > 0 {
            let job_id = query.rsplit(' ').next().unwrap();
            response.metadata_mut().insert(
                SQL_JOB_ID_METADATA_KEY,
                format!("job-{job_id}").parse().unwrap(),
            );
        }
        Ok(response)
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
        let initial = Any::decode(request.get_ref().ticket.clone())
            .ok()
            .and_then(|envelope| Any::decode(envelope.value).ok())
            .and_then(|command| command.unpack::<CommandStatementQuery>().ok().flatten());
        if let Some(command) = &initial {
            self.statements.lock().unwrap().push(command.query.clone());
            if command.query.starts_with("OPTIMIZE ")
                && let Some(error) = &self.optimize_error
            {
                return Err(error.clone());
            }
            if (command.query.starts_with("PREWARM")
                || command.query.starts_with("EXPLAIN (FORMAT JSON) PREWARM"))
                && let Some(error) = &self.prewarm_error
            {
                return Err(error.clone());
            }
            if command.query == "SELECT rejected" {
                return Err(Status::invalid_argument("invalid statement"));
            }
            self.capture_headers(request.metadata());
            assert_eq!(
                request.metadata().get(PROTOCOL_HEADER).unwrap(),
                PROTOCOL_VERSION
            );
        }
        if initial.is_none() {
            self.result_deadlines.lock().unwrap().push(
                request
                    .metadata()
                    .get("grpc-timeout")
                    .map(|value| value.to_str().unwrap().to_owned()),
            );
        }
        let query = initial
            .as_ref()
            .map(|command| command.query.as_bytes())
            .unwrap_or(request.get_ref().ticket.as_ref());
        let ticket = query;
        let gate_refresh = ticket == b"REFRESH MATERIALIZED VIEW \"cancel_view\""
            || ticket == b"SELECT job cancel denied";
        let empty = ticket == b"SELECT empty";
        let slow = ticket == b"SELECT slow get";
        let large = ticket == b"SELECT large message";
        let result = self.result_for_query(std::str::from_utf8(ticket).unwrap_or_default());
        let schema = result.schema();
        let gate_refresh_release = self.gate_refresh_release.clone();
        let input = futures::stream::once(async move {
            if gate_refresh {
                gate_refresh_release.notified().await;
            }
            if slow {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            (!empty).then_some(Ok(result))
        })
        .filter_map(futures::future::ready)
        .chain(
            futures::stream::iter(self.result_tail.clone()).then(|item| async {
                // Let tonic flush the preceding batch before sending an error trailer.
                if item.is_err() {
                    tokio::task::yield_now().await;
                }
                item.map_err(|status| FlightError::Tonic(Box::new(status)))
            }),
        );
        let mut encoder = FlightDataEncoderBuilder::new().with_schema(schema);
        if large {
            encoder = encoder.with_max_flight_data_size(8 * 1024 * 1024);
        }
        let stream = match &self.result_frames {
            Some(frames) => futures::stream::iter(frames.clone()).map(Ok).boxed(),
            None => encoder.build(input).map_err(Status::from).boxed(),
        };
        if let Some(initial) = initial {
            Ok(self.adaptive_response(&initial.query, stream))
        } else {
            Ok(Response::new(stream))
        }
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
        if query == "SELECT cancel unspecified"
            && self.cancel_unspecified_count.fetch_add(1, Ordering::SeqCst) > 0
        {
            return Err(Status::not_found("query cancellation completed"));
        }
        let status = if query == "SELECT job cancel denied" {
            CancelStatus::NotCancellable
        } else if query == "SELECT cancel unspecified" {
            CancelStatus::Unspecified
        } else if query == "SELECT cancelling" {
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

    /// Answers with the parameter row it was sent, so a test can compare what
    /// came back with what went out.
    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        self.capture_headers(request.metadata());
        let mut messages = request.into_inner();
        let first = messages
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty exchange"))?;
        let query = first
            .flight_descriptor
            .as_ref()
            .and_then(|descriptor| Any::decode(descriptor.cmd.as_ref()).ok())
            .and_then(|any| any.unpack::<CommandStatementQuery>().ok().flatten())
            .map(|command| command.query)
            .ok_or_else(|| Status::invalid_argument("exchange without a statement"))?;
        let data = futures::stream::once(async move { Ok(first) })
            .chain(messages)
            .map_err(|status| FlightError::Tonic(Box::new(status)));
        let parameters = FlightRecordBatchStream::new_from_flight_data(data)
            .try_next()
            .await
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.exchanges
            .lock()
            .unwrap()
            .push((query.clone(), parameters.clone()));
        if query == "SELECT exchange refused" {
            return Err(Status::invalid_argument(
                "parameter 1 is not referenced by the statement",
            ));
        }
        if query == "SELECT exchange stall" {
            let dropped = DropSignal(self.exchange_drops.clone());
            return Ok(Response::new(
                futures::stream::pending()
                    .map(move |message| {
                        let _dropped = &dropped;
                        message
                    })
                    .boxed(),
            ));
        }
        let rows =
            parameters.ok_or_else(|| Status::invalid_argument("exchange without parameters"))?;
        let rows = self
            .result_overrides
            .lock()
            .unwrap()
            .get(&query)
            .cloned()
            .unwrap_or(rows);
        let schema = rows.schema();
        let rest: BoxStream<'static, arrow_flight::error::Result<RecordBatch>> =
            if query == "SELECT exchange slow" {
                futures::stream::pending().boxed()
            } else {
                futures::stream::empty().boxed()
            };
        let delay = if query == "SELECT exchange delayed" {
            Duration::from_millis(300)
        } else {
            Duration::ZERO
        };
        let dropped = DropSignal(self.exchange_drops.clone());
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(
                futures::stream::once(async move {
                    tokio::time::sleep(delay).await;
                    Ok(rows)
                })
                .chain(rest),
            )
            .map_err(Status::from)
            .map(move |message| {
                let _dropped = &dropped;
                message
            });
        Ok(self.adaptive_response(&query, stream.boxed()))
    }
}

fn description_batch(document: serde_json::Value) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("description", DataType::Utf8, false)]).with_metadata(
        HashMap::from([("lancedb.describe.format".into(), "json".into())]),
    );
    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(StringArray::from(vec![document.to_string()]))],
    )
    .unwrap()
}

fn parameter_row() -> RecordBatch {
    let vector = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        [Some([0.1_f32, 1e-45, -0.0].map(Some))],
        3,
    );
    RecordBatch::try_from_iter([
        ("vector", Arc::new(vector) as ArrayRef),
        ("k", Arc::new(Int64Array::from(vec![10])) as ArrayRef),
    ])
    .unwrap()
}

fn parameterized(query: &str, parameters: RecordBatch) -> ExecuteQueryRequest {
    ExecuteQueryRequest {
        parameters: Some(parameters),
        ..ExecuteQueryRequest::new(query)
    }
}

async fn start_sql_service(
    service: TestSqlService,
    client_config: ClientConfig,
) -> (SqlClient, tokio::sync::oneshot::Sender<()>) {
    start_sql_service_with_prefix(service, client_config, None).await
}

async fn start_sql_service_with_prefix(
    service: TestSqlService,
    client_config: ClientConfig,
    database_prefix: Option<&str>,
) -> (SqlClient, tokio::sync::oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = futures::stream::try_unfold(listener, |listener| async {
        let (socket, _) = listener.accept().await?;
        Ok::<_, std::io::Error>(Some((socket, listener)))
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            }),
    );
    let client = SqlClient::new(
        "analytics".to_string(),
        database_prefix.map(str::to_owned),
        "test-key".to_string(),
        None,
        Some(format!("grpc://{address}")),
        client_config,
    );
    (client, shutdown_tx)
}

async fn wait_for(count: &AtomicUsize, expected: usize, what: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while count.load(Ordering::SeqCst) < expected {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

/// The statement and its row go out on one exchange -- no poll, no ticket --
/// and the row arrives as exactly the bytes that were sent.
#[tokio::test]
async fn parameters_travel_on_one_exchange() {
    let service = TestSqlService::default();
    let exchanges = service.exchanges.clone();
    let query_count = service.query_count.clone();
    let do_get_count = service.do_get_count.clone();
    let headers = service.headers.clone();
    let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;

    let parameters = parameter_row();
    let query = client
        .execute(ExecuteQueryRequest {
            default_namespace_path: vec!["events".to_string(), "raw".to_string()],
            ..parameterized("SELECT echo", parameters.clone())
        })
        .await
        .unwrap();
    assert_eq!(query.describe().await.unwrap().status, QueryStatus::Running);
    let rows = collect_result(&query).await.unwrap();
    assert_eq!(rows, vec![parameters.clone()]);
    let echoed = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap()
        .values()
        .iter()
        .map(|value| value.to_bits())
        .collect::<Vec<_>>();
    assert_eq!(
        echoed,
        [0.1_f32, 1e-45, -0.0].map(f32::to_bits),
        "every float keeps its exact bits, the subnormal and the sign of zero included"
    );

    let finished = query.describe().await.unwrap();
    assert_eq!(finished.status, QueryStatus::Finished);
    assert_eq!(finished.progress, Some(1.0));
    assert_eq!(
        client.describe(query.id()).await.unwrap().status,
        QueryStatus::Finished
    );
    assert!(query.reader().await.is_err());

    assert_eq!(
        *exchanges.lock().unwrap(),
        vec![("SELECT echo".to_string(), Some(parameters))]
    );
    assert_eq!(query_count.load(Ordering::SeqCst), 0);
    assert_eq!(do_get_count.load(Ordering::SeqCst), 0);
    let headers = headers.lock().unwrap();
    assert_eq!(headers.len(), 1);
    assert_eq!(headers[0].database, "analytics");
    assert_eq!(headers[0].namespace_path, "events$raw");
    assert_eq!(headers[0].api_key, "test-key");
}

#[tokio::test]
async fn running_job_backed_queries_expose_their_own_durable_job_id() {
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        ..Default::default()
    };
    let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;

    let first = client
        .execute(ExecuteQueryRequest::new("SELECT durable job first"))
        .await
        .unwrap();
    let second = client
        .execute(ExecuteQueryRequest::new("SELECT durable job second"))
        .await
        .unwrap();
    assert_eq!(first.job_id(), None);
    assert_eq!(second.job_id(), None);

    let first_description = first.describe().await.unwrap();
    let second_description = second.describe().await.unwrap();
    assert_eq!(first_description.status, QueryStatus::Running);
    assert_eq!(first_description.job_id.as_deref(), Some("job-first"));
    assert_eq!(second_description.status, QueryStatus::Running);
    assert_eq!(second_description.job_id.as_deref(), Some("job-second"));
    assert_ne!(first.id(), second.id());
    assert_ne!(first_description.job_id, second_description.job_id);

    let reader = first.reader().await.unwrap();
    assert_eq!(
        reader
            .schema()
            .metadata()
            .get(SQL_JOB_ID_METADATA_KEY)
            .map(String::as_str),
        Some("job-first")
    );
    assert_eq!(first.job_id().as_deref(), Some("job-first"));
    assert_eq!(second.job_id().as_deref(), Some("job-second"));

    let reader_only = client
        .execute(ExecuteQueryRequest::new("SELECT durable job reader-only"))
        .await
        .unwrap();
    let reader_only_result = reader_only.reader().await.unwrap();
    assert_eq!(
        reader_only_result
            .schema()
            .metadata()
            .get(SQL_JOB_ID_METADATA_KEY)
            .map(String::as_str),
        Some("job-reader-only")
    );

    let ordinary = client
        .execute(ExecuteQueryRequest::new("SELECT ordinary"))
        .await
        .unwrap();
    assert_eq!(ordinary.describe().await.unwrap().job_id, None);
}

#[tokio::test]
async fn direct_job_backed_queries_expose_the_initial_response_job_id() {
    let service = TestSqlService {
        adaptive: true,
        ..Default::default()
    };
    let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let query = client
        .execute(ExecuteQueryRequest::new("SELECT direct durable job direct"))
        .await
        .unwrap();

    assert_eq!(query.job_id().as_deref(), Some("job-direct"));
    assert_eq!(
        query.describe().await.unwrap().job_id.as_deref(),
        Some("job-direct")
    );
    let reader = query.reader().await.unwrap();
    assert_eq!(
        reader
            .schema()
            .metadata()
            .get(SQL_JOB_ID_METADATA_KEY)
            .map(String::as_str),
        Some("job-direct")
    );
    let batches = reader.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0]
            .schema()
            .metadata()
            .get(SQL_JOB_ID_METADATA_KEY)
            .map(String::as_str),
        Some("job-direct")
    );
}

/// The server plans before it answers, so a refusal is the submission's.
#[tokio::test]
async fn a_refused_statement_fails_its_submission() {
    let (client, _shutdown) =
        start_sql_service(TestSqlService::default(), ClientConfig::default()).await;
    let refused = client
        .execute(parameterized("SELECT exchange refused", parameter_row()))
        .await
        .unwrap_err();
    assert!(
        refused
            .to_string()
            .contains("not referenced by the statement"),
        "{refused}"
    );

    let two_rows =
        RecordBatch::try_from_iter([("k", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef)])
            .unwrap();
    assert!(matches!(
        client.execute(parameterized("SELECT echo", two_rows)).await,
        Err(Error::InvalidInput { .. })
    ));
}

/// The call is the statement's lifetime: cancelling a read, or dropping a
/// handle nobody read, ends the call and so the work behind it.
#[tokio::test]
async fn cancelling_or_abandoning_an_exchange_ends_the_call() {
    let service = TestSqlService::default();
    let drops = service.exchange_drops.clone();
    let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;

    let reading = client
        .execute(parameterized("SELECT exchange slow", parameter_row()))
        .await
        .unwrap();
    let mut rows = reading.reader().await.unwrap();
    assert_eq!(rows.try_next().await.unwrap().unwrap(), parameter_row());
    reading.cancel().await.unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), rows.try_next())
            .await
            .expect("cancellation must wake the reader"),
        Err(Error::JobCancelled { .. })
    ));
    assert_eq!(
        reading.describe().await.unwrap().status,
        QueryStatus::Cancelled
    );
    wait_for(&drops, 1, "cancelling must end the call on the server").await;
    reading.cancel().await.unwrap();

    let unread = client
        .execute(parameterized("SELECT exchange slow", parameter_row()))
        .await
        .unwrap();
    let id = unread.id();
    drop(unread);
    wait_for(&drops, 2, "dropping an unread handle must end the call").await;
    assert_eq!(
        client.describe(id).await.unwrap().status,
        QueryStatus::Cancelled
    );

    let dropped_reader = client
        .execute(parameterized("SELECT exchange slow", parameter_row()))
        .await
        .unwrap();
    let rows = dropped_reader.reader().await.unwrap();
    drop(rows);
    wait_for(&drops, 3, "dropping a reader must end the call").await;
}

/// A reader abandoned before the first batch -- its task cancelled -- must
/// leave the rows for the next reader, as a polled query does, rather than
/// take the call down with it.
#[tokio::test]
async fn an_abandoned_reader_leaves_the_rows_for_the_next_one() {
    let service = TestSqlService::default();
    let drops = service.exchange_drops.clone();
    let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;

    let delayed = Arc::new(
        client
            .execute(parameterized("SELECT exchange delayed", parameter_row()))
            .await
            .unwrap(),
    );
    let abandoned = {
        let delayed = delayed.clone();
        tokio::spawn(async move { delayed.reader().await.map(|_| ()) })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    abandoned.abort();
    assert!(abandoned.await.unwrap_err().is_cancelled());
    assert_eq!(
        delayed.describe().await.unwrap().status,
        QueryStatus::Running
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0, "the call must survive");

    assert_eq!(
        collect_result(&delayed).await.unwrap(),
        vec![parameter_row()]
    );
    assert_eq!(
        delayed.describe().await.unwrap().status,
        QueryStatus::Finished
    );
}

/// A reader that gives up before the first batch has already taken the rows,
/// so the query must say it failed rather than report `Running` for a call
/// that is gone.
#[tokio::test]
async fn a_reader_that_times_out_reports_the_failure() {
    let mut client_config = ClientConfig::default();
    client_config.timeout_config.timeout = Some(Duration::from_millis(200));
    let service = TestSqlService::default();
    let drops = service.exchange_drops.clone();
    let (client, _shutdown) = start_sql_service(service, client_config).await;

    // Real loopback I/O must finish before the clock can advance to the timeout.
    // A blocking task inhibits Tokio's automatic clock advance during submission.
    tokio::time::pause();
    let submission_client = client.clone();
    let stalled = tokio::task::spawn_blocking(move || {
        tokio::runtime::Handle::current().block_on(
            submission_client.execute(parameterized("SELECT exchange stall", parameter_row())),
        )
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_overall_timeout(stalled.reader().await, "result");
    tokio::time::resume();
    wait_for(&drops, 1, "a timed-out reader must end the call").await;
    let Err(retried) = stalled.reader().await else {
        panic!("the rows are gone, so a second reader cannot succeed");
    };
    for error in [
        stalled.describe().await.unwrap_err(),
        client.describe(stalled.id()).await.unwrap_err(),
        retried,
    ] {
        assert!(
            matches!(&error, Error::Runtime { message } if message == "SQL query result timed out"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn catalog_connections_use_explicit_sql_endpoint_and_database_scope() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = futures::stream::try_unfold(listener, |listener| async {
        let (socket, _) = listener.accept().await?;
        Ok::<_, std::io::Error>(Some((socket, listener)))
    });
    let service = TestSqlService::default();
    let headers = service.headers.clone();
    let expected = service.result.clone();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            }),
    );
    let options = RemoteCatalogOptions {
        api_key: Some("catalog-key".into()),
        sql_host_override: Some(format!("grpc://{address}")),
        ..Default::default()
    };
    for name in ["analytics", "team/search"] {
        let database =
            RemoteDatabase::for_catalog("https://catalog.example", Some(name), &options).unwrap();
        let query = database
            .execute_query_async(ExecuteQueryRequest {
                default_namespace_path: vec![],
                ..ExecuteQueryRequest::new("SELECT 42")
            })
            .await
            .unwrap();
        assert_eq!(
            collect_result(&query).await.unwrap(),
            vec![expected.clone()]
        );
    }
    {
        let headers = headers.lock().unwrap();
        assert!(headers.iter().any(|header| header.database == "analytics"));
        assert!(
            headers
                .iter()
                .any(|header| header.database == "team/search")
        );
        for header in headers.iter() {
            assert_eq!(header.api_key, "catalog-key");
            assert_eq!(header.namespace_path, "public");
            assert!(header.database_prefix.is_empty());
        }
    }
    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn remote_query_builder_sends_the_complete_query() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = futures::stream::try_unfold(listener, |listener| async {
        let (socket, _) = listener.accept().await?;
        Ok::<_, std::io::Error>(Some((socket, listener)))
    });
    let service = TestSqlService {
        adaptive: true,
        ..Default::default()
    };
    let statements = service.statements.clone();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            }),
    );
    let connection = crate::connect("db://analytics")
        .region("us-east-1")
        .api_key("test-key")
        .host_override("http://127.0.0.1:1")
        .sql_host_override(&format!("grpc://{address}"))
        .execute()
        .await
        .unwrap();
    for query in [
        "SELECT kind, count(*) AS n FROM events GROUP BY kind",
        "SELECT id, c.text FROM docs, chunk(body) AS c",
        "SELECT id, tag FROM events, UNNEST(tags) AS tag",
        "SELECT a.id FROM a JOIN b ON a.id = b.id",
    ] {
        connection
            .create_materialized_view_from_query("result", query)
            .auto_refresh_enabled(false)
            .execute_async()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(
            statements.lock().unwrap().last().unwrap(),
            &format!(
                "CREATE MATERIALIZED VIEW \"result\" WITH (auto_refresh_enabled = false) AS {query}"
            )
        );
    }
    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn remote_create_uses_sql_and_preserves_options_and_namespace() {
    for defer in [false, true] {
        let service = TestSqlService {
            adaptive: true,
            defer,
            ..Default::default()
        };
        let statements = service.statements.clone();
        let headers = service.headers.clone();
        let count = service.query_count.clone();
        let (sql_client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let db = RemoteDatabase::new_mock(|request| -> http::Response<String> {
            panic!("CREATE must use SQL, not REST: {}", request.url().path());
        })
        .with_sql_client(sql_client.clone());
        for (enabled, interval, cpu, gpu, expected) in [
            (None, None, None, None, ""),
            (
                Some(true),
                None,
                None,
                None,
                " WITH (auto_refresh_enabled = true)",
            ),
            (
                Some(false),
                Some(0.5),
                None,
                None,
                " WITH (auto_refresh_enabled = false, auto_refresh_interval_seconds = 0.5)",
            ),
            (
                None,
                None,
                Some(8000),
                Some(0),
                " WITH (total_cpu_millis = 8000, total_gpu_count = 0)",
            ),
        ] {
            let job = db
                .create_materialized_view_async(CreateMaterializedViewRequest {
                    gpu_class: Default::default(),
                    gpu_count: Default::default(),
                    total_cpu_millis: cpu,
                    total_memory_bytes: None,
                    total_gpu_count: gpu,

                    exist_ok: false,
                    name: "adults".into(),
                    namespace_path: vec!["team".into(), "reports".into()],
                    query: "SELECT * FROM people".into(),
                    with_no_data: false,
                    auto_refresh_enabled: enabled,
                    auto_refresh_interval_seconds: interval,
                })
                .await
                .unwrap();
            assert_eq!(job.job_type().as_deref(), Some("query"));
            let query_id =
                Uuid::parse_str(job.id().expect("SQL-backed job must expose its ID")).unwrap();
            job.wait().await.unwrap();
            job.wait().await.unwrap();
            assert_eq!(job.status().await.unwrap(), "finished");
            assert!(job.events(Default::default()).await.is_err());
            assert_eq!(job.job_type().as_deref(), Some("query"));
            assert_eq!(
                sql_client.describe(query_id).await.unwrap().status,
                QueryStatus::Finished
            );
            assert_eq!(
                statements.lock().unwrap().last().unwrap(),
                &format!("CREATE MATERIALIZED VIEW \"adults\"{expected} AS SELECT * FROM people")
            );
        }
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(statements.lock().unwrap().len(), 4);
        assert!(
            headers
                .lock()
                .unwrap()
                .iter()
                .all(|header| header.namespace_path == "team$reports")
        );
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn remote_create_returns_the_registry_job_from_the_early_status_row() {
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        materialized_view_job_handle_supported: true,
        ..Default::default()
    };
    let headers = service.headers.clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new("operation", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("job_id", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    service.result_overrides.lock().unwrap().insert(
        "CREATE MATERIALIZED VIEW \"adults\" AS SELECT * FROM people".into(),
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["CREATE MATERIALIZED VIEW"])),
                Arc::new(StringArray::from(vec!["adults"])),
                Arc::new(StringArray::from(vec!["job-1"])),
                Arc::new(StringArray::from(vec!["submitted"])),
            ],
        )
        .unwrap(),
    );
    let (sql_client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let db = RemoteDatabase::new_mock(|request| -> http::Response<String> {
        assert_eq!(request.url().path(), "/v1/jobs/describe");
        http::Response::builder()
            .status(200)
            .body(
                r#"{"job_id":"job-1","job_type":"sql","job_state":"DONE","creation_ms":1}"#
                    .to_string(),
            )
            .unwrap()
    })
    .with_sql_client(sql_client);

    let job = db
        .create_materialized_view_async(CreateMaterializedViewRequest {
            gpu_class: Default::default(),
            gpu_count: Default::default(),
            total_cpu_millis: None,
            total_memory_bytes: None,
            total_gpu_count: None,

            exist_ok: false,
            name: "adults".into(),
            namespace_path: vec!["team".into(), "reports".into()],
            query: "SELECT * FROM people".into(),
            with_no_data: false,
            auto_refresh_enabled: None,
            auto_refresh_interval_seconds: None,
        })
        .await
        .unwrap();

    assert_eq!(job.id(), Some("job-1"));
    job.refresh().await.unwrap();
    assert_eq!(job.job_type().as_deref(), Some("sql"));
    job.wait().await.unwrap();
    let headers = headers.lock().unwrap();
    assert!(headers.iter().any(|header| {
        header.materialized_view_job_handle.as_deref() == Some("early")
            && header.namespace_path == "team$reports"
    }));
    assert!(
        headers
            .iter()
            .all(|header| header.namespace_path == "team$reports")
    );
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn remote_create_if_not_exists_accepts_a_completed_noop_without_a_registry_job() {
    for defer in [false, true] {
        let service = TestSqlService {
            adaptive: true,
            defer,
            materialized_view_job_handle_supported: true,
            ..Default::default()
        };
        let statement = "CREATE MATERIALIZED VIEW IF NOT EXISTS \"adults\" AS SELECT * FROM people";
        service.result_overrides.lock().unwrap().insert(
            statement.into(),
            RecordBatch::try_from_iter([
                (
                    "status",
                    Arc::new(StringArray::from(vec!["already_exists"])) as arrow_array::ArrayRef,
                ),
                (
                    "job_id",
                    Arc::new(StringArray::from(vec![""])) as arrow_array::ArrayRef,
                ),
            ])
            .unwrap(),
        );
        let statements = service.statements.clone();
        let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let db = RemoteDatabase::new_mock(|request| -> http::Response<String> {
            panic!(
                "a skipped creation must not poll a registry job: {}",
                request.url()
            );
        })
        .with_sql_client(client);
        let job = db
            .create_materialized_view_async(CreateMaterializedViewRequest {
                gpu_class: Default::default(),
                gpu_count: Default::default(),
                total_cpu_millis: None,
                total_memory_bytes: None,
                total_gpu_count: None,

                exist_ok: true,
                name: "adults".into(),
                namespace_path: vec!["team".into()],
                query: "SELECT * FROM people".into(),
                with_no_data: false,
                auto_refresh_enabled: None,
                auto_refresh_interval_seconds: None,
            })
            .await
            .unwrap();
        if defer {
            assert!(job.id().is_none());
        } else {
            assert_eq!(job.job_type().as_deref(), Some("query"));
        }
        job.wait().await.unwrap();
        assert_eq!(job.status().await.unwrap(), "finished");
        assert_eq!(*statements.lock().unwrap(), [statement]);
        shutdown.send(()).unwrap();
    }
}

#[rstest::rstest]
#[case::submitted("submitted", " job-1 ", Ok(Some(" job-1 ")), Ok(Some(" job-1 ")))]
#[case::blank_id("submitted", " \t ", Err("blank job_id"), Err("blank job_id"))]
#[case::missing_id("submitted", "", Err("returned no job_id"), Err("returned no job_id"))]
#[case::already_exists("already_exists", "", Ok(None), Err("create-only already_exists"))]
#[case::malformed_noop("already_exists", " \t ", Err("blank job_id"), Err("blank job_id"))]
#[tokio::test]
async fn remote_materialized_view_receipt_with_empty_batches(
    #[case] status: &str,
    #[case] job_id: &str,
    #[case] expected_create: std::result::Result<Option<&str>, &str>,
    #[case] expected_refresh: std::result::Result<Option<&str>, &str>,
) {
    let row = RecordBatch::try_from_iter([
        (
            "status",
            Arc::new(StringArray::from(vec![status])) as ArrayRef,
        ),
        (
            "job_id",
            Arc::new(StringArray::from(vec![job_id])) as ArrayRef,
        ),
    ])
    .unwrap();
    let empty = row.slice(0, 0);
    // FlightDataEncoderBuilder skips zero-row batches. Encode them explicitly on the wire.
    let frames =
        arrow_flight::utils::batches_to_flight_data(&row.schema(), vec![empty.clone(), row, empty])
            .unwrap();
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        materialized_view_job_handle_supported: true,
        result_frames: Some(frames),
        ..Default::default()
    };
    let statements = service.statements.clone();
    let (sql, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let db = RemoteDatabase::new_mock(|r| -> http::Response<String> {
        panic!("unexpected HTTP request during create: {}", r.url())
    })
    .with_sql_client(sql.clone());
    let table = receipt_test_table(sql);
    let create = db
        .create_materialized_view_async(CreateMaterializedViewRequest {
            name: "events".into(),
            namespace_path: vec!["team".into()],
            query: "SELECT * FROM people".into(),
            exist_ok: true,
            with_no_data: false,
            auto_refresh_enabled: None,
            auto_refresh_interval_seconds: None,
            total_cpu_millis: None,
            total_memory_bytes: None,
            total_gpu_count: None,
            gpu_class: None,
            gpu_count: None,
        })
        .await;
    let refresh = table.refresh_materialized_view_async(None, false).await;
    for (operation, actual, expected) in [
        (
            "CREATE",
            create.as_ref().map(|job| job.id()),
            expected_create,
        ),
        (
            "REFRESH",
            refresh.as_ref().map(|job| job.id()),
            expected_refresh,
        ),
    ] {
        match expected {
            Ok(id) => assert_eq!(actual.unwrap(), id, "{operation}"),
            Err(message) => {
                let err = actual.unwrap_err().to_string();
                assert!(err.contains(message), "{operation}: {err}");
            }
        }
    }
    assert_eq!(
        *statements.lock().unwrap(),
        [
            r#"CREATE MATERIALIZED VIEW IF NOT EXISTS "events" AS SELECT * FROM people"#,
            r#"REFRESH MATERIALIZED VIEW "events""#,
        ]
    );
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn remote_create_registry_handle_reopens_describes_and_cancels() {
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        materialized_view_job_handle_supported: true,
        ..Default::default()
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("operation", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("job_id", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    service.result_overrides.lock().unwrap().insert(
        "CREATE MATERIALIZED VIEW \"adults\" AS SELECT * FROM people".into(),
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["CREATE MATERIALIZED VIEW"])),
                Arc::new(StringArray::from(vec!["adults"])),
                Arc::new(StringArray::from(vec!["job-1"])),
                Arc::new(StringArray::from(vec!["submitted"])),
            ],
        )
        .unwrap(),
    );
    let (sql_client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let describes = Arc::new(AtomicUsize::new(0));
    let seen_describes = describes.clone();
    let describe_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen_describe_ids = describe_ids.clone();
    let cancel_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen_cancel_ids = cancel_ids.clone();
    let db = RemoteDatabase::new_mock(move |request| {
        let body = request
            .body()
            .and_then(|body| body.as_bytes())
            .and_then(|body| serde_json::from_slice::<serde_json::Value>(body).ok())
            .unwrap_or_default();
        let job_id = body["job_id"].as_str().unwrap_or_default().to_string();
        match request.url().path() {
            "/v1/jobs/describe" => {
                assert_eq!(job_id, "job-1");
                seen_describe_ids.lock().unwrap().push(job_id.clone());
                seen_describes.fetch_add(1, Ordering::SeqCst);
                http::Response::builder()
                    .status(200)
                    .body(format!(
                        r#"{{"job_id":"{job_id}","job_type":"sql","job_state":"IN_PROGRESS","creation_ms":1}}"#
                    ))
                    .unwrap()
            }
            "/v1/jobs/cancel" => {
                assert_eq!(job_id, "job-1");
                seen_cancel_ids.lock().unwrap().push(job_id);
                http::Response::builder().status(200).body(String::new()).unwrap()
            }
            path => panic!("unexpected request: {path}"),
        }
    })
    .with_sql_client(sql_client);

    let job = db
        .create_materialized_view_async(CreateMaterializedViewRequest {
            gpu_class: Default::default(),
            gpu_count: Default::default(),
            total_cpu_millis: None,
            total_memory_bytes: None,
            total_gpu_count: None,

            exist_ok: false,
            name: "adults".into(),
            namespace_path: vec!["team".into(), "reports".into()],
            query: "SELECT * FROM people".into(),
            with_no_data: false,
            auto_refresh_enabled: None,
            auto_refresh_interval_seconds: None,
        })
        .await
        .unwrap();
    assert_eq!(job.id(), Some("job-1"));

    job.refresh().await.unwrap();
    assert_eq!(job.job_type().as_deref(), Some("sql"));
    assert_eq!(job.state().as_deref(), Some("running"));

    let reopened = db.open_job("job-1").await.unwrap();
    assert_eq!(reopened.id(), Some("job-1"));
    assert_eq!(reopened.job_type().as_deref(), Some("sql"));
    reopened.cancel().await.unwrap();

    assert_eq!(*describe_ids.lock().unwrap(), vec!["job-1", "job-1"]);
    assert_eq!(*cancel_ids.lock().unwrap(), vec!["job-1"]);
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn query_backed_jobs_propagate_submission_and_execution_failures() {
    let (client, shutdown) = start_sql_service(
        TestSqlService {
            adaptive: true,
            defer: true,
            ..Default::default()
        },
        ClientConfig::default(),
    )
    .await;
    let rejected = client
        .submit_as_job("SELECT rejected".into(), vec![], || async { Ok(()) })
        .await;
    assert!(rejected.is_err());
    let finished = Arc::new(AtomicBool::new(false));
    let callback_finished = finished.clone();
    let job = client
        .submit_as_job("SELECT fail".into(), vec![], move || async move {
            callback_finished.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();
    assert!(matches!(job.wait().await, Err(Error::JobFailed { .. })));
    assert!(matches!(job.wait().await, Err(Error::JobFailed { .. })));
    assert_eq!(job.status().await.unwrap(), "failed");
    assert!(!finished.load(Ordering::SeqCst));
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn function_failure_preserves_the_isolated_sql_job_id() {
    let (client, shutdown) = start_sql_service(
        TestSqlService {
            adaptive: true,
            defer: true,
            ..Default::default()
        },
        ClientConfig::default(),
    )
    .await;
    let job = client
        .submit_as_job("SELECT function failure".into(), vec![], || async {
            Ok(())
        })
        .await
        .unwrap();

    let Err(Error::JobFailed { job_id, failure }) = job.wait().await else {
        panic!("a typed Function failure must stay a JobFailed error");
    };
    assert_eq!(job_id.as_deref(), Some("job-1"));
    assert_eq!(failure.sql_job_id.as_deref(), Some("job-1"));
    assert_eq!(failure.phase, None);
    assert_eq!(failure.retryable, None);
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn fast_durable_job_failure_exposes_the_same_job_id_on_the_query_handle() {
    let (client, shutdown) = start_sql_service(
        TestSqlService {
            adaptive: true,
            defer: true,
            ..Default::default()
        },
        ClientConfig::default(),
    )
    .await;
    let query = client
        .execute(ExecuteQueryRequest {
            query: "SELECT durable job failure".into(),
            default_namespace_path: vec![],
            parameters: None,
            options: Default::default(),
        })
        .await
        .unwrap();
    assert_eq!(query.job_id(), None);

    let Err(Error::JobFailed { job_id, .. }) = collect_result(&query).await else {
        panic!("a failed durable query must remain a structured JobFailed error");
    };
    assert_eq!(job_id.as_deref(), Some("job-1"));
    assert_eq!(query.job_id().as_deref(), Some("job-1"));
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn rejected_job_cancellation_does_not_abort_sql_completion() {
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        ..Default::default()
    };
    let do_get_count = service.do_get_count.clone();
    let release = service.gate_refresh_release.clone();
    let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let job = client
        .submit_as_job("SELECT job cancel denied".into(), vec![], || async {
            Ok(())
        })
        .await
        .unwrap();
    wait_for(&do_get_count, 1, "job did not begin reading its SQL result").await;
    assert!(matches!(
        job.cancel().await,
        Err(Error::NotSupported { .. })
    ));
    assert_eq!(job.status().await.unwrap(), "running");
    release.notify_one();
    job.wait().await.unwrap();
    assert_eq!(job.status().await.unwrap(), "finished");
    job.cancel().await.unwrap();
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn remote_refresh_omits_unknown_metrics_and_forwards_full() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = futures::stream::try_unfold(listener, |listener| async {
        let (socket, _) = listener.accept().await?;
        Ok::<_, std::io::Error>(Some((socket, listener)))
    });
    let result =
        description_batch(serde_json::json!({"name":"view", "query":"SELECT * FROM source"}));
    let service = TestSqlService {
        result,
        ..Default::default()
    };
    let statements = service.statements.clone();
    let headers = service.headers.clone();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            }),
    );
    let sql_client = SqlClient::new(
        "analytics".into(),
        None,
        "test-key".into(),
        None,
        Some(format!("grpc://{address}")),
        ClientConfig::default(),
    );
    let selectors = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = selectors.clone();
    let client = client_with_handler(move |request| {
        let body = request
            .body()
            .and_then(reqwest::Body::as_bytes)
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).ok())
            .unwrap_or_default();
        captured.lock().unwrap().push(body["version"].clone());
        let text = match request.url().path() {
            "/v1/table/ns$view/describe/" => r#"{"version": 42, "schema": {"fields": []}}"#,
            other => panic!("unexpected REST request: {other}"),
        };
        http::Response::builder()
            .status(200)
            .body(text.to_string())
            .unwrap()
    });
    let table = RemoteTable::new_with_sql_client(
        client,
        "analytics".into(),
        "view".into(),
        vec!["ns".into()],
        "ns$view".into(),
        Default::default(),
        Some(sql_client),
    );
    let database = RemoteDatabase::new_mock(|request| -> http::Response<String> {
        panic!("unexpected database request: {}", request.url());
    });
    let view =
        crate::MaterializedView::from_table(crate::Table::new(Arc::new(table), Arc::new(database)))
            .await
            .unwrap();

    let first = view.refresh().execute().await.unwrap();
    let second = view
        .refresh()
        .full(true)
        .execute_async()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();

    let third = view
        .refresh()
        .full(true)
        .total_cpu_millis(8000)
        .total_memory_bytes(16 << 30)
        .total_gpu_count(2)
        .gpu_class("h100")
        .gpu_count(2)
        .execute_async()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    for invalid in [
        view.refresh().gpu_class("H200:8"),
        view.refresh().total_gpu_count(0).gpu_class("h100"),
    ] {
        assert!(matches!(
            invalid.execute_async().await,
            Err(Error::InvalidInput { .. })
        ));
    }
    for result in [first, second, third] {
        assert_eq!(result.mode, None);
        assert_eq!(result.rows_written, None);
        assert_eq!(result.source_version, None);
        assert_eq!(result.version, 42);
        // Query-backed jobs serialize their typed result before returning it.
        let json = serde_json::to_value(result).unwrap();
        assert!(json["mode"].is_null());
        assert!(json["rows_written"].is_null());
        assert!(json["source_version"].is_null());
    }
    assert!(matches!(
        view.refresh().source_version(7).execute_async().await,
        Err(Error::NotSupported { .. })
    ));
    assert_eq!(
        *statements.lock().unwrap(),
        [
            "DESCRIBE MATERIALIZED VIEW \"view\" FORMAT JSON",
            "REFRESH MATERIALIZED VIEW \"view\"",
            "REFRESH MATERIALIZED VIEW \"view\" WITH (full = true)",
            "REFRESH MATERIALIZED VIEW \"view\" WITH (full = true, total_cpu_millis = 8000, total_memory_bytes = 17179869184, total_gpu_count = 2, gpu_class = 'h100', gpu_count = 2)",
        ]
    );
    assert!(
        headers
            .lock()
            .unwrap()
            .iter()
            .all(|h| h.namespace_path == "ns")
    );
    assert!(
        selectors
            .lock()
            .unwrap()
            .iter()
            .all(serde_json::Value::is_null)
    );
    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelling_remote_refresh_cancels_sql_query() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = futures::stream::try_unfold(listener, |listener| async {
        let (socket, _) = listener.accept().await?;
        Ok::<_, std::io::Error>(Some((socket, listener)))
    });
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        ..Default::default()
    };
    let cancel_count = service.cancel_count.clone();
    let do_get_count = service.do_get_count.clone();
    let gate_refresh_release = service.gate_refresh_release.clone();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            }),
    );
    let sql_client = SqlClient::new(
        "analytics".into(),
        None,
        "test-key".into(),
        None,
        Some(format!("grpc://{address}")),
        ClientConfig::default(),
    );
    let client = client_with_handler(|request| -> http::Response<String> {
        panic!(
            "cancelled refresh must not read result metadata: {}",
            request.url().path()
        );
    });
    let table = RemoteTable::new_with_sql_client(
        client,
        "analytics".into(),
        "cancel_view".into(),
        vec![],
        "cancel_view".into(),
        Default::default(),
        Some(sql_client),
    );
    let job = table
        .refresh_materialized_view_async(None, false)
        .await
        .unwrap();
    assert_eq!(job.job_type().as_deref(), Some("query"));
    assert!(job.id().is_some());
    tokio::time::timeout(Duration::from_secs(10), async {
        while do_get_count.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    job.cancel().await.unwrap();
    assert!(matches!(job.wait().await, Err(Error::JobCancelled { .. })));
    assert_eq!(cancel_count.load(Ordering::SeqCst), 1);

    gate_refresh_release.notify_one();
    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
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
    let incremental_finished = service.incremental_finished.clone();
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
    assert_overall_timeout(collect_result(&timeout_query).await, "result");
    wait_for(
        &cancel_count,
        1,
        "the overall timeout must cancel execution",
    )
    .await;
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
        QueryStatus::Cancelled
    );

    let rejected_cancel = timeout_client
        .submit("SELECT cancel denied", &["public".to_string()])
        .await
        .unwrap();
    assert!(rejected_cancel.cancel().await.is_err());
    assert!(rejected_cancel.cancel().await.is_err());
    assert_ne!(
        rejected_cancel.describe().await.unwrap().status,
        QueryStatus::Cancelled
    );

    let unspecified_cancel = timeout_client
        .submit("SELECT cancel unspecified", &["public".to_string()])
        .await
        .unwrap();
    assert!(unspecified_cancel.cancel().await.is_err());
    assert!(unspecified_cancel.cancel().await.is_err());
    assert_ne!(
        unspecified_cancel.describe().await.unwrap().status,
        QueryStatus::Cancelled
    );
    assert_eq!(
        collect_result(&unspecified_cancel).await.unwrap(),
        vec![expected.clone()]
    );

    let uncertain_cancel = timeout_client
        .submit("SELECT cancel timeout", &["public".to_string()])
        .await
        .unwrap();
    assert_overall_timeout(uncertain_cancel.cancel().await, "cancellation");
    assert!(uncertain_cancel.cancel().await.is_err());
    assert_ne!(
        uncertain_cancel.describe().await.unwrap().status,
        QueryStatus::Cancelled
    );
    assert_eq!(
        collect_result(&uncertain_cancel).await.unwrap(),
        vec![expected.clone()]
    );

    let first = client
        .submit("SELECT 'super-secret'", &["public".to_string()])
        .await
        .unwrap();
    assert_eq!(first.id().get_version_num(), 7);
    assert!(!first.id().to_string().contains("super-secret"));
    header_provider.delay_next.store(true, Ordering::SeqCst);
    let describe_started = Instant::now();
    let first_description = client.describe(first.id()).await.unwrap();
    assert!(describe_started.elapsed() >= Duration::from_millis(1_100));
    assert_eq!(first_description.status, QueryStatus::Finished);
    assert_eq!(first_description.progress, Some(1.0));
    let first_result = collect_result(&first).await.unwrap();
    assert!(first.reader().await.is_err());

    let incremental = client
        .submit("SELECT incremental", &["public".to_string()])
        .await
        .unwrap();
    let mut incremental_result = incremental.reader().await.unwrap();
    let first_incremental_batch =
        tokio::time::timeout(Duration::from_millis(100), incremental_result.try_next())
            .await
            .expect("the first partial result must arrive before query completion")
            .unwrap()
            .unwrap();
    assert_eq!(first_incremental_batch, expected);
    assert!(!incremental_finished.load(Ordering::SeqCst));
    let remaining_incremental_batches = incremental_result.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(remaining_incremental_batches, vec![expected.clone()]);
    assert!(incremental_finished.load(Ordering::SeqCst));

    let interrupted_result = Arc::new(
        client
            .submit("SELECT no info", &["public".to_string()])
            .await
            .unwrap(),
    );
    let continuation_count_before = first_continuation_count.load(Ordering::SeqCst);
    let interrupted_result_task = {
        let interrupted_result = interrupted_result.clone();
        tokio::spawn(async move { interrupted_result.reader().await })
    };
    tokio::time::timeout(Duration::from_millis(100), async {
        while first_continuation_count.load(Ordering::SeqCst) == continuation_count_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("result preparation must start polling");
    interrupted_result_task.abort();
    assert!(
        interrupted_result_task
            .await
            .is_err_and(|error| error.is_cancelled())
    );
    assert_eq!(
        collect_result(&interrupted_result).await.unwrap(),
        vec![expected.clone()],
        "cancelling result preparation must release the one-shot result claim",
    );

    let dropped_reader = client
        .submit("SELECT slow", &["public".to_string()])
        .await
        .unwrap();
    let Some(RegisteredQuery::Polled(tracked_dropped_reader)) =
        client.queries.get(dropped_reader.id())
    else {
        panic!("a submitted statement is registered as a polled query");
    };
    let continuation_count_before = first_continuation_count.load(Ordering::SeqCst);
    let dropped_result_stream = dropped_reader.reader().await.unwrap();
    tokio::time::timeout(Duration::from_millis(100), async {
        while first_continuation_count.load(Ordering::SeqCst) == continuation_count_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the result producer must start continuation polling");
    assert!(Arc::strong_count(&tracked_dropped_reader) >= 4);
    drop(dropped_result_stream);
    tokio::time::timeout(Duration::from_millis(100), async {
        while Arc::strong_count(&tracked_dropped_reader) != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping a result reader must stop its producer");

    let staged = client
        .submit("SELECT no info", &["public".to_string()])
        .await
        .unwrap();
    let staged_running = client.describe(staged.id()).await.unwrap();
    assert_eq!(staged_running.status, QueryStatus::Running);
    let staged_finished = client.describe(staged.id()).await.unwrap();
    assert_eq!(staged_finished.status, QueryStatus::Finished);

    let empty = client
        .submit("SELECT empty", &["public".to_string()])
        .await
        .unwrap();
    let empty_result = empty.reader().await.unwrap();
    assert_eq!(empty_result.schema(), expected.schema());
    let empty_result = empty_result.try_collect::<Vec<_>>().await.unwrap();

    let large = client
        .submit("SELECT large message", &["public".to_string()])
        .await
        .unwrap();
    let large_result = collect_result(&large).await.unwrap();
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

    let dictionary = client
        .submit("SELECT dictionary", &["public".to_string()])
        .await
        .unwrap();
    let dictionary_result = collect_result(&dictionary).await.unwrap();
    assert_eq!(dictionary_result.len(), 1);
    assert_eq!(
        dictionary_result[0].schema().field(0).data_type(),
        &DataType::Utf8,
    );
    assert_eq!(
        dictionary_result[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "dictionary value",
    );

    let cancelled = client
        .submit(
            "SELECT cancelled",
            &["events".to_string(), "raw".to_string()],
        )
        .await
        .unwrap();
    cancelled.cancel().await.unwrap();
    assert_eq!(
        cancelled.describe().await.unwrap().status,
        QueryStatus::Cancelled
    );
    assert!(matches!(
        cancelled.reader().await,
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
        tokio::spawn(async move { collect_result(&slow).await })
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
        tokio::spawn(async move { collect_result(&slow_get).await })
    };
    while do_get_count.load(Ordering::SeqCst) == do_get_count_before_slow {
        tokio::task::yield_now().await;
    }
    slow_get.cancel().await.unwrap();
    assert_eq!(
        slow_get.describe().await.unwrap().status,
        QueryStatus::Cancelled
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(150), slow_get_result_task)
            .await
            .expect("cancellation must wake result fetching")
            .unwrap(),
        Err(Error::JobCancelled { .. })
    ));
    assert!(slow_get.reader().await.is_err());

    let restored = Arc::new(
        RemoteQuery::new(
            Uuid::now_v7(),
            client.inner.clone(),
            vec!["public".to_string()],
            PollInfo {
                flight_descriptor: Some(FlightDescriptor::new_cmd("restored")),
                ..Default::default()
            },
            None,
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
        tokio::spawn(async move { collect_result(&cancelling).await })
    };
    tokio::time::sleep(Duration::from_millis(25)).await;
    cancelling.cancel().await.unwrap();
    assert_eq!(
        cancelling.describe().await.unwrap().status,
        QueryStatus::Cancelling
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(150), cancelling_result_task)
            .await
            .expect("an accepted cancellation must wake result polling")
            .unwrap(),
        Err(Error::JobCancelled { .. })
    ));
    cancelling.cancel().await.unwrap();
    assert_eq!(
        cancelling.describe().await.unwrap().status,
        QueryStatus::Cancelled
    );

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
    let cancel_race_result = collect_result(&cancel_race).await.unwrap();
    tokio::time::timeout(Duration::from_millis(500), cancel_race_task)
        .await
        .expect("completed result must make in-flight cancellation a no-op")
        .unwrap()
        .unwrap();
    assert_eq!(
        cancel_race.describe().await.unwrap().status,
        QueryStatus::Finished
    );
    assert_eq!(cancel_race_result, vec![expected.clone()]);
    assert!(cancel_race.reader().await.is_err());

    let no_info = Arc::new(
        client
            .submit("SELECT no info", &["public".to_string()])
            .await
            .unwrap(),
    );
    let continuation_count_before = first_continuation_count.load(Ordering::SeqCst);
    let no_info_result_task = {
        let no_info = no_info.clone();
        tokio::spawn(async move { collect_result(&no_info).await })
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
    assert_eq!(
        collect_result(&retried).await.unwrap(),
        vec![expected.clone()]
    );

    let failed = client
        .submit("SELECT fail", &["public".to_string()])
        .await
        .unwrap();
    let before_failure = cancel_count.load(Ordering::SeqCst);
    assert!(collect_result(&failed).await.is_err());
    wait_for(
        &cancel_count,
        before_failure + 1,
        "failed result reads must cancel execution",
    )
    .await;

    let registry = QueryRegistry::new();
    for descriptor in ["active-one", "active-two"] {
        let id = Uuid::now_v7();
        let query = Arc::new(
            RemoteQuery::new(
                id,
                client.inner.clone(),
                vec!["public".to_string()],
                PollInfo {
                    flight_descriptor: Some(FlightDescriptor::new_cmd(descriptor)),
                    ..Default::default()
                },
                None,
            )
            .unwrap(),
        );
        registry.insert(id, RegisteredQuery::Polled(query.clone()));
        let Some(RegisteredQuery::Polled(found)) = registry.get(id) else {
            panic!("the registry returns the query it was given");
        };
        assert!(Arc::ptr_eq(&found, &query));
    }

    let expired_id = Uuid::now_v7();
    let expired_query = Arc::new(
        RemoteQuery::new(
            expired_id,
            client.inner.clone(),
            vec!["public".to_string()],
            PollInfo {
                flight_descriptor: Some(FlightDescriptor::new_cmd("expired")),
                expiration_time: Some(Default::default()),
                ..Default::default()
            },
            None,
        )
        .unwrap(),
    );
    registry.insert(expired_id, RegisteredQuery::Polled(expired_query));
    assert!(registry.get(expired_id).is_none());

    let stale_id = Uuid::now_v7();
    let stale_query = Arc::new(
        RemoteQuery::new(
            stale_id,
            client.inner.clone(),
            vec!["public".to_string()],
            PollInfo {
                flight_descriptor: Some(FlightDescriptor::new_cmd("stale")),
                ..Default::default()
            },
            None,
        )
        .unwrap(),
    );
    *stale_query.last_accessed.lock().unwrap() = Instant::now() - ABANDONED_QUERY_RETENTION;
    registry.insert(stale_id, RegisteredQuery::Polled(stale_query.clone()));
    drop(stale_query);
    assert!(registry.get(stale_id).is_none());

    assert_eq!(client.initialized_client_count().await, 1);
    assert_eq!(query_count.load(Ordering::SeqCst), 21);
    assert_eq!(do_get_count.load(Ordering::SeqCst), 17);
    assert_eq!(cancel_count.load(Ordering::SeqCst), 16);
    assert_eq!(first_result, vec![expected.clone()]);
    assert!(empty_result.is_empty());
    assert!(client.describe(Uuid::nil()).await.is_err());
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

#[test]
fn validates_metadata_with_header_map() {
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, "X-Custom-Header", "value").unwrap();
    assert_eq!(headers.get("x-custom-header").unwrap(), "value");
    assert!(insert_header(&mut headers, "bad header", "value").is_err());
    assert!(insert_header(&mut headers, "valid-header", "bad\nvalue").is_err());
}

#[tokio::test]
async fn adaptive_streams_start_on_one_rpc_for_both_inputs() {
    for parameters in [None, Some(parameter_row())] {
        let service = TestSqlService {
            adaptive: true,
            ..Default::default()
        };
        let polls = service.query_count.clone();
        let gets = service.do_get_count.clone();
        let exchanges = service.exchanges.clone();
        let expected = parameters.clone().unwrap_or_else(|| service.result.clone());
        let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let with_parameters = parameters.is_some();
        let query = client
            .execute(ExecuteQueryRequest {
                parameters,
                ..ExecuteQueryRequest::new("SELECT echo")
            })
            .await
            .unwrap();
        assert_eq!(query.describe().await.unwrap().status, QueryStatus::Running);
        assert_eq!(collect_result(&query).await.unwrap(), vec![expected]);
        assert_eq!(
            query.describe().await.unwrap().status,
            QueryStatus::Finished
        );
        assert!(query.reader().await.is_err());
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(gets.load(Ordering::SeqCst), usize::from(!with_parameters));
        assert_eq!(
            exchanges.lock().unwrap().len(),
            usize::from(with_parameters)
        );
    }
}

#[tokio::test]
async fn adaptive_handoffs_share_polling_and_survive_handle_drop() {
    for parameters in [None, Some(parameter_row())] {
        let service = TestSqlService {
            adaptive: true,
            defer: true,
            ..Default::default()
        };
        let polls = service.query_count.clone();
        let gets = service.do_get_count.clone();
        let exchanges = service.exchanges.clone();
        let expected = service.result.clone();
        let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let with_parameters = parameters.is_some();
        let query = client
            .execute(ExecuteQueryRequest {
                parameters,
                ..ExecuteQueryRequest::new("SELECT adaptive")
            })
            .await
            .unwrap();
        let id = query.id();
        assert_eq!(collect_result(&query).await.unwrap(), vec![expected]);
        assert_eq!(
            query.describe().await.unwrap().status,
            QueryStatus::Finished
        );
        assert!(query.reader().await.is_err());
        drop(query);
        assert_eq!(
            client.describe(id).await.unwrap().status,
            QueryStatus::Finished
        );
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "the original SQL must not be resubmitted through polling"
        );
        assert_eq!(
            gets.load(Ordering::SeqCst),
            1 + usize::from(!with_parameters)
        );
        assert_eq!(
            exchanges.lock().unwrap().len(),
            usize::from(with_parameters)
        );
    }
}

#[tokio::test]
async fn adaptive_handoffs_use_server_cancellation_for_both_inputs() {
    for parameters in [None, Some(parameter_row())] {
        let service = TestSqlService {
            adaptive: true,
            defer: true,
            ..Default::default()
        };
        let cancels = service.cancel_count.clone();
        let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let query = client
            .execute(ExecuteQueryRequest {
                parameters,
                ..ExecuteQueryRequest::new("SELECT cancel")
            })
            .await
            .unwrap();
        query.cancel().await.unwrap();
        assert_eq!(cancels.load(Ordering::SeqCst), 1);
        assert_eq!(
            query.describe().await.unwrap().status,
            QueryStatus::Cancelled
        );
        assert!(query.reader().await.is_err());
    }
}

#[tokio::test]
async fn malformed_adaptive_decision_never_resubmits_the_statement() {
    for parameters in [None, Some(parameter_row())] {
        let service = TestSqlService {
            adaptive: true,
            invalid_decision: true,
            ..Default::default()
        };
        let polls = service.query_count.clone();
        let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;
        assert!(
            client
                .execute(ExecuteQueryRequest {
                    parameters,
                    ..ExecuteQueryRequest::new("SELECT invalid")
                })
                .await
                .is_err()
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn remote_drop_materialized_view_uses_a_query_job() {
    for defer in [false, true] {
        let service = TestSqlService {
            adaptive: true,
            defer,
            ..Default::default()
        };
        let statements = service.statements.clone();
        let headers = service.headers.clone();
        let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let db = RemoteDatabase::new_mock(|request| -> http::Response<String> {
            panic!("unexpected REST request: {}", request.url().path());
        })
        .with_sql_client(client.clone());
        let job = db
            .drop_materialized_view_async("daily_sales", &["team".into(), "reports".into()])
            .await
            .unwrap();
        assert_eq!(job.job_type().as_deref(), Some("query"));
        let id = Uuid::parse_str(job.id().unwrap()).unwrap();
        job.wait().await.unwrap();
        job.wait().await.unwrap();
        assert_eq!(
            client.describe(id).await.unwrap().status,
            QueryStatus::Finished
        );
        assert_eq!(
            *statements.lock().unwrap(),
            ["DROP MATERIALIZED VIEW \"daily_sales\""]
        );
        assert!(
            headers
                .lock()
                .unwrap()
                .iter()
                .all(|header| header.namespace_path == "team$reports")
        );
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn remote_optimize_submits_a_compaction_job_over_sql() {
    let service = TestSqlService {
        adaptive: true,
        ..Default::default()
    };
    let statements = service.statements.clone();
    let headers = service.headers.clone();
    let (sql_client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let client = client_with_handler(|request| -> http::Response<String> {
        panic!("unexpected REST request: {}", request.url().path());
    });
    let table = RemoteTable::new_with_sql_client(
        client,
        "analytics".into(),
        "events".into(),
        vec!["team".into(), "reports".into()],
        "team$reports$events".into(),
        Default::default(),
        Some(sql_client),
    );

    let stats = table
        .optimize(crate::table::optimize::OptimizeAction::Compact {
            options: crate::table::optimize::CompactionOptions::default(),
            remap_options: None,
        })
        .await
        .unwrap();

    // The SQL job adapter does not expose compaction metrics, so
    // `compaction` is a zeroed placeholder, not real numbers -- but it must
    // be `Some`: callers (including the Python/Node bindings) unwrap it
    // unconditionally on success.
    assert_eq!(
        stats.compaction,
        Some(lance::dataset::optimize::CompactionMetrics::default())
    );
    assert!(stats.prune.is_none());
    assert_eq!(
        *statements.lock().unwrap(),
        ["OPTIMIZE \"events\" REWRITE DATA"]
    );
    assert!(
        headers
            .lock()
            .unwrap()
            .iter()
            .all(|header| header.namespace_path == "team$reports")
    );
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn remote_optimize_on_a_branch_submits_a_with_clause_over_sql() {
    let service = TestSqlService {
        adaptive: true,
        ..Default::default()
    };
    let statements = service.statements.clone();
    let (sql_client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let client = client_with_handler(|request| -> http::Response<String> {
        panic!("unexpected REST request: {}", request.url().path());
    });
    let table = RemoteTable::new_with_sql_client(
        client,
        "analytics".into(),
        "events".into(),
        vec!["team".into(), "reports".into()],
        "team$reports$events".into(),
        Default::default(),
        Some(sql_client),
    )
    .with_branch(Some("o'brien".to_string()));

    table
        .optimize(crate::table::optimize::OptimizeAction::Compact {
            options: crate::table::optimize::CompactionOptions::default(),
            remap_options: None,
        })
        .await
        .unwrap();

    assert_eq!(
        *statements.lock().unwrap(),
        ["OPTIMIZE \"events\" REWRITE DATA WITH (branch = 'o''brien')"]
    );
    shutdown.send(()).unwrap();
}

/// Regression test for the language bindings' `optimize()` convenience
/// method, which used to submit a compaction job and then fail on the next
/// action because `Prune` and `Index` are not supported remotely -- so a
/// retry on that failure resubmitted another compaction job. `optimize_full`
/// is what both the Python and Node bindings now call instead of
/// orchestrating `Compact`, `Prune`, and `Index` themselves; on a remote
/// table it must submit only the compaction job and return zeroed prune
/// stats rather than attempting `Prune` and `Index` at all.
#[tokio::test]
async fn remote_optimize_full_skips_prune_and_index() {
    let service = TestSqlService {
        adaptive: true,
        ..Default::default()
    };
    let statements = service.statements.clone();
    let (sql_client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let client = client_with_handler(|request| -> http::Response<String> {
        panic!("unexpected REST request: {}", request.url().path());
    });
    let remote_table = RemoteTable::new_with_sql_client(
        client,
        "analytics".into(),
        "events".into(),
        vec!["team".into(), "reports".into()],
        "team$reports$events".into(),
        Default::default(),
        Some(sql_client),
    );
    let database = Arc::new(RemoteDatabase::new_mock(
        |request: reqwest::Request| -> http::Response<String> {
            panic!("unexpected REST request: {}", request.url().path());
        },
    ));
    let table = crate::Table::new(Arc::new(remote_table), database);

    let stats = table
        .optimize_full(
            crate::table::optimize::CompactionOptions::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Compaction was submitted (and its placeholder stats are present)...
    assert_eq!(
        stats.compaction,
        Some(lance::dataset::optimize::CompactionMetrics::default())
    );
    // ...but Prune and Index were skipped rather than attempted and failed,
    // so the call succeeds with zeroed prune stats instead of returning an
    // error after the job has already been submitted.
    assert_eq!(
        stats.prune,
        Some(lance::dataset::cleanup::RemovalStats::default())
    );
    assert_eq!(
        *statements.lock().unwrap(),
        ["OPTIMIZE \"events\" REWRITE DATA"]
    );
    shutdown.send(()).unwrap();
}

/// `delete_unverified: Some(false)` is what every real Python call sends
/// (the public `optimize()` default is `False`, not `None`), so it must be
/// treated the same as "not asking to prune" -- not rejected -- or a plain
/// `table.optimize()` from Python would never succeed on a remote table.
#[tokio::test]
async fn remote_optimize_full_treats_delete_unverified_false_as_default() {
    let service = TestSqlService {
        adaptive: true,
        ..Default::default()
    };
    let statements = service.statements.clone();
    let (sql_client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let client = client_with_handler(|request| -> http::Response<String> {
        panic!("unexpected REST request: {}", request.url().path());
    });
    let remote_table = RemoteTable::new_with_sql_client(
        client,
        "analytics".into(),
        "events".into(),
        vec!["team".into(), "reports".into()],
        "team$reports$events".into(),
        Default::default(),
        Some(sql_client),
    );
    let database = Arc::new(RemoteDatabase::new_mock(
        |request: reqwest::Request| -> http::Response<String> {
            panic!("unexpected REST request: {}", request.url().path());
        },
    ));
    let table = crate::Table::new(Arc::new(remote_table), database);

    let stats = table
        .optimize_full(
            crate::table::optimize::CompactionOptions::default(),
            None,
            None,
            Some(false),
        )
        .await
        .unwrap();

    assert_eq!(
        stats.prune,
        Some(lance::dataset::cleanup::RemovalStats::default())
    );
    assert_eq!(statements.lock().unwrap().len(), 1);
    shutdown.send(()).unwrap();
}

/// Regression test: a caller who explicitly asks a remote table's
/// `optimize()` to prune (an older-than cutoff, an absolute timestamp, or
/// `delete_unverified=True`) used to get a success result reporting zero
/// bytes removed, even though no pruning was attempted at all -- silently
/// dropping their storage-reclaim request. It must instead reject before
/// anything is submitted, the same way a non-default `CompactionOptions` is.
#[tokio::test]
async fn remote_optimize_full_rejects_explicit_pruning_options() {
    async fn assert_rejected_before_submission(
        older_than: Option<chrono::Duration>,
        before_timestamp: Option<chrono::DateTime<chrono::Utc>>,
        delete_unverified: Option<bool>,
    ) {
        let service = TestSqlService {
            adaptive: true,
            ..Default::default()
        };
        let statements = service.statements.clone();
        let (sql_client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let client = client_with_handler(|request| -> http::Response<String> {
            panic!("unexpected REST request: {}", request.url().path());
        });
        let remote_table = RemoteTable::new_with_sql_client(
            client,
            "analytics".into(),
            "events".into(),
            vec!["team".into(), "reports".into()],
            "team$reports$events".into(),
            Default::default(),
            Some(sql_client),
        );
        let database = Arc::new(RemoteDatabase::new_mock(
            |request: reqwest::Request| -> http::Response<String> {
                panic!("unexpected REST request: {}", request.url().path());
            },
        ));
        let table = crate::Table::new(Arc::new(remote_table), database);

        let error = table
            .optimize_full(
                crate::table::optimize::CompactionOptions::default(),
                older_than,
                before_timestamp,
                delete_unverified,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::NotSupported { .. }), "{error:?}");
        assert!(statements.lock().unwrap().is_empty());
        shutdown.send(()).unwrap();
    }

    assert_rejected_before_submission(Some(chrono::Duration::days(1)), None, None).await;
    assert_rejected_before_submission(None, Some(chrono::Utc::now()), None).await;
    assert_rejected_before_submission(None, None, Some(true)).await;
}

fn sql_listing_batch(column: &str, values: &[&str], token: Option<&str>) -> RecordBatch {
    let mut columns: Vec<(&str, ArrayRef)> = Vec::new();
    if column == "name" {
        columns.push((
            "namespace_path",
            Arc::new(StringArray::from(vec![
                r#"["team","reports"]"#;
                values.len()
            ])),
        ));
    }
    columns.push((column, Arc::new(StringArray::from(values.to_vec()))));
    let batch = RecordBatch::try_from_iter(columns).unwrap();
    let metadata = token
        .map(|token| HashMap::from([("lancedb.next_page_token".to_owned(), token.to_owned())]))
        .unwrap_or_default();
    let schema = Arc::new(batch.schema().as_ref().clone().with_metadata(metadata));
    RecordBatch::try_new(schema, batch.columns().to_vec()).unwrap()
}

#[tokio::test]
async fn remote_mv_listing_uses_server_continuation_tokens() {
    for defer in [false, true] {
        let token = "server'opaque:token";
        let next_sql =
            "SHOW MATERIALIZED VIEWS IN \"team$reports\" LIMIT 2 FROM 'server''opaque:token'";
        let service = TestSqlService {
            adaptive: true,
            defer,
            result_overrides: Arc::new(std::sync::Mutex::new(HashMap::from([
                (
                    "SHOW MATERIALIZED VIEWS IN \"team$reports\" LIMIT 2".to_owned(),
                    sql_listing_batch("name", &["a", "b"], Some(token)),
                ),
                (next_sql.to_owned(), sql_listing_batch("name", &["z"], None)),
            ]))),
            ..Default::default()
        };
        let statements = service.statements.clone();
        let headers = service.headers.clone();
        let (client, shutdown) = start_sql_service_with_prefix(
            service,
            ClientConfig::default(),
            Some("tenant/production"),
        )
        .await;
        let db = RemoteDatabase::new_mock(|_| -> http::Response<String> {
            panic!("unexpected REST request")
        })
        .with_sql_client(client);
        let namespace = vec!["team".to_owned(), "reports".to_owned()];
        let first = db
            .list_materialized_views(
                &namespace,
                crate::listing::ListingOptions::default().page_limit(2),
            )
            .await
            .unwrap();
        assert_eq!(first.items, ["a", "b"]);
        assert_eq!(first.page_token.as_deref(), Some(token));
        let second = db
            .list_materialized_views(
                &namespace,
                crate::listing::ListingOptions::default()
                    .page_limit(2)
                    .page_token(first.page_token.unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(second.items, ["z"]);
        assert!(second.page_token.is_none());
        assert_eq!(
            *statements.lock().unwrap(),
            [
                "SHOW MATERIALIZED VIEWS IN \"team$reports\" LIMIT 2",
                next_sql
            ]
        );
        for header in headers.lock().unwrap().iter() {
            assert_eq!(header.database, "analytics");
            assert_eq!(header.database_prefix, "tenant/production");
            assert_eq!(header.namespace_path, "team$reports");
        }
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn remote_describe_materialized_view_preserves_the_sql_definition() {
    for defer in [false, true] {
        let definition = "SELECT s.x, d.label FROM source s JOIN dim d ON s.id = d.id";
        let result =
            description_batch(serde_json::json!({"name":"daily_sales", "query":definition}));
        let service = TestSqlService {
            adaptive: true,
            defer,
            result,
            ..Default::default()
        };
        let statements = service.statements.clone();
        let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let table = RemoteTable::new_with_sql_client(
            client_with_handler(|request| -> http::Response<String> {
                panic!("unexpected REST request: {}", request.url().path());
            }),
            "analytics".into(),
            "daily_sales".into(),
            vec!["analytics".into()],
            "analytics$daily_sales".into(),
            Default::default(),
            Some(client),
        );
        let info = table.materialized_view_info().await.unwrap();
        assert_eq!(info.definition_sql, definition);
        assert!(info.parsed_definition.is_none());
        assert_eq!(
            *statements.lock().unwrap(),
            ["DESCRIBE MATERIALIZED VIEW \"daily_sales\" FORMAT JSON"]
        );
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn storage_integration_methods_use_sql_with_namespace_and_typed_metadata() {
    let info: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/storage_integration.json"
    ))
    .unwrap();
    let columns: Vec<(String, ArrayRef)> = info
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, value)| {
            let column: ArrayRef = match value {
                serde_json::Value::Bool(value) => {
                    Arc::new(arrow_array::BooleanArray::from(vec![*value]))
                }
                serde_json::Value::Number(value) => {
                    Arc::new(Int64Array::from(vec![value.as_i64().unwrap()]))
                }
                serde_json::Value::Null => Arc::new(StringArray::from(vec![None::<String>])),
                serde_json::Value::String(value) => {
                    Arc::new(StringArray::from(vec![value.clone()]))
                }
                value => Arc::new(StringArray::from(vec![value.to_string()])),
            };
            (name.clone(), column)
        })
        .collect();
    for defer in [false, true] {
        let service = TestSqlService {
            result: RecordBatch::try_from_iter(columns.clone()).unwrap(),
            adaptive: true,
            defer,
            ..Default::default()
        };
        let statements = service.statements.clone();
        let headers = service.headers.clone();
        let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let database = RemoteDatabase::new_mock(|request| -> http::Response<String> {
            panic!(
                "Storage integration operation used REST: {}",
                request.url().path()
            );
        })
        .with_sql_client(client);
        let namespace = vec!["platform".into(), "aws".into()];
        let configuration = CreateStorageIntegrationRequest::s3(
            "s3://customer-bucket/raw/",
            "arn:aws:iam::123456789012:role/access",
        )
        .with_aws_region("us-east-1");
        let created = database
            .create_storage_integration("raw", configuration, &namespace)
            .await
            .unwrap();
        assert_eq!(created.database, "default");
        assert_eq!(
            created.trust_policy["Statement"][0]["Condition"]["StringEquals"]["sts:ExternalId"],
            created.aws_external_id
        );
        assert_eq!(
            database
                .describe_storage_integration("raw", &namespace)
                .await
                .unwrap()
                .aws_external_id,
            created.aws_external_id
        );
        assert_eq!(
            database
                .list_storage_integrations(&namespace, Default::default())
                .await
                .unwrap()
                .items,
            ["raw"]
        );
        database
            .drop_storage_integration("raw", &namespace)
            .await
            .unwrap();
        let credential = SecretReference::new("key")
            .with_database("platform/raw")
            .with_namespace(vec!["credentials".into()]);
        database
            .create_storage_integration(
                "raw",
                CreateStorageIntegrationRequest::s3_with_secrets(
                    "s3://customer-bucket/raw/",
                    credential.clone(),
                    credential,
                ),
                &namespace,
            )
            .await
            .unwrap();
        let statements = statements.lock().unwrap();
        assert!(statements[0].contains("STORAGE_AWS_REGION = 'us-east-1'"));
        assert_eq!(statements[1], "DESCRIBE STORAGE INTEGRATION \"raw\"");
        assert_eq!(
            statements[2],
            "SHOW STORAGE INTEGRATIONS IN \"platform$aws\" LIMIT 1000"
        );
        assert_eq!(statements[3], "DROP STORAGE INTEGRATION \"raw\"");
        assert!(statements[4].contains(
            "STORAGE_AWS_ACCESS_KEY_ID_SECRET = \"platform/raw\".\"credentials\".\"key\""
        ));
        assert!(
            headers
                .lock()
                .unwrap()
                .iter()
                .all(|header| header.namespace_path == "platform$aws")
        );
        let _ = shutdown.send(());
    }
}

#[tokio::test]
async fn function_creation_and_description_use_sql_query_handles() {
    const VERSION: &str = include_str!(
        "../../tests/fixtures/first_class_functions/v1/remote_function_version.canonical.json"
    );
    const REQUEST: &str = include_str!(
        "../../tests/fixtures/first_class_functions/v1/remote_function_registration_request.json"
    );
    for (defer, with_models, exist_ok) in [
        (false, false, false),
        (false, true, false),
        (true, false, false),
        (true, true, false),
        (false, false, true),
        (false, true, true),
        (true, false, true),
        (true, true, true),
    ] {
        let mut request_json: serde_json::Value = serde_json::from_str(REQUEST).unwrap();
        request_json["batch_size"] = serde_json::json!(2);
        request_json["max_concurrency"] = serde_json::json!(4);
        request_json["total_cpu_millis"] = serde_json::json!(8000);
        request_json["total_memory_bytes"] = serde_json::json!(17179869184_u64);
        request_json["total_gpu_count"] = serde_json::json!(0);
        request_json["row_timeout_seconds"] = serde_json::json!(0.75);
        request_json["secret_bindings"] = serde_json::json!([{
            "kind": "env",
            "variable": "HF_TOKEN",
            "secret_ref": {"name": "hf-prod"},
        }]);
        let mut version_json: serde_json::Value = serde_json::from_str(VERSION).unwrap();
        version_json["batch_size"] = serde_json::json!(12);
        for field in ["total_cpu_millis", "total_memory_bytes", "total_gpu_count"] {
            version_json[field] = request_json[field].clone();
        }
        version_json["max_concurrency"] = serde_json::json!(4);
        version_json["row_timeout_seconds"] = serde_json::json!(8.0);
        version_json["result_stability"] = serde_json::json!("input_and_context");
        version_json["runtime"] = request_json["runtime"].clone();
        let requested_index_url = "https://requested.example/simple/";
        version_json["runtime"]["environment"]["build"] = serde_json::json!({
            "index_url": requested_index_url,
            "find_links": ["https://downloads.example/wheels"],
        });
        version_json["resolved_environment"] = serde_json::json!({
            "packages": [
                {
                    "name": "torch",
                    "version": "2.9.0+cu130",
                    "source": "https://packages.example/simple"
                },
                {
                    "name": "transformers",
                    "version": "4.57.1"
                }
            ],
            "provenance": {
                "configured_index_urls": ["https://packages.example/simple"],
                "configured_find_links": [],
                "resolved_indexes": ["https://packages.example/simple"],
                "cuda_toolkit": "12.8",
                "torch_version": "torch==2.9.0+cu130"
            },
            "warnings": [
                "transformers resolved to transformers==4.57.1 but is not pinned exactly; add transformers==4.57.1 to pin it."
            ]
        });
        version_json["resolved_environments"] = serde_json::json!({
            "linux/amd64": {
                "packages": [{"name": "torch", "version": "2.9.0+cu130"}]
            },
            "linux/arm64": {
                "packages": []
            }
        });
        if with_models {
            let models = serde_json::json!([{
                "repo": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "1110a243fdf4706b3f48f1d95db1a4f5529b4d41",
            }]);
            request_json["models"] = models.clone();
            version_json["image"]["models"] = models;
        }
        let expected_models = version_json["image"].get("models");
        let mut expected_request = request_json.clone();
        expected_request.as_object_mut().unwrap().remove("name");
        let mut request =
            crate::function::FunctionRegistrationRequest::from_json(&request_json.to_string())
                .unwrap();
        assert!(!request.exist_ok);
        request.exist_ok = exist_ok;
        let result = RecordBatch::try_from_iter(vec![(
            "definition",
            Arc::new(StringArray::from(vec![version_json.to_string()])) as ArrayRef,
        )])
        .unwrap();
        let service = TestSqlService {
            adaptive: true,
            defer,
            result,
            ..Default::default()
        };
        let statements = service.statements.clone();
        let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let db = RemoteDatabase::new_mock(|request| -> http::Response<String> {
            panic!("unexpected REST request: {}", request.url().path());
        })
        .with_sql_client(client.clone());
        let job = db
            .create_function_async(request, &["team".into()])
            .await
            .unwrap();
        assert_eq!(job.job_type().as_deref(), Some("query"));
        let id = Uuid::parse_str(job.id().unwrap()).unwrap();
        let created = job.wait().await.unwrap();
        assert_eq!(created.name(), "embed");
        assert_eq!(created.version(), "1");
        assert_eq!(created.batch_size(), Some(12));
        assert_eq!(created.total_cpu_millis(), Some(8000));
        assert_eq!(created.total_memory_bytes(), Some(17179869184));
        assert_eq!(created.total_gpu_count(), Some(0));
        assert_eq!(created.max_concurrency(), Some(4));
        assert_eq!(created.row_timeout_seconds(), Some(8.0));
        assert_eq!(created.result_stability(), Some("input_and_context"));
        assert_eq!(created.runtime().unwrap().kind(), "python");
        assert_eq!(
            created.resolved_environment().unwrap().packages[0].version,
            "2.9.0+cu130"
        );
        assert_eq!(created.resolved_environments().len(), 2);
        assert!(
            created.resolved_environments()["linux/arm64"]
                .packages
                .is_empty()
        );
        // Assert the actual SQL body, after the same native JSON decode used
        // by the Python binding, rather than only the Python declaration.
        {
            let seen = statements.lock().unwrap();
            let encoded = seen[0]
                .strip_prefix(&format!(
                    "CREATE FUNCTION{} \"normalize_score\" FROM '",
                    if exist_ok { " IF NOT EXISTS" } else { "" }
                ))
                .unwrap()
                .strip_suffix('\'')
                .unwrap();
            let sent: serde_json::Value =
                serde_json::from_str(&encoded.replace("''", "'")).unwrap();
            assert_eq!(sent, expected_request);
        }
        let created_json: serde_json::Value =
            serde_json::from_str(&created.to_canonical_json().unwrap()).unwrap();
        assert_eq!(created_json["image"].get("models"), expected_models);
        assert_eq!(created_json["batch_size"], 12);
        assert_eq!(created_json["max_concurrency"], 4);
        assert_eq!(created_json["row_timeout_seconds"], 8.0);
        assert_eq!(created_json["result_stability"], "input_and_context");
        assert_eq!(
            created_json["resolved_environment"]["packages"][0]["version"],
            "2.9.0+cu130"
        );
        assert!(
            created_json["resolved_environment"]["packages"][1]
                .get("source")
                .is_none()
        );
        assert_eq!(
            created_json["resolved_environments"]["linux/amd64"]["packages"][0]["version"],
            "2.9.0+cu130"
        );
        assert_eq!(
            created_json["resolved_environments"]["linux/arm64"]["packages"],
            serde_json::json!([])
        );
        assert_eq!(
            client.describe(id).await.unwrap().status,
            QueryStatus::Finished
        );
        let described = db
            .get_function("embed", "1", &["team".into()])
            .await
            .unwrap();
        assert_eq!(described.name(), "embed");
        assert_eq!(described.batch_size(), Some(12));
        assert_eq!(described.max_concurrency(), Some(4));
        assert_eq!(described.row_timeout_seconds(), Some(8.0));
        assert_eq!(described.result_stability(), Some("input_and_context"));
        assert_eq!(described.runtime().unwrap().kind(), "python");
        assert_eq!(described.requested_index_url(), Some(requested_index_url));
        assert_eq!(
            described
                .resolved_environment()
                .unwrap()
                .provenance
                .as_ref()
                .unwrap()
                .torch_version
                .as_deref(),
            Some("torch==2.9.0+cu130")
        );
        assert_eq!(described.resolved_environments().len(), 2);
        let described_json: serde_json::Value =
            serde_json::from_str(&described.to_canonical_json().unwrap()).unwrap();
        assert_eq!(described_json["image"].get("models"), expected_models);
        assert_eq!(described_json["batch_size"], 12);
        assert_eq!(described_json["max_concurrency"], 4);
        assert_eq!(described_json["row_timeout_seconds"], 8.0);
        assert_eq!(described_json["result_stability"], "input_and_context");
        assert_eq!(
            described_json["runtime"]["environment"]["build"],
            serde_json::json!({"index_url": requested_index_url})
        );
        assert_eq!(
            described_json["resolved_environment"]["provenance"]["configured_index_urls"],
            serde_json::json!(["https://packages.example/simple"])
        );
        let latest = db
            .get_function_latest("embed", &["team".into()])
            .await
            .unwrap();
        assert_eq!(latest.name(), "embed");
        assert_eq!(latest.version(), "1");
        assert_eq!(latest.batch_size(), Some(12));
        let listed = db
            .list_functions(&["team".into()], Default::default())
            .await
            .unwrap();
        assert_eq!(listed.items.len(), 1);
        assert_eq!(listed.items[0].batch_size(), Some(12));
        assert_eq!(listed.items[0].max_concurrency(), Some(4));
        assert_eq!(listed.items[0].row_timeout_seconds(), Some(8.0));
        assert_eq!(
            listed.items[0].result_stability(),
            Some("input_and_context")
        );
        assert_eq!(listed.items[0].runtime().unwrap().kind(), "python");
        assert_eq!(
            listed.items[0]
                .resolved_environment()
                .unwrap()
                .warnings
                .len(),
            1
        );
        assert_eq!(listed.items[0].resolved_environments().len(), 2);
        let listed_json: serde_json::Value =
            serde_json::from_str(&listed.items[0].to_canonical_json().unwrap()).unwrap();
        assert_eq!(listed_json["image"].get("models"), expected_models);
        assert_eq!(listed_json["batch_size"], 12);
        assert_eq!(listed_json["max_concurrency"], 4);
        assert_eq!(listed_json["row_timeout_seconds"], 8.0);
        assert_eq!(listed_json["result_stability"], "input_and_context");
        let seen = statements.lock().unwrap();
        assert_eq!(seen[1], "DESCRIBE FUNCTION \"embed\" WITH (version = '1')");
        assert_eq!(seen[2], "DESCRIBE FUNCTION \"embed\"");
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn function_creation_refresh_lock_is_forwarded_only_when_true() {
    const VERSION: &str = include_str!(
        "../../tests/fixtures/first_class_functions/v1/remote_function_version.canonical.json"
    );
    const REQUEST: &str = include_str!(
        "../../tests/fixtures/first_class_functions/v1/remote_function_registration_request.json"
    );
    for (refresh_lock, expect_field) in [(false, false), (true, true)] {
        let mut request_json: serde_json::Value = serde_json::from_str(REQUEST).unwrap();
        if refresh_lock {
            request_json["refresh_lock"] = serde_json::json!(true);
        }
        let request =
            crate::function::FunctionRegistrationRequest::from_json(&request_json.to_string())
                .unwrap();
        assert_eq!(request.refresh_lock, refresh_lock);
        let result = RecordBatch::try_from_iter(vec![(
            "definition",
            Arc::new(StringArray::from(vec![VERSION.to_string()])) as ArrayRef,
        )])
        .unwrap();
        let service = TestSqlService {
            adaptive: true,
            defer: false,
            result,
            ..Default::default()
        };
        let statements = service.statements.clone();
        let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let db = RemoteDatabase::new_mock(|request| -> http::Response<String> {
            panic!("unexpected REST request: {}", request.url().path());
        })
        .with_sql_client(client.clone());
        let job = db.create_function_async(request, &[]).await.unwrap();
        job.wait().await.unwrap();
        let seen = statements.lock().unwrap();
        let encoded = seen[0]
            .strip_prefix(r#"CREATE FUNCTION "normalize_score" FROM '"#)
            .unwrap()
            .strip_suffix('\'')
            .unwrap();
        let sent: serde_json::Value = serde_json::from_str(&encoded.replace("''", "'")).unwrap();
        assert_eq!(sent.get("refresh_lock").is_some(), expect_field);
        if expect_field {
            assert_eq!(sent["refresh_lock"], serde_json::json!(true));
        }
        drop(seen);
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn function_creation_rejects_invalid_execution_controls_before_sql() {
    const REQUEST: &str = include_str!(
        "../../tests/fixtures/first_class_functions/v1/remote_function_controlled_registration_request.json"
    );
    let service = TestSqlService::default();
    let statements = service.statements.clone();
    let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let db = RemoteDatabase::new_mock(|request| -> http::Response<String> {
        panic!("unexpected REST request: {}", request.url().path());
    })
    .with_sql_client(client.clone());

    for timeout in [f64::NAN, f64::INFINITY, -1.0] {
        let mut request = crate::function::FunctionRegistrationRequest::from_json(REQUEST).unwrap();
        request.row_timeout_seconds = Some(timeout);
        let error = db.create_function_async(request, &[]).await.unwrap_err();
        assert!(
            matches!(error, Error::InvalidInput { message } if message.contains("row_timeout_seconds"))
        );
    }

    let mut request = crate::function::FunctionRegistrationRequest::from_json(REQUEST).unwrap();
    request.batch_size = Some(0);
    let error = db.create_function_async(request, &[]).await.unwrap_err();
    assert!(matches!(error, Error::InvalidInput { message } if message.contains("batch_size")));

    let mut request = crate::function::FunctionRegistrationRequest::from_json(REQUEST).unwrap();
    request.max_concurrency = Some(0);
    let error = db.create_function_async(request, &[]).await.unwrap_err();
    assert!(
        matches!(error, Error::InvalidInput { message } if message.contains("max_concurrency"))
    );
    assert!(statements.lock().unwrap().is_empty());
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn function_drop_keeps_the_query_id_and_reports_catalog_removal() {
    for (status, expected) in [("dropped", true), ("not_found", false)] {
        let result = RecordBatch::try_from_iter(vec![(
            "status",
            Arc::new(StringArray::from(vec![status])) as ArrayRef,
        )])
        .unwrap();
        let service = TestSqlService {
            adaptive: true,
            defer: true,
            result,
            ..Default::default()
        };
        let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let db = RemoteDatabase::new_mock(|_| -> http::Response<String> {
            panic!("unexpected REST request");
        })
        .with_sql_client(client.clone());
        let (dropped, job) = db.drop_function_async("embed", "1", &[]).await.unwrap();
        assert_eq!(dropped, expected);
        assert_eq!(job.job_type().as_deref(), Some("query"));
        let id = Uuid::parse_str(job.id().unwrap()).unwrap();
        job.wait().await.unwrap();
        job.wait().await.unwrap();
        assert_eq!(
            client.describe(id).await.unwrap().status,
            QueryStatus::Finished
        );
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn function_drop_decodes_server_warnings_for_the_binding_bridge() {
    let mut warning_values = ListBuilder::new(StringBuilder::new());
    warning_values
        .values()
        .append_value("materialized view \"pinned\" references Function \"embed\" version \"1\"");
    warning_values.append(true);
    let result = RecordBatch::try_from_iter(vec![
        (
            "status",
            Arc::new(StringArray::from(vec!["dropped"])) as ArrayRef,
        ),
        ("warnings", Arc::new(warning_values.finish()) as ArrayRef),
    ])
    .unwrap();
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        result,
        ..Default::default()
    };
    let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let db = RemoteDatabase::new_mock(|_| -> http::Response<String> {
        panic!("unexpected REST request");
    })
    .with_sql_client(client);

    let (dropped, warnings) = db
        .drop_function_with_warnings("embed", "1", &[])
        .await
        .unwrap();
    assert!(dropped);
    assert_eq!(
        warnings,
        ["materialized view \"pinned\" references Function \"embed\" version \"1\""]
    );
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn secret_writes_bind_credentials_and_never_put_them_in_sql() {
    for defer in [false, true] {
        let service = TestSqlService {
            adaptive: true,
            defer,
            ..Default::default()
        };
        let exchanges = service.exchanges.clone();
        let statements = service.statements.clone();
        let headers = service.headers.clone();
        let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let db = RemoteDatabase::new_mock(|request| -> http::Response<String> {
            panic!("unexpected REST request: {}", request.url().path());
        })
        .with_sql_client(client);
        let connection = Connection::new(
            Arc::new(db),
            Arc::new(crate::embeddings::MemoryRegistry::new()),
        );
        connection
            .create_secret("api-key", "credential's value", &["team".into()])
            .await
            .unwrap();
        connection
            .create_secret("api-key", "ignored-value", &["team".into()])
            .exist_ok(true)
            .await
            .unwrap();
        connection
            .alter_secret("api-key", "rotated-value", &["team".into()])
            .await
            .unwrap();
        connection
            .drop_secret("api-key", &["team".into()])
            .await
            .unwrap();
        assert_eq!(
            statements.lock().unwrap().as_slice(),
            ["DROP SECRET IF EXISTS \"api-key\""]
        );
        let seen = exchanges.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert_eq!(
            seen[0].0,
            "CREATE SECRET \"api-key\" TYPE = GENERIC_STRING SECRET_STRING = $1"
        );
        assert_eq!(
            seen[1].0,
            "CREATE SECRET IF NOT EXISTS \"api-key\" TYPE = GENERIC_STRING SECRET_STRING = $1"
        );
        assert_eq!(seen[2].0, "ALTER SECRET \"api-key\" SET SECRET_STRING = $1");
        assert!(
            headers
                .lock()
                .unwrap()
                .iter()
                .all(|header| header.namespace_path == "team")
        );
        assert_eq!(
            seen[0]
                .1
                .as_ref()
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "credential's value"
        );
        assert_eq!(
            seen[1]
                .1
                .as_ref()
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "ignored-value"
        );
        assert_eq!(
            seen[2]
                .1
                .as_ref()
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "rotated-value"
        );
        shutdown.send(()).unwrap();
    }
}
#[tokio::test]
async fn secret_description_uses_the_sql_metadata_projection() {
    let result = RecordBatch::try_from_iter(vec![
        (
            "name",
            Arc::new(StringArray::from(vec!["api-key"])) as ArrayRef,
        ),
        (
            "created_at_millis",
            Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef,
        ),
        (
            "updated_at_millis",
            Arc::new(arrow_array::Int64Array::from(vec![2])) as ArrayRef,
        ),
    ])
    .unwrap();
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        result,
        ..Default::default()
    };
    let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let db = RemoteDatabase::new_mock(|_| -> http::Response<String> {
        panic!("unexpected REST request");
    })
    .with_sql_client(client);
    let info = db.describe_secret("api-key", &[]).await.unwrap();
    assert_eq!(info.name, "api-key");
    assert_eq!(info.updated_at_millis, 2);
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn secret_listing_uses_server_continuation_tokens() {
    for defer in [false, true] {
        let token = "server'opaque:token";
        let next_sql = "SHOW SECRETS IN \"team$reports\" LIMIT 2 FROM 'server''opaque:token'";
        let service = TestSqlService {
            adaptive: true,
            defer,
            result_overrides: Arc::new(std::sync::Mutex::new(HashMap::from([
                (
                    "SHOW SECRETS IN \"team$reports\" LIMIT 2".to_owned(),
                    sql_listing_batch("name", &["a", "b"], Some(token)),
                ),
                (next_sql.to_owned(), sql_listing_batch("name", &["z"], None)),
            ]))),
            ..Default::default()
        };
        let statements = service.statements.clone();
        let headers = service.headers.clone();
        let (client, shutdown) = start_sql_service_with_prefix(
            service,
            ClientConfig::default(),
            Some("tenant/production"),
        )
        .await;
        let db = RemoteDatabase::new_mock(|_| -> http::Response<String> {
            panic!("unexpected REST request")
        })
        .with_sql_client(client);
        let namespace = vec!["team".to_owned(), "reports".to_owned()];
        let first = db
            .list_secrets(
                &namespace,
                crate::listing::ListingOptions::default().page_limit(2),
            )
            .await
            .unwrap();
        assert_eq!(first.items, ["a", "b"]);
        assert_eq!(first.page_token.as_deref(), Some(token));
        let second = db
            .list_secrets(
                &namespace,
                crate::listing::ListingOptions::default()
                    .page_limit(2)
                    .page_token(first.page_token.unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(second.items, ["z"]);
        assert!(second.page_token.is_none());
        assert_eq!(
            *statements.lock().unwrap(),
            ["SHOW SECRETS IN \"team$reports\" LIMIT 2", next_sql]
        );
        for header in headers.lock().unwrap().iter() {
            assert_eq!(header.database, "analytics");
            assert_eq!(header.database_prefix, "tenant/production");
            assert_eq!(header.namespace_path, "team$reports");
        }
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn storage_integration_listing_uses_server_continuation_tokens() {
    for defer in [false, true] {
        let token = "server'opaque:token";
        let next_sql =
            "SHOW STORAGE INTEGRATIONS IN \"team$reports\" LIMIT 2 FROM 'server''opaque:token'";
        let service = TestSqlService {
            adaptive: true,
            defer,
            result_overrides: Arc::new(std::sync::Mutex::new(HashMap::from([
                (
                    "SHOW STORAGE INTEGRATIONS IN \"team$reports\" LIMIT 2".to_owned(),
                    sql_listing_batch("name", &["a", "b"], Some(token)),
                ),
                (next_sql.to_owned(), sql_listing_batch("name", &["z"], None)),
            ]))),
            ..Default::default()
        };
        let statements = service.statements.clone();
        let headers = service.headers.clone();
        let (client, shutdown) = start_sql_service_with_prefix(
            service,
            ClientConfig::default(),
            Some("tenant/production"),
        )
        .await;
        let db = RemoteDatabase::new_mock(|_| -> http::Response<String> {
            panic!("unexpected REST request")
        })
        .with_sql_client(client);
        let namespace = vec!["team".to_owned(), "reports".to_owned()];
        let first = db
            .list_storage_integrations(
                &namespace,
                crate::listing::ListingOptions::default().page_limit(2),
            )
            .await
            .unwrap();
        assert_eq!(first.items, ["a", "b"]);
        assert_eq!(first.page_token.as_deref(), Some(token));
        let second = db
            .list_storage_integrations(
                &namespace,
                crate::listing::ListingOptions::default()
                    .page_limit(2)
                    .page_token(first.page_token.unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(second.items, ["z"]);
        assert!(second.page_token.is_none());
        assert_eq!(
            *statements.lock().unwrap(),
            [
                "SHOW STORAGE INTEGRATIONS IN \"team$reports\" LIMIT 2",
                next_sql
            ]
        );
        for header in headers.lock().unwrap().iter() {
            assert_eq!(header.database, "analytics");
            assert_eq!(header.database_prefix, "tenant/production");
            assert_eq!(header.namespace_path, "team$reports");
        }
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn secret_listing_preserves_empty_continuations_and_validates_page_options() {
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        result_overrides: Arc::new(std::sync::Mutex::new(HashMap::from([
            (
                "SHOW SECRETS IN \"team$reports\" LIMIT 2".to_owned(),
                sql_listing_batch("name", &[], Some("continue-empty")),
            ),
            (
                "SHOW SECRETS IN \"team$reports\" LIMIT 2 FROM 'continue-empty'".to_owned(),
                sql_listing_batch("name", &["last"], None),
            ),
        ]))),
        ..Default::default()
    };
    let statements = service.statements.clone();
    let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let db = RemoteDatabase::new_mock(|_| -> http::Response<String> {
        panic!("unexpected REST request")
    })
    .with_sql_client(client);
    let namespace = vec!["team".to_owned(), "reports".to_owned()];
    let page = db
        .list_secrets(
            &namespace,
            crate::listing::ListingOptions::default().page_limit(2),
        )
        .await
        .unwrap();
    assert!(page.items.is_empty());
    assert_eq!(page.page_token.as_deref(), Some("continue-empty"));
    let last = db
        .list_secrets(
            &namespace,
            crate::listing::ListingOptions::default()
                .page_limit(2)
                .page_token(page.page_token.unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(last.items, ["last"]);
    assert!(last.page_token.is_none());
    for limit in [10_001, u32::MAX] {
        assert!(matches!(
            db.list_secrets(
                &namespace,
                crate::listing::ListingOptions::default().page_limit(limit)
            )
            .await,
            Err(Error::InvalidInput { .. })
        ));
    }
    assert!(
        db.list_secrets(
            &namespace,
            crate::listing::ListingOptions::default().page_token("")
        )
        .await
        .is_err()
    );
    let zero = db
        .list_secrets(
            &namespace,
            crate::listing::ListingOptions::default().page_limit(0),
        )
        .await
        .unwrap();
    assert!(zero.items.is_empty());
    assert!(zero.page_token.is_none());
    assert_eq!(statements.lock().unwrap().len(), 2);
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn function_listing_uses_server_tokens_for_numeric_revision_pages() {
    const VERSION: &str = include_str!(
        "../../tests/fixtures/first_class_functions/v1/remote_function_version.canonical.json"
    );
    let mut second: serde_json::Value = serde_json::from_str(VERSION).unwrap();
    second["version"] = serde_json::json!("2");
    let second = second.to_string();
    let mut tenth: serde_json::Value = serde_json::from_str(VERSION).unwrap();
    tenth["version"] = serde_json::json!("10");
    let tenth = tenth.to_string();
    for defer in [false, true] {
        let service = TestSqlService {
            adaptive: true,
            defer,
            result_overrides: Arc::new(std::sync::Mutex::new(HashMap::from([
                (
                    "SHOW FUNCTIONS HISTORY IN \"team\" LIMIT 2".to_owned(),
                    sql_listing_batch("definition", &[VERSION, &second], Some("revision-cursor")),
                ),
                (
                    "SHOW FUNCTIONS HISTORY IN \"team\" LIMIT 2 FROM 'revision-cursor'".to_owned(),
                    sql_listing_batch("definition", &[&tenth], None),
                ),
            ]))),
            ..Default::default()
        };
        let statements = service.statements.clone();
        let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let db = RemoteDatabase::new_mock(|_| -> http::Response<String> {
            panic!("unexpected REST request")
        })
        .with_sql_client(client);
        let first = db
            .list_functions(
                &["team".into()],
                crate::listing::ListingOptions::default().page_limit(2),
            )
            .await
            .unwrap();
        assert_eq!(
            first
                .items
                .iter()
                .map(|item| item.version())
                .collect::<Vec<_>>(),
            ["1", "2"]
        );
        assert_eq!(first.page_token.as_deref(), Some("revision-cursor"));
        let last = db
            .list_functions(
                &["team".into()],
                crate::listing::ListingOptions::default()
                    .page_limit(2)
                    .page_token(first.page_token.unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(last.items.len(), 1);
        assert_eq!(last.items[0].version(), "10");
        assert!(last.page_token.is_none());
        assert_eq!(
            *statements.lock().unwrap(),
            [
                "SHOW FUNCTIONS HISTORY IN \"team\" LIMIT 2",
                "SHOW FUNCTIONS HISTORY IN \"team\" LIMIT 2 FROM 'revision-cursor'"
            ]
        );
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn view_create_and_describe_return_metadata_through_sql() {
    let schema = r#"{"fields":[{"name":"name","nullable":true,"type":{"type":"utf8"}}],"metadata":{"lancedb::view::default_db":"db","lancedb::view::default_namespace":"team"}}"#;
    for defer in [false, true] {
        let result = RecordBatch::try_from_iter(vec![
            (
                "name",
                Arc::new(StringArray::from(vec!["adults"])) as ArrayRef,
            ),
            (
                "query",
                Arc::new(StringArray::from(vec!["SELECT name FROM people"])) as ArrayRef,
            ),
            (
                "schema",
                Arc::new(StringArray::from(vec![schema])) as ArrayRef,
            ),
        ])
        .unwrap();
        let service = TestSqlService {
            adaptive: true,
            defer,
            result,
            ..Default::default()
        };
        let statements = service.statements.clone();
        let headers = service.headers.clone();
        let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let db = RemoteDatabase::new_mock(|_| -> http::Response<String> {
            panic!("unexpected REST request");
        })
        .with_sql_client(client);
        let connection = Connection::new(Arc::new(db), Arc::new(MemoryRegistry::new()));
        let created = connection
            .create_view("adults", "SELECT name FROM people", &["team".into()])
            .await
            .unwrap();
        let existing = connection
            .create_view(
                "adults",
                "SELECT replacement FROM other_people",
                &["team".into()],
            )
            .exist_ok(true)
            .await
            .unwrap();
        let described = connection
            .describe_view("adults", &["team".into()])
            .await
            .unwrap();
        assert_eq!(created, described);
        assert_eq!(existing, described);
        assert_eq!(created.schema.fields()[0].name(), "name");
        assert_eq!(created.default_namespace_path, ["team"]);
        assert_eq!(
            statements.lock().unwrap().as_slice(),
            [
                "CREATE VIEW \"adults\" AS SELECT name FROM people",
                "CREATE VIEW IF NOT EXISTS \"adults\" AS SELECT replacement FROM other_people",
                "DESCRIBE VIEW \"adults\""
            ]
        );
        assert!(
            headers
                .lock()
                .unwrap()
                .iter()
                .all(|header| header.namespace_path == "team")
        );
        shutdown.send(()).unwrap();
    }
}
#[tokio::test]
async fn view_drop_uses_a_repeatable_query_job() {
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        ..Default::default()
    };
    let statements = service.statements.clone();
    let (client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let db = RemoteDatabase::new_mock(|_| -> http::Response<String> {
        panic!("unexpected REST request");
    })
    .with_sql_client(client.clone());
    let job = db
        .drop_view_async("adults", &["team".into()])
        .await
        .unwrap();
    assert_eq!(job.job_type().as_deref(), Some("query"));
    let id = Uuid::parse_str(job.id().unwrap()).unwrap();
    job.wait().await.unwrap();
    job.wait().await.unwrap();
    assert_eq!(
        client.describe(id).await.unwrap().status,
        QueryStatus::Finished
    );
    assert_eq!(
        statements.lock().unwrap().as_slice(),
        ["DROP VIEW \"adults\""]
    );
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn view_listing_uses_server_continuation_tokens() {
    for defer in [false, true] {
        let token = "server'opaque:token";
        let next_sql = "SHOW VIEWS IN \"team$reports\" LIMIT 2 FROM 'server''opaque:token'";
        let service = TestSqlService {
            adaptive: true,
            defer,
            result_overrides: Arc::new(std::sync::Mutex::new(HashMap::from([
                (
                    "SHOW VIEWS IN \"team$reports\" LIMIT 2".to_owned(),
                    sql_listing_batch("name", &["a", "b"], Some(token)),
                ),
                (next_sql.to_owned(), sql_listing_batch("name", &["z"], None)),
            ]))),
            ..Default::default()
        };
        let statements = service.statements.clone();
        let headers = service.headers.clone();
        let (client, shutdown) = start_sql_service_with_prefix(
            service,
            ClientConfig::default(),
            Some("tenant/production"),
        )
        .await;
        let db = RemoteDatabase::new_mock(|_| -> http::Response<String> {
            panic!("unexpected REST request")
        })
        .with_sql_client(client);
        let namespace = vec!["team".to_owned(), "reports".to_owned()];
        let first = db
            .list_views(
                &namespace,
                crate::listing::ListingOptions::default().page_limit(2),
            )
            .await
            .unwrap();
        assert_eq!(first.items, ["a", "b"]);
        assert_eq!(first.page_token.as_deref(), Some(token));
        let second = db
            .list_views(
                &namespace,
                crate::listing::ListingOptions::default()
                    .page_limit(2)
                    .page_token(first.page_token.unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(second.items, ["z"]);
        assert!(second.page_token.is_none());
        assert_eq!(
            *statements.lock().unwrap(),
            ["SHOW VIEWS IN \"team$reports\" LIMIT 2", next_sql]
        );
        for header in headers.lock().unwrap().iter() {
            assert_eq!(header.database, "analytics");
            assert_eq!(header.database_prefix, "tenant/production");
            assert_eq!(header.namespace_path, "team$reports");
        }
        shutdown.send(()).unwrap();
    }
}

#[test]
fn sql_status_keeps_input_category_and_request_ids() {
    for (code, category) in [
        (tonic::Code::InvalidArgument, "column_not_found"),
        (tonic::Code::InvalidArgument, "schema_mismatch"),
        (tonic::Code::NotFound, "table_not_found"),
        (tonic::Code::NotFound, "reference_not_found"),
        (tonic::Code::Internal, "column_not_found"),
    ] {
        let mut status = tonic::Status::new(code, "original diagnostic");
        status
            .metadata_mut()
            .insert("x-lancedb-error-code", category.parse().unwrap());
        status
            .metadata_mut()
            .insert("x-job-request-id", "worker-request".parse().unwrap());
        for job_id in [None, Some("sql-job-1")] {
            if let Some(job_id) = job_id {
                status
                    .metadata_mut()
                    .insert("x-lancedb-sql-job-id", job_id.parse().unwrap());
            }
            let error = super::sql_status_error("current-request", &status);
            assert_eq!(
                error.remote_error_metadata().unwrap().job_id.as_deref(),
                job_id
            );
            let source = match &error {
                Error::JobFailed {
                    job_id: actual_job_id,
                    failure,
                } => {
                    assert_eq!(actual_job_id.as_deref(), job_id);
                    assert_eq!(failure.sql_job_id.as_deref(), job_id);
                    failure.source.as_deref().unwrap()
                }
                error => {
                    assert!(job_id.is_none());
                    error
                }
            };
            let Error::Sql {
                message,
                request_id,
                grpc_code,
                metadata,
            } = source
            else {
                panic!("SQL status must remain structured: {source}");
            };
            assert_eq!(message, "original diagnostic");
            assert_eq!(request_id, "current-request");
            assert_eq!(*grpc_code, code as i32);
            assert_eq!(metadata.error_code.as_deref(), Some(category));
            assert_eq!(metadata.job_id.as_deref(), job_id);
            assert_eq!(metadata.job_request_id.as_deref(), Some("worker-request"));
        }
    }
    let error = super::sql_status_error("request", &tonic::Status::not_found("column_not_found"));
    assert!(error.remote_error_metadata().unwrap().error_code.is_none());
}

#[test]
fn sql_status_reports_structured_resource_identity() {
    let mut status = tonic::Status::not_found("Function 'missing' was not found");
    status
        .metadata_mut()
        .insert("x-lancedb-error-code", "not_found".parse().unwrap());
    status.metadata_mut().insert(
        "x-lancedb-resource",
        "eyJraW5kIjoiZnVuY3Rpb24iLCJuYW1lIjoibWlzc2luZyIsIm5hbWVzcGFjZV9wYXRoIjpbIm5zIl19"
            .parse()
            .unwrap(),
    );
    status
        .metadata_mut()
        .insert("x-lancedb-sql-job-id", "sql-job-1".parse().unwrap());

    let error = super::sql_status_error("current-request", &status);
    let resource = error
        .remote_error_metadata()
        .unwrap()
        .resource
        .as_ref()
        .unwrap();
    assert_eq!(resource.kind, "function");
    assert_eq!(resource.name, "missing");
    assert_eq!(resource.namespace_path, ["ns"]);
}

#[tokio::test]
async fn remote_optimize_forwards_per_job_options_on_main_and_branch() {
    for branch in [None, Some("o'brien")] {
        let service = TestSqlService {
            adaptive: true,
            ..Default::default()
        };
        let statements = service.statements.clone();
        let headers = service.headers.clone();
        let (sql_client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let client = client_with_handler(|request| -> http::Response<String> {
            panic!("unexpected REST request: {}", request.url().path());
        });
        let table = RemoteTable::new_with_sql_client(
            client,
            "analytics".into(),
            "events".into(),
            vec!["team".into()],
            "team$events".into(),
            Default::default(),
            Some(sql_client),
        )
        .with_branch(branch.map(str::to_string));
        table
            .optimize(crate::table::optimize::OptimizeAction::Compact {
                options: crate::table::optimize::CompactionOptions {
                    target_rows_per_fragment: 100,
                    max_source_rows: Some(500),
                    io_buffer_size: Some(0),
                    batch_size: Some(16),
                    ..Default::default()
                },
                remap_options: None,
            })
            .await
            .unwrap();
        let prefix = if branch.is_some() {
            "branch = 'o''brien', "
        } else {
            ""
        };
        assert_eq!(
            *statements.lock().unwrap(),
            [format!(
                "OPTIMIZE \"events\" REWRITE DATA WITH ({prefix}target_rows_per_fragment = 100, max_source_rows = 500, io_buffer_size = 0, batch_size = 16)"
            )]
        );
        assert!(
            headers
                .lock()
                .unwrap()
                .iter()
                .all(|h| h.namespace_path == "team")
        );
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn remote_compact_preserves_omissions_and_explicit_default_values() {
    use crate::table::{CompactionOptions, CompactionOverrides};
    let target = CompactionOptions::default().target_rows_per_fragment;
    for (options, suffix) in [
        (CompactionOverrides::default(), String::new()),
        (
            CompactionOverrides {
                target_rows_per_fragment: Some(target),
                ..Default::default()
            },
            format!(" WITH (target_rows_per_fragment = {target})"),
        ),
        (
            CompactionOverrides {
                io_buffer_size: Some(0),
                ..Default::default()
            },
            " WITH (io_buffer_size = 0)".into(),
        ),
    ] {
        let service = TestSqlService {
            adaptive: true,
            ..Default::default()
        };
        let statements = service.statements.clone();
        let (sql_client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let client = client_with_handler(|request| -> http::Response<String> {
            panic!("unexpected REST request: {}", request.url().path());
        });
        let table = RemoteTable::new_with_sql_client(
            client,
            "analytics".into(),
            "events".into(),
            vec![],
            "events".into(),
            Default::default(),
            Some(sql_client),
        );
        table.compact(options).await.unwrap();
        assert_eq!(
            *statements.lock().unwrap(),
            [format!("OPTIMIZE \"events\" REWRITE DATA{suffix}")]
        );
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn remote_optimize_parser_errors_are_clear_without_resubmission() {
    use crate::table::optimize::CompactionOverrides;
    for (code, message, unsupported) in [
        (
            tonic::Code::InvalidArgument,
            "ParserError(\"Expected: end of statement, found: REWRITE at Line: 1\")",
            true,
        ),
        (
            tonic::Code::InvalidArgument,
            "ParserError: Expected: TABLE, found: events",
            true,
        ),
        (
            tonic::Code::InvalidArgument,
            "Unknown OPTIMIZE option 'io_buffer_size'",
            true,
        ),
        (
            tonic::Code::InvalidArgument,
            "ParserError: future grammar diagnostic",
            true,
        ),
        (
            tonic::Code::InvalidArgument,
            "Unknown OPTIMIZE option 'future_option'",
            true,
        ),
        (
            tonic::Code::PermissionDenied,
            "ParserError: future grammar diagnostic",
            false,
        ),
        (tonic::Code::PermissionDenied, "permission denied", false),
        (tonic::Code::NotFound, "table not found", false),
        (tonic::Code::Unavailable, "transport error", false),
        (
            tonic::Code::InvalidArgument,
            "invalid table configuration",
            false,
        ),
    ] {
        let status = Status::new(code, message);
        let service = TestSqlService {
            adaptive: true,
            optimize_error: Some(status),
            ..Default::default()
        };
        let statements = service.statements.clone();
        let (sql_client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let client =
            client_with_handler(|_| -> http::Response<String> { panic!("no REST fallback") });
        let table = RemoteTable::new_with_sql_client(
            client,
            "analytics".into(),
            "events".into(),
            vec![],
            "events".into(),
            Default::default(),
            Some(sql_client),
        );
        let error = table
            .compact(CompactionOverrides::default())
            .await
            .unwrap_err();
        assert_eq!(
            matches!(error, Error::NotSupported { .. }),
            unsupported,
            "{error:?}"
        );
        assert!(error.to_string().contains(message));
        assert!(error.to_string().contains("request_id"));
        assert_eq!(statements.lock().unwrap().len(), 1);
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn remote_optimize_branch_options_are_not_dropped() {
    let service = TestSqlService {
        adaptive: true,
        optimize_error: Some(Status::invalid_argument(
            "ParserError: Expected: ), found: , at Line: 1",
        )),
        ..Default::default()
    };
    let statements = service.statements.clone();
    let (sql_client, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let client = client_with_handler(|_| -> http::Response<String> { panic!("no REST fallback") });
    let table = RemoteTable::new_with_sql_client(
        client,
        "analytics".into(),
        "events".into(),
        vec![],
        "events".into(),
        Default::default(),
        Some(sql_client),
    )
    .with_branch(Some("exp".into()));
    let error = table
        .compact(crate::table::optimize::CompactionOverrides {
            io_buffer_size: Some(0),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(error, Error::NotSupported { .. }));
    assert!(error.to_string().contains("compatible release"));
    assert_eq!(
        *statements.lock().unwrap(),
        ["OPTIMIZE \"events\" REWRITE DATA WITH (branch = 'exp', io_buffer_size = 0)"]
    );
    shutdown.send(()).unwrap();
}

fn prewarm_result() -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "job_id",
            Arc::new(StringArray::from(vec!["warming-job"])) as ArrayRef,
        ),
        (
            "table_version",
            Arc::new(arrow_array::UInt64Array::from(vec![42])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn receipt_test_table(
    sql: SqlClient,
) -> RemoteTable<crate::remote::client::test_utils::MockSender> {
    RemoteTable::new_with_sql_client(
        client_with_handler(|r| -> http::Response<String> {
            panic!(
                "unexpected HTTP request while reading a job receipt: {}",
                r.url()
            )
        }),
        "analytics".into(),
        "events".into(),
        vec!["team".into()],
        "team$events".into(),
        Default::default(),
        Some(sql),
    )
}

#[tokio::test]
async fn remote_maintenance_job_receipts_require_one_row_and_a_complete_stream() {
    let receipt = prewarm_result();
    let empty = receipt.slice(0, 0);
    let with_ids = |ids: ArrayRef| {
        let versions = arrow_array::UInt64Array::from(vec![42; ids.len()]);
        RecordBatch::try_from_iter([
            ("job_id", ids),
            ("table_version", Arc::new(versions) as ArrayRef),
        ])
        .unwrap()
    };
    let ids = |values: Vec<Option<&str>>| with_ids(Arc::new(StringArray::from(values)));
    let invalid = "inspect jobs before retrying";
    // All three commands receive the same wire response.
    for (first, tail, expected) in [
        (
            empty.clone(),
            vec![Ok(receipt.clone()), Ok(empty.clone())],
            Ok("warming-job"),
        ),
        (ids(vec![Some(" id ")]), vec![], Ok(" id ")),
        (empty, vec![], Err(invalid)),
        (ids(vec![None]), vec![], Err(invalid)),
        (ids(vec![Some("")]), vec![], Err(invalid)),
        (ids(vec![Some(" \t ")]), vec![], Err(invalid)),
        (
            with_ids(Arc::new(Int64Array::from(vec![7]))),
            vec![],
            Err(invalid),
        ),
        (receipt.project(&[1]).unwrap(), vec![], Err(invalid)),
        (ids(vec![Some("one"), Some("two")]), vec![], Err(invalid)),
        (receipt.clone(), vec![Ok(receipt.clone())], Err(invalid)),
        (
            receipt,
            vec![Err(Status::data_loss("receipt stream interrupted"))],
            Err("receipt stream interrupted"),
        ),
    ] {
        let service = TestSqlService {
            adaptive: true,
            result: first,
            result_tail: tail,
            ..Default::default()
        };
        let statements = service.statements.clone();
        let (sql, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let table = receipt_test_table(sql);
        let compact = table.compact_async(Default::default()).await;
        let prewarm = table.prewarm_data_async(Default::default()).await;
        let cleanup = table.cleanup_async(Default::default()).await;
        for (result, command) in [
            (compact.as_ref().map(|j| j.id()), "OPTIMIZE"),
            (prewarm.as_ref().map(|j| j.id()), "PREWARM"),
            (cleanup.as_ref().map(|j| j.id()), "VACUUM"),
        ] {
            match expected {
                Ok(id) => assert_eq!(result.unwrap(), Some(id), "{command}"),
                Err(message) => {
                    let error = result.unwrap_err().to_string();
                    assert!(error.contains(message), "{command}: {error}");
                    if message == invalid {
                        assert!(error.contains(command), "{error}");
                    }
                }
            }
        }
        let commands: Vec<_> = statements
            .lock()
            .unwrap()
            .iter()
            .map(|sql| sql.split_whitespace().next().unwrap().to_owned())
            .collect();
        assert_eq!(commands, ["OPTIMIZE", "PREWARM", "VACUUM"]);
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn remote_prewarm_receipt_requires_a_non_null_uint64_version() {
    let id = prewarm_result().project(&[0]).unwrap();
    for version in [
        None,
        Some(Arc::new(Int64Array::from(vec![42])) as ArrayRef),
        Some(Arc::new(arrow_array::UInt64Array::from(vec![None])) as ArrayRef),
    ] {
        let result = match version {
            None => id.clone(),
            Some(version) => RecordBatch::try_from_iter([
                ("job_id", id.column(0).clone()),
                ("table_version", version),
            ])
            .unwrap(),
        };
        let service = TestSqlService {
            adaptive: true,
            result,
            ..Default::default()
        };
        let statements = service.statements.clone();
        let (sql, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let table = receipt_test_table(sql);
        let error = table
            .prewarm_data_async(Default::default())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("table_version") && error.contains("inspect jobs before retrying"),
            "{error}"
        );
        assert_eq!(statements.lock().unwrap().len(), 1);
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn remote_prewarm_accepts_a_receipt_for_a_bound_filter() {
    let statement = r#"PREWARM WITH (branch = 'exp') AS SELECT * FROM "events" WHERE tag = $tag"#;
    let parameters = RecordBatch::try_from_iter([(
        "tag",
        Arc::new(StringArray::from(vec!["train's data"])) as ArrayRef,
    )])
    .unwrap();
    let service = TestSqlService {
        adaptive: true,
        ..Default::default()
    };
    service
        .result_overrides
        .lock()
        .unwrap()
        .insert(statement.to_owned(), prewarm_result());
    let exchanges = service.exchanges.clone();
    let statements = service.statements.clone();
    let headers = service.headers.clone();
    let (sql, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let table = receipt_test_table(sql).with_branch(Some("exp".into()));
    let job = table
        .prewarm_data_async(
            crate::table::PrewarmDataOptions::new()
                .filter("tag = $tag")
                .parameters(parameters.clone()),
        )
        .await
        .unwrap();
    assert_eq!(job.id(), Some("warming-job"));
    assert_eq!(
        *exchanges.lock().unwrap(),
        [(statement.to_owned(), Some(parameters))]
    );
    assert!(statements.lock().unwrap().is_empty());
    assert!(
        headers
            .lock()
            .unwrap()
            .iter()
            .all(|h| h.database == "analytics" && h.namespace_path == "team")
    );
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn remote_prewarm_data_returns_the_durable_job_without_polling() {
    use crate::table::PrewarmDataOptions;
    for branch in [None, Some("experiment"), Some("o'brien")] {
        let service = TestSqlService {
            adaptive: true,
            result: prewarm_result(),
            ..Default::default()
        };
        let statements = service.statements.clone();
        let headers = service.headers.clone();
        let (sql, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = polls.clone();
        let client = client_with_handler(move |request| -> http::Response<String> {
            assert_eq!(request.url().path(), "/v1/jobs/describe");
            let value: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            assert_eq!(value["job_id"], "warming-job");
            observed.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(200)
                .body(
                    serde_json::json!({
                "job_id":"warming-job", "job_type":"prewarm_page_cache", "job_state":"DONE",
                "creation_ms":1,"spec":{"table_version":42},
                "result":{"table_version":42,"executors_targeted":2,"executors_completed":2,"fragments_selected":4},
                "operation_progress":{"phase":"reading","executors_completed":2}})
                    .to_string(),
                )
                .unwrap()
        });
        let table = RemoteTable::new_with_sql_client(
            client,
            "analytics".into(),
            "clip\"name".into(),
            vec!["team".into(), "inner".into()],
            "team$inner$clip\"name".into(),
            Default::default(),
            Some(sql),
        )
        .with_branch(branch.map(str::to_string));
        let job = table
            .prewarm_data_async(
                PrewarmDataOptions::new()
                    .columns(["attrs.score", "`caption.text`", "attrs.score"])
                    .filter("quality > 0.7")
                    .concurrency(4),
            )
            .await
            .unwrap();
        assert_eq!(job.id(), Some("warming-job"));
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "acceptance must not eagerly poll HTTP"
        );
        let branch_sql = branch
            .map(|b| format!("branch = '{}', ", b.replace('\'', "''")))
            .unwrap_or_default();
        assert_eq!(
            *statements.lock().unwrap(),
            [format!(
                "PREWARM WITH ({branch_sql}concurrency = 4) AS SELECT \"attrs\".\"score\", \"caption.text\" FROM \"clip\"\"name\" WHERE quality > 0.7"
            )]
        );
        assert!(
            headers
                .lock()
                .unwrap()
                .iter()
                .all(|h| h.namespace_path == "team$inner" && h.database == "analytics")
        );
        job.wait().await.unwrap();
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        job.refresh().await.unwrap();
        assert_eq!(job.spec().unwrap()["table_version"], 42);
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn remote_legacy_prewarm_data_submits_sql_and_returns_on_acceptance() {
    for columns in [None, Some(vec![]), Some(vec!["payload".into()])] {
        let service = TestSqlService {
            adaptive: true,
            result: prewarm_result(),
            ..Default::default()
        };
        let statements = service.statements.clone();
        let (sql, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let table = RemoteTable::new_with_sql_client(
            client_with_handler(|r| -> http::Response<String> {
                panic!(
                    "legacy prewarm must neither submit nor poll REST: {}",
                    r.url()
                );
            }),
            "analytics".into(),
            "clips".into(),
            vec![],
            "clips".into(),
            Default::default(),
            Some(sql),
        );
        table.prewarm_data(columns.clone()).await.unwrap();
        let projection = if columns.as_ref().is_some_and(|c| !c.is_empty()) {
            "\"payload\""
        } else {
            "*"
        };
        assert_eq!(
            *statements.lock().unwrap(),
            [format!("PREWARM AS SELECT {projection} FROM \"clips\"")]
        );
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn remote_prewarm_data_rejects_bad_receipts_without_resubmission() {
    let service = TestSqlService {
        adaptive: true,
        ..Default::default()
    };
    let statements = service.statements.clone();
    let (sql, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let table = RemoteTable::new_with_sql_client(
        client_with_handler(|r| -> http::Response<String> {
            panic!("must not retry malformed receipt through REST: {}", r.url());
        }),
        "analytics".into(),
        "clips".into(),
        vec![],
        "clips".into(),
        Default::default(),
        Some(sql),
    );
    let error = table.prewarm_data(None).await.unwrap_err();
    assert!(error.to_string().contains("inspect jobs before retrying"));
    assert_eq!(statements.lock().unwrap().len(), 1);
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn remote_prewarm_errors_preserve_diagnostics_and_never_fall_back() {
    for (status, unsupported) in [
        (
            Status::invalid_argument("Unknown PREWARM option 'branch'"),
            true,
        ),
        (Status::permission_denied("source Select denied"), false),
        (
            Status::invalid_argument("ParserError: expected expression, found invalid_filter"),
            false,
        ),
    ] {
        let service = TestSqlService {
            adaptive: true,
            prewarm_error: Some(status),
            ..Default::default()
        };
        let statements = service.statements.clone();
        let (sql, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let table = RemoteTable::new_with_sql_client(
            client_with_handler(|r| -> http::Response<String> {
                panic!("must not retry through REST: {}", r.url());
            }),
            "analytics".into(),
            "clips".into(),
            vec![],
            "clips".into(),
            Default::default(),
            Some(sql),
        );
        let error = table.prewarm_data(None).await.unwrap_err();
        assert_eq!(
            matches!(error, Error::NotSupported { .. }),
            unsupported,
            "{error}"
        );
        let preview_error = table
            .explain_prewarm_data(Default::default())
            .await
            .unwrap_err();
        assert_eq!(
            matches!(preview_error, Error::NotSupported { .. }),
            unsupported,
            "{preview_error}"
        );
        assert_eq!(statements.lock().unwrap().len(), 2);
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn remote_prewarm_requires_azure_ack_before_execution() {
    for supported in [false, true] {
        let service = TestSqlService {
            adaptive: true,
            result: prewarm_result(),
            prewarm_azure_supported: supported,
            ..Default::default()
        };
        let statements = service.statements.clone();
        let mut config = ClientConfig::default();
        config.extra_headers.insert(
            "x-azure-storage-account-name".into(),
            "sourceaccount".into(),
        );
        let (sql, shutdown) = start_sql_service(service, config).await;
        let table = RemoteTable::new_with_sql_client(
            client_with_handler(|r| -> http::Response<String> {
                panic!("must not use REST: {}", r.url());
            }),
            "analytics".into(),
            "clips".into(),
            vec![],
            "clips".into(),
            Default::default(),
            Some(sql),
        );
        let result = table.prewarm_data(None).await;
        assert_eq!(result.is_ok(), supported, "{result:?}");
        assert_eq!(statements.lock().unwrap().len(), usize::from(supported));
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn remote_prewarm_wait_surfaces_warming_job_failure_or_cancellation() {
    for state in ["FAILED", "CANCELLED"] {
        let service = TestSqlService {
            adaptive: true,
            result: prewarm_result(),
            ..Default::default()
        };
        let (sql, shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let client = client_with_handler(move |r| -> http::Response<String> {
            assert_eq!(r.url().path(), "/v1/jobs/describe");
            http::Response::builder()
                .status(200)
                .body(
                    serde_json::json!({
                "job_id":"warming-job","job_type":"prewarm_page_cache","job_state":state,
                "creation_ms":1,"spec":{"table_version":42},
                "result":{"table_version":42,"executors_targeted":2,"executors_completed":2,"fragments_selected":4},
                "operation_progress":{"phase":"reading","executors_completed":2}})
                    .to_string(),
                )
                .unwrap()
        });
        let table = RemoteTable::new_with_sql_client(
            client,
            "analytics".into(),
            "clips".into(),
            vec![],
            "clips".into(),
            Default::default(),
            Some(sql),
        );
        let job = table.prewarm_data_async(Default::default()).await.unwrap();
        assert_eq!(job.id(), Some("warming-job"));
        let error = job.wait().await.unwrap_err();
        if state == "FAILED" {
            assert!(matches!(error, Error::JobFailed { .. }), "{error}");
        } else {
            assert!(matches!(error, Error::JobCancelled { .. }), "{error}");
        }
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn remote_prewarm_rejects_historical_checkout_without_submitting() {
    let service = TestSqlService {
        adaptive: true,
        result: prewarm_result(),
        ..Default::default()
    };
    let statements = service.statements.clone();
    let (sql, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let table = RemoteTable::new_with_sql_client(
        client_with_handler(|r| -> http::Response<String> {
            assert_eq!(r.url().path(), "/v1/table/clips/describe/");
            http::Response::builder()
                .status(200)
                .body(r#"{"version":7,"schema":{"fields":[]}}"#.to_string())
                .unwrap()
        }),
        "analytics".into(),
        "clips".into(),
        vec![],
        "clips".into(),
        Default::default(),
        Some(sql),
    );
    table.checkout(7).await.unwrap();
    let error = table.prewarm_data(None).await.unwrap_err();
    assert!(matches!(error, Error::NotSupported { .. }));
    assert!(error.to_string().contains("historical version 7"));
    let error = table
        .explain_prewarm_data(Default::default())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::NotSupported { .. }));
    assert!(error.to_string().contains("historical version 7"));
    assert!(statements.lock().unwrap().is_empty());
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn remote_cleanup_tracks_registry_job_and_real_result() {
    use crate::table::CleanupOptions;
    let service = TestSqlService {
        adaptive: true,
        result: RecordBatch::try_from_iter([(
            "job_id",
            Arc::new(StringArray::from(vec!["cleanup-job"])) as ArrayRef,
        )])
        .unwrap(),
        ..Default::default()
    };
    let statements = service.statements.clone();
    let (sql, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = polls.clone();
    let client = client_with_handler(move |request| {
        assert_eq!(request.url().path(), "/v1/jobs/describe");
        observed.fetch_add(1, Ordering::SeqCst);
        http::Response::builder()
            .status(200)
            .body(
                serde_json::json!({
                    "job_id":"cleanup-job","job_type":"cleanup","job_state":"DONE",
                    "result":{"format_version":1,"min_age_seconds":0,"statistics_complete":true,
                        "old_versions":2,"bytes_removed":123}
                })
                .to_string(),
            )
            .unwrap()
    });
    let table = RemoteTable::new_with_sql_client(
        client,
        "analytics".into(),
        "events".into(),
        vec!["team".into(), "inner".into()],
        "team$inner$events".into(),
        Default::default(),
        Some(sql),
    )
    .with_branch(Some("o'brien".into()));
    let job = table
        .cleanup_async(CleanupOptions::new().min_age_seconds(0))
        .await
        .unwrap();
    assert_eq!(job.id(), Some("cleanup-job"));
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let result = job.wait().await.unwrap();
    assert_eq!(result.old_versions, Some(2));
    assert_eq!(result.bytes_removed, Some(123));
    assert_eq!(
        *statements.lock().unwrap(),
        ["VACUUM \"events\" WITH (branch = 'o''brien', min_age_seconds = 0)"]
    );
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn remote_cleanup_rejects_historical_checkout_without_submitting() {
    let table = RemoteTable::new_with_sql_client(
        client_with_handler(|request| {
            assert_eq!(request.url().path(), "/v1/table/events/describe/");
            http::Response::builder()
                .status(200)
                .body(r#"{"version":7,"schema":{"fields":[]}}"#.to_string())
                .unwrap()
        }),
        "analytics".into(),
        "events".into(),
        vec![],
        "events".into(),
        Default::default(),
        None,
    );
    table.checkout(7).await.unwrap();
    assert!(table.cleanup_async(Default::default()).await.is_err());
}

#[tokio::test]
async fn remote_compaction_tracks_registry_job_and_real_result() {
    use crate::table::optimize::CompactionOverrides;
    let service = TestSqlService {
        adaptive: true,
        result: RecordBatch::try_from_iter([(
            "job_id",
            Arc::new(StringArray::from(vec!["compacting-job"])) as ArrayRef,
        )])
        .unwrap(),
        ..Default::default()
    };
    let statements = service.statements.clone();
    let (sql, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = polls.clone();
    let client = client_with_handler(move |request| {
        assert_eq!(request.url().path(), "/v1/jobs/describe");
        let body: serde_json::Value =
            serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        assert_eq!(body["job_id"], "compacting-job");
        observed.fetch_add(1, Ordering::SeqCst);
        http::Response::builder()
            .status(200)
            .body(
                serde_json::json!({
            "job_id":"compacting-job","job_type":"compact","job_state":"DONE",
            "result":{"partial_commit":false,"rows_succeeded":12,"rows_failed":0,
                "completion_percentage":100.0,"files_added":1,"files_removed":3,
                "fragments_added":1,"fragments_removed":3}})
                .to_string(),
            )
            .unwrap()
    });
    let table = RemoteTable::new_with_sql_client(
        client,
        "analytics".into(),
        "events".into(),
        vec!["team".into(), "inner".into()],
        "team$inner$events".into(),
        Default::default(),
        Some(sql),
    )
    .with_branch(Some("experiment".into()));
    let job = table
        .compact_async(CompactionOverrides {
            target_rows_per_fragment: Some(100),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(job.id(), Some("compacting-job"));
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let result = job.wait().await.unwrap();
    assert_eq!(result.rows_succeeded, 12);
    assert_eq!(result.fragments_removed, Some(3));
    assert!(!result.more_work);
    assert_eq!(
        *statements.lock().unwrap(),
        [
            "OPTIMIZE \"events\" REWRITE DATA WITH (branch = 'experiment', target_rows_per_fragment = 100)"
        ]
    );
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn remote_prewarm_preview_uses_sql_without_any_http_jobs() {
    use crate::table::PrewarmDataOptions;
    for azure in [false, true] {
        let preview = serde_json::json!({
            "format_version":1,"table_version":42,"plan":"metadata preview",
            "pruning":{"mode":"zonemap_ranges","input_fragments":4,
                "selected_fragments":1,"pruned_fragments":3,"selected_row_ranges":2}
        });
        let service = TestSqlService {
            adaptive: true,
            result: RecordBatch::try_from_iter([
                (
                    "plan_type",
                    Arc::new(StringArray::from(vec!["logical_plan"])) as ArrayRef,
                ),
                (
                    "plan",
                    Arc::new(StringArray::from(vec![preview.to_string()])) as ArrayRef,
                ),
            ])
            .unwrap(),
            prewarm_azure_supported: true,
            ..Default::default()
        };
        let statements = service.statements.clone();
        let headers = service.headers.clone();
        let mut config = ClientConfig::default();
        if azure {
            config.extra_headers.insert(
                "x-azure-storage-account-name".into(),
                "sourceaccount".into(),
            );
        }
        let (sql, shutdown) = start_sql_service(service, config).await;
        let client = client_with_handler(|_| -> http::Response<String> {
            panic!("preview must not use HTTP job APIs or REST fallback")
        });
        let table = RemoteTable::new_with_sql_client(
            client,
            "analytics".into(),
            "clips".into(),
            vec!["team".into()],
            "team$clips".into(),
            Default::default(),
            Some(sql),
        )
        .with_branch(Some("experiment".into()));
        let plan = table
            .explain_prewarm_data(
                PrewarmDataOptions::new()
                    .columns(["attrs.score"])
                    .filter("id > 0")
                    .concurrency(4),
            )
            .await
            .unwrap();
        assert_eq!(plan.table_version, 42);
        assert_eq!(plan.pruning.selected_row_ranges, Some(2));
        assert_eq!(
            *statements.lock().unwrap(),
            [
                "EXPLAIN (FORMAT JSON) PREWARM WITH (branch = 'experiment', concurrency = 4) AS SELECT \"attrs\".\"score\" FROM \"clips\" WHERE id > 0"
            ]
        );
        assert!(
            headers
                .lock()
                .unwrap()
                .iter()
                .all(|h| h.namespace_path == "team" && h.database == "analytics")
        );
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn remote_cleanup_preview_uses_sql_without_job_calls() {
    let payload = serde_json::json!({"format_version":1,"source_version":42,"min_age_seconds":0,
        "cutoff_millis":123,"versions_to_remove":2,"bytes_to_remove":100});
    let service = TestSqlService {
        adaptive: true,
        result: RecordBatch::try_from_iter([
            (
                "plan_type",
                Arc::new(StringArray::from(vec!["logical_plan"])) as ArrayRef,
            ),
            (
                "plan",
                Arc::new(StringArray::from(vec![payload.to_string()])) as ArrayRef,
            ),
        ])
        .unwrap(),
        ..Default::default()
    };
    let statements = service.statements.clone();
    let (sql, shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let table = RemoteTable::new_with_sql_client(
        client_with_handler(|_| -> http::Response<String> {
            panic!("preview must not use HTTP jobs or execution fallback")
        }),
        "analytics".into(),
        "events".into(),
        vec!["team".into()],
        "team$events".into(),
        Default::default(),
        Some(sql),
    );
    let plan = table
        .explain_cleanup(crate::table::CleanupOptions::new().min_age_seconds(0))
        .await
        .unwrap();
    assert_eq!(plan.source_version, 42);
    assert_eq!(plan.versions_to_remove, 2);
    assert_eq!(
        *statements.lock().unwrap(),
        ["EXPLAIN (FORMAT JSON) VACUUM \"events\" WITH (min_age_seconds = 0)"]
    );
    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn sql_deadline_expires_unread_streamed_and_deferred_queries() {
    for deferred in [false, true] {
        for parameters in [None, Some(parameter_row())] {
            let service = TestSqlService {
                adaptive: true,
                defer: deferred,
                ..TestSqlService::default()
            };
            let cancellations = service.cancel_count.clone();
            let headers = service.headers.clone();
            let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;
            let mut request = ExecuteQueryRequest::new("SELECT slow get");
            request.parameters = parameters;
            request.options.timeout = Some(Duration::from_secs(3));
            let query = client.execute(request).await.unwrap();
            assert!(headers.lock().unwrap()[0].grpc_timeout.is_some());
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(4)).await;
            tokio::task::yield_now().await;
            tokio::time::resume();
            assert_overall_timeout(query.describe().await, "result");
            assert_overall_timeout(client.describe(query.id()).await, "result");
            assert_overall_timeout(query.reader().await, "result");
            if deferred {
                wait_for(
                    &cancellations,
                    1,
                    "expiry must cancel deferred execution without a reader",
                )
                .await;
            }
            let ordinary = client
                .execute(ExecuteQueryRequest::new("SELECT 1"))
                .await
                .unwrap();
            assert!(
                !collect_result(&ordinary).await.unwrap().is_empty(),
                "query options must not change the connection defaults"
            );
        }
    }
}

#[tokio::test]
async fn a_deferred_result_timeout_sends_cancellation() {
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        ..TestSqlService::default()
    };
    let cancellations = service.cancel_count.clone();
    let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let mut request = ExecuteQueryRequest::new("SELECT slow get");
    request.options.timeout = Some(Duration::from_secs(3));
    let query = client.execute(request).await.unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(2_900)).await;
    let result = query.reader().await;
    tokio::time::resume();
    assert_overall_timeout(result, "result");
    wait_for(
        &cancellations,
        1,
        "a deferred reader timeout must cancel execution",
    )
    .await;
}

#[tokio::test]
async fn sql_read_timeout_is_per_query_and_cancels_deferred_execution() {
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        ..TestSqlService::default()
    };
    let cancellations = service.cancel_count.clone();
    let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let mut request = ExecuteQueryRequest::new("SELECT slow get");
    request.options.read_timeout = Some(Duration::from_millis(100));
    let query = client.execute(request).await.unwrap();
    let error = collect_result(&query).await.unwrap_err();
    assert!(
        error.to_string().contains("SQL result read timed out"),
        "{error}"
    );
    wait_for(
        &cancellations,
        1,
        "a read timeout must cancel deferred execution",
    )
    .await;
    let next = client
        .execute(ExecuteQueryRequest::new("SELECT slow get"))
        .await
        .unwrap();
    assert!(!collect_result(&next).await.unwrap().is_empty());
}

#[tokio::test]
async fn deferred_cancellation_on_drop_is_opt_in() {
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        ..TestSqlService::default()
    };
    let cancellations = service.cancel_count.clone();
    let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let detached = client
        .execute(ExecuteQueryRequest::new("SELECT 1"))
        .await
        .unwrap();
    let id = detached.id();
    drop(detached);
    assert_eq!(
        client.describe(id).await.unwrap().status,
        QueryStatus::Finished
    );
    assert_eq!(cancellations.load(Ordering::SeqCst), 0);
    let mut request = ExecuteQueryRequest::new("SELECT 1");
    request.options.cancel_on_drop = true;
    let query = client.execute(request).await.unwrap();
    let id = query.id();
    // Drop outside the async runtime, as Python garbage collection may do.
    std::thread::spawn(move || drop(query)).join().unwrap();
    wait_for(&cancellations, 1, "dropping an opted-in handle must cancel").await;
    assert_eq!(
        client.describe(id).await.unwrap().status,
        QueryStatus::Cancelled
    );
}

#[tokio::test]
async fn deferred_reader_owns_cancellation_after_the_handle_is_dropped() {
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        ..TestSqlService::default()
    };
    let cancellations = service.cancel_count.clone();
    let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let mut request = ExecuteQueryRequest::new("SELECT slow");
    request.options.cancel_on_drop = true;
    let query = client.execute(request).await.unwrap();
    let rows = query.reader().await.unwrap();
    drop(query);
    assert_eq!(
        cancellations.load(Ordering::SeqCst),
        0,
        "an active reader must survive dropping its query handle"
    );
    drop(rows);
    wait_for(
        &cancellations,
        1,
        "dropping the reader must cancel deferred execution",
    )
    .await;
}

#[tokio::test]
async fn a_completed_query_is_not_cancelled_on_drop_or_deadline() {
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        ..TestSqlService::default()
    };
    let cancellations = service.cancel_count.clone();
    let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let mut request = ExecuteQueryRequest::new("SELECT 1");
    request.options.timeout = Some(Duration::from_secs(3));
    request.options.cancel_on_drop = true;
    let query = client.execute(request).await.unwrap();
    assert!(!collect_result(&query).await.unwrap().is_empty());
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::task::yield_now().await;
    tokio::time::resume();
    assert_eq!(
        query.describe().await.unwrap().status,
        QueryStatus::Finished
    );
    drop(query);
    assert_eq!(cancellations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn sql_deadline_applies_to_buffered_results_and_stops_its_timer_on_completion() {
    for deferred in [false, true] {
        let service = TestSqlService {
            adaptive: true,
            defer: deferred,
            ..TestSqlService::default()
        };
        let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;
        let mut request = ExecuteQueryRequest::new("SELECT 1");
        request.options.timeout = Some(Duration::from_secs(3));
        let query = client.execute(request).await.unwrap();
        let mut rows = query.reader().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            query.describe().await.unwrap().status,
            QueryStatus::Finished
        );
        match client.queries.get(query.id()).unwrap() {
            RegisteredQuery::Polled(query) => {
                assert!(query.deadline_task.lock().unwrap().is_none())
            }
            RegisteredQuery::Streamed(query) => assert!(query.deadline_stopped()),
        }
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(4)).await;
        tokio::time::resume();
        assert_overall_timeout(rows.try_next().await, "result");
        assert!(rows.try_next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn sql_deadline_is_forwarded_to_deferred_result_requests() {
    let service = TestSqlService {
        adaptive: true,
        defer: true,
        ..TestSqlService::default()
    };
    let deadlines = service.result_deadlines.clone();
    let headers = service.headers.clone();
    let (client, _shutdown) = start_sql_service(service, ClientConfig::default()).await;
    let mut request = ExecuteQueryRequest::new("SELECT 1");
    request.options.timeout = Some(Duration::from_secs(3));
    let query = client.execute(request).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!collect_result(&query).await.unwrap().is_empty());
    let deadlines = deadlines.lock().unwrap();
    let result_deadline = deadlines[0]
        .as_deref()
        .expect("the result request must carry an execution deadline");
    assert_ne!(
        Some(result_deadline),
        headers.lock().unwrap()[0].grpc_timeout.as_deref(),
        "the timeout budget must not restart for the result request"
    );
}

#[tokio::test]
async fn sql_rejects_timeouts_that_cannot_be_encoded_by_grpc() {
    let (client, _shutdown) =
        start_sql_service(TestSqlService::default(), ClientConfig::default()).await;
    let mut request = ExecuteQueryRequest::new("SELECT 1");
    request.options.timeout = Some(Duration::from_secs(u64::MAX));
    assert!(matches!(
        client.execute(request).await,
        Err(Error::InvalidInput { .. })
    ));
}
