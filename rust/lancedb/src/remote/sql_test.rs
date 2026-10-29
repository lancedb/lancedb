// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::atomic::AtomicUsize;

use arrow_array::builder::StringDictionaryBuilder;
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
use crate::database::Database;
use crate::materialized_view::CreateMaterializedViewRequest;
use crate::remote::RemoteCatalogOptions;
use crate::remote::client::HeaderProvider;
use crate::remote::client::test_utils::client_with_handler;
use crate::remote::db::RemoteDatabase;
use crate::remote::table::RemoteTable;
use crate::table::BaseTable;

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
    gate_refresh_release: Arc<Notify>,
    exchanges: ReceivedExchanges,
    /// How many exchange responses have been dropped, finished or not.
    exchange_drops: Arc<AtomicUsize>,
    result: RecordBatch,
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
            database: header("database"),
            namespace_path: header("namespace-path"),
            request_id: header("x-request-id"),
            api_key: header("x-api-key"),
            database_prefix: metadata
                .get("x-lancedb-database-prefix")
                .map(|value| value.to_str().unwrap().to_string())
                .unwrap_or_default(),
        });
    }
    fn adaptive_response(
        &self,
        query: &str,
        rows: BoxStream<'static, std::result::Result<FlightData, Status>>,
    ) -> Response<BoxStream<'static, std::result::Result<FlightData, Status>>> {
        if !self.adaptive {
            return Response::new(rows);
        }
        let poll_info =
            self.defer.then(|| PollInfo {
                info: Some(FlightInfo::new().with_endpoint(
                    FlightEndpoint::new().with_ticket(Ticket::new(query.to_string())),
                )),
                flight_descriptor: Some(FlightDescriptor::new_cmd(format!("poll:1:{query}"))),
                progress: Some(0.25),
                expiration_time: None,
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
            gate_refresh_release: Arc::new(Notify::new()),
            exchanges: Arc::new(std::sync::Mutex::new(Vec::new())),
            exchange_drops: Arc::new(AtomicUsize::new(0)),
            result,
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
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
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
        let complete = if query == "SELECT no info" {
            stage >= 2
        } else {
            stage >= 1
        };

        let first_ticket = if query == "SELECT incremental" {
            format!("{query}:first")
        } else {
            query.clone()
        };
        let mut info = FlightInfo::new().with_endpoint(
            FlightEndpoint::new()
                .with_ticket(Ticket::new(first_ticket))
                .with_location("grpc://127.0.0.1:1"),
        );
        if query == "SELECT incremental" && stage > 0 {
            info = info.with_endpoint(
                FlightEndpoint::new()
                    .with_ticket(Ticket::new(format!("{query}:second")))
                    .with_location("grpc://127.0.0.1:1"),
            );
        }
        if query != "SELECT empty" {
            let schema = if query == "SELECT large message" {
                self.large_result.schema_ref()
            } else if query == "SELECT dictionary" {
                self.dictionary_result.schema_ref()
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
        let initial = Any::decode(request.get_ref().ticket.clone())
            .ok()
            .and_then(|envelope| Any::decode(envelope.value).ok())
            .and_then(|command| command.unpack::<CommandStatementQuery>().ok().flatten());
        if let Some(command) = &initial {
            self.statements.lock().unwrap().push(command.query.clone());
            if command.query == "SELECT rejected" {
                return Err(Status::invalid_argument("invalid statement"));
            }
            self.capture_headers(request.metadata());
            assert_eq!(
                request.metadata().get(PROTOCOL_HEADER).unwrap(),
                PROTOCOL_VERSION
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
        let result = if large {
            self.large_result.clone()
        } else if ticket == b"SELECT dictionary" {
            self.dictionary_result.clone()
        } else {
            self.result.clone()
        };
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
        .filter_map(futures::future::ready);
        let mut encoder = FlightDataEncoderBuilder::new().with_schema(schema);
        if large {
            encoder = encoder.with_max_flight_data_size(8 * 1024 * 1024);
        }
        let stream = encoder.build(input).map_err(Status::from);
        if let Some(initial) = initial {
            Ok(self.adaptive_response(&initial.query, stream.boxed()))
        } else {
            Ok(Response::new(stream.boxed()))
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
            return Ok(Response::new(futures::stream::pending().boxed()));
        }
        let rows =
            parameters.ok_or_else(|| Status::invalid_argument("exchange without parameters"))?;
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
        None,
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
    let (client, _shutdown) = start_sql_service(TestSqlService::default(), client_config).await;

    let stalled = client
        .execute(parameterized("SELECT exchange stall", parameter_row()))
        .await
        .unwrap();
    assert_overall_timeout(stalled.reader().await, "result");
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
        for (enabled, interval, expected) in [
            (None, None, ""),
            (Some(true), None, " WITH (auto_refresh_enabled = true)"),
            (
                Some(false),
                Some(0.5),
                " WITH (auto_refresh_enabled = false, auto_refresh_interval_seconds = 0.5)",
            ),
        ] {
            let job = db
                .create_materialized_view_async(CreateMaterializedViewRequest {
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
        assert_eq!(statements.lock().unwrap().len(), 3);
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
    let result = RecordBatch::try_from_iter(vec![
        (
            "name",
            Arc::new(StringArray::from(vec!["view"])) as ArrayRef,
        ),
        (
            "query",
            Arc::new(StringArray::from(vec!["SELECT * FROM source"])) as ArrayRef,
        ),
    ])
    .unwrap();
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

    for result in [first, second] {
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
            "DESCRIBE MATERIALIZED VIEW \"view\"",
            "REFRESH MATERIALIZED VIEW \"view\"",
            "REFRESH MATERIALIZED VIEW \"view\" WITH (full = true)",
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
    assert!(collect_result(&failed).await.is_err());

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
    assert_eq!(cancel_count.load(Ordering::SeqCst), 15);
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
async fn remote_mv_listing_uses_scoped_sql_keyset_pagination() {
    for defer in [false, true] {
        let result = RecordBatch::try_from_iter(vec![(
            "name",
            Arc::new(StringArray::from(vec!["a", "b'c", "z"])) as ArrayRef,
        )])
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
        let db = RemoteDatabase::new_mock(|request| -> http::Response<String> {
            panic!("unexpected REST request: {}", request.url().path());
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
        assert_eq!(first.items, ["a", "b'c"]);
        let token = first.page_token.unwrap();
        db.list_materialized_views(
            &namespace,
            crate::listing::ListingOptions::default()
                .page_limit(2)
                .page_token(&token),
        )
        .await
        .unwrap();
        assert_eq!(
            *statements.lock().unwrap(),
            [
                "SHOW MATERIALIZED VIEWS LIMIT 3",
                "SHOW MATERIALIZED VIEWS WHERE name > 'b''c' LIMIT 3"
            ]
        );
        assert!(
            headers
                .lock()
                .unwrap()
                .iter()
                .all(|header| header.namespace_path == "team$reports")
        );
        for (path, token) in [(vec![], token), (namespace, "invalid".into())] {
            assert!(matches!(
                db.list_materialized_views(
                    &path,
                    crate::listing::ListingOptions::default().page_token(token)
                )
                .await,
                Err(Error::InvalidInput { .. })
            ));
        }
        assert_eq!(statements.lock().unwrap().len(), 2);
        shutdown.send(()).unwrap();
    }
}

#[tokio::test]
async fn remote_describe_materialized_view_preserves_the_sql_definition() {
    for defer in [false, true] {
        let definition = "SELECT s.x, d.label FROM source s JOIN dim d ON s.id = d.id";
        let result = RecordBatch::try_from_iter(vec![(
            "query",
            Arc::new(StringArray::from(vec![definition])) as ArrayRef,
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
        let table = RemoteTable::new_with_sql_client(
            client_with_handler(|request| -> http::Response<String> {
                panic!("unexpected REST request: {}", request.url().path());
            }),
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
            ["DESCRIBE MATERIALIZED VIEW \"daily_sales\""]
        );
        shutdown.send(()).unwrap();
    }
}
