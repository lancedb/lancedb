// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::atomic::AtomicUsize;

use arrow_array::builder::StringDictionaryBuilder;
use arrow_array::{Array, Int64Array, StringArray, types::Int32Type};
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

#[derive(Clone)]
struct TestSqlService {
    query_count: Arc<AtomicUsize>,
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
    result: RecordBatch,
    large_result: RecordBatch,
    dictionary_result: RecordBatch,
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
            query_count: Arc::new(AtomicUsize::new(0)),
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
        let ticket = request.get_ref().ticket.as_ref();
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
        if query == "SELECT cancel unspecified"
            && self.cancel_unspecified_count.fetch_add(1, Ordering::SeqCst) > 0
        {
            return Err(Status::not_found("query cancellation completed"));
        }
        let status = if query == "SELECT cancel unspecified" {
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

    let unspecified_cancel = timeout_client
        .submit("SELECT cancel unspecified", &["public".to_string()])
        .await
        .unwrap();
    assert!(unspecified_cancel.cancel().await.is_err());
    assert!(unspecified_cancel.cancel().await.is_err());
    assert_ne!(
        unspecified_cancel.describe().await.unwrap().status,
        "cancelled"
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
    let tracked_dropped_reader = client.queries.get(dropped_reader.id()).unwrap();
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
        registry.insert(id, query.clone());
        assert!(Arc::ptr_eq(&registry.get(id).unwrap(), &query));
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
    registry.insert(expired_id, expired_query);
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
    registry.insert(stale_id, stale_query.clone());
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
