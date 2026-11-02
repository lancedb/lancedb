// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! End-to-end TLS and SQL file-execution coverage for the Flight SQL transport.
//!
//! A throwaway CA signs a server certificate for `127.0.0.1`, a real Flight
//! server serves it over TLS, and the CLI's executor connects with the CA
//! supplied through `--ca-cert`. The negative cases prove verification is
//! genuinely happening rather than being skipped.

use std::net::SocketAddr;
use std::pin::Pin;

use arrow_array::{Int32Array, RecordBatch};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::{Any, CommandStatementQuery};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_schema::{DataType, Field, Schema};
use futures::{Stream, TryStreamExt};
use lancedb_cli::server_config::TlsOptions;
use lancedb_cli::sql_executor::SdkSqlExecutor as FlightSqlExecutor;
use lancedb_cli::sql_executor::{SqlQueryExecutor, SqlResult};
use prost::Message;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

/// The single column the stub server returns, asserted by the tests.
const ANSWER: i32 = 42;

/// tonic's server-side TLS needs a process-level rustls crypto provider; unlike
/// the client path it has no built-in fallback.
fn init_crypto_provider() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Minimal Flight service: enough for `execute_query` to complete a round trip.
/// Everything the CLI does not call is left unimplemented on purpose.
#[derive(Clone, Default)]
struct StubFlightService {
    queries: Option<Arc<Mutex<Vec<String>>>>,
}

/// The SDK wraps a Flight SQL command in a ticket's field 2.
#[derive(Clone, PartialEq, Message)]
struct CommandTicket {
    #[prost(bytes = "vec", tag = "2")]
    command: Vec<u8>,
}

impl StubFlightService {
    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "answer",
            DataType::Int32,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![ANSWER]))]).unwrap()
    }
}

#[tonic::async_trait]
impl FlightService for StubFlightService {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new("stub"));
        Ok(Response::new(FlightInfo::new().with_endpoint(endpoint)))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        if let Some(queries) = &self.queries {
            let ticket = CommandTicket::decode(request.into_inner().ticket).unwrap();
            let command = Any::decode(ticket.command.as_slice())
                .unwrap()
                .unpack::<CommandStatementQuery>()
                .unwrap()
                .unwrap();
            queries.lock().unwrap().push(command.query);
        }
        let batch = Self::batch();
        let stream = FlightDataEncoderBuilder::new()
            .build(futures::stream::once(async move { Ok(batch) }))
            .map_err(|err| Status::internal(err.to_string()));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new("stub"));
        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .try_with_schema(&Self::batch().schema())
            .unwrap();
        Ok(Response::new(PollInfo {
            info: Some(info),
            flight_descriptor: None,
            progress: Some(1.0),
            expiration_time: None,
        }))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

/// A CA plus the server and client certificates it signed. The server
/// certificate is valid for `127.0.0.1`.
struct TestCerts {
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

fn generate_certs() -> TestCerts {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, PKCS_ECDSA_P256_SHA256};

    let mut ca_params = CertificateParams::new(vec!["Test CA".to_string()]);
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_pair = Some(KeyPair::generate(&PKCS_ECDSA_P256_SHA256).unwrap());
    let ca_cert = rcgen::Certificate::from_params(ca_params).unwrap();

    // The SANs must cover the address the client dials, or the handshake fails
    // hostname verification even with the right CA.
    let mut server_params =
        CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()]);
    server_params.key_pair = Some(KeyPair::generate(&PKCS_ECDSA_P256_SHA256).unwrap());
    let server_cert = rcgen::Certificate::from_params(server_params).unwrap();

    let mut client_params = CertificateParams::new(vec!["test-client".to_string()]);
    client_params.key_pair = Some(KeyPair::generate(&PKCS_ECDSA_P256_SHA256).unwrap());
    let client_cert = rcgen::Certificate::from_params(client_params).unwrap();

    TestCerts {
        ca_pem: ca_cert.serialize_pem().unwrap(),
        server_cert_pem: server_cert.serialize_pem_with_signer(&ca_cert).unwrap(),
        server_key_pem: server_cert.serialize_private_key_pem(),
        client_cert_pem: client_cert.serialize_pem_with_signer(&ca_cert).unwrap(),
        client_key_pem: client_cert.serialize_private_key_pem(),
    }
}

/// Start the stub Flight server over TLS on an ephemeral port. The server stops
/// when the returned shutdown sender is dropped.
async fn start_tls_server(certs: &TestCerts) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    start_server(certs, false).await
}

/// Same, but the server demands a client certificate signed by the test CA.
async fn start_mtls_server(certs: &TestCerts) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    start_server(certs, true).await
}

async fn start_server(
    certs: &TestCerts,
    require_client_cert: bool,
) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    init_crypto_provider();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();

    let identity = Identity::from_pem(&certs.server_cert_pem, &certs.server_key_pem);
    let mut tls = ServerTlsConfig::new().identity(identity);
    if require_client_cert {
        tls = tls.client_ca_root(tonic::transport::Certificate::from_pem(&certs.ca_pem));
    }
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        Server::builder()
            .tls_config(tls)
            .unwrap()
            .add_service(FlightServiceServer::new(StubFlightService::default()))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    (addr, shutdown_tx)
}

fn write_pem(dir: &tempfile::TempDir, name: &str, contents: &str) -> String {
    let path = dir.path().join(name);
    std::fs::write(&path, contents).unwrap();
    path.to_str().unwrap().to_string()
}

/// Exercise argument parsing, file reading, script execution, and SQL transport.
/// The stub records SQL without executing it against a database.
async fn run_sql_cli(args: &[&str], script: Option<&str>) -> (std::process::Output, Vec<String>) {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let sql_endpoint = format!("http://{}", listener.local_addr().unwrap());
    let service = StubFlightService {
        queries: Some(queries.clone()),
    };
    let sql_server = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let http_endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app =
        axum::Router::new().fallback(|| async { axum::Json(serde_json::json!({"tables": []})) });
    let http_server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("script.sql");
    if let Some(script) = script {
        std::fs::write(&path, script).unwrap();
    }
    let mut config = lancedb_cli::server_config::Config::default();
    config.set_profile(
        "test",
        lancedb_cli::server_config::Profile {
            sql_server_url: Some(sql_endpoint),
            http_server_url: Some(http_endpoint),
            database: Some("test".into()),
            api_key: Some("test-key".into()),
            ..Default::default()
        },
    );
    config.set_default_profile("test");
    config
        .save_to_path(&dir.path().join(".lancedb/config.toml"))
        .unwrap();
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_lancedb"))
        .args(args)
        .current_dir(dir.path())
        .stdin(std::process::Stdio::null())
        .env("HOME", dir.path())
        .env("XDG_DATA_HOME", dir.path().join("data"))
        .env("XDG_CONFIG_HOME", dir.path().join("config"))
        .env_remove("LANCEDB_API_KEY")
        .env_remove("LANCEDB_OIDC_ACCESS_TOKEN")
        .env_remove("LANCEDB_OIDC_ACCESS_TOKEN_FILE")
        .env_remove("LANCEDB_CA_CERT")
        .env_remove("LANCEDB_CLIENT_CERT")
        .env_remove("LANCEDB_CLIENT_KEY")
        .output()
        .await
        .unwrap();
    sql_server.abort();
    http_server.abort();
    let recorded = queries.lock().unwrap().clone();
    (output, recorded)
}

async fn run_sql_file(script: &str) -> (std::process::Output, Vec<String>) {
    run_sql_cli(
        &["sql", "--file", "script.sql", "--format", "json"],
        Some(script),
    )
    .await
}

#[tokio::test]
async fn no_subcommand_opens_the_sql_shell() {
    for args in [vec![], vec!["sql"]] {
        let (output, queries) = run_sql_cli(&args, None).await;
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("LanceDB CLI v"), "{stdout}");
        assert!(
            stdout.contains("Type \\? for help or \\q to quit"),
            "{stdout}"
        );
        assert!(queries.is_empty());
    }
}

#[tokio::test]
async fn sql_options_work_with_and_without_the_subcommand() {
    for prefix in [vec![], vec!["sql"]] {
        let mut args = prefix.clone();
        args.extend(["--command", "SELECT 42", "--format", "json"]);
        let (output, queries) = run_sql_cli(&args, None).await;
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(queries, ["SELECT 42"]);
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value, serde_json::json!([{"answer": ANSWER}]));

        let mut args = prefix;
        args.extend(["--file", "script.sql", "--format", "json"]);
        let (output, queries) = run_sql_cli(&args, Some("SELECT 'semi;colon';")).await;
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(queries, ["SELECT 'semi;colon'"]);
    }
}

#[tokio::test]
async fn sql_files_preserve_comments_and_quoted_semicolons() {
    for (script, expected) in [
        (
            "SHOW JOBS; CANCEL JOB 'job-123';",
            vec!["SHOW JOBS", "CANCEL JOB 'job-123'"],
        ),
        (
            "SELECT 1 -- documentation; DELETE FROM customers\n;",
            vec!["SELECT 1 -- documentation; DELETE FROM customers"],
        ),
        (
            "SELECT 1 /* documentation; DELETE FROM customers */; SELECT 2;",
            vec![
                "SELECT 1 /* documentation; DELETE FROM customers */",
                "SELECT 2",
            ],
        ),
        (
            "/* outer; /* nested; DELETE FROM customers */ still a comment; */ SELECT 3;",
            vec!["/* outer; /* nested; DELETE FROM customers */ still a comment; */ SELECT 3"],
        ),
        (
            "SELECT 1 /*! ; DELETE FROM customers */;",
            vec!["SELECT 1 /*! ; DELETE FROM customers */"],
        ),
        (
            r#"SELECT 'it''s; unchanged', "col;""name", `back;``tick`; SELECT 'next'"#,
            vec![
                r#"SELECT 'it''s; unchanged', "col;""name", `back;``tick`"#,
                "SELECT 'next'",
            ],
        ),
        (
            "SELECT $$text; -- still text$$, $tag$more; /* text */$tag$; SELECT 3;",
            vec![
                "SELECT $$text; -- still text$$, $tag$more; /* text */$tag$",
                "SELECT 3",
            ],
        ),
        (
            r"SELECT E'it\'s; unchanged'; SELECT 2;",
            vec![r"SELECT E'it\'s; unchanged'", "SELECT 2"],
        ),
        (
            "SELECT 'text; -- still a string /* not a comment */'; SELECT 2;",
            vec![
                "SELECT 'text; -- still a string /* not a comment */'",
                "SELECT 2",
            ],
        ),
        (
            "SELECT 'café; 🦀';\r\n-- comment; DELETE FROM customers\r\nSELECT 2; ;",
            vec![
                "SELECT 'café; 🦀'",
                "-- comment; DELETE FROM customers\r\nSELECT 2",
            ],
        ),
        (
            "; -- documentation; DELETE FROM customers\n; /* comment; SELECT 2 */; ;",
            vec![],
        ),
    ] {
        let (output, queries) = run_sql_file(script).await;
        assert!(
            output.status.success(),
            "script {script:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(queries, expected, "script: {script:?}");
    }
}

#[tokio::test]
async fn sql_files_with_unterminated_tokens_submit_no_statements() {
    for script in [
        "SELECT 1; SELECT 'unterminated; DELETE FROM customers",
        "SELECT 1; SELECT \"unterminated; DELETE FROM customers",
        "SELECT 1; SELECT `unterminated; DELETE FROM customers",
        "SELECT 1; SELECT $tag$unterminated; DELETE FROM customers",
        "SELECT 1; /* unterminated; DELETE FROM customers",
    ] {
        let (output, queries) = run_sql_file(script).await;
        assert!(!output.status.success(), "script: {script:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("Failed to tokenize SQL script"),
            "unexpected error: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            queries.is_empty(),
            "submitted SQL for {script:?}: {queries:?}"
        );
    }
}

#[tokio::test]
async fn ca_cert_enables_a_tls_flight_sql_query() {
    let certs = generate_certs();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = write_pem(&dir, "ca.pem", &certs.ca_pem);
    let (addr, _shutdown) = start_tls_server(&certs).await;

    let mut executor = FlightSqlExecutor::new_with_api_key(String::new(), "sk_test".to_string())
        .with_tls(TlsOptions {
            ca_cert: Some(ca_path),
            ..TlsOptions::default()
        });

    executor
        .connect(&format!("https://{addr}"))
        .await
        .expect("TLS connect with the signing CA should succeed");
    assert!(executor.is_connected().await);

    let result = executor
        .execute_query("SELECT 1")
        .await
        .expect("query over TLS should succeed");
    match result {
        SqlResult::Query(batches) => {
            let rows: Vec<i32> = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            assert_eq!(rows, vec![ANSWER]);
        }
        other => panic!("expected a query result, got {other:?}"),
    }
}

#[tokio::test]
async fn tls_connect_fails_without_the_signing_ca() {
    let certs = generate_certs();
    let (addr, _shutdown) = start_tls_server(&certs).await;

    // No CA configured: the server's certificate chains to a CA that is not in
    // the platform trust store, so verification must reject it.
    let mut executor = FlightSqlExecutor::new_with_api_key(String::new(), "sk_test".to_string());

    assert!(
        executor.connect(&format!("https://{addr}")).await.is_err(),
        "an untrusted server certificate must not be accepted"
    );
    assert!(!executor.is_connected().await);
}

#[tokio::test]
async fn tls_connect_fails_with_an_unrelated_ca() {
    let certs = generate_certs();
    // A second, unrelated CA: right file shape, wrong signer.
    let other = generate_certs();
    let dir = tempfile::tempdir().unwrap();
    let unrelated_ca = write_pem(&dir, "other-ca.pem", &other.ca_pem);
    let (addr, _shutdown) = start_tls_server(&certs).await;

    let mut executor = FlightSqlExecutor::new_with_api_key(String::new(), "sk_test".to_string())
        .with_tls(TlsOptions {
            ca_cert: Some(unrelated_ca),
            ..TlsOptions::default()
        });

    assert!(
        executor.connect(&format!("https://{addr}")).await.is_err(),
        "a certificate signed by a different CA must not be accepted"
    );
}

#[tokio::test]
async fn a_missing_ca_cert_file_is_reported() {
    let mut executor = FlightSqlExecutor::new_with_api_key(String::new(), "sk_test".to_string())
        .with_tls(TlsOptions {
            ca_cert: Some("/nonexistent/ca.pem".to_string()),
            ..TlsOptions::default()
        });

    let err = executor
        .connect("https://127.0.0.1:1")
        .await
        .expect_err("a missing CA file must fail loudly");
    assert!(
        err.to_string().contains("/nonexistent/ca.pem"),
        "error should name the missing file, got: {err}"
    );
}

#[tokio::test]
async fn a_client_certificate_satisfies_an_mtls_server() {
    let certs = generate_certs();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = write_pem(&dir, "ca.pem", &certs.ca_pem);
    let cert_path = write_pem(&dir, "client.pem", &certs.client_cert_pem);
    let key_path = write_pem(&dir, "client-key.pem", &certs.client_key_pem);
    let (addr, _shutdown) = start_mtls_server(&certs).await;

    let mut executor = FlightSqlExecutor::new_with_api_key(String::new(), "sk_test".to_string())
        .with_tls(TlsOptions {
            ca_cert: Some(ca_path),
            client_cert: Some(cert_path),
            client_key: Some(key_path),
        });

    executor
        .connect(&format!("https://{addr}"))
        .await
        .expect("mTLS connect with a CA-signed client certificate should succeed");
    executor
        .execute_query("SELECT 1")
        .await
        .expect("query over mTLS should succeed");
}

#[tokio::test]
async fn an_mtls_server_rejects_a_client_with_no_certificate() {
    let certs = generate_certs();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = write_pem(&dir, "ca.pem", &certs.ca_pem);
    let (addr, _shutdown) = start_mtls_server(&certs).await;

    // Trusts the server, but presents no identity of its own.
    let mut executor = FlightSqlExecutor::new_with_api_key(String::new(), "sk_test".to_string())
        .with_tls(TlsOptions {
            ca_cert: Some(ca_path),
            ..TlsOptions::default()
        });

    // TLS 1.3 sends the client certificate after the client considers the
    // handshake done, so the rejection can surface at connect or at the first
    // request. Either way the exchange must not succeed.
    let rejected = match executor.connect(&format!("https://{addr}")).await {
        Err(_) => true,
        Ok(()) => executor.execute_query("SELECT 1").await.is_err(),
    };
    assert!(
        rejected,
        "an mTLS server must reject a client with no certificate"
    );
}

#[tokio::test]
async fn a_client_certificate_without_its_key_is_rejected_at_resolve() {
    use lancedb_cli::server_config::ConnectionArgs;

    let args = ConnectionArgs {
        client_cert: Some("/tmp/client.pem".to_string()),
        ..ConnectionArgs::default()
    };
    let err = args
        .resolve(None)
        .expect_err("a client certificate without a key must be rejected");
    assert!(
        err.to_string().contains("--client-key"),
        "error should name the missing flag, got: {err}"
    );
}
