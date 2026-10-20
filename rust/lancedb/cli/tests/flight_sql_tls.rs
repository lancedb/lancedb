// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright LanceDB Authors

//! End-to-end TLS coverage for the Flight SQL transport.
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
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_schema::{DataType, Field, Schema};
use futures::{Stream, TryStreamExt};
use lancedb_cli::server_config::TlsOptions;
use lancedb_cli::sql_executor::SdkSqlExecutor as FlightSqlExecutor;
use lancedb_cli::sql_executor::{SqlQueryExecutor, SqlResult};
use std::sync::Arc;
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
#[derive(Clone)]
struct StubFlightService;

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
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
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
            .add_service(FlightServiceServer::new(StubFlightService))
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
