# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from datetime import timedelta
import importlib
from types import SimpleNamespace
import uuid

import pyarrow as pa
import pytest

import lancedb
from lancedb.remote import ClientConfig
from lancedb.remote import TlsConfig


sql_module = importlib.import_module("lancedb._sql")


class FakeRemoteConnection:
    def __init__(
        self,
        *,
        db_name="analytics",
        host_override="http://localhost:10024",
        client_config=None,
    ):
        self.api_key = "test-key"
        self.db_name = db_name
        self.host_override = host_override
        self.client_config = client_config or ClientConfig()
        self.closed = False

    async def close(self):
        self.closed = True


class RecordingFlightCallOptions:
    def __init__(self, *, timeout=None, headers=None):
        self.timeout_argument = timeout
        self.headers = headers


def test_sql_resolves_database_with_connect(monkeypatch):
    connection = FakeRemoteConnection()
    connect_args = {}
    expected = pa.table({"value": [1]})

    def connect(database, **kwargs):
        connect_args["database"] = database
        connect_args.update(kwargs)
        return connection

    monkeypatch.setattr(lancedb, "connect", connect)
    monkeypatch.setattr(sql_module, "RemoteDBConnection", FakeRemoteConnection)
    execute_args = {}

    def execute(query, resolved_connection, uri, namespace_path):
        execute_args.update(
            query=query,
            connection=resolved_connection,
            uri=uri,
            namespace_path=namespace_path,
        )
        return expected

    monkeypatch.setattr(sql_module, "_execute_flight_sql", execute)

    actual = lancedb.sql(
        "SELECT 1",
        database="analytics",
        namespace_path="events$raw",
        api_key="test-key",
        region="us-east-2",
        host_override="http://localhost:10024",
        flight_sql_uri="grpc://localhost:10025",
        storage_options={"allow_http": "true"},
    )

    assert actual == expected
    assert connect_args == {
        "database": "db://analytics",
        "api_key": "test-key",
        "region": "us-east-2",
        "host_override": "http://localhost:10024",
        "client_config": None,
        "storage_options": {"allow_http": "true"},
    }
    assert execute_args == {
        "query": "SELECT 1",
        "connection": connection,
        "uri": "grpc://localhost:10025",
        "namespace_path": "events$raw",
    }
    assert connection.closed


def test_sql_uses_default_database_and_namespace(monkeypatch):
    connection = FakeRemoteConnection(db_name="lancedb")
    observed = {}

    def connect(database, **kwargs):
        observed["database_uri"] = database
        return connection

    def execute(query, resolved_connection, uri, namespace_path):
        observed["namespace_path"] = namespace_path
        return pa.table({"value": [1]})

    monkeypatch.setattr(lancedb, "connect", connect)
    monkeypatch.setattr(sql_module, "RemoteDBConnection", FakeRemoteConnection)
    monkeypatch.setattr(sql_module, "_execute_flight_sql", execute)

    lancedb.sql("SELECT 1")

    assert observed == {
        "database_uri": "db://lancedb",
        "namespace_path": "public",
    }
    assert connection.closed


@pytest.mark.parametrize(
    "database",
    ["", "db://analytics", "analytics/tenant1", "user@analytics", "analytics:1234"],
)
def test_sql_rejects_invalid_database_name(monkeypatch, database):
    def connect(*args, **kwargs):
        pytest.fail("invalid database must be rejected before connecting")

    monkeypatch.setattr(lancedb, "connect", connect)
    with pytest.raises(ValueError, match="database"):
        lancedb.sql("SELECT 1", database=database)


@pytest.mark.parametrize("namespace_path", ["", "café"])
def test_sql_rejects_invalid_namespace_path(monkeypatch, namespace_path):
    def connect(*args, **kwargs):
        pytest.fail("invalid namespace must be rejected before connecting")

    monkeypatch.setattr(lancedb, "connect", connect)
    with pytest.raises(ValueError, match="namespace_path"):
        lancedb.sql("SELECT 1", namespace_path=namespace_path)


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("grpc://sql.example.com:50051", "grpc://sql.example.com:50051"),
        ("grpc+tcp://sql.example.com", "grpc://sql.example.com:10025"),
        ("grpcs://sql.example.com", "grpc+tls://sql.example.com:10026"),
        ("https://[::1]:443", "grpc+tls://[::1]:443"),
    ],
)
def test_normalize_flight_sql_uri(uri, expected):
    assert sql_module._normalize_flight_sql_uri(uri) == expected


def test_rejects_zero_flight_sql_port():
    with pytest.raises(ValueError, match="greater than zero"):
        sql_module._normalize_flight_sql_uri("grpc://localhost:0")


def test_derive_flight_sql_uri_from_plaintext_host_override():
    connection = FakeRemoteConnection(host_override="http://localhost:10024")

    assert (
        sql_module._resolve_flight_sql_uri(connection, None) == "grpc://localhost:10025"
    )


@pytest.mark.parametrize(
    "host_override",
    [None, "https://sql.example.com:10024"],
)
def test_flight_sql_uri_required_when_endpoint_is_ambiguous(host_override):
    connection = FakeRemoteConnection(host_override=host_override)

    with pytest.raises(ValueError, match="flight_sql_uri is required"):
        sql_module._resolve_flight_sql_uri(connection, None)


def test_encode_command_statement_query():
    query = "SELECT 1"
    query_bytes = query.encode()
    command = b"\x0a" + bytes([len(query_bytes)]) + query_bytes
    type_url = sql_module._COMMAND_STATEMENT_QUERY_TYPE_URL
    expected = (
        b"\x0a"
        + bytes([len(type_url)])
        + type_url
        + b"\x12"
        + bytes([len(command)])
        + command
    )

    assert sql_module._encode_command_statement_query(query) == expected


def test_rejects_unsupported_hostname_verification_setting():
    connection = FakeRemoteConnection(
        host_override="https://localhost:10024",
        client_config=ClientConfig(tls_config=TlsConfig(assert_hostname=False)),
    )

    with pytest.raises(ValueError, match="cannot disable hostname verification"):
        sql_module._flight_client_kwargs(connection, "grpc+tls://localhost:10026")


def test_tls_custom_ca_is_added_to_public_roots(monkeypatch, tmp_path):
    public_ca_file = tmp_path / "public-ca.pem"
    public_ca_file.write_bytes(b"PUBLIC-CA\n")
    ca_file = tmp_path / "ca.pem"
    ca_file.write_bytes(b"CUSTOM-CA")
    monkeypatch.setattr(sql_module.certifi, "where", lambda: str(public_ca_file))
    connection = FakeRemoteConnection(
        client_config=ClientConfig(tls_config=TlsConfig(ssl_ca_cert=str(ca_file)))
    )

    kwargs = sql_module._flight_client_kwargs(connection, "grpc+tls://localhost:10026")

    assert kwargs["tls_root_certs"] == b"PUBLIC-CA\nCUSTOM-CA"


def test_mtls_requires_certificate_and_key():
    connection = FakeRemoteConnection(
        client_config=ClientConfig(tls_config=TlsConfig(cert_file="cert.pem"))
    )

    with pytest.raises(ValueError, match="requires both cert_file and key_file"):
        sql_module._flight_client_kwargs(connection, "grpc+tls://localhost:10026")


def test_execute_flight_sql_fetches_all_endpoints(monkeypatch):
    first = pa.table({"value": [1, 2]})
    second = pa.table({"value": [3]})
    readers = {
        "ticket-1": SimpleNamespace(read_all=lambda: first),
        "ticket-2": SimpleNamespace(read_all=lambda: second),
    }
    observed = {}

    clients = []

    class FakeFlightClient:
        def __init__(self, uri, **kwargs):
            self.uri = uri
            self.client_kwargs = kwargs
            self.closed = False
            clients.append(self)

        def get_flight_info(self, descriptor, options):
            observed["descriptor"] = descriptor
            observed["get_options"] = options
            return SimpleNamespace(
                endpoints=[
                    SimpleNamespace(ticket="ticket-1", locations=[]),
                    SimpleNamespace(
                        ticket="ticket-2",
                        locations=[SimpleNamespace(uri=b"grpcs://worker")],
                    ),
                ]
            )

        def do_get(self, ticket, options):
            observed.setdefault("get_calls", []).append((self.uri, ticket, options))
            return readers[ticket]

        def close(self):
            self.closed = True

    real_flight = sql_module._flight_module()
    fake_flight = SimpleNamespace(
        FlightCallOptions=RecordingFlightCallOptions,
        FlightClient=FakeFlightClient,
        FlightDescriptor=real_flight.FlightDescriptor,
    )
    monkeypatch.setattr(sql_module, "_flight_module", lambda: fake_flight)
    connection = FakeRemoteConnection(
        client_config=ClientConfig(
            timeout_config={"read_timeout": timedelta(seconds=7)},
            extra_headers={"X-Extra": "value"},
            header_provider=SimpleNamespace(
                get_headers=lambda: {"Authorization": "Bearer oauth-token"}
            ),
        )
    )

    result = sql_module._execute_flight_sql(
        "SELECT 1", connection, None, namespace_path="events$raw"
    )

    assert result == pa.concat_tables([first, second])
    assert [client.uri for client in clients] == [
        "grpc://localhost:10025",
        "grpc+tls://worker:10026",
    ]
    assert all(client.closed for client in clients)
    assert [(uri, ticket) for uri, ticket, _ in observed["get_calls"]] == [
        ("grpc://localhost:10025", "ticket-1"),
        ("grpc+tls://worker:10026", "ticket-2"),
    ]

    all_options = [observed["get_options"]] + [
        options for _, _, options in observed["get_calls"]
    ]
    request_ids = set()
    for options in all_options:
        headers = dict(options.headers)
        assert headers[b"authorization"] == b"Bearer oauth-token"
        assert headers[b"database"] == b"analytics"
        assert headers[b"namespace-path"] == b"events$raw"
        assert headers[b"x-extra"] == b"value"
        request_ids.add(headers[b"x-request-id"])
        assert uuid.UUID(headers[b"x-request-id"].decode()).version == 4
    assert observed["get_options"].timeout_argument == 7
    assert all(
        options.timeout_argument is None for _, _, options in observed["get_calls"]
    )
    assert len(request_ids) == 1


def test_flight_error_preserves_type_and_includes_statement_request_id(monkeypatch):
    observed = {}

    class FailingFlightClient:
        def __init__(self, uri, **kwargs):
            self.closed = False

        def get_flight_info(self, descriptor, options):
            observed["request_id"] = dict(options.headers)[b"x-request-id"].decode()
            raise OSError("server unavailable")

        def close(self):
            self.closed = True
            observed["closed"] = True

    real_flight = sql_module._flight_module()
    fake_flight = SimpleNamespace(
        FlightCallOptions=real_flight.FlightCallOptions,
        FlightClient=FailingFlightClient,
        FlightDescriptor=real_flight.FlightDescriptor,
    )
    monkeypatch.setattr(sql_module, "_flight_module", lambda: fake_flight)

    with pytest.raises(OSError, match="Flight SQL request") as exc_info:
        sql_module._execute_flight_sql("SELECT 1", FakeRemoteConnection(), None)

    assert observed["request_id"] in str(exc_info.value)
    assert observed["closed"]


def test_flight_options_use_default_timeout():
    flight = SimpleNamespace(FlightCallOptions=RecordingFlightCallOptions)
    planning_options = sql_module._flight_call_options(
        FakeRemoteConnection(), flight, "request-id"
    )
    streaming_options = sql_module._flight_call_options(
        FakeRemoteConnection(),
        flight,
        "request-id",
        streaming=True,
    )

    assert planning_options.timeout_argument == 300
    assert streaming_options.timeout_argument is None


def test_flight_options_use_remaining_overall_deadline(monkeypatch):
    monkeypatch.setattr(sql_module.time, "monotonic", lambda: 100.0)
    flight = SimpleNamespace(FlightCallOptions=RecordingFlightCallOptions)

    options = sql_module._flight_call_options(
        FakeRemoteConnection(),
        flight,
        "request-id",
        streaming=True,
        deadline=112.5,
    )

    assert options.timeout_argument == 12.5


def test_flight_options_use_read_timeout_environment(monkeypatch):
    monkeypatch.setenv("LANCE_CLIENT_READ_TIMEOUT", "17")
    flight = SimpleNamespace(FlightCallOptions=RecordingFlightCallOptions)

    options = sql_module._flight_call_options(
        FakeRemoteConnection(), flight, "request-id"
    )

    assert options.timeout_argument == 17


def test_timeout_environment_fallback_and_precedence(monkeypatch):
    monkeypatch.setenv("LANCE_CLIENT_TIMEOUT", "29")

    assert sql_module._timeout_seconds(None, "LANCE_CLIENT_TIMEOUT") == 29
    assert (
        sql_module._timeout_seconds(timedelta(seconds=7), "LANCE_CLIENT_TIMEOUT") == 7
    )


def test_timeout_environment_rejects_invalid_values(monkeypatch):
    monkeypatch.setenv("LANCE_CLIENT_TIMEOUT", "-1")

    with pytest.raises(ValueError, match="LANCE_CLIENT_TIMEOUT"):
        sql_module._timeout_seconds(None, "LANCE_CLIENT_TIMEOUT")


def test_explicit_api_key_header_suppresses_derived_bearer():
    connection = FakeRemoteConnection(
        client_config=ClientConfig(extra_headers={"X-Api-Key": "other-key"})
    )

    options = sql_module._flight_call_options(
        connection, sql_module._flight_module(), "request-id"
    )

    headers = dict(options.headers)
    assert headers[b"x-api-key"] == b"other-key"
    assert b"authorization" not in headers


def test_tls_coordinator_rejects_plaintext_endpoint():
    endpoint = SimpleNamespace(
        ticket="ticket", locations=[SimpleNamespace(uri=b"grpc://worker:10025")]
    )

    with pytest.raises(ValueError, match="TLS-to-plaintext"):
        sql_module._read_endpoint(
            endpoint,
            primary_client=SimpleNamespace(),
            connection=FakeRemoteConnection(),
            flight=sql_module._flight_module(),
            request_id="request-id",
            primary_uri="grpc+tls://coordinator:10026",
            deadline=None,
        )


def test_rejects_non_ascii_metadata():
    connection = FakeRemoteConnection(
        client_config=ClientConfig(extra_headers={"x-name": "José"})
    )

    with pytest.raises(ValueError, match="metadata must be ASCII"):
        sql_module._flight_call_options(
            connection, sql_module._flight_module(), "request-id"
        )


def test_flight_dependency_error_is_deferred(monkeypatch):
    def missing_flight():
        raise ImportError("lancedb.sql requires a PyArrow build with Flight support")

    monkeypatch.setattr(sql_module, "_flight_module", missing_flight)

    with pytest.raises(ImportError, match="PyArrow build with Flight support"):
        sql_module._execute_flight_sql(
            "SELECT 1", FakeRemoteConnection(), flight_sql_uri=None
        )
