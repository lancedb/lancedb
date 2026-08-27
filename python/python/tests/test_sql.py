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
        host_override="http://localhost:10024",
        client_config=None,
    ):
        self.api_key = "test-key"
        self.db_name = "analytics"
        self.host_override = host_override
        self.client_config = client_config or ClientConfig()
        self.closed = False

    async def close(self):
        self.closed = True


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
    monkeypatch.setattr(
        sql_module,
        "_execute_flight_sql",
        lambda query, resolved_connection, uri: expected,
    )

    actual = lancedb.sql(
        "SELECT 1",
        database="db://analytics",
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
    assert connection.closed


def test_sql_rejects_local_database(monkeypatch):
    local_connection = SimpleNamespace(closed=False)

    def close():
        local_connection.closed = True

    local_connection.close = close
    monkeypatch.setattr(lancedb, "connect", lambda *args, **kwargs: local_connection)

    with pytest.raises(ValueError, match="remote db://"):
        lancedb.sql("SELECT 1", database="/tmp/database")
    assert local_connection.closed


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
                        locations=[SimpleNamespace(uri=b"grpc://worker:10025")],
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
        FlightCallOptions=real_flight.FlightCallOptions,
        FlightClient=FakeFlightClient,
        FlightDescriptor=real_flight.FlightDescriptor,
    )
    monkeypatch.setattr(sql_module, "_flight_module", lambda: fake_flight)
    connection = FakeRemoteConnection(
        client_config=ClientConfig(
            timeout_config={"read_timeout": timedelta(seconds=7)},
            extra_headers={"X-Extra": "value"},
        )
    )

    result = sql_module._execute_flight_sql("SELECT 1", connection, None)

    assert result == pa.concat_tables([first, second])
    assert [client.uri for client in clients] == [
        "grpc://localhost:10025",
        "grpc://worker:10025",
    ]
    assert all(client.closed for client in clients)
    assert [(uri, ticket) for uri, ticket, _ in observed["get_calls"]] == [
        ("grpc://localhost:10025", "ticket-1"),
        ("grpc://worker:10025", "ticket-2"),
    ]

    all_options = [observed["get_options"]] + [
        options for _, _, options in observed["get_calls"]
    ]
    request_ids = set()
    for options in all_options:
        headers = dict(options.headers)
        assert headers[b"authorization"] == b"Bearer test-key"
        assert headers[b"database"] == b"analytics"
        assert headers[b"x-extra"] == b"value"
        request_ids.add(headers[b"x-request-id"])
        assert uuid.UUID(headers[b"x-request-id"].decode()).version == 4
        assert options.timeout == 7
    assert len(request_ids) == len(all_options)


def test_flight_dependency_error_is_deferred(monkeypatch):
    def missing_flight():
        raise ImportError("lancedb.sql requires a PyArrow build with Flight support")

    monkeypatch.setattr(sql_module, "_flight_module", missing_flight)

    with pytest.raises(ImportError, match="PyArrow build with Flight support"):
        sql_module._execute_flight_sql(
            "SELECT 1", FakeRemoteConnection(), flight_sql_uri=None
        )
