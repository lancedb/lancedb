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


sql_module = importlib.import_module("lancedb.sql")


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
    monkeypatch.setattr(lancedb, "connect", lambda *args, **kwargs: object())

    with pytest.raises(ValueError, match="remote db://"):
        lancedb.sql("SELECT 1", database="/tmp/database")


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


def test_execute_flight_sql_fetches_all_endpoints(monkeypatch):
    first = pa.table({"value": [1, 2]})
    second = pa.table({"value": [3]})
    readers = {
        "ticket-1": SimpleNamespace(read_all=lambda: first),
        "ticket-2": SimpleNamespace(read_all=lambda: second),
    }
    observed = {}

    class FakeFlightClient:
        def __init__(self, uri, **kwargs):
            observed["uri"] = uri
            observed["client_kwargs"] = kwargs
            self.closed = False

        def get_flight_info(self, descriptor, options):
            observed["descriptor"] = descriptor
            observed["options"] = options
            return SimpleNamespace(
                endpoints=[
                    SimpleNamespace(ticket="ticket-1"),
                    SimpleNamespace(ticket="ticket-2"),
                ]
            )

        def do_get(self, ticket, options):
            assert options is observed["options"]
            return readers[ticket]

        def close(self):
            self.closed = True
            observed["closed"] = True

    monkeypatch.setattr(sql_module.flight, "FlightClient", FakeFlightClient)
    connection = FakeRemoteConnection(
        client_config=ClientConfig(
            timeout_config={"read_timeout": timedelta(seconds=7)},
            extra_headers={"x-extra": "value"},
        )
    )

    result = sql_module._execute_flight_sql("SELECT 1", connection, None)

    assert result == pa.concat_tables([first, second])
    assert observed["uri"] == "grpc://localhost:10025"
    assert observed["closed"]
    headers = dict(observed["options"].headers)
    assert headers["authorization"] == "Bearer test-key"
    assert headers["database"] == "analytics"
    assert headers["x-extra"] == "value"
    assert uuid.UUID(headers["x-request-id"]).version == 4
    assert observed["options"].timeout == 7
