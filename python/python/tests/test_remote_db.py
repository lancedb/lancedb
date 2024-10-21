# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import contextlib
from datetime import timedelta
import http.server
import threading
import uuid

import lancedb
from lancedb.remote import ClientConfig
from lancedb.remote.errors import HttpError, RetryError
import pytest
import pyarrow as pa


def make_mock_http_handler(handler):
    class MockLanceDBHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            handler(self)

        def do_POST(self):
            handler(self)

    return MockLanceDBHandler


@contextlib.contextmanager
def mock_lancedb_connection(handler):
    with http.server.HTTPServer(
        ("localhost", 8080), make_mock_http_handler(handler)
    ) as server:
        handle = threading.Thread(target=server.serve_forever)
        handle.start()

        db = lancedb.connect(
            "db://dev",
            api_key="fake",
            host_override="http://localhost:8080",
            client_config={
                "retry_config": {"retries": 2},
                "timeout_config": {
                    "connect_timeout": 1,
                },
            },
        )

        try:
            yield db
        finally:
            server.shutdown()
            handle.join()


@contextlib.asynccontextmanager
async def mock_lancedb_connection_async(handler):
    with http.server.HTTPServer(
        ("localhost", 8080), make_mock_http_handler(handler)
    ) as server:
        handle = threading.Thread(target=server.serve_forever)
        handle.start()

        db = await lancedb.connect_async(
            "db://dev",
            api_key="fake",
            host_override="http://localhost:8080",
            client_config={
                "retry_config": {"retries": 2},
                "timeout_config": {
                    "connect_timeout": 1,
                },
            },
        )

        try:
            yield db
        finally:
            server.shutdown()
            handle.join()


@pytest.mark.asyncio
async def test_async_remote_db():
    def handler(request):
        # We created a UUID request id
        request_id = request.headers["x-request-id"]
        assert uuid.UUID(request_id).version == 4

        # We set a user agent with the current library version
        user_agent = request.headers["User-Agent"]
        assert user_agent == f"LanceDB-Python-Client/{lancedb.__version__}"

        request.send_response(200)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(b'{"tables": []}')

    async with mock_lancedb_connection_async(handler) as db:
        table_names = await db.table_names()
        assert table_names == []


@pytest.mark.asyncio
async def test_http_error():
    request_id_holder = {"request_id": None}

    def handler(request):
        request_id_holder["request_id"] = request.headers["x-request-id"]

        request.send_response(507)
        request.end_headers()
        request.wfile.write(b"Internal Server Error")

    async with mock_lancedb_connection_async(handler) as db:
        with pytest.raises(HttpError) as exc_info:
            await db.table_names()

        assert exc_info.value.request_id == request_id_holder["request_id"]
        assert "Internal Server Error" in str(exc_info.value)


@pytest.mark.asyncio
async def test_retry_error():
    request_id_holder = {"request_id": None}

    def handler(request):
        request_id_holder["request_id"] = request.headers["x-request-id"]

        request.send_response(429)
        request.end_headers()
        request.wfile.write(b"Try again later")

    async with mock_lancedb_connection_async(handler) as db:
        with pytest.raises(RetryError) as exc_info:
            await db.table_names()

        assert exc_info.value.request_id == request_id_holder["request_id"]

        cause = exc_info.value.__cause__
        assert isinstance(cause, HttpError)
        assert "Try again later" in str(cause)
        assert cause.request_id == request_id_holder["request_id"]
        assert cause.status_code == 429


def test_query_sync():
    def handler(request):
        if request.path == "/v1/table/test/describe/":
            request.send_response(200)
            request.send_header("Content-Type", "application/json")
            request.end_headers()
            request.wfile.write(b"{}")
        elif request.path == "/v1/table/test/query/":
            request.send_response(200)
            request.send_header("Content-Type", "application/vnd.apache.arrow.stream")
            request.end_headers()
            data = pa.table(
                {
                    "id": pa.array([1, 2, 3]),
                }
            )
            with pa.ipc.new_stream(request.wfile, schema=data.schema) as stream:
                stream.write_table(data)
        else:
            request.send_response(404)
            request.end_headers()

    with mock_lancedb_connection(handler) as db:
        assert repr(db) == "RemoteConnect(name=dev)"
        table = db.open_table("test")
        assert repr(table) == "RemoteTable(dev.test)"

        data = table.search([1, 2, 3]).to_list()
        expected = [{"id": 1}, {"id": 2}, {"id": 3}]
        assert data == expected


def test_create_client():
    mandatory_args = {
        "uri": "db://dev",
        "api_key": "fake-api-key",
        "region": "us-east-1",
    }

    db = lancedb.connect(**mandatory_args)
    assert isinstance(db.client_config, ClientConfig)

    db = lancedb.connect(**mandatory_args, client_config={})
    assert isinstance(db.client_config, ClientConfig)

    db = lancedb.connect(
        **mandatory_args,
        client_config=ClientConfig(timeout_config={"connect_timeout": 42}),
    )
    assert isinstance(db.client_config, ClientConfig)
    assert db.client_config.timeout_config.connect_timeout == timedelta(seconds=42)

    db = lancedb.connect(
        **mandatory_args,
        client_config={"timeout_config": {"connect_timeout": timedelta(seconds=42)}},
    )
    assert isinstance(db.client_config, ClientConfig)
    assert db.client_config.timeout_config.connect_timeout == timedelta(seconds=42)

    db = lancedb.connect(
        **mandatory_args, client_config=ClientConfig(retry_config={"retries": 42})
    )
    assert isinstance(db.client_config, ClientConfig)
    assert db.client_config.retry_config.retries == 42

    db = lancedb.connect(
        **mandatory_args, client_config={"retry_config": {"retries": 42}}
    )
    assert isinstance(db.client_config, ClientConfig)
    assert db.client_config.retry_config.retries == 42

    with pytest.warns(DeprecationWarning):
        db = lancedb.connect(**mandatory_args, connection_timeout=42)
        assert db.client_config.timeout_config.connect_timeout == timedelta(seconds=42)

    with pytest.warns(DeprecationWarning):
        db = lancedb.connect(**mandatory_args, read_timeout=42)
        assert db.client_config.timeout_config.read_timeout == timedelta(seconds=42)

    with pytest.warns(DeprecationWarning):
        lancedb.connect(**mandatory_args, request_thread_pool=10)
