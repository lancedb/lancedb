# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import http.server
import threading
from unittest.mock import MagicMock
import uuid

import lancedb
import pyarrow as pa
from lancedb.remote.client import VectorQuery, VectorQueryResult
import pytest


class FakeLanceDBClient:
    def close(self):
        pass

    def query(self, table_name: str, query: VectorQuery) -> VectorQueryResult:
        assert table_name == "test"
        t = pa.schema([]).empty_table()
        return VectorQueryResult(t)

    def post(self, path: str):
        pass

    def mount_retry_adapter_for_table(self, table_name: str):
        pass


def test_remote_db():
    conn = lancedb.connect("db://client-will-be-injected", api_key="fake")
    setattr(conn, "_client", FakeLanceDBClient())

    table = conn["test"]
    table.schema = pa.schema([pa.field("vector", pa.list_(pa.float32(), 2))])
    table.search([1.0, 2.0]).to_pandas()


def test_create_empty_table():
    client = MagicMock()
    conn = lancedb.connect("db://client-will-be-injected", api_key="fake")

    conn._client = client

    schema = pa.schema([pa.field("vector", pa.list_(pa.float32(), 2))])

    client.post.return_value = {"status": "ok"}
    table = conn.create_table("test", schema=schema)
    assert table.name == "test"
    assert client.post.call_args[0][0] == "/v1/table/test/create/"

    json_schema = {
        "fields": [
            {
                "name": "vector",
                "nullable": True,
                "type": {
                    "type": "fixed_size_list",
                    "fields": [
                        {"name": "item", "nullable": True, "type": {"type": "float"}}
                    ],
                    "length": 2,
                },
            },
        ]
    }
    client.post.return_value = {"schema": json_schema}
    assert table.schema == schema
    assert client.post.call_args[0][0] == "/v1/table/test/describe/"

    client.post.return_value = 0
    assert table.count_rows(None) == 0


def test_create_table_with_recordbatches():
    client = MagicMock()
    conn = lancedb.connect("db://client-will-be-injected", api_key="fake")

    conn._client = client

    batch = pa.RecordBatch.from_arrays([pa.array([[1.0, 2.0], [3.0, 4.0]])], ["vector"])

    client.post.return_value = {"status": "ok"}
    table = conn.create_table("test", [batch], schema=batch.schema)
    assert table.name == "test"
    assert client.post.call_args[0][0] == "/v1/table/test/create/"


def make_mock_http_handler(handler):
    class MockLanceDBHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            handler(self)

        def do_POST(self):
            handler(self)

    return MockLanceDBHandler


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

    def run_server():
        with http.server.HTTPServer(
            ("localhost", 8080), make_mock_http_handler(handler)
        ) as server:
            # we will only make one request
            server.handle_request()

    handle = threading.Thread(target=run_server)
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
    table_names = await db.table_names()
    assert table_names == []

    handle.join()
