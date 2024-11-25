# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from concurrent.futures import ThreadPoolExecutor
import contextlib
from datetime import timedelta
import http.server
import json
import threading
from unittest.mock import MagicMock
import uuid

import lancedb
from lancedb.conftest import MockTextEmbeddingFunction
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
async def test_async_checkout():
    def handler(request):
        if request.path == "/v1/table/test/describe/":
            request.send_response(200)
            request.send_header("Content-Type", "application/json")
            request.end_headers()
            response = json.dumps({"version": 42, "schema": {"fields": []}})
            request.wfile.write(response.encode())
            return

        content_len = int(request.headers.get("Content-Length"))
        body = request.rfile.read(content_len)
        body = json.loads(body)

        print("body is", body)

        count = 0
        if body["version"] == 1:
            count = 100
        elif body["version"] == 2:
            count = 200
        elif body["version"] is None:
            count = 300

        request.send_response(200)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(json.dumps(count).encode())

    async with mock_lancedb_connection_async(handler) as db:
        table = await db.open_table("test")
        assert await table.count_rows() == 300
        await table.checkout(1)
        assert await table.count_rows() == 100
        await table.checkout(2)
        assert await table.count_rows() == 200
        await table.checkout_latest()
        assert await table.count_rows() == 300


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


def test_table_add_in_threadpool():
    def handler(request):
        if request.path == "/v1/table/test/insert/":
            request.send_response(200)
            request.end_headers()
        elif request.path == "/v1/table/test/create/":
            request.send_response(200)
            request.send_header("Content-Type", "application/json")
            request.end_headers()
            request.wfile.write(b"{}")
        elif request.path == "/v1/table/test/describe/":
            request.send_response(200)
            request.send_header("Content-Type", "application/json")
            request.end_headers()
            payload = json.dumps(
                dict(
                    version=1,
                    schema=dict(
                        fields=[
                            dict(name="id", type={"type": "int64"}, nullable=False),
                        ]
                    ),
                )
            )
            request.wfile.write(payload.encode())
        else:
            request.send_response(404)
            request.end_headers()

    with mock_lancedb_connection(handler) as db:
        table = db.create_table("test", [{"id": 1}])
        with ThreadPoolExecutor(3) as executor:
            futures = []
            for _ in range(10):
                future = executor.submit(table.add, [{"id": 1}])
                futures.append(future)

            for future in futures:
                future.result()


@contextlib.contextmanager
def query_test_table(query_handler):
    def handler(request):
        if request.path == "/v1/table/test/describe/":
            request.send_response(200)
            request.send_header("Content-Type", "application/json")
            request.end_headers()
            request.wfile.write(b"{}")
        elif request.path == "/v1/table/test/query/":
            content_len = int(request.headers.get("Content-Length"))
            body = request.rfile.read(content_len)
            body = json.loads(body)

            data = query_handler(body)

            request.send_response(200)
            request.send_header("Content-Type", "application/vnd.apache.arrow.file")
            request.end_headers()

            with pa.ipc.new_file(request.wfile, schema=data.schema) as f:
                f.write_table(data)
        else:
            request.send_response(404)
            request.end_headers()

    with mock_lancedb_connection(handler) as db:
        assert repr(db) == "RemoteConnect(name=dev)"
        table = db.open_table("test")
        assert repr(table) == "RemoteTable(dev.test)"
        yield table


def test_query_sync_minimal():
    def handler(body):
        assert body == {
            "distance_type": "l2",
            "k": 10,
            "prefilter": False,
            "refine_factor": None,
            "ef": None,
            "vector": [1.0, 2.0, 3.0],
            "nprobes": 20,
            "version": None,
        }

        return pa.table({"id": [1, 2, 3]})

    with query_test_table(handler) as table:
        data = table.search([1, 2, 3]).to_list()
        expected = [{"id": 1}, {"id": 2}, {"id": 3}]
        assert data == expected


def test_query_sync_empty_query():
    def handler(body):
        assert body == {
            "k": 10,
            "filter": "true",
            "vector": [],
            "columns": ["id"],
            "version": None,
        }

        return pa.table({"id": [1, 2, 3]})

    with query_test_table(handler) as table:
        data = table.search(None).where("true").select(["id"]).limit(10).to_list()
        expected = [{"id": 1}, {"id": 2}, {"id": 3}]
        assert data == expected


def test_query_sync_maximal():
    def handler(body):
        assert body == {
            "distance_type": "cosine",
            "k": 42,
            "prefilter": True,
            "refine_factor": 10,
            "vector": [1.0, 2.0, 3.0],
            "nprobes": 5,
            "ef": None,
            "filter": "id > 0",
            "columns": ["id", "name"],
            "vector_column": "vector2",
            "fast_search": True,
            "with_row_id": True,
            "version": None,
        }

        return pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    with query_test_table(handler) as table:
        (
            table.search([1, 2, 3], vector_column_name="vector2", fast_search=True)
            .metric("cosine")
            .limit(42)
            .refine_factor(10)
            .nprobes(5)
            .where("id > 0", prefilter=True)
            .with_row_id(True)
            .select(["id", "name"])
            .to_list()
        )


def test_query_sync_multiple_vectors():
    def handler(_body):
        return pa.table({"id": [1]})

    with query_test_table(handler) as table:
        results = table.search([[1, 2, 3], [4, 5, 6]]).limit(1).to_list()
        assert len(results) == 2
        results.sort(key=lambda x: x["query_index"])
        assert results == [{"id": 1, "query_index": 0}, {"id": 1, "query_index": 1}]


def test_query_sync_fts():
    def handler(body):
        assert body == {
            "full_text_query": {
                "query": "puppy",
                "columns": [],
            },
            "k": 10,
            "vector": [],
            "version": None,
        }

        return pa.table({"id": [1, 2, 3]})

    with query_test_table(handler) as table:
        (table.search("puppy", query_type="fts").to_list())

    def handler(body):
        assert body == {
            "full_text_query": {
                "query": "puppy",
                "columns": ["name", "description"],
            },
            "k": 42,
            "vector": [],
            "with_row_id": True,
            "version": None,
        }

        return pa.table({"id": [1, 2, 3]})

    with query_test_table(handler) as table:
        (
            table.search("puppy", query_type="fts", fts_columns=["name", "description"])
            .with_row_id(True)
            .limit(42)
            .to_list()
        )


def test_query_sync_hybrid():
    def handler(body):
        if "full_text_query" in body:
            # FTS query
            assert body == {
                "full_text_query": {
                    "query": "puppy",
                    "columns": [],
                },
                "k": 42,
                "vector": [],
                "with_row_id": True,
                "version": None,
            }
            return pa.table({"_rowid": [1, 2, 3], "_score": [0.1, 0.2, 0.3]})
        else:
            # Vector query
            assert body == {
                "distance_type": "l2",
                "k": 42,
                "prefilter": False,
                "refine_factor": None,
                "vector": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "nprobes": 20,
                "ef": None,
                "with_row_id": True,
                "version": None,
            }
            return pa.table({"_rowid": [1, 2, 3], "_distance": [0.1, 0.2, 0.3]})

    with query_test_table(handler) as table:
        embedding_func = MockTextEmbeddingFunction()
        embedding_config = MagicMock()
        embedding_config.function = embedding_func

        embedding_funcs = MagicMock()
        embedding_funcs.get = MagicMock(return_value=embedding_config)
        table.embedding_functions = embedding_funcs

        (table.search("puppy", query_type="hybrid").limit(42).to_list())


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
