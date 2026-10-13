# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Thread

import pytest

import lancedb
from lancedb.db import AsyncConnection, DBConnection
from lancedb.remote.errors import HttpError


@pytest.fixture
def catalog_server():
    requests = []
    responses = []

    class Handler(BaseHTTPRequestHandler):
        def handle_request(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            requests.append(
                (
                    self.path,
                    dict(self.headers.items()),
                    json.loads(body) if body else None,
                )
            )
            status, response = responses.pop(0)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            if status != 204:
                self.wfile.write(json.dumps(response).encode())

        do_GET = handle_request
        do_POST = handle_request

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}", requests, responses
        finally:
            server.shutdown()
            thread.join()


def test_catalog_sync_scope_and_serialization(catalog_server):
    endpoint, requests, responses = catalog_server
    responses.extend(
        [
            (204, None),
            (200, {}),
            (200, {"tables": []}),
            (200, {"tables": []}),
            (200, {"namespaces": ["team/search"]}),
            (204, None),
        ]
    )
    catalog = lancedb.connect_catalog(
        endpoint,
        api_key="secret",
        sql_host_override="invalid://localhost",
        client_config={
            "extra_headers": {
                "X-LanceDB-Database": "wrong",
                "X-LanceDB-Database-Prefix": "wrong",
            }
        },
    )
    assert isinstance(catalog, lancedb.Catalog)
    assert catalog.uri == endpoint
    db = catalog.create_database("team/search", exist_ok=True)
    assert isinstance(db, DBConnection)
    with pytest.raises(ValueError, match="sql_host_override must use"):
        db.execute_query_async("SELECT 1")
    db = catalog.connect_database("team/search")
    assert isinstance(db, DBConnection)
    assert db.table_names() == []
    restored = lancedb.deserialize_conn(db.serialize())
    assert restored.sql_host_override == "invalid://localhost"
    for connection in (db, restored):
        with pytest.raises(ValueError, match="sql_host_override must use"):
            connection.execute_query_async("SELECT 1")
    assert restored.table_names() == []
    assert list(catalog.list_databases()) == ["team/search"]
    catalog.drop_database("team/search", ignore_missing=True)
    assert requests[0][0] == "/v1/namespace/team%2Fsearch/create"
    assert requests[0][2] == {"mode": "ExistOk"}
    assert requests[1][0] == "/v1/namespace/team%2Fsearch/describe"
    assert requests[4][0] == "/v1/namespace/$/list"
    assert requests[5][2] == {"mode": "Skip", "behavior": "Restrict"}
    for i, (_, headers, _) in enumerate(requests):
        headers = {key.lower(): value for key, value in headers.items()}
        assert headers.get("x-lancedb-database") == (
            "team/search" if i in (2, 3) else None
        )
        assert "x-lancedb-database-prefix" not in headers
        assert headers["x-api-key"] == "secret"


@pytest.mark.asyncio
async def test_catalog_async_and_errors(catalog_server):
    endpoint, requests, responses = catalog_server
    responses.extend(
        [
            (200, {}),
            (200, {"tables": []}),
            (404, {"error": "missing"}),
            (409, {"error": "exists"}),
            (400, {"error": "not empty"}),
            (404, {"error": "missing"}),
        ]
    )
    catalog = await lancedb.connect_catalog_async(
        endpoint, sql_host_override="invalid://localhost"
    )
    assert isinstance(catalog, lancedb.AsyncCatalog)
    db = await catalog.connect_database("analytics")
    assert isinstance(db, AsyncConnection)
    with pytest.raises(ValueError, match="sql_host_override must use"):
        await db.execute_query_async("SELECT 1")
    assert await db.table_names() == []
    with pytest.raises(ValueError, match="missing"):
        await catalog.connect_database("missing")
    with pytest.raises(ValueError, match="exists"):
        await catalog.create_database("exists")
    with pytest.raises(HttpError):
        await catalog.drop_database("full")
    await catalog.drop_database("missing", ignore_missing=True)
    assert requests[4][2] == {"mode": "Fail", "behavior": "Restrict"}


@pytest.mark.parametrize("endpoint", ["/tmp/catalog", "s3://bucket", "db://db"])
def test_catalog_requires_remote_endpoint(endpoint):
    with pytest.raises(ValueError, match="endpoint"):
        lancedb.connect_catalog(endpoint)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.asyncio
@pytest.mark.parametrize("page_token,page_limit", [(None, None), ("start/token", 2)])
async def test_database_names_iteration(
    catalog_server, asynchronous, page_token, page_limit
):
    endpoint, requests, responses = catalog_server
    responses.extend(
        [
            (200, {"namespaces": ["first", "second"], "page_token": "a/b"}),
            (200, {"namespaces": [], "page_token": "next"}),
            (200, {"namespaces": ["last", "final"], "page_token": ""}),
        ]
    )
    catalog = (
        await lancedb.connect_catalog_async(endpoint)
        if asynchronous
        else lancedb.connect_catalog(endpoint)
    )
    names = catalog.list_databases(page_token=page_token, page_limit=page_limit)
    assert requests == []
    assert names.num_page_results() == 0
    assert names.page_token() == page_token
    for cached, expected in [(1, "first"), (0, "second")]:
        assert (await names.__anext__() if asynchronous else next(names)) == expected
        assert names.num_page_results() == cached
        assert names.page_token() == "a/b"
        assert len(requests) == 1
    assert (await names.__anext__() if asynchronous else next(names)) == "last"
    assert names.num_page_results() == 1
    assert names.page_token() is None
    assert (await names.__anext__() if asynchronous else next(names)) == "final"
    assert names.num_page_results() == 0
    assert names.page_token() is None
    for _ in range(2):
        with pytest.raises(StopAsyncIteration if asynchronous else StopIteration):
            if asynchronous:
                await names.__anext__()
            else:
                next(names)
    from urllib.parse import parse_qs, urlsplit

    for request, token in zip(requests, [page_token, "a/b", "next"]):
        url = urlsplit(request[0])
        assert url.path == "/v1/namespace/$/list"
        expected = {}
        if token is not None:
            expected["page_token"] = [token]
        if page_limit is not None:
            expected["limit"] = [str(page_limit)]
        assert parse_qs(url.query) == expected


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("fail", [False, True])
@pytest.mark.asyncio
async def test_database_names_empty_or_error(catalog_server, asynchronous, fail):
    endpoint, requests, responses = catalog_server
    responses.append(
        (401, {"error": "unauthorized"}) if fail else (200, {"namespaces": []})
    )
    catalog = (
        await lancedb.connect_catalog_async(endpoint)
        if asynchronous
        else lancedb.connect_catalog(endpoint)
    )
    names = catalog.list_databases()
    if fail:
        with pytest.raises(HttpError):
            if asynchronous:
                await names.__anext__()
            else:
                next(names)
    assert ([name async for name in names] if asynchronous else list(names)) == []
    assert len(requests) == 1


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.asyncio
async def test_database_names_resume(catalog_server, asynchronous):
    endpoint, requests, responses = catalog_server
    responses.extend(
        [
            (200, {"namespaces": ["first"], "page_token": "resume/token"}),
            (200, {"namespaces": ["second"]}),
        ]
    )
    catalog = (
        await lancedb.connect_catalog_async(endpoint)
        if asynchronous
        else lancedb.connect_catalog(endpoint)
    )
    names = catalog.list_databases(page_limit=1)
    assert (await names.__anext__() if asynchronous else next(names)) == "first"
    assert names.num_page_results() == 0
    resumed = catalog.list_databases(page_token=names.page_token(), page_limit=1)
    assert resumed.page_token() == "resume/token"
    assert len(requests) == 1
    assert ([name async for name in resumed] if asynchronous else list(resumed)) == [
        "second"
    ]
    assert resumed.page_token() is None
    assert resumed.num_page_results() == 0
    assert requests[1][0] == "/v1/namespace/$/list?limit=1&page_token=resume%2Ftoken"


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("page_limit", [0, 2**32 - 1])
@pytest.mark.asyncio
async def test_database_names_invalid_limit(catalog_server, asynchronous, page_limit):
    endpoint, requests, _ = catalog_server
    catalog = (
        await lancedb.connect_catalog_async(endpoint)
        if asynchronous
        else lancedb.connect_catalog(endpoint)
    )
    names = catalog.list_databases(page_limit=page_limit)
    with pytest.raises(ValueError, match="limit"):
        if asynchronous:
            await names.__anext__()
        else:
            next(names)
    assert names.num_page_results() == 0
    assert requests == []
