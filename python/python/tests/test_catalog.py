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
            (200, {"tables": []}),
            (200, {"tables": []}),
            (200, {"namespaces": ["team/search"], "page_token": "next"}),
            (204, None),
        ]
    )
    catalog = lancedb.connect_catalog(
        endpoint,
        api_key="secret",
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
    assert db.table_names() == []
    restored = lancedb.deserialize_conn(db.serialize())
    assert restored.table_names() == []
    page = catalog.list_databases(limit=1, page_token="a/b")
    assert page == lancedb.ListDatabasesResponse(["team/search"], "next")
    catalog.drop_database("team/search", ignore_missing=True)
    assert requests[0][0] == "/v1/namespace/team%2Fsearch/create"
    assert requests[0][2] == {"mode": "ExistOk"}
    assert requests[3][0] == "/v1/namespace/%24/list?limit=1&page_token=a%2Fb"
    assert requests[4][2] == {"mode": "Skip", "behavior": "Restrict"}
    for i, (_, headers, _) in enumerate(requests):
        headers = {key.lower(): value for key, value in headers.items()}
        assert headers.get("x-lancedb-database") == (
            "team/search" if i in (1, 2) else None
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
    catalog = await lancedb.connect_catalog_async(endpoint)
    assert isinstance(catalog, lancedb.AsyncCatalog)
    db = await catalog.open_database("analytics")
    assert isinstance(db, AsyncConnection)
    assert await db.table_names() == []
    with pytest.raises(ValueError, match="missing"):
        await catalog.open_database("missing")
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
