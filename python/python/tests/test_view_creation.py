# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json

import pyarrow as pa
import pytest

import lancedb


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    ("exist_ok", "existing"),
    [(None, False), (False, False), (True, False), (True, True)],
)
async def test_create_view_exist_ok_preserves_server_metadata(
    asynchronous, exist_ok, existing
):
    flight = pytest.importorskip("pyarrow.flight")
    original_query = "SELECT 'original' AS original"
    submitted_query = (
        "SELECT 'replacement' AS replacement" if existing else original_query
    )
    schema = {
        "fields": [{"name": "original", "nullable": True, "type": {"type": "utf8"}}],
        "metadata": {
            "lancedb::view::default_db": "analytics",
            "lancedb::view::default_namespace": "team",
        },
    }
    result = pa.table(
        {"name": ["saved"], "query": [original_query], "schema": [json.dumps(schema)]}
    )

    class ViewServer(flight.FlightServerBase):
        def __init__(self):
            super().__init__("grpc://127.0.0.1:0")
            self.requests = []

        def do_get(self, context, ticket):
            self.requests.append(ticket.ticket)
            return flight.RecordBatchStream(result)

    with ViewServer() as server:
        connection_options = {
            "api_key": "test-key",
            "host_override": "http://localhost:10024",
            "sql_host_override": f"grpc://127.0.0.1:{server.port}",
        }
        kwargs = {} if exist_ok is None else {"exist_ok": exist_ok}
        if asynchronous:
            db = await lancedb.connect_async("db://analytics", **connection_options)
            view = await db.create_view(
                "saved", submitted_query, namespace_path=["team"], **kwargs
            )
        else:
            db = lancedb.connect("db://analytics", **connection_options)
            view = db.create_view(
                "saved", submitted_query, namespace_path=["team"], **kwargs
            )
        (request,) = server.requests

    prefix = "CREATE VIEW IF NOT EXISTS" if exist_ok else "CREATE VIEW"
    assert f'{prefix} "saved" AS {submitted_query}'.encode() in request
    assert view.name == "saved"
    assert view.namespace_path == ["team"]
    assert view.default_namespace_path == ["team"]
    assert view.query == original_query
    assert view.schema.names == ["original"]
