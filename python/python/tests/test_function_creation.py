# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
from pathlib import Path

import pyarrow as pa
import pytest

import lancedb
from lancedb.functions import function


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["sync", "sync_job", "async_job"])
@pytest.mark.parametrize("exist_ok", [None, False, True])
async def test_function_exist_ok_forwards_sql_and_keeps_stored_version(entry, exist_ok):
    flight = pytest.importorskip("pyarrow.flight")
    fixture = (
        Path(__file__).parents[3]
        / "rust/lancedb/tests/fixtures/first_class_functions/v1"
        / "remote_function_version.canonical.json"
    )
    stored = json.loads(fixture.read_text())
    result = pa.table({"definition": [json.dumps(stored)]})

    @function(name=stored["name"])
    def replacement(value: float) -> float:
        return value * 2

    class FunctionServer(flight.FlightServerBase):
        def __init__(self):
            super().__init__("grpc://127.0.0.1:0")
            self.requests = []

        def do_get(self, context, ticket):
            self.requests.append(ticket.ticket)
            return flight.RecordBatchStream(result)

    with FunctionServer() as server:
        options = {
            "api_key": "test-key",
            "host_override": "http://localhost:10024",
            "sql_host_override": f"grpc://127.0.0.1:{server.port}",
        }
        kwargs = {} if exist_ok is None else {"exist_ok": exist_ok}
        kwargs["namespace_path"] = ["team"]
        if entry == "async_job":
            db = await lancedb.connect_async("db://analytics", **options)
            job = await db.create_function_async(replacement, **kwargs)
            version = await job.wait()
        else:
            db = lancedb.connect("db://analytics", **options)
            if entry == "sync_job":
                version = db.create_function_async(replacement, **kwargs).wait()
            else:
                version = db.create_function(replacement, **kwargs)
        (request,) = server.requests

    prefix = "CREATE FUNCTION IF NOT EXISTS" if exist_ok else "CREATE FUNCTION"
    assert f'{prefix} "{stored["name"]}" FROM '.encode() in request
    assert b'"exist_ok"' not in request
    assert json.loads(version.to_canonical_json()) == stored
