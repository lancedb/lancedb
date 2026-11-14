# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
from pathlib import Path

import pyarrow as pa
import pytest

import lancedb


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    ("exist_ok", "existing"),
    [(None, False), (False, False), (True, False), (True, True)],
)
async def test_storage_integration_exist_ok_preserves_server_metadata(
    asynchronous, exist_ok, existing
):
    flight = pytest.importorskip("pyarrow.flight")
    fixture = (
        Path(__file__).parents[3]
        / "rust/lancedb/tests/fixtures/storage_integration.json"
    )
    stored = json.loads(fixture.read_text())
    stored["namespace_path"] = ["team"]
    columns = {}
    for key, value in stored.items():
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        columns[key] = pa.array([None], type=pa.string()) if value is None else [value]
    result = pa.table(columns)
    prefix = "s3://replacement-bucket/raw/" if existing else stored["storage_prefix"]
    role = (
        "arn:aws:iam::999999999999:role/replacement"
        if existing
        else stored["aws_role_arn"]
    )

    class IntegrationServer(flight.FlightServerBase):
        def __init__(self):
            super().__init__("grpc://127.0.0.1:0")
            self.requests = []

        def do_get(self, context, ticket):
            self.requests.append(ticket.ticket)
            return flight.RecordBatchStream(result)

    with IntegrationServer() as server:
        connection_options = {
            "api_key": "test-key",
            "host_override": "http://localhost:10024",
            "sql_host_override": f"grpc://127.0.0.1:{server.port}",
        }
        kwargs = {} if exist_ok is None else {"exist_ok": exist_ok}
        kwargs.update(
            storage_provider="S3",
            storage_prefix=prefix,
            aws_role_arn=role,
            namespace_path=["team"],
        )
        uri = f"db://{stored['database']}"
        if asynchronous:
            db = await lancedb.connect_async(uri, **connection_options)
            info = await db.create_storage_integration(stored["name"], **kwargs)
        else:
            db = lancedb.connect(uri, **connection_options)
            info = db.create_storage_integration(stored["name"], **kwargs)
        (request,) = server.requests

    command = (
        "CREATE STORAGE INTEGRATION IF NOT EXISTS"
        if exist_ok
        else "CREATE STORAGE INTEGRATION"
    )
    assert f'{command} "raw"'.encode() in request
    assert prefix.encode() in request
    assert role.encode() in request
    assert info.storage_prefix == stored["storage_prefix"]
    assert info.aws_role_arn == stored["aws_role_arn"]
    assert info.aws_external_id == stored["aws_external_id"]
    assert info.created_at_millis == stored["created_at_millis"]
    assert info.namespace_path == ["team"]
