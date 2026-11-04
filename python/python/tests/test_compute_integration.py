# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import contextlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.flight as flight
import pytest

import lancedb
from lancedb.authz import Object, Privilege
from lancedb.compute_integration import ComputeIntegrationInfo
from lancedb.storage_integration import SecretReference


INFO = json.loads(
    (
        Path(__file__).parents[3]
        / "rust/lancedb/tests/fixtures/compute_integration.json"
    ).read_text()
)


@contextlib.contextmanager
def remote_server(text_type=pa.string()):
    calls = []

    class Server(flight.FlightServerBase):
        def __init__(self):
            super().__init__("grpc://127.0.0.1:0")

        def do_get(self, context, ticket):
            calls.append(ticket.ticket)
            if b"SHOW COMPUTE INTEGRATIONS" in ticket.ticket:
                result = pa.table({"name": pa.array(["zeta", "batch"], type=text_type)})
            elif b"DROP COMPUTE INTEGRATION" in ticket.ticket:
                result = pa.table({"dropped": [True]})
            else:
                row = dict(INFO)
                for field in (
                    "namespace_path",
                    "kubernetes_bearer_token_secret",
                    "kubernetes_ca_cert_secret",
                ):
                    if row[field] is not None:
                        row[field] = json.dumps(row[field])
                result = pa.table(
                    {
                        field: pa.array(
                            [value],
                            type=pa.bool_()
                            if field == "access_verified"
                            else pa.int64()
                            if field == "created_at_millis"
                            else text_type,
                        )
                        for field, value in row.items()
                    }
                )
            return flight.RecordBatchStream(result)

    with Server() as server:
        yield f"grpc://127.0.0.1:{server.port}", calls


@pytest.mark.parametrize(
    "text_type", [pa.string(), pa.large_string(), pa.string_view()]
)
def test_sync_compute_lifecycle_uses_sql_and_secret_references(text_type):
    with remote_server(text_type) as (host, calls):
        db = lancedb.connect(
            "db://default",
            api_key="fake",
            sql_host_override=host,
            host_override="http://127.0.0.1:1",
        )
        info = db.create_compute_integration(
            "batch",
            "https://api.example.com:6443",
            "jobs",
            kubernetes_bearer_token_secret=SecretReference(
                "token", database="shared", namespace_path=("credentials",)
            ),
            kubernetes_service_account="worker",
        )
        assert isinstance(info, ComputeIntegrationInfo)
        assert isinstance(info.kubernetes_bearer_token_secret, SecretReference)
        assert not info.access_verified
        assert db.describe_compute_integration("batch") == info
        listing = db.list_compute_integrations(page_limit=1)
        assert len(calls) == 2
        assert list(listing) == ["batch", "zeta"]
        db.drop_compute_integration("batch")
        assert b'CREATE COMPUTE INTEGRATION "batch"' in calls[0]
        assert (
            b'KUBERNETES_BEARER_TOKEN_SECRET = "shared"."credentials"."token"'
            in calls[0]
        )
        assert b"KUBERNETES_SERVICE_ACCOUNT = 'worker'" in calls[0]
        assert b'DROP COMPUTE INTEGRATION "batch"' in calls[-1]


@pytest.mark.asyncio
async def test_async_compute_lifecycle_and_invalid_reference():
    with remote_server() as (host, calls):
        db = await lancedb.connect_async(
            "db://default",
            api_key="fake",
            sql_host_override=host,
            host_override="http://127.0.0.1:1",
        )
        info = await db.create_compute_integration(
            "batch",
            "https://api.example.com:6443",
            "jobs",
            kubernetes_bearer_token_secret="token",
        )
        assert await db.describe_compute_integration("batch") == info
        assert [name async for name in db.list_compute_integrations()] == [
            "batch",
            "zeta",
        ]
        await db.drop_compute_integration("batch")
        count = len(calls)
        with pytest.raises(TypeError):
            await db.create_compute_integration(
                "bad",
                "https://api.example.com",
                "jobs",
                kubernetes_bearer_token_secret={"value": "plaintext"},
            )
        with pytest.raises(ValueError):
            await db.describe_compute_integration("a/b")
        assert len(calls) == count


def test_compute_authorization_object_and_privilege():
    assert (
        str(Object.compute_integration(database="default", name="batch"))
        == "compute_integration:default:public$batch"
    )
    assert Privilege.CREATE_COMPUTE_INTEGRATION.value == "CREATE_COMPUTE_INTEGRATION"
