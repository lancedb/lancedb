# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import contextlib
import json
from pathlib import Path

import pytest
import pyarrow as pa
import pyarrow.flight as flight

import lancedb
from lancedb.authz import Object, Privilege
from lancedb.db import AsyncConnection, DBConnection
from lancedb.catalog import Catalog, AsyncCatalog
from lancedb.storage_integration import SecretReference, StorageIntegrationInfo


INFO = json.loads(
    (
        Path(__file__).parents[3]
        / "rust/lancedb/tests/fixtures/storage_integration.json"
    ).read_text()
)
NAME = "raw"
ROLE = "arn:aws:iam::123456789012:role/access"


@contextlib.contextmanager
def remote_server(text_type=pa.string()):
    calls = []
    info = dict(INFO)

    class Server(flight.FlightServerBase):
        def __init__(self):
            super().__init__("grpc://127.0.0.1:0")

        def do_get(self, context, ticket):
            command = ticket.ticket
            calls.append(command)
            if b"SHOW STORAGE INTEGRATIONS" in command:
                result = pa.table({"name": pa.array(["zeta", NAME], type=text_type)})
            elif b"DROP STORAGE INTEGRATION" in command:
                result = pa.table({"dropped": [True]})
            else:
                if b"STORAGE_AWS_ACCESS_KEY_ID_SECRET" in command:
                    info["aws_role_arn"] = None
                    info["trust_policy"] = None
                    info["aws_access_key_id_secret"] = {
                        "database": "shared",
                        "namespace_path": ["credentials"],
                        "name": "access_key",
                    }
                    info["aws_secret_access_key_secret"] = {
                        "database": "shared",
                        "namespace_path": ["credentials"],
                        "name": "secret_key",
                    }
                row = dict(info)
                for field in [
                    "namespace_path",
                    "trust_policy",
                    "permissions_policy",
                    "aws_access_key_id_secret",
                    "aws_secret_access_key_secret",
                ]:
                    if field in row and row[field] is not None:
                        row[field] = json.dumps(row[field])
                row.setdefault("aws_access_key_id_secret", None)
                row.setdefault("aws_secret_access_key_secret", None)
                result = pa.table(
                    {
                        field: pa.array(
                            [value],
                            type=(
                                pa.bool_()
                                if field == "access_verified"
                                else pa.int64()
                                if field == "created_at_millis"
                                else text_type
                            ),
                        )
                        for field, value in row.items()
                    }
                )
            return flight.RecordBatchStream(result)

    with Server() as server:
        yield f"grpc://127.0.0.1:{server.port}", calls


def connect(host):
    return lancedb.connect(
        "db://default",
        api_key="fake",
        sql_host_override=host,
        host_override="http://127.0.0.1:1",
    )


def test_storage_integration_methods_belong_to_databases():
    assert hasattr(DBConnection, "create_storage_integration")
    assert hasattr(AsyncConnection, "create_storage_integration")
    assert not hasattr(Catalog, "create_storage_integration")
    assert not hasattr(AsyncCatalog, "create_storage_integration")


@pytest.mark.parametrize(
    "text_type", [pa.string(), pa.large_string(), pa.string_view()]
)
def test_sync_storage_integration_lifecycle_and_aws_information(text_type):
    with remote_server(text_type) as (host, calls):
        db = connect(host)
        info = db.create_storage_integration(
            NAME, INFO["storage_prefix"], aws_role_arn=ROLE
        )
        assert isinstance(info, lancedb.StorageIntegrationInfo)
        assert info.aws_account_id == "001122334455"
        assert not info.access_verified
        assert (
            info.trust_policy["Statement"][0]["Condition"]["StringEquals"][
                "sts:ExternalId"
            ]
            == info.aws_external_id
        )
        assert db.describe_storage_integration(NAME) == info
        listing = db.list_storage_integrations(page_limit=1)
        assert len(calls) == 2
        assert list(listing) == ["raw", "zeta"]
        assert db.drop_storage_integration(NAME) is None
    assert b'CREATE STORAGE INTEGRATION "raw"' in calls[0]
    assert b"STORAGE_AWS_ROLE_ARN = '" + ROLE.encode() + b"'" in calls[0]
    assert b'DROP STORAGE INTEGRATION "raw"' in calls[-1]


@pytest.mark.asyncio
async def test_async_storage_integration_lifecycle():
    with remote_server() as (host, _calls):
        db = await lancedb.connect_async(
            "db://default",
            api_key="fake",
            sql_host_override=host,
            host_override="http://127.0.0.1:1",
        )
        info = await db.create_storage_integration(
            NAME, INFO["storage_prefix"], aws_role_arn=ROLE
        )
        assert await db.describe_storage_integration(NAME) == info
        assert [name async for name in db.list_storage_integrations()] == [
            "raw",
            "zeta",
        ]
        await db.drop_storage_integration(NAME)
        with pytest.raises(ValueError):
            await db.describe_storage_integration("a//b")


def test_integration_authorization_object_and_creation_privilege():
    value = Object.storage_integration(database="default", name=NAME)
    assert str(value) == "storage_integration:default:public$raw"
    assert Privilege.CREATE_STORAGE_INTEGRATION.value == "CREATE_STORAGE_INTEGRATION"


@pytest.mark.parametrize(
    "text_type", [pa.string(), pa.large_string(), pa.string_view()]
)
def test_static_credentials_use_explicit_cross_database_secret_references(text_type):
    with remote_server(text_type) as (host, calls):
        db = connect(host)
        info = db.create_storage_integration(
            "raw",
            INFO["storage_prefix"],
            aws_access_key_id_secret=SecretReference(
                "access_key", database="shared", namespace_path=("credentials",)
            ),
            aws_secret_access_key_secret=SecretReference(
                "secret_key", database="shared", namespace_path=("credentials",)
            ),
        )
        assert info.aws_role_arn is None
        assert db.describe_storage_integration("raw") == info
        assert info.aws_access_key_id_secret.database == "shared"
        assert b"STORAGE_AWS_ROLE_ARN" not in calls[0]
        assert (
            b'STORAGE_AWS_ACCESS_KEY_ID_SECRET = "shared"."credentials"."access_key"'
            in calls[0]
        )
        with pytest.raises(ValueError):
            db.create_storage_integration(
                "bad",
                INFO["storage_prefix"],
                aws_role_arn=ROLE,
                aws_access_key_id_secret="key",
            )
        assert len(calls) == 2


def test_metadata_defaults_an_omitted_role_to_none():
    data = dict(INFO)
    data.pop("aws_role_arn")
    assert StorageIntegrationInfo(**data).aws_role_arn is None


def test_namespace_connections_keep_existing_apis_available(tmp_path):
    db = lancedb.connect_namespace("dir", {"root": str(tmp_path)})
    with pytest.raises(NotImplementedError):
        db.create_storage_integration("raw", "s3://customer/raw/", aws_role_arn=ROLE)
