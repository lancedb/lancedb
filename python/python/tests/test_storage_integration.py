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
                if b"HUGGINGFACE_API_KEY_SECRET" in command:
                    info.update(
                        storage_provider="HUGGINGFACE",
                        storage_prefix="hf://datasets/acme/private/raw/",
                        aws_role_arn=None,
                        aws_region=None,
                        aws_account_id="",
                        aws_iam_principal_arn="",
                        trust_policy=None,
                        permissions_policy=None,
                        configuration_instructions=(
                            "Grant the referenced identity access to the prefix."
                        ),
                        huggingface_api_key_secret=dict(
                            database="shared",
                            namespace_path=["credentials"],
                            name="hf_key",
                        ),
                    )
                if b"GCP_CREDENTIALS_JSON_SECRET" in command:
                    info.update(
                        storage_provider="GCP",
                        storage_prefix="gs://customer-data/raw/",
                        aws_role_arn=None,
                        aws_region=None,
                        aws_account_id="",
                        aws_iam_principal_arn="",
                        trust_policy=None,
                        permissions_policy=None,
                        configuration_instructions=(
                            "Grant the referenced identity access to the prefix."
                        ),
                        gcp_credentials_json_secret=dict(
                            database="shared",
                            namespace_path=["credentials"],
                            name="gcp",
                        ),
                    )
                if b"STORAGE_AZURE_STORAGE_ACCOUNT" in command:
                    info.update(
                        storage_provider="AZURE",
                        storage_prefix="az://customer-data@customeraccount.blob.core.windows.net/raw/",
                        azure_storage_account="customeraccount",
                        aws_role_arn=None,
                        trust_policy=None,
                        permissions_policy=None,
                    )
                    for field, name in (
                        ("azure_tenant_id_secret", "tenant"),
                        ("azure_client_id_secret", "client"),
                        ("azure_client_secret_secret", "credential"),
                    ):
                        info[field] = dict(
                            database="shared", namespace_path=["credentials"], name=name
                        )
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
                    "azure_tenant_id_secret",
                    "azure_client_id_secret",
                    "azure_client_secret_secret",
                    "gcp_credentials_json_secret",
                    "huggingface_api_key_secret",
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


@pytest.mark.parametrize(
    "text_type", [pa.string(), pa.large_string(), pa.string_view()]
)
def test_azure_lifecycle_uses_three_qualified_secret_references(text_type):
    with remote_server(text_type) as (host, calls):
        db = connect(host)
        refs = [
            SecretReference(name, database="shared", namespace_path=("credentials",))
            for name in ("tenant", "client", "credential")
        ]
        info = db.create_storage_integration(
            "blobs",
            "az://customer-data/raw/",
            storage_provider="AZURE",
            azure_storage_account="customeraccount",
            azure_tenant_id_secret=refs[0],
            azure_client_id_secret=refs[1],
            azure_client_secret_secret=refs[2],
        )
        assert info.storage_provider == "AZURE"
        assert info.azure_storage_account == "customeraccount"
        assert info.azure_tenant_id_secret.name == "tenant"
        assert info.azure_client_id_secret.database == "shared"
        assert info.azure_client_secret_secret.name == "credential"
        assert info.trust_policy is None
        assert info.permissions_policy is None
        assert not info.access_verified
        assert db.describe_storage_integration("blobs") == info
        assert list(db.list_storage_integrations()) == ["raw", "zeta"]
        db.drop_storage_integration("blobs")
        sql = calls[0]
        assert b"STORAGE_PROVIDER = 'AZURE'" in sql
        assert b"STORAGE_AZURE_STORAGE_ACCOUNT = 'customeraccount'" in sql
        assert (
            b'STORAGE_AZURE_CLIENT_SECRET_SECRET = "shared"."credentials"."credential"'
            in sql
        )
        assert b"STORAGE_AWS" not in sql


@pytest.mark.asyncio
async def test_async_azure_requires_account_and_all_three_secret_references():
    with remote_server() as (host, calls):
        db = await lancedb.connect_async(
            "db://default",
            api_key="fake",
            sql_host_override=host,
            host_override="http://127.0.0.1:1",
        )
        config = dict(
            storage_provider="AZURE",
            azure_storage_account="customeraccount",
            azure_tenant_id_secret="tenant",
            azure_client_id_secret="client",
            azure_client_secret_secret="credential",
        )
        for missing in (
            "azure_storage_account",
            "azure_tenant_id_secret",
            "azure_client_id_secret",
            "azure_client_secret_secret",
        ):
            partial = dict(config)
            partial.pop(missing)
            with pytest.raises(ValueError):
                await db.create_storage_integration(
                    "blobs", "az://customer-data/raw/", **partial
                )
        with pytest.raises(ValueError):
            await db.create_storage_integration(
                "blobs", "az://customer-data/raw/", aws_role_arn=ROLE, **config
            )
        assert calls == []
        info = await db.create_storage_integration(
            "blobs", "az://customer-data/raw/", **config
        )
        assert info.storage_provider == "AZURE"
        assert b'STORAGE_AZURE_TENANT_ID_SECRET = "public"."tenant"' in calls[0]


@pytest.mark.parametrize(
    "text_type", [pa.string(), pa.large_string(), pa.string_view()]
)
def test_gcp_lifecycle_uses_one_qualified_json_secret(text_type):
    with remote_server(text_type) as (host, calls):
        db = connect(host)
        info = db.create_storage_integration(
            "gcs",
            "gs://customer-data/raw/",
            storage_provider="GCP",
            gcp_credentials_json_secret=SecretReference(
                "gcp", database="shared", namespace_path=("credentials",)
            ),
        )
        assert info.storage_provider == "GCP"
        assert info.gcp_credentials_json_secret.name == "gcp"
        assert info.gcp_credentials_json_secret.database == "shared"
        assert info.aws_role_arn is None
        assert info.azure_storage_account is None
        assert info.aws_iam_principal_arn == ""
        assert info.trust_policy is None
        assert not info.access_verified
        assert db.describe_storage_integration("gcs") == info
        assert list(db.list_storage_integrations()) == ["raw", "zeta"]
        db.drop_storage_integration("gcs")
        assert b"STORAGE_PROVIDER = 'GCP'" in calls[0]
        assert b'GCP_CREDENTIALS_JSON_SECRET = "shared"."credentials"."gcp"' in calls[0]
        assert b"STORAGE_AWS" not in calls[0]
        assert b"STORAGE_AZURE" not in calls[0]


@pytest.mark.asyncio
async def test_async_gcp_requires_one_secret_and_rejects_other_cloud_options():
    with remote_server() as (host, calls):
        db = await lancedb.connect_async(
            "db://default",
            api_key="fake",
            sql_host_override=host,
            host_override="http://127.0.0.1:1",
        )
        for invalid in (
            {},
            {"gcp_credentials_json_secret": "gcp", "aws_role_arn": ROLE},
            {"gcp_credentials_json_secret": "gcp", "azure_storage_account": "account"},
        ):
            with pytest.raises(ValueError):
                await db.create_storage_integration(
                    "gcs", "gs://customer-data/raw/", storage_provider="GCP", **invalid
                )
        with pytest.raises(ValueError):
            await db.create_storage_integration(
                "gcs",
                "gs://customer-data/raw/",
                aws_role_arn=ROLE,
                gcp_credentials_json_secret="gcp",
            )
        assert calls == []
        info = await db.create_storage_integration(
            "gcs",
            "gs://customer-data/raw/",
            storage_provider="GCP",
            gcp_credentials_json_secret="gcp",
        )
        assert info.storage_provider == "GCP"
        assert b'GCP_CREDENTIALS_JSON_SECRET = "public"."gcp"' in calls[0]


@pytest.mark.parametrize(
    "text_type", [pa.string(), pa.large_string(), pa.string_view()]
)
def test_huggingface_lifecycle_uses_one_qualified_api_key_secret(text_type):
    with remote_server(text_type) as (host, calls):
        db = connect(host)
        info = db.create_storage_integration(
            "hub",
            "hf://datasets/acme/private/raw/",
            storage_provider="HUGGINGFACE",
            huggingface_api_key_secret=SecretReference(
                "hf_key", database="shared", namespace_path=("credentials",)
            ),
        )
        assert info.storage_provider == "HUGGINGFACE"
        assert info.huggingface_api_key_secret.name == "hf_key"
        assert info.huggingface_api_key_secret.database == "shared"
        assert info.aws_role_arn is None
        assert info.azure_storage_account is None
        assert info.aws_iam_principal_arn == ""
        assert info.trust_policy is None
        assert not info.access_verified
        assert db.describe_storage_integration("hub") == info
        assert list(db.list_storage_integrations()) == ["raw", "zeta"]
        db.drop_storage_integration("hub")
        assert b"STORAGE_PROVIDER = 'HUGGINGFACE'" in calls[0]
        assert (
            b'HUGGINGFACE_API_KEY_SECRET = "shared"."credentials"."hf_key"' in calls[0]
        )
        assert b"STORAGE_AWS" not in calls[0]
        assert b"STORAGE_AZURE" not in calls[0]


@pytest.mark.asyncio
async def test_async_huggingface_requires_one_secret_and_rejects_other_cloud_options():
    with remote_server() as (host, calls):
        db = await lancedb.connect_async(
            "db://default",
            api_key="fake",
            sql_host_override=host,
            host_override="http://127.0.0.1:1",
        )
        for invalid in (
            {},
            {"huggingface_api_key_secret": "hf_key", "aws_role_arn": ROLE},
            {
                "huggingface_api_key_secret": "hf_key",
                "azure_storage_account": "account",
            },
        ):
            with pytest.raises(ValueError):
                await db.create_storage_integration(
                    "hub",
                    "hf://datasets/acme/private/raw/",
                    storage_provider="HUGGINGFACE",
                    **invalid,
                )
        with pytest.raises(ValueError):
            await db.create_storage_integration(
                "hub",
                "hf://datasets/acme/private/raw/",
                aws_role_arn=ROLE,
                huggingface_api_key_secret="hf_key",
            )
        assert calls == []
        info = await db.create_storage_integration(
            "hub",
            "hf://datasets/acme/private/raw/",
            storage_provider="HUGGINGFACE",
            huggingface_api_key_secret="hf_key",
        )
        assert info.storage_provider == "HUGGINGFACE"
        assert b'HUGGINGFACE_API_KEY_SECRET = "public"."hf_key"' in calls[0]
