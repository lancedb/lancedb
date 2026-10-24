# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import contextlib
import http.server
import json
from pathlib import Path
import threading
import urllib.parse

import pytest

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
def remote_server():
    calls = []
    state = {"info": dict(INFO)}

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def respond(self):
            url = urllib.parse.urlsplit(self.path)
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if url.path == "/v1/namespace/system/describe":
                assert self.headers.get("x-lancedb-database") is None
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b"{}")
                return
            assert self.headers.get("x-lancedb-database") == "system"
            request = json.loads(body) if body else {}
            calls.append((self.command, url.path, request))
            if url.path.endswith("/list"):
                query = urllib.parse.parse_qs(url.query)
                result = (
                    {"storage_integrations": [{"name": "zeta"}]}
                    if query.get("page_token") == ["next"]
                    else {
                        "storage_integrations": [{"name": NAME}],
                        "page_token": "next",
                    }
                )
            elif url.path.endswith("/drop"):
                result = {"dropped": True}
            else:
                result = dict(state["info"])
                if request.get("aws_access_key_id_secret"):
                    result.pop("aws_role_arn", None)
                    result["trust_policy"] = None
                    result["aws_access_key_id_secret"] = request[
                        "aws_access_key_id_secret"
                    ]
                    result["aws_secret_access_key_secret"] = request[
                        "aws_secret_access_key_secret"
                    ]
            if url.path.endswith("/create"):
                state["info"] = dict(result)
            payload = json.dumps(result).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        do_GET = respond
        do_POST = respond

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_storage_integration_methods_belong_to_databases():
    assert hasattr(DBConnection, "create_storage_integration")
    assert hasattr(AsyncConnection, "create_storage_integration")
    assert not hasattr(Catalog, "create_storage_integration")
    assert not hasattr(AsyncCatalog, "create_storage_integration")


def test_sync_storage_integration_lifecycle_and_aws_information():
    with remote_server() as (host, calls):
        db = lancedb.connect_catalog(
            host,
            api_key="fake",
            client_config={"retry_config": {"retries": 0}},
        )
        db = db.connect_database("system")
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
    assert calls[0][1] == "/v1/storage_integration/raw/create"
    assert calls[0][2] == {
        "storage_provider": "S3",
        "storage_prefix": INFO["storage_prefix"],
        "aws_role_arn": ROLE,
    }
    assert calls[-1][1] == "/v1/storage_integration/raw/drop"


@pytest.mark.asyncio
async def test_async_storage_integration_lifecycle():
    with remote_server() as (host, _calls):
        db = await lancedb.connect_catalog_async(
            host,
            api_key="fake",
            client_config={"retry_config": {"retries": 0}},
        )
        db = await db.connect_database("system")
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
    value = Object.storage_integration(database="system", name=NAME)
    assert str(value) == "storage_integration:system:public$raw"
    assert Privilege.CREATE_STORAGE_INTEGRATION.value == "CREATE_STORAGE_INTEGRATION"


def test_static_credentials_use_explicit_cross_database_secret_references():
    with remote_server() as (host, calls):
        db = lancedb.connect_catalog(host, api_key="fake").connect_database("system")
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
        assert "aws_role_arn" not in calls[0][2]
        assert calls[0][2]["aws_access_key_id_secret"] == {
            "database": "shared",
            "namespace_path": ["credentials"],
            "name": "access_key",
        }
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
