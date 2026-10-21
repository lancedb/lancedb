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


INFO = json.loads(
    (
        Path(__file__).parents[3]
        / "rust/lancedb/tests/fixtures/storage_integration.json"
    ).read_text()
)
NAMESPACE = ["analytics", "ingest"]
ROLE = "arn:aws:iam::123456789012:role/access"


@contextlib.contextmanager
def remote_server():
    calls = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def respond(self):
            url = urllib.parse.urlsplit(self.path)
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            calls.append((self.command, url.path, json.loads(body) if body else {}))
            if url.path.endswith("/list"):
                query = urllib.parse.parse_qs(url.query)
                result = (
                    {"storage_integrations": [{"name": "zeta"}]}
                    if query.get("page_token") == ["next"]
                    else {
                        "storage_integrations": [{"name": "raw"}],
                        "page_token": "next",
                    }
                )
            elif url.path.endswith("/drop"):
                result = {"dropped": True}
            else:
                result = INFO
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


def test_sync_storage_integration_lifecycle_and_aws_information():
    with remote_server() as (host, calls):
        db = lancedb.connect(
            "db://dev",
            api_key="fake",
            host_override=host,
            client_config={"retry_config": {"retries": 0}},
        )
        info = db.create_storage_integration(
            "raw", INFO["storage_prefix"], aws_role_arn=ROLE, namespace_path=NAMESPACE
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
        assert db.describe_storage_integration("raw", namespace_path=NAMESPACE) == info
        listing = db.list_storage_integrations(namespace_path=NAMESPACE, page_limit=1)
        assert len(calls) == 2
        assert list(listing) == ["raw", "zeta"]
        assert db.drop_storage_integration("raw", namespace_path=NAMESPACE) is None
    assert calls[0][1] == "/v1/storage_integration/analytics$ingest$raw/create"
    assert calls[0][2] == {
        "storage_provider": "S3",
        "storage_prefix": INFO["storage_prefix"],
        "aws_role_arn": ROLE,
    }
    assert calls[-1][1] == "/v1/storage_integration/analytics$ingest$raw/drop"


@pytest.mark.asyncio
async def test_async_storage_integration_lifecycle():
    with remote_server() as (host, _calls):
        db = await lancedb.connect_async(
            "db://dev",
            api_key="fake",
            host_override=host,
            client_config={"retry_config": {"retries": 0}},
        )
        info = await db.create_storage_integration(
            "raw", INFO["storage_prefix"], aws_role_arn=ROLE, namespace_path=NAMESPACE
        )
        assert (
            await db.describe_storage_integration("raw", namespace_path=NAMESPACE)
            == info
        )
        assert [
            name
            async for name in db.list_storage_integrations(namespace_path=NAMESPACE)
        ] == ["raw", "zeta"]
        await db.drop_storage_integration("raw", namespace_path=NAMESPACE)
        with pytest.raises(ValueError):
            await db.describe_storage_integration("a/b")


def test_integration_authorization_object_and_creation_privilege():
    value = Object.storage_integration(
        database="db", name="raw", namespace_path=NAMESPACE
    )
    assert str(value) == "storage_integration:db:analytics$ingest$raw"
    assert Privilege.CREATE_STORAGE_INTEGRATION.value == "CREATE_STORAGE_INTEGRATION"
