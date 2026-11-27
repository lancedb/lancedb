# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from datetime import timedelta
import json
from urllib.parse import parse_qs, urlparse

import lancedb
from lancedb.table import Table
import pytest

from test_remote_db import mock_lancedb_connection


@pytest.fixture
def legacy_table():
    """Implement the pre-existing abstract API without the two new members."""

    def unsupported(self, *args, **kwargs):
        raise NotImplementedError

    def update_field_metadata(self, updates):
        self.metadata_updates.append(updates)
        return object()  # The deprecated wrapper must not expose this result.

    # Supply concrete implementations through normal ABC class creation. Leave
    # namespace and replace_field_metadata inherited, as an old subclass would.
    implementations = {
        name: unsupported
        for name in Table.__abstractmethods__
        if name not in {"namespace", "replace_field_metadata"}
    }
    implementations.update(
        name=property(lambda self: "legacy"),
        update_field_metadata=update_field_metadata,
    )
    legacy_class = type("LegacyTable", (Table,), implementations)
    table = legacy_class()
    table.metadata_updates = []
    return table


def test_legacy_table_namespace_and_id(legacy_table):
    assert legacy_table.namespace == []
    assert legacy_table.id == "legacy"
    namespace = legacy_table.namespace
    namespace.append("changed")
    assert legacy_table.namespace == []
    assert legacy_table.id == "legacy"


def test_legacy_table_replace_field_metadata(legacy_table):
    with pytest.warns(DeprecationWarning, match="update_field_metadata"):
        result = legacy_table.replace_field_metadata("nested.field", {"a": "b"})
    assert result is None
    assert legacy_table.metadata_updates == [
        {"path": "nested.field", "metadata": {"a": "b"}, "replace": True}
    ]


def respond(request, body, status=200):
    request.send_response(status)
    request.send_header("Content-Type", "application/json")
    request.end_headers()
    request.wfile.write(json.dumps(body).encode())


def test_remote_uri():
    with mock_lancedb_connection(lambda request: respond(request, {})) as db:
        assert db.uri == "db://dev"


@pytest.mark.parametrize("attribute", ["session", "read_consistency_interval"])
def test_remote_unsupported_properties(attribute):
    with mock_lancedb_connection(lambda request: respond(request, {})) as db:
        with pytest.raises(NotImplementedError, match=attribute):
            getattr(db, attribute)


@pytest.mark.parametrize("tables", [[], ["first", "last"]])
def test_remote_len_paginates_and_contains_looks_up_table(tables):
    requests = []

    def handler(request):
        requests.append(request.path)
        url = urlparse(request.path)
        if request.command == "POST":
            name = url.path.split("/")[3]
            assert url.path == f"/v1/table/{name}/describe/"
            respond(request, {}, status=200 if name in tables else 404)
            return
        assert request.command == "GET"
        assert url.path == "/v1/namespace/$/table/list"
        query = parse_qs(url.query)
        if tables and "page_token" not in query:
            respond(request, {"tables": [tables[0]], "page_token": "opaque-next"})
        else:
            if tables:
                assert query["page_token"] == ["opaque-next"]
            respond(request, {"tables": tables[1:]})

    with mock_lancedb_connection(handler) as db:
        assert len(db) == len(tables)
        assert len(requests) == (2 if tables else 1)
        requests.clear()
        for name in ("last", "missing", "first"):
            assert (name in db) is (name in tables)
            assert requests == [f"/v1/table/{name}/describe/"]
            requests.clear()


@pytest.mark.parametrize("namespace", [[], ["parent", "child"]])
def test_remote_table_identifiers(namespace):
    def handler(request):
        respond(request, {"version": 1, "schema": {"fields": []}})

    with mock_lancedb_connection(handler) as db:
        table = db.open_table("rfm", namespace_path=namespace)
        assert table.namespace == namespace
        assert table.id == "$".join(namespace + ["rfm"])


def test_remote_replace_field_metadata():
    updates = []

    def handler(request):
        if request.path.endswith("/describe/"):
            respond(request, {"version": 1, "schema": {"fields": []}})
        else:
            assert request.path == "/v1/table/rfm/update_field_metadata/"
            body = request.rfile.read(int(request.headers["Content-Length"]))
            updates.append(json.loads(body))
            respond(request, {"version": 2})

    with mock_lancedb_connection(handler) as db:
        table = db.open_table("rfm")
        with pytest.warns(DeprecationWarning, match="update_field_metadata"):
            assert table.replace_field_metadata("id", {"a": "b"}) is None
    assert updates == [
        {"updates": [{"path": "id", "metadata": {"a": "b"}, "replace": True}]}
    ]


@pytest.mark.parametrize("custom_session", [False, True])
def test_local_properties_and_metadata(tmp_path, custom_session):
    supplied = lancedb.Session() if custom_session else None
    db = lancedb.connect(
        tmp_path, session=supplied, read_consistency_interval=timedelta(seconds=5)
    )
    session = db.session
    assert session is supplied
    before = session.size_bytes if session is not None else 0
    table = db.create_table("rfm", [{"id": i} for i in range(3)])
    assert table.to_arrow().num_rows == 3
    if supplied is not None:
        assert supplied.size_bytes > before
    assert db.read_consistency_interval == timedelta(seconds=5)
    assert table.namespace == []
    assert table.id == "rfm"
    with pytest.warns(DeprecationWarning):
        table.replace_field_metadata("id", {"old": "value"})
    with pytest.warns(DeprecationWarning):
        table.replace_field_metadata("id", {"a": "b"})
    assert table.schema.field("id").metadata == {b"a": b"b"}


def test_namespace_connection_properties_remain_writable(tmp_path):
    session = lancedb.Session()
    interval = timedelta(seconds=5)
    with lancedb.connect_namespace(
        "dir",
        {"root": str(tmp_path)},
        session=session,
        read_consistency_interval=interval,
    ) as db:
        assert db.session is session
        assert db.read_consistency_interval == interval
        db.read_consistency_interval = timedelta(0)
        assert db.read_consistency_interval == timedelta(0)
