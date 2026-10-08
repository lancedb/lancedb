# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from datetime import timedelta
import json
from urllib.parse import parse_qs, urlparse

import lancedb
import pytest

from test_remote_db import mock_lancedb_connection


def respond(request, body):
    request.send_response(200)
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
def test_remote_len_and_contains_follow_pages(tables):
    requests = []

    def handler(request):
        requests.append(request.path)
        query = parse_qs(urlparse(request.path).query)
        if tables and "page_token" not in query:
            respond(request, {"tables": [tables[0]], "page_token": "opaque-next"})
        else:
            if tables:
                assert query["page_token"] == ["opaque-next"]
            respond(request, {"tables": tables[1:]})

    with mock_lancedb_connection(handler) as db:
        assert len(db) == len(tables)
        assert ("last" in db) == bool(tables)
        assert "missing" not in db
        requests.clear()
        assert ("first" in db) == bool(tables)
        assert len(requests) == 1


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
def test_local_session_exposes_database_cache(tmp_path, custom_session):
    supplied = lancedb.Session() if custom_session else None
    db = lancedb.connect(
        tmp_path, session=supplied, read_consistency_interval=timedelta(seconds=5)
    )
    session = db.session
    assert isinstance(session, lancedb.Session)
    before = session.size_bytes
    table = db.create_table("rfm", [{"id": i} for i in range(3)])
    assert table.to_arrow().num_rows == 3
    assert session.size_bytes > before
    assert db.session.size_bytes == session.size_bytes
    if supplied is not None:
        assert supplied.size_bytes == session.size_bytes
        assert supplied.approx_num_items == session.approx_num_items
    assert db.read_consistency_interval == timedelta(seconds=5)
    assert table.namespace == []
    assert table.id == "rfm"
    with pytest.warns(DeprecationWarning):
        table.replace_field_metadata("id", {"old": "value"})
    with pytest.warns(DeprecationWarning):
        table.replace_field_metadata("id", {"a": "b"})
    assert table.schema.field("id").metadata == {b"a": b"b"}


def test_local_session_after_close(tmp_path):
    db = lancedb.connect(tmp_path)
    db.close()
    with pytest.raises(RuntimeError, match="Connection is closed"):
        _ = db.session


def test_local_session_from_inner(tmp_path):
    from lancedb.background_loop import LOOP
    from lancedb.db import LanceDBConnection

    async_db = LOOP.run(lancedb.connect_async(tmp_path))
    db = LanceDBConnection.from_inner(async_db._inner, None)
    assert isinstance(db.session, lancedb.Session)
    db.close()


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
