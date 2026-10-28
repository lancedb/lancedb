# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import contextlib
import http.server
import json
import threading

import lancedb
import pytest
import pyarrow as pa
from lancedb.db import AsyncConnection
from lancedb.materialized_view import MaterializedViewDefinition
from lancedb.remote.db import RemoteDBConnection


STABLE_ROW_IDS = {"new_table_enable_stable_row_ids": "true"}


def make_db(tmp_path):
    db = lancedb.connect(tmp_path, storage_options=STABLE_ROW_IDS)
    db.create_table(
        "people",
        [
            {"name": "ada", "age": 36},
            {"name": "kid", "age": 7},
            {"name": "grace", "age": 85},
        ],
    )
    return db


@contextlib.contextmanager
def mock_remote_materialized_view_refresh():
    requests = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            requests.append(self.path)
            assert self.path == "/v1/table/view/describe/"
            response = {"version": 42, "schema": {"fields": []}}
            encoded = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    with http.server.HTTPServer(("localhost", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            yield f"http://localhost:{server.server_address[1]}", requests
        finally:
            server.shutdown()
            thread.join()


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize("background", [False, True])
@pytest.mark.parametrize("full", [False, True])
async def test_remote_refresh_reports_unknown_metrics(sync, background, full):
    flight = pytest.importorskip("pyarrow.flight")

    class RefreshServer(flight.FlightServerBase):
        def __init__(self):
            super().__init__("grpc://127.0.0.1:0")
            self.commands = []

        def do_get(self, context, ticket):
            self.commands.append(ticket.ticket)
            if b"DESCRIBE MATERIALIZED VIEW" in ticket.ticket:
                return flight.RecordBatchStream(
                    pa.table(
                        {
                            "name": ["view"],
                            "query": ["SELECT * FROM source"],
                        }
                    )
                )
            assert b"REFRESH MATERIALIZED VIEW" in ticket.ticket
            # SQL completion carries no refresh statistics.
            return flight.RecordBatchStream(pa.table({"status": ["ok"]}))

    with (
        mock_remote_materialized_view_refresh() as (host, requests),
        RefreshServer() as server,
    ):
        options = {"full": True} if full else {}
        connect = lancedb.connect if sync else lancedb.connect_async
        db = connect(
            "db://dev",
            api_key="fake",
            host_override=host,
            sql_host_override=f"grpc://127.0.0.1:{server.port}",
            client_config={"retry_config": {"retries": 0}},
        )
        if sync:
            view = db.open_materialized_view("view")
            result = (
                view.refresh_async(**options).wait()
                if background
                else view.refresh(**options)
            )
        else:
            db = await db
            view = await db.open_materialized_view("view")
            if background:
                job = await view.refresh_async(**options)
                result = await job.wait()
            else:
                result = await view.refresh(**options)

        assert result.mode is None
        assert result.rows_written is None
        assert result.source_version is None
        assert result.version == 42
        assert repr(result) == (
            "RefreshMaterializedViewResult(mode=None, rows_written=None, "
            "source_version=None, version=42)"
        )
        describes = [
            command
            for command in server.commands
            if b"DESCRIBE MATERIALIZED VIEW" in command
        ]
        refreshes = [
            command
            for command in server.commands
            if b"REFRESH MATERIALIZED VIEW" in command
        ]
        assert len(describes) == 2
        assert len(refreshes) == 1
        assert all(
            b'DESCRIBE MATERIALIZED VIEW "view"' in command for command in describes
        )
        (refresh,) = refreshes
        assert b'REFRESH MATERIALIZED VIEW "view"' in refresh
        assert (b"WITH (full = true)" in refresh) is full
        assert not any("count_rows" in path for path in requests)


def test_sync_remote_create_uses_public_async_connection():
    calls = []

    class StubAsyncTable:
        name = "adults"

    class StubAsyncMaterializedView:
        table = StubAsyncTable()

    class StrictAsyncConnection:
        async def create_materialized_view(
            self,
            name,
            source,
            *,
            select=None,
            where=None,
            limit=None,
            with_no_data=False,
            auto_refresh_enabled=None,
            auto_refresh_interval_seconds=None,
        ):
            calls.append(
                (
                    name,
                    source,
                    select,
                    where,
                    limit,
                    with_no_data,
                    auto_refresh_enabled,
                    auto_refresh_interval_seconds,
                )
            )
            return StubAsyncMaterializedView()

        async def drop_materialized_view(self, name, *, namespace_path=None):
            calls.append(("drop", name, namespace_path))

    db = RemoteDBConnection.__new__(RemoteDBConnection)
    db._conn = StrictAsyncConnection()
    db.db_name = "example"
    db.serialize = lambda: "{}"

    view = db.create_materialized_view(
        "adults",
        "people",
        select=["name"],
        where="age >= 18",
        limit=10,
        with_no_data=True,
        auto_refresh_enabled=True,
        auto_refresh_interval_seconds=0.5,
    )
    assert view.name == "adults"
    assert calls == [("adults", "people", ["name"], "age >= 18", 10, True, True, 0.5)]

    db.drop_materialized_view("adults", namespace_path=["analytics"])
    assert calls[-1] == ("drop", "adults", ["analytics"])


@pytest.mark.experimental_functions
def test_create_refresh_and_query(tmp_path):
    db = make_db(tmp_path)
    view = db.create_materialized_view(
        "adults",
        "people",
        select=["name", ("shout", "upper(name)")],
        where="age >= 18",
    )
    assert view.name == "adults"
    assert view.table.count_rows() == 2

    rows = view.table.search().to_list()
    assert sorted(row["shout"] for row in rows) == ["ADA", "GRACE"]


@pytest.mark.experimental_functions
def test_create_and_refresh_jobs(tmp_path):
    db = make_db(tmp_path)
    create_job = db.create_materialized_view_async(
        "adults", "people", where="age >= 18", with_no_data=True
    )
    assert create_job.id is None
    assert create_job.job_type is None
    assert create_job.wait() is None

    view = db.open_materialized_view("adults")
    refresh_job = view.refresh_async()
    assert refresh_job.id is None
    assert refresh_job.job_type is None
    result = refresh_job.wait()
    assert result.mode == "rebuild"
    assert result.rows_written == 2
    assert view.table.count_rows() == 2

    drop_job = db.drop_materialized_view_async("adults")
    assert drop_job.id is None
    assert drop_job.wait() is None
    assert "adults" not in list(db.list_materialized_views())


@pytest.mark.experimental_functions
def test_definition_round_trips(tmp_path):
    db = make_db(tmp_path)
    db.create_materialized_view("adults", "people", where="age >= 18")

    view = db.open_materialized_view("adults")
    assert view.definition == MaterializedViewDefinition(
        query="SELECT name, age FROM people WHERE age >= 18"
    )


@pytest.mark.experimental_functions
@pytest.mark.parametrize("full", [False, True])
def test_full_refresh_after_append(tmp_path, full):
    db = make_db(tmp_path)
    view = db.create_materialized_view("copy", "people", with_no_data=True)
    view.refresh()

    db.open_table("people").add([{"name": "alan", "age": 41}])
    result = view.refresh(full=full)
    assert result.mode == "rebuild"
    assert result.rows_written == 4
    assert result.source_version == db.open_table("people").version
    assert view.table.count_rows() == 4

    assert view.refresh().mode == "rebuild"


@pytest.mark.experimental_functions
def test_full_refresh_can_pin_local_source_version(tmp_path):
    db = make_db(tmp_path)
    source = db.open_table("people")
    version = source.version
    view = db.create_materialized_view("copy", "people", with_no_data=True)
    source.add([{"name": "alan", "age": 41}])

    result = view.refresh(full=True, source_version=version)
    assert result.mode == "rebuild"
    assert result.rows_written == 3
    assert result.source_version == version
    assert view.table.count_rows() == 3


@pytest.mark.experimental_functions
def test_full_refresh_after_update(tmp_path):
    db = make_db(tmp_path)
    view = db.create_materialized_view("copy", "people", with_no_data=True)
    view.refresh()

    db.open_table("people").update(where="name = 'kid'", values={"age": 8})
    result = view.refresh()
    assert result.mode == "rebuild"
    assert result.rows_written == 3
    rows = view.table.search().to_list()
    assert sorted(row["age"] for row in rows) == [8, 36, 85]


@pytest.mark.experimental_functions
def test_legacy_storage_source_update_rebuilds(tmp_path):
    db = lancedb.connect(
        tmp_path,
        storage_options={**STABLE_ROW_IDS, "new_table_data_storage_version": "legacy"},
    )
    db.create_table("people", [{"name": "ada", "age": 36}, {"name": "kid", "age": 7}])
    view = db.create_materialized_view("copy", "people", with_no_data=True)
    view.refresh()

    db.open_table("people").update(where="name = 'kid'", values={"age": 8})
    result = view.refresh()
    assert result.mode == "rebuild"
    rows = view.table.search().to_list()
    assert sorted(row["age"] for row in rows) == [8, 36]


@pytest.mark.experimental_functions
def test_direct_writes_to_a_view_are_refused(tmp_path):
    db = make_db(tmp_path)
    view = db.create_materialized_view("copy", "people")
    table = view.table
    row = [{"name": "eve", "age": 1}]
    refused = pytest.raises(NotImplementedError, match="is a materialized view")

    with refused:
        table.add(row)
    with refused:
        table.update(where="name = 'kid'", values={"age": 8})
    with refused:
        table.delete("age < 18")
    with refused:
        table.merge_insert("name").when_not_matched_insert_all().execute(row)
    with refused:
        table.add_columns({"older": "age + 1"})
    with refused:
        table.drop_columns(["age"])
    assert table.count_rows() == 3

    db.open_table("people").add(row)
    assert view.refresh().rows_written == 4


@pytest.mark.experimental_functions
def test_list_and_not_a_view(tmp_path):
    db = make_db(tmp_path)
    db.create_materialized_view("adults", "people", where="age >= 18")

    assert list(db.list_materialized_views()) == ["adults"]
    with pytest.raises(ValueError, match="not a materialized view"):
        db.open_materialized_view("people")
    with pytest.raises(ValueError, match="not a materialized view"):
        db.drop_materialized_view("people")

    db.drop_materialized_view("adults")
    assert list(db.list_materialized_views()) == []


@pytest.mark.experimental_functions
def test_invalid_expression_fails_at_create(tmp_path):
    db = make_db(tmp_path)
    with pytest.raises(Exception, match="missing"):
        db.create_materialized_view("bad", "people", select=[("x", "missing + 1")])
    assert "bad" not in db.list_tables().tables


@pytest.mark.experimental_functions
@pytest.mark.asyncio
async def test_async_create_refresh_and_open(tmp_path):
    db = await lancedb.connect_async(tmp_path, storage_options=STABLE_ROW_IDS)
    await db.create_table("people", [{"name": "ada", "age": 36}])

    view = await db.create_materialized_view(
        "shouts",
        "people",
        select=[("shout", "upper(name)")],
        with_no_data=True,
    )
    result = await view.refresh()
    assert result.mode == "rebuild"
    assert result.rows_written == 1

    reopened = await db.open_materialized_view("shouts")
    definition = await reopened.definition()
    assert definition.query == "SELECT upper(name) AS shout FROM people"
    assert [item async for item in db.list_materialized_views()] == ["shouts"]


@pytest.mark.experimental_functions
@pytest.mark.asyncio
async def test_async_create_and_refresh_jobs(tmp_path):
    db = await lancedb.connect_async(tmp_path, storage_options=STABLE_ROW_IDS)
    await db.create_table("people", [{"name": "ada", "age": 36}])

    create_job = await db.create_materialized_view_async(
        "adults", "people", with_no_data=True
    )
    assert create_job.id is None
    assert create_job.job_type is None
    assert await create_job.wait() is None

    view = await db.open_materialized_view("adults")
    refresh_job = await view.refresh_async()
    assert refresh_job.id is None
    assert refresh_job.job_type is None
    result = await refresh_job.wait()
    assert result.mode == "rebuild"
    assert result.rows_written == 1

    drop_job = await db.drop_materialized_view_async("adults")
    assert drop_job.id is None
    assert await drop_job.wait() is None
    assert "adults" not in [item async for item in db.list_materialized_views()]


@pytest.mark.experimental_functions
@pytest.mark.asyncio
async def test_async_refresh_rebuilds(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    await db.create_table("people", [{"name": "ada", "age": 36}])
    view = await db.create_materialized_view("copy", "people", with_no_data=True)
    await view.refresh()

    table = await db.open_table("people")
    await table.add([{"name": "alan", "age": 41}])
    result = await view.refresh()
    assert result.mode == "rebuild"
    assert result.rows_written == 2


@pytest.mark.experimental_functions
def test_source_does_not_require_stable_row_ids(tmp_path):
    db = lancedb.connect(tmp_path)
    db.create_table("plain", [{"x": 1}])
    view = db.create_materialized_view("v", "plain")
    assert view.table.count_rows() == 1


@pytest.mark.experimental_functions
def test_bare_select_names_are_quoted(tmp_path):
    db = lancedb.connect(tmp_path, storage_options=STABLE_ROW_IDS)
    db.create_table("odd_names", [{"order item": "widget", "select": 2}])

    view = db.create_materialized_view(
        "quoted",
        "odd_names",
        select=["order item", "select"],
        with_no_data=True,
    )
    result = view.refresh()
    assert result.rows_written == 1
    rows = view.table.search().to_list()
    assert rows[0]["order item"] == "widget"
    assert rows[0]["select"] == 2


@pytest.mark.experimental_functions
def test_scalar_select_is_one_column(tmp_path):
    db = make_db(tmp_path)
    view = db.create_materialized_view("just_name", "people", select="name")
    view.refresh()
    rows = view.table.search().to_list()
    assert set(rows[0]) == {"name"}
    assert sorted(row["name"] for row in rows) == ["ada", "grace", "kid"]


@pytest.mark.experimental_functions
@pytest.mark.asyncio
async def test_async_scalar_select_is_one_column(tmp_path):
    db = await lancedb.connect_async(tmp_path, storage_options=STABLE_ROW_IDS)
    await db.create_table("people", [{"name": "ada", "age": 36}])
    view = await db.create_materialized_view("just_name", "people", select="name")
    await view.refresh()
    rows = await view.table.query().to_list()
    assert set(rows[0]) == {"name"}


@pytest.mark.experimental_functions
def test_limit_above_i64_max_is_refused(tmp_path):
    db = make_db(tmp_path)
    with pytest.raises(ValueError, match="exceeds the maximum"):
        db.create_materialized_view("too_big", "people", limit=2**63)
    # The boundary is fine, and zero still means an empty view.
    db.create_materialized_view("at_max", "people", limit=2**63 - 1)
    empty = db.create_materialized_view("none", "people", limit=0)
    empty.refresh()
    assert empty.table.count_rows() == 0


def _namespace_db(tmp_path):
    return lancedb.connect_namespace(
        "dir",
        {"root": str(tmp_path)},
        storage_options=STABLE_ROW_IDS,
    )


@pytest.mark.experimental_functions
def test_namespace_connection_materialized_views(tmp_path):
    db = _namespace_db(tmp_path)
    db.create_table(
        "people",
        [{"name": "ada", "age": 36}, {"name": "kid", "age": 7}],
        storage_options=STABLE_ROW_IDS,
    )

    view = db.create_materialized_view("adults", "people", where="age >= 18")
    view.refresh()
    assert view.table.count_rows() == 1
    assert list(db.list_materialized_views()) == ["adults"]

    reopened = db.open_materialized_view("adults")
    assert reopened.definition.query.startswith("SELECT name, age FROM ")
    with pytest.raises(ValueError, match="not a materialized view"):
        db.open_materialized_view("people")

    create_job = db.create_materialized_view_async(
        "job_view", "people", with_no_data=True
    )
    assert create_job.wait() is None
    refresh_job = db.open_materialized_view("job_view").refresh_async()
    assert refresh_job.wait().rows_written == 2

    assert db.drop_materialized_view_async("job_view").wait() is None
    db.drop_materialized_view("adults")
    assert list(db.list_materialized_views()) == []


@pytest.mark.experimental_functions
@pytest.mark.asyncio
async def test_async_namespace_connection_materialized_views(tmp_path):
    db = lancedb.connect_namespace_async(
        "dir",
        {"root": str(tmp_path)},
        storage_options=STABLE_ROW_IDS,
    )
    await db.create_table(
        "people",
        [{"name": "ada", "age": 36}, {"name": "kid", "age": 7}],
        storage_options=STABLE_ROW_IDS,
    )

    view = await db.create_materialized_view("adults", "people", where="age >= 18")
    await view.refresh()
    assert await view.table.count_rows() == 1
    assert [item async for item in db.list_materialized_views()] == ["adults"]

    reopened = await db.open_materialized_view("adults")
    assert (await reopened.definition()).query.startswith("SELECT name, age FROM ")

    # The view's table came through the namespace, not straight from the
    # inner connection: a bare inner table carries no namespace context, so
    # its pushdown routing differs from a table the namespace opened.
    through_namespace = await db.open_table("adults")
    for handle in (view.table, reopened.table):
        assert (
            handle._route_pushdown_to_rust == through_namespace._route_pushdown_to_rust
        )
        assert handle._namespace_path == through_namespace._namespace_path

    create_job = await db.create_materialized_view_async(
        "job_view", "people", with_no_data=True
    )
    assert await create_job.wait() is None
    job_view = await db.open_materialized_view("job_view")
    refresh_job = await job_view.refresh_async()
    assert (await refresh_job.wait()).rows_written == 2

    drop_job = await db.drop_materialized_view_async("job_view")
    assert await drop_job.wait() is None
    await db.drop_materialized_view("adults")
    assert [item async for item in db.list_materialized_views()] == []


def test_stored_queries_and_legacy_layouts_are_read():
    import pyarrow as pa

    from lancedb.materialized_view import _definition_from_schema

    def read(definition: dict) -> MaterializedViewDefinition:
        schema = pa.schema([pa.field("id", pa.int32())]).with_metadata(
            {b"mv.definition": json.dumps(definition).encode()}
        )
        return _definition_from_schema(schema, "v")

    query = "SELECT id, c.chunk FROM ns.docs, UNNEST(chunks) AS c WHERE id > 1"
    assert read({"format": 1, "query": query}).query == query

    # The structured layout written before the format number reads as the
    # query it described, under either of its kind tags.
    assert (
        read(
            {
                "kind": "namespaced_select",
                "source_table": "people",
                "source_namespace": ["ns"],
                "projections": [
                    {"output": "name", "expression": "`name`"},
                    {"output": "Shout", "expression": "upper(name)"},
                ],
                "filter": "age >= 18",
                "limit": 10,
            }
        ).query
        == "SELECT `name`, upper(name) AS `Shout` FROM ns.people "
        "WHERE age >= 18 LIMIT 10"
    )
    assert read({"kind": "select", "source_table": "people"}).query == (
        "SELECT * FROM people"
    )

    # A newer writer's layout is reported, never guessed at.
    for newer in (
        {"format": 3, "query": query},
        {"kind": "select_v3", "source_table": "people"},
    ):
        with pytest.raises(NotImplementedError, match="cannot refresh"):
            read(newer)


@pytest.mark.experimental_functions
def test_local_materialized_view_listing_pages_and_resume(tmp_path):
    db = make_db(tmp_path)
    db.create_materialized_view("a_view", "people", with_no_data=True)
    db.create_materialized_view("z_view", "people", with_no_data=True)
    db.create_table("ordinary", [{"id": 1}])
    names = db.list_materialized_views(page_limit=1)
    assert names.num_page_results() == 0
    first = next(names)
    assert names.num_page_results() == 0
    assert names.page_token() is not None
    resumed = db.list_materialized_views(page_token=names.page_token(), page_limit=1)
    assert sorted([first, *resumed]) == ["a_view", "z_view"]
    assert resumed.page_token() is None
    assert list(resumed) == []


class QueryBackedCreateJob:
    id = "0198f1b2-c3d4-7e5f-8123-456789abcdef"

    job_type = "query"

    async def wait(self):
        return None

    async def status(self):
        return "finished"


class CapturingCreateConnection:
    def __init__(self):
        self.calls = []

    async def create_materialized_view_async(self, name, source, **options):
        self.calls.append((name, source, options))
        return QueryBackedCreateJob()


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"auto_refresh_enabled": True},
        {"auto_refresh_enabled": False, "auto_refresh_interval_seconds": 0.5},
    ],
)
def test_remote_create_forwards_refresh_options_without_interval_default(options):
    native = CapturingCreateConnection()
    db = RemoteDBConnection.__new__(RemoteDBConnection)
    db._conn = AsyncConnection(native)
    job = db.create_materialized_view_async("adults", "people", **options)
    assert job.id == QueryBackedCreateJob.id
    assert job.job_type == "query"
    assert not hasattr(job, "query_id")
    assert job.wait() is None
    assert job.status() == "finished"
    assert native.calls == [
        (
            "adults",
            "people",
            {
                "projections": None,
                "filter": None,
                "limit": None,
                "with_no_data": False,
                "auto_refresh_enabled": options.get("auto_refresh_enabled"),
                "auto_refresh_interval_seconds": options.get(
                    "auto_refresh_interval_seconds"
                ),
            },
        )
    ]


@pytest.mark.asyncio
async def test_async_remote_create_keeps_interval_unspecified():
    native = CapturingCreateConnection()
    db = AsyncConnection(native)
    job = await db.create_materialized_view_async(
        "adults", "people", auto_refresh_enabled=True
    )
    assert job.id == QueryBackedCreateJob.id
    assert job.job_type == "query"
    assert not hasattr(job, "query_id")
    assert await job.wait() is None
    assert await job.status() == "finished"
    assert native.calls[0][2]["auto_refresh_enabled"] is True
    assert native.calls[0][2]["auto_refresh_interval_seconds"] is None


@pytest.mark.experimental_functions
@pytest.mark.parametrize(
    "options",
    [
        {"auto_refresh_enabled": True},
        {"auto_refresh_interval_seconds": 300},
    ],
)
def test_embedded_create_refuses_automatic_refresh_before_creating_view(
    tmp_path, options
):
    db = make_db(tmp_path)
    with pytest.raises(NotImplementedError, match="Cloud or Enterprise"):
        db.create_materialized_view("adults", "people", **options)
    assert "adults" not in db.table_names()


@pytest.mark.parametrize("seconds", [0, -1, float("nan"), float("inf")])
def test_remote_create_rejects_invalid_interval_without_request(seconds):
    db = lancedb.connect("db://dev", api_key="fake", host_override="http://127.0.0.1:1")
    with pytest.raises(ValueError, match="positive, finite"):
        db.create_materialized_view_async(
            "mv", "source", auto_refresh_interval_seconds=seconds
        )


@pytest.mark.experimental_functions
def test_namespace_forwards_automatic_refresh_request(tmp_path):
    db = lancedb.connect_namespace("dir", {"root": str(tmp_path)})
    with pytest.raises(NotImplementedError, match="Cloud or Enterprise"):
        db.create_materialized_view("mv", "source", auto_refresh_enabled=True)


@pytest.mark.experimental_functions
@pytest.mark.asyncio
async def test_async_namespace_forwards_automatic_refresh_request(tmp_path):
    db = lancedb.connect_namespace_async("dir", {"root": str(tmp_path)})
    with pytest.raises(NotImplementedError, match="Cloud or Enterprise"):
        await db.create_materialized_view("mv", "source", auto_refresh_enabled=True)


@pytest.mark.experimental_functions
def test_embedded_explicitly_disabled_refresh_is_supported(tmp_path):
    db = make_db(tmp_path)
    view = db.create_materialized_view("mv", "people", auto_refresh_enabled=False)
    assert view.table.count_rows() == 3
