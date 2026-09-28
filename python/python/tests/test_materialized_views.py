# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import contextlib
import http.server
import json
import threading

import lancedb
import pytest
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
def mock_remote_materialized_views():
    requests = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            requests.append(self.path)
            encoded = json.dumps({"views": ["daily_sales"]}).encode()
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


@contextlib.contextmanager
def mock_remote_materialized_view_create():
    requests = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length) or b"{}")
            requests.append((self.path, body))
            job_id = "mv-drop-123" if self.path.endswith("/drop") else "mv-create-123"
            encoded = json.dumps({"job_id": job_id}).encode()
            self.send_response(202)
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


def test_remote_list_uses_namespace_route():
    with mock_remote_materialized_views() as (host, requests):
        db = lancedb.connect(
            "db://dev",
            api_key="fake",
            host_override=host,
            client_config={"retry_config": {"retries": 0}},
        )
        assert db.list_materialized_views() == ["daily_sales"]
    assert requests == ["/v1/namespace/$/materialized_view/list"]


def test_remote_create_async_returns_server_job():
    with mock_remote_materialized_view_create() as (host, requests):
        db = lancedb.connect(
            "db://dev",
            api_key="fake",
            host_override=host,
            client_config={"retry_config": {"retries": 0}},
        )
        job = db.create_materialized_view_async("adults", "people", where="age >= 18")
        assert job.id == "mv-create-123"
    assert requests == [
        (
            "/v1/materialized_view/adults/create",
            {"query": 'SELECT * FROM "people" WHERE age >= 18', "with_no_data": False},
        )
    ]


def test_remote_drop_async_returns_server_job():
    with mock_remote_materialized_view_create() as (host, requests):
        db = lancedb.connect(
            "db://dev",
            api_key="fake",
            host_override=host,
            client_config={"retry_config": {"retries": 0}},
        )
        job = db.drop_materialized_view_async("adults")
        assert job.id == "mv-drop-123"
    assert requests == [("/v1/materialized_view/adults/drop", {})]


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
        ):
            calls.append((name, source, select, where, limit, with_no_data))
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
    )
    assert view.name == "adults"
    assert calls == [("adults", "people", ["name"], "age >= 18", 10, True)]

    db.drop_materialized_view("adults", namespace_path=["analytics"])
    assert calls[-1] == ("drop", "adults", ["analytics"])


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


def test_create_and_refresh_jobs(tmp_path):
    db = make_db(tmp_path)
    create_job = db.create_materialized_view_async(
        "adults", "people", where="age >= 18", with_no_data=True
    )
    assert create_job.id is None
    assert create_job.wait() is None

    view = db.open_materialized_view("adults")
    refresh_job = view.refresh_async()
    assert refresh_job.id is None
    result = refresh_job.wait()
    assert result.mode == "rebuild"
    assert result.rows_written == 2
    assert view.table.count_rows() == 2

    drop_job = db.drop_materialized_view_async("adults")
    assert drop_job.id is None
    assert drop_job.wait() is None
    assert "adults" not in db.list_materialized_views()


def test_definition_round_trips(tmp_path):
    db = make_db(tmp_path)
    db.create_materialized_view("adults", "people", where="age >= 18")

    view = db.open_materialized_view("adults")
    assert view.definition == MaterializedViewDefinition(
        query="SELECT name, age FROM people WHERE age >= 18"
    )


def test_incremental_refresh_after_append(tmp_path):
    db = make_db(tmp_path)
    view = db.create_materialized_view("copy", "people", with_no_data=True)
    view.refresh()

    db.open_table("people").add([{"name": "alan", "age": 41}])
    result = view.refresh()
    assert result.mode == "incremental"
    assert result.rows_written == 1
    assert view.table.count_rows() == 4

    assert view.refresh().mode == "no_op"


def test_incremental_refresh_after_update(tmp_path):
    db = make_db(tmp_path)
    view = db.create_materialized_view("copy", "people", with_no_data=True)
    view.refresh()

    db.open_table("people").update(where="name = 'kid'", values={"age": 8})
    result = view.refresh()
    assert result.mode == "incremental"
    assert result.rows_written == 1
    rows = view.table.search().to_list()
    assert sorted(row["age"] for row in rows) == [8, 36, 85]


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


def test_list_and_not_a_view(tmp_path):
    db = make_db(tmp_path)
    db.create_materialized_view("adults", "people", where="age >= 18")

    assert db.list_materialized_views() == ["adults"]
    with pytest.raises(ValueError, match="not a materialized view"):
        db.open_materialized_view("people")
    with pytest.raises(ValueError, match="not a materialized view"):
        db.drop_materialized_view("people")

    db.drop_materialized_view("adults")
    assert db.list_materialized_views() == []


def test_invalid_expression_fails_at_create(tmp_path):
    db = make_db(tmp_path)
    with pytest.raises(Exception, match="missing"):
        db.create_materialized_view("bad", "people", select=[("x", "missing + 1")])
    assert "bad" not in db.list_tables().tables


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
    assert await db.list_materialized_views() == ["shouts"]


@pytest.mark.asyncio
async def test_async_create_and_refresh_jobs(tmp_path):
    db = await lancedb.connect_async(tmp_path, storage_options=STABLE_ROW_IDS)
    await db.create_table("people", [{"name": "ada", "age": 36}])

    create_job = await db.create_materialized_view_async(
        "adults", "people", with_no_data=True
    )
    assert create_job.id is None
    assert await create_job.wait() is None

    view = await db.open_materialized_view("adults")
    refresh_job = await view.refresh_async()
    assert refresh_job.id is None
    result = await refresh_job.wait()
    assert result.mode == "rebuild"
    assert result.rows_written == 1

    drop_job = await db.drop_materialized_view_async("adults")
    assert drop_job.id is None
    assert await drop_job.wait() is None
    assert "adults" not in await db.list_materialized_views()


@pytest.mark.asyncio
async def test_async_incremental(tmp_path):
    db = await lancedb.connect_async(tmp_path, storage_options=STABLE_ROW_IDS)
    await db.create_table("people", [{"name": "ada", "age": 36}])
    view = await db.create_materialized_view("copy", "people", with_no_data=True)
    await view.refresh()

    table = await db.open_table("people")
    await table.add([{"name": "alan", "age": 41}])
    result = await view.refresh()
    assert result.mode == "incremental"
    assert result.rows_written == 1


def test_source_requires_stable_row_ids(tmp_path):
    db = lancedb.connect(tmp_path)
    db.create_table("plain", [{"x": 1}])
    with pytest.raises(Exception, match="stable row ids"):
        db.create_materialized_view("v", "plain")


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


def test_scalar_select_is_one_column(tmp_path):
    db = make_db(tmp_path)
    view = db.create_materialized_view("just_name", "people", select="name")
    view.refresh()
    rows = view.table.search().to_list()
    assert set(rows[0]) - {"__source_row_id"} == {"name"}
    assert sorted(row["name"] for row in rows) == ["ada", "grace", "kid"]


@pytest.mark.asyncio
async def test_async_scalar_select_is_one_column(tmp_path):
    db = await lancedb.connect_async(tmp_path, storage_options=STABLE_ROW_IDS)
    await db.create_table("people", [{"name": "ada", "age": 36}])
    view = await db.create_materialized_view("just_name", "people", select="name")
    await view.refresh()
    rows = await view.table.query().to_list()
    assert set(rows[0]) - {"__source_row_id"} == {"name"}


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
    assert db.list_materialized_views() == ["adults"]

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
    assert db.list_materialized_views() == []


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
    assert await db.list_materialized_views() == ["adults"]

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
    assert await db.list_materialized_views() == []


def test_stored_queries_and_legacy_layouts_are_read():
    import json

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

    for fmt, function in [(3, "vector_duplicate_pairs"), (4, "vector_dedup")]:
        native = f"SELECT * FROM {function}('images', 3, 'phash', 4)"
        assert read({"format": fmt, "query": native}).query == native

    # A newer writer's layout is reported, never guessed at.
    for newer in (
        {"format": 5, "query": query},
        {"kind": "select_v3", "source_table": "people"},
    ):
        with pytest.raises(NotImplementedError, match="cannot refresh"):
            read(newer)


def indexed_dedup_source(db, name="images"):
    import pyarrow as pa
    from lancedb.index import IvfFlat

    vectors = pa.FixedSizeListArray.from_arrays(
        pa.array(
            [v for i in range(256) for v in (float(i if i < 3 else i * 10), 0.0)],
            type=pa.float32(),
        ),
        2,
    )
    table = db.create_table(name, pa.table({"id": range(256), "vector": vectors}))
    table.create_index("vector", config=IvfFlat(num_partitions=1))
    return table


@pytest.mark.parametrize("stable", [False, True])
def test_vector_dedup_source_captures_snapshot_and_preserves_source(tmp_path, stable):
    db = lancedb.connect(
        tmp_path,
        storage_options={"new_table_enable_stable_row_ids": str(stable).lower()},
    )
    source = indexed_dedup_source(db, "images.with.dot")
    version = source.version
    expression = lancedb.vector_dedup(
        source.name, column="vector", distance_threshold=1
    )
    view = db.create_materialized_view("clean", expression)
    assert "vector_dedup(" in view.definition.query
    assert f", {version}, 'vector', 1.0)" in view.definition.query
    assert view.table.count_rows() == 255
    assert [r["id"] for r in view.table.search().where("id < 3").to_list()] == [0, 2]
    assert source.version == version
    assert source.count_rows() == 256

    source.delete("id = 0")
    view.refresh_async().wait()
    assert view.table.search().where("id = 0").to_list() == [
        {"id": 0, "vector": [0.0, 0.0]}
    ]
    assert expression.dataset_version is None  # reusable immutable declaration


def test_vector_dedup_create_job_and_explicit_snapshot(tmp_path):
    db = lancedb.connect(tmp_path)
    source = indexed_dedup_source(db)
    version = source.version
    source.delete("id = 0")
    job = db.create_materialized_view_async(
        "clean",
        lancedb.vector_dedup(
            "images", column="vector", distance_threshold=1, dataset_version=version
        ),
    )
    assert job.wait() is None
    view = db.open_materialized_view("clean")
    assert view.table.count_rows() == 255
    assert len(view.table.search().where("id = 0").to_list()) == 1


@pytest.mark.asyncio
async def test_async_vector_dedup_declaration_and_creation(tmp_path):
    sync_db = lancedb.connect(tmp_path)
    source = indexed_dedup_source(sync_db)
    db = await lancedb.connect_async(tmp_path)
    expression = lancedb.vector_dedup("images", column="vector", distance_threshold=1)
    view = await db.create_materialized_view("clean", expression, with_no_data=True)
    assert await view.table.count_rows() == 0
    # The snapshot is captured at declaration even if refresh comes later.
    source.delete("id = 0")
    job = await view.refresh_async()
    await job.wait()
    assert await view.table.count_rows() == 255
    another = await db.create_materialized_view_async(
        "later", expression, with_no_data=True
    )
    await another.wait()
    later = await db.open_materialized_view("later")
    assert (await later.definition()).query != (await view.definition()).query
    assert await later.table.count_rows() == 0


@pytest.mark.parametrize(
    "options", [{"select": ["id"]}, {"select": []}, {"where": "id > 0"}, {"limit": 1}]
)
def test_vector_dedup_rejects_extra_clauses_before_opening_source(tmp_path, options):
    db = lancedb.connect(tmp_path)
    expression = lancedb.vector_dedup("missing", column="vector", distance_threshold=1)
    with pytest.raises(ValueError, match="cannot be combined"):
        db.create_materialized_view("bad", expression, **options)
    assert db.list_tables().tables == []


@pytest.mark.parametrize("version", [0, -1, True, 1.5, "2", 2**64])
def test_vector_dedup_rejects_invalid_snapshot(version):
    with pytest.raises(ValueError, match="dataset_version"):
        lancedb.vector_dedup(
            "images", column="vector", distance_threshold=1, dataset_version=version
        )


@pytest.mark.parametrize(
    "threshold", [float("nan"), float("inf"), -float("inf"), 1e100, 10**1000, True, "4"]
)
def test_vector_dedup_rejects_invalid_threshold(threshold):
    with pytest.raises((ValueError, TypeError), match="distance_threshold"):
        lancedb.vector_dedup("images", column="vector", distance_threshold=threshold)


def test_vector_dedup_descriptor_is_frozen():
    from dataclasses import FrozenInstanceError

    expression = lancedb.vector_dedup("images", column="vector", distance_threshold=1)
    with pytest.raises(FrozenInstanceError):
        expression.column = "other"


@pytest.mark.parametrize("with_no_data", [False, True])
def test_remote_vector_dedup_uses_mv_endpoint_without_sql_client(with_no_data):
    with mock_remote_materialized_view_create() as (host, requests):
        db = lancedb.connect(
            "db://dev",
            api_key="fake",
            host_override=host,
            sql_host_override="grpc://127.0.0.1:1",
            client_config={"retry_config": {"retries": 0}},
        )
        job = db.create_materialized_view_async(
            "clean",
            lancedb.vector_dedup(
                "images.with'dot",
                column="vector'field",
                distance_threshold=4,
                dataset_version=7,
            ),
            with_no_data=with_no_data,
        )
        assert job.id == "mv-create-123"
    assert requests == [
        (
            "/v1/materialized_view/clean/create",
            {
                "query": (
                    "SELECT * FROM vector_dedup('`images.with''dot`', "
                    "7, 'vector''field', 4.0)"
                ),
                "with_no_data": with_no_data,
            },
        )
    ]
