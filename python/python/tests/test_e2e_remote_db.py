# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
from datetime import timedelta
from uuid import uuid4

import lancedb
import numpy as np
import pyarrow as pa
import pytest
from lancedb import LanceDBConnection, function
from lancedb.compaction import CompactionResult
from lancedb.index import FTS
from lancedb.prewarm import PrewarmDataResult

# TODO: setup integ test mark and script


@pytest.fixture
def maintenance_connection_options():
    uri = os.environ.get("LANCEDB_URI")
    if not uri:
        pytest.skip(
            "Set LANCEDB_URI to a server supporting SQL maintenance job results"
        )
    assert uri.startswith("db://")
    return {
        "uri": uri,
        "api_key": os.environ.get("LANCEDB_API_KEY"),
        "region": os.environ.get("LANCEDB_REGION", "us-east-1"),
        "host_override": os.environ.get("LANCEDB_HOST_OVERRIDE"),
        "sql_host_override": os.environ.get("LANCEDB_SQL_HOST_OVERRIDE"),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("operation", ["compact", "prewarm", "prewarm_legacy"])
async def test_maintenance_job_result_and_reopen_against_server(
    maintenance_connection_options, asynchronous, operation
):
    """Exercise real SQL receipts, workers and HTTP job results with matching versions.

    Configure LANCEDB_URI, LANCEDB_API_KEY and the REST/Flight SQL endpoint
    overrides for a writable database. Workers must publish maintenance results;
    an unsupported server fails the contract rather than silently skipping it.
    """
    if asynchronous:
        connection = await lancedb.connect_async(**maintenance_connection_options)
    else:
        connection = lancedb.connect(**maintenance_connection_options)

    async def call(method, *args, **kwargs):
        result = method(*args, **kwargs)
        return await result if asynchronous else result

    name = f"maintenance_{uuid4().hex}"
    created = False
    expected = [{"id": i, "payload": i * 10} for i in range(16)]
    timeout = timedelta(minutes=5)
    try:
        table = await call(
            connection.create_table, name, pa.Table.from_pylist(expected[:4])
        )
        created = True
        for start in (4, 8, 12):
            await call(table.add, pa.Table.from_pylist(expected[start : start + 4]))
        version = await table.version() if asynchronous else table.version
        if operation == "compact":
            job = await call(
                table.compact_async,
                compaction_options={
                    "target_rows_per_fragment": 100,
                    "batch_size": 128,
                    "io_buffer_size": 0,
                },
            )
        elif operation == "prewarm_legacy":
            assert (
                await call(
                    table.prewarm_data,
                    ["payload"],
                    filter="id >= 8",
                    concurrency=2,
                )
                is None
            )
            reader = await call(
                connection.execute_query, f'SHOW JOBS FOR TABLE "{name}"'
            )
            batches = await call(reader.read_all)
            jobs = pa.Table.from_batches(batches) if asynchronous else batches
            ids = [
                row["job_id"]
                for row in jobs.to_pylist()
                if row["job_type"] == "prewarm_page_cache"
            ]
            assert len(ids) == 1, jobs.to_pylist()
            job = await call(connection.open_job, ids[0])
        else:
            job = await call(
                table.prewarm_data_async,
                ["payload"],
                filter="id >= 8",
                concurrency=2,
            )
        assert job.id
        progress = []
        result = await call(job.wait, timeout=timeout, progress=progress.append)
        assert any(snapshot["state"] == "finished" for snapshot in progress)
        await call(job.refresh)
        assert await call(job.status) == "finished"
        if operation == "compact":
            assert isinstance(result, CompactionResult)
            assert job.job_type == "compact"
            assert not result.partial_commit and result.rows_failed == 0
            assert result.completion_percentage == 100
            assert 0 <= result.rows_succeeded <= len(expected)
            assert result.files_added is not None and result.files_removed is not None
        else:
            assert isinstance(result, PrewarmDataResult)
            assert job.job_type == "prewarm_page_cache"
            assert result.table_version == version == job.spec["table_version"]
            assert job.spec["columns"] == ["payload"]
            assert job.spec["concurrency"] == 2
            assert result.fragments_selected > 0
            assert result.executors_completed == result.executors_targeted
            assert result.executors_partial == 0
            assert result.bytes_read is None and result.ranges_completed is None
        reopened = await call(connection.open_job, job.id)
        assert await call(reopened.wait, timeout=timeout) == result
        query = table.query() if asynchronous else table.search()
        actual = await call(
            query.select(["id", "payload"]).limit(len(expected) + 1).to_arrow
        )
        assert sorted(actual.to_pylist(), key=lambda row: row["id"]) == expected
    finally:
        try:
            if created:
                await call(connection.drop_table, name)
        finally:
            connection.close()


@pytest.mark.skipif(
    not os.environ.get("LANCEDB_URI"), reason="Set LANCEDB_URI to a JSON FTS server"
)
def test_json_fts_against_server():
    """Requires a writable database and capacity to run the distributed indexer.

    Configure LANCEDB_URI, LANCEDB_API_KEY, and optionally LANCEDB_REGION and
    LANCEDB_HOST_OVERRIDE before running this test.
    """
    uri = os.environ["LANCEDB_URI"]
    assert uri.startswith("db://"), "LANCEDB_URI must identify a remote database"
    conn = lancedb.connect(
        uri,
        api_key=os.environ.get("LANCEDB_API_KEY"),
        region=os.environ.get("LANCEDB_REGION", "us-east-1"),
        host_override=os.environ.get("LANCEDB_HOST_OVERRIDE"),
    )
    name = f"json_fts_{uuid4().hex}"
    table = conn.create_table(
        name,
        pa.table(
            {
                "id": [0, 1, 2],
                "doc": pa.array(
                    [
                        '{"meta":{"tag":"nature"},"author":"alice"}',
                        '{"meta":{"tag":"city"},"author":"nature"}',
                        '{"meta":{"tag":"nature"},"author":"bob"}',
                    ],
                    type=pa.json_(),
                ),
                "raw": pa.array([b"nature"] * 3, type=pa.large_binary()),
            }
        ),
    )
    try:
        table.create_index("doc", config=FTS(), name="doc_fts")
        table.wait_for_index(["doc_fts"], timeout=timedelta(minutes=5))
        stats = table.index_stats("doc_fts")
        assert stats.num_indexed_rows == 3
        assert stats.num_unindexed_rows == 0
        assert stats.index_type == "FTS"
        for query, expected in [
            ("meta.tag,str,nature", [0, 2]),
            ("meta.tag,str,alice", []),
            ("meta.tag,str,tag", []),
        ]:
            search = table.search(query, query_type="fts", fts_columns="doc")
            assert sorted(row["id"] for row in search.to_list()) == expected
            plan = search.explain_plan(True)
            assert "MatchQuery" in plan, plan
            assert "FlatMatchQuery" not in plan, plan
        with pytest.raises(ValueError, match="FTS.*lance.json.*raw binary"):
            table.create_index("raw", config=FTS())
    finally:
        conn.drop_table(name)


@pytest.mark.skipif(
    not os.environ.get("LANCEDB_URI"),
    reason="Set LANCEDB_URI to a server with Python Functions enabled",
)
def test_function_execution_controls_through_sql_and_materialized_view():
    """Exercise registered batch controls through the real remote runtime.

    The Function returns the input batch length for every row. A cap of two must
    therefore produce only one- or two-element observations in both a direct
    SELECT and a materialized view refreshed after an append.
    """
    suffix = uuid4().hex[:12]
    conn = lancedb.connect(
        uri=os.environ["LANCEDB_URI"],
        api_key=os.environ.get("LANCEDB_API_KEY"),
        region=os.environ.get("LANCEDB_REGION", "us-east-1"),
        host_override=os.environ.get("LANCEDB_HOST_OVERRIDE"),
        sql_host_override=os.environ.get("LANCEDB_SQL_HOST_OVERRIDE"),
    )
    function_name = f"batch_probe_{suffix}"
    source_name = f"batch_probe_source_{suffix}"
    view_name = f"batch_probe_view_{suffix}"
    registered_version = None

    @function(
        name=function_name,
        input_schema=pa.schema([pa.field("value", pa.int64(), nullable=False)]),
        output_schema=pa.int64(),
        batch_size=2,
        row_timeout_seconds=15,
    )
    def observed_batch(value: pa.Array) -> pa.Array:
        size = len(value)
        return pa.array([size] * size, type=pa.int64())

    def row_batch_sizes(table_name):
        rows = (
            conn.execute_query(
                f"SELECT {function_name}(value) AS observed FROM {table_name}"
            )
            .read_all()
            .to_pylist()
        )
        return [row["observed"] for row in rows]

    def drop_table_if_present(table_name):
        try:
            conn.drop_table(table_name)
        except Exception:
            pass

    try:
        table = conn.create_table(
            source_name,
            pa.table(
                {"value": pa.array(range(8), type=pa.int64())},
                schema=pa.schema([pa.field("value", pa.int64(), nullable=False)]),
            ),
        )
        registered_version = conn.create_function(observed_batch, namespace_path=None)

        direct = row_batch_sizes(source_name)
        assert len(direct) == 8
        assert all(1 <= size <= 2 for size in direct)

        view = conn.create_materialized_view(
            view_name,
            source=source_name,
            select=[("observed", f"{function_name}(value)")],
        )
        initial_view = [
            row["observed"]
            for row in view.table.search().select(["observed"]).to_list()
        ]
        assert len(initial_view) == 8
        assert all(1 <= size <= 2 for size in initial_view)

        table.add(
            pa.table(
                {"value": pa.array(range(8, 12), type=pa.int64())},
                schema=pa.schema([pa.field("value", pa.int64(), nullable=False)]),
            )
        )
        view.refresh()
        refreshed = [
            row["observed"]
            for row in view.table.search().select(["observed"]).to_list()
        ]
        assert len(refreshed) == 12
        assert all(1 <= size <= 2 for size in refreshed)
    finally:
        drop_table_if_present(view_name)
        drop_table_if_present(source_name)
        if registered_version is not None:
            try:
                conn.drop_function(function_name, version=registered_version.version)
            except Exception:
                pass


@pytest.mark.skip(reason="Need to set up a local server")
def test_against_local_server():
    conn = LanceDBConnection("lancedb+http://localhost:10024")
    table = conn.open_table("sift1m_ivf1024_pq16")
    df = table.search(np.random.rand(128)).to_pandas()
    assert len(df) == 10


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_prewarm_preview_against_server(
    maintenance_connection_options, asynchronous
):
    """Preview is typed, source-versioned, and creates no maintenance jobs."""
    from lancedb.prewarm import PrewarmDataPlan

    connection = (
        await lancedb.connect_async(**maintenance_connection_options)
        if asynchronous
        else lancedb.connect(**maintenance_connection_options)
    )

    async def call(method, *args, **kwargs):
        result = method(*args, **kwargs)
        return await result if asynchronous else result

    async def jobs():
        reader = await call(connection.execute_query, f'SHOW JOBS FOR TABLE "{name}"')
        result = await call(reader.read_all)
        if asynchronous:
            return [row for batch in result for row in batch.to_pylist()]
        return result.to_pylist()

    name = f"preview_{uuid4().hex}"
    created = False
    try:
        table = await call(connection.create_table, name, [{"id": 1, "payload": "one"}])
        created = True
        version = await table.version() if asynchronous else table.version
        before = await jobs()
        for predicate, mode, selected in [
            (None, "unfiltered", 1),
            ("TRUE", "full_warm_fallback", 1),
            ("FALSE", "constant", 0),
            ("id > 10", "full_warm_fallback", 1),
        ]:
            preview = await call(
                table.explain_prewarm_data, ["payload"], filter=predicate, concurrency=2
            )
            assert isinstance(preview, PrewarmDataPlan)
            assert preview.table_version == version
            assert preview.pruning.mode == mode
            assert preview.pruning.input_fragments == 1
            assert preview.pruning.selected_fragments == selected
            if mode == "full_warm_fallback":
                assert preview.pruning.fallback_reason
        assert await jobs() == before
        assert (await table.version() if asynchronous else table.version) == version
        # A preview does not reserve the source snapshot for a later request.
        await call(table.add, [{"id": 2, "payload": "two"}])
        newer = await call(table.explain_prewarm_data)
        assert newer.table_version > version
        assert newer.pruning.input_fragments == 2
        assert await jobs() == before
    finally:
        if created:
            await call(connection.drop_table, name)
        connection.close()
