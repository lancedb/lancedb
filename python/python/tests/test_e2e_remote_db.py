# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
from datetime import timedelta
from uuid import uuid4

import lancedb
import numpy as np
import pyarrow as pa
import pytest
from lancedb import LanceDBConnection, udf
from lancedb.index import FTS

# TODO: setup integ test mark and script


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

    @udf(
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
