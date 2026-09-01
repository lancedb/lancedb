# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json

import pyarrow as pa
import pytest
from datafusion.substrait import Serde

import lancedb
from lancedb import col
from lancedb import sql_functions as F
from lancedb.dataframe import AsyncDataFrame


def test_dataframe_builds_substrait_plan(tmp_path):
    db = lancedb.connect(tmp_path)
    db.create_table(
        "MyEvents",
        [
            {"region": "west", "amount": 10, "active": True},
            {"region": "east", "amount": 20, "active": False},
        ],
    )

    frame = (
        db.table("MyEvents")
        .filter(col("active"))
        .aggregate(["region"], [F.sum(col("amount")).alias("total")])
        .sort(col("total").sort(ascending=False))
        .limit(10)
    )

    assert frame.schema == pa.schema(
        [pa.field("region", pa.string()), pa.field("total", pa.int64())]
    )
    serialized = frame.to_substrait()
    assert len(serialized) > 0
    plan_json = json.loads(Serde.deserialize_bytes(serialized).to_json())
    assert "MyEvents" in str(plan_json)

    total = db.table("MyEvents").aggregate(None, F.sum(col("amount")).alias("total"))
    assert total.schema.names == ["total"]


def test_dataframe_set_operations_build_plans(tmp_path):
    db = lancedb.connect(tmp_path)
    db.create_table("left", [{"id": 1}])
    db.create_table("right", [{"id": 2}])
    left = db.table("left")
    right = db.table("right")

    for frame in [
        left.union(right),
        left.union(right, distinct=True),
        left.intersect(right),
        left.intersect(right, distinct=True),
        left.except_all(right),
        left.except_all(right, distinct=True),
    ]:
        assert frame.to_substrait()


def test_dataframe_direct_execution_uses_connection(tmp_path):
    db = lancedb.connect(tmp_path)
    db.create_table("events", [{"id": 1}])
    frame = db.table("events").select("id")

    class RecordingConnection:
        def __init__(self):
            self.calls = []

        def execute_substrait(self, plan, **kwargs):
            self.calls.append(("execute", plan, kwargs))
            return "reader"

        def execute_substrait_async(self, plan, **kwargs):
            self.calls.append(("execute_async", plan, kwargs))
            return "query"

    connection = RecordingConnection()
    frame._connection = connection

    assert frame.execute() == "reader"
    assert frame.execute_async() == "query"
    assert [call[0] for call in connection.calls] == ["execute", "execute_async"]
    assert all(call[2]["version"] for call in connection.calls)
    assert all(
        call[2]["default_namespace_path"] == ["public"] for call in connection.calls
    )


@pytest.mark.asyncio
async def test_async_dataframe_direct_execution(tmp_path):
    db = lancedb.connect(tmp_path)
    db.create_table("events", [{"id": 1}])
    sync_frame = db.table("events")

    class RecordingAsyncConnection:
        async def execute_substrait(self, plan, **kwargs):
            return "reader"

        async def execute_substrait_async(self, plan, **kwargs):
            return "query"

    frame = AsyncDataFrame(RecordingAsyncConnection(), sync_frame._inner, ["public"])
    assert await frame.execute() == "reader"
    assert await frame.execute_async() == "query"
