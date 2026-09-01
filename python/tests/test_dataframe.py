# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pyarrow as pa

import lancedb
from lancedb import col
from lancedb import sql_functions as F


def test_dataframe_builds_substrait_plan(tmp_path):
    db = lancedb.connect(tmp_path)
    db.create_table(
        "events",
        [
            {"region": "west", "amount": 10, "active": True},
            {"region": "east", "amount": 20, "active": False},
        ],
    )

    frame = (
        db.table("events")
        .filter(col("active"))
        .aggregate(["region"], [F.sum(col("amount")).alias("total")])
        .sort(col("total").sort(ascending=False))
        .limit(10)
    )

    assert frame.schema == pa.schema(
        [pa.field("region", pa.string()), pa.field("total", pa.int64())]
    )
    assert len(frame.to_substrait()) > 0


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
    assert all(
        call[2]["default_namespace_path"] == ["public"] for call in connection.calls
    )
