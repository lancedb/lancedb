# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pyarrow as pa
import pytest

import lancedb
from lancedb import col
from lancedb import sql_functions as F


def test_dataframe_builds_lazy_plan_from_open_table(tmp_path):
    db = lancedb.connect(tmp_path)
    db.create_table(
        "MyEvents",
        [
            {"region": "west", "amount": 10, "active": True},
            {"region": "east", "amount": 20, "active": False},
        ],
    )

    frame = (
        db.open_table("MyEvents")
        .to_df()
        .filter(col("active"))
        .aggregate(["region"], [F.sum(col("amount")).alias("total")])
        .sort(col("total").sort(ascending=False))
        .limit(10)
    )

    assert frame.schema == pa.schema(
        [pa.field("region", pa.string()), pa.field("total", pa.int64())]
    )
    assert "MyEvents" in repr(frame)

    total = (
        db.open_table("MyEvents")
        .to_df()
        .aggregate(None, F.sum(col("amount")).alias("total"))
    )
    assert total.schema.names == ["total"]
    with pytest.raises(TypeError, match="aggregates must contain Expr"):
        db.open_table("MyEvents").to_df().aggregate(None, "amount")

    assert col("amount").sort().nulls_first is False
    assert col("amount").sort(ascending=False).nulls_first is True
    assert "ASC NULLS LAST" in repr(db.open_table("MyEvents").to_df().sort("amount"))


def test_dataframe_set_operations_build_plans(tmp_path):
    db = lancedb.connect(tmp_path)
    db.create_table("left", [{"id": 1}])
    db.create_table("right", [{"id": 2}])
    left = db.open_table("left").to_df()
    right = db.open_table("right").to_df()

    for frame in [
        left.union(right),
        left.union(right, distinct=True),
        left.intersect(right),
        left.intersect(right, distinct=True),
        left.except_all(right),
        left.except_all(right, distinct=True),
    ]:
        assert frame.schema.names == ["id"]


def test_dataframe_rejects_plans_from_different_connections(tmp_path):
    first_db = lancedb.connect(tmp_path / "first")
    second_db = lancedb.connect(tmp_path / "second")
    first_db.create_table("events", [{"id": 1}])
    second_db.create_table("events", [{"id": 2}])
    first = first_db.open_table("events").to_df()
    second = second_db.open_table("events").to_df()

    with pytest.raises(ValueError, match="same connection and namespace"):
        first.join(second, on="id")
    with pytest.raises(ValueError, match="same connection and namespace"):
        first.union(second)
    with pytest.raises(ValueError, match="at least one key"):
        first.join(first, on=[])


def test_dataframe_qualified_columns_disambiguate_aliased_join(tmp_path):
    db = lancedb.connect(tmp_path)
    db.create_table("events", [{"id": 1, "value": 10}])

    source = db.open_table("events").to_df()
    left = source.alias("left").with_column_renamed("value", "renamed")
    right = source.alias("right").with_column_renamed("value", "renamed")
    joined = left.join(right, on="id").select(
        left.col("renamed").alias("left_value"),
        right.column("renamed").alias("right_value"),
    )

    assert joined.schema.names == ["left_value", "right_value"]
    assert "Join" in repr(joined)

    with pytest.raises(ValueError, match="missing"):
        source.with_column_renamed("missing", "value")
    with pytest.raises(ValueError, match="missing"):
        source.drop("missing")


def test_dataframe_executes_local_plan_in_process(tmp_path):
    db = lancedb.connect(tmp_path)
    db.create_table("events", [{"id": 1}])
    frame = db.open_table("events").to_df().select("id")

    assert frame.execute().read_all().to_pydict() == {"id": [1]}
    query = frame.execute_async()
    assert query.describe().status == "running"
    assert query.reader().read_all().to_pydict() == {"id": [1]}
    assert query.describe().status == "finished"


def test_dataframe_rejects_checked_out_versions_and_branches(tmp_path):
    db = lancedb.connect(tmp_path)
    table = db.create_table("events", [{"id": 1}])
    version = table.version
    table.add([{"id": 2}])

    table.checkout(version)
    with pytest.raises(NotImplementedError, match="checked-out versions or branches"):
        table.to_df()

    table.checkout_latest()
    assert table.to_df().schema.names == ["id"]

    branch = table.branches.create("exp")
    with pytest.raises(NotImplementedError, match="checked-out versions or branches"):
        branch.to_df()


def test_dataframe_exports_are_public():
    assert {"DataFrame", "AsyncDataFrame", "SortExpr", "sql_functions"} <= set(
        lancedb.__all__
    )


def test_sort_expr_uses_identity_equality():
    first = col("first").sort()
    second = col("second").sort()

    assert (first == second) is False
    assert len({first, second}) == 2


@pytest.mark.asyncio
async def test_async_dataframe_executes_local_plan_in_process(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    await db.create_table("events", [{"id": 1}])
    table = await db.open_table("events")
    frame = (await table.to_df()).select("id")

    assert [
        batch.to_pydict() for batch in await (await frame.execute()).read_all()
    ] == [{"id": [1]}]
    query = await frame.execute_async()
    assert (await query.describe()).status == "running"
    assert [batch.to_pydict() for batch in await (await query.reader()).read_all()] == [
        {"id": [1]}
    ]
    assert (await query.describe()).status == "finished"
