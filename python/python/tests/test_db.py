#  Copyright 2023 LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import re
from datetime import timedelta

import lancedb
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from lancedb.pydantic import LanceModel, Vector


@pytest.mark.parametrize("use_tantivy", [True, False])
def test_basic(tmp_path, use_tantivy):
    db = lancedb.connect(tmp_path)

    assert db.uri == str(tmp_path)
    assert db.table_names() == []

    class SimpleModel(LanceModel):
        item: str
        price: float
        vector: Vector(2)

    table = db.create_table(
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
        schema=SimpleModel,
    )

    with pytest.raises(
        ValueError, match="Cannot add a single LanceModel to a table. Use a list."
    ):
        table.add(SimpleModel(item="baz", price=30.0, vector=[1.0, 2.0]))

    rs = table.search([100, 100]).limit(1).to_pandas()
    assert len(rs) == 1
    assert rs["item"].iloc[0] == "bar"

    rs = table.search([100, 100]).where("price < 15").limit(2).to_pandas()
    assert len(rs) == 1
    assert rs["item"].iloc[0] == "foo"

    table.create_fts_index("item", use_tantivy=use_tantivy)
    rs = table.search("bar", query_type="fts").to_pandas()
    assert len(rs) == 1
    assert rs["item"].iloc[0] == "bar"

    assert db.table_names() == ["test"]
    assert "test" in db
    assert len(db) == 1

    assert db.open_table("test").name == db["test"].name


def test_ingest_pd(tmp_path):
    db = lancedb.connect(tmp_path)

    assert db.uri == str(tmp_path)
    assert db.table_names() == []

    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    table = db.create_table("test", data=data)
    rs = table.search([100, 100]).limit(1).to_pandas()
    assert len(rs) == 1
    assert rs["item"].iloc[0] == "bar"

    rs = table.search([100, 100]).where("price < 15").limit(2).to_pandas()
    assert len(rs) == 1
    assert rs["item"].iloc[0] == "foo"

    assert db.table_names() == ["test"]
    assert "test" in db
    assert len(db) == 1

    assert db.open_table("test").name == db["test"].name


def test_ingest_iterator(tmp_path):
    class PydanticSchema(LanceModel):
        vector: Vector(2)
        item: str
        price: float

    arrow_schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.utf8()),
            pa.field("price", pa.float32()),
        ]
    )

    def make_batches():
        for _ in range(5):
            yield from [
                # pandas
                pd.DataFrame(
                    {
                        "vector": [[3.1, 4.1], [1, 1]],
                        "item": ["foo", "bar"],
                        "price": [10.0, 20.0],
                    }
                ),
                # pylist
                [
                    {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
                    {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
                ],
                # recordbatch
                pa.RecordBatch.from_arrays(
                    [
                        pa.array([[3.1, 4.1], [5.9, 26.5]], pa.list_(pa.float32(), 2)),
                        pa.array(["foo", "bar"]),
                        pa.array([10.0, 20.0]),
                    ],
                    ["vector", "item", "price"],
                ),
                # pa Table
                pa.Table.from_arrays(
                    [
                        pa.array([[3.1, 4.1], [5.9, 26.5]], pa.list_(pa.float32(), 2)),
                        pa.array(["foo", "bar"]),
                        pa.array([10.0, 20.0]),
                    ],
                    ["vector", "item", "price"],
                ),
                # pydantic list
                [
                    PydanticSchema(vector=[3.1, 4.1], item="foo", price=10.0),
                    PydanticSchema(vector=[5.9, 26.5], item="bar", price=20.0),
                ],
                # TODO: test pydict separately. it is unique column number and
                # name constraints
            ]

    def run_tests(schema):
        db = lancedb.connect(tmp_path)
        tbl = db.create_table("table2", make_batches(), schema=schema, mode="overwrite")
        tbl.to_pandas()
        assert tbl.search([3.1, 4.1]).limit(1).to_pandas()["_distance"][0] == 0.0
        assert tbl.search([5.9, 26.5]).limit(1).to_pandas()["_distance"][0] == 0.0
        tbl_len = len(tbl)
        tbl.add(make_batches())
        assert tbl_len == 50
        assert len(tbl) == tbl_len * 2
        assert len(tbl.list_versions()) == 3
        db.drop_database()

    run_tests(arrow_schema)
    run_tests(PydanticSchema)


def test_table_names(tmp_path):
    db = lancedb.connect(tmp_path)
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    db.create_table("test2", data=data)
    db.create_table("test1", data=data)
    db.create_table("test3", data=data)
    assert db.table_names() == ["test1", "test2", "test3"]


@pytest.mark.asyncio
async def test_table_names_async(tmp_path):
    db = lancedb.connect(tmp_path)
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    db.create_table("test2", data=data)
    db.create_table("test1", data=data)
    db.create_table("test3", data=data)

    db = await lancedb.connect_async(tmp_path)
    assert await db.table_names() == ["test1", "test2", "test3"]

    assert await db.table_names(limit=1) == ["test1"]
    assert await db.table_names(start_after="test1", limit=1) == ["test2"]
    assert await db.table_names(start_after="test1") == ["test2", "test3"]


def test_create_mode(tmp_path):
    db = lancedb.connect(tmp_path)
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    db.create_table("test", data=data)

    with pytest.raises(Exception):
        db.create_table("test", data=data)

    new_data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["fizz", "buzz"],
            "price": [10.0, 20.0],
        }
    )
    tbl = db.create_table("test", data=new_data, mode="overwrite")
    assert tbl.to_pandas().item.tolist() == ["fizz", "buzz"]


def test_create_exist_ok(tmp_path):
    db = lancedb.connect(tmp_path)
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    tbl = db.create_table("test", data=data)

    with pytest.raises(OSError):
        db.create_table("test", data=data)

    # open the table but don't add more rows
    tbl2 = db.create_table("test", data=data, exist_ok=True)
    assert tbl.name == tbl2.name
    assert tbl.schema == tbl2.schema
    assert len(tbl) == len(tbl2)

    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), list_size=2)),
            pa.field("item", pa.utf8()),
            pa.field("price", pa.float64()),
        ]
    )
    tbl3 = db.create_table("test", schema=schema, exist_ok=True)
    assert tbl3.schema == schema

    bad_schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), list_size=2)),
            pa.field("item", pa.utf8()),
            pa.field("price", pa.float64()),
            pa.field("extra", pa.float32()),
        ]
    )
    with pytest.raises(ValueError):
        db.create_table("test", schema=bad_schema, exist_ok=True)


@pytest.mark.asyncio
async def test_connect(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    assert str(db) == f"NativeDatabase(uri={tmp_path}, read_consistency_interval=None)"

    db = await lancedb.connect_async(
        tmp_path, read_consistency_interval=timedelta(seconds=5)
    )
    assert str(db) == f"NativeDatabase(uri={tmp_path}, read_consistency_interval=5s)"


@pytest.mark.asyncio
async def test_close(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    assert db.is_open()
    db.close()
    assert not db.is_open()

    with pytest.raises(RuntimeError, match="is closed"):
        await db.table_names()


@pytest.mark.asyncio
async def test_context_manager(tmp_path):
    with await lancedb.connect_async(tmp_path) as db:
        assert db.is_open()
    assert not db.is_open()


@pytest.mark.asyncio
async def test_create_mode_async(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    await db.create_table("test", data=data)

    with pytest.raises(RuntimeError):
        await db.create_table("test", data=data)

    new_data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["fizz", "buzz"],
            "price": [10.0, 20.0],
        }
    )
    _tbl = await db.create_table("test", data=new_data, mode="overwrite")

    # MIGRATION: to_pandas() is not available in async
    # assert tbl.to_pandas().item.tolist() == ["fizz", "buzz"]


@pytest.mark.asyncio
async def test_create_exist_ok_async(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    tbl = await db.create_table("test", data=data)

    with pytest.raises(RuntimeError):
        await db.create_table("test", data=data)

    # open the table but don't add more rows
    tbl2 = await db.create_table("test", data=data, exist_ok=True)
    assert tbl.name == tbl2.name
    assert await tbl.schema() == await tbl2.schema()

    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), list_size=2)),
            pa.field("item", pa.utf8()),
            pa.field("price", pa.float64()),
        ]
    )
    tbl3 = await db.create_table("test", schema=schema, exist_ok=True)
    assert await tbl3.schema() == schema

    # Migration: When creating a table, but the table already exists, but
    # the schema is different, it should raise an error.
    # bad_schema = pa.schema(
    #     [
    #         pa.field("vector", pa.list_(pa.float32(), list_size=2)),
    #         pa.field("item", pa.utf8()),
    #         pa.field("price", pa.float64()),
    #         pa.field("extra", pa.float32()),
    #     ]
    # )
    # with pytest.raises(ValueError):
    #     await db.create_table("test", schema=bad_schema, exist_ok=True)


def test_open_table_sync(tmp_path):
    db = lancedb.connect(tmp_path)
    db.create_table("test", data=[{"id": 0}])
    assert db.open_table("test").count_rows() == 1
    assert db.open_table("test", index_cache_size=0).count_rows() == 1
    with pytest.raises(FileNotFoundError, match="does not exist"):
        db.open_table("does_not_exist")


@pytest.mark.asyncio
async def test_open_table(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    await db.create_table("test", data=data)

    tbl = await db.open_table("test")
    assert tbl.name == "test"
    assert (
        re.search(
            r"NativeTable\(test, uri=.*test\.lance, read_consistency_interval=None\)",
            str(tbl),
        )
        is not None
    )
    assert await tbl.schema() == pa.schema(
        {
            "vector": pa.list_(pa.float32(), list_size=2),
            "item": pa.utf8(),
            "price": pa.float64(),
        }
    )

    # No way to verify this yet, but at least make sure we
    # can pass the parameter
    await db.open_table("test", index_cache_size=0)

    with pytest.raises(ValueError, match="was not found"):
        await db.open_table("does_not_exist")


def test_delete_table(tmp_path):
    db = lancedb.connect(tmp_path)
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    db.create_table("test", data=data)

    with pytest.raises(Exception):
        db.create_table("test", data=data)

    assert db.table_names() == ["test"]

    db.drop_table("test")
    assert db.table_names() == []

    db.create_table("test", data=data)
    assert db.table_names() == ["test"]

    # dropping a table that does not exist should pass
    # if ignore_missing=True
    db.drop_table("does_not_exist", ignore_missing=True)


def test_drop_database(tmp_path):
    db = lancedb.connect(tmp_path)
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    new_data = pd.DataFrame(
        {
            "vector": [[5.1, 4.1], [5.9, 10.5]],
            "item": ["kiwi", "avocado"],
            "price": [12.0, 17.0],
        }
    )
    db.create_table("test", data=data)
    with pytest.raises(Exception):
        db.create_table("test", data=data)

    assert db.table_names() == ["test"]

    db.create_table("new_test", data=new_data)
    db.drop_database()
    assert db.table_names() == []

    # it should pass when no tables are present
    db.create_table("test", data=new_data)
    db.drop_table("test")
    assert db.table_names() == []
    db.drop_database()
    assert db.table_names() == []

    # creating an empty database with schema
    schema = pa.schema([pa.field("vector", pa.list_(pa.float32(), list_size=2))])
    db.create_table("empty_table", schema=schema)
    # dropping a empty database should pass
    db.drop_database()
    assert db.table_names() == []


def test_empty_or_nonexistent_table(tmp_path):
    db = lancedb.connect(tmp_path)
    with pytest.raises(Exception):
        db.create_table("test_with_no_data")

    with pytest.raises(Exception):
        db.open_table("does_not_exist")

    schema = pa.schema([pa.field("a", pa.int64(), nullable=False)])
    test = db.create_table("test", schema=schema)

    class TestModel(LanceModel):
        a: int

    test2 = db.create_table("test2", schema=TestModel)
    assert test.schema == test2.schema


@pytest.mark.asyncio
async def test_create_in_v2_mode(tmp_path):
    def make_data():
        for i in range(10):
            yield pa.record_batch([pa.array([x for x in range(1024)])], names=["x"])

    def make_table():
        return pa.table([pa.array([x for x in range(10 * 1024)])], names=["x"])

    schema = pa.schema([pa.field("x", pa.int64())])

    db = await lancedb.connect_async(tmp_path)

    # Create table in v1 mode
    tbl = await db.create_table("test", data=make_data(), schema=schema)

    async def is_in_v2_mode(tbl):
        batches = await tbl.query().to_batches(max_batch_length=1024 * 10)
        num_batches = 0
        async for batch in batches:
            num_batches += 1
        return num_batches < 10

    assert not await is_in_v2_mode(tbl)

    # Create table in v2 mode
    tbl = await db.create_table(
        "test_v2", data=make_data(), schema=schema, use_legacy_format=False
    )

    assert await is_in_v2_mode(tbl)

    # Add data (should remain in v2 mode)
    await tbl.add(make_table())

    assert await is_in_v2_mode(tbl)

    # Create empty table in v2 mode and add data
    tbl = await db.create_table(
        "test_empty_v2", data=None, schema=schema, use_legacy_format=False
    )
    await tbl.add(make_table())

    assert await is_in_v2_mode(tbl)

    # Create empty table uses v1 mode by default
    tbl = await db.create_table("test_empty_v2_default", data=None, schema=schema)
    await tbl.add(make_table())

    assert not await is_in_v2_mode(tbl)


def test_replace_index(tmp_path):
    db = lancedb.connect(uri=tmp_path)
    table = db.create_table(
        "test",
        [
            {"vector": np.random.rand(128), "item": "foo", "price": float(i)}
            for i in range(1000)
        ],
    )
    table.create_index(
        num_partitions=2,
        num_sub_vectors=4,
    )

    with pytest.raises(Exception):
        table.create_index(
            num_partitions=2,
            num_sub_vectors=4,
            replace=False,
        )

    table.create_index(
        num_partitions=2,
        num_sub_vectors=4,
        replace=True,
        index_cache_size=10,
    )


def test_prefilter_with_index(tmp_path):
    db = lancedb.connect(uri=tmp_path)
    data = [
        {"vector": np.random.rand(128), "item": "foo", "price": float(i)}
        for i in range(1000)
    ]
    sample_key = data[100]["vector"]
    table = db.create_table(
        "test",
        data,
    )
    table.create_index(
        num_partitions=2,
        num_sub_vectors=4,
    )
    table = (
        table.search(sample_key)
        .where("price == 500", prefilter=True)
        .limit(5)
        .to_arrow()
    )
    assert table.num_rows == 1


def test_create_table_with_invalid_names(tmp_path):
    db = lancedb.connect(uri=tmp_path)
    data = [{"vector": np.random.rand(128), "item": "foo"} for i in range(10)]
    with pytest.raises(ValueError):
        db.create_table("foo/bar", data)
    with pytest.raises(ValueError):
        db.create_table("foo bar", data)
    with pytest.raises(ValueError):
        db.create_table("foo$$bar", data)
    db.create_table("foo.bar", data)
