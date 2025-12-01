# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import re
from datetime import timedelta
import os

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


def test_ingest_iterator(mem_db: lancedb.DBConnection):
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
        tbl = mem_db.create_table("table2", make_batches(), schema=schema)
        tbl.to_pandas()
        assert tbl.search([3.1, 4.1]).limit(1).to_pandas()["_distance"][0] == 0.0
        assert tbl.search([5.9, 26.5]).limit(1).to_pandas()["_distance"][0] == 0.0
        tbl_len = len(tbl)
        tbl.add(make_batches())
        assert tbl_len == 50
        assert len(tbl) == tbl_len * 2
        assert len(tbl.list_versions()) == 2
        mem_db.drop_database()

    run_tests(arrow_schema)
    run_tests(PydanticSchema)


def test_table_names(tmp_db: lancedb.DBConnection):
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    tmp_db.create_table("test2", data=data)
    tmp_db.create_table("test1", data=data)
    tmp_db.create_table("test3", data=data)
    assert tmp_db.table_names() == ["test1", "test2", "test3"]

    # Test that positional arguments for page_token and limit
    result = list(tmp_db.table_names("test1", 1))  # page_token="test1", limit=1
    assert result == ["test2"], f"Expected ['test2'], got {result}"

    # Test mixed positional and keyword arguments
    result = list(tmp_db.table_names("test2", limit=2))
    assert result == ["test3"], f"Expected ['test3'], got {result}"

    # Test that namespace parameter can be passed as keyword
    result = list(tmp_db.table_names(namespace=[]))
    assert len(result) == 3


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


def test_create_mode(tmp_db: lancedb.DBConnection):
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    tmp_db.create_table("test", data=data)

    with pytest.raises(Exception):
        tmp_db.create_table("test", data=data)

    new_data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["fizz", "buzz"],
            "price": [10.0, 20.0],
        }
    )
    tbl = tmp_db.create_table("test", data=new_data, mode="overwrite")
    assert tbl.to_pandas().item.tolist() == ["fizz", "buzz"]


def test_create_table_from_iterator(mem_db: lancedb.DBConnection):
    def gen_data():
        for _ in range(10):
            yield pa.RecordBatch.from_arrays(
                [
                    pa.array([[3.1, 4.1]], pa.list_(pa.float32(), 2)),
                    pa.array(["foo"]),
                    pa.array([10.0]),
                ],
                ["vector", "item", "price"],
            )

    table = mem_db.create_table("test", data=gen_data())
    assert table.count_rows() == 10


@pytest.mark.asyncio
async def test_create_table_from_iterator_async(mem_db_async: lancedb.AsyncConnection):
    def gen_data():
        for _ in range(10):
            yield pa.RecordBatch.from_arrays(
                [
                    pa.array([[3.1, 4.1]], pa.list_(pa.float32(), 2)),
                    pa.array(["foo"]),
                    pa.array([10.0]),
                ],
                ["vector", "item", "price"],
            )

    table = await mem_db_async.create_table("test", data=gen_data())
    assert await table.count_rows() == 10


def test_create_exist_ok(tmp_db: lancedb.DBConnection):
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    tbl = tmp_db.create_table("test", data=data)

    with pytest.raises(ValueError):
        tmp_db.create_table("test", data=data)

    # open the table but don't add more rows
    tbl2 = tmp_db.create_table("test", data=data, exist_ok=True)
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
    tbl3 = tmp_db.create_table("test", schema=schema, exist_ok=True)
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
        tmp_db.create_table("test", schema=bad_schema, exist_ok=True)


@pytest.mark.asyncio
async def test_connect(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    assert str(db) == f"ListingDatabase(uri={tmp_path}, read_consistency_interval=None)"

    db = await lancedb.connect_async(
        tmp_path, read_consistency_interval=timedelta(seconds=5)
    )
    assert str(db) == f"ListingDatabase(uri={tmp_path}, read_consistency_interval=5s)"


@pytest.mark.asyncio
async def test_close(mem_db_async: lancedb.AsyncConnection):
    assert mem_db_async.is_open()
    mem_db_async.close()
    assert not mem_db_async.is_open()

    with pytest.raises(RuntimeError, match="is closed"):
        await mem_db_async.table_names()


@pytest.mark.asyncio
async def test_context_manager():
    with await lancedb.connect_async("memory://") as db:
        assert db.is_open()
    assert not db.is_open()


@pytest.mark.asyncio
async def test_create_mode_async(tmp_db_async: lancedb.AsyncConnection):
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    await tmp_db_async.create_table("test", data=data)

    with pytest.raises(ValueError, match="already exists"):
        await tmp_db_async.create_table("test", data=data)

    new_data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["fizz", "buzz"],
            "price": [10.0, 20.0],
        }
    )
    _tbl = await tmp_db_async.create_table("test", data=new_data, mode="overwrite")

    # MIGRATION: to_pandas() is not available in async
    # assert tbl.to_pandas().item.tolist() == ["fizz", "buzz"]


@pytest.mark.asyncio
async def test_create_exist_ok_async(tmp_db_async: lancedb.AsyncConnection):
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    tbl = await tmp_db_async.create_table("test", data=data)

    with pytest.raises(ValueError, match="already exists"):
        await tmp_db_async.create_table("test", data=data)

    # open the table but don't add more rows
    tbl2 = await tmp_db_async.create_table("test", data=data, exist_ok=True)
    assert tbl.name == tbl2.name
    assert await tbl.schema() == await tbl2.schema()

    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), list_size=2)),
            pa.field("item", pa.utf8()),
            pa.field("price", pa.float64()),
        ]
    )
    tbl3 = await tmp_db_async.create_table("test", schema=schema, exist_ok=True)
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


@pytest.mark.asyncio
async def test_create_table_v2_manifest_paths_async(tmp_path):
    db_with_v2_paths = await lancedb.connect_async(
        tmp_path, storage_options={"new_table_enable_v2_manifest_paths": "true"}
    )
    db_no_v2_paths = await lancedb.connect_async(
        tmp_path, storage_options={"new_table_enable_v2_manifest_paths": "false"}
    )
    # Create table in v2 mode with v2 manifest paths enabled
    tbl = await db_with_v2_paths.create_table(
        "test_v2_manifest_paths",
        data=[{"id": 0}],
    )
    assert await tbl.uses_v2_manifest_paths()
    manifests_dir = tmp_path / "test_v2_manifest_paths.lance" / "_versions"
    for manifest in os.listdir(manifests_dir):
        assert re.match(r"\d{20}\.manifest", manifest)

    # Start a table in V1 mode then migrate
    tbl = await db_no_v2_paths.create_table(
        "test_v2_migration",
        data=[{"id": 0}],
    )
    assert not await tbl.uses_v2_manifest_paths()
    manifests_dir = tmp_path / "test_v2_migration.lance" / "_versions"
    for manifest in os.listdir(manifests_dir):
        assert re.match(r"\d\.manifest", manifest)

    await tbl.migrate_manifest_paths_v2()
    assert await tbl.uses_v2_manifest_paths()

    for manifest in os.listdir(manifests_dir):
        assert re.match(r"\d{20}\.manifest", manifest)


@pytest.mark.asyncio
async def test_create_table_with_stable_row_ids_async(tmp_path):
    """Test that enable_stable_row_ids parameter works correctly."""
    import lance

    db = await lancedb.connect_async(tmp_path)

    # Create table with stable row IDs enabled
    await db.create_table(
        "with_stable",
        data=[{"id": i} for i in range(10)],
        enable_stable_row_ids=True,
    )
    # Check that stable row IDs are enabled via fragment metadata
    lance_ds_with = lance.dataset(tmp_path / "with_stable.lance")
    fragments_with = lance_ds_with.get_fragments()
    assert len(fragments_with) > 0
    assert fragments_with[0].metadata.row_id_meta is not None

    # Create table without stable row IDs (default)
    await db.create_table(
        "without_stable",
        data=[{"id": i} for i in range(10)],
        enable_stable_row_ids=False,
    )
    lance_ds_without = lance.dataset(tmp_path / "without_stable.lance")
    fragments_without = lance_ds_without.get_fragments()
    assert len(fragments_without) > 0
    assert fragments_without[0].metadata.row_id_meta is None


def test_create_table_with_stable_row_ids_sync(tmp_db: lancedb.DBConnection):
    """Test that enable_stable_row_ids parameter works correctly (sync API)."""
    # Create table with stable row IDs enabled
    tbl_with = tmp_db.create_table(
        "with_stable",
        data=[{"id": i} for i in range(10)],
        enable_stable_row_ids=True,
    )
    lance_ds_with = tbl_with.to_lance()
    fragments_with = lance_ds_with.get_fragments()
    assert len(fragments_with) > 0
    assert fragments_with[0].metadata.row_id_meta is not None

    # Create table without stable row IDs (default)
    tbl_without = tmp_db.create_table(
        "without_stable",
        data=[{"id": i} for i in range(10)],
        enable_stable_row_ids=False,
    )
    lance_ds_without = tbl_without.to_lance()
    fragments_without = lance_ds_without.get_fragments()
    assert len(fragments_without) > 0
    assert fragments_without[0].metadata.row_id_meta is None


@pytest.mark.asyncio
async def test_create_table_stable_row_ids_via_storage_options(tmp_path):
    """Test that enable_stable_row_ids can be set via storage_options at connect time."""
    import lance

    # Connect with stable row IDs enabled as default for new tables
    db_with = await lancedb.connect_async(
        tmp_path, storage_options={"new_table_enable_stable_row_ids": "true"}
    )
    # Connect without stable row IDs (default)
    db_without = await lancedb.connect_async(
        tmp_path, storage_options={"new_table_enable_stable_row_ids": "false"}
    )

    # Create table using connection with stable row IDs enabled
    await db_with.create_table(
        "with_stable_via_opts",
        data=[{"id": i} for i in range(10)],
    )
    lance_ds_with = lance.dataset(tmp_path / "with_stable_via_opts.lance")
    fragments_with = lance_ds_with.get_fragments()
    assert len(fragments_with) > 0
    assert fragments_with[0].metadata.row_id_meta is not None

    # Create table using connection without stable row IDs
    await db_without.create_table(
        "without_stable_via_opts",
        data=[{"id": i} for i in range(10)],
    )
    lance_ds_without = lance.dataset(tmp_path / "without_stable_via_opts.lance")
    fragments_without = lance_ds_without.get_fragments()
    assert len(fragments_without) > 0
    assert fragments_without[0].metadata.row_id_meta is None


def test_open_table_sync(tmp_db: lancedb.DBConnection):
    tmp_db.create_table("test", data=[{"id": 0}])
    assert tmp_db.open_table("test").count_rows() == 1
    assert tmp_db.open_table("test", index_cache_size=0).count_rows() == 1
    with pytest.raises(ValueError, match="Table 'does_not_exist' was not found"):
        tmp_db.open_table("does_not_exist")


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


def test_delete_table(tmp_db: lancedb.DBConnection):
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    tmp_db.create_table("test", data=data)

    with pytest.raises(Exception):
        tmp_db.create_table("test", data=data)

    assert tmp_db.table_names() == ["test"]

    tmp_db.drop_table("test")
    assert tmp_db.table_names() == []

    tmp_db.create_table("test", data=data)
    assert tmp_db.table_names() == ["test"]

    # dropping a table that does not exist should pass
    # if ignore_missing=True
    tmp_db.drop_table("does_not_exist", ignore_missing=True)

    tmp_db.drop_all_tables()

    assert tmp_db.table_names() == []


@pytest.mark.asyncio
async def test_delete_table_async(tmp_db: lancedb.DBConnection):
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )

    tmp_db.create_table("test", data=data)

    with pytest.raises(Exception):
        tmp_db.create_table("test", data=data)

    assert tmp_db.table_names() == ["test"]

    tmp_db.drop_table("test")
    assert tmp_db.table_names() == []

    tmp_db.create_table("test", data=data)
    assert tmp_db.table_names() == ["test"]

    tmp_db.drop_table("does_not_exist", ignore_missing=True)


def test_drop_database(tmp_db: lancedb.DBConnection):
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
    tmp_db.create_table("test", data=data)
    with pytest.raises(Exception):
        tmp_db.create_table("test", data=data)

    assert tmp_db.table_names() == ["test"]

    tmp_db.create_table("new_test", data=new_data)
    tmp_db.drop_database()
    assert tmp_db.table_names() == []

    # it should pass when no tables are present
    tmp_db.create_table("test", data=new_data)
    tmp_db.drop_table("test")
    assert tmp_db.table_names() == []
    tmp_db.drop_database()
    assert tmp_db.table_names() == []

    # creating an empty database with schema
    schema = pa.schema([pa.field("vector", pa.list_(pa.float32(), list_size=2))])
    tmp_db.create_table("empty_table", schema=schema)
    # dropping a empty database should pass
    tmp_db.drop_database()
    assert tmp_db.table_names() == []


def test_empty_or_nonexistent_table(mem_db: lancedb.DBConnection):
    with pytest.raises(Exception):
        mem_db.create_table("test_with_no_data")

    with pytest.raises(Exception):
        mem_db.open_table("does_not_exist")

    schema = pa.schema([pa.field("a", pa.int64(), nullable=False)])
    test = mem_db.create_table("test", schema=schema)

    class TestModel(LanceModel):
        a: int

    test2 = mem_db.create_table("test2", schema=TestModel)
    assert test.schema == test2.schema


@pytest.mark.asyncio
async def test_create_in_v2_mode():
    def make_data():
        for i in range(10):
            yield pa.record_batch([pa.array([x for x in range(1024)])], names=["x"])

    def make_table():
        return pa.table([pa.array([x for x in range(10 * 1024)])], names=["x"])

    schema = pa.schema([pa.field("x", pa.int64())])

    # Create table in v1 mode

    v1_db = await lancedb.connect_async(
        "memory://", storage_options={"new_table_data_storage_version": "legacy"}
    )

    tbl = await v1_db.create_table("test", data=make_data(), schema=schema)

    async def is_in_v2_mode(tbl):
        batches = (
            await tbl.query().limit(10 * 1024).to_batches(max_batch_length=1024 * 10)
        )
        num_batches = 0
        async for batch in batches:
            num_batches += 1
        return num_batches < 10

    assert not await is_in_v2_mode(tbl)

    # Create table in v2 mode
    v2_db = await lancedb.connect_async(
        "memory://", storage_options={"new_table_data_storage_version": "stable"}
    )

    tbl = await v2_db.create_table("test_v2", data=make_data(), schema=schema)

    assert await is_in_v2_mode(tbl)

    # Add data (should remain in v2 mode)
    await tbl.add(make_table())

    assert await is_in_v2_mode(tbl)

    # Create empty table in v2 mode and add data
    tbl = await v2_db.create_table("test_empty_v2", data=None, schema=schema)
    await tbl.add(make_table())

    assert await is_in_v2_mode(tbl)

    # Db uses v2 mode by default
    db = await lancedb.connect_async("memory://")

    tbl = await db.create_table("test_empty_v2_default", data=None, schema=schema)
    await tbl.add(make_table())

    assert await is_in_v2_mode(tbl)


def test_replace_index(mem_db: lancedb.DBConnection):
    table = mem_db.create_table(
        "test",
        [
            {"vector": np.random.rand(32), "item": "foo", "price": float(i)}
            for i in range(512)
        ],
    )
    table.create_index(
        num_partitions=2,
        num_sub_vectors=2,
    )

    with pytest.raises(Exception):
        table.create_index(
            num_partitions=2,
            num_sub_vectors=4,
            replace=False,
        )

    table.create_index(
        num_partitions=1,
        num_sub_vectors=2,
        replace=True,
        index_cache_size=10,
    )


def test_prefilter_with_index(mem_db: lancedb.DBConnection):
    data = [
        {"vector": np.random.rand(32), "item": "foo", "price": float(i)}
        for i in range(512)
    ]
    sample_key = data[100]["vector"]
    table = mem_db.create_table(
        "test",
        data,
    )
    table.create_index(
        num_partitions=2,
        num_sub_vectors=2,
    )
    table = (
        table.search(sample_key)
        .where("price == 500", prefilter=True)
        .limit(5)
        .to_arrow()
    )
    assert table.num_rows == 1


def test_create_table_with_invalid_names(tmp_db: lancedb.DBConnection):
    data = [{"vector": np.random.rand(128), "item": "foo"} for i in range(10)]
    with pytest.raises(ValueError):
        tmp_db.create_table("foo/bar", data)
    with pytest.raises(ValueError):
        tmp_db.create_table("foo bar", data)
    with pytest.raises(ValueError):
        tmp_db.create_table("foo$$bar", data)
    tmp_db.create_table("foo.bar", data)


def test_bypass_vector_index_sync(tmp_db: lancedb.DBConnection):
    data = [{"vector": np.random.rand(32)} for _ in range(512)]
    sample_key = data[100]["vector"]
    table = tmp_db.create_table(
        "test",
        data,
    )

    table.create_index(
        num_partitions=2,
        num_sub_vectors=2,
    )

    plan_with_index = table.search(sample_key).explain_plan(verbose=True)
    assert "ANN" in plan_with_index

    plan_without_index = (
        table.search(sample_key).bypass_vector_index().explain_plan(verbose=True)
    )
    assert "KNN" in plan_without_index


def test_local_namespace_operations(tmp_path):
    """Test that local mode namespace operations behave as expected."""
    # Create a local database connection
    db = lancedb.connect(tmp_path)

    # Test list_namespaces returns empty list for root namespace
    namespaces = list(db.list_namespaces())
    assert namespaces == []

    # Test list_namespaces with non-empty namespace raises NotImplementedError
    with pytest.raises(
        NotImplementedError,
        match="Namespace operations are not supported for listing database",
    ):
        list(db.list_namespaces(namespace=["test"]))


def test_local_create_namespace_not_supported(tmp_path):
    """Test that create_namespace is not supported in local mode."""
    db = lancedb.connect(tmp_path)

    with pytest.raises(
        NotImplementedError,
        match="Namespace operations are not supported for listing database",
    ):
        db.create_namespace(["test_namespace"])


def test_local_drop_namespace_not_supported(tmp_path):
    """Test that drop_namespace is not supported in local mode."""
    db = lancedb.connect(tmp_path)

    with pytest.raises(
        NotImplementedError,
        match="Namespace operations are not supported for listing database",
    ):
        db.drop_namespace(["test_namespace"])


def test_clone_table_latest_version(tmp_path):
    """Test cloning a table with the latest version (default behavior)"""
    import os

    db = lancedb.connect(tmp_path)

    # Create source table with some data
    data = [
        {"id": 1, "text": "hello", "vector": [1.0, 2.0]},
        {"id": 2, "text": "world", "vector": [3.0, 4.0]},
    ]
    source_table = db.create_table("source", data=data)

    # Add more data to create a new version
    more_data = [{"id": 3, "text": "test", "vector": [5.0, 6.0]}]
    source_table.add(more_data)

    # Clone the table (should get latest version with 3 rows)
    source_uri = os.path.join(tmp_path, "source.lance")
    cloned_table = db.clone_table("cloned", source_uri)

    # Verify cloned table has all 3 rows
    assert cloned_table.count_rows() == 3
    assert "cloned" in db.table_names()

    # Verify data matches
    cloned_data = cloned_table.to_pandas()
    assert len(cloned_data) == 3
    assert set(cloned_data["id"].tolist()) == {1, 2, 3}


def test_clone_table_specific_version(tmp_path):
    """Test cloning a table from a specific version"""
    import os

    db = lancedb.connect(tmp_path)

    # Create source table with initial data
    data = [
        {"id": 1, "text": "hello", "vector": [1.0, 2.0]},
        {"id": 2, "text": "world", "vector": [3.0, 4.0]},
    ]
    source_table = db.create_table("source", data=data)

    # Get the initial version
    initial_version = source_table.version

    # Add more data to create a new version
    more_data = [{"id": 3, "text": "test", "vector": [5.0, 6.0]}]
    source_table.add(more_data)

    # Verify source now has 3 rows
    assert source_table.count_rows() == 3

    # Clone from the initial version (should have only 2 rows)
    source_uri = os.path.join(tmp_path, "source.lance")
    cloned_table = db.clone_table("cloned", source_uri, source_version=initial_version)

    # Verify cloned table has only the initial 2 rows
    assert cloned_table.count_rows() == 2
    cloned_data = cloned_table.to_pandas()
    assert set(cloned_data["id"].tolist()) == {1, 2}


def test_clone_table_with_tag(tmp_path):
    """Test cloning a table from a tagged version"""
    import os

    db = lancedb.connect(tmp_path)

    # Create source table with initial data
    data = [
        {"id": 1, "text": "hello", "vector": [1.0, 2.0]},
        {"id": 2, "text": "world", "vector": [3.0, 4.0]},
    ]
    source_table = db.create_table("source", data=data)

    # Create a tag for the current version
    source_table.tags.create("v1.0", source_table.version)

    # Add more data after the tag
    more_data = [{"id": 3, "text": "test", "vector": [5.0, 6.0]}]
    source_table.add(more_data)

    # Verify source now has 3 rows
    assert source_table.count_rows() == 3

    # Clone from the tagged version (should have only 2 rows)
    source_uri = os.path.join(tmp_path, "source.lance")
    cloned_table = db.clone_table("cloned", source_uri, source_tag="v1.0")

    # Verify cloned table has only the tagged version's 2 rows
    assert cloned_table.count_rows() == 2
    cloned_data = cloned_table.to_pandas()
    assert set(cloned_data["id"].tolist()) == {1, 2}


def test_clone_table_deep_clone_fails(tmp_path):
    """Test that deep clone raises an unsupported error"""
    import os

    db = lancedb.connect(tmp_path)

    # Create source table with some data
    data = [
        {"id": 1, "text": "hello", "vector": [1.0, 2.0]},
        {"id": 2, "text": "world", "vector": [3.0, 4.0]},
    ]
    db.create_table("source", data=data)

    # Try to create a deep clone (should fail)
    source_uri = os.path.join(tmp_path, "source.lance")
    with pytest.raises(Exception, match="Deep clone is not yet implemented"):
        db.clone_table("cloned", source_uri, is_shallow=False)
