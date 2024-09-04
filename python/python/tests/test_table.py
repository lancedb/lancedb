# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import functools
from copy import copy
from datetime import date, datetime, timedelta
from pathlib import Path
from time import sleep
from typing import List
from unittest.mock import PropertyMock, patch
import os

import lance
import lancedb
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
import pytest_asyncio
from lancedb.conftest import MockTextEmbeddingFunction
from lancedb.db import AsyncConnection, LanceDBConnection
from lancedb.embeddings import EmbeddingFunctionConfig, EmbeddingFunctionRegistry
from lancedb.pydantic import LanceModel, Vector
from lancedb.table import LanceTable
from pydantic import BaseModel


class MockDB:
    def __init__(self, uri: Path):
        self.uri = str(uri)
        self.read_consistency_interval = None

    @functools.cached_property
    def is_managed_remote(self) -> bool:
        return False


@pytest.fixture
def db(tmp_path) -> MockDB:
    return MockDB(tmp_path)


@pytest_asyncio.fixture
async def db_async(tmp_path) -> AsyncConnection:
    return await lancedb.connect_async(
        tmp_path, read_consistency_interval=timedelta(seconds=0)
    )


def test_basic(db):
    ds = LanceTable.create(
        db,
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    ).to_lance()

    table = LanceTable(db, "test")
    assert table.name == "test"
    assert table.schema == ds.schema
    assert table.to_lance().to_table() == ds.to_table()


@pytest.mark.asyncio
async def test_close(db_async: AsyncConnection):
    table = await db_async.create_table("some_table", data=[{"id": 0}])
    assert table.is_open()
    table.close()
    assert not table.is_open()

    with pytest.raises(Exception, match="Table some_table is closed"):
        await table.count_rows()
    assert str(table) == "ClosedTable(some_table)"


@pytest.mark.asyncio
async def test_update_async(db_async: AsyncConnection):
    table = await db_async.create_table("some_table", data=[{"id": 0}])
    assert await table.count_rows("id == 0") == 1
    assert await table.count_rows("id == 7") == 0
    await table.update({"id": 7})
    assert await table.count_rows("id == 7") == 1
    assert await table.count_rows("id == 0") == 0
    await table.add([{"id": 2}])
    await table.update(where="id % 2 == 0", updates_sql={"id": "5"})
    assert await table.count_rows("id == 7") == 1
    assert await table.count_rows("id == 2") == 0
    assert await table.count_rows("id == 5") == 1
    await table.update({"id": 10}, where="id == 5")
    assert await table.count_rows("id == 10") == 1


def test_create_table(db):
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.string()),
            pa.field("price", pa.float32()),
        ]
    )
    expected = pa.Table.from_arrays(
        [
            pa.FixedSizeListArray.from_arrays(pa.array([3.1, 4.1, 5.9, 26.5]), 2),
            pa.array(["foo", "bar"]),
            pa.array([10.0, 20.0]),
        ],
        schema=schema,
    )
    data = [
        [
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ]
    ]
    df = pd.DataFrame(data[0])
    data.append(df)
    data.append(pa.Table.from_pandas(df, schema=schema))

    for i, d in enumerate(data):
        tbl = (
            LanceTable.create(db, f"test_{i}", data=d, schema=schema)
            .to_lance()
            .to_table()
        )
        assert expected == tbl


def test_empty_table(db):
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.string()),
            pa.field("price", pa.float32()),
        ]
    )
    tbl = LanceTable.create(db, "test", schema=schema)
    data = [
        {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
        {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
    ]
    tbl.add(data=data)


def test_add(db):
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.string()),
            pa.field("price", pa.float64()),
        ]
    )

    table = LanceTable.create(
        db,
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    )
    _add(table, schema)

    table = LanceTable.create(db, "test2", schema=schema)
    table.add(
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    )
    _add(table, schema)


def test_add_pydantic_model(db):
    # https://github.com/lancedb/lancedb/issues/562

    class Metadata(BaseModel):
        source: str
        timestamp: datetime

    class Document(BaseModel):
        content: str
        meta: Metadata

    class LanceSchema(LanceModel):
        id: str
        vector: Vector(2)
        li: List[int]
        payload: Document

    tbl = LanceTable.create(db, "mytable", schema=LanceSchema, mode="overwrite")
    assert tbl.schema == LanceSchema.to_arrow_schema()

    # add works
    expected = LanceSchema(
        id="id",
        vector=[0.0, 0.0],
        li=[1, 2, 3],
        payload=Document(
            content="foo", meta=Metadata(source="bar", timestamp=datetime.now())
        ),
    )
    tbl.add([expected])

    result = tbl.search([0.0, 0.0]).limit(1).to_pydantic(LanceSchema)[0]
    assert result == expected

    flattened = tbl.search([0.0, 0.0]).limit(1).to_pandas(flatten=1)
    assert len(flattened.columns) == 6  # _distance is automatically added

    really_flattened = tbl.search([0.0, 0.0]).limit(1).to_pandas(flatten=True)
    assert len(really_flattened.columns) == 7


@pytest.mark.asyncio
async def test_add_async(db_async: AsyncConnection):
    table = await db_async.create_table(
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    )
    assert await table.count_rows() == 2
    await table.add(
        data=[
            {"vector": [10.0, 11.0], "item": "baz", "price": 30.0},
        ],
    )
    table = await db_async.open_table("test")
    assert await table.count_rows() == 3


def test_polars(db):
    data = {
        "vector": [[3.1, 4.1], [5.9, 26.5]],
        "item": ["foo", "bar"],
        "price": [10.0, 20.0],
    }
    # Ingest polars dataframe
    table = LanceTable.create(db, "test", data=pl.DataFrame(data))
    assert len(table) == 2

    result = table.to_pandas()
    assert np.allclose(result["vector"].tolist(), data["vector"])
    assert result["item"].tolist() == data["item"]
    assert np.allclose(result["price"].tolist(), data["price"])

    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.large_string()),
            pa.field("price", pa.float64()),
        ]
    )
    assert table.schema == schema

    # search results to polars dataframe
    q = [3.1, 4.1]
    result = table.search(q).limit(1).to_polars()
    assert np.allclose(result["vector"][0], q)
    assert result["item"][0] == "foo"
    assert np.allclose(result["price"][0], 10.0)

    # enter table to polars dataframe
    result = table.to_polars()
    assert np.allclose(result.collect()["vector"].to_list(), data["vector"])

    # make sure filtering isn't broken
    filtered_result = result.filter(pl.col("item").is_in(["foo", "bar"])).collect()
    assert len(filtered_result) == 2


def _add(table, schema):
    # table = LanceTable(db, "test")
    assert len(table) == 2

    table.add([{"vector": [6.3, 100.5], "item": "new", "price": 30.0}])
    assert len(table) == 3

    expected = pa.Table.from_arrays(
        [
            pa.FixedSizeListArray.from_arrays(
                pa.array([3.1, 4.1, 5.9, 26.5, 6.3, 100.5]), 2
            ),
            pa.array(["foo", "bar", "new"]),
            pa.array([10.0, 20.0, 30.0]),
        ],
        schema=schema,
    )
    assert expected == table.to_arrow()


def test_versioning(db):
    table = LanceTable.create(
        db,
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    )

    assert len(table.list_versions()) == 2
    assert table.version == 2

    table.add([{"vector": [6.3, 100.5], "item": "new", "price": 30.0}])
    assert len(table.list_versions()) == 3
    assert table.version == 3
    assert len(table) == 3

    table.checkout(2)
    assert table.version == 2
    assert len(table) == 2


def test_create_index_method():
    with patch.object(
        LanceTable, "_dataset_mut", new_callable=PropertyMock
    ) as mock_dataset:
        # Setup mock responses
        mock_dataset.return_value.create_index.return_value = None

        # Create a LanceTable object
        connection = LanceDBConnection(uri="mock.uri")
        table = LanceTable(connection, "test_table")

        # Call the create_index method
        table.create_index(
            metric="L2",
            num_partitions=256,
            num_sub_vectors=96,
            vector_column_name="vector",
            replace=True,
            index_cache_size=256,
        )

        # Check that the _dataset.create_index method was called
        # with the right parameters
        mock_dataset.return_value.create_index.assert_called_once_with(
            column="vector",
            index_type="IVF_PQ",
            metric="L2",
            num_partitions=256,
            num_sub_vectors=96,
            replace=True,
            accelerator=None,
            index_cache_size=256,
        )


def test_add_with_nans(db):
    # by default we raise an error on bad input vectors
    bad_data = [
        {"vector": [np.nan], "item": "bar", "price": 20.0},
        {"vector": [5], "item": "bar", "price": 20.0},
        {"vector": [np.nan, np.nan], "item": "bar", "price": 20.0},
        {"vector": [np.nan, 5.0], "item": "bar", "price": 20.0},
    ]
    for row in bad_data:
        with pytest.raises(ValueError):
            LanceTable.create(
                db,
                "error_test",
                data=[{"vector": [3.1, 4.1], "item": "foo", "price": 10.0}, row],
            )

    table = LanceTable.create(
        db,
        "drop_test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [np.nan], "item": "bar", "price": 20.0},
            {"vector": [5], "item": "bar", "price": 20.0},
            {"vector": [np.nan, np.nan], "item": "bar", "price": 20.0},
        ],
        on_bad_vectors="drop",
    )
    assert len(table) == 1

    # We can fill bad input with some value
    table = LanceTable.create(
        db,
        "fill_test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [np.nan], "item": "bar", "price": 20.0},
            {"vector": [np.nan, np.nan], "item": "bar", "price": 20.0},
        ],
        on_bad_vectors="fill",
        fill_value=0.0,
    )
    assert len(table) == 3
    arrow_tbl = table.to_lance().to_table(filter="item == 'bar'")
    v = arrow_tbl["vector"].to_pylist()[0]
    assert np.allclose(v, np.array([0.0, 0.0]))


def test_restore(db):
    table = LanceTable.create(
        db,
        "my_table",
        data=[{"vector": [1.1, 0.9], "type": "vector"}],
    )
    table.add([{"vector": [0.5, 0.2], "type": "vector"}])
    table.restore(2)
    assert len(table.list_versions()) == 4
    assert len(table) == 1

    expected = table.to_arrow()
    table.checkout(2)
    table.restore()
    assert len(table.list_versions()) == 5
    assert table.to_arrow() == expected

    table.restore(5)  # latest version should be no-op
    assert len(table.list_versions()) == 5

    with pytest.raises(ValueError):
        table.restore(6)

    with pytest.raises(ValueError):
        table.restore(0)


def test_merge(db, tmp_path):
    table = LanceTable.create(
        db,
        "my_table",
        data=[{"vector": [1.1, 0.9], "id": 0}, {"vector": [1.2, 1.9], "id": 1}],
    )
    other_table = pa.table({"document": ["foo", "bar"], "id": [0, 1]})
    table.merge(other_table, left_on="id")
    assert len(table.list_versions()) == 3
    expected = pa.table(
        {"vector": [[1.1, 0.9], [1.2, 1.9]], "id": [0, 1], "document": ["foo", "bar"]},
        schema=table.schema,
    )
    assert table.to_arrow() == expected

    other_dataset = lance.write_dataset(other_table, tmp_path / "other_table.lance")
    table.restore(1)
    table.merge(other_dataset, left_on="id")


def test_delete(db):
    table = LanceTable.create(
        db,
        "my_table",
        data=[{"vector": [1.1, 0.9], "id": 0}, {"vector": [1.2, 1.9], "id": 1}],
    )
    assert len(table) == 2
    assert len(table.list_versions()) == 2
    table.delete("id=0")
    assert len(table.list_versions()) == 3
    assert table.version == 3
    assert len(table) == 1
    assert table.to_pandas()["id"].tolist() == [1]


def test_update(db):
    table = LanceTable.create(
        db,
        "my_table",
        data=[{"vector": [1.1, 0.9], "id": 0}, {"vector": [1.2, 1.9], "id": 1}],
    )
    assert len(table) == 2
    assert len(table.list_versions()) == 2
    table.update(where="id=0", values={"vector": [1.1, 1.1]})
    assert len(table.list_versions()) == 3
    assert table.version == 3
    assert len(table) == 2
    v = table.to_arrow()["vector"].combine_chunks()
    v = v.values.to_numpy().reshape(2, 2)
    assert np.allclose(v, np.array([[1.2, 1.9], [1.1, 1.1]]))


def test_update_types(db):
    table = LanceTable.create(
        db,
        "my_table",
        data=[
            {
                "id": 0,
                "str": "foo",
                "float": 1.1,
                "timestamp": datetime(2021, 1, 1),
                "date": date(2021, 1, 1),
                "vector1": [1.0, 0.0],
                "vector2": [1.0, 1.0],
                "binary": b"abc",
            }
        ],
    )
    # Update with SQL
    table.update(
        values_sql=dict(
            id="1",
            str="'bar'",
            float="2.2",
            timestamp="TIMESTAMP '2021-01-02 00:00:00'",
            date="DATE '2021-01-02'",
            vector1="[2.0, 2.0]",
            vector2="[3.0, 3.0]",
            binary="X'646566'",
        )
    )
    actual = table.to_arrow().to_pylist()[0]
    expected = dict(
        id=1,
        str="bar",
        float=2.2,
        timestamp=datetime(2021, 1, 2),
        date=date(2021, 1, 2),
        vector1=[2.0, 2.0],
        vector2=[3.0, 3.0],
        binary=b"def",
    )
    assert actual == expected

    # Update with values
    table.update(
        values=dict(
            id=2,
            str="baz",
            float=3.3,
            timestamp=datetime(2021, 1, 3),
            date=date(2021, 1, 3),
            vector1=[3.0, 3.0],
            vector2=np.array([4.0, 4.0]),
            binary=b"def",
        )
    )
    actual = table.to_arrow().to_pylist()[0]
    expected = dict(
        id=2,
        str="baz",
        float=3.3,
        timestamp=datetime(2021, 1, 3),
        date=date(2021, 1, 3),
        vector1=[3.0, 3.0],
        vector2=[4.0, 4.0],
        binary=b"def",
    )
    assert actual == expected


def test_merge_insert(db):
    table = LanceTable.create(
        db,
        "my_table",
        data=pa.table({"a": [1, 2, 3], "b": ["a", "b", "c"]}),
    )
    assert len(table) == 3
    version = table.version

    new_data = pa.table({"a": [2, 3, 4], "b": ["x", "y", "z"]})

    # upsert
    table.merge_insert(
        "a"
    ).when_matched_update_all().when_not_matched_insert_all().execute(new_data)

    expected = pa.table({"a": [1, 2, 3, 4], "b": ["a", "x", "y", "z"]})
    assert table.to_arrow().sort_by("a") == expected

    table.restore(version)

    # conditional update
    table.merge_insert("a").when_matched_update_all(where="target.b = 'b'").execute(
        new_data
    )
    expected = pa.table({"a": [1, 2, 3], "b": ["a", "x", "c"]})
    assert table.to_arrow().sort_by("a") == expected

    table.restore(version)

    # insert-if-not-exists
    table.merge_insert("a").when_not_matched_insert_all().execute(new_data)

    expected = pa.table({"a": [1, 2, 3, 4], "b": ["a", "b", "c", "z"]})
    assert table.to_arrow().sort_by("a") == expected

    table.restore(version)

    new_data = pa.table({"a": [2, 4], "b": ["x", "z"]})

    # replace-range
    table.merge_insert(
        "a"
    ).when_matched_update_all().when_not_matched_insert_all().when_not_matched_by_source_delete(
        "a > 2"
    ).execute(new_data)

    expected = pa.table({"a": [1, 2, 4], "b": ["a", "x", "z"]})
    assert table.to_arrow().sort_by("a") == expected

    table.restore(version)

    # replace-range no condition
    table.merge_insert(
        "a"
    ).when_matched_update_all().when_not_matched_insert_all().when_not_matched_by_source_delete().execute(
        new_data
    )

    expected = pa.table({"a": [2, 4], "b": ["x", "z"]})
    assert table.to_arrow().sort_by("a") == expected


def test_create_with_embedding_function(db):
    class MyTable(LanceModel):
        text: str
        vector: Vector(10)

    func = MockTextEmbeddingFunction()
    texts = ["hello world", "goodbye world", "foo bar baz fizz buzz"]
    df = pd.DataFrame({"text": texts, "vector": func.compute_source_embeddings(texts)})

    conf = EmbeddingFunctionConfig(
        source_column="text", vector_column="vector", function=func
    )
    table = LanceTable.create(
        db,
        "my_table",
        schema=MyTable,
        embedding_functions=[conf],
    )
    table.add(df)

    query_str = "hi how are you?"
    query_vector = func.compute_query_embeddings(query_str)[0]
    expected = table.search(query_vector).limit(2).to_arrow()

    actual = table.search(query_str).limit(2).to_arrow()
    assert actual == expected


def test_create_f16_table(db):
    class MyTable(LanceModel):
        text: str
        vector: Vector(128, value_type=pa.float16())

    df = pd.DataFrame(
        {
            "text": [f"s-{i}" for i in range(10000)],
            "vector": [np.random.randn(128).astype(np.float16) for _ in range(10000)],
        }
    )
    table = LanceTable.create(
        db,
        "f16_tbl",
        schema=MyTable,
    )
    table.add(df)
    table.create_index(num_partitions=2, num_sub_vectors=8)

    query = df.vector.iloc[2]
    expected = table.search(query).limit(2).to_arrow()

    assert "s-2" in expected["text"].to_pylist()


def test_add_with_embedding_function(db):
    emb = EmbeddingFunctionRegistry.get_instance().get("test")()

    class MyTable(LanceModel):
        text: str = emb.SourceField()
        vector: Vector(emb.ndims()) = emb.VectorField()

    table = LanceTable.create(db, "my_table", schema=MyTable)

    texts = ["hello world", "goodbye world", "foo bar baz fizz buzz"]
    df = pd.DataFrame({"text": texts})
    table.add(df)

    texts = ["the quick brown fox", "jumped over the lazy dog"]
    table.add([{"text": t} for t in texts])

    query_str = "hi how are you?"
    query_vector = emb.compute_query_embeddings(query_str)[0]
    expected = table.search(query_vector).limit(2).to_arrow()

    actual = table.search(query_str).limit(2).to_arrow()
    assert actual == expected


def test_multiple_vector_columns(db):
    class MyTable(LanceModel):
        text: str
        vector1: Vector(10)
        vector2: Vector(10)

    table = LanceTable.create(
        db,
        "my_table",
        schema=MyTable,
    )

    v1 = np.random.randn(10)
    v2 = np.random.randn(10)
    data = [
        {"vector1": v1, "vector2": v2, "text": "foo"},
        {"vector1": v2, "vector2": v1, "text": "bar"},
    ]
    df = pd.DataFrame(data)
    table.add(df)

    q = np.random.randn(10)
    result1 = table.search(q, vector_column_name="vector1").limit(1).to_pandas()
    result2 = table.search(q, vector_column_name="vector2").limit(1).to_pandas()

    assert result1["text"].iloc[0] != result2["text"].iloc[0]


def test_create_scalar_index(db):
    vec_array = pa.array(
        [[1, 1], [2, 2], [3, 3], [4, 4], [5, 5]], pa.list_(pa.float32(), 2)
    )
    test_data = pa.Table.from_pydict(
        {"x": ["c", "b", "a", "e", "b"], "y": [1, 2, 3, 4, 5], "vector": vec_array}
    )
    table = LanceTable.create(
        db,
        "my_table",
        data=test_data,
    )
    table.create_scalar_index("x")
    indices = table.to_lance().list_indices()
    assert len(indices) == 1
    scalar_index = indices[0]
    assert scalar_index["type"] == "BTree"

    # Confirm that prefiltering still works with the scalar index column
    results = table.search().where("x = 'c'").to_arrow()
    assert results == test_data.slice(0, 1)
    results = table.search([5, 5]).to_arrow()
    assert results["_distance"][0].as_py() == 0
    results = table.search([5, 5]).where("x != 'b'").to_arrow()
    assert results["_distance"][0].as_py() > 0


def test_empty_query(db):
    table = LanceTable.create(
        db,
        "my_table",
        data=[{"text": "foo", "id": 0}, {"text": "bar", "id": 1}],
    )
    df = table.search().select(["id"]).where("text='bar'").limit(1).to_pandas()
    val = df.id.iloc[0]
    assert val == 1

    table = LanceTable.create(db, "my_table2", data=[{"id": i} for i in range(100)])
    df = table.search().select(["id"]).to_pandas()
    assert len(df) == 10
    df = table.search().select(["id"]).limit(None).to_pandas()
    assert len(df) == 100
    df = table.search().select(["id"]).limit(-1).to_pandas()
    assert len(df) == 100


def test_search_with_schema_inf_single_vector(db):
    class MyTable(LanceModel):
        text: str
        vector_col: Vector(10)

    table = LanceTable.create(
        db,
        "my_table",
        schema=MyTable,
    )

    v1 = np.random.randn(10)
    v2 = np.random.randn(10)
    data = [
        {"vector_col": v1, "text": "foo"},
        {"vector_col": v2, "text": "bar"},
    ]
    df = pd.DataFrame(data)
    table.add(df)

    q = np.random.randn(10)
    result1 = table.search(q, vector_column_name="vector_col").limit(1).to_pandas()
    result2 = table.search(q).limit(1).to_pandas()

    assert result1["text"].iloc[0] == result2["text"].iloc[0]


def test_search_with_schema_inf_multiple_vector(db):
    class MyTable(LanceModel):
        text: str
        vector1: Vector(10)
        vector2: Vector(10)

    table = LanceTable.create(
        db,
        "my_table",
        schema=MyTable,
    )

    v1 = np.random.randn(10)
    v2 = np.random.randn(10)
    data = [
        {"vector1": v1, "vector2": v2, "text": "foo"},
        {"vector1": v2, "vector2": v1, "text": "bar"},
    ]
    df = pd.DataFrame(data)
    table.add(df)

    q = np.random.randn(10)
    with pytest.raises(ValueError):
        table.search(q).limit(1).to_pandas()


def test_compact_cleanup(db):
    table = LanceTable.create(
        db,
        "my_table",
        data=[{"text": "foo", "id": 0}, {"text": "bar", "id": 1}],
    )

    table.add([{"text": "baz", "id": 2}])
    assert len(table) == 3
    assert table.version == 3

    stats = table.compact_files()
    assert len(table) == 3
    # Compact_files bump 2 versions.
    assert table.version == 5
    assert stats.fragments_removed > 0
    assert stats.fragments_added == 1

    stats = table.cleanup_old_versions()
    assert stats.bytes_removed == 0

    stats = table.cleanup_old_versions(older_than=timedelta(0), delete_unverified=True)
    assert stats.bytes_removed > 0
    assert table.version == 5

    with pytest.raises(Exception, match="Version 3 no longer exists"):
        table.checkout(3)


def test_count_rows(db):
    table = LanceTable.create(
        db,
        "my_table",
        data=[{"text": "foo", "id": 0}, {"text": "bar", "id": 1}],
    )
    assert len(table) == 2
    assert table.count_rows() == 2
    assert table.count_rows(filter="text='bar'") == 1


def test_hybrid_search(db, tmp_path):
    # This test uses an FTS index
    pytest.importorskip("lancedb.fts")

    db = MockDB(str(tmp_path))
    # Create a LanceDB table schema with a vector and a text column
    emb = EmbeddingFunctionRegistry.get_instance().get("test")()

    class MyTable(LanceModel):
        text: str = emb.SourceField()
        vector: Vector(emb.ndims()) = emb.VectorField()

    # Initialize the table using the schema
    table = LanceTable.create(
        db,
        "my_table",
        schema=MyTable,
    )

    # Create a list of 10 unique english phrases
    phrases = [
        "great kid don't get cocky",
        "now that's a name I haven't heard in a long time",
        "if you strike me down I shall become more powerful than you imagine",
        "I find your lack of faith disturbing",
        "I've got a bad feeling about this",
        "never tell me the odds",
        "I am your father",
        "somebody has to save our skins",
        "New strategy R2 let the wookiee win",
        "Arrrrggghhhhhhh",
    ]

    # Add the phrases and vectors to the table
    table.add([{"text": p} for p in phrases])

    # Create a fts index
    table.create_fts_index("text")

    result1 = (
        table.search("Our father who art in heaven", query_type="hybrid")
        .rerank(normalize="score")
        .to_pydantic(MyTable)
    )
    result2 = (  # noqa
        table.search("Our father who art in heaven", query_type="hybrid")
        .rerank(normalize="rank")
        .to_pydantic(MyTable)
    )
    result3 = table.search(
        "Our father who art in heaven", query_type="hybrid"
    ).to_pydantic(MyTable)

    assert result1 == result3

    # with post filters
    result = (
        table.search("Arrrrggghhhhhhh", query_type="hybrid")
        .where("text='Arrrrggghhhhhhh'")
        .to_list()
    )
    len(result) == 1


@pytest.mark.parametrize(
    "consistency_interval", [None, timedelta(seconds=0), timedelta(seconds=0.1)]
)
def test_consistency(tmp_path, consistency_interval):
    db = lancedb.connect(tmp_path)
    table = LanceTable.create(db, "my_table", data=[{"id": 0}])

    db2 = lancedb.connect(tmp_path, read_consistency_interval=consistency_interval)
    table2 = db2.open_table("my_table")
    assert table2.version == table.version

    table.add([{"id": 1}])

    if consistency_interval is None:
        assert table2.version == table.version - 1
        table2.checkout_latest()
        assert table2.version == table.version
    elif consistency_interval == timedelta(seconds=0):
        assert table2.version == table.version
    else:
        # (consistency_interval == timedelta(seconds=0.1)
        assert table2.version == table.version - 1
        sleep(0.1)
        assert table2.version == table.version


def test_restore_consistency(tmp_path):
    db = lancedb.connect(tmp_path)
    table = LanceTable.create(db, "my_table", data=[{"id": 0}])

    db2 = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table2 = db2.open_table("my_table")
    assert table2.version == table.version

    # If we call checkout, it should lose consistency
    table_fixed = copy(table2)
    table_fixed.checkout(table.version)
    # But if we call checkout_latest, it should be consistent again
    table_ref_latest = copy(table_fixed)
    table_ref_latest.checkout_latest()
    table.add([{"id": 2}])
    assert table_fixed.version == table.version - 1
    assert table_ref_latest.version == table.version


# Schema evolution
def test_add_columns(tmp_path):
    db = lancedb.connect(tmp_path)
    data = pa.table({"id": [0, 1]})
    table = LanceTable.create(db, "my_table", data=data)
    table.add_columns({"new_col": "id + 2"})
    assert table.to_arrow().column_names == ["id", "new_col"]
    assert table.to_arrow()["new_col"].to_pylist() == [2, 3]


def test_alter_columns(tmp_path):
    db = lancedb.connect(tmp_path)
    data = pa.table({"id": [0, 1]})
    table = LanceTable.create(db, "my_table", data=data)
    table.alter_columns({"path": "id", "rename": "new_id"})
    assert table.to_arrow().column_names == ["new_id"]


def test_drop_columns(tmp_path):
    db = lancedb.connect(tmp_path)
    data = pa.table({"id": [0, 1], "category": ["a", "b"]})
    table = LanceTable.create(db, "my_table", data=data)
    table.drop_columns(["category"])
    assert table.to_arrow().column_names == ["id"]


@pytest.mark.asyncio
async def test_time_travel(db_async: AsyncConnection):
    # Setup
    table = await db_async.create_table("some_table", data=[{"id": 0}])
    version = await table.version()
    await table.add([{"id": 1}])
    assert await table.count_rows() == 2
    # Make sure we can rewind
    await table.checkout(version)
    assert await table.count_rows() == 1
    # Can't add data in time travel mode
    with pytest.raises(
        ValueError,
        match="table cannot be modified when a specific version is checked out",
    ):
        await table.add([{"id": 2}])
    # Can go back to normal mode
    await table.checkout_latest()
    assert await table.count_rows() == 2
    # Should be able to add data again
    await table.add([{"id": 3}])
    assert await table.count_rows() == 3
    # Now checkout and restore
    await table.checkout(version)
    await table.restore()
    assert await table.count_rows() == 1
    # Should be able to add data
    await table.add([{"id": 4}])
    assert await table.count_rows() == 2
    # Can't use restore if not checked out
    with pytest.raises(ValueError, match="checkout before running restore"):
        await table.restore()


@pytest.mark.asyncio
async def test_optimize(db_async: AsyncConnection):
    table = await db_async.create_table(
        "test",
        data=[{"x": [1]}],
    )
    await table.add(
        data=[
            {"x": [2]},
        ],
    )
    stats = await table.optimize()
    expected = (
        "OptimizeStats(compaction=CompactionStats { fragments_removed: 2, "
        "fragments_added: 1, files_removed: 2, files_added: 1 }, "
        "prune=RemovalStats { bytes_removed: 0, old_versions_removed: 0 })"
    )
    assert str(stats) == expected
    assert stats.compaction.files_removed == 2
    assert stats.compaction.files_added == 1
    assert stats.compaction.fragments_added == 1
    assert stats.compaction.fragments_removed == 2
    assert stats.prune.bytes_removed == 0
    assert stats.prune.old_versions_removed == 0

    stats = await table.optimize(cleanup_older_than=timedelta(seconds=0))
    assert stats.prune.bytes_removed > 0
    assert stats.prune.old_versions_removed == 3

    assert await table.query().to_arrow() == pa.table({"x": [[1], [2]]})


@pytest.mark.asyncio
async def test_optimize_delete_unverified(db_async: AsyncConnection, tmp_path):
    table = await db_async.create_table(
        "test",
        data=[{"x": [1]}],
    )
    await table.add(
        data=[
            {"x": [2]},
        ],
    )
    version = await table.version()
    path = tmp_path / "test.lance" / "_versions" / f"{version - 1}.manifest"
    os.remove(path)
    stats = await table.optimize(delete_unverified=False)
    assert stats.prune.old_versions_removed == 0
    stats = await table.optimize(
        cleanup_older_than=timedelta(seconds=0), delete_unverified=True
    )
    assert stats.prune.old_versions_removed == 2
