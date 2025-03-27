# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
from datetime import date, datetime, timedelta
from time import sleep
from typing import List
from unittest.mock import patch

import lancedb
from lancedb.index import HnswPq, HnswSq, IvfPq
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset
import pytest
from lancedb.conftest import MockTextEmbeddingFunction
from lancedb.db import AsyncConnection, DBConnection
from lancedb.embeddings import EmbeddingFunctionConfig, EmbeddingFunctionRegistry
from lancedb.pydantic import LanceModel, Vector
from lancedb.table import LanceTable
from pydantic import BaseModel


def test_basic(mem_db: DBConnection):
    data = [
        {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
        {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
    ]
    table = mem_db.create_table("test", data=data)

    assert table.name == "test"
    assert "LanceTable(name='test', version=1, _conn=LanceDBConnection(" in repr(table)
    expected_schema = pa.schema(
        {
            "vector": pa.list_(pa.float32(), 2),
            "item": pa.string(),
            "price": pa.float64(),
        }
    )
    assert table.schema == expected_schema

    expected_data = pa.Table.from_pylist(data, schema=expected_schema)
    assert table.to_arrow() == expected_data


def test_input_data_type(mem_db: DBConnection, tmp_path):
    schema = pa.schema(
        {
            "id": pa.int64(),
            "name": pa.string(),
            "age": pa.int32(),
        }
    )

    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 35, 40, 45],
    }
    record_batch = pa.RecordBatch.from_pydict(data, schema=schema)
    pa_reader = pa.RecordBatchReader.from_batches(record_batch.schema, [record_batch])
    pa_table = pa.Table.from_batches([record_batch])

    def create_dataset(tmp_path):
        path = os.path.join(tmp_path, "test_source_dataset")
        pa.dataset.write_dataset(pa_table, path, format="parquet")
        return pa.dataset.dataset(path, format="parquet")

    pa_dataset = create_dataset(tmp_path)
    pa_scanner = pa_dataset.scanner()

    input_types = [
        ("RecordBatchReader", pa_reader),
        ("RecordBatch", record_batch),
        ("Table", pa_table),
        ("Dataset", pa_dataset),
        ("Scanner", pa_scanner),
    ]
    for input_type, input_data in input_types:
        table_name = f"test_{input_type.lower()}"
        table = mem_db.create_table(table_name, data=input_data)
        assert table.schema == schema
        assert table.count_rows() == 5

        assert table.schema == schema
        assert table.to_arrow() == pa_table


@pytest.mark.asyncio
async def test_close(mem_db_async: AsyncConnection):
    table = await mem_db_async.create_table("some_table", data=[{"id": 0}])
    assert table.is_open()
    table.close()
    assert not table.is_open()

    with pytest.raises(Exception, match="Table some_table is closed"):
        await table.count_rows()
    assert str(table) == "ClosedTable(some_table)"


@pytest.mark.asyncio
async def test_update_async(mem_db_async: AsyncConnection):
    table = await mem_db_async.create_table("some_table", data=[{"id": 0}])
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


def test_create_table(mem_db: DBConnection):
    schema = pa.schema(
        {
            "vector": pa.list_(pa.float32(), 2),
            "item": pa.string(),
            "price": pa.float64(),
        }
    )
    expected = pa.table(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        },
        schema=schema,
    )
    rows = [
        {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
        {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
    ]
    df = pd.DataFrame(rows)
    pa_table = pa.Table.from_pandas(df, schema=schema)
    data = [
        ("Rows", rows),
        ("pd_DataFrame", df),
        ("pa_Table", pa_table),
    ]

    for name, d in data:
        tbl = mem_db.create_table(name, data=d, schema=schema).to_arrow()
        assert expected == tbl


def test_empty_table(mem_db: DBConnection):
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.string()),
            pa.field("price", pa.float32()),
        ]
    )
    tbl = mem_db.create_table("test", schema=schema)
    data = [
        {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
        {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
    ]
    tbl.add(data=data)


def test_add_dictionary(mem_db: DBConnection):
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.string()),
            pa.field("price", pa.float32()),
        ]
    )
    tbl = mem_db.create_table("test", schema=schema)
    data = {"vector": [3.1, 4.1], "item": "foo", "price": 10.0}
    with pytest.raises(ValueError) as excep_info:
        tbl.add(data=data)
    assert (
        str(excep_info.value)
        == "Cannot add a single dictionary to a table. Use a list."
    )


def test_add(mem_db: DBConnection):
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.string()),
            pa.field("price", pa.float64()),
        ]
    )

    def _add(table, schema):
        assert len(table) == 2

        table.add([{"vector": [6.3, 100.5], "item": "new", "price": 30.0}])
        assert len(table) == 3

        expected = pa.table(
            {
                "vector": [[3.1, 4.1], [5.9, 26.5], [6.3, 100.5]],
                "item": ["foo", "bar", "new"],
                "price": [10.0, 20.0, 30.0],
            },
            schema=schema,
        )
        assert expected == table.to_arrow()

    # Append to table created with data
    table = mem_db.create_table(
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    )
    _add(table, schema)

    # Append to table created empty with schema
    table = mem_db.create_table("test2", schema=schema)
    table.add(
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    )
    _add(table, schema)


def test_add_struct(mem_db: DBConnection):
    # https://github.com/lancedb/lancedb/issues/2114
    schema = pa.schema(
        [
            (
                "stuff",
                pa.struct(
                    [
                        ("b", pa.int64()),
                        ("a", pa.int64()),
                        # TODO: also test subset of nested.
                    ]
                ),
            )
        ]
    )

    # Create test data with fields in same order
    data = [{"stuff": {"b": 1, "a": 2}}]
    # pa.Table.from_pylist() will reorder the fields. We need to make sure
    # we fix the field order later, before casting.
    table = mem_db.create_table("test", schema=schema)
    table.add(data)

    data = [{"stuff": {"b": 4}}]
    table.add(data)

    expected = pa.table(
        {
            "stuff": [{"b": 1, "a": 2}, {"b": 4, "a": None}],
        },
        schema=schema,
    )
    assert table.to_arrow() == expected

    # Also check struct in list
    schema = pa.schema(
        {
            "s_list": pa.list_(
                pa.struct(
                    [
                        ("b", pa.int64()),
                        ("a", pa.int64()),
                    ]
                )
            )
        }
    )
    data = [{"s_list": [{"b": 1, "a": 2}, {"b": 4}]}]
    table = mem_db.create_table("test", schema=schema)
    table.add(data)


def test_add_subschema(mem_db: DBConnection):
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2), nullable=True),
            pa.field("item", pa.string(), nullable=True),
            pa.field("price", pa.float64(), nullable=False),
        ]
    )
    table = mem_db.create_table("test", schema=schema)

    data = {"price": 10.0, "item": "foo"}
    table.add([data])
    data = pd.DataFrame({"price": [2.0], "vector": [[3.1, 4.1]]})
    table.add(data)
    data = {"price": 3.0, "vector": [5.9, 26.5], "item": "bar"}
    table.add([data])

    expected = pa.table(
        {
            "vector": [None, [3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", None, "bar"],
            "price": [10.0, 2.0, 3.0],
        },
        schema=schema,
    )
    assert table.to_arrow() == expected

    data = {"item": "foo"}
    # We can't omit a column if it's not nullable
    with pytest.raises(RuntimeError, match="Append with different schema"):
        table.add([data])

    # We can add it if we make the column nullable
    table.alter_columns(dict(path="price", nullable=True))
    table.add([data])

    expected_schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2), nullable=True),
            pa.field("item", pa.string(), nullable=True),
            pa.field("price", pa.float64(), nullable=True),
        ]
    )
    expected = pa.table(
        {
            "vector": [None, [3.1, 4.1], [5.9, 26.5], None],
            "item": ["foo", None, "bar", "foo"],
            "price": [10.0, 2.0, 3.0, None],
        },
        schema=expected_schema,
    )
    assert table.to_arrow() == expected


def test_add_nullability(mem_db: DBConnection):
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2), nullable=False),
            pa.field("id", pa.string(), nullable=False),
        ]
    )
    table = mem_db.create_table("test", schema=schema)
    assert table.schema.field("vector").nullable is False

    nullable_schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2), nullable=True),
            pa.field("id", pa.string(), nullable=True),
        ]
    )
    data = pa.table(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "id": ["foo", "bar"],
        },
        schema=nullable_schema,
    )
    # We can add nullable schema if it doesn't actually contain nulls
    table.add(data)

    expected = data.cast(schema)
    assert table.to_arrow() == expected

    data = pa.table(
        {
            "vector": [None],
            "id": ["baz"],
        },
        schema=nullable_schema,
    )
    # We can't add nullable schema if it contains nulls
    with pytest.raises(
        Exception,
        match=(
            "The field `vector` contained null values even though "
            "the field is marked non-null in the schema"
        ),
    ):
        table.add(data)

    # But we can make it nullable
    table.alter_columns(dict(path="vector", nullable=True))
    table.add(data)

    expected_schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2), nullable=True),
            pa.field("id", pa.string(), nullable=False),
        ]
    )
    expected = pa.table(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5], None],
            "id": ["foo", "bar", "baz"],
        },
        schema=expected_schema,
    )
    assert table.to_arrow() == expected


def test_add_pydantic_model(mem_db: DBConnection):
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

    tbl = mem_db.create_table("mytable", schema=LanceSchema, mode="overwrite")
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
async def test_add_async(mem_db_async: AsyncConnection):
    table = await mem_db_async.create_table(
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
    assert await table.count_rows() == 3


def test_polars(mem_db: DBConnection):
    data = {
        "vector": [[3.1, 4.1], [5.9, 26.5]],
        "item": ["foo", "bar"],
        "price": [10.0, 20.0],
    }
    # Ingest polars dataframe
    table = mem_db.create_table("test", data=pl.DataFrame(data))
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


def test_versioning(mem_db: DBConnection):
    table = mem_db.create_table(
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    )

    assert len(table.list_versions()) == 1
    assert table.version == 1

    table.add([{"vector": [6.3, 100.5], "item": "new", "price": 30.0}])
    assert len(table.list_versions()) == 2
    assert table.version == 2
    assert len(table) == 3

    table.checkout(1)
    assert table.version == 1
    assert len(table) == 2


@patch("lancedb.table.AsyncTable.create_index")
def test_create_index_method(mock_create_index, mem_db: DBConnection):
    table = mem_db.create_table(
        "test",
        data=[
            {"vector": [3.1, 4.1]},
            {"vector": [5.9, 26.5]},
        ],
    )

    table.create_index(
        metric="l2",
        num_partitions=256,
        num_sub_vectors=96,
        vector_column_name="vector",
        replace=True,
        index_cache_size=256,
        num_bits=4,
    )
    expected_config = IvfPq(
        distance_type="l2",
        num_partitions=256,
        num_sub_vectors=96,
        num_bits=4,
    )
    mock_create_index.assert_called_with("vector", replace=True, config=expected_config)

    table.create_index(
        vector_column_name="my_vector",
        metric="dot",
        index_type="IVF_HNSW_PQ",
        replace=False,
    )
    expected_config = HnswPq(distance_type="dot")
    mock_create_index.assert_called_with(
        "my_vector", replace=False, config=expected_config
    )

    table.create_index(
        vector_column_name="my_vector",
        metric="cosine",
        index_type="IVF_HNSW_SQ",
        sample_rate=0.1,
        m=29,
        ef_construction=10,
    )
    expected_config = HnswSq(
        distance_type="cosine", sample_rate=0.1, m=29, ef_construction=10
    )
    mock_create_index.assert_called_with(
        "my_vector", replace=True, config=expected_config
    )


def test_add_with_nans(mem_db: DBConnection):
    # by default we raise an error on bad input vectors
    bad_data = [
        {"vector": [np.nan], "item": "bar", "price": 20.0},
        {"vector": [5], "item": "bar", "price": 20.0},
        {"vector": [np.nan, np.nan], "item": "bar", "price": 20.0},
        {"vector": [np.nan, 5.0], "item": "bar", "price": 20.0},
    ]
    for row in bad_data:
        with pytest.raises(ValueError):
            mem_db.create_table(
                "error_test",
                data=[{"vector": [3.1, 4.1], "item": "foo", "price": 10.0}, row],
            )

    table = mem_db.create_table(
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
    table = mem_db.create_table(
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
    arrow_tbl = table.search().where("item == 'bar'").to_arrow()
    v = arrow_tbl["vector"].to_pylist()[0]
    assert np.allclose(v, np.array([0.0, 0.0]))


def test_restore(mem_db: DBConnection):
    table = mem_db.create_table(
        "my_table",
        data=[{"vector": [1.1, 0.9], "type": "vector"}],
    )
    table.add([{"vector": [0.5, 0.2], "type": "vector"}])
    table.restore(1)
    assert len(table.list_versions()) == 3
    assert len(table) == 1

    expected = table.to_arrow()
    table.checkout(1)
    table.restore()
    assert len(table.list_versions()) == 4
    assert table.to_arrow() == expected

    table.restore(4)  # latest version should be no-op
    assert len(table.list_versions()) == 5

    with pytest.raises(ValueError):
        table.restore(6)

    with pytest.raises(ValueError):
        table.restore(0)


def test_merge(tmp_db: DBConnection, tmp_path):
    pytest.importorskip("lance")
    import lance

    table = tmp_db.create_table(
        "my_table",
        schema=pa.schema(
            {
                "vector": pa.list_(pa.float32(), 2),
                "id": pa.int64(),
            }
        ),
    )
    table.add([{"vector": [1.1, 0.9], "id": 0}, {"vector": [1.2, 1.9], "id": 1}])
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


def test_delete(mem_db: DBConnection):
    table = mem_db.create_table(
        "my_table",
        data=[{"vector": [1.1, 0.9], "id": 0}, {"vector": [1.2, 1.9], "id": 1}],
    )
    assert len(table) == 2
    assert len(table.list_versions()) == 1
    table.delete("id=0")
    assert len(table.list_versions()) == 2
    assert table.version == 2
    assert len(table) == 1
    assert table.to_pandas()["id"].tolist() == [1]


def test_update(mem_db: DBConnection):
    table = mem_db.create_table(
        "my_table",
        data=[{"vector": [1.1, 0.9], "id": 0}, {"vector": [1.2, 1.9], "id": 1}],
    )
    assert len(table) == 2
    assert len(table.list_versions()) == 1
    table.update(where="id=0", values={"vector": [1.1, 1.1]})
    assert len(table.list_versions()) == 2
    assert table.version == 2
    assert len(table) == 2
    v = table.to_arrow()["vector"].combine_chunks()
    v = v.values.to_numpy().reshape(2, 2)
    assert np.allclose(v, np.array([[1.2, 1.9], [1.1, 1.1]]))


def test_update_types(mem_db: DBConnection):
    table = mem_db.create_table(
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


def test_merge_insert(mem_db: DBConnection):
    table = mem_db.create_table(
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
    (
        table.merge_insert("a")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete("a > 2")
        .execute(new_data)
    )

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


# We vary the data format because there are slight differences in how
# subschemas are handled in different formats
@pytest.mark.parametrize(
    "data_format",
    [
        lambda table: table,
        lambda table: table.to_pandas(),
        lambda table: table.to_pylist(),
    ],
    ids=["pa.Table", "pd.DataFrame", "rows"],
)
def test_merge_insert_subschema(mem_db: DBConnection, data_format):
    initial_data = pa.table(
        {"id": range(3), "a": [1.0, 2.0, 3.0], "c": ["x", "x", "x"]}
    )
    table = mem_db.create_table("my_table", data=initial_data)

    new_data = pa.table({"id": [2, 3], "c": ["y", "y"]})
    new_data = data_format(new_data)
    (
        table.merge_insert(on="id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(new_data)
    )

    expected = pa.table(
        {"id": [0, 1, 2, 3], "a": [1.0, 2.0, 3.0, None], "c": ["x", "x", "y", "y"]}
    )
    assert table.to_arrow().sort_by("id") == expected


@pytest.mark.asyncio
async def test_merge_insert_async(mem_db_async: AsyncConnection):
    data = pa.table({"a": [1, 2, 3], "b": ["a", "b", "c"]})
    table = await mem_db_async.create_table("some_table", data=data)
    assert await table.count_rows() == 3
    version = await table.version()

    new_data = pa.table({"a": [2, 3, 4], "b": ["x", "y", "z"]})

    # upsert
    await (
        table.merge_insert("a")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(new_data)
    )
    expected = pa.table({"a": [1, 2, 3, 4], "b": ["a", "x", "y", "z"]})
    assert (await table.to_arrow()).sort_by("a") == expected

    await table.checkout(version)
    await table.restore()

    # conditional update
    await (
        table.merge_insert("a")
        .when_matched_update_all(where="target.b = 'b'")
        .execute(new_data)
    )
    expected = pa.table({"a": [1, 2, 3], "b": ["a", "x", "c"]})
    assert (await table.to_arrow()).sort_by("a") == expected

    await table.checkout(version)
    await table.restore()

    # insert-if-not-exists
    await table.merge_insert("a").when_not_matched_insert_all().execute(new_data)
    expected = pa.table({"a": [1, 2, 3, 4], "b": ["a", "b", "c", "z"]})
    assert (await table.to_arrow()).sort_by("a") == expected

    await table.checkout(version)
    await table.restore()

    # replace-range
    new_data = pa.table({"a": [2, 4], "b": ["x", "z"]})
    await (
        table.merge_insert("a")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete("a > 2")
        .execute(new_data)
    )
    expected = pa.table({"a": [1, 2, 4], "b": ["a", "x", "z"]})
    assert (await table.to_arrow()).sort_by("a") == expected

    await table.checkout(version)
    await table.restore()

    # replace-range no condition
    await (
        table.merge_insert("a")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete()
        .execute(new_data)
    )
    expected = pa.table({"a": [2, 4], "b": ["x", "z"]})
    assert (await table.to_arrow()).sort_by("a") == expected


def test_create_with_embedding_function(mem_db: DBConnection):
    class MyTable(LanceModel):
        text: str
        vector: Vector(10)

    func = MockTextEmbeddingFunction.create()
    texts = ["hello world", "goodbye world", "foo bar baz fizz buzz"]
    df = pd.DataFrame({"text": texts, "vector": func.compute_source_embeddings(texts)})

    conf = EmbeddingFunctionConfig(
        source_column="text", vector_column="vector", function=func
    )
    table = mem_db.create_table(
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


def test_create_f16_table(mem_db: DBConnection):
    class MyTable(LanceModel):
        text: str
        vector: Vector(32, value_type=pa.float16())

    df = pd.DataFrame(
        {
            "text": [f"s-{i}" for i in range(512)],
            "vector": [np.random.randn(32).astype(np.float16) for _ in range(512)],
        }
    )
    table = mem_db.create_table(
        "f16_tbl",
        schema=MyTable,
    )
    table.add(df)
    table.create_index(num_partitions=2, num_sub_vectors=2)

    query = df.vector.iloc[2]
    expected = table.search(query).limit(2).to_arrow()

    assert "s-2" in expected["text"].to_pylist()


def test_add_with_embedding_function(mem_db: DBConnection):
    emb = EmbeddingFunctionRegistry.get_instance().get("test").create()

    class MyTable(LanceModel):
        text: str = emb.SourceField()
        vector: Vector(emb.ndims()) = emb.VectorField()

    table = mem_db.create_table("my_table", schema=MyTable)

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


def test_multiple_vector_columns(mem_db: DBConnection):
    class MyTable(LanceModel):
        text: str
        vector1: Vector(10)
        vector2: Vector(10)

    table = mem_db.create_table(
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


def test_create_scalar_index(mem_db: DBConnection):
    vec_array = pa.array(
        [[1, 1], [2, 2], [3, 3], [4, 4], [5, 5]], pa.list_(pa.float32(), 2)
    )
    test_data = pa.Table.from_pydict(
        {"x": ["c", "b", "a", "e", "b"], "y": [1, 2, 3, 4, 5], "vector": vec_array}
    )
    table = mem_db.create_table(
        "my_table",
        data=test_data,
    )
    table.create_scalar_index("x")
    indices = table.list_indices()
    assert len(indices) == 1
    scalar_index = indices[0]
    assert scalar_index.index_type == "BTree"

    # Confirm that prefiltering still works with the scalar index column
    results = table.search().where("x = 'c'").to_arrow()
    assert results == test_data.slice(0, 1)
    results = table.search([5, 5]).to_arrow()
    assert results["_distance"][0].as_py() == 0
    results = table.search([5, 5]).where("x != 'b'").to_arrow()
    assert results["_distance"][0].as_py() > 0

    table.drop_index(scalar_index.name)
    indices = table.list_indices()
    assert len(indices) == 0


def test_empty_query(mem_db: DBConnection):
    table = mem_db.create_table(
        "my_table",
        data=[{"text": "foo", "id": 0}, {"text": "bar", "id": 1}],
    )
    df = table.search().select(["id"]).where("text='bar'").limit(1).to_pandas()
    val = df.id.iloc[0]
    assert val == 1

    table = mem_db.create_table("my_table2", data=[{"id": i} for i in range(100)])
    df = table.search().select(["id"]).to_pandas()
    assert len(df) == 100
    # None is the same as default
    df = table.search().select(["id"]).limit(None).to_pandas()
    assert len(df) == 100
    # invalid limist is the same as None, wihch is the same as default
    df = table.search().select(["id"]).limit(-1).to_pandas()
    assert len(df) == 100
    # valid limit should work
    df = table.search().select(["id"]).limit(42).to_pandas()
    assert len(df) == 42


def test_search_with_schema_inf_single_vector(mem_db: DBConnection):
    class MyTable(LanceModel):
        text: str
        vector_col: Vector(10)

    table = mem_db.create_table(
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


def test_search_with_schema_inf_multiple_vector(mem_db: DBConnection):
    class MyTable(LanceModel):
        text: str
        vector1: Vector(10)
        vector2: Vector(10)

    table = mem_db.create_table(
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


def test_compact_cleanup(tmp_db: DBConnection):
    pytest.importorskip("lance")
    table = tmp_db.create_table(
        "my_table",
        data=[{"text": "foo", "id": 0}, {"text": "bar", "id": 1}],
    )

    table.add([{"text": "baz", "id": 2}])
    assert len(table) == 3
    assert table.version == 2

    stats = table.compact_files()
    assert len(table) == 3
    # Compact_files bump 2 versions.
    assert table.version == 4
    assert stats.fragments_removed > 0
    assert stats.fragments_added == 1

    stats = table.cleanup_old_versions()
    assert stats.bytes_removed == 0

    stats = table.cleanup_old_versions(older_than=timedelta(0), delete_unverified=True)
    assert stats.bytes_removed > 0
    assert table.version == 4

    with pytest.raises(Exception, match="Version 3 no longer exists"):
        table.checkout(3)


def test_count_rows(mem_db: DBConnection):
    table = mem_db.create_table(
        "my_table",
        data=[{"text": "foo", "id": 0}, {"text": "bar", "id": 1}],
    )
    assert len(table) == 2
    assert table.count_rows() == 2
    assert table.count_rows(filter="text='bar'") == 1


def setup_hybrid_search_table(db: DBConnection, embedding_func):
    # Create a LanceDB table schema with a vector and a text column
    emb = EmbeddingFunctionRegistry.get_instance().get(embedding_func).create()

    class MyTable(LanceModel):
        text: str = emb.SourceField()
        vector: Vector(emb.ndims()) = emb.VectorField()

    # Initialize the table using the schema
    table = db.create_table(
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

    return table, MyTable, emb


def test_hybrid_search(tmp_db: DBConnection):
    # This test uses an FTS index
    pytest.importorskip("lancedb.fts")
    pytest.importorskip("lance")

    table, MyTable, emb = setup_hybrid_search_table(tmp_db, "test")

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

    # Test that double and single quote characters are handled with phrase_query()
    (
        table.search(
            '"Aren\'t you a little short for a stormtrooper?" -- Leia',
            query_type="hybrid",
        )
        .phrase_query(True)
        .to_pydantic(MyTable)
    )

    assert result1 == result3

    # with post filters
    result = (
        table.search("Arrrrggghhhhhhh", query_type="hybrid")
        .where("text='Arrrrggghhhhhhh'")
        .to_list()
    )
    assert len(result) == 1

    # with explicit query type
    vector_query = list(range(emb.ndims()))
    result = (
        table.search(query_type="hybrid")
        .vector(vector_query)
        .text("Arrrrggghhhhhhh")
        .to_arrow()
    )
    assert len(result) > 0
    assert "_relevance_score" in result.column_names

    # with vector_column_name
    result = (
        table.search(query_type="hybrid", vector_column_name="vector")
        .vector(vector_query)
        .text("Arrrrggghhhhhhh")
        .to_arrow()
    )
    assert len(result) > 0
    assert "_relevance_score" in result.column_names

    # fail if only text or vector is provided
    with pytest.raises(ValueError):
        table.search(query_type="hybrid").to_list()
    with pytest.raises(ValueError):
        table.search(query_type="hybrid").vector(vector_query).to_list()
    with pytest.raises(ValueError):
        table.search(query_type="hybrid").text("Arrrrggghhhhhhh").to_list()


def test_hybrid_search_metric_type(tmp_db: DBConnection):
    # This test uses an FTS index
    pytest.importorskip("lancedb.fts")
    pytest.importorskip("lance")

    # Need to use nonnorm as the embedding function so l2 and dot results
    # are different
    table, _, _ = setup_hybrid_search_table(tmp_db, "nonnorm")

    # with custom metric
    result_dot = (
        table.search("feeling lucky", query_type="hybrid")
        .distance_type("dot")
        .to_arrow()
    )
    result_l2 = table.search("feeling lucky", query_type="hybrid").to_arrow()
    assert len(result_dot) > 0
    assert len(result_l2) > 0
    assert result_dot["_relevance_score"] != result_l2["_relevance_score"]


@pytest.mark.parametrize(
    "consistency_interval", [None, timedelta(seconds=0), timedelta(seconds=0.1)]
)
def test_consistency(tmp_path, consistency_interval):
    db = lancedb.connect(tmp_path)
    table = db.create_table("my_table", data=[{"id": 0}])

    db2 = lancedb.connect(tmp_path, read_consistency_interval=consistency_interval)
    table2 = db2.open_table("my_table")
    if consistency_interval is not None:
        assert "read_consistency_interval=datetime.timedelta(" in repr(db2)
        assert "read_consistency_interval=datetime.timedelta(" in repr(table2)
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
    table = db.create_table("my_table", data=[{"id": 0}])
    assert table.version == 1

    db2 = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table2 = db2.open_table("my_table")
    assert table2.version == table.version

    # If we call checkout, it should lose consistency
    table2.checkout(table.version)
    table.add([{"id": 2}])
    assert table2.version == 1
    # But if we call checkout_latest, it should be consistent again
    table2.checkout_latest()
    assert table2.version == table.version


# Schema evolution
def test_add_columns(mem_db: DBConnection):
    data = pa.table({"id": [0, 1]})
    table = LanceTable.create(mem_db, "my_table", data=data)
    table.add_columns({"new_col": "id + 2"})
    assert table.to_arrow().column_names == ["id", "new_col"]
    assert table.to_arrow()["new_col"].to_pylist() == [2, 3]

    table.add_columns({"null_int": "cast(null as bigint)"})
    assert table.schema.field("null_int").type == pa.int64()


@pytest.mark.asyncio
async def test_add_columns_async(mem_db_async: AsyncConnection):
    data = pa.table({"id": [0, 1]})
    table = await mem_db_async.create_table("my_table", data=data)
    await table.add_columns({"new_col": "id + 2"})
    data = await table.to_arrow()
    assert data.column_names == ["id", "new_col"]
    assert data["new_col"].to_pylist() == [2, 3]


@pytest.mark.asyncio
async def test_add_columns_with_schema(mem_db_async: AsyncConnection):
    data = pa.table({"id": [0, 1]})
    table = await mem_db_async.create_table("my_table", data=data)
    await table.add_columns(
        [pa.field("x", pa.int64()), pa.field("vector", pa.list_(pa.float32(), 8))]
    )

    assert await table.schema() == pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("x", pa.int64()),
            pa.field("vector", pa.list_(pa.float32(), 8)),
        ]
    )

    table = await mem_db_async.create_table("table2", data=data)
    await table.add_columns(
        pa.schema(
            [pa.field("y", pa.int64()), pa.field("emb", pa.list_(pa.float32(), 8))]
        )
    )
    assert await table.schema() == pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("y", pa.int64()),
            pa.field("emb", pa.list_(pa.float32(), 8)),
        ]
    )


def test_alter_columns(mem_db: DBConnection):
    data = pa.table({"id": [0, 1]})
    table = mem_db.create_table("my_table", data=data)
    table.alter_columns({"path": "id", "rename": "new_id"})
    assert table.to_arrow().column_names == ["new_id"]


@pytest.mark.asyncio
async def test_alter_columns_async(mem_db_async: AsyncConnection):
    data = pa.table({"id": [0, 1]})
    table = await mem_db_async.create_table("my_table", data=data)
    await table.alter_columns({"path": "id", "rename": "new_id"})
    assert (await table.to_arrow()).column_names == ["new_id"]
    await table.alter_columns(dict(path="new_id", data_type=pa.int16(), nullable=True))
    data = await table.to_arrow()
    assert data.column(0).type == pa.int16()
    assert data.schema.field(0).nullable


def test_drop_columns(mem_db: DBConnection):
    data = pa.table({"id": [0, 1], "category": ["a", "b"]})
    table = mem_db.create_table("my_table", data=data)
    table.drop_columns(["category"])
    assert table.to_arrow().column_names == ["id"]


@pytest.mark.asyncio
async def test_drop_columns_async(mem_db_async: AsyncConnection):
    data = pa.table({"id": [0, 1], "category": ["a", "b"]})
    table = await mem_db_async.create_table("my_table", data=data)
    await table.drop_columns(["category"])
    assert (await table.to_arrow()).column_names == ["id"]


@pytest.mark.asyncio
async def test_time_travel(mem_db_async: AsyncConnection):
    # Setup
    table = await mem_db_async.create_table("some_table", data=[{"id": 0}])
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


def test_sync_optimize(mem_db: DBConnection):
    table = mem_db.create_table(
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    )

    table.create_scalar_index("price", index_type="BTREE")
    stats = table.index_stats("price_idx")
    assert stats["num_indexed_rows"] == 2

    table.add([{"vector": [2.0, 2.0], "item": "baz", "price": 30.0}])
    assert table.count_rows() == 3
    table.optimize()
    stats = table.index_stats("price_idx")
    assert stats["num_indexed_rows"] == 3


@pytest.mark.asyncio
async def test_sync_optimize_in_async(mem_db: DBConnection):
    table = mem_db.create_table(
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    )

    table.create_scalar_index("price", index_type="BTREE")
    stats = table.index_stats("price_idx")
    assert stats["num_indexed_rows"] == 2

    table.add([{"vector": [2.0, 2.0], "item": "baz", "price": 30.0}])
    assert table.count_rows() == 3
    table.optimize()


@pytest.mark.asyncio
async def test_optimize(mem_db_async: AsyncConnection):
    table = await mem_db_async.create_table(
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
async def test_optimize_delete_unverified(tmp_db_async: AsyncConnection, tmp_path):
    table = await tmp_db_async.create_table(
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


def test_replace_field_metadata(tmp_path):
    db = lancedb.connect(tmp_path)
    table = db.create_table("my_table", data=[{"x": 0}])
    table.replace_field_metadata("x", {"foo": "bar"})
    schema = table.schema
    field = schema[0].metadata
    assert field == {b"foo": b"bar"}
