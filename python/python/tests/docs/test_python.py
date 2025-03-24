# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

# --8<-- [start:import-lancedb]
import lancedb

# --8<-- [end:import-lancedb]
# --8<-- [start:import-pandas]
import pandas as pd

# --8<-- [end:import-pandas]
# --8<-- [start:import-iterable]
from typing import Iterable

# --8<-- [end:import-iterable]
# --8<-- [start:import-pyarrow]
import pyarrow as pa

# --8<-- [end:import-pyarrow]
# --8<-- [start:import-polars]
import polars as pl

# --8<-- [end:import-polars]
# --8<-- [start:import-lancedb-pydantic]
from lancedb.pydantic import Vector, LanceModel

# --8<-- [end:import-lancedb-pydantic]
import pytest


# --8<-- [start:make_batches]
def make_batches() -> Iterable[pa.RecordBatch]:
    for i in range(5):
        yield pa.RecordBatch.from_arrays(
            [
                pa.array([[3.1, 4.1], [5.9, 26.5]]),
                pa.array(["foo", "bar"]),
                pa.array([10.0, 20.0]),
            ],
            ["vector", "item", "price"],
        )


# --8<-- [end:make_batches]


def test_pandas_and_pyarrow():
    # --8<-- [start:connect_to_lancedb]
    uri = "data/sample-lancedb"
    db = lancedb.connect(uri)
    # --8<-- [end:connect_to_lancedb]
    # --8<-- [start:create_table_pandas]
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    table = db.create_table("pd_table", data=data)
    # --8<-- [end:create_table_pandas]
    # --8<-- [start:create_table_iterable]
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32())),
            pa.field("item", pa.utf8()),
            pa.field("price", pa.float32()),
        ]
    )
    table = db.create_table("iterable_table", data=make_batches(), schema=schema)
    # --8<-- [end:create_table_iterable]
    # --8<-- [start:vector_search]
    # Open the table previously created.
    table = db.open_table("pd_table")

    query_vector = [100, 100]
    # Pandas DataFrame
    df = table.search(query_vector).limit(1).to_pandas()
    print(df)
    # --8<-- [end:vector_search]
    # --8<-- [start:vector_search_with_filter]
    # Apply the filter via LanceDB
    results = table.search([100, 100]).where("price < 15").to_pandas()
    assert len(results) == 1
    assert results["item"].iloc[0] == "foo"

    # Apply the filter via Pandas
    df = results = table.search([100, 100]).to_pandas()
    results = df[df.price < 15]
    assert len(results) == 1
    assert results["item"].iloc[0] == "foo"
    # --8<-- [end:vector_search_with_filter]


@pytest.mark.asyncio
async def test_pandas_and_pyarrow_async():
    # --8<-- [start:connect_to_lancedb_async]
    uri = "data/sample-lancedb"
    async_db = await lancedb.connect_async(uri)
    # --8<-- [end:connect_to_lancedb_async]
    # --8<-- [start:create_table_pandas_async]
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    await async_db.create_table("pd_table_async", data=data)
    # --8<-- [end:create_table_pandas_async]
    # --8<-- [start:create_table_iterable_async]
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32())),
            pa.field("item", pa.utf8()),
            pa.field("price", pa.float32()),
        ]
    )
    await async_db.create_table(
        "iterable_table_async", data=make_batches(), schema=schema
    )
    # --8<-- [end:create_table_iterable_async]
    # --8<-- [start:vector_search_async]
    # Open the table previously created.
    async_tbl = await async_db.open_table("pd_table_async")

    query_vector = [100, 100]
    # Pandas DataFrame
    df = await (await async_tbl.search(query_vector)).limit(1).to_pandas()
    print(df)
    # --8<-- [end:vector_search_async]
    # --8<-- [start:vector_search_with_filter_async]
    # Apply the filter via LanceDB
    results = await (await async_tbl.search([100, 100])).where("price < 15").to_pandas()
    assert len(results) == 1
    assert results["item"].iloc[0] == "foo"

    # Apply the filter via Pandas
    df = results = await (await async_tbl.search([100, 100])).to_pandas()
    results = df[df.price < 15]
    assert len(results) == 1
    assert results["item"].iloc[0] == "foo"
    # --8<-- [end:vector_search_with_filter_async]


# --8<-- [start:class_Item]
class Item(LanceModel):
    vector: Vector(2)
    item: str
    price: float


# --8<-- [end:class_Item]


def test_polars():
    uri = "data/sample-lancedb"
    db = lancedb.connect(uri)

    # --8<-- [start:create_table_polars]
    data = pl.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    table = db.create_table("pl_table", data=data)
    # --8<-- [end:create_table_polars]
    # --8<-- [start:vector_search_polars]
    query = [3.0, 4.0]
    result = table.search(query).limit(1).to_polars()
    print(result)
    print(type(result))
    # --8<-- [end:vector_search_polars]
    # --8<-- [start:create_table_pydantic]
    table = db.create_table("pydantic_table", schema=Item)
    df = pl.DataFrame(data)
    # Add Polars DataFrame to table
    table.add(df)
    # --8<-- [end:create_table_pydantic]
    # --8<-- [start:dump_table_lazyform]
    ldf = table.to_polars()
    print(type(ldf))
    # --8<-- [end:dump_table_lazyform]
    # --8<-- [start:print_table_lazyform]
    print(ldf.first().collect())
    # --8<-- [end:print_table_lazyform]


@pytest.mark.asyncio
async def test_polars_async():
    uri = "data/sample-lancedb"
    db = await lancedb.connect_async(uri)

    # --8<-- [start:create_table_polars_async]
    data = pl.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    table = await db.create_table("pl_table_async", data=data)
    # --8<-- [end:create_table_polars_async]
    # --8<-- [start:vector_search_polars_async]
    query = [3.0, 4.0]
    result = await (await table.search(query)).limit(1).to_polars()
    print(result)
    print(type(result))
    # --8<-- [end:vector_search_polars_async]
