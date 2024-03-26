import shutil

# --8<-- [start:imports]
import lancedb
import pandas as pd
import pyarrow as pa

# --8<-- [end:imports]
import pytest
from numpy.random import randint, random

shutil.rmtree("data/sample-lancedb", ignore_errors=True)


def test_quickstart():
    # --8<-- [start:connect]
    uri = "data/sample-lancedb"
    db = lancedb.connect(uri)
    # --8<-- [end:connect]

    # --8<-- [start:create_table]
    data = [
        {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
        {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
    ]

    # Synchronous client
    tbl = db.create_table("my_table", data=data)
    # --8<-- [end:create_table]

    # --8<-- [start:create_table_pandas]
    df = pd.DataFrame(
        [
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ]
    )
    # Synchronous client
    tbl = db.create_table("table_from_df", data=df)
    # --8<-- [end:create_table_pandas]

    # --8<-- [start:create_empty_table]
    schema = pa.schema([pa.field("vector", pa.list_(pa.float32(), list_size=2))])
    # Synchronous client
    tbl = db.create_table("empty_table", schema=schema)
    # --8<-- [end:create_empty_table]
    # --8<-- [start:open_table]
    # Synchronous client
    tbl = db.open_table("my_table")
    # --8<-- [end:open_table]
    # --8<-- [start:table_names]
    # Synchronous client
    print(db.table_names())
    # --8<-- [end:table_names]
    # Synchronous client
    # --8<-- [start:add_data]
    # Option 1: Add a list of dicts to a table
    data = [
        {"vector": [1.3, 1.4], "item": "fizz", "price": 100.0},
        {"vector": [9.5, 56.2], "item": "buzz", "price": 200.0},
    ]
    tbl.add(data)

    # Option 2: Add a pandas DataFrame to a table
    df = pd.DataFrame(data)
    tbl.add(data)
    # --8<-- [end:add_data]
    # --8<-- [start:vector_search]
    # Synchronous client
    tbl.search([100, 100]).limit(2).to_pandas()
    # --8<-- [end:vector_search]
    tbl.add(
        [
            {"vector": random(2), "item": "autogen", "price": randint(100)}
            for _ in range(1000)
        ]
    )
    # --8<-- [start:create_index]
    # Synchronous client
    tbl.create_index(num_sub_vectors=1)
    # --8<-- [end:create_index]
    # --8<-- [start:delete_rows]
    # Synchronous client
    tbl.delete('item = "fizz"')
    # --8<-- [end:delete_rows]
    # --8<-- [start:drop_table]
    # Synchronous client
    db.drop_table("my_table")
    # --8<-- [end:drop_table]


@pytest.mark.asyncio
async def test_quickstart_async():
    # --8<-- [start:connect_async]
    # LanceDb offers both a synchronous and an asynchronous client.  There are still a
    # few operations that are only supported by the synchronous client (e.g. embedding
    # functions, full text search) but both APIs should soon be equivalent

    # In this guide we will give examples of both clients.  In other guides we will
    # typically only provide examples with one client or the other.
    uri = "data/sample-lancedb"
    async_db = await lancedb.connect_async(uri)
    # --8<-- [end:connect_async]

    data = [
        {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
        {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
    ]

    # --8<-- [start:create_table_async]
    # Asynchronous client
    async_tbl = await async_db.create_table("my_table2", data=data)
    # --8<-- [end:create_table_async]

    df = pd.DataFrame(
        [
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ]
    )

    # --8<-- [start:create_table_async_pandas]
    # Asynchronous client
    async_tbl = await async_db.create_table("table_from_df2", df)
    # --8<-- [end:create_table_async_pandas]

    schema = pa.schema([pa.field("vector", pa.list_(pa.float32(), list_size=2))])
    # --8<-- [start:create_empty_table_async]
    # Asynchronous client
    async_tbl = await async_db.create_table("empty_table2", schema=schema)
    # --8<-- [end:create_empty_table_async]
    # --8<-- [start:open_table_async]
    # Asynchronous client
    async_tbl = await async_db.open_table("my_table2")
    # --8<-- [end:open_table_async]
    # --8<-- [start:table_names_async]
    # Asynchronous client
    print(await async_db.table_names())
    # --8<-- [end:table_names_async]
    # --8<-- [start:add_data_async]
    # Asynchronous client
    await async_tbl.add(data)
    # --8<-- [end:add_data_async]
    # Add sufficient data for training
    data = [{"vector": [x, x], "item": "filler", "price": x * x} for x in range(1000)]
    await async_tbl.add(data)
    # --8<-- [start:vector_search_async]
    # Asynchronous client
    await async_tbl.vector_search([100, 100]).limit(2).to_pandas()
    # --8<-- [end:vector_search_async]
    # --8<-- [start:create_index_async]
    # Asynchronous client (must specify column to index)
    await async_tbl.create_index("vector")
    # --8<-- [end:create_index_async]
    # --8<-- [start:delete_rows_async]
    # Asynchronous client
    await async_tbl.delete('item = "fizz"')
    # --8<-- [end:delete_rows_async]
    # --8<-- [start:drop_table_async]
    # Asynchronous client
    await async_db.drop_table("my_table2")
    # --8<-- [end:drop_table_async]
