# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

# --8<-- [start:imports]
import lancedb
import pandas as pd
import pyarrow as pa
# --8<-- [end:imports]

import pytest
from numpy.random import randint, random


def test_quickstart(tmp_path):
    # --8<-- [start:set_uri]
    uri = "data/sample-lancedb"
    # --8<-- [end:set_uri]
    uri = tmp_path
    # --8<-- [start:connect]
    db = lancedb.connect(uri)
    # --8<-- [end:connect]

    # --8<-- [start:create_table]
    data = [
        {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
        {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
    ]

    tbl = db.create_table("my_table", data=data)
    # --8<-- [end:create_table]

    # --8<-- [start:create_table_pandas]
    df = pd.DataFrame(
        [
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ]
    )
    tbl = db.create_table("table_from_df", data=df)
    # --8<-- [end:create_table_pandas]

    # --8<-- [start:create_empty_table]
    schema = pa.schema([pa.field("vector", pa.list_(pa.float32(), list_size=2))])
    tbl = db.create_table("empty_table", schema=schema)
    # --8<-- [end:create_empty_table]
    # --8<-- [start:open_table]
    tbl = db.open_table("my_table")
    # --8<-- [end:open_table]
    # --8<-- [start:table_names]
    print(db.table_names())
    # --8<-- [end:table_names]
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
    tbl.search([100, 100]).limit(2).to_pandas()
    # --8<-- [end:vector_search]
    tbl.add(
        [
            {"vector": random(2), "item": "autogen", "price": randint(100)}
            for _ in range(1000)
        ]
    )
    # --8<-- [start:add_columns]
    tbl.add_columns({"double_price": "cast((price * 2) as float)"})
    # --8<-- [end:add_columns]
    # --8<-- [start:alter_columns]
    tbl.alter_columns(
        {
            "path": "double_price",
            "rename": "dbl_price",
            "data_type": pa.float64(),
            "nullable": True,
        }
    )
    # --8<-- [end:alter_columns]
    # --8<-- [start:alter_columns_vector]
    tbl.alter_columns(
        {
            "path": "vector",
            "data_type": pa.list_(pa.float16(), list_size=2),
        }
    )
    # --8<-- [end:alter_columns_vector]
    # Change it back since we can get a panic with fp16
    tbl.alter_columns(
        {
            "path": "vector",
            "data_type": pa.list_(pa.float32(), list_size=2),
        }
    )
    # --8<-- [start:drop_columns]
    tbl.drop_columns(["dbl_price"])
    # --8<-- [end:drop_columns]
    # --8<-- [start:create_index]
    tbl.create_index(num_sub_vectors=1)
    # --8<-- [end:create_index]
    # --8<-- [start:delete_rows]
    tbl.delete('item = "fizz"')
    # --8<-- [end:delete_rows]
    # --8<-- [start:drop_table]
    db.drop_table("my_table")
    # --8<-- [end:drop_table]


@pytest.mark.asyncio
async def test_quickstart_async(tmp_path):
    uri = tmp_path
    # --8<-- [start:connect_async]
    db = await lancedb.connect_async(uri)
    # --8<-- [end:connect_async]
    # --8<-- [start:create_table_async]
    data = [
        {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
        {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
    ]

    tbl = await db.create_table("my_table_async", data=data)
    # --8<-- [end:create_table_async]
    # --8<-- [start:create_table_async_pandas]
    df = pd.DataFrame(
        [
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ]
    )

    tbl = await db.create_table("table_from_df_async", df)
    # --8<-- [end:create_table_async_pandas]
    # --8<-- [start:create_empty_table_async]
    schema = pa.schema([pa.field("vector", pa.list_(pa.float32(), list_size=2))])
    tbl = await db.create_table("empty_table_async", schema=schema)
    # --8<-- [end:create_empty_table_async]
    # --8<-- [start:open_table_async]
    tbl = await db.open_table("my_table_async")
    # --8<-- [end:open_table_async]
    # --8<-- [start:table_names_async]
    print(await db.table_names())
    # --8<-- [end:table_names_async]
    # --8<-- [start:add_data_async]
    # Option 1: Add a list of dicts to a table
    data = [
        {"vector": [1.3, 1.4], "item": "fizz", "price": 100.0},
        {"vector": [9.5, 56.2], "item": "buzz", "price": 200.0},
    ]
    await tbl.add(data)

    # Option 2: Add a pandas DataFrame to a table
    df = pd.DataFrame(data)
    await tbl.add(data)
    # --8<-- [end:add_data_async]
    # Add sufficient data for training
    data = [{"vector": [x, x], "item": "filler", "price": x * x} for x in range(1000)]
    await tbl.add(data)
    # --8<-- [start:vector_search_async]
    await tbl.vector_search([100, 100]).limit(2).to_pandas()
    # --8<-- [end:vector_search_async]
    # --8<-- [start:add_columns_async]
    await tbl.add_columns({"double_price": "cast((price * 2) as float)"})
    # --8<-- [end:add_columns_async]
    # --8<-- [start:alter_columns_async]
    await tbl.alter_columns(
        {
            "path": "double_price",
            "rename": "dbl_price",
            "data_type": pa.float64(),
            "nullable": True,
        }
    )
    # --8<-- [end:alter_columns_async]
    # --8<-- [start:alter_columns_async_vector]
    await tbl.alter_columns(
        {
            "path": "vector",
            "data_type": pa.list_(pa.float16(), list_size=2),
        }
    )
    # --8<-- [end:alter_columns_async_vector]
    # Change it back since we can get a panic with fp16
    await tbl.alter_columns(
        {
            "path": "vector",
            "data_type": pa.list_(pa.float32(), list_size=2),
        }
    )
    # --8<-- [start:drop_columns_async]
    await tbl.drop_columns(["dbl_price"])
    # --8<-- [end:drop_columns_async]
    await tbl.vector_search([100, 100]).limit(2).to_pandas()
    # --8<-- [end:vector_search_async]
    # --8<-- [start:create_index_async]
    await tbl.create_index("vector")
    # --8<-- [end:create_index_async]
    # --8<-- [start:delete_rows_async]
    await tbl.delete('item = "fizz"')
    # --8<-- [end:delete_rows_async]
    # --8<-- [start:drop_table_async]
    await db.drop_table("my_table_async")
    # --8<-- [end:drop_table_async]
