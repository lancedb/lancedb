# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

# --8<-- [start:basic_imports]
import json

import lancedb
import pandas as pd
import polars as pl
import pyarrow as pa
# --8<-- [end:basic_imports]
import pytest

data_path = "tests/camelot.json"

def test_basic_usage(db_path_factory):
    uri = "./basic_usage_db"
    uri = db_path_factory("basic_usage_db")
    db = lancedb.connect(uri)

    # --8<-- [start:data_load]
    with open(data_path, "r") as f:
        data = json.load(f)
    # --8<-- [end:data_load]

    # --8<-- [start:basic_create_table]
    table = db.create_table("camelot", data=data, mode="overwrite")
    # --8<-- [end:basic_create_table]
    assert len(table) == 8

    # --8<-- [start:basic_open_table]
    table = db.open_table("camelot")
    # --8<-- [end:basic_open_table]

    # --8<-- [start:basic_create_table_pandas]
    pandas_df = pd.DataFrame(data)
    table_pd = db.create_table("camelot_pd", data=pandas_df, mode="overwrite")
    # --8<-- [end:basic_create_table_pandas]
    assert len(table_pd) == 8
    db.drop_table("camelot_pd")

    # --8<-- [start:basic_create_table_polars]
    polars_df = pl.DataFrame(data)
    table_pl = db.create_table("camelot_pl", data=polars_df, mode="overwrite")
    # --8<-- [end:basic_create_table_polars]
    assert len(table_pl) == 8
    db.drop_table("camelot_pl")

    # --8<-- [start:basic_create_empty_table]
    schema = pa.schema(
        [
            pa.field("id", pa.uint16()),
            pa.field("name", pa.string()),
            pa.field("role", pa.string()),
            pa.field("description", pa.string()),
            pa.field("vector", pa.list_(pa.float32(), 4)),
            pa.field(
                "stats",
                pa.struct(
                    [
                        pa.field("strength", pa.int8()),
                        pa.field("courage", pa.int8()),
                        pa.field("magic", pa.int8()),
                        pa.field("wisdom", pa.int8()),
                    ]
                ),
            ),
        ]
    )
    db.create_table("camelot_pa", schema=schema, mode="overwrite")
    # --8<-- [end:basic_create_empty_table]
    assert "camelot_pa" in db.list_tables().tables
    db.drop_table("camelot_pa")

    # --8<-- [start:basic_add_data]
    magical_characters = [
        {
            "id": 9,
            "name": "Morgan le Fay",
            "role": "Sorceress",
            "description": "A powerful enchantress, Arthur's half-sister, and a complex figure who oscillates between aiding and opposing Camelot.",
            "vector": [0.10, 0.84, 0.25, 0.70],
            "stats": { "strength": 2, "courage": 3, "magic": 5, "wisdom": 4 }
        },
        {
            "id": 10,
            "name": "The Lady of the Lake",
            "role": "Mystical Guardian",
            "description": "A mysterious supernatural figure associated with Avalon, known for giving Arthur the sword Excalibur.",
            "vector": [0.00, 0.90, 0.58, 0.88],
            "stats": { "strength": 2, "courage": 3, "magic": 5, "wisdom": 5 }
        }
    ]
    table.add(magical_characters)
    # --8<-- [end:basic_add_data]
    assert len(table) == 10

    # --8<-- [start:basic_vector_search]
    query_vector = [0.03, 0.85, 0.61, 0.90]
    result = table.search(query_vector).limit(5).to_polars()
    print(result)
    # --8<-- [end:basic_vector_search]

    # --8<-- [start:basic_add_columns]
    table.add_columns(
        {
            "power": "cast(((stats.strength + stats.courage + stats.magic + stats.wisdom) / 4.0) as float)"
        }
    )
    # --8<-- [end:basic_add_columns]
    assert "power" in table.schema.names

    # Run examples to illustrate search
    # --8<-- [start:basic_vector_search_q1]
    # Who are the characters similar to  "wizard"?
    query_vector_1 = [0.03, 0.85, 0.61, 0.90]
    r1 = (
        table.search(query_vector_1)
        .limit(5)
        .select(["name", "role", "description"])
        .to_polars()
    )
    print(r1)
    # --8<-- [end:basic_vector_search_q1]

    # --8<-- [start:basic_vector_search_q2]
    # Who are the characters with high magic stats?
    query_vector_2 = [0.03, 0.85, 0.61, 0.90]
    r2 = (
        table.search(query_vector_2)
        .where("stats.magic > 3")
        .select(["name", "role", "description"])
        .limit(5)
        .to_polars()
    )
    print(r2)
    # --8<-- [end:basic_vector_search_q2]

    # --8<-- [start:basic_vector_search_q3]
    # Who are the strongest characters?
    r3 = (
        table.search()
        .where("stats.strength > 3")
        .select(["name", "role", "description"])
        .limit(5)
        .to_polars()
    )
    print(r3)
    # --8<-- [end:basic_vector_search_q3]

    # --8<-- [start:basic_vector_search_q4]
    # Who are the strongest characters?
    r4 = (
        table.search()
        .select(["name", "role", "description", "power"])
        .to_polars()
    )
    print(r4)
    # --8<-- [end:basic_vector_search_q4]

    # --8<-- [start:basic_sort_polars]
    # Sort Polars DataFrame by power in descending order
    print(r4.sort("power", descending=True).limit(5))
    # --8<-- [end:basic_sort_polars]
    sorted_power = r4.sort("power", descending=True)["power"].to_list()
    assert sorted_power == sorted(sorted_power, reverse=True)
    assert sorted_power[0] == 4.0

    # --8<-- [start:basic_drop_columns]
    table.drop_columns(["power"])
    # --8<-- [end:basic_drop_columns]
    # --8<-- [start:basic_delete_rows]
    table.delete('role = "Traitor Knight"')
    # --8<-- [end:basic_delete_rows]
    assert len(table) == 9
    # --8<-- [start:basic_drop_table]
    db.drop_table("camelot")
    # --8<-- [end:basic_drop_table]
    assert "camelot" not in db.list_tables().tables


@pytest.mark.asyncio
async def test_basic_usage_async_api(db_path_factory):
    uri = db_path_factory("basic_usage_async_db")
    with open(data_path, "r") as f:
        data = json.load(f)

    # --8<-- [start:basic_async_api]
    import lancedb

    async_db = await lancedb.connect_async(uri)
    async_table = await async_db.create_table(
        "camelot_async",
        data=data,
        mode="overwrite",
    )

    query_vector = [0.03, 0.85, 0.61, 0.90]
    async_results = await (
        await async_table.search(query_vector)
    ).limit(5).select(["name", "role", "description"]).to_polars()
    print(async_results)
    # --8<-- [end:basic_async_api]

    assert async_results.height >= 1
