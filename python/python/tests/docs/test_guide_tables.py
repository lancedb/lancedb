# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

# --8<-- [start:import-lancedb]
import lancedb

# --8<-- [end:import-lancedb]
# --8<-- [start:import-pandas]
import pandas as pd

# --8<-- [end:import-pandas]
# --8<-- [start:import-pyarrow]
import pyarrow as pa

# --8<-- [end:import-pyarrow]
# --8<-- [start:import-polars]
import polars as pl

# --8<-- [end:import-polars]
# --8<-- [start:import-numpy]
import numpy as np

# --8<-- [end:import-numpy]
# --8<-- [start:import-lancedb-pydantic]
from lancedb.pydantic import Vector, LanceModel

# --8<-- [end:import-lancedb-pydantic]
# --8<-- [start:import-datetime]
from datetime import timedelta

# --8<-- [end:import-datetime]
# --8<-- [start:import-embeddings]
from lancedb.embeddings import get_registry

# --8<-- [end:import-embeddings]
# --8<-- [start:import-pydantic-basemodel]
from pydantic import BaseModel

# --8<-- [end:import-pydantic-basemodel]
import pytest


# --8<-- [start:class-Content]
class Content(LanceModel):
    movie_id: int
    vector: Vector(128)
    genres: str
    title: str
    imdb_id: int

    @property
    def imdb_url(self) -> str:
        return f"https://www.imdb.com/title/tt{self.imdb_id}"


# --8<-- [end:class-Content]
# --8<-- [start:class-Document]
class Document(BaseModel):
    content: str
    source: str


# --8<-- [end:class-Document]
# --8<-- [start:class-NestedSchema]
class NestedSchema(LanceModel):
    id: str
    vector: Vector(1536)
    document: Document


# --8<-- [end:class-NestedSchema]
# --8<-- [start:class-Item]
class Item(LanceModel):
    vector: Vector(2)
    item: str
    price: float


# --8<-- [end:class-Item]


# --8<-- [start:make_batches]
def make_batches():
    for i in range(5):
        yield pa.RecordBatch.from_arrays(
            [
                pa.array(
                    [[3.1, 4.1, 5.1, 6.1], [5.9, 26.5, 4.7, 32.8]],
                    pa.list_(pa.float32(), 4),
                ),
                pa.array(["foo", "bar"]),
                pa.array([10.0, 20.0]),
            ],
            ["vector", "item", "price"],
        )


# --8<-- [end:make_batches]


# --8<-- [start:make_batches_for_add]
def make_batches_for_add():
    for i in range(5):
        yield [
            {"vector": [3.1, 4.1], "item": "peach", "price": 6.0},
            {"vector": [5.9, 26.5], "item": "pear", "price": 5.0},
        ]


# --8<-- [end:make_batches_for_add]


def test_table():
    # --8<-- [start:connect]
    uri = "data/sample-lancedb"
    db = lancedb.connect(uri)
    # --8<-- [end:connect]
    # --8<-- [start:create_table]
    data = [
        {"vector": [1.1, 1.2], "lat": 45.5, "long": -122.7},
        {"vector": [0.2, 1.8], "lat": 40.1, "long": -74.1},
    ]
    db.create_table("test_table", data)
    db["test_table"].head()
    # --8<-- [end:create_table]
    # --8<-- [start:create_table_exist_ok]
    db.create_table("test_table", data, exist_ok=True)
    # --8<-- [end:create_table_exist_ok]
    # --8<-- [start:create_table_overwrite]
    db.create_table("test_table", data, mode="overwrite")
    # --8<-- [end:create_table_overwrite]
    # --8<-- [start:create_table_from_pandas]
    data = pd.DataFrame(
        {
            "vector": [[1.1, 1.2, 1.3, 1.4], [0.2, 1.8, 0.4, 3.6]],
            "lat": [45.5, 40.1],
            "long": [-122.7, -74.1],
        }
    )
    db.create_table("my_table_pandas", data)
    db["my_table_pandas"].head()
    # --8<-- [end:create_table_from_pandas]
    # --8<-- [start:create_table_custom_schema]
    custom_schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 4)),
            pa.field("lat", pa.float32()),
            pa.field("long", pa.float32()),
        ]
    )

    tbl = db.create_table("my_table_custom_schema", data, schema=custom_schema)
    # --8<-- [end:create_table_custom_schema]
    # --8<-- [start:create_table_from_polars]
    data = pl.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    tbl = db.create_table("my_table_pl", data)
    # --8<-- [end:create_table_from_polars]
    # --8<-- [start:create_table_from_arrow_table]
    dim = 16
    total = 2
    schema = pa.schema(
        [pa.field("vector", pa.list_(pa.float16(), dim)), pa.field("text", pa.string())]
    )
    data = pa.Table.from_arrays(
        [
            pa.array(
                [np.random.randn(dim).astype(np.float16) for _ in range(total)],
                pa.list_(pa.float16(), dim),
            ),
            pa.array(["foo", "bar"]),
        ],
        ["vector", "text"],
    )
    tbl = db.create_table("f16_tbl", data, schema=schema)
    # --8<-- [end:create_table_from_arrow_table]
    # --8<-- [start:create_table_from_pydantic]
    tbl = db.create_table("movielens_small", schema=Content)
    # --8<-- [end:create_table_from_pydantic]
    # --8<-- [start:create_table_nested_schema]
    tbl = db.create_table("nested_table", schema=NestedSchema)
    # --8<-- [end:create_table_nested_schema]
    # --8<-- [start:create_table_from_batch]
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 4)),
            pa.field("item", pa.utf8()),
            pa.field("price", pa.float32()),
        ]
    )
    db.create_table("batched_tale", make_batches(), schema=schema)
    # --8<-- [end:create_table_from_batch]
    # --8<-- [start:list_tables]
    print(db.table_names())
    # --8<-- [end:list_tables]
    # --8<-- [start:open_table]
    tbl = db.open_table("test_table")
    # --8<-- [end:open_table]
    # --8<-- [start:create_empty_table]
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.string()),
            pa.field("price", pa.float32()),
        ]
    )
    tbl = db.create_table("test_empty_table", schema=schema)
    # --8<-- [end:create_empty_table]
    # --8<-- [start:create_empty_table_pydantic]
    tbl = db.create_table("test_empty_table_new", schema=Item.to_arrow_schema())
    # --8<-- [end:create_empty_table_pydantic]
    # --8<-- [start:add_table_from_pandas]
    df = pd.DataFrame(
        {
            "vector": [[1.3, 1.4], [9.5, 56.2]],
            "item": ["banana", "apple"],
            "price": [5.0, 7.0],
        }
    )

    tbl.add(df)
    # --8<-- [end:add_table_from_pandas]
    # --8<-- [start:add_table_from_polars]
    df = pl.DataFrame(
        {
            "vector": [[1.3, 1.4], [9.5, 56.2]],
            "item": ["banana", "apple"],
            "price": [5.0, 7.0],
        }
    )

    tbl.add(df)
    # --8<-- [end:add_table_from_polars]
    # --8<-- [start:add_table_from_batch]
    tbl.add(make_batches_for_add())
    # --8<-- [end:add_table_from_batch]
    # --8<-- [start:add_table_from_pyarrow]
    pa_table = pa.Table.from_arrays(
        [
            pa.array([[9.1, 6.7], [9.9, 31.2]], pa.list_(pa.float32(), 2)),
            pa.array(["mango", "orange"]),
            pa.array([7.0, 4.0]),
        ],
        ["vector", "item", "price"],
    )
    tbl.add(pa_table)
    # --8<-- [end:add_table_from_pyarrow]
    # --8<-- [start:add_table_from_pydantic]
    pydantic_model_items = [
        Item(vector=[8.1, 4.7], item="pineapple", price=10.0),
        Item(vector=[6.9, 9.3], item="avocado", price=9.0),
    ]
    tbl.add(pydantic_model_items)
    # --8<-- [end:add_table_from_pydantic]
    # --8<-- [start:delete_row]
    tbl.delete('item = "fizz"')
    # --8<-- [end:delete_row]
    # --8<-- [start:delete_specific_row]
    data = [
        {"x": 1, "vector": [1, 2]},
        {"x": 2, "vector": [3, 4]},
        {"x": 3, "vector": [5, 6]},
    ]
    # Synchronous client
    tbl = db.create_table("delete_row", data)
    tbl.to_pandas()
    #   x      vector
    # 0  1  [1.0, 2.0]
    # 1  2  [3.0, 4.0]
    # 2  3  [5.0, 6.0]

    tbl.delete("x = 2")
    tbl.to_pandas()
    #   x      vector
    # 0  1  [1.0, 2.0]
    # 1  3  [5.0, 6.0]
    # --8<-- [end:delete_specific_row]
    # --8<-- [start:delete_list_values]
    to_remove = [1, 5]
    to_remove = ", ".join(str(v) for v in to_remove)

    tbl.delete(f"x IN ({to_remove})")
    tbl.to_pandas()
    #   x      vector
    # 0  3  [5.0, 6.0]
    # --8<-- [end:delete_list_values]
    # --8<-- [start:update_table]
    # Create a table from a pandas DataFrame
    data = pd.DataFrame({"x": [1, 2, 3], "vector": [[1, 2], [3, 4], [5, 6]]})

    tbl = db.create_table("test_table", data, mode="overwrite")
    # Update the table where x = 2
    tbl.update(where="x = 2", values={"vector": [10, 10]})
    # Get the updated table as a pandas DataFrame
    df = tbl.to_pandas()
    print(df)
    # --8<-- [end:update_table]
    # --8<-- [start:update_table_sql]
    # Update the table where x = 2
    tbl.update(values_sql={"x": "x + 1"})
    print(tbl.to_pandas())
    # --8<-- [end:update_table_sql]
    # --8<-- [start:table_strong_consistency]
    uri = "data/sample-lancedb"
    db = lancedb.connect(uri, read_consistency_interval=timedelta(0))
    tbl = db.open_table("test_table")
    # --8<-- [end:table_strong_consistency]
    # --8<-- [start:table_eventual_consistency]
    uri = "data/sample-lancedb"
    db = lancedb.connect(uri, read_consistency_interval=timedelta(seconds=5))
    tbl = db.open_table("test_table")
    # --8<-- [end:table_eventual_consistency]
    # --8<-- [start:table_checkout_latest]
    tbl = db.open_table("test_table")

    # (Other writes happen to my_table from another process)

    # Check for updates
    tbl.checkout_latest()
    # --8<-- [end:table_checkout_latest]


@pytest.mark.skip
def test_table_with_embedding():
    db = lancedb.connect("data/sample-lancedb")
    # --8<-- [start:create_table_with_embedding]
    embed_fcn = get_registry().get("huggingface").create(name="BAAI/bge-small-en-v1.5")

    class Schema(LanceModel):
        text: str = embed_fcn.SourceField()
        vector: Vector(embed_fcn.ndims()) = embed_fcn.VectorField(default=None)

    tbl = db.create_table("my_table_with_embedding", schema=Schema, mode="overwrite")
    models = [Schema(text="hello"), Schema(text="world")]
    tbl.add(models)
    # --8<-- [end:create_table_with_embedding]


@pytest.mark.skip
async def test_table_with_embedding_async():
    async_db = await lancedb.connect_async("data/sample-lancedb")
    # --8<-- [start:create_table_async_with_embedding]
    embed_fcn = get_registry().get("huggingface").create(name="BAAI/bge-small-en-v1.5")

    class Schema(LanceModel):
        text: str = embed_fcn.SourceField()
        vector: Vector(embed_fcn.ndims()) = embed_fcn.VectorField(default=None)

    async_tbl = await async_db.create_table(
        "my_table_async_with_embedding", schema=Schema, mode="overwrite"
    )
    models = [Schema(text="hello"), Schema(text="world")]
    await async_tbl.add(models)
    # --8<-- [end:create_table_async_with_embedding]


@pytest.mark.asyncio
async def test_table_async():
    # --8<-- [start:connect_async]
    uri = "data/sample-lancedb"
    async_db = await lancedb.connect_async(uri)
    # --8<-- [end:connect_async]
    # --8<-- [start:create_table_async]
    data = [
        {"vector": [1.1, 1.2], "lat": 45.5, "long": -122.7},
        {"vector": [0.2, 1.8], "lat": 40.1, "long": -74.1},
    ]
    async_tbl = await async_db.create_table("test_table_async", data)
    await async_tbl.head()
    # --8<-- [end:create_table_async]
    # --8<-- [start:create_table_async_exist_ok]
    await async_db.create_table("test_table_async", data, exist_ok=True)
    # --8<-- [end:create_table_async_exist_ok]
    # --8<-- [start:create_table_async_overwrite]
    await async_db.create_table("test_table_async", data, mode="overwrite")
    # --8<-- [end:create_table_async_overwrite]
    # --8<-- [start:create_table_async_from_pandas]
    data = pd.DataFrame(
        {
            "vector": [[1.1, 1.2, 1.3, 1.4], [0.2, 1.8, 0.4, 3.6]],
            "lat": [45.5, 40.1],
            "long": [-122.7, -74.1],
        }
    )
    async_tbl = await async_db.create_table("my_table_async_pd", data)
    await async_tbl.head()
    # --8<-- [end:create_table_async_from_pandas]
    # --8<-- [start:create_table_async_custom_schema]
    custom_schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 4)),
            pa.field("lat", pa.float32()),
            pa.field("long", pa.float32()),
        ]
    )
    async_tbl = await async_db.create_table(
        "my_table_async_custom_schema", data, schema=custom_schema
    )
    # --8<-- [end:create_table_async_custom_schema]
    # --8<-- [start:create_table_async_from_polars]
    data = pl.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    async_tbl = await async_db.create_table("my_table_async_pl", data)
    # --8<-- [end:create_table_async_from_polars]
    # --8<-- [start:create_table_async_from_arrow_table]
    dim = 16
    total = 2
    schema = pa.schema(
        [pa.field("vector", pa.list_(pa.float16(), dim)), pa.field("text", pa.string())]
    )
    data = pa.Table.from_arrays(
        [
            pa.array(
                [np.random.randn(dim).astype(np.float16) for _ in range(total)],
                pa.list_(pa.float16(), dim),
            ),
            pa.array(["foo", "bar"]),
        ],
        ["vector", "text"],
    )
    async_tbl = await async_db.create_table("f16_tbl_async", data, schema=schema)
    # --8<-- [end:create_table_async_from_arrow_table]
    # --8<-- [start:create_table_async_from_pydantic]
    async_tbl = await async_db.create_table("movielens_small_async", schema=Content)
    # --8<-- [end:create_table_async_from_pydantic]
    # --8<-- [start:create_table_async_nested_schema]
    async_tbl = await async_db.create_table("nested_table_async", schema=NestedSchema)
    # --8<-- [end:create_table_async_nested_schema]
    # --8<-- [start:create_table_async_from_batch]
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 4)),
            pa.field("item", pa.utf8()),
            pa.field("price", pa.float32()),
        ]
    )
    await async_db.create_table("batched_table", make_batches(), schema=schema)
    # --8<-- [end:create_table_async_from_batch]
    # --8<-- [start:list_tables_async]
    print(await async_db.table_names())
    # --8<-- [end:list_tables_async]
    # --8<-- [start:open_table_async]
    async_tbl = await async_db.open_table("test_table_async")
    # --8<-- [end:open_table_async]
    # --8<-- [start:create_empty_table_async]
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.string()),
            pa.field("price", pa.float32()),
        ]
    )
    async_tbl = await async_db.create_table("test_empty_table_async", schema=schema)
    # --8<-- [end:create_empty_table_async]
    # --8<-- [start:create_empty_table_async_pydantic]
    async_tbl = await async_db.create_table(
        "test_empty_table_async_new", schema=Item.to_arrow_schema()
    )
    # --8<-- [end:create_empty_table_async_pydantic]
    # --8<-- [start:add_table_async_from_pandas]
    df = pd.DataFrame(
        {
            "vector": [[1.3, 1.4], [9.5, 56.2]],
            "item": ["banana", "apple"],
            "price": [5.0, 7.0],
        }
    )
    await async_tbl.add(df)
    # --8<-- [end:add_table_async_from_pandas]
    # --8<-- [start:add_table_async_from_polars]
    df = pl.DataFrame(
        {
            "vector": [[1.3, 1.4], [9.5, 56.2]],
            "item": ["banana", "apple"],
            "price": [5.0, 7.0],
        }
    )
    await async_tbl.add(df)
    # --8<-- [end:add_table_async_from_polars]
    # --8<-- [start:add_table_async_from_batch]
    await async_tbl.add(make_batches_for_add())
    # --8<-- [end:add_table_async_from_batch]
    # --8<-- [start:add_table_async_from_pyarrow]
    pa_table = pa.Table.from_arrays(
        [
            pa.array([[9.1, 6.7], [9.9, 31.2]], pa.list_(pa.float32(), 2)),
            pa.array(["mango", "orange"]),
            pa.array([7.0, 4.0]),
        ],
        ["vector", "item", "price"],
    )
    await async_tbl.add(pa_table)
    # --8<-- [end:add_table_async_from_pyarrow]
    # --8<-- [start:add_table_async_from_pydantic]
    pydantic_model_items = [
        Item(vector=[8.1, 4.7], item="pineapple", price=10.0),
        Item(vector=[6.9, 9.3], item="avocado", price=9.0),
    ]
    await async_tbl.add(pydantic_model_items)
    # --8<-- [end:add_table_async_from_pydantic]
    # --8<-- [start:delete_row_async]
    await async_tbl.delete('item = "fizz"')
    # --8<-- [end:delete_row_async]
    # --8<-- [start:delete_specific_row_async]
    data = [
        {"x": 1, "vector": [1, 2]},
        {"x": 2, "vector": [3, 4]},
        {"x": 3, "vector": [5, 6]},
    ]
    async_db = await lancedb.connect_async(uri)
    async_tbl = await async_db.create_table("delete_row_async", data)
    await async_tbl.to_pandas()
    #   x      vector
    # 0  1  [1.0, 2.0]
    # 1  2  [3.0, 4.0]
    # 2  3  [5.0, 6.0]

    await async_tbl.delete("x = 2")
    await async_tbl.to_pandas()
    #   x      vector
    # 0  1  [1.0, 2.0]
    # 1  3  [5.0, 6.0]
    # --8<-- [end:delete_specific_row_async]
    # --8<-- [start:delete_list_values_async]
    to_remove = [1, 5]
    to_remove = ", ".join(str(v) for v in to_remove)

    await async_tbl.delete(f"x IN ({to_remove})")
    await async_tbl.to_pandas()
    #   x      vector
    # 0  3  [5.0, 6.0]
    # --8<-- [end:delete_list_values_async]
    # --8<-- [start:update_table_async]
    # Create a table from a pandas DataFrame
    data = pd.DataFrame({"x": [1, 2, 3], "vector": [[1, 2], [3, 4], [5, 6]]})

    async_tbl = await async_db.create_table("update_table_async", data)
    # Update the table where x = 2
    await async_tbl.update({"vector": [10, 10]}, where="x = 2")
    # Get the updated table as a pandas DataFrame
    df = await async_tbl.to_pandas()
    # Print the DataFrame
    print(df)
    # --8<-- [end:update_table_async]
    # --8<-- [start:update_table_sql_async]
    # Update the table where x = 2
    await async_tbl.update(updates_sql={"x": "x + 1"})
    print(await async_tbl.to_pandas())
    # --8<-- [end:update_table_sql_async]
    # --8<-- [start:table_async_strong_consistency]
    uri = "data/sample-lancedb"
    async_db = await lancedb.connect_async(uri, read_consistency_interval=timedelta(0))
    async_tbl = await async_db.open_table("test_table_async")
    # --8<-- [end:table_async_strong_consistency]
    # --8<-- [start:table_async_eventual_consistency]
    uri = "data/sample-lancedb"
    async_db = await lancedb.connect_async(
        uri, read_consistency_interval=timedelta(seconds=5)
    )
    async_tbl = await async_db.open_table("test_table_async")
    # --8<-- [end:table_async_eventual_consistency]
    # --8<-- [start:table_async_checkout_latest]
    async_tbl = await async_db.open_table("test_table_async")

    # (Other writes happen to test_table_async from another process)

    # Check for updates
    await async_tbl.checkout_latest()
    # --8<-- [end:table_async_checkout_latest]
