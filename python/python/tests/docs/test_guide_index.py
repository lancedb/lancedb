# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

# --8<-- [start:import-lancedb]
import lancedb

# --8<-- [end:import-lancedb]
# --8<-- [start:import-lancedb-ivfpq]
from lancedb.index import IvfPq

# --8<-- [end:import-lancedb-ivfpq]
# --8<-- [start:import-lancedb-btree-bitmap]
from lancedb.index import BTree, Bitmap

# --8<-- [end:import-lancedb-btree-bitmap]
# --8<-- [start:import-numpy]
import numpy as np

# --8<-- [end:import-numpy]
import pytest


def test_ann_index():
    # --8<-- [start:create_ann_index]
    uri = "data/sample-lancedb"

    # Create 5,000 sample vectors
    data = [
        {"vector": row, "item": f"item {i}"}
        for i, row in enumerate(np.random.random((5_000, 32)).astype("float32"))
    ]

    db = lancedb.connect(uri)
    # Add the vectors to a table
    tbl = db.create_table("my_vectors", data=data)
    # Create and train the index - you need to have enough data in the table
    # for an effective training step
    tbl.create_index(num_partitions=2, num_sub_vectors=4)
    # --8<-- [end:create_ann_index]
    # --8<-- [start:vector_search]
    tbl.search(np.random.random((32))).limit(2).nprobes(20).refine_factor(
        10
    ).to_pandas()
    # --8<-- [end:vector_search]
    # --8<-- [start:vector_search_with_filter]
    tbl.search(np.random.random((32))).where("item != 'item 1141'").to_pandas()
    # --8<-- [end:vector_search_with_filter]
    # --8<-- [start:vector_search_with_select]
    tbl.search(np.random.random((32))).select(["vector"]).to_pandas()
    # --8<-- [end:vector_search_with_select]


@pytest.mark.asyncio
async def test_ann_index_async():
    # --8<-- [start:create_ann_index_async]
    uri = "data/sample-lancedb"

    # Create 5,000 sample vectors
    data = [
        {"vector": row, "item": f"item {i}"}
        for i, row in enumerate(np.random.random((5_000, 32)).astype("float32"))
    ]

    async_db = await lancedb.connect_async(uri)
    # Add the vectors to a table
    async_tbl = await async_db.create_table("my_vectors_async", data=data)
    # Create and train the index - you need to have enough data in the table
    # for an effective training step
    await async_tbl.create_index(
        "vector", config=IvfPq(num_partitions=2, num_sub_vectors=4)
    )
    # --8<-- [end:create_ann_index_async]
    # --8<-- [start:vector_search_async]
    await (
        (await async_tbl.search(np.random.random((32))))
        .limit(2)
        .nprobes(20)
        .refine_factor(10)
        .to_pandas()
    )
    # --8<-- [end:vector_search_async]
    # --8<-- [start:vector_search_async_with_filter]
    await (
        (await async_tbl.search(np.random.random((32))))
        .where("item != 'item 1141'")
        .to_pandas()
    )
    # --8<-- [end:vector_search_async_with_filter]
    # --8<-- [start:vector_search_async_with_select]
    await (
        (await async_tbl.search(np.random.random((32)))).select(["vector"]).to_pandas()
    )
    # --8<-- [end:vector_search_async_with_select]


def test_scalar_index():
    # --8<-- [start:basic_scalar_index]
    uri = "data/sample-lancedb"
    db = lancedb.connect(uri)
    books = [
        {
            "book_id": 1,
            "publisher": "plenty of books",
            "tags": ["fantasy", "adventure"],
        },
        {"book_id": 2, "publisher": "book town", "tags": ["non-fiction"]},
        {"book_id": 3, "publisher": "oreilly", "tags": ["textbook"]},
    ]
    table = db.create_table("books", books)
    table.create_scalar_index("book_id")  # BTree by default
    table.create_scalar_index("publisher", index_type="BITMAP")
    # --8<-- [end:basic_scalar_index]
    # --8<-- [start:search_with_scalar_index]
    table = db.open_table("books")
    table.search().where("book_id = 2").to_pandas()
    # --8<-- [end:search_with_scalar_index]
    # --8<-- [start:vector_search_with_scalar_index]
    data = [
        {"book_id": 1, "vector": [1.0, 2]},
        {"book_id": 2, "vector": [3.0, 4]},
        {"book_id": 3, "vector": [5.0, 6]},
    ]

    table = db.create_table("book_with_embeddings", data)
    (table.search([1, 2]).where("book_id != 3", prefilter=True).to_pandas())
    # --8<-- [end:vector_search_with_scalar_index]
    # --8<-- [start:update_scalar_index]
    table.add([{"vector": [7, 8], "book_id": 4}])
    table.optimize()
    # --8<-- [end:update_scalar_index]


@pytest.mark.asyncio
async def test_scalar_index_async():
    # --8<-- [start:basic_scalar_index_async]
    uri = "data/sample-lancedb"
    async_db = await lancedb.connect_async(uri)
    books = [
        {
            "book_id": 1,
            "publisher": "plenty of books",
            "tags": ["fantasy", "adventure"],
        },
        {"book_id": 2, "publisher": "book town", "tags": ["non-fiction"]},
        {"book_id": 3, "publisher": "oreilly", "tags": ["textbook"]},
    ]
    async_tbl = await async_db.create_table("books_async", books)
    await async_tbl.create_index("book_id", config=BTree())  # BTree by default
    await async_tbl.create_index("publisher", config=Bitmap())
    # --8<-- [end:basic_scalar_index_async]
    # --8<-- [start:search_with_scalar_index_async]
    async_tbl = await async_db.open_table("books_async")
    await async_tbl.query().where("book_id = 2").to_pandas()
    # --8<-- [end:search_with_scalar_index_async]
    # --8<-- [start:vector_search_with_scalar_index_async]
    data = [
        {"book_id": 1, "vector": [1.0, 2]},
        {"book_id": 2, "vector": [3.0, 4]},
        {"book_id": 3, "vector": [5.0, 6]},
    ]
    async_tbl = await async_db.create_table("book_with_embeddings_async", data)
    (await (await async_tbl.search([1, 2])).where("book_id != 3").to_pandas())
    # --8<-- [end:vector_search_with_scalar_index_async]
    # --8<-- [start:update_scalar_index_async]
    await async_tbl.add([{"vector": [7, 8], "book_id": 4}])
    await async_tbl.optimize()
    # --8<-- [end:update_scalar_index_async]
