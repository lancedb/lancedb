# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from datetime import timedelta
import random

import pyarrow as pa
import pytest
import pytest_asyncio
from lancedb import AsyncConnection, AsyncTable, connect_async
from lancedb.index import BTree, IvfFlat, IvfPq, Bitmap, LabelList, HnswPq, HnswSq


@pytest_asyncio.fixture
async def db_async(tmp_path) -> AsyncConnection:
    return await connect_async(tmp_path, read_consistency_interval=timedelta(seconds=0))


def sample_fixed_size_list_array(nrows, dim):
    vector_data = pa.array([float(i) for i in range(dim * nrows)], pa.float32())
    return pa.FixedSizeListArray.from_arrays(vector_data, dim)


DIM = 8
NROWS = 256


@pytest_asyncio.fixture
async def some_table(db_async):
    data = pa.Table.from_pydict(
        {
            "id": list(range(NROWS)),
            "vector": sample_fixed_size_list_array(NROWS, DIM),
            "tags": [
                [f"tag{random.randint(0, 8)}" for _ in range(2)] for _ in range(NROWS)
            ],
        }
    )
    return await db_async.create_table(
        "some_table",
        data,
    )


@pytest_asyncio.fixture
async def binary_table(db_async):
    data = [
        {
            "id": i,
            "vector": [i] * 128,
        }
        for i in range(NROWS)
    ]
    return await db_async.create_table(
        "binary_table",
        data,
        schema=pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.uint8(), 128)),
            ]
        ),
    )


@pytest.mark.asyncio
async def test_create_scalar_index(some_table: AsyncTable):
    # Can create
    await some_table.create_index("id")
    # Can recreate if replace=True
    await some_table.create_index("id", replace=True)
    indices = await some_table.list_indices()
    assert str(indices) == '[Index(BTree, columns=["id"], name="id_idx")]'
    assert len(indices) == 1
    assert indices[0].index_type == "BTree"
    assert indices[0].columns == ["id"]
    # Can't recreate if replace=False
    with pytest.raises(RuntimeError, match="already exists"):
        await some_table.create_index("id", replace=False)
    # can also specify index type
    await some_table.create_index("id", config=BTree())

    await some_table.drop_index("id_idx")
    indices = await some_table.list_indices()
    assert len(indices) == 0


@pytest.mark.asyncio
async def test_create_bitmap_index(some_table: AsyncTable):
    await some_table.create_index("id", config=Bitmap())
    indices = await some_table.list_indices()
    assert str(indices) == '[Index(Bitmap, columns=["id"], name="id_idx")]'
    indices = await some_table.list_indices()
    assert len(indices) == 1
    index_name = indices[0].name
    stats = await some_table.index_stats(index_name)
    assert stats.index_type == "BITMAP"
    assert stats.distance_type is None
    assert stats.num_indexed_rows == await some_table.count_rows()
    assert stats.num_unindexed_rows == 0
    assert stats.num_indices == 1


@pytest.mark.asyncio
async def test_create_label_list_index(some_table: AsyncTable):
    await some_table.create_index("tags", config=LabelList())
    indices = await some_table.list_indices()
    assert str(indices) == '[Index(LabelList, columns=["tags"], name="tags_idx")]'


@pytest.mark.asyncio
async def test_create_vector_index(some_table: AsyncTable):
    # Can create
    await some_table.create_index("vector")
    # Can recreate if replace=True
    await some_table.create_index("vector", replace=True)
    # Can't recreate if replace=False
    with pytest.raises(RuntimeError, match="already exists"):
        await some_table.create_index("vector", replace=False)
    # Can also specify index type
    await some_table.create_index("vector", config=IvfPq(num_partitions=100))
    indices = await some_table.list_indices()
    assert len(indices) == 1
    assert indices[0].index_type == "IvfPq"
    assert indices[0].columns == ["vector"]
    assert indices[0].name == "vector_idx"

    stats = await some_table.index_stats("vector_idx")
    assert stats.index_type == "IVF_PQ"
    assert stats.distance_type == "l2"
    assert stats.num_indexed_rows == await some_table.count_rows()
    assert stats.num_unindexed_rows == 0
    assert stats.num_indices == 1


@pytest.mark.asyncio
async def test_create_4bit_ivfpq_index(some_table: AsyncTable):
    # Can create
    await some_table.create_index("vector", config=IvfPq(num_bits=4))
    # Can recreate if replace=True
    await some_table.create_index("vector", config=IvfPq(num_bits=4), replace=True)
    # Can't recreate if replace=False
    with pytest.raises(RuntimeError, match="already exists"):
        await some_table.create_index("vector", replace=False)
    indices = await some_table.list_indices()
    assert len(indices) == 1
    assert indices[0].index_type == "IvfPq"
    assert indices[0].columns == ["vector"]
    assert indices[0].name == "vector_idx"

    stats = await some_table.index_stats("vector_idx")
    assert stats.index_type == "IVF_PQ"
    assert stats.distance_type == "l2"
    assert stats.num_indexed_rows == await some_table.count_rows()
    assert stats.num_unindexed_rows == 0
    assert stats.num_indices == 1


@pytest.mark.asyncio
async def test_create_hnswpq_index(some_table: AsyncTable):
    await some_table.create_index("vector", config=HnswPq(num_partitions=10))
    indices = await some_table.list_indices()
    assert len(indices) == 1


@pytest.mark.asyncio
async def test_create_hnswsq_index(some_table: AsyncTable):
    await some_table.create_index("vector", config=HnswSq(num_partitions=10))
    indices = await some_table.list_indices()
    assert len(indices) == 1


@pytest.mark.asyncio
async def test_create_index_with_binary_vectors(binary_table: AsyncTable):
    await binary_table.create_index(
        "vector", config=IvfFlat(distance_type="hamming", num_partitions=10)
    )
    indices = await binary_table.list_indices()
    assert len(indices) == 1
    assert indices[0].index_type == "IvfFlat"
    assert indices[0].columns == ["vector"]
    assert indices[0].name == "vector_idx"

    stats = await binary_table.index_stats("vector_idx")
    assert stats.index_type == "IVF_FLAT"
    assert stats.distance_type == "hamming"
    assert stats.num_indexed_rows == await binary_table.count_rows()
    assert stats.num_unindexed_rows == 0
    assert stats.num_indices == 1

    # the dataset contains vectors with all values from 0 to 255
    for v in range(256):
        res = await binary_table.query().nearest_to([v] * 128).to_arrow()
        assert res["id"][0].as_py() == v
