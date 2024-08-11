# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from datetime import timedelta
import random

import pyarrow as pa
import pytest
import pytest_asyncio
from lancedb import AsyncConnection, AsyncTable, connect_async
from lancedb.index import BTree, IvfPq, Bitmap, LabelList


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


@pytest.mark.asyncio
async def test_create_scalar_index(some_table: AsyncTable):
    # Can create
    await some_table.create_index("id")
    # Can recreate if replace=True
    await some_table.create_index("id", replace=True)
    indices = await some_table.list_indices()
    assert str(indices) == '[Index(BTree, columns=["id"])]'
    assert len(indices) == 1
    assert indices[0].index_type == "BTree"
    assert indices[0].columns == ["id"]
    # Can't recreate if replace=False
    with pytest.raises(RuntimeError, match="already exists"):
        await some_table.create_index("id", replace=False)
    # can also specify index type
    await some_table.create_index("id", config=BTree())


@pytest.mark.asyncio
async def test_create_bitmap_index(some_table: AsyncTable):
    await some_table.create_index("id", config=Bitmap())
    # TODO: Fix via https://github.com/lancedb/lance/issues/2039
    # indices = await some_table.list_indices()
    # assert str(indices) == '[Index(Bitmap, columns=["id"])]'


@pytest.mark.asyncio
async def test_create_label_list_index(some_table: AsyncTable):
    await some_table.create_index("tags", config=LabelList())
    # TODO: Fix via https://github.com/lancedb/lance/issues/2039
    # indices = await some_table.list_indices()
    # assert str(indices) == '[Index(LabelList, columns=["id"])]'


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
