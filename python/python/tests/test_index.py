from datetime import timedelta

import pyarrow as pa
import pytest
import pytest_asyncio
from lancedb import AsyncConnection, AsyncTable, connect_async
from lancedb.index import Index


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
            "id": list(range(256)),
            "vector": sample_fixed_size_list_array(NROWS, DIM),
        }
    )
    return await db_async.create_table(
        "some_table",
        data,
    )


@pytest_asyncio.fixture
async def two_vec_columns_table(db_async):
    data = pa.Table.from_pydict(
        {
            "id": list(range(256)),
            "vector1": sample_fixed_size_list_array(NROWS, DIM),
            "vector2": sample_fixed_size_list_array(NROWS, DIM),
        }
    )
    return await db_async.create_table(
        "some_table",
        data,
    )


@pytest.mark.asyncio
async def test_create_scalar_index(some_table: AsyncTable):
    # Can create
    await some_table.create_index(Index.btree(), column="id")
    # Can recreate if replace=True
    await some_table.create_index(Index.btree(), column="id", replace=True)
    # Can't recreate if replace=False
    with pytest.raises(RuntimeError, match="already exists"):
        await some_table.create_index(Index.btree(), column="id", replace=False)
    # Can't create without column
    with pytest.raises(ValueError, match="column must be specified"):
        await some_table.create_index(Index.btree())


@pytest.mark.asyncio
async def test_create_vector_index(some_table: AsyncTable):
    # Can create
    await some_table.create_index(Index.ivf_pq())
    # Can recreate if replace=True
    await some_table.create_index(Index.ivf_pq(), replace=True)
    # Can't recreate if replace=False
    with pytest.raises(RuntimeError, match="already exists"):
        await some_table.create_index(Index.ivf_pq(), replace=False)


@pytest.mark.asyncio
async def test_create_vector_index_two_vector_cols(
    two_vec_columns_table: AsyncTable,
):
    # Cannot create if column not specified
    with pytest.raises(ValueError, match="specify the column to index"):
        await two_vec_columns_table.create_index(Index.ivf_pq())
    # Can create if column is specified
    await two_vec_columns_table.create_index(Index.ivf_pq(), column="vector1")
