# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from datetime import timedelta
import random
from typing import get_args, get_type_hints

import pyarrow as pa
import pytest
import pytest_asyncio
from lancedb import AsyncConnection, AsyncTable, connect_async
from lancedb.index import (
    BTree,
    IvfFlat,
    IvfPq,
    IvfSq,
    IvfHnswPq,
    IvfHnswSq,
    IvfHnswFlat,
    IvfRq,
    Bitmap,
    LabelList,
    Fm,
    HnswPq,
    HnswSq,
    HnswFlat,
    FTS,
)
from lancedb.table import IndexStatistics


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
            "fsb": pa.array([bytes([i]) for i in range(NROWS)], pa.binary(1)),
            "tags": [
                [f"tag{random.randint(0, 8)}" for _ in range(2)] for _ in range(NROWS)
            ],
            "is_active": [random.choice([True, False]) for _ in range(NROWS)],
            "data": [random.randbytes(random.randint(0, 128)) for _ in range(NROWS)],
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
    assert str(indices).startswith(
        '[IndexConfig(name="id_idx", index_type="BTree", columns=["id"]'
    )
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
async def test_create_nested_scalar_index_lists_canonical_paths(db_async):
    metadata_type = pa.struct(
        [
            pa.field("user_id", pa.int32()),
            pa.field("user.id", pa.int32()),
        ]
    )
    mixed_case_metadata_type = pa.struct([pa.field("userId", pa.int32())])
    escaped_metadata_type = pa.struct([pa.field("user-id", pa.int32())])
    literal_type = pa.struct([pa.field("a.b", pa.int32())])
    data = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array(
                [
                    {"user_id": 10, "user.id": 100},
                    {"user_id": 20, "user.id": 200},
                    {"user_id": 30, "user.id": 300},
                ],
                type=metadata_type,
            ),
            pa.array(
                [{"userId": 10}, {"userId": 20}, {"userId": 30}],
                type=mixed_case_metadata_type,
            ),
            pa.array(
                [{"user-id": 10}, {"user-id": 20}, {"user-id": 30}],
                type=escaped_metadata_type,
            ),
            pa.array(
                [{"a.b": 10}, {"a.b": 20}, {"a.b": 30}],
                type=literal_type,
            ),
        ],
        names=[
            "rowId",
            "row-id",
            "userId",
            "user_id",
            "metadata",
            "MetaData",
            "meta-data",
            "literal",
        ],
    )
    table = await db_async.create_table("nested_scalar_index", data)

    await table.create_index("rowId", config=BTree(), name="row_id_idx")
    await table.create_index("`row-id`", config=BTree(), name="row_dash_id_idx")
    await table.create_index("userId", config=BTree(), name="top_user_id_idx")
    await table.create_index("user_id", config=BTree(), name="top_snake_user_id_idx")
    await table.create_index(
        "metadata.user_id", config=BTree(), name="nested_user_id_idx"
    )
    await table.create_index(
        "metadata.`user.id`", config=BTree(), name="escaped_user_id_idx"
    )
    await table.create_index(
        "MetaData.userId", config=BTree(), name="mixed_case_metadata_user_id_idx"
    )
    await table.create_index(
        "`meta-data`.`user-id`", config=BTree(), name="escaped_names_idx"
    )
    await table.create_index("literal.`a.b`", config=BTree(), name="literal_dot_idx")

    columns_by_name = {
        index.name: index.columns for index in await table.list_indices()
    }
    assert columns_by_name["row_id_idx"] == ["rowId"]
    assert columns_by_name["row_dash_id_idx"] == ["`row-id`"]
    assert columns_by_name["top_user_id_idx"] == ["userId"]
    assert columns_by_name["top_snake_user_id_idx"] == ["user_id"]
    assert columns_by_name["nested_user_id_idx"] == ["metadata.user_id"]
    assert columns_by_name["escaped_user_id_idx"] == ["metadata.`user.id`"]
    assert columns_by_name["mixed_case_metadata_user_id_idx"] == ["MetaData.userId"]
    assert columns_by_name["escaped_names_idx"] == ["`meta-data`.`user-id`"]
    assert columns_by_name["literal_dot_idx"] == ["literal.`a.b`"]

    for index_name in columns_by_name:
        stats = await table.index_stats(index_name)
        assert stats is not None
        assert stats.num_indexed_rows == 3


@pytest.mark.asyncio
async def test_create_fixed_size_binary_index(some_table: AsyncTable):
    await some_table.create_index("fsb", config=BTree())
    indices = await some_table.list_indices()
    assert str(indices).startswith(
        '[IndexConfig(name="fsb_idx", index_type="BTree", columns=["fsb"]'
    )
    assert len(indices) == 1
    assert indices[0].index_type == "BTree"
    assert indices[0].columns == ["fsb"]


@pytest.mark.asyncio
async def test_create_fm_index(some_table: AsyncTable):
    # FM-Index accelerates substring search on string/binary columns.
    await some_table.create_index("data", config=Fm())
    indices = await some_table.list_indices()
    assert len(indices) == 1
    assert indices[0].index_type == "Fm"
    assert indices[0].columns == ["data"]


@pytest.mark.asyncio
async def test_create_bitmap_index(some_table: AsyncTable):
    await some_table.create_index("id", config=Bitmap())
    await some_table.create_index("is_active", config=Bitmap())
    await some_table.create_index("data", config=Bitmap())
    indices = await some_table.list_indices()
    assert len(indices) == 3
    # list_indices returns indices in alphabetical order by name
    assert indices[0].index_type == "Bitmap"
    assert indices[0].columns == ["data"]
    assert indices[1].index_type == "Bitmap"
    assert indices[1].columns == ["id"]
    assert indices[2].index_type == "Bitmap"
    assert indices[2].columns == ["is_active"]

    index_name = indices[0].name
    stats = await some_table.index_stats(index_name)
    assert stats.index_type == "BITMAP"
    assert stats.distance_type is None
    assert stats.num_indexed_rows == await some_table.count_rows()
    assert stats.num_unindexed_rows == 0
    assert stats.num_indices == 1

    assert (
        "ScalarIndexQuery"
        in await some_table.query().where("is_active = TRUE").explain_plan()
    )


@pytest.mark.asyncio
async def test_create_label_list_index(some_table: AsyncTable):
    await some_table.create_index("tags", config=LabelList())
    indices = await some_table.list_indices()
    assert str(indices).startswith(
        '[IndexConfig(name="tags_idx", index_type="LabelList", columns=["tags"]'
    )
    plan = await some_table.query().where("array_has(tags, 'tag0')").explain_plan()
    assert "ScalarIndexQuery" in plan


@pytest.mark.asyncio
async def test_create_large_list_label_list_index(db_async):
    data = pa.Table.from_pydict(
        {"tags": [[f"tag{i % 2}", "shared"] for i in range(16)]},
        schema=pa.schema([pa.field("tags", pa.large_list(pa.string()))]),
    )
    table = await db_async.create_table("large_list_label_list_index", data)

    await table.create_index("tags", config=LabelList())
    indices = await table.list_indices()
    assert str(indices).startswith(
        '[IndexConfig(name="tags_idx", index_type="LabelList", columns=["tags"]'
    )
    plan = await table.query().where("array_has(tags, 'shared')").explain_plan()
    assert "ScalarIndexQuery" in plan


@pytest.mark.asyncio
async def test_create_label_list_index_rejects_list_struct(db_async):
    item_type = pa.struct(
        [
            pa.field("tag", pa.string()),
            pa.field(
                "metadata",
                pa.struct([pa.field("userId", pa.string())]),
            ),
        ]
    )
    data = pa.Table.from_pylist(
        [
            {
                "items": [
                    {"tag": "tag0", "metadata": {"userId": "user0"}},
                    {"tag": "shared", "metadata": {"userId": "user1"}},
                ]
            }
        ],
        schema=pa.schema([pa.field("items", pa.list_(item_type))]),
    )
    table = await db_async.create_table("list_struct_label_list_index", data)

    with pytest.raises(Exception, match="LabelList index cannot be created"):
        await table.create_index("items", config=LabelList())


@pytest.mark.asyncio
async def test_full_text_search_index(some_table: AsyncTable):
    await some_table.create_index("tags", config=FTS(with_position=False))
    indices = await some_table.list_indices()
    assert str(indices).startswith(
        '[IndexConfig(name="tags_idx", index_type="FTS", columns=["tags"]'
    )

    await some_table.prewarm_index("tags_idx")

    res = await (await some_table.search("tag0")).to_arrow()
    assert res.num_rows > 0


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
async def test_create_ivfrq_index(some_table: AsyncTable):
    await some_table.create_index("vector", config=IvfRq(num_bits=1))
    indices = await some_table.list_indices()
    assert len(indices) == 1
    assert indices[0].index_type == "IvfRq"
    assert indices[0].columns == ["vector"]
    assert indices[0].name == "vector_idx"


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
async def test_create_hnswsq_alias_index(some_table: AsyncTable):
    await some_table.create_index("vector", config=IvfHnswSq(num_partitions=5))
    indices = await some_table.list_indices()
    assert len(indices) == 1
    assert indices[0].index_type in {"HnswSq", "IvfHnswSq"}


@pytest.mark.asyncio
async def test_create_hnswpq_alias_index(some_table: AsyncTable):
    await some_table.create_index("vector", config=IvfHnswPq(num_partitions=5))
    indices = await some_table.list_indices()
    assert len(indices) == 1
    assert indices[0].index_type in {"HnswPq", "IvfHnswPq"}


@pytest.mark.asyncio
async def test_create_hnswflat_index(some_table: AsyncTable):
    await some_table.create_index("vector", config=HnswFlat(num_partitions=10))
    indices = await some_table.list_indices()
    assert len(indices) == 1


@pytest.mark.asyncio
async def test_create_hnswflat_alias_index(some_table: AsyncTable):
    await some_table.create_index("vector", config=IvfHnswFlat(num_partitions=5))
    indices = await some_table.list_indices()
    assert len(indices) == 1
    assert indices[0].index_type in {"HnswFlat", "IvfHnswFlat"}


@pytest.mark.asyncio
async def test_create_ivfsq_index(some_table: AsyncTable):
    await some_table.create_index("vector", config=IvfSq(num_partitions=10))
    indices = await some_table.list_indices()
    assert len(indices) == 1
    assert indices[0].index_type == "IvfSq"
    stats = await some_table.index_stats(indices[0].name)
    assert stats.index_type == "IVF_SQ"
    assert stats.distance_type == "l2"
    assert stats.num_indexed_rows == await some_table.count_rows()
    assert stats.num_unindexed_rows == 0


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


def test_index_statistics_index_type_lists_all_supported_values():
    expected_index_types = {
        "IVF_FLAT",
        "IVF_SQ",
        "IVF_PQ",
        "IVF_RQ",
        "IVF_HNSW_SQ",
        "IVF_HNSW_PQ",
        "IVF_HNSW_FLAT",
        "FTS",
        "BTREE",
        "BITMAP",
        "LABEL_LIST",
    }

    assert (
        set(get_args(get_type_hints(IndexStatistics)["index_type"]))
        == expected_index_types
    )
