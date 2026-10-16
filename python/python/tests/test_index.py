# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from datetime import timedelta
import random
import struct
from typing import get_args, get_type_hints

import pyarrow as pa
import pytest
import pytest_asyncio
import lancedb
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
    RTree,
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
async def test_create_index_async_returns_done_job(some_table: AsyncTable):
    job = await some_table.create_index_async("id", config=BTree())
    assert job.id is None
    assert await job.wait() is None
    assert len(await some_table.list_indices()) == 1
    await job.cancel()


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
async def test_index_config_repr(db_async):
    # Use >= 1000 rows so the thousands separator in the repr is exercised.
    nrows = 1500
    table = await db_async.create_table(
        "repr_table", pa.Table.from_pydict({"id": list(range(nrows))})
    )
    await table.create_index("id", config=BTree())
    indices = await table.list_indices()
    assert len(indices) == 1

    r = repr(indices[0])
    assert r.startswith('IndexConfig(name="id_idx", index_type="BTree", columns=["id"]')
    # Integer counts use `_` thousands separators (valid Python int syntax).
    assert "num_indexed_rows=1_500" in r
    assert "num_unindexed_rows=0" in r
    # created_at renders as a datetime so the value round-trips.
    assert "created_at=datetime.datetime(" in r
    assert r.endswith(")")


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


POINT_TYPE = pa.struct([("x", pa.float64()), ("y", pa.float64())])
POLYGON_TYPE = pa.list_(pa.field("rings", pa.list_(pa.field("vertices", POINT_TYPE))))


def geoarrow_field(name: str, extension: str, data_type: pa.DataType) -> pa.Field:
    return pa.field(
        name,
        data_type,
        metadata={
            "ARROW:extension:name": extension,
            "ARROW:extension:metadata": "{}",
        },
    )


# The triangle's bounding box covers the 25 grid points with x <= 4 and y <= 4,
# but only the 15 with x + y <= 4 intersect the triangle itself.
TRIANGLE = "ST_GeomFromText('POLYGON ((0 0, 4 0, 0 4, 0 0))')"
INTERSECTS_TRIANGLE = f"ST_Intersects(geom, {TRIANGLE})"
TRIANGLE_IDS = [10 * y + x for y in range(10) for x in range(10) if x + y <= 4]


def grid_points(nrows: int) -> pa.Table:
    """A grid of GeoArrow points in which row ``10 * y + x`` sits at ``(x, y)``."""
    ids = list(range(nrows))
    geom = pa.StructArray.from_arrays(
        [
            pa.array([float(i % 10) for i in ids]),
            pa.array([float(i // 10) for i in ids]),
        ],
        names=["x", "y"],
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            geoarrow_field("geom", "geoarrow.point", POINT_TYPE),
            pa.field("vector", pa.list_(pa.float32(), DIM)),
        ]
    )
    return pa.Table.from_arrays(
        [pa.array(ids, pa.int64()), geom, sample_fixed_size_list_array(nrows, DIM)],
        schema=schema,
    )


@pytest_asyncio.fixture
async def point_table(db_async):
    return await db_async.create_table("point_table", grid_points(100))


async def query_ids(query) -> list:
    return (await query.to_arrow())["id"].to_pylist()


@pytest.mark.asyncio
async def test_create_rtree_index(point_table: AsyncTable):
    assert sorted(await query_ids(point_table.query().where(INTERSECTS_TRIANGLE))) == (
        TRIANGLE_IDS
    )

    await point_table.create_index("geom", config=RTree())
    indices = await point_table.list_indices()
    assert len(indices) == 1
    assert indices[0].name == "geom_idx"
    assert indices[0].index_type == "RTree"
    assert indices[0].columns == ["geom"]
    stats = await point_table.index_stats("geom_idx")
    assert stats.index_type == "RTREE"
    assert stats.num_indexed_rows == 100
    assert stats.num_unindexed_rows == 0

    # The index narrows the scan to the bounding box; the exact predicate then
    # drops the candidates that fall outside the triangle.
    query = point_table.query().where(INTERSECTS_TRIANGLE)
    assert "ScalarIndexQuery" in await query.explain_plan(True)
    assert sorted(await query_ids(query)) == TRIANGLE_IDS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "predicate, expected_ids",
    [
        (INTERSECTS_TRIANGLE, TRIANGLE_IDS),
        (f"ST_Within(geom, {TRIANGLE})", [11, 12, 21]),
        (f"ST_CoveredBy(geom, {TRIANGLE})", TRIANGLE_IDS),
        (
            f"ST_Touches(geom, {TRIANGLE})",
            [0, 1, 2, 3, 4, 10, 13, 20, 22, 30, 31, 40],
        ),
        (f"ST_Intersects(geom, {TRIANGLE}) AND id > 20", [21, 22, 30, 31, 40]),
        ("ST_Intersects(geom, ST_Point(3.0, 3.0))", [33]),
        ("ST_Contains(geom, ST_Point(3.0, 3.0))", [33]),
        ("ST_Covers(geom, ST_Point(3.0, 3.0))", [33]),
    ],
)
async def test_rtree_index_accelerates_spatial_filters(
    point_table: AsyncTable, predicate: str, expected_ids: list
):
    unindexed = sorted(await query_ids(point_table.query().where(predicate)))
    assert unindexed == expected_ids

    await point_table.create_index("geom", config=RTree())
    query = point_table.query().where(predicate)
    assert "ScalarIndexQuery" in await query.explain_plan(True)
    assert sorted(await query_ids(query)) == expected_ids


@pytest.mark.asyncio
async def test_rtree_index_prefilters_vector_search(point_table: AsyncTable):
    await point_table.create_index("geom", config=RTree())
    # Row 99 sits outside the triangle, and its nearest vectors are rows 98, 97, ...
    far_corner = list(range(99 * DIM, 100 * DIM))

    prefiltered = point_table.query().nearest_to(far_corner).where(INTERSECTS_TRIANGLE)
    assert "ScalarIndexQuery" in await prefiltered.explain_plan(True)
    assert await query_ids(prefiltered.limit(5)) == [40, 31, 30, 22, 21]

    postfiltered = (
        point_table.query()
        .nearest_to(far_corner)
        .where(INTERSECTS_TRIANGLE)
        .postfilter()
        .limit(5)
    )
    assert await query_ids(postfiltered) == []


def square(corner: float) -> list:
    """A 2 x 2 square polygon with its lower left corner at ``(corner, corner)``."""
    ring = [(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)]
    return [[{"x": corner + x, "y": corner + y} for x, y in ring]]


@pytest_asyncio.fixture
async def polygon_table(db_async):
    """Ten overlapping squares; row ``i`` covers ``(i, i)`` to ``(i + 2, i + 2)``."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            geoarrow_field("area", "geoarrow.polygon", POLYGON_TYPE),
        ]
    )
    data = pa.Table.from_arrays(
        [
            pa.array(range(10), pa.int64()),
            pa.array([square(float(i)) for i in range(10)], POLYGON_TYPE),
        ],
        schema=schema,
    )
    return await db_async.create_table("polygons", data)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "predicate, expected_ids",
    [
        ("ST_Intersects(area, ST_GeomFromText('POINT (3.5 3.5)'))", [2, 3]),
        ("ST_Contains(area, ST_GeomFromText('POINT (3.5 3.5)'))", [2, 3]),
        ("ST_Covers(area, ST_GeomFromText('POINT (3.5 3.5)'))", [2, 3]),
        ("ST_Crosses(area, ST_GeomFromText('LINESTRING (2.5 2.5, 2.5 10)'))", [1, 2]),
        (
            "ST_Overlaps(area, ST_GeomFromText('POLYGON ((3 3, 5 3, 5 5, 3 5, 3 3))'))",
            [2, 4],
        ),
        (
            "ST_Touches(area, ST_GeomFromText('POLYGON ((3 3, 5 3, 5 5, 3 5, 3 3))'))",
            [1, 5],
        ),
    ],
)
async def test_create_rtree_index_on_polygons(
    polygon_table: AsyncTable, predicate: str, expected_ids: list
):
    assert sorted(await query_ids(polygon_table.query().where(predicate))) == (
        expected_ids
    )

    await polygon_table.create_index("area", config=RTree(page_size=2))
    assert (await polygon_table.list_indices())[0].index_type == "RTree"

    query = polygon_table.query().where(predicate)
    assert "ScalarIndexQuery" in await query.explain_plan(True)
    assert sorted(await query_ids(query)) == expected_ids


@pytest.mark.asyncio
async def test_rtree_index_with_null_geometries(db_async):
    # Every third row has no geometry; the others sit on the diagonal at (i, i).
    geom = pa.array(
        [None if i % 3 == 0 else {"x": float(i), "y": float(i)} for i in range(10)],
        POINT_TYPE,
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            geoarrow_field("geom", "geoarrow.point", POINT_TYPE),
        ]
    )
    table = await db_async.create_table(
        "nulls",
        pa.Table.from_arrays([pa.array(range(10), pa.int64()), geom], schema=schema),
    )
    box = "ST_GeomFromText('POLYGON ((-1 -1, 1.5 -1, 1.5 1.5, -1 1.5, -1 -1))')"
    expected = {
        "geom IS NULL": [0, 3, 6, 9],
        "geom IS NOT NULL": [1, 2, 4, 5, 7, 8],
        # Row 0 is inside the box but has no geometry, so it matches neither.
        f"ST_Intersects(geom, {box})": [1],
        f"NOT ST_Intersects(geom, {box})": [2, 4, 5, 7, 8],
    }
    for predicate, ids in expected.items():
        assert sorted(await query_ids(table.query().where(predicate))) == ids

    await table.create_index("geom", config=RTree())
    for predicate, ids in expected.items():
        assert sorted(await query_ids(table.query().where(predicate))) == ids
    for predicate in ["geom IS NULL", f"ST_Intersects(geom, {box})"]:
        assert "ScalarIndexQuery" in await table.query().where(predicate).explain_plan(
            True
        )


@pytest.mark.asyncio
async def test_create_rtree_index_on_wkb(db_async):
    """GeoParquet and GeoPandas data usually arrives as well-known binary."""

    def wkb_point(x: float, y: float) -> bytes:
        # Byte order (1 = little endian), geometry type (1 = point), x, y.
        return struct.pack("<BIdd", 1, 1, x, y)

    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            geoarrow_field("geom", "geoarrow.wkb", pa.binary()),
        ]
    )
    data = pa.Table.from_arrays(
        [
            pa.array(range(100), pa.int64()),
            pa.array([wkb_point(i % 10, i // 10) for i in range(100)], pa.binary()),
        ],
        schema=schema,
    )
    table = await db_async.create_table("wkb", data)
    assert sorted(await query_ids(table.query().where(INTERSECTS_TRIANGLE))) == (
        TRIANGLE_IDS
    )

    await table.create_index("geom", config=RTree())
    query = table.query().where(INTERSECTS_TRIANGLE)
    assert "ScalarIndexQuery" in await query.explain_plan(True)
    assert sorted(await query_ids(query)) == TRIANGLE_IDS


@pytest.mark.asyncio
async def test_rtree_index_after_append_and_optimize(point_table: AsyncTable):
    await point_table.create_index("geom", config=RTree())
    # Rows 100..199 continue the grid above the first one, at y = 10..19.
    await point_table.add(grid_points(200).slice(100))
    across_both = (
        "ST_Intersects(geom, ST_GeomFromText('POLYGON ((0 9, 2 9, 2 11, 0 11, 0 9))'))"
    )
    expected = [90, 91, 92, 100, 101, 102, 110, 111, 112]

    stats = await point_table.index_stats("geom_idx")
    assert stats is not None
    assert (stats.num_indexed_rows, stats.num_unindexed_rows) == (100, 100)
    assert sorted(await query_ids(point_table.query().where(across_both))) == expected

    await point_table.optimize()
    stats = await point_table.index_stats("geom_idx")
    assert stats is not None
    assert (stats.num_indexed_rows, stats.num_unindexed_rows) == (200, 0)
    query = point_table.query().where(across_both)
    assert "ScalarIndexQuery" in await query.explain_plan(True)
    assert sorted(await query_ids(query)) == expected


@pytest.mark.asyncio
async def test_geoarrow_column_round_trips(point_table: AsyncTable):
    """Enabling geo support must not change how GeoArrow columns are stored or read."""
    expected = grid_points(100)
    schema = await point_table.schema()
    assert schema.field("geom").metadata == expected.schema.field("geom").metadata
    assert schema.field("geom").type == POINT_TYPE

    data = (await point_table.query().to_arrow()).sort_by("id")
    assert data["geom"].to_pylist() == expected["geom"].to_pylist()
    assert sorted(await query_ids(point_table.query().where("id < 3"))) == [0, 1, 2]


# GeoDataFusion evaluates an asymmetric predicate with its arguments swapped when
# the constant comes first, and panics on a NULL constant geometry without an index.
# Both are fixed upstream; remove these markers when Lance picks up the fix.
CONSTANT_FIRST_BUG = pytest.mark.xfail(
    reason="GeoDataFusion relates the constant-first form with swapped arguments",
    strict=True,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("indexed", [False, True])
@pytest.mark.parametrize(
    "predicate, expected_ids",
    [
        pytest.param(
            f"ST_Contains({TRIANGLE}, geom)", [11, 12, 21], marks=CONSTANT_FIRST_BUG
        ),
        pytest.param(
            f"ST_Covers({TRIANGLE}, geom)", TRIANGLE_IDS, marks=CONSTANT_FIRST_BUG
        ),
        # Symmetric predicates already give the right answer either way.
        (f"ST_Intersects({TRIANGLE}, geom)", TRIANGLE_IDS),
    ],
)
async def test_spatial_filter_constant_first(
    point_table: AsyncTable, indexed: bool, predicate: str, expected_ids: list
):
    if indexed:
        await point_table.create_index("geom", config=RTree())
    assert sorted(await query_ids(point_table.query().where(predicate))) == (
        expected_ids
    )


@CONSTANT_FIRST_BUG
@pytest.mark.asyncio
async def test_delete_with_constant_first_spatial_filter(point_table: AsyncTable):
    # Delete everything outside the triangle; the interior points must survive.
    await point_table.delete(f"NOT ST_Contains({TRIANGLE}, geom)")
    assert sorted(await query_ids(point_table.query())) == [11, 12, 21]


@CONSTANT_FIRST_BUG
@pytest.mark.asyncio
async def test_update_with_constant_first_spatial_filter(point_table: AsyncTable):
    await point_table.update({"id": "-id"}, where=f"ST_Contains({TRIANGLE}, geom)")
    assert sorted(await query_ids(point_table.query().where("id < 0"))) == [
        -21,
        -12,
        -11,
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "indexed",
    [
        pytest.param(
            False,
            marks=pytest.mark.xfail(
                reason="GeoDataFusion panics on a NULL constant geometry",
                raises=Exception,
                strict=True,
            ),
        ),
        True,
    ],
)
async def test_spatial_filter_with_null_constant(point_table: AsyncTable, indexed):
    if indexed:
        await point_table.create_index("geom", config=RTree())
    null_geometry = "ST_GeomFromText(CAST(NULL AS VARCHAR))"
    for predicate in [
        f"ST_Intersects(geom, {null_geometry})",
        f"ST_Within(geom, {null_geometry})",
    ]:
        assert await query_ids(point_table.query().where(predicate)) == []


@pytest.mark.asyncio
async def test_create_rtree_index_rejects_invalid_input(point_table: AsyncTable):
    with pytest.raises(ValueError, match="page_size must be at least 2"):
        await point_table.create_index("geom", config=RTree(page_size=1))
    # Only the message is part of the contract, not the exception type.
    with pytest.raises(Exception, match="GeoArrow"):
        await point_table.create_index("id", config=RTree())
    assert await point_table.list_indices() == []


def test_create_rtree_index_sync(tmp_path):
    db = lancedb.connect(tmp_path)
    table = db.create_table("points", grid_points(100))
    table.create_index("geom", config=RTree())
    assert [index.index_type for index in table.list_indices()] == ["RTree"]
    table.create_index("geom", config=RTree(page_size=16), name="geom_rtree")
    assert table.index_stats("geom_rtree").index_type == "RTREE"

    query = table.search().where(INTERSECTS_TRIANGLE).limit(100)
    assert "ScalarIndexQuery" in query.explain_plan(True)
    assert sorted(query.to_arrow()["id"].to_pylist()) == TRIANGLE_IDS


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
async def test_create_ivf_index_reports_unsplittable_partitions(db_async):
    dim = 8
    num_partitions = 300  # More than 256 selects hierarchical k-means.
    base_vectors = [[float(row == column) for column in range(dim)] for row in range(5)]
    vectors = pa.array(base_vectors * 200, pa.list_(pa.float32(), dim))
    table = await db_async.create_table(
        "unsplittable_partitions",
        pa.table({"vector": vectors}),
    )

    error_pattern = (
        rf"Cannot create {num_partitions} IVF partitions: k-means could only form"
    )
    with pytest.raises(RuntimeError, match=error_pattern):
        await table.create_index(
            "vector",
            config=IvfFlat(
                distance_type="dot",
                num_partitions=num_partitions,
                max_iterations=10,
            ),
        )


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
        "RTREE",
    }

    assert (
        set(get_args(get_type_hints(IndexStatistics)["index_type"]))
        == expected_index_types
    )
