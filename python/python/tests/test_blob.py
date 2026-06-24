# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pyarrow as pa
import pytest

import lancedb
from lancedb._blob import read_row_ids_from_hits, stash_auto_row_ids
from lancedb.schema import blob_column_paths, blob_v2_column_paths


def _blobTable(name, rows):
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table(name, schema=schema)
    table.add(rows)
    return table


def _blobArray(name, values):
    blob_type = lancedb.blob(name).type
    storage_type = blob_type.storage_type
    storage = pa.StructArray.from_arrays(
        [
            pa.array(values, type=pa.large_binary()),
            pa.array([None] * len(values), type=pa.string()),
            pa.array([None] * len(values), type=pa.uint64()),
            pa.array([None] * len(values), type=pa.uint64()),
        ],
        fields=list(storage_type),
    )
    return pa.ExtensionArray.from_storage(blob_type, storage)


def _rowIdsById(table):
    hits = table.search().with_row_id(True).limit(1000).to_arrow()
    assert "_rowid" in hits.column_names
    return dict(zip(hits["id"].to_pylist(), hits["_rowid"].to_pylist()))


def testBlobFactoryDeclaresV2Field():
    field = lancedb.blob("image")
    assert isinstance(field.type, pa.ExtensionType)
    assert field.type.extension_name == "lance.blob.v2"


def testBlobV2ColumnPathsIncludeListChildren():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("info", pa.struct([lancedb.blob("blob")])),
            pa.field("images", pa.list_(lancedb.blob("image"))),
            pa.field("large_images", pa.large_list(lancedb.blob("large_image"))),
            pa.field(
                "fixed_images",
                pa.list_(lancedb.blob("fixed_image"), list_size=2),
            ),
        ]
    )

    assert blob_v2_column_paths(schema) == [
        "info.blob",
        "images.image",
        "large_images.large_image",
        "fixed_images.fixed_image",
    ]


def _legacyV1Table(name):
    db = lancedb.connect("memory:///")
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "legacy", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
            ),
        ]
    )
    table = db.create_table(name, schema=schema)
    table.add([{"id": 1, "legacy": b"old"}])
    return table


def testBlobV2ColumnPathsExcludeLegacyMetadata():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            lancedb.blob("image"),
            pa.field(
                "legacy", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
            ),
        ]
    )
    assert blob_v2_column_paths(schema) == ["image"]
    assert blob_column_paths(schema) == ["image", "legacy"]


def testBlobV2PathsMatchBlobColumns():
    table = _blobTable("paths_match", [{"id": 1, "image": b"x"}])
    assert blob_v2_column_paths(table.schema) == table.blob_columns()

    db = lancedb.connect("memory:///")
    info = pa.StructArray.from_arrays(
        [
            pa.array(["first"], type=pa.string()),
            _blobArray("blob", [b"nested"]),
        ],
        names=["name", "blob"],
    )
    data = pa.Table.from_arrays(
        [pa.array([1], type=pa.int64()), info],
        names=["id", "info"],
    )
    nested = db.create_table("nested_paths", data=data)
    assert blob_v2_column_paths(nested.schema) == nested.blob_columns()


def testAutoRowIdStashRoundTrip():
    tbl = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "_rowid": pa.array([10, 20], type=pa.uint64()),
        }
    )

    stashed = stash_auto_row_ids(tbl)

    assert stashed.column_names == ["id"]
    assert read_row_ids_from_hits(stashed) == [10, 20]


def testBlobQueryOmitsAutoRowId():
    table = _blobTable("rowid", [{"id": 1, "image": b"x"}])
    hits = table.search().limit(10).to_arrow()
    assert "_rowid" not in hits.column_names


def testBlobQueryExplicitRowIdOptIn():
    table = _blobTable("explicit_rowid", [{"id": 1, "image": b"x"}])
    hits = table.search().with_row_id(True).limit(10).to_arrow()
    assert "_rowid" in hits.column_names


def testFetchBlobsRoundTrip():
    table = _blobTable(
        "round_trip",
        [{"id": 1, "image": b"alpha"}, {"id": 2, "image": b"beta"}],
    )
    by_id = _rowIdsById(table)
    blobs = table.fetch_blobs("image", [by_id[1], by_id[2]])
    assert [blobs[0].as_py(), blobs[1].as_py()] == [b"alpha", b"beta"]


def testFetchBlobsAcceptsQueryResult():
    table = _blobTable("from_result", [{"id": 1, "image": b"gamma"}])
    hits = table.search().limit(10).to_arrow()
    assert "_rowid" not in hits.column_names
    blobs = table.fetch_blobs("image", hits)
    assert {blobs[i].as_py() for i in range(len(blobs))} == {b"gamma"}


def testFetchBlobsNullAlignment():
    table = _blobTable(
        "nulls",
        [{"id": 1, "image": b"present"}, {"id": 2, "image": None}],
    )
    by_id = _rowIdsById(table)
    request = [by_id[1], by_id[2], by_id[1]]
    blobs = table.fetch_blobs("image", request)
    assert len(blobs) == len(request)
    assert blobs[0].as_py() == b"present"
    assert blobs[1].as_py() is None
    assert blobs[2].as_py() == b"present"


def testFetchBlobsNestedPath():
    db = lancedb.connect("memory:///")
    info = pa.StructArray.from_arrays(
        [
            pa.array(["first", "second"], type=pa.string()),
            _blobArray("blob", [b"nested-alpha", b"nested-beta"]),
        ],
        names=["name", "blob"],
    )
    data = pa.Table.from_arrays(
        [pa.array([1, 2], type=pa.int64()), info],
        names=["id", "info"],
    )
    table = db.create_table("nested", data=data)

    by_id = _rowIdsById(table)
    blobs = table.fetch_blobs("info.blob", [by_id[1], by_id[2]])
    assert [blobs[0].as_py(), blobs[1].as_py()] == [b"nested-alpha", b"nested-beta"]


def testFetchBlobFilesLazyRead():
    payload = b"lazy-read" * 100
    table = _blobTable("lazy", [{"id": 1, "image": payload}])
    by_id = _rowIdsById(table)
    handles = table.fetch_blob_files("image", [by_id[1]])
    assert len(handles) == 1
    assert handles[0].read() == payload


def testFetchBlobFilesNullAlignment():
    table = _blobTable(
        "lazy_nulls",
        [{"id": 1, "image": b"here"}, {"id": 2, "image": None}],
    )
    by_id = _rowIdsById(table)
    handles = table.fetch_blob_files("image", [by_id[2], by_id[1]])
    assert len(handles) == 2
    assert handles[0] is None
    assert handles[1].read() == b"here"


def testFetchBlobsRejectsNonBlobColumn():
    table = _blobTable("reject", [{"id": 1, "image": b"x"}])
    with pytest.raises(ValueError, match="not a blob column"):
        table.fetch_blobs("id", [0])


def testLegacyV1QueryOmitsAutoRowId():
    table = _legacyV1Table("legacy_v1")
    hits = table.search().select(["legacy"]).limit(10).to_arrow()
    assert "_rowid" not in hits.column_names


def testFetchBlobsRejectsLegacyV1Column():
    table = _legacyV1Table("legacy_fetch")
    with pytest.raises(ValueError, match="legacy blob column.*blob v2"):
        table.fetch_blobs("legacy", [0])


@pytest.mark.asyncio
async def testAsyncFetchBlobFilesLazyRead():
    db = await lancedb.connect_async("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = await db.create_table("async_lazy", schema=schema)
    payload = b"async-lazy" * 100
    await table.add([{"id": 1, "image": payload}])
    hits = (
        await table.query().select({"image_alias": "image"}).limit(10).to_arrow()
    ).combine_chunks()
    assert "_rowid" not in hits.column_names
    handles = await table.fetch_blob_files("image", hits)
    assert len(handles) == 1
    assert await handles[0].aread() == payload


def testFetchBlobsFromQueryResultWithoutRowIdRaises():
    table = _blobTable("no_rowid", [{"id": 1, "image": b"x"}])
    hits = table.search().select(["id"]).to_arrow()
    assert "_rowid" not in hits.column_names
    with pytest.raises(ValueError, match="_rowid"):
        table.fetch_blobs("image", hits)
