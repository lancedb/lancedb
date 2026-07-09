# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import io

import pyarrow as pa
import pyarrow.compute as pc
import pytest

import lancedb
from lancedb._blob import read_row_ids_from_hits, stash_auto_row_ids
from lancedb.index import FTS
from lancedb.schema import blob_column_paths, blob_v2_column_paths


def _blob_table(name, rows):
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table(name, schema=schema)
    table.add(rows)
    return table


def _blob_array(name, values):
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


def _row_ids_by_id(table):
    hits = table.search().with_row_id(True).limit(1000).to_arrow()
    assert "_rowid" in hits.column_names
    return dict(zip(hits["id"].to_pylist(), hits["_rowid"].to_pylist()))


def test_blob_factory_declares_v2_field():
    field = lancedb.blob("image")
    assert isinstance(field.type, pa.ExtensionType)
    assert field.type.extension_name == "lance.blob.v2"


def test_blob_v2_column_paths_include_list_children():
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


def _legacy_v1_table(name):
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


def test_blob_v2_column_paths_exclude_legacy_metadata():
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


def test_blob_v2_paths_match_blob_columns():
    table = _blob_table("paths_match", [{"id": 1, "image": b"x"}])
    assert blob_v2_column_paths(table.schema) == table.blob_columns()

    db = lancedb.connect("memory:///")
    info = pa.StructArray.from_arrays(
        [
            pa.array(["first"], type=pa.string()),
            _blob_array("blob", [b"nested"]),
        ],
        names=["name", "blob"],
    )
    data = pa.Table.from_arrays(
        [pa.array([1], type=pa.int64()), info],
        names=["id", "info"],
    )
    nested = db.create_table("nested_paths", data=data)
    assert blob_v2_column_paths(nested.schema) == nested.blob_columns()


def test_auto_row_id_stash_round_trip():
    table = _blob_table(
        "stash_round_trip",
        [{"id": 1, "image": b"alpha"}, {"id": 2, "image": b"beta"}],
    )
    hits = table.search().with_row_id(True).limit(10).to_arrow()
    row_ids = hits["_rowid"].to_pylist()

    stashed = stash_auto_row_ids(hits, ["image"])

    assert "_rowid" not in stashed.column_names
    assert stashed.schema.field("image").metadata == hits.schema.field("image").metadata
    assert read_row_ids_from_hits(stashed, "image") == row_ids


def test_blob_query_omits_auto_row_id():
    table = _blob_table("rowid", [{"id": 1, "image": b"x"}])
    hits = table.search().limit(10).to_arrow()
    assert "_rowid" not in hits.column_names


def test_blob_query_explicit_row_id_opt_in():
    table = _blob_table("explicit_rowid", [{"id": 1, "image": b"x"}])
    hits = table.search().with_row_id(True).limit(10).to_arrow()
    assert "_rowid" in hits.column_names


def test_table_to_pandas_descriptions_mode_omits_row_id():
    table = _blob_table("descriptions_no_leak", [{"id": 1, "image": b"x"}])
    df = table.to_pandas(blob_mode="descriptions")
    descriptor = df["image"].iloc[0]
    assert "_lance_row_id" not in descriptor
    assert set(descriptor.keys()) == {"kind", "position", "size", "blob_id", "blob_uri"}


@pytest.mark.asyncio
async def test_async_table_to_pandas_descriptions_mode_omits_row_id():
    db = await lancedb.connect_async("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = await db.create_table("descriptions_no_leak_async", schema=schema)
    await table.add([{"id": 1, "image": b"x"}])
    df = await table.to_pandas(blob_mode="descriptions")
    descriptor = df["image"].iloc[0]
    assert "_lance_row_id" not in descriptor
    assert set(descriptor.keys()) == {"kind", "position", "size", "blob_id", "blob_uri"}


def test_fetch_blobs_round_trip():
    table = _blob_table(
        "round_trip",
        [{"id": 1, "image": b"alpha"}, {"id": 2, "image": b"beta"}],
    )
    by_id = _row_ids_by_id(table)
    blobs = table.fetch_blobs("image", [by_id[1], by_id[2]])
    assert [blobs[0].as_py(), blobs[1].as_py()] == [b"alpha", b"beta"]


def test_fetch_blobs_accepts_query_result():
    table = _blob_table("from_result", [{"id": 1, "image": b"gamma"}])
    hits = table.search().limit(10).to_arrow()
    assert "_rowid" not in hits.column_names
    blobs = table.fetch_blobs("image", hits)
    assert {blobs[i].as_py() for i in range(len(blobs))} == {b"gamma"}


def test_fetch_blobs_null_alignment():
    table = _blob_table(
        "nulls",
        [{"id": 1, "image": b"present"}, {"id": 2, "image": None}],
    )
    by_id = _row_ids_by_id(table)
    request = [by_id[1], by_id[2], by_id[1]]
    blobs = table.fetch_blobs("image", request)
    assert len(blobs) == len(request)
    assert blobs[0].as_py() == b"present"
    assert blobs[1].as_py() is None
    assert blobs[2].as_py() == b"present"


def test_fetch_blobs_nested_path():
    db = lancedb.connect("memory:///")
    info = pa.StructArray.from_arrays(
        [
            pa.array(["first", "second"], type=pa.string()),
            _blob_array("blob", [b"nested-alpha", b"nested-beta"]),
        ],
        names=["name", "blob"],
    )
    data = pa.Table.from_arrays(
        [pa.array([1, 2], type=pa.int64()), info],
        names=["id", "info"],
    )
    table = db.create_table("nested", data=data)

    by_id = _row_ids_by_id(table)
    blobs = table.fetch_blobs("info.blob", [by_id[1], by_id[2]])
    assert [blobs[0].as_py(), blobs[1].as_py()] == [b"nested-alpha", b"nested-beta"]


def test_fetch_blob_files_lazy_read():
    payload = b"lazy-read" * 100
    table = _blob_table("lazy", [{"id": 1, "image": payload}])
    by_id = _row_ids_by_id(table)
    handles = table.fetch_blob_files("image", [by_id[1]])
    assert len(handles) == 1
    assert handles[0].read() == payload


def test_fetch_blob_files_null_alignment():
    table = _blob_table(
        "lazy_nulls",
        [{"id": 1, "image": b"here"}, {"id": 2, "image": None}],
    )
    by_id = _row_ids_by_id(table)
    handles = table.fetch_blob_files("image", [by_id[2], by_id[1]])
    assert len(handles) == 2
    assert handles[0] is None
    assert handles[1].read() == b"here"


def test_fetch_blobs_rejects_non_blob_column():
    table = _blob_table("reject", [{"id": 1, "image": b"x"}])
    with pytest.raises(ValueError, match="not a blob column"):
        table.fetch_blobs("id", [0])


def test_legacy_v1_query_omits_auto_row_id():
    table = _legacy_v1_table("legacy_v1")
    hits = table.search().select(["legacy"]).limit(10).to_arrow()
    assert "_rowid" not in hits.column_names


def test_fetch_blobs_rejects_legacy_v1_column():
    table = _legacy_v1_table("legacy_fetch")
    with pytest.raises(ValueError, match="legacy blob column.*blob v2"):
        table.fetch_blobs("legacy", [0])


@pytest.mark.asyncio
async def test_async_fetch_blob_files_lazy_read():
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


def test_fetch_blobs_from_query_result_without_row_id_raises():
    table = _blob_table("no_rowid", [{"id": 1, "image": b"x"}])
    hits = table.search().select(["id"]).to_arrow()
    assert "_rowid" not in hits.column_names
    with pytest.raises(ValueError, match="_rowid"):
        table.fetch_blobs("image", hits)


_HYBRID_BLOB_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("text", pa.utf8()),
        pa.field("vector", pa.list_(pa.float32(), list_size=2)),
        lancedb.blob("image"),
    ]
)
_HYBRID_BLOB_ROWS = [
    {"id": 1, "text": "hello alpha", "vector": [1.0, 0.0], "image": b"alpha"},
    {"id": 2, "text": "hello beta", "vector": [0.9, 0.1], "image": b"beta"},
    {"id": 3, "text": "other", "vector": [0.0, 1.0], "image": b"other"},
]


def _hybrid_blob_table(db):
    table = db.create_table("hybrid_blob_fetch", schema=_HYBRID_BLOB_SCHEMA)
    table.add(_HYBRID_BLOB_ROWS)
    table.create_index("text", config=FTS(with_position=False))
    return table


async def _hybrid_blob_table_async(db):
    table = await db.create_table("hybrid_blob_fetch_async", schema=_HYBRID_BLOB_SCHEMA)
    await table.add(_HYBRID_BLOB_ROWS)
    await table.create_index("text", config=FTS(with_position=False))
    return table


def test_blob_v2_hybrid_fetch_blobs():
    table = _hybrid_blob_table(lancedb.connect("memory:///"))
    hits = (
        table.search(query_type="hybrid")
        .vector([1.0, 0.0])
        .text("hello")
        .select(["id", "image"])
        .limit(2)
        .to_arrow()
    )

    assert "_rowid" not in hits.column_names
    assert "_lance_row_id" in hits.schema.field("image").type.names
    blobs = table.fetch_blobs("image", hits)
    assert {blobs[i].as_py() for i in range(len(blobs))} == {b"alpha", b"beta"}


@pytest.mark.asyncio
async def test_blob_v2_hybrid_fetch_blobs_async():
    db = await lancedb.connect_async("memory:///hybrid_blob_fetch_async")
    table = await _hybrid_blob_table_async(db)
    hits = await (
        table.query()
        .nearest_to([1.0, 0.0])
        .nearest_to_text("hello")
        .select(["id", "image"])
        .limit(2)
        .to_arrow()
    )

    assert "_rowid" not in hits.column_names
    assert "_lance_row_id" in hits.schema.field("image").type.names
    blobs = await table.fetch_blobs("image", hits)
    assert {blobs[i].as_py() for i in range(len(blobs))} == {b"alpha", b"beta"}


def test_blob_file_seek_read_and_read_range():
    payload = _identifiable_payload(1024)
    table = _blob_table("seek_read", [{"id": 1, "image": payload}])
    by_id = _row_ids_by_id(table)
    handle = table.fetch_blob_files("image", [by_id[1]])[0]

    assert handle.seek(100) == 100
    assert handle.read(16) == payload[100:116]
    handle.seek(100)
    assert handle.read_range(500, 8) == payload[500:508]
    assert handle.tell() == 100

    with pytest.raises(ValueError, match="whence"):
        handle.seek(0, 99)


def test_fetch_blob_files_from_query_partial_read():
    payload = _identifiable_payload(65536)
    table = _blob_table("query_partial", [{"id": 1, "image": payload}])
    hits = table.search().select(["id", "image"]).limit(1).to_arrow()
    assert "_rowid" not in hits.column_names

    handle = table.fetch_blob_files("image", hits)[0]
    assert handle.size() == 65536
    assert handle.read_range(0, 128) == payload[:128]
    assert handle.tell() == 0
    assert handle.seek(40000) == 40000
    assert handle.read(16) == payload[40000:40016]


def test_blob_file_buffered_reader():
    payload = _identifiable_payload(4096)
    table = _blob_table("buffered_reader", [{"id": 1, "image": payload}])
    hits = table.search().select(["id", "image"]).limit(1).to_arrow()
    handle = table.fetch_blob_files("image", hits)[0]
    reader = io.BufferedReader(handle)
    assert reader.read(8) == payload[:8]
    assert reader.read(8) == payload[8:16]
    assert reader.read() == payload[16:]


def test_fetch_blob_files_cross_fragment_nulls_and_dups():
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("cross_fragment", schema=schema)
    table.add([{"id": 1, "image": b"alpha"}])
    table.add([{"id": 2, "image": None}, {"id": 3, "image": b"beta"}])

    by_id = _row_ids_by_id(table)
    request = [by_id[3], by_id[2], by_id[1], by_id[3]]
    handles = table.fetch_blob_files("image", request)
    assert len(handles) == 4
    assert handles[1] is None
    assert handles[0].read() == b"beta"
    assert handles[2].read() == b"alpha"
    assert handles[3].seek(1) == 1
    assert handles[3].read() == b"eta"


def test_blob_file_pyav_decode_seek(tmp_path):
    av = pytest.importorskip("av")
    import fractions

    clip = tmp_path / "clip.mp4"
    with av.open(str(clip), mode="w") as container:
        stream = container.add_stream("mpeg4", rate=5)
        stream.width, stream.height, stream.pix_fmt = 32, 32, "yuv420p"
        stream.time_base = fractions.Fraction(1, 5)
        for pts in range(5):
            frame = av.VideoFrame(32, 32, "yuv420p")
            frame.pts = pts
            container.mux(stream.encode(frame))
        container.mux(stream.encode(None))

    table = _blob_table("pyav", [{"id": 1, "image": clip.read_bytes()}])
    hits = table.search().select(["image"]).limit(1).to_arrow()
    handle = table.fetch_blob_files("image", hits)[0]

    with av.open(handle) as container:
        stream = container.streams.video[0]
        container.seek(0)
        assert next(container.decode(stream)) is not None


def test_blob_v2_hybrid_fetch_blob_files_seek():
    table = _hybrid_blob_table(lancedb.connect("memory:///"))
    hits = (
        table.search(query_type="hybrid")
        .vector([1.0, 0.0])
        .text("hello")
        .select(["id", "image"])
        .limit(2)
        .to_arrow()
    )
    assert "_rowid" not in hits.column_names

    handles = table.fetch_blob_files("image", hits)
    assert len(handles) == 2
    assert {handle.read_range(0, 2) for handle in handles} == {b"al", b"be"}
    first = handles[0]
    assert first.seek(1) == 1
    assert first.read(2) in {b"lp", b"et"}


def test_blob_file_header_sniff_from_search():
    payload = b"%PDF-1.7\n" + bytes(4096)
    table = _blob_table("header_sniff", [{"id": 1, "image": payload}])
    hits = table.search().select(["id", "image"]).limit(1).to_arrow()
    handle = table.fetch_blob_files("image", hits)[0]
    assert handle.read_range(0, 4) == b"%PDF"
    assert handle.tell() == 0


def test_blob_file_multiple_handles_independent_cursors():
    table = _blob_table(
        "multi_handle",
        [{"id": 1, "image": b"first-payload"}, {"id": 2, "image": b"second-payload"}],
    )
    by_id = _row_ids_by_id(table)
    first, second = table.fetch_blob_files("image", [by_id[1], by_id[2]])
    assert first.seek(6) == 6
    assert second.tell() == 0
    assert first.read(7) == b"payload"
    assert second.read(6) == b"second"


def test_fetch_blob_files_nested_path_seek():
    db = lancedb.connect("memory:///")
    info = pa.StructArray.from_arrays(
        [
            pa.array(["first", "second"], type=pa.string()),
            _blob_array("blob", [b"nested-alpha", b"nested-beta"]),
        ],
        names=["name", "blob"],
    )
    data = pa.Table.from_arrays(
        [pa.array([1, 2], type=pa.int64()), info],
        names=["id", "info"],
    )
    table = db.create_table("nested_seek", data=data)
    by_id = _row_ids_by_id(table)
    handle = table.fetch_blob_files("info.blob", [by_id[2]])[0]
    assert handle.seek(7) == 7
    assert handle.read() == b"beta"


def test_fetch_blobs_survives_sort_after_query():
    table = _blob_table(
        "sort_survives",
        [{"id": i, "image": f"payload-{i}".encode()} for i in range(5)],
    )
    hits = table.search().select(["id", "image"]).to_arrow()
    sort_idx = pc.sort_indices(hits["id"], sort_keys=[("id", "descending")])
    sorted_hits = hits.take(sort_idx)

    blobs = table.fetch_blobs("image", sorted_hits)
    expected = [f"payload-{i}".encode() for i in sorted_hits["id"].to_pylist()]
    assert [blobs[i].as_py() for i in range(len(blobs))] == expected


def test_fetch_blobs_survives_filter_and_sort_after_query():
    table = _blob_table(
        "filter_sort_survives",
        [{"id": i, "image": f"payload-{i}".encode()} for i in range(5)],
    )
    hits = table.search().select(["id", "image"]).to_arrow()
    filtered = hits.filter(pc.field("id") >= 2)
    sort_idx = pc.sort_indices(filtered["id"], sort_keys=[("id", "descending")])
    filtered_sorted = filtered.take(sort_idx)

    blobs = table.fetch_blobs("image", filtered_sorted)
    expected = [f"payload-{i}".encode() for i in filtered_sorted["id"].to_pylist()]
    assert [blobs[i].as_py() for i in range(len(blobs))] == expected


def test_fetch_blob_files_survives_sort_after_query():
    table = _blob_table(
        "lazy_sort_survives",
        [{"id": i, "image": f"payload-{i}".encode()} for i in range(5)],
    )
    hits = table.search().select(["id", "image"]).to_arrow()
    sort_idx = pc.sort_indices(hits["id"], sort_keys=[("id", "descending")])
    sorted_hits = hits.take(sort_idx)

    handles = table.fetch_blob_files("image", sorted_hits)
    expected = [f"payload-{i}".encode() for i in sorted_hits["id"].to_pylist()]
    assert [handle.read() for handle in handles] == expected


def test_fetch_blobs_nested_path_survives_sort_after_query():
    db = lancedb.connect("memory:///")
    values = [f"payload-{i}".encode() for i in range(4)]
    info = pa.StructArray.from_arrays(
        [pa.array(["row"] * 4, type=pa.string()), _blob_array("blob", values)],
        names=["name", "blob"],
    )
    data = pa.Table.from_arrays(
        [pa.array(range(4), type=pa.int64()), info],
        names=["id", "info"],
    )
    table = db.create_table("nested_sort_survives", data=data)

    hits = table.search().to_arrow()
    sort_idx = pc.sort_indices(hits["id"], sort_keys=[("id", "descending")])
    sorted_hits = hits.take(sort_idx)

    blobs = table.fetch_blobs("info.blob", sorted_hits)
    expected = [f"payload-{i}".encode() for i in sorted_hits["id"].to_pylist()]
    assert [blobs[i].as_py() for i in range(len(blobs))] == expected


def _identifiable_payload(size: int) -> bytes:
    block = 256
    return b"".join(bytes([i % 256]) * block for i in range(size // block))
