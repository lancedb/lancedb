# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import os

import lancedb
import pyarrow as pa
import pytest


def test_blob_factory_produces_v2_field():
    field = lancedb.blob("image")
    assert field.name == "image"
    assert field.metadata.get(b"ARROW:extension:name") == b"lance.blob.v2"
    assert isinstance(field.type, pa.StructType)
    child_names = {field.type.field(i).name for i in range(field.type.num_fields)}
    assert child_names == {"data", "uri"}


@pytest.mark.skipif(
    not os.environ.get("LANCEDB_BLOB_INTEGRATION"),
    reason="requires compiled extension (make develop)",
)
def test_inline_blob_write_take_round_trip():
    db = lancedb.connect("memory:///")
    payload_a = os.urandom(50_000)
    payload_b = os.urandom(50_000)
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            lancedb.blob("image"),
        ]
    )
    table = db.create_table("t", schema=schema)
    table.add(
        [
            {"id": 1, "image": payload_a},
            {"id": 2, "image": payload_b},
        ]
    )

    result = table.search().limit(100).to_arrow()
    assert "_rowid" in result.column_names
    bytes_arr = table.take_blobs("image", result)
    values = [bytes_arr[i].as_py() for i in range(len(result))]
    assert payload_a in values
    assert payload_b in values


@pytest.mark.skipif(
    not os.environ.get("LANCEDB_BLOB_INTEGRATION"),
    reason="requires compiled extension (make develop)",
)
def test_take_blobs_on_non_blob_column_raises():
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("t", schema=schema)
    table.add([{"id": 1, "image": b"bytes"}])
    with pytest.raises(ValueError, match="not a blob column"):
        table.take_blobs("id", [0])
    with pytest.raises(ValueError, match="no column named"):
        table.take_blobs("missing", [0])


@pytest.mark.skipif(
    not os.environ.get("LANCEDB_BLOB_INTEGRATION"),
    reason="requires compiled extension (make develop)",
)
def test_take_blobs_aligns_nulls_with_row_ids():
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("nulls", schema=schema)
    table.add([{"id": 1, "image": b"present"}, {"id": 2, "image": None}])
    hits = table.search().limit(10).to_arrow()
    bytes_arr = table.take_blobs("image", hits)
    assert len(bytes_arr) == len(hits), "output length matches input rows"
    vals = [bytes_arr[i].as_py() for i in range(len(bytes_arr))]
    assert None in vals, "null row stays null in the result"
    assert b"present" in vals, "non-null row is present"


@pytest.mark.skipif(
    not os.environ.get("LANCEDB_BLOB_INTEGRATION"),
    reason="requires compiled extension (make develop)",
)
def test_take_blob_files_lazy_read():
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("lazy", schema=schema)
    payload = b"lazy-read" * 100
    table.add([{"id": 1, "image": payload}])
    hits = table.search().limit(10).to_arrow()
    files = table.take_blob_files("image", hits)
    assert len(files) == 1
    assert files[0].read() == payload


@pytest.mark.skipif(
    not os.environ.get("LANCEDB_BLOB_INTEGRATION"),
    reason="requires compiled extension (make develop)",
)
def test_pydantic_lance_model_with_blob():
    from lancedb.pydantic import Blob, LanceModel

    class Photo(LanceModel):
        id: int
        image: Blob

    schema = Photo.to_arrow_schema()
    blob_field = schema.field("image")
    assert blob_field.metadata is not None
    assert blob_field.metadata.get(b"ARROW:extension:name") == b"lance.blob.v2"
    assert isinstance(blob_field.type, pa.StructType)

    db = lancedb.connect("memory:///")
    table = db.create_table("p", schema=Photo)
    table.add([{"id": 1, "image": b"pyd-one"}, {"id": 2, "image": b"pyd-two"}])
    hits = table.search().limit(10).to_arrow()
    bytes_arr = table.take_blobs("image", hits)
    values = {bytes_arr[i].as_py() for i in range(len(hits))}
    assert values == {b"pyd-one", b"pyd-two"}


@pytest.mark.skipif(
    not os.environ.get("LANCEDB_BLOB_INTEGRATION"),
    reason="requires compiled extension (make develop)",
)
def test_blob_column_returns_descriptor_by_default():
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("desc", schema=schema)
    table.add([{"id": 1, "image": b"some-bytes"}])

    arrow = table.search().limit(10).to_arrow()
    assert pa.types.is_struct(arrow.schema.field("image").type), (
        "a plain query returns the blob column as a descriptor struct, not bytes"
    )

    rows = table.search().limit(10).to_list()
    assert not isinstance(rows[0]["image"], (bytes, bytearray)), (
        "to_list does not materialize blob bytes"
    )

    df = table.search().limit(10).to_pandas()
    assert not isinstance(df["image"].iloc[0], (bytes, bytearray)), (
        "to_pandas does not materialize blob bytes"
    )


@pytest.mark.skipif(
    not os.environ.get("LANCEDB_BLOB_INTEGRATION"),
    reason="requires compiled extension (make develop)",
)
def test_pydantic_optional_blob_roundtrip():
    from typing import Optional

    from lancedb.pydantic import Blob, LanceModel

    class Photo(LanceModel):
        id: int
        image: Optional[Blob]

    db = lancedb.connect("memory:///")
    table = db.create_table("optblob", schema=Photo)
    table.add([{"id": 1, "image": b"present"}, {"id": 2, "image": None}])
    hits = table.search().limit(10).to_arrow()
    bytes_arr = table.take_blobs("image", hits)
    vals = [bytes_arr[i].as_py() for i in range(len(hits))]
    assert b"present" in vals
    assert None in vals, "an optional blob written as None comes back null"


@pytest.mark.skipif(
    not os.environ.get("LANCEDB_BLOB_INTEGRATION"),
    reason="requires compiled extension (make develop)",
)
def test_take_blobs_without_rowid_gives_helpful_error():
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("norow", schema=schema)
    table.add([{"id": 1, "image": b"x"}])
    hits = table.search().limit(10).to_arrow().drop(["_rowid"])
    with pytest.raises(ValueError, match="_rowid"):
        table.take_blobs("image", hits)


@pytest.mark.skipif(
    not os.environ.get("LANCEDB_BLOB_INTEGRATION"),
    reason="requires compiled extension (make develop)",
)
def test_take_blobs_accepts_query_result_table():
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("qt", schema=schema)
    table.add([{"id": 1, "image": b"alpha"}, {"id": 2, "image": b"beta"}])
    hits = table.search().limit(10).to_arrow()
    assert "_rowid" in hits.column_names
    bytes_arr = table.take_blobs("image", hits)
    vals = {bytes_arr[i].as_py() for i in range(len(bytes_arr))}
    assert vals == {b"alpha", b"beta"}


@pytest.mark.skipif(
    not os.environ.get("LANCEDB_BLOB_INTEGRATION"),
    reason="requires compiled extension (make develop)",
)
def test_blob_projection_auto_includes_rowid_only_when_needed():
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("auto_rowid", schema=schema)
    table.add([{"id": 1, "image": b"alpha"}])

    blob_hits = table.search().select(["id", "image"]).limit(10).to_arrow()
    assert "_rowid" in blob_hits.column_names
    assert table.take_blobs("image", blob_hits)[0].as_py() == b"alpha"

    id_hits = table.search().select(["id"]).limit(10).to_arrow()
    assert "_rowid" not in id_hits.column_names
