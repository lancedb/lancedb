# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import importlib.util

import lancedb
import pyarrow as pa

if importlib.util.find_spec("lance") is not None:
    raise RuntimeError(
        "Blob v2 documentation tests must run without pylance. "
        "Install docs/web-tests/blob-v2/py/requirements.txt into a fresh virtualenv."
    )

INLINE_MAX = 64 * 1024
DEDICATED_MIN = 4 * 1024 * 1024


def test_blob_api_definition(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # --8<-- [start:blob_api_setup]
    import lancedb

    db = lancedb.connect("./blob_db")
    # --8<-- [end:blob_api_setup]
    assert db.list_tables().tables == []

    # --8<-- [start:blob_api_schema]
    import lancedb
    import pyarrow as pa

    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("label", pa.string()),
            lancedb.blob("image"),
        ]
    )
    # --8<-- [end:blob_api_schema]
    image = schema.field("image").type
    assert image.extension_name == "lance.blob.v2"
    assert type(image).__module__ == "lancedb.schema"

    # --8<-- [start:blob_api_ingest]
    import io
    from PIL import Image

    def jpeg_with_make(color, make):
        image = Image.new("RGB", (8, 8), color=color)
        exif = Image.Exif()
        exif[0x010F] = make
        buf = io.BytesIO()
        image.save(buf, format="JPEG", exif=exif)
        return buf.getvalue()

    tbl = db.create_table(
        "photos",
        data=[
            {"id": 1, "label": "red", "image": jpeg_with_make("red", "TestCam")},
            {"id": 2, "label": "blue", "image": jpeg_with_make("blue", "OtherCam")},
        ],
        schema=schema,
        mode="overwrite",
    )
    # --8<-- [end:blob_api_ingest]
    assert len(tbl) == 2

    # --8<-- [start:blob_api_bytes]
    rows = (
        tbl.search().where("id = 1").select(["id", "image"]).to_list(blob_mode="bytes")
    )
    # --8<-- [end:blob_api_bytes]
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert "_rowid" not in rows[0]
    assert rows[0]["image"].startswith(b"\xff\xd8")

    # --8<-- [start:blob_api_files]
    from PIL import ExifTags

    rows = tbl.search().where("id = 1").select(["label"]).with_row_id(True).to_list()
    with tbl.fetch_blob_files("image", [rows[0]["_rowid"]])[0] as image_file:
        make = Image.open(image_file).getexif().get(ExifTags.Base.Make)
    print(rows[0]["label"], make)
    # --8<-- [end:blob_api_files]
    assert rows[0]["label"] == "red"
    assert make == "TestCam"

    # --8<-- [start:blob_api_fetch]
    rows = tbl.search().select(["id"]).with_row_id(True).to_list()
    row_ids = [row["_rowid"] for row in rows]
    payloads = tbl.fetch_blobs("image", row_ids)
    # --8<-- [end:blob_api_fetch]
    assert all(payload.startswith(b"\xff\xd8") for payload in payloads.to_pylist())

    # --8<-- [start:blob_api_ranges]
    slices = tbl.fetch_blob_ranges("image", [(row_ids[0], 0, 2)])

    with tbl.fetch_blob_files("image", [row_ids[0]])[0] as image_file:
        parts = image_file.read_ranges([(0, 2)])
    # --8<-- [end:blob_api_ranges]
    assert slices.to_pylist() == [b"\xff\xd8"]
    assert parts == [b"\xff\xd8"]


def test_eager_query_returns_blob_bytes_without_row_id(tmp_path):
    db = lancedb.connect(tmp_path)
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    payload = b"inline-bytes"
    tbl = db.create_table(
        "photos",
        data=[{"id": 1, "image": payload}],
        schema=schema,
    )

    rows = (
        tbl.search().where("id = 1").select(["id", "image"]).to_list(blob_mode="bytes")
    )
    arrow_bytes = (
        tbl.search().where("id = 1").select(["id", "image"]).to_arrow(blob_mode="bytes")
    )
    described = tbl.to_pandas(blob_mode="descriptions")

    assert rows == [{"id": 1, "image": payload}]
    assert "_rowid" not in rows[0]
    assert arrow_bytes.column("image").to_pylist() == [payload]
    assert "_rowid" not in arrow_bytes.column_names
    assert described["image"].iloc[0]["kind"] == 0


def test_deferred_fetch_reads_blob_by_row_id(tmp_path):
    db = lancedb.connect(tmp_path)
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    payload = b"deferred-bytes"
    tbl = db.create_table(
        "photos",
        data=[{"id": 1, "image": payload}],
        schema=schema,
    )

    described = (
        tbl.search()
        .where("id = 1")
        .select(["id", "image"])
        .to_list(blob_mode="descriptions")
    )
    assert described[0]["image"]["kind"] == 0
    assert "_rowid" not in described[0]

    found = tbl.search().where("id = 1").select(["id"]).with_row_id(True).to_list()
    row_id = found[0]["_rowid"]
    fetched = tbl.fetch_blobs("image", [row_id]).to_pylist()
    with tbl.fetch_blob_files("image", [row_id])[0] as handle:
        streamed = handle.read()

    assert fetched == [payload]
    assert streamed == payload


def _marked(size, marker):
    payload = bytearray(size)
    payload[0] = marker
    payload[-1] = marker
    return bytes(payload)


def test_storage_tiers_round_trip(tmp_path):
    cases = [
        (1, _marked(INLINE_MAX, 11), 0),
        (2, _marked(INLINE_MAX + 1, 22), 1),
        (3, _marked(DEDICATED_MIN, 33), 1),
        (4, _marked(DEDICATED_MIN + 1, 44), 2),
    ]
    db = lancedb.connect(tmp_path)
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    tbl = db.create_table(
        "tiers",
        data=[{"id": row_id, "image": payload} for row_id, payload, _kind in cases],
        schema=schema,
    )

    described = {
        row["id"]: row["image"]
        for row in tbl.search()
        .select(["id", "image"])
        .to_list(blob_mode="descriptions")
    }
    located = {
        row["id"]: row["_rowid"]
        for row in tbl.search().select(["id"]).with_row_id(True).to_list()
    }
    fetched = {
        row_id: tbl.fetch_blobs("image", [located[row_id]]).to_pylist()[0]
        for row_id, _payload, _kind in cases
    }

    for row_id, payload, kind in cases:
        descriptor = described[row_id]
        assert descriptor["kind"] == kind
        assert descriptor["size"] == len(payload)
        assert fetched[row_id] == payload
        with tbl.fetch_blob_files("image", [located[row_id]])[0] as handle:
            assert handle.read_range(0, 1) == payload[:1]
            assert handle.read_range(len(payload) - 1, 1) == payload[-1:]
