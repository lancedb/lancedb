# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


def test_photo_tutorial(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # --8<-- [start:photo_files]
    import struct
    import zlib
    from pathlib import Path

    def solid_png(rgb):
        def chunk(kind, data):
            checksum = zlib.crc32(kind + data) & 0xFFFFFFFF
            return (
                struct.pack(">I", len(data)) + kind + data + struct.pack(">I", checksum)
            )

        header = struct.pack(">IIBBBBB", 8, 8, 8, 2, 0, 0, 0)
        pixels = (b"\x00" + bytes(rgb) * 8) * 8
        return (
            b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", header)
            + chunk(b"IDAT", zlib.compress(pixels))
            + chunk(b"IEND", b"")
        )

    inputs = Path("photo-inputs")
    inputs.mkdir()
    for label, rgb in [("red", (255, 0, 0)), ("blue", (0, 0, 255))]:
        (inputs / f"{label}.png").write_bytes(solid_png(rgb))
    # --8<-- [end:photo_files]

    # --8<-- [start:photo_connect_embedded]
    import lancedb

    db = lancedb.connect("./photo-db")
    # --8<-- [end:photo_connect_embedded]

    # --8<-- [start:photo_ingest]
    import pyarrow as pa

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("label", pa.string(), nullable=False),
            lancedb.blob("image", nullable=False),
        ],
        metadata={"lance.blob.managed": "false"},
    )
    photos = db.create_table("photos", schema=schema)
    photos.add(
        [
            {"id": 1, "label": "red", "image": (inputs / "red.png").read_bytes()},
            {"id": 2, "label": "blue", "image": (inputs / "blue.png").read_bytes()},
        ]
    )
    assert photos.count_rows() == 2
    # --8<-- [end:photo_ingest]

    # --8<-- [start:photo_fetch]
    hits = (
        photos.search()
        .where("label = 'red'")
        .select(["id", "label"])
        .with_row_id(True)
        .limit(10)
        .to_list()
    )
    assert [row["id"] for row in hits] == [1]
    payloads = photos.fetch_blobs("image", [row["_rowid"] for row in hits]).to_pylist()
    assert payloads == [(inputs / "red.png").read_bytes()]
    saved = Path("selected-red.png")
    saved.write_bytes(payloads[0])
    assert saved.read_bytes() == (inputs / "red.png").read_bytes()
    # --8<-- [end:photo_fetch]

    # --8<-- [start:photo_bytes]
    rows = (
        photos.search()
        .where("label = 'red'")
        .select(["id", "image"])
        .limit(10)
        .to_list(blob_mode="bytes")
    )
    assert rows == [{"id": 1, "image": (inputs / "red.png").read_bytes()}]
    # --8<-- [end:photo_bytes]

    # --8<-- [start:photo_ranges]
    with photos.fetch_blob_files("image", [hits[0]["_rowid"]])[0] as reader:
        assert reader.read_range(0, 8) == b"\x89PNG\r\n\x1a\n"
    # --8<-- [end:photo_ranges]

    # --8<-- [start:photo_copy_embedded]
    selected = (
        photos.search()
        .where("label = 'red'")
        .select(["id", "label", "image"])
        .limit(10)
        .to_arrow(blob_mode="bytes")
    )
    db.create_table("photo_subset", data=selected, schema=schema)
    # --8<-- [end:photo_copy_embedded]

    # --8<-- [start:photo_verify]
    photo_subset = db.open_table("photo_subset")
    assert photo_subset.count_rows() == 1
    copied = (
        photo_subset.search()
        .select(["id", "label"])
        .with_row_id(True)
        .limit(10)
        .to_list()
    )
    assert [(row["id"], row["label"]) for row in copied] == [(1, "red")]
    copied_bytes = photo_subset.fetch_blobs(
        "image", [row["_rowid"] for row in copied]
    ).to_pylist()
    assert copied_bytes == [(inputs / "red.png").read_bytes()]
    # --8<-- [end:photo_verify]


def enterprise_connection():
    # --8<-- [start:photo_connect_enterprise]
    import os

    import lancedb

    db = lancedb.connect(
        os.environ["LANCEDB_URI"],
        api_key=os.environ["LANCEDB_API_KEY"],
        region=os.environ["LANCEDB_REGION"],
        host_override=os.environ["LANCEDB_HOST_OVERRIDE"],
        sql_host_override=os.environ["LANCEDB_SQL_HOST_OVERRIDE"],
    )
    # --8<-- [end:photo_connect_enterprise]
    return db


def copy_with_enterprise_sql(db):
    # --8<-- [start:photo_copy_sql]
    db.execute_query(
        "CREATE TABLE photo_subset (id BIGINT, label VARCHAR, image BLOB)"
    ).read_all()
    db.execute_query(
        "INSERT INTO photo_subset (id, label, image) "
        "SELECT id, label, image FROM photos WHERE label = 'red'"
    ).read_all()
    # --8<-- [end:photo_copy_sql]
