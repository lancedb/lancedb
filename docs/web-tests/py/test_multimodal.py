# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import io
import pytest
import lancedb
import numpy as np
import pyarrow as pa

try:
    # --8<-- [start:multimodal_imports]
    import io
    import lancedb
    import numpy as np
    import pyarrow as pa
    from PIL import Image
    # --8<-- [end:multimodal_imports]
except ImportError:
    pass


def test_multimodal_ingestion(db_path_factory):
    pytest.importorskip("PIL")

    # --8<-- [start:create_dummy_data]
    def create_dummy_image(color):
        img = Image.new("RGB", (100, 100), color=color)
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()

    data = [
        {
            "id": 1,
            "filename": "red_square.png",
            "vector": np.random.rand(128).astype(np.float32),
            "image_blob": create_dummy_image("red"),
            "label": "red",
        },
        {
            "id": 2,
            "filename": "blue_square.png",
            "vector": np.random.rand(128).astype(np.float32),
            "image_blob": create_dummy_image("blue"),
            "label": "blue",
        },
    ]
    # --8<-- [end:create_dummy_data]

    # --8<-- [start:define_schema]
    schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("filename", pa.string()),
            pa.field("vector", pa.list_(pa.float32(), 128)),
            pa.field("image_blob", pa.binary()),
            pa.field("label", pa.string()),
        ]
    )
    # --8<-- [end:define_schema]

    db = lancedb.connect(db_path_factory("multimodal_db"))

    # --8<-- [start:ingest_data]
    tbl = db.create_table("images", data=data, schema=schema, mode="overwrite")
    # --8<-- [end:ingest_data]

    assert len(tbl) == 2

    # --8<-- [start:search_data]
    query_vector = np.random.rand(128).astype(np.float32)
    results = tbl.search(query_vector).limit(1).to_pandas()
    # --8<-- [end:search_data]

    # --8<-- [start:process_results]
    for _, row in results.iterrows():
        image_bytes = row["image_blob"]
        image = Image.open(io.BytesIO(image_bytes))
        print(f"Retrieved image: {row['filename']}, Size: {image.size}")
    # --8<-- [end:process_results]

    assert len(results) == 1


def test_blob_api_definition(tmp_path, monkeypatch):
    pytest.importorskip("lance")
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
            lancedb.blob(
                "video",
                inline_size_threshold=64 * 1024,
                dedicated_size_threshold=4 * 1024 * 1024,
            ),
        ]
    )
    # --8<-- [end:blob_api_schema]

    # --8<-- [start:blob_api_ingest]
    tbl = db.create_table("videos", schema=schema)

    tbl.add(
        [
            {"id": 1, "video": b"inline_video_bytes"},
            {"id": 2, "video": b"second_video_bytes"},
        ]
    )
    # --8<-- [end:blob_api_ingest]
    assert len(tbl) == 2

    # --8<-- [start:blob_api_fetch]
    rows = tbl.search().select(["id", "video"]).with_row_id(True).to_list()
    row_ids = [row["_rowid"] for row in rows]

    payloads = tbl.fetch_blobs("video", row_ids)

    with tbl.fetch_blob_files("video", [row_ids[0]])[0] as blob_file:
        header = blob_file.read_range(0, 6)
    # --8<-- [end:blob_api_fetch]

    assert payloads.to_pylist() == [
        b"inline_video_bytes",
        b"second_video_bytes",
    ]
    assert header == b"inline"

    # --8<-- [start:blob_api_ranges]
    slices = tbl.fetch_blob_ranges(
        "video",
        [(row_ids[0], 0, 6), (row_ids[1], 0, 6), (row_ids[1], 7, 5)],
    )

    with tbl.fetch_blob_files("video", [row_ids[1]])[0] as blob_file:
        parts = blob_file.read_ranges([(0, 6), (7, 5)])
    # --8<-- [end:blob_api_ranges]

    assert slices.to_pylist() == [b"inline", b"second", b"video"]
    assert parts == [b"second", b"video"]


def test_legacy_blob_api_definition(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # --8<-- [start:legacy_blob_api_schema]
    import pyarrow as pa

    # Define schema with Blob API metadata for lazy loading
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "video",
                pa.large_binary(),
                metadata={"lance-encoding:blob": "true"},  # Enable Blob API
            ),
        ]
    )
    # --8<-- [end:legacy_blob_api_schema]

    # --8<-- [start:legacy_blob_api_ingest]
    import lancedb

    db = lancedb.connect("./blob_db")

    # Create sample data
    data = [
        {"id": 1, "video": b"fake_video_bytes_1"},
        {"id": 2, "video": b"fake_video_bytes_2"},
    ]

    # Create the table
    tbl = db.create_table("videos", data=data, schema=schema)
    # --8<-- [end:legacy_blob_api_ingest]
    assert len(tbl) == 2


def test_legacy_blob_api_to_pandas(db_path_factory):
    db = lancedb.connect(db_path_factory("legacy_blob_to_pandas_db"))
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "video",
                pa.large_binary(),
                metadata={"lance-encoding:blob": "true"},
            ),
        ]
    )
    tbl = db.create_table(
        "videos",
        data=[
            {"id": 1, "video": b"fake_video_bytes_1"},
            {"id": 2, "video": b"fake_video_bytes_2"},
        ],
        schema=schema,
        mode="overwrite",
    )

    # --8<-- [start:legacy_blob_api_to_pandas]
    # Default: blob columns come back lazily
    df_lazy = tbl.to_pandas()

    # Materialize blob bytes eagerly
    df_bytes = tbl.to_pandas(blob_mode="bytes")

    # Return descriptors instead of payloads
    df_desc = tbl.to_pandas(blob_mode="descriptions")

    # Forward extra kwargs to PyArrow's to_pandas
    df_typed = tbl.to_pandas(split_blocks=True, self_destruct=True)
    # --8<-- [end:legacy_blob_api_to_pandas]

    assert len(df_lazy) == 2
    assert isinstance(df_bytes["video"].iloc[0], bytes)
    assert df_bytes["video"].tolist() == [
        b"fake_video_bytes_1",
        b"fake_video_bytes_2",
    ]
    assert len(df_desc) == 2
    assert len(df_typed) == 2


@pytest.mark.asyncio
async def test_legacy_query_to_pandas_kwargs(db_path_factory):
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("vector", pa.list_(pa.float32(), 128)),
            pa.field(
                "video",
                pa.large_binary(),
                metadata={"lance-encoding:blob": "true"},
            ),
        ]
    )
    data = [
        {
            "id": i,
            "vector": np.random.rand(128).astype(np.float32),
            "video": f"fake_video_bytes_{i}".encode(),
        }
        for i in range(10)
    ]

    db = lancedb.connect(db_path_factory("legacy_query_to_pandas_db"))
    tbl = db.create_table("search_demo", data=data, schema=schema, mode="overwrite")

    async_db = await lancedb.connect_async(
        str(db_path_factory("legacy_query_to_pandas_async_db"))
    )
    tbl_async = await async_db.create_table(
        "search_demo", data=data, schema=schema, mode="overwrite"
    )

    query_vector = np.random.rand(128).astype(np.float32)

    # --8<-- [start:legacy_query_to_pandas_kwargs]
    # Plain scan query: blob_mode is supported end to end
    df_lazy = (
        tbl.search()
        .where("id = 1")
        .select(["id", "video"])
        .to_pandas(blob_mode="lazy")
    )

    # Same call shape works on async query builders
    df_bytes = await (
        tbl_async.query()
        .where("id = 1")
        .select(["id", "video"])
        .to_pandas(blob_mode="bytes")
    )

    # Vector / FTS / hybrid queries can't materialize blob columns,
    # so omit them from the projection
    df_vec = (
        tbl.search(query_vector)
        .limit(10)
        .select(["id", "vector"])
        .to_pandas(split_blocks=True, self_destruct=True)
    )
    # --8<-- [end:legacy_query_to_pandas_kwargs]

    assert len(df_lazy) == 1
    assert len(df_bytes) == 1
    assert df_bytes["video"].iloc[0] == b"fake_video_bytes_1"
    assert len(df_vec) == 10
    assert "video" not in df_vec.columns


def test_append_keeps_table_blob_thresholds(tmp_path):
    pytest.importorskip("lance")
    import lance

    db = lancedb.connect(tmp_path)
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            lancedb.blob("video", inline_size_threshold=64 * 1024),
        ]
    )
    tbl = db.create_table("videos", schema=schema)
    other = pa.schema(
        [
            pa.field("id", pa.int64()),
            lancedb.blob("video", inline_size_threshold=8 * 1024),
        ]
    )
    payload = b"x" * (20 * 1024)
    tbl.add(
        pa.table(
            [pa.array([1], type=pa.int64()), lance.blob_array([payload])],
            schema=other,
        )
    )

    stored = tbl.schema.field("video").metadata[
        b"lance-encoding:blob-inline-size-threshold"
    ]
    assert stored == b"65536"
    descriptor = tbl.search().select(["video"]).to_arrow(blob_mode="descriptions")
    assert descriptor.column("video")[0].as_py()["kind"] == 0


def test_blob_api_to_pandas(db_path_factory):
    db = lancedb.connect(db_path_factory("blob_to_pandas_db"))
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            lancedb.blob("video"),
        ]
    )
    tbl = db.create_table(
        "videos",
        data=[
            {"id": 1, "video": b"fake_video_bytes_1"},
            {"id": 2, "video": b"fake_video_bytes_2"},
        ],
        schema=schema,
        mode="overwrite",
    )

    # --8<-- [start:blob_api_to_pandas]
    df_lazy = tbl.to_pandas()

    df_bytes = tbl.to_pandas(blob_mode="bytes")

    df_desc = tbl.to_pandas(blob_mode="descriptions")

    arrow_bytes = tbl.to_arrow(blob_mode="bytes")

    df_typed = tbl.to_pandas(split_blocks=True, self_destruct=True)
    # --8<-- [end:blob_api_to_pandas]

    assert len(df_lazy) == 2
    assert isinstance(df_bytes["video"].iloc[0], bytes)
    assert df_bytes["video"].tolist() == [
        b"fake_video_bytes_1",
        b"fake_video_bytes_2",
    ]
    assert arrow_bytes["video"].to_pylist() == [
        b"fake_video_bytes_1",
        b"fake_video_bytes_2",
    ]
    assert len(df_desc) == 2
    assert len(df_typed) == 2


@pytest.mark.asyncio
async def test_query_to_pandas_kwargs(db_path_factory):
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("vector", pa.list_(pa.float32(), 128)),
            lancedb.blob("video"),
        ]
    )
    data = [
        {
            "id": i,
            "vector": np.random.rand(128).astype(np.float32),
            "video": f"fake_video_bytes_{i}".encode(),
        }
        for i in range(10)
    ]

    db = lancedb.connect(db_path_factory("query_to_pandas_db"))
    tbl = db.create_table("search_demo", data=data, schema=schema, mode="overwrite")

    async_db = await lancedb.connect_async(
        str(db_path_factory("query_to_pandas_async_db"))
    )
    tbl_async = await async_db.create_table(
        "search_demo", data=data, schema=schema, mode="overwrite"
    )

    query_vector = np.random.rand(128).astype(np.float32)

    # --8<-- [start:query_to_pandas_kwargs]
    df_lazy = (
        tbl.search().where("id = 1").select(["id", "video"]).to_pandas(blob_mode="lazy")
    )

    df_bytes = await (
        tbl_async.query()
        .where("id = 1")
        .select(["id", "video"])
        .to_pandas(blob_mode="bytes")
    )

    df_vec = (
        tbl.search(query_vector)
        .limit(2)
        .select(["id", "video"])
        .to_pandas(blob_mode="bytes", split_blocks=True, self_destruct=True)
    )
    # --8<-- [end:query_to_pandas_kwargs]

    assert len(df_lazy) == 1
    assert len(df_bytes) == 1
    assert df_bytes["video"].iloc[0] == b"fake_video_bytes_1"
    assert len(df_vec) == 2
    assert isinstance(df_vec["video"].iloc[0], bytes)
