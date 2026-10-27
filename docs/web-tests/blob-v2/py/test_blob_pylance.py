# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Blob reads that import pylance.

Install pylance==13.0.0 with --no-deps after lancedb==0.41.0b10.
See docs/web-tests/blob-v2/README.md.
"""

import lance
import lancedb
import numpy as np
import pyarrow as pa
import pytest


def test_blob_api_to_pandas(tmp_path):
    db = lancedb.connect(tmp_path)
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
    assert all(isinstance(value, lance.BlobFile) for value in df_lazy["video"])
    assert isinstance(df_bytes["video"].iloc[0], bytes)
    assert df_bytes["video"].tolist() == [
        b"fake_video_bytes_1",
        b"fake_video_bytes_2",
    ]
    assert arrow_bytes["video"].to_pylist() == [
        b"fake_video_bytes_1",
        b"fake_video_bytes_2",
    ]
    assert "_rowid" not in df_bytes.columns
    assert len(df_desc) == 2
    assert set(df_desc["video"].iloc[0]) == {
        "kind",
        "position",
        "size",
        "blob_id",
        "blob_uri",
    }
    assert len(df_typed) == 2


@pytest.mark.asyncio
async def test_query_to_pandas_kwargs(tmp_path):
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

    db = lancedb.connect(tmp_path / "sync")
    tbl = db.create_table("search_demo", data=data, schema=schema, mode="overwrite")

    async_db = await lancedb.connect_async(str(tmp_path / "async"))
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
    assert "_rowid" not in df_bytes.columns
    assert len(df_bytes) == 1
    assert df_bytes["video"].iloc[0] == b"fake_video_bytes_1"
    assert len(df_vec) == 2
    assert isinstance(df_vec["video"].iloc[0], bytes)


def test_append_keeps_table_blob_thresholds(tmp_path):
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
