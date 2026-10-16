# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""Video index at ingest, keyframe-aware frame reads from a blob column."""

import io

import numpy as np
import pyarrow as pa
import pytest

import lancedb
from lancedb.video import (
    VIDEO_INDEX_COLUMNS,
    VIDEO_INDEX_SCHEMA,
    VideoFrames,
    add_videos,
    video_schema,
    _DecoderCache,
    _merge_spans,
    video_index,
    with_video_index,
)

av = pytest.importorskip("av")

FPS = 10
GOP = 10


def solid_color(frame_number: int) -> tuple[int, int, int]:
    """A distinct colour per frame, far enough apart to survive lossy encoding."""
    return (
        (frame_number * 37) % 256,
        (frame_number * 91) % 256,
        (frame_number * 53) % 256,
    )


def encode_video(
    n_frames: int,
    width: int = 64,
    height: int = 48,
    noise: bool = False,
    container_format: str = "mp4",
    codec: str = "libx264",
) -> bytes:
    """A clip with a keyframe every GOP frames; frame i is a solid colour, or noise."""
    rng = np.random.default_rng(0)
    buffer = io.BytesIO()
    with av.open(buffer, "w", format=container_format) as container:
        stream = container.add_stream(codec, rate=FPS)
        stream.width, stream.height, stream.pix_fmt = width, height, "yuv420p"
        stream.options = {"g": str(GOP), "crf": "18", "preset": "ultrafast"}
        for i in range(n_frames):
            image = np.empty((height, width, 3), dtype=np.uint8)
            image[:] = (
                rng.integers(0, 256, image.shape, dtype=np.uint8)
                if noise
                else solid_color(i)
            )
            for packet in stream.encode(
                av.VideoFrame.from_ndarray(image, format="rgb24")
            ):
                container.mux(packet)
        for packet in stream.encode():
            container.mux(packet)
    return buffer.getvalue()


def mean_color(frame) -> np.ndarray:
    """Mean RGB of a (3, H, W) frame, tensor or array."""
    array = frame.numpy() if hasattr(frame, "numpy") else np.asarray(frame)
    return array.reshape(3, -1).mean(axis=1)


@pytest.fixture
def clips_table(tmp_path):
    videos = [encode_video(60), encode_video(25)]
    schema = pa.schema(
        [pa.field("name", pa.string()), lancedb.blob("video"), *VIDEO_INDEX_SCHEMA]
    )
    table = lancedb.connect(tmp_path).create_table("clips", schema=schema)
    # bytes are written as plain binary and land in the blob column by name
    rows = pa.table({"name": ["a", "b"], "video": pa.array(videos, pa.large_binary())})
    table.add(with_video_index(rows, "video"))
    row_ids = (
        table.search()
        .select(["name"])
        .with_row_id(True)
        .to_arrow()["_rowid"]
        .to_pylist()
    )
    return table, row_ids, videos


def test_video_index_records_header_and_keyframes():
    index = video_index(encode_video(60))
    assert index["file_size"] == len(encode_video(60))
    header = list(zip(index["header_offsets"], index["header_lengths"]))
    assert header and all(0 <= o and o + n <= index["file_size"] for o, n in header)
    assert index["kf_indices"] == [0, 10, 20, 30, 40, 50]
    assert index["kf_positions"] == sorted(index["kf_positions"])
    assert all(0 < p < index["file_size"] for p in index["kf_positions"])


def test_with_video_index_appends_columns(clips_table):
    table, _, videos = clips_table
    assert all(name in table.schema.names for name in VIDEO_INDEX_COLUMNS)
    sizes = table.search().select(["file_size"]).to_arrow()["file_size"].to_pylist()
    assert sorted(sizes) == sorted(len(v) for v in videos)


def test_fetch_frames_returns_the_right_frames(clips_table):
    table, row_ids, _ = clips_table
    frames = VideoFrames(table, "video")
    clips = frames.fetch_frames(
        [(row_ids[0], [3, 17, 59]), (row_ids[1], [24]), (row_ids[0], [0])]
    )
    assert tuple(clips[0].shape) == (3, 3, 48, 64)
    assert tuple(clips[1].shape) == (1, 3, 48, 64)
    for clip, numbers in ((clips[0], [3, 17, 59]), (clips[1], [24]), (clips[2], [0])):
        for frame, number in zip(clip, numbers):
            assert np.abs(mean_color(frame) - solid_color(number)).max() < 12


def test_fetch_reads_a_span_not_the_file(tmp_path):
    video = encode_video(
        100, width=640, height=480, noise=True
    )  # a few MB, many keyframe spans
    schema = pa.schema([lancedb.blob("video"), *VIDEO_INDEX_SCHEMA])
    table = lancedb.connect(tmp_path).create_table("big", schema=schema)
    table.add(
        with_video_index(
            pa.table({"video": pa.array([video], pa.large_binary())}), "video"
        )
    )
    [row_id] = (
        table.search()
        .select(["file_size"])
        .with_row_id(True)
        .to_arrow()["_rowid"]
        .to_pylist()
    )

    frames = VideoFrames(table, "video")
    [clip] = frames.fetch_frames([(row_id, [31, 32])])
    assert tuple(clip.shape) == (2, 3, 480, 640)
    _, source = frames._cache.get(row_id)
    fetched = source.buffered + source.fallback_bytes
    assert (
        fetched < len(video) / 4
    )  # the window's keyframe span plus the file head, not the file
    # a second window on the same file only adds its own span
    frames.fetch_frames([(row_id, [85])])
    assert fetched < source.buffered + source.fallback_bytes < len(video) / 2


def test_fetch_by_timestamp(clips_table):
    table, row_ids, _ = clips_table
    frames = VideoFrames(table, "video")
    [clip] = frames.fetch([(row_ids[0], [2.0, 2.1])], fps=FPS)
    assert np.abs(mean_color(clip[0]) - solid_color(20)).max() < 12
    assert np.abs(mean_color(clip[1]) - solid_color(21)).max() < 12


def test_decoder_cache_evicts_by_count_and_bytes():
    cache = _DecoderCache(capacity=2, byte_budget=100)
    cache.put(1, "a", nbytes=60)
    cache.put(2, "b", nbytes=30)
    cache.put(3, "c", nbytes=30)  # over count: evicts 1
    assert 1 not in cache and 2 in cache and 3 in cache
    cache.put(4, "d", nbytes=90)  # over bytes: evicts 2 then 3
    assert 4 in cache and 2 not in cache and 3 not in cache


def test_merge_spans_coalesces_nearby_ranges():
    assert _merge_spans([(0, 10), (5, 20), (1_000_000, 1_000_010)]) == [
        (0, 20),
        (1_000_000, 1_000_010),
    ]


@pytest.mark.parametrize(
    "container_format,codec",
    [("mp4", "libx264"), ("mov", "libx264"), ("matroska", "libx264"), ("avi", "mpeg4")],
)
def test_other_containers_and_codecs(tmp_path, container_format, codec):
    video = encode_video(40, container_format=container_format, codec=codec)
    schema = pa.schema([lancedb.blob("video"), *VIDEO_INDEX_SCHEMA])
    table = lancedb.connect(tmp_path).create_table("clip", schema=schema)
    rows = pa.table({"video": pa.array([video], pa.large_binary())})
    table.add(with_video_index(rows, "video"))
    hits = table.search().select(["file_size"]).with_row_id(True).to_arrow()
    [row_id] = hits["_rowid"].to_pylist()

    [clip] = VideoFrames(table, "video").fetch_frames([(row_id, [5, 33])])
    assert tuple(clip.shape) == (2, 3, 48, 64)
    assert np.abs(mean_color(clip[0]) - solid_color(5)).max() < 16
    assert np.abs(mean_color(clip[1]) - solid_color(33)).max() < 16


def test_video_schema_and_add_videos(tmp_path):
    (tmp_path / "a.mp4").write_bytes(encode_video(30))
    table = lancedb.connect(tmp_path / "db").create_table(
        "clips", schema=video_schema([pa.field("name", pa.string())])
    )
    add_videos(table, [tmp_path / "a.mp4", encode_video(20)], name=["a", "b"])
    assert table.count_rows() == 2
    assert all(c in table.schema.names for c in ("name", "video", *VIDEO_INDEX_COLUMNS))
    [row_id] = (
        table.search()
        .where("name = 'b'")
        .select(["name"])
        .with_row_id(True)
        .to_arrow()["_rowid"]
        .to_pylist()
    )
    [clip] = VideoFrames(table, "video").fetch_frames([(row_id, [7])])
    assert np.abs(mean_color(clip[0]) - solid_color(7)).max() < 12
