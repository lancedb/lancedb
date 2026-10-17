# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""Frame reads from video files stored in a blob column.

Index each file once at ingest (:func:`with_video_index`), then
:class:`VideoFrames` fetches frames by row and frame number, reading only the
byte ranges the decoder needs: the container header (whatever the demuxer read
to open and seek the file at index time: the mp4 ``moov`` box, an AVI ``idx1``
index, Matroska cues) and the span from the keyframe before the first requested
frame to the keyframe after the last one. An 8-frame window of a 500 MB file
costs about 1 MB of transfer, most of it the header, read once per file.

The video index is a few plain columns next to the blob (byte spans, keyframe
positions, frame count, fps), so it travels with the table and needs no sidecar::

    import lancedb
    from lancedb.video import VideoFrames, add_videos, video_schema

    db = lancedb.connect("s3://bucket/clips")
    schema = video_schema([pa.field("name", pa.string())])
    table = db.create_table("clips", schema=schema)
    add_videos(table, paths, name=names)  # reads, indexes and writes the files

    frames = VideoFrames(table, "video", key="name")  # one per process or worker
    clips = frames.fetch_frames([("clip_0", [0, 1, 2]), ("clip_3", [120])])
    # clips[0]: uint8 tensor (3, 3, H, W); clips[1]: (1, 3, H, W)

For training, :class:`VideoTransform` turns rows of any table that points at
(video, frame) into items with decoded clips. A video generation loader over a
captioned video table, one sample per 16-frame window::

    import pyarrow as pa
    import lancedb
    from lancedb.streaming import StreamingDataset, StreamingDataLoader
    from lancedb.video import (
        VideoTransform, add_videos, clip_windows, video_schema,
    )

    db = lancedb.connect(uri)

    # videos: one row per file
    #   caption: string | video: blob | file_size, header_offsets, header_lengths,
    #   kf_indices, kf_positions, n_frames, fps: the index, filled by add_videos
    schema = video_schema([pa.field("caption", pa.string())])
    videos = db.create_table("videos", schema=schema)
    add_videos(videos, paths, caption=captions)

    # clips: one row per training sample, a 16-frame window into one video
    #   video_row: int64 (row id in videos) | start: int64 (first frame)
    #   | caption: string, copied from the video's row
    #   a 240-frame video gives 15 rows, no overlap; stride=8 would give 29
    windows = clip_windows(videos, 16, columns=["caption"])
    clips = db.create_table("clips", windows)

    # item per clips row: {"caption": str, "video": uint8 (16, 3, H, W)}
    transform = VideoTransform(
        uri, "videos", fields={"video": ("video_row", "start")}, window=range(16)
    )
    ds = StreamingDataset(clips, num_splits=4, shuffle=True, transform=transform)
    loader = StreamingDataLoader(ds, batch_size=8, num_workers=4)
    # batch: {"caption": [8 strings], "video": uint8 tensor (8, 16, 3, H, W)}

Requires ``av`` (PyAV) to build the index. Decoding uses torchcodec when it is
installed and falls back to PyAV otherwise. Works with any container and codec
FFmpeg can demux and decode; mp4, mov, mkv and avi are covered by the tests.
"""

from __future__ import annotations

import bisect
import io
import multiprocessing as mp
import threading
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Sequence

import numpy as np
import pyarrow as pa

VIDEO_INDEX_COLUMNS = (
    "file_size",
    "header_offsets",
    "header_lengths",
    "kf_indices",
    "kf_positions",
    "n_frames",
    "fps",
)
VIDEO_INDEX_SCHEMA = pa.schema(
    [
        pa.field("file_size", pa.int64()),
        # byte ranges the demuxer reads to open the file and seek into it
        pa.field("header_offsets", pa.list_(pa.int64())),
        pa.field("header_lengths", pa.list_(pa.int64())),
        # frame number and byte position of every keyframe
        pa.field("kf_indices", pa.list_(pa.int64())),
        pa.field("kf_positions", pa.list_(pa.int64())),
        pa.field("n_frames", pa.int64()),
        pa.field("fps", pa.float64()),
    ]
)

# Decoders read past the bytes a frame span strictly needs: the container probe on
# open and the next chunk header after a span. Padding prefetched ranges by these
# amounts
# keeps those reads off the one-at-a-time fallback path.
_PROBE_BYTES = 256 << 10
_SLACK = 64 << 10
# Per-file keyframe indexes kept in memory per VideoFrames
# (a file with 45k keyframes is about 720 KB).
_INDEX_CACHE_FILES = 2048


# ---------------------------------------------------------------------------
# Index: computed once per file, stored as columns
# ---------------------------------------------------------------------------


class _TracingFile(io.RawIOBase):
    """Pass-through file that records the byte ranges read through it."""

    def __init__(self, inner: io.IOBase, spans: list[tuple[int, int]]) -> None:
        super().__init__()
        self._inner = inner
        self._spans = spans

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._inner.seek(offset, whence)

    def tell(self) -> int:
        return self._inner.tell()

    def readinto(self, buffer) -> int:
        start = self._inner.tell()
        data = self._inner.read(len(buffer))
        buffer[: len(data)] = data
        if data:
            self._spans.append((start, start + len(data)))
        return len(data)


def video_index(source: str | Path | bytes | io.IOBase) -> dict[str, Any]:
    """The byte index of one video file.

    ``header_offsets``/``header_lengths`` are the ranges the demuxer read to
    open the file and seek into it, so a reader can prefetch them without
    knowing the container. ``kf_indices[i]`` is the frame number of the i-th
    keyframe and ``kf_positions[i]`` its byte offset, so a frame span maps to
    a byte span.
    """
    import av

    if isinstance(source, (str, Path)):
        handle: io.IOBase = open(source, "rb")
    elif isinstance(source, (bytes, bytearray, memoryview)):
        handle = io.BytesIO(source)
    else:
        handle = source
    handle.seek(0, io.SEEK_END)
    file_size = handle.tell()

    # 1. What the demuxer touches to open the file and seek into the middle.
    reads: list[tuple[int, int]] = []
    handle.seek(0)
    with av.open(_TracingFile(handle, reads)) as container:
        stream = container.streams.video[0]
        fps = float(stream.average_rate)
        if container.duration:
            container.seek(container.duration // 2, backward=True, any_frame=False)
        next(container.demux(stream), None)
    header = _merge_spans(reads)

    # 2. Every keyframe, from a full demux.
    keyframes: list[tuple[int, int]] = []
    n_frames = 0
    handle.seek(0)
    with av.open(handle) as container:
        stream = container.streams.video[0]
        for packet in container.demux(stream):
            if packet.pts is None:
                continue
            n_frames += 1
            if not packet.is_keyframe or packet.pos is None:
                continue
            frame_number = round(float(packet.pts * packet.time_base) * fps)
            keyframes.append((frame_number, packet.pos))
    keyframes.sort()

    if isinstance(source, (str, Path)):
        handle.close()
    return {
        "file_size": file_size,
        "header_offsets": [start for start, _ in header],
        "header_lengths": [end - start for start, end in header],
        "kf_indices": [index for index, _ in keyframes],
        "kf_positions": [position for _, position in keyframes],
        "n_frames": n_frames,
        "fps": fps,
    }


def video_schema(
    fields: Sequence[pa.Field] = (), column: str = "video", **blob_options: Any
) -> pa.Schema:
    """A table schema with a video blob column and its index columns.

    ``fields`` are the caller's own columns (``pa.field("name", pa.string())``);
    ``blob_options`` go to :func:`lancedb.blob`.
    """
    from .schema import blob

    return pa.schema([*fields, blob(column, **blob_options), *VIDEO_INDEX_SCHEMA])


def add_videos(
    table,
    videos: Sequence[str | Path | bytes],
    column: str = "video",
    *,
    workers: int | None = None,
    batch_size: int = 16,
    **columns: Sequence[Any],
) -> None:
    """Read, index and append video files to a table made with :func:`video_schema`.

    ``videos`` are paths or bytes; ``columns`` are the caller's own columns,
    one value per video, e.g. ``add_videos(table, paths, name=names)``.

    Files are indexed ``workers`` at a time in a process pool (the demux holds
    the GIL) and appended ``batch_size`` files per ``table.add``, so memory is
    bounded by one batch of files, not the whole call. ``workers=None`` runs
    in-process. For a cluster, shard the paths and call this from every
    machine: Lance appends commit concurrently.
    """
    names = list(columns)
    values = [list(v) for v in columns.values()]
    if any(len(v) != len(videos) for v in values):
        raise ValueError("every column needs one value per video")
    # spawn, not fork: lancedb's async runtime does not survive a fork cleanly
    pool = (
        ProcessPoolExecutor(workers, mp_context=mp.get_context("spawn"))
        if workers
        else None
    )
    try:
        for start in range(0, len(videos), batch_size):
            batch = list(videos[start : start + batch_size])
            paths = [
                None if isinstance(v, (bytes, bytearray)) else Path(v) for v in batch
            ]
            if pool is not None and all(paths):
                indexes = list(pool.map(video_index, paths))
            else:
                indexes = [
                    video_index(v if p is None else p) for v, p in zip(batch, paths)
                ]
            data = [v if p is None else p.read_bytes() for v, p in zip(batch, paths)]
            rows = pa.table(
                {
                    **{n: v[start : start + batch_size] for n, v in zip(names, values)},
                    column: pa.array(data, pa.large_binary()),
                }
            )
            table.add(_append_index(rows, indexes))
    finally:
        if pool is not None:
            pool.shutdown()


def _append_index(table: pa.Table, indexes: list[dict[str, Any]]) -> pa.Table:
    index_table = pa.Table.from_pylist(indexes, schema=VIDEO_INDEX_SCHEMA)
    for field in VIDEO_INDEX_SCHEMA:
        table = table.append_column(field, index_table.column(field.name))
    return table


def with_video_index(data: pa.Table | pa.RecordBatch, column: str) -> pa.Table:
    """Append the video index columns for the video files in ``column``.

    Use at ingest, before ``create_table`` or ``add``, so the index is written
    together with the blobs.
    """
    table = pa.Table.from_batches([data]) if isinstance(data, pa.RecordBatch) else data
    return _append_index(table, [video_index(b.as_py()) for b in table.column(column)])


# ---------------------------------------------------------------------------
# Range-fetched file view and decoder cache
# ---------------------------------------------------------------------------


def _merge_spans(
    spans: list[tuple[int, int]], gap: int = _SLACK
) -> list[tuple[int, int]]:
    """Coalesce overlapping or nearby byte ranges into fewer requests."""
    merged: list[tuple[int, int]] = []
    for start, end in sorted(spans):
        if merged and start <= merged[-1][1] + gap:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


class _SparseFile(io.RawIOBase):
    """File-like view over a blob: serves fetched ranges, fetches the rest on demand."""

    def __init__(self, size: int, open_handle: Callable[[], Any]) -> None:
        super().__init__()
        self._size = size
        self._open_handle = open_handle
        self._handle = None
        self._starts: list[int] = []
        self._chunks: list[bytes] = []
        self._pos = 0
        self.buffered = 0
        self.fallback_bytes = 0

    def add(self, offset: int, data: bytes) -> None:
        end = offset + len(data)
        lo = bisect.bisect_left(self._starts, offset)
        if lo > 0 and self._starts[lo - 1] + len(self._chunks[lo - 1]) >= offset:
            lo -= 1
        hi = bisect.bisect_right(self._starts, end)
        if lo == hi:
            self._starts.insert(lo, offset)
            self._chunks.insert(lo, data)
        else:
            merged_start = min(offset, self._starts[lo])
            merged_end = max(
                end, max(self._starts[i] + len(self._chunks[i]) for i in range(lo, hi))
            )
            merged = bytearray(merged_end - merged_start)
            for i in range(lo, hi):
                at = self._starts[i] - merged_start
                merged[at : at + len(self._chunks[i])] = self._chunks[i]
            merged[offset - merged_start : offset - merged_start + len(data)] = data
            del self._starts[lo:hi]
            del self._chunks[lo:hi]
            self._starts.insert(lo, merged_start)
            self._chunks.insert(lo, bytes(merged))
        self.buffered = sum(len(chunk) for chunk in self._chunks)

    def covers(self, start: int, end: int) -> bool:
        index = bisect.bisect_right(self._starts, start) - 1
        return index >= 0 and self._starts[index] + len(self._chunks[index]) >= end

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self._pos = offset
        elif whence == io.SEEK_CUR:
            self._pos += offset
        else:
            self._pos = self._size + offset
        return self._pos

    def tell(self) -> int:
        return self._pos

    def readinto(self, buffer) -> int:
        if self._pos >= self._size:
            return 0
        want = min(len(buffer), self._size - self._pos)
        index = bisect.bisect_right(self._starts, self._pos) - 1
        if index >= 0:
            start, chunk = self._starts[index], self._chunks[index]
            inside = self._pos - start
            if inside < len(chunk):
                data = chunk[inside : inside + want]
                buffer[: len(data)] = data
                self._pos += len(data)
                return len(data)
        # A miss: read up to the next buffered range so bytes are never fetched twice.
        next_index = bisect.bisect_right(self._starts, self._pos)
        if next_index < len(self._starts):
            want = min(want, self._starts[next_index] - self._pos)
        if self._handle is None:
            self._handle = self._open_handle()
        data = self._handle.read_range(self._pos, want)
        self.fallback_bytes += len(data)
        buffer[: len(data)] = data
        self._pos += len(data)
        return len(data)


class _DecoderCache:
    """LRU of open decoders, bounded by count and by buffered bytes."""

    def __init__(self, capacity: int, byte_budget: int | None):
        self.capacity = capacity
        self.byte_budget = byte_budget
        self._items: OrderedDict[int, tuple[Any, int]] = OrderedDict()
        self._total_bytes = 0

    def __contains__(self, key: int) -> bool:
        return key in self._items

    def get(self, key: int):
        self._items.move_to_end(key)
        return self._items[key][0]

    def put(self, key: int, value, nbytes: int = 0) -> None:
        if key in self._items:
            self._total_bytes -= self._items[key][1]
        self._items[key] = (value, nbytes)
        self._items.move_to_end(key)
        self._total_bytes += nbytes
        while len(self._items) > 1 and (
            len(self._items) > self.capacity
            or (self.byte_budget is not None and self._total_bytes > self.byte_budget)
        ):
            _, (_, evicted) = self._items.popitem(last=False)
            self._total_bytes -= evicted


# ---------------------------------------------------------------------------
# Decoders
# ---------------------------------------------------------------------------


def _open_decoder(source: io.RawIOBase):
    try:
        from torchcodec.decoders import VideoDecoder
    except ImportError:
        return _PyAVDecoder(source)
    return VideoDecoder(source, seek_mode="approximate")


class _PyAVDecoder:
    """Frame-number access through PyAV, for environments without torchcodec."""

    def __init__(self, source: io.RawIOBase) -> None:
        import av

        self._container = av.open(source)
        self._stream = self._container.streams.video[0]
        self._fps = float(self._stream.average_rate)

    def get_frames_at(self, indices: Sequence[int]) -> np.ndarray:
        frames = []
        for index in indices:
            ts = index / self._fps
            self._container.seek(
                int(ts / self._stream.time_base),
                stream=self._stream,
                backward=True,
                any_frame=False,
            )
            for frame in self._container.decode(self._stream):
                if frame.pts * self._stream.time_base >= ts - 0.5 / self._fps:
                    frames.append(frame.to_ndarray(format="rgb24").transpose(2, 0, 1))
                    break
        return np.stack(frames)


def clip_windows(
    table: Any,
    length: int,
    stride: int | None = None,
    columns: Sequence[str] = (),
) -> pa.Table:
    """One row per ``length``-frame window of every video in ``table``.

    Columns ``video_row`` (the row id in ``table``) and ``start`` (first frame),
    plus any ``columns`` copied from the video's row, say a caption. Windows
    start every ``stride`` frames (default ``length``, no overlap). The result
    is a frames table for :class:`VideoTransform` with
    ``fields={"video": ("video_row", "start")}`` and ``window=range(length)``.
    """
    stride = stride or length
    rows = (
        table.search()
        .select(["n_frames", *columns])
        .with_row_id(True)
        .limit(None)
        .to_arrow()
        .to_pylist()
    )
    out: dict[str, list] = {"video_row": [], "start": [], **{c: [] for c in columns}}
    for row in rows:
        for start in range(0, row["n_frames"] - length + 1, stride):
            out["video_row"].append(row["_rowid"])
            out["start"].append(start)
            for c in columns:
                out[c].append(row[c])
    return pa.table(out)


# ---------------------------------------------------------------------------
# Public reader
# ---------------------------------------------------------------------------


class VideoFrames:
    """Keyframe-aware frame reads from a table's video blob column.

    One instance per process (or DataLoader worker). Requests name a row by
    row id, or by a value of ``key`` (a column that identifies the file, say
    ``"name"``), resolved with one filtered read of that column each. It holds
    no copy of the table: a table handle, the keyframe index of recently used
    files (bounded to 2,048 files), and open decoders in an LRU bounded by
    ``decoder_cache_size``
    and ``byte_budget`` of fetched bytes, so a worker's memory is at most
    that budget plus the decoders' own state. Repeated reads from the same file
    reuse its header and already-fetched spans. ``probe_bytes`` is how much of
    the file head is fetched on open; the decoder's own probe reads beyond it
    fall back to ranged reads, which is correct but one round trip each.
    """

    def __init__(
        self,
        table,
        column: str = "video",
        *,
        decoder_cache_size: int = 256,
        byte_budget: int | None = 2 << 30,
        decode_threads: int = 8,
        probe_bytes: int = _PROBE_BYTES,
        key: str | None = None,
    ) -> None:
        self._table = table
        self._column = column
        self._key = key
        self._key_rows: dict[Any, int] = {}
        self._probe = probe_bytes
        self._cache = _DecoderCache(decoder_cache_size, byte_budget)
        self._lock = (
            threading.Lock()
        )  # one fetch at a time; files decode in parallel inside
        self._index: OrderedDict[int, dict[str, Any]] = (
            OrderedDict()
        )  # per-file keyframe index, bounded
        self._pool = ThreadPoolExecutor(max_workers=decode_threads)

    def fetch_frames(self, requests: Sequence[tuple[int, Sequence[int]]]) -> list[Any]:
        """Decode frames by row id and frame number.

        ``requests`` is a list of ``(row, frame_numbers)`` where ``row`` is a
        row id, or a value of the ``key`` column when one was given. Returns one
        ``(T, 3, H, W)`` uint8 array per request, in order: a torch tensor with
        torchcodec, a numpy array with the PyAV fallback. Safe to call from
        several threads.
        """
        with self._lock:
            if self._key is not None:
                requests = [(self._row_id(k), frames) for k, frames in requests]
            return self._fetch_frames(requests)

    def _row_id(self, key_value: Any) -> int:
        """Row id for a key value: one filtered read of the key column, cached."""
        if key_value not in self._key_rows:
            literal = repr(key_value) if isinstance(key_value, str) else key_value
            hits = (
                self._table.search()
                .where(f"{self._key} = {literal}")
                .select([self._key])
                .with_row_id(True)
                .limit(2)
                .to_arrow()
            )
            if hits.num_rows != 1:
                raise KeyError(f"{hits.num_rows} rows have {self._key} = {key_value!r}")
            self._key_rows[key_value] = hits["_rowid"][0].as_py()
        return self._key_rows[key_value]

    def _fetch_frames(self, requests: Sequence[tuple[int, Sequence[int]]]) -> list[Any]:
        windows: dict[int, list[tuple[int, int]]] = {}
        for row_id, frame_numbers in requests:
            windows.setdefault(row_id, []).append(
                (min(frame_numbers), max(frame_numbers))
            )
        decoders = self._prepare(windows)

        results: list[Any] = [None] * len(requests)
        by_row: dict[int, list[int]] = {}
        for request_idx, (row_id, _) in enumerate(requests):
            by_row.setdefault(row_id, []).append(request_idx)

        def decode(row_id: int) -> None:
            # one thread per file: a decoder is not safe to share across threads
            decoder = decoders[row_id]
            for request_idx in by_row[row_id]:
                batch = decoder.get_frames_at(indices=list(requests[request_idx][1]))
                results[request_idx] = getattr(batch, "data", batch)

        list(self._pool.map(decode, by_row))
        return results

    def fetch(
        self, requests: Sequence[tuple[int, Sequence[float]]], fps: float
    ) -> list[Any]:
        """:meth:`fetch_frames` with timestamps in seconds at a known frame rate."""
        return self.fetch_frames(
            [(row, [round(t * fps) for t in ts]) for row, ts in requests]
        )

    # -- internals --------------------------------------------------------

    def _load_index(self, row_ids: list[int]) -> None:
        missing = [row for row in row_ids if row not in self._index]
        if not missing:
            return
        names = set(self._table.schema.names)
        # lerobot-lancedb tables carry the mp4 moov box instead of header spans.
        header = ("header_offsets", "header_lengths")
        if not set(header) <= names:
            header = ("moov_offset", "moov_size")
        columns = ["file_size", "kf_indices", "kf_positions", *header]
        if not set(columns) <= names:
            raise ValueError(
                f"the table needs columns {VIDEO_INDEX_COLUMNS} (see with_video_index)"
            )
        batch = (
            self._table.take_row_ids(missing).select(columns).with_row_id().to_arrow()
        )
        if batch.num_rows != len(missing):
            raise ValueError(f"{batch.num_rows} rows found for {len(missing)} row ids")
        for row in batch.to_pylist():
            offsets, lengths = (row[name] for name in header)
            if not isinstance(offsets, list):
                offsets, lengths = [offsets], [lengths]
            self._index[row["_rowid"]] = {
                "file_size": row["file_size"],
                "header": list(zip(offsets, lengths)),
                "kf_indices": np.asarray(row["kf_indices"], dtype=np.int64),
                "kf_positions": np.asarray(row["kf_positions"], dtype=np.int64),
            }
        while len(self._index) > _INDEX_CACHE_FILES:
            self._index.popitem(last=False)

    def _span(self, meta: dict[str, Any], first: int, last: int) -> tuple[int, int]:
        """Bytes covering frames [first, last]: preceding keyframe to the next one."""
        kf_indices, kf_positions = meta["kf_indices"], meta["kf_positions"]
        start = max(int(np.searchsorted(kf_indices, first, side="right")) - 1, 0)
        end_idx = int(np.searchsorted(kf_indices, last, side="right"))
        end = (
            int(kf_positions[end_idx])
            if end_idx < len(kf_positions)
            else meta["file_size"]
        )
        return int(kf_positions[start]), min(end + _SLACK, meta["file_size"])

    def _prepare(self, windows: dict[int, list[tuple[int, int]]]) -> dict[int, Any]:
        """Fetch every byte span this call needs at once, open missing decoders.

        Returns the call's decoders by row id. The caller holds them for the
        length of the call: with more files in one call than the cache keeps,
        the LRU would otherwise drop one before it is used.
        """
        self._load_index(list(windows))
        sources: dict[int, _SparseFile] = {}
        decoders: dict[int, Any] = {}  # pinned now: a put below may evict them
        spans: dict[int, list[tuple[int, int]]] = {}
        for row_id, row_windows in windows.items():
            meta = self._index[row_id]
            if row_id in self._cache:
                decoders[row_id], source = self._cache.get(row_id)
            else:
                source = _SparseFile(
                    meta["file_size"], lambda row=row_id: self._open_handle(row)
                )
                size = meta["file_size"]
                first_keyframe = (
                    int(meta["kf_positions"][0]) if len(meta["kf_positions"]) else 0
                )
                spans.setdefault(row_id, []).extend(
                    [
                        (0, min(self._probe, size)),
                        (first_keyframe, min(first_keyframe + self._probe, size)),
                        *(
                            (start, min(start + length + _SLACK, size))
                            for start, length in meta["header"]
                        ),
                    ]
                )
            sources[row_id] = source
            for first, last in row_windows:
                span = self._span(meta, first, last)
                if not source.covers(*span):
                    spans.setdefault(row_id, []).append(span)

        requests: list[tuple[int, int, int]] = []
        targets: list[tuple[int, int]] = []
        for row_id, row_spans in spans.items():
            for start, end in _merge_spans(row_spans):
                requests.append((row_id, start, end - start))
                targets.append((row_id, start))
        if requests:
            payloads = self._table.fetch_blob_ranges(self._column, requests)
            for (row_id, offset), payload in zip(targets, payloads):
                sources[row_id].add(offset, payload.as_py())

        # open the new files in parallel: a probe each, independent of one another
        to_open = [row_id for row_id in sources if row_id not in decoders]
        opened = self._pool.map(lambda row: _open_decoder(sources[row]), to_open)
        for row_id, decoder in zip(to_open, opened):
            decoders[row_id] = decoder
            self._cache.put(
                row_id, (decoder, sources[row_id]), nbytes=sources[row_id].buffered
            )
        return decoders

    def _open_handle(self, row_id: int):
        return self._table.fetch_blob_files(self._column, [row_id])[0]


# ---------------------------------------------------------------------------
# Batch transform for lancedb.StreamingDataset / torch DataLoader
# ---------------------------------------------------------------------------


class VideoTransform:
    """Turn rows of a per-frame table into training items with decoded frames.

    Pass it to ``StreamingDataset(..., transform=)`` or call it on the batch a
    map-style ``__getitems__`` fetched::

        transform = VideoTransform(uri, "videos", fields={
            "laptop": ("video_row_laptop", "frame_laptop"),
            "phone": ("video_row_phone", "frame_phone"),
        })
        ds = StreamingDataset(frames_table, transform=transform)

    ``fields`` is ``{output name: (which video, which frame)}``: the two
    columns of the frames table that say which row of the videos table the
    image comes from (a row id, or a ``key`` value) and its frame number. Every
    other column of the batch passes through under its own name, as a tensor
    when it is numeric. ``window`` lists frame offsets, so ``[-1, 0, 1]`` gives
    a 3-frame clip per field. All images of the batch are fetched in one call.
    One ``VideoFrames`` is opened per process on first use, so the transform
    pickles into DataLoader workers.
    """

    def __init__(
        self,
        uri: str,
        table: str,
        *,
        fields: dict[str, tuple[str, str]],
        column: str = "video",
        window: Sequence[int] = (0,),
        key: str | None = None,
        decode_threads: int = 16,
        **reader_options: Any,
    ) -> None:
        self.uri, self.table, self.column, self.key = uri, table, column, key
        self.fields, self.window = dict(fields), list(window)
        self.reader_options = {"decode_threads": decode_threads, **reader_options}
        self._video: VideoFrames | None = None

    def __call__(self, batch: pa.RecordBatch | pa.Table) -> list[dict[str, Any]]:
        if self._video is None:
            import lancedb

            videos = lancedb.connect(self.uri).open_table(self.table)
            self._video = VideoFrames(
                videos, self.column, key=self.key, **self.reader_options
            )
        # One dict per row, like a per-item __getitem__ but for the whole batch:
        # columns that locate a frame are consumed, every other column passes
        # through by name, and each field's image lands under the field's key.
        rows = batch.to_pylist()
        used = {c for pair in self.fields.values() for c in pair}
        items = [
            {c: _to_tensor(v) for c, v in r.items() if c not in used} for r in rows
        ]
        names = list(self.fields)
        # All images of the batch in one fetch_frames call: one blob range
        # request for every file, decoded in parallel. That is the speed-up over
        # fetching per item.
        requests = [
            (r[file_col], [r[frame_col] + o for o in self.window])
            for name in names
            for file_col, frame_col in [self.fields[name]]
            for r in rows
        ]
        clips = self._video.fetch_frames(requests)
        for k, name in enumerate(names):
            for item, clip in zip(items, clips[k * len(rows) : (k + 1) * len(rows)]):
                item[name] = clip if len(self.window) > 1 else clip[0]
        return items


def _to_tensor(value: Any) -> Any:
    try:
        import torch
    except ImportError:
        return value
    return torch.as_tensor(value) if isinstance(value, (list, int, float)) else value
