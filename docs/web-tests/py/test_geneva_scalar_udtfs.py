# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pytest
from unittest.mock import MagicMock


def test_scalar_udtf_iterator():
    def extract_video_segment(path, start, end):
        return bytes(f"clip:{start}-{end}", "utf-8")

    # --8<-- [start:scalar_udtf_iterator]
    from geneva import chunker
    from typing import Iterator, NamedTuple

    class Clip(NamedTuple):
        clip_start: float
        clip_end: float
        clip_bytes: bytes

    @chunker
    def extract_clips(video_path: str, duration: float) -> Iterator[Clip]:
        """Yields multiple clips per video."""
        clip_length = 10.0
        for start in range(0, int(duration), int(clip_length)):
            end = min(start + clip_length, duration)
            clip_data = extract_video_segment(video_path, start, end)
            yield Clip(clip_start=start, clip_end=end, clip_bytes=clip_data)
    # --8<-- [end:scalar_udtf_iterator]

    from geneva.transformer import Chunker
    assert isinstance(extract_clips, Chunker)
    assert extract_clips.input_columns == ["video_path", "duration"]
    assert set(extract_clips.output_schema.names) == {"clip_start", "clip_end", "clip_bytes"}

    clips = list(extract_clips.func("/v/a.mp4", 30.0))
    assert len(clips) == 3
    assert clips[0].clip_start == 0.0
    assert clips[2].clip_end == 30.0


def test_scalar_udtf_list_return():
    from geneva import chunker
    from typing import NamedTuple

    class Clip(NamedTuple):
        clip_start: float
        clip_end: float
        clip_bytes: bytes

    # --8<-- [start:scalar_udtf_list]
    @chunker
    def extract_clips(video_path: str, duration: float) -> list[Clip]:
        clips = []
        for start in range(0, int(duration), 10):
            end = min(start + 10, duration)
            clips.append(Clip(clip_start=start, clip_end=end, clip_bytes=b"..."))
        return clips
    # --8<-- [end:scalar_udtf_list]

    from geneva.transformer import Chunker
    assert isinstance(extract_clips, Chunker)
    clips = extract_clips.func("/v/a.mp4", 30.0)
    assert len(clips) == 3
    assert all(c.clip_bytes == b"..." for c in clips)


def test_scalar_udtf_batch():
    import pyarrow as pa
    from geneva import chunker

    clip_schema = pa.schema([
        ("clip_start", pa.float64()),
        ("clip_end", pa.float64()),
    ])

    # --8<-- [start:scalar_udtf_batch]
    @chunker(batch=True, output_schema=clip_schema)
    def extract_clips(batch: pa.RecordBatch) -> pa.RecordBatch:
        """Process rows in batches. Same 1:N semantic per row."""
        ...
    # --8<-- [end:scalar_udtf_batch]

    from geneva.transformer import Chunker
    assert isinstance(extract_clips, Chunker)
    assert extract_clips.batch is True


def test_create_scalar_udtf_view(monkeypatch):
    from typing import Iterator, NamedTuple
    from geneva.transformer import chunker

    class Clip(NamedTuple):
        clip_start: float
        clip_end: float
        clip_bytes: bytes

    @chunker
    def extract_clips(video_path: str, duration: float) -> Iterator[Clip]:
        for start in range(0, int(duration), 10):
            yield Clip(clip_start=start, clip_end=min(start + 10.0, duration), clip_bytes=b"")

    import geneva
    from unittest.mock import create_autospec
    mock_clips = MagicMock()
    mock_db = create_autospec(geneva.db.Connection, instance=True)
    mock_db.create_udtf_view.return_value = mock_clips
    monkeypatch.setattr("geneva.connect", MagicMock(return_value=mock_db))

    # --8<-- [start:create_scalar_udtf_view]
    import geneva

    db = geneva.connect("/data/mydb")
    videos = db.open_table("videos")

    # Create the 1:N materialized view
    clips = db.create_udtf_view(
        "clips",
        source=videos.search(None).select(["video_path", "metadata"]),
        udtf=extract_clips,
    )

    # Populate — runs the UDTF on every source row
    clips.refresh()
    # --8<-- [end:create_scalar_udtf_view]

    call_kwargs = mock_db.create_udtf_view.call_args
    assert call_kwargs.args[0] == "clips"
    assert call_kwargs.kwargs["udtf"] is extract_clips
    mock_clips.refresh.assert_called_once()


def test_add_columns_scalar_udtf():
    import pyarrow as pa
    from geneva.transformer import udf

    clips = MagicMock()
    embed_model = MagicMock()
    embed_model.encode.return_value = [0.1] * 512

    # --8<-- [start:add_columns_scalar_udtf]
    @udf(data_type=pa.list_(pa.float32(), 512))
    def clip_embedding(clip_bytes: bytes) -> list[float]:
        return embed_model.encode(clip_bytes)

    # Add an embedding column to the clips table
    clips.add_columns({"embedding": clip_embedding})

    # Backfill computes embeddings for all existing clips
    clips.backfill("embedding")
    # --8<-- [end:add_columns_scalar_udtf]

    clips.add_columns.assert_called_once_with({"embedding": clip_embedding})
    clips.backfill.assert_called_once_with("embedding")


def test_incremental_refresh():
    videos = MagicMock()
    clips = MagicMock()
    new_video_data = [{"video_path": "/v/c.mp4", "duration": 45.0}]

    # --8<-- [start:incremental_refresh]
    # Add new videos to the source table
    videos.add(new_video_data)

    # Incremental refresh — only processes the new videos
    clips.refresh()
    # --8<-- [end:incremental_refresh]

    videos.add.assert_called_once_with(new_video_data)
    clips.refresh.assert_called_once()


def test_chaining_udtf_views(monkeypatch):
    from typing import Iterator, NamedTuple
    from geneva.transformer import chunker

    class Clip(NamedTuple):
        clip_start: float
        clip_end: float

    class Frame(NamedTuple):
        frame_index: int
        frame_bytes: bytes

    @chunker
    def extract_clips(video_path: str, duration: float) -> Iterator[Clip]:
        for start in range(0, int(duration), 10):
            yield Clip(clip_start=start, clip_end=min(start + 10.0, duration))

    @chunker
    def extract_frames(clip_start: float, clip_end: float) -> Iterator[Frame]:
        yield Frame(frame_index=0, frame_bytes=b"")

    import geneva
    from unittest.mock import create_autospec
    mock_db = create_autospec(geneva.db.Connection, instance=True)
    monkeypatch.setattr("geneva.connect", MagicMock(return_value=mock_db))

    db = geneva.connect("/data/mydb")
    videos = db.open_table("videos")

    # --8<-- [start:chaining_udtf_views]
    # videos → clips (1:N)
    clips = db.create_udtf_view(
        "clips", source=videos.search(None), udtf=extract_clips
    )

    # clips → frames (1:N)
    frames = db.create_udtf_view(
        "frames", source=clips.search(None), udtf=extract_frames
    )
    # --8<-- [end:chaining_udtf_views]

    assert mock_db.create_udtf_view.call_count == 2
    first_call = mock_db.create_udtf_view.call_args_list[0]
    second_call = mock_db.create_udtf_view.call_args_list[1]
    assert first_call.kwargs["udtf"] is extract_clips
    assert second_call.kwargs["udtf"] is extract_frames


def test_document_chunking_udtf():
    # --8<-- [start:document_chunking_udtf]
    from geneva import chunker
    from typing import Iterator, NamedTuple

    class Chunk(NamedTuple):
        chunk_index: int
        chunk_text: str

    @chunker
    def chunk_document(text: str) -> Iterator[Chunk]:
        """Split a document into overlapping chunks."""
        words = text.split()
        chunk_size = 500
        overlap = 50
        for i, start in enumerate(range(0, len(words), chunk_size - overlap)):
            chunk_words = words[start:start + chunk_size]
            yield Chunk(chunk_index=i, chunk_text=" ".join(chunk_words))
    # --8<-- [end:document_chunking_udtf]

    sample_text = " ".join(["word"] * 600)
    chunks = list(chunk_document.func(sample_text))
    assert len(chunks) == 2
    assert chunks[0].chunk_index == 0
    assert chunks[1].chunk_index == 1
    assert len(chunks[0].chunk_text.split()) == 500


def test_document_chunking_full(monkeypatch):
    import pyarrow as pa
    from unittest.mock import MagicMock

    import geneva
    from unittest.mock import create_autospec
    mock_chunks_table = MagicMock()
    mock_db = create_autospec(geneva.db.Connection, instance=True)
    mock_db.create_udtf_view.return_value = mock_chunks_table
    monkeypatch.setattr("geneva.connect", MagicMock(return_value=mock_db))

    embedding_model = MagicMock()
    embedding_model.encode.return_value = [0.1] * 1536

    # --8<-- [start:document_chunking_full]
    from geneva import connect, chunker, udf
    from typing import Iterator, NamedTuple
    import pyarrow as pa

    class Chunk(NamedTuple):
        chunk_index: int
        chunk_text: str

    @chunker
    def chunk_document(text: str) -> Iterator[Chunk]:
        """Split a document into overlapping chunks."""
        words = text.split()
        chunk_size = 500
        overlap = 50
        for i, start in enumerate(range(0, len(words), chunk_size - overlap)):
            chunk_words = words[start:start + chunk_size]
            yield Chunk(chunk_index=i, chunk_text=" ".join(chunk_words))

    db = connect("/data/mydb")
    docs = db.open_table("documents")

    # Create chunked view — inherits doc_id, title, etc. from source
    chunks = db.create_udtf_view(
        "doc_chunks",
        source=docs.search(None).select(["doc_id", "title", "text"]),
        udtf=chunk_document,
    )
    chunks.refresh()

    # Add embeddings to chunks for semantic search
    @udf(data_type=pa.list_(pa.float32(), 1536))
    def embed_text(chunk_text: str) -> list[float]:
        return embedding_model.encode(chunk_text)

    chunks.add_columns({"embedding": embed_text})
    chunks.backfill("embedding")  # Backfills embeddings on all existing chunks

    # Query — parent columns available alongside chunk columns
    chunks.search(None).select(["doc_id", "title", "chunk_text", "embedding"]).to_pandas()
    # --8<-- [end:document_chunking_full]

    call_kwargs = mock_db.create_udtf_view.call_args
    assert call_kwargs.args[0] == "doc_chunks"
    assert call_kwargs.kwargs["udtf"] is chunk_document
    mock_chunks_table.refresh.assert_called_once()
    mock_chunks_table.add_columns.assert_called_once()
    mock_chunks_table.backfill.assert_called_once_with("embedding")
