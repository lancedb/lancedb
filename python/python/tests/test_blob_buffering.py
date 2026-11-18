# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
import threading
from contextlib import contextmanager
from urllib.parse import urlparse

import pyarrow as pa
import pytest

import lancedb
from test_remote_db import (
    BLOB_DESCRIBE_RESPONSE,
    blob_descriptor_take_table,
    mock_lancedb_connection,
)


@contextmanager
def remote_buffer_table(*, http_requests=None, truncate_first=False):
    payload = bytes(range(256)) * 4096
    requests = []

    def handler(request):
        path = urlparse(request.path).path
        if http_requests is not None:
            http_requests.append(path)
        if path.endswith("/describe/"):
            body = json.dumps(BLOB_DESCRIBE_RESPONSE).encode()
            content_type = "application/json"
        elif path.endswith("/query/"):
            data = json.loads(
                request.rfile.read(int(request.headers["Content-Length"]))
            )
            assert data["columns"] == ["image"]
            table = blob_descriptor_take_table().slice(0, 1)
            images = table["image"].combine_chunks()
            images = pa.StructArray.from_arrays(
                [
                    images.field("kind"),
                    images.field("position"),
                    pa.array([len(payload)], pa.uint64()),
                    images.field("blob_id"),
                    images.field("blob_uri"),
                ],
                fields=list(images.type),
            )
            table = table.set_column(0, table.schema.field(0), images)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
            body = sink.getvalue().to_pybytes()
            content_type = "application/vnd.apache.arrow.stream"
        else:
            assert path == "/v1/table/test/blob/image/10/bytes"
            assert "version=1" in request.path
            start, end = map(int, request.headers["Range"][6:].split("-"))
            requests.append((start, end + 1))
            body = payload[start : end + 1]
            if truncate_first and len(requests) == 1:
                body = body[:5]
            request.send_response(206)
            request.send_header("Content-Range", f"bytes {start}-{end}/{len(payload)}")
            request.send_header("Content-Length", str(len(body)))
            request.end_headers()
            request.wfile.write(body)
            return
        request.send_response(200)
        request.send_header("Content-Type", content_type)
        request.send_header("Content-Length", str(len(body)))
        request.send_header("phalanx-version", "0.5.0")
        request.send_header("x-lancedb-version", "1")
        request.end_headers()
        request.wfile.write(body)

    with mock_lancedb_connection(handler, blob_request_concurrency=1) as db:
        yield db.open_table("test"), payload, requests


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("buffer_size", [None, 0, 8, 512 * 1024])
async def test_remote_read_ahead_over_http(use_async, buffer_size):
    with remote_buffer_table() as (table, payload, requests):
        if use_async:
            files = await table._table.fetch_blob_files(
                "image", [10], buffer_size=buffer_size
            )
        else:
            files = table.fetch_blob_files("image", [10], buffer_size=buffer_size)
        file = files[0]
        assert requests == []
        assert file.read(2) == payload[:2]
        window = 512 * 1024 if buffer_size is None else max(buffer_size, 2)
        assert requests == [(0, window)]
        assert file.read(2) == payload[2:4]
        assert len(requests) == (2 if buffer_size == 0 else 1)
        file.seek(1)
        assert file.read(1) == payload[1:2]
        assert len(requests) == (3 if buffer_size == 0 else 1)
        before = len(requests)
        assert file.read_range(0, 1) == payload[:1]
        assert len(requests) == before + 1
        assert requests[-1] == (0, 1)
        assert file.tell() == 2
        file.set_buffer_size(0)
        assert file.read(1) == payload[2:3]
        assert requests[-1] == (2, 3)
        file.set_buffer_size(16)
        assert file.read(1) == payload[3:4]
        assert requests[-1] == (3, 19)
        file.seek(len(payload) - 1)
        assert file.read(4) == payload[-1:]
        assert requests[-1] == (len(payload) - 1, len(payload))
        before = len(requests)
        assert file.read(1) == b""
        assert len(requests) == before
        file.close()
        with pytest.raises(ValueError, match="closed"):
            file.set_buffer_size(0)


def test_short_prefetch_over_http_preserves_cursor():
    with remote_buffer_table(truncate_first=True) as (table, payload, requests):
        file = table.fetch_blob_files("image", [10], buffer_size=8)[0]
        with pytest.raises(RuntimeError, match="returned 5 bytes, expected 8"):
            file.read(6)
        assert file.tell() == 0
        assert file.read(3) == payload[:3]
        assert file.tell() == 3
        assert requests == [(0, 8), (0, 8)]
        file.close()


def test_idle_prefetch_releases_http_concurrency_slot():
    with remote_buffer_table() as (table, payload, requests):
        first, second = table.fetch_blob_files("image", [10, 10])
        assert first.read(1) == payload[:1]
        results = []
        errors = []

        def read_other():
            try:
                results.append(second.read(1))
            except Exception as error:
                errors.append(error)

        reader = threading.Thread(target=read_other, daemon=True)
        reader.start()
        reader.join(timeout=2)
        completed_while_first_idle = not reader.is_alive()
        first.close()
        reader.join(timeout=2)
        second.close()
        assert completed_while_first_idle, (
            "idle prefetch retained the HTTP request slot"
        )
        assert not errors
        assert results == [payload[:1]]
        assert requests == [(0, 512 * 1024), (0, 512 * 1024)]


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_local_buffer_configuration_through_python(tmp_path, use_async):
    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "blobs", [{"image": b"0123456789"}], schema=pa.schema([lancedb.blob("image")])
    )
    row_ids = table.search().with_row_id(True).to_arrow()["_rowid"]
    if use_async:
        files = await table._table.fetch_blob_files("image", row_ids, buffer_size=0)
    else:
        files = table.fetch_blob_files("image", row_ids, buffer_size=0)
    file = files[0]
    assert file.read(2) == b"01"
    file.set_buffer_size(4)
    assert file.read(2) == b"23"
    file.seek(1)
    assert file.read(3) == b"123"
    assert file.read_range(8, 2) == b"89"
    with pytest.raises(ValueError, match="non-negative"):
        file.set_buffer_size(-1)
    with pytest.raises(TypeError):
        file.set_buffer_size(1.5)
    file.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_open_descriptor_files_forward_buffer_size(use_async):
    with remote_buffer_table() as (table, payload, requests):
        descriptor = {
            "kind": 1,
            "position": 0,
            "size": len(payload),
            "blob_id": 0,
            "blob_uri": "",
            "source": {
                "uri": "s3://bucket/test.lance",
                "version": 1,
                "committed_at_micros": 1,
                "column": "image",
                "row_reference": 10,
                "row_is_address": False,
            },
        }
        if use_async:
            files = [
                file
                async for file in table._table.open_blob_files(
                    [None, descriptor], buffer_size=8
                )
            ]
        else:
            files = list(table.open_blob_files([None, descriptor], buffer_size=8))
        assert files[0] is None
        assert files[1].read(1) == payload[:1]
        assert requests == [(0, 8)]
        files[1].close()


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_invalid_buffer_size_rejects_before_http(use_async):
    http_requests = []
    with remote_buffer_table(http_requests=http_requests) as (table, _, requests):
        http_requests.clear()
        with pytest.raises(ValueError, match="non-negative"):
            if use_async:
                await table._table.fetch_blob_files("image", [10], buffer_size=-1)
            else:
                table.fetch_blob_files("image", [10], buffer_size=-1)
        assert requests == []
        assert http_requests == []
