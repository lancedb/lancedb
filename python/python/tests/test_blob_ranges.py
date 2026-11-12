# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Exercise the public sync/async range APIs over bounded loopback HTTP."""

import asyncio
import hashlib
import json
from pathlib import Path
import socket
import threading

import lancedb._lancedb as native
import pyarrow as pa
import pytest

from lancedb.remote.errors import HttpError, TableNotFoundError
from test_remote_db import mock_lancedb_connection

MIB = 1024 * 1024
ROUTE = "/v1/table/test/fetch_blob_ranges/"


def payload(row, offset, length, version):
    pattern = bytes((row * 17 + i + version) % 251 for i in range(251))
    start = offset % 251
    return (pattern * ((start + length + 250) // 251))[start : start + length]


class RangeServer:
    def __init__(self, fault=None):
        self.fault = fault
        self.calls = []
        self.fetches = []
        self.stalled = threading.Event()
        self.release = threading.Event()

    def __call__(self, request):
        request.connection.settimeout(15)
        size = int(request.headers.get("Content-Length", 0))
        assert size <= 256 * 1024
        body = json.loads(request.rfile.read(size) or b"{}")
        self.calls.append((request.command, request.path, body))
        if request.path in (
            "/v1/table/test/describe/",
            "/v1/table/test/branches/list/",
        ):
            request.send_response(200)
            request.send_header("Content-Type", "application/json")
            request.send_header("phalanx-version", "1.13.0-beta.0")
            if request.path.endswith("/describe/"):
                request.send_header("x-lancedb-blob-ranges", "1")
            request.end_headers()
            response = (
                {
                    "branches": {
                        "experiment": {
                            "parentBranch": None,
                            "parentVersion": 1,
                            "createAt": 1,
                            "manifestSize": 1,
                        }
                    }
                }
                if request.path.endswith("branches/list/")
                else {"version": 8, "schema": {"fields": []}}
            )
            request.wfile.write(json.dumps(response).encode())
            return
        assert (request.command, request.path) == ("POST", ROUTE)
        self.fetches.append(body)
        assert body["column"] == "image"
        assert body["branch"] == "experiment"
        ranges = body["ranges"]
        assert len(ranges) <= 1024
        assert sum(r["length"] for r in ranges) <= 64 * MIB
        version = body["version"] or (7 if len(self.fetches) == 1 else 8)
        values = [
            None
            if r["row_id"] == 20
            else payload(r["row_id"], r["offset"], r["length"], version)
            for r in ranges
        ]
        fault = self.fault if len(self.fetches) > 1 else None
        if fault == "stall" and len(self.fetches) == 2:
            request.send_response(200)
            request.send_header("Content-Type", "application/vnd.apache.arrow.stream")
            request.send_header("x-lancedb-version", str(version))
            request.end_headers()
            request.wfile.flush()
            self.stalled.set()
            assert self.release.wait(15), "test did not release stalled response"
            return
        if fault == "structured400":
            request.send_response(400)
            request.send_header("x-lancedb-error-code", "invalid_input")
            request.end_headers()
            request.wfile.write(b'{"error":"range exceeds blob size"}')
            return
        if isinstance(fault, int):
            request.send_response(fault)
            request.end_headers()
            request.wfile.write(b"ordinary server failure")
            return
        if fault == "disconnect":
            request.connection.shutdown(socket.SHUT_RDWR)
            return
        column = "wrong" if fault == "column" else "value"
        data_type = pa.binary() if fault == "type" else pa.large_binary()
        if fault == "count":
            values = []
        elif fault == "length":
            values = [b""]
        batch = pa.table({column: pa.array(values, type=data_type)})
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_table(batch, max_chunksize=257)
        ipc = sink.getvalue().to_pybytes()
        if fault == "eos":
            ipc = ipc[:-8]
        elif fault == "body":
            ipc = ipc[: len(ipc) // 2]
        request.send_response(200)
        request.send_header("Content-Type", "application/vnd.apache.arrow.stream")
        if fault != "version":
            request.send_header("x-lancedb-version", str(version))
        request.send_header(
            "Content-Length", str(len(ipc) + (10 if fault == "transport" else 0))
        )
        request.end_headers()
        for i in range(0, len(ipc), 8192):
            request.wfile.write(ipc[i : i + 8192])


async def fetch(table, requests, use_async):
    result = table.fetch_blob_ranges("image", requests)
    return await result if use_async else result


def api(db, use_async, version=None):
    table = db.open_table("test", branch="experiment", version=version)
    return table._table if use_async else table


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("version", [None, 3])
async def test_range_sdk_order_snapshot_and_overwrite(use_async, version):
    handler = RangeServer()
    with mock_lancedb_connection(handler) as db:
        table = api(db, use_async, version)
        requests = [(20 if i % 9 == 0 else 10 + i % 7, i, i % 4) for i in range(1025)]
        requests += [(10, 0, 0), (10, 2, 3), (10, 2, 3)]
        result = await fetch(table, requests, use_async)
        assert isinstance(result, pa.LargeBinaryArray)
        assert result.to_pylist() == [
            None if row == 20 else payload(row, offset, length, version or 7)
            for row, offset, length in requests
        ]
        assert [b["version"] for b in handler.fetches] == [version, version or 7]
        result = await fetch(table, [(10, 0, 1)], use_async)
        assert result.to_pylist() == [payload(10, 0, 1, version or 8)]
        assert handler.fetches[-1]["version"] == version
        assert len(handler.fetches) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_range_sdk_cached_reopen_negotiates_capability(use_async):
    handler = RangeServer()
    with mock_lancedb_connection(handler) as db:
        for version in (7, 8):
            table = api(db, use_async)
            result = await fetch(table, [(10, 0, 1)], use_async)
            assert result.to_pylist() == [payload(10, 0, 1, version)]
        assert len(handler.fetches) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_range_sdk_large_single_and_aggregate_budget(use_async):
    handler = RangeServer()
    with mock_lancedb_connection(handler) as db:
        table = api(db, use_async)
        requests = [
            (10, 3, 33 * MIB),
            (30, 7, 33 * MIB),
            (40, 11, 70 * MIB),
            (20, 0, 2**64 - 1),
        ]
        # The loopback server advances latest after the first partition.
        result = await fetch(table, requests, use_async)
        assert len(result) == 4
        for i, (row, offset, length) in enumerate(requests[:3]):
            expected = payload(row, offset, length, 7)
            assert (
                hashlib.sha256(result[i].as_buffer()).digest()
                == hashlib.sha256(expected).digest()
            )
            assert len(result[i].as_buffer()) == length
        assert result[3].as_py() is None
        assert len(handler.fetches) == 3
        assert [b["version"] for b in handler.fetches] == [None, 7, 7]


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize(
    "fault",
    [
        "type",
        "column",
        "count",
        "length",
        "eos",
        "body",
        "transport",
        "version",
        "disconnect",
        401,
        403,
        404,
        429,
        500,
    ],
)
async def test_range_sdk_later_failure_rejects_entire_call(use_async, fault):
    handler = RangeServer(fault=fault)
    with mock_lancedb_connection(
        handler, retry_config={"retries": 2, "connect_retries": 2, "read_retries": 2}
    ) as db:
        table = api(db, use_async)
        with pytest.raises(Exception) as exc:
            await fetch(table, [(10, 0, 1)] * 1025, use_async)
        if fault == 404:
            assert isinstance(exc.value, TableNotFoundError)
            assert isinstance(exc.value.__cause__, HttpError)
            assert exc.value.__cause__.status_code == 404
        else:
            assert not isinstance(exc.value, ValueError)
        if isinstance(fault, int):
            assert exc.value.status_code == fault
        assert len(handler.fetches) == 2
        assert all(
            path in ("/v1/table/test/describe/", "/v1/table/test/branches/list/", ROUTE)
            for _, path, _ in handler.calls
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_range_sdk_structured_400_preserves_value_error(use_async):
    handler = RangeServer(fault="structured400")
    with mock_lancedb_connection(handler) as db:
        table = api(db, use_async)
        with pytest.raises(ValueError, match="range exceeds blob size"):
            await fetch(table, [(10, 0, 1)] * 1025, use_async)
        assert len(handler.fetches) == 2


@pytest.mark.asyncio
async def test_range_sdk_async_cancel_releases_only_request_permit():
    handler = RangeServer(fault="stall")
    with mock_lancedb_connection(handler, blob_request_concurrency=1) as db:
        table = api(db, True)
        pending = asyncio.create_task(fetch(table, [(10, 0, 1)] * 2049, True))
        try:
            assert await asyncio.to_thread(handler.stalled.wait, 10)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            assert len(handler.fetches) == 2
            result = await asyncio.wait_for(fetch(table, [(10, 0, 1)], True), 5)
            assert result.to_pylist() == [payload(10, 0, 1, 8)]
            assert len(handler.fetches) == 3
        finally:
            pending.cancel()
            handler.release.set()


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_range_sdk_integer_validation_and_empty_zero_io(use_async):
    handler = RangeServer()
    with mock_lancedb_connection(handler) as db:
        table = api(db, use_async)
        before = len(handler.calls)
        assert (await fetch(table, [], use_async)).to_pylist() == []
        for request in [
            (-1, 0, 1),
            (2**64, 0, 1),
            (10, -1, 1),
            (10, 0, -1),
            (10, 0, 2**64),
            (10, 0.5, 1),
            (20, 2**64 - 1, 1),
            (10, 0),
        ]:
            with pytest.raises((ValueError, OverflowError, TypeError)):
                await fetch(table, [request], use_async)
        assert len(handler.calls) == before


def test_range_sdk_loaded_artifact_identity():
    artifact = Path(native.__file__).resolve()
    digest = hashlib.sha256()
    with artifact.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    print(f"loaded native artifact={artifact} sha256={digest.hexdigest()}")
    assert artifact.is_file()
