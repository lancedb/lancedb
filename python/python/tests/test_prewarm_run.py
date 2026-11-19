# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json

import pytest
from lancedb.exceptions import NotResidentError, PrewarmRunError
from lancedb.prewarm_run import RowRange
from test_remote_db import mock_lancedb_connection, mock_lancedb_connection_async

RUN = "/v1/table/test/prewarm_run"

DESCRIBE = {
    "version": 42,
    "schema": {"fields": [{"name": "id", "type": {"type": "int64"}, "nullable": True}]},
}

VIEW = {
    "status": "open",
    "reading_wave": 0,
    "highest_announced": None,
    "live_ranks": 1,
    "horizon": 2,
    "wave_positions": 64,
    "heartbeat_interval_ms": 3_600_000,
}


def make_handler(replies):
    """Serve `replies[call]` as (status, body) and record each call's body."""
    seen = []

    def handler(request):
        path = request.path.rstrip("/")
        if path == "/v1/table/test/describe":
            status, body = 200, DESCRIBE
        else:
            length = int(request.headers.get("Content-Length", 0))
            payload = json.loads(request.rfile.read(length) or b"null")
            call = "open" if path == RUN else path.removeprefix(f"{RUN}/run-1/")
            seen.append((call, payload))
            status, body = replies[call]
        request.send_response(status)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(json.dumps(body).encode())

    return handler, seen


REPLIES = {
    "open": (200, {"run_id": "run-1", "version": 42, "wave_positions": 64}),
    "join": (200, VIEW),
    "announce": (200, {"highest_announced": 1}),
    "wave_status": (200, {"wave": 0, "status": "ready"}),
    "rewarm": (200, {}),
    "close": (200, VIEW),
}

EXPECTED_CALLS = [
    (
        "open",
        {
            "run_key": "job-7-attempt-0-epoch-0",
            "version": 42,
            "columns": ["id"],
            "world_size": 2,
        },
    ),
    ("join", {"rank": 0, "current_wave": 0}),
    (
        "announce",
        {
            "rank": 0,
            "waves": [
                {"wave": 0, "ranges": [{"fragment_id": 3, "start": 0, "end": 8}]},
                {"wave": 1, "ranges": []},
            ],
        },
    ),
    (
        "rewarm",
        {"rank": 0, "wave": 0, "ranges": [{"fragment_id": 3, "start": 2, "end": 4}]},
    ),
    ("close", {"rank": 0}),
]


def test_prewarm_run_sync():
    handler, seen = make_handler(REPLIES)
    with mock_lancedb_connection(handler) as db:
        table = db.open_table("test")
        run = table.open_prewarm_run(
            "job-7-attempt-0-epoch-0", version=42, world_size=2, columns=["id"]
        )
        assert (run.run_id, run.version) == ("run-1", 42)
        assert run.wave_positions == 64
        assert table.attach_prewarm_run("run-1", 42).wave_positions is None
        rank = run.join(0)
        assert rank.rank == 0
        assert rank.view().wave_positions == 64
        assert rank.view().announce_limit == 2
        assert rank.announce({1: [], 0: [RowRange(3, 0, 8)]}) == 1
        rank.wait_ready(0, timeout=60)
        rank.rewarm(0, [(3, 2, 4)])
        rank.close()
    calls = [call for call in seen if call[0] != "wave_status"]
    assert calls == EXPECTED_CALLS


@pytest.mark.asyncio
async def test_prewarm_run_async():
    handler, seen = make_handler(REPLIES)
    async with mock_lancedb_connection_async(handler) as db:
        table = await db.open_table("test")
        run = await table.open_prewarm_run(
            "job-7-attempt-0-epoch-0", version=42, world_size=2, columns=["id"]
        )
        rank = await run.join(0)
        assert await rank.announce({0: [(3, 0, 8)], 1: []}) == 1
        await rank.wait_ready(0, timeout=60)
        await rank.rewarm(0, [RowRange(3, 2, 4)])
        await rank.close()
    calls = [call for call in seen if call[0] != "wave_status"]
    assert calls == EXPECTED_CALLS


def test_failed_run_raises_prewarm_run_error():
    failed = {
        "code": 0,
        "error": "wave 0 needed more than 3 re-warms",
        "error_code": "run_failed",
    }
    handler, _ = make_handler({**REPLIES, "wave_status": (410, failed)})
    with mock_lancedb_connection(handler) as db:
        rank = (
            db.open_table("test")
            .open_prewarm_run("job-7", version=42, world_size=1)
            .join(0)
        )
        with pytest.raises(PrewarmRunError, match="more than 3 re-warms") as raised:
            rank.wait_ready(0, timeout=60)
    assert raised.value.run_id == "run-1"
    assert raised.value.code == "run_failed"


def test_server_without_run_support():
    handler, _ = make_handler({"open": (404, {"error": "no route"})})
    with mock_lancedb_connection(handler) as db:
        with pytest.raises(Exception, match="does not support prewarm runs"):
            db.open_table("test").open_prewarm_run("job-7", version=42, world_size=1)


def cache_only_handler(headers):
    """Answer cache-only queries 412 not_resident, recording the header."""

    def handler(request):
        if request.path.rstrip("/") == "/v1/table/test/describe":
            status, body = 200, DESCRIBE
        else:
            assert request.path.rstrip("/") == "/v1/table/test/query"
            header = request.headers.get("x-lancedb-cache-only")
            headers.append(header)
            status, body = (
                412,
                {
                    "code": 0,
                    "error": "fragment 3 is not in the cache",
                    "error_code": "not_resident",
                },
            )
        request.send_response(status)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(json.dumps(body).encode())

    return handler


def test_cache_only_query_raises_not_resident():
    headers = []
    with mock_lancedb_connection(cache_only_handler(headers)) as db:
        table = db.open_table("test")
        table.checkout(42)
        cache_only = table.cache_only()
        with pytest.raises(NotResidentError, match="not in the cache") as raised:
            cache_only.search().limit(1).to_arrow()
    assert raised.value.request_id
    assert headers == ["true"]


@pytest.mark.asyncio
async def test_cache_only_query_raises_not_resident_async():
    headers = []
    async with mock_lancedb_connection_async(cache_only_handler(headers)) as db:
        table = await db.open_table("test")
        await table.checkout(42)
        cache_only = await table.cache_only()
        with pytest.raises(NotResidentError):
            await cache_only.query().limit(1).to_arrow()
    assert headers == ["true"]


def test_cache_only_survives_pickling():
    with mock_lancedb_connection(cache_only_handler([])) as db:
        table = db.open_table("test")
        table.checkout(42)
        state = table.cache_only().__getstate__()
    assert state["cache_only"] is True
    assert state["checkout_version"] == 42


def test_cache_only_needs_a_checked_out_version():
    with mock_lancedb_connection(cache_only_handler([])) as db:
        with pytest.raises(ValueError, match="fixed version"):
            db.open_table("test").cache_only()


def test_attached_reader_waits_without_joining():
    handler, seen = make_handler(REPLIES)
    with mock_lancedb_connection(handler) as db:
        reader = db.open_table("test").attach_prewarm_run("run-1", 42).reader(1)
        assert reader.rank == 1
        reader.wait_ready(0, timeout=60)
        reader.rewarm(0, [RowRange(3, 2, 4)])
    calls = [call for call, _ in seen]
    assert "open" not in calls and "join" not in calls
    assert (
        "rewarm",
        {"rank": 1, "wave": 0, "ranges": [{"fragment_id": 3, "start": 2, "end": 4}]},
    ) in seen


@pytest.mark.asyncio
async def test_attached_reader_async():
    handler, seen = make_handler(REPLIES)
    async with mock_lancedb_connection_async(handler) as db:
        table = await db.open_table("test")
        run = table.attach_prewarm_run("run-1", 42)
        assert (run.run_id, run.version) == ("run-1", 42)
        await run.reader(2).wait_ready(0, timeout=60)
    assert [call for call, _ in seen] == ["wave_status"]
