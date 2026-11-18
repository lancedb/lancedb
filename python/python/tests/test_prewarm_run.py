# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json

import pytest
from lancedb.exceptions import PrewarmRunError
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
    "open": (200, {"run_id": "run-1", "version": 42}),
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
    ("join", {"rank": 0}),
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
