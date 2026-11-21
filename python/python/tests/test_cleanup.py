# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json

import pyarrow as pa
import pytest

import lancedb
from lancedb.cleanup import CleanupResult
from test_remote_db import _create_table_handler, _mock_remote_sql_connection


@pytest.mark.parametrize("counts", [(0, 0), (2, 123), (None, None)])
def test_cleanup_result_preserves_measurements(counts):
    versions, size = counts
    result = CleanupResult.from_json(
        json.dumps(
            {
                "format_version": 1,
                "min_age_seconds": 0,
                "statistics_complete": versions is not None,
                "old_versions": versions,
                "bytes_removed": size,
                "future": True,
            }
        )
    )
    assert result.old_versions == versions
    assert result.bytes_removed == size


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"format_version": 2},
        {
            "format_version": 1,
            "min_age_seconds": 0,
            "statistics_complete": False,
            "old_versions": 1,
        },
    ],
)
def test_cleanup_result_rejects_missing_unknown_or_inconsistent_contract(payload):
    with pytest.raises(ValueError):
        CleanupResult.from_json(json.dumps(payload))


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "outcome", ["counts", "no_op", "unknown", "missing", "FAILED", "CANCELLED"]
)
async def test_cleanup_submission_wait_and_reopen(asynchronous, outcome):
    flight = pytest.importorskip("pyarrow.flight")
    polls = []
    result = {
        "format_version": 1,
        "min_age_seconds": 0,
        "statistics_complete": outcome != "unknown",
        "old_versions": None
        if outcome == "unknown"
        else 0
        if outcome == "no_op"
        else 2,
        "bytes_removed": None
        if outcome == "unknown"
        else 0
        if outcome == "no_op"
        else 123,
    }

    class Server(flight.FlightServerBase):
        def __init__(self):
            super().__init__("grpc://127.0.0.1:0")
            self.tickets = []

        def do_get(self, context, ticket):
            self.tickets.append(ticket.ticket)
            return flight.RecordBatchStream(pa.table({"job_id": ["cleanup-job"]}))

    def handler(request):
        if request.path != "/v1/jobs/describe":
            return _create_table_handler(request)
        polls.append(
            json.loads(request.rfile.read(int(request.headers["Content-Length"])))
        )
        request.send_response(200)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(
            json.dumps(
                {
                    "job_id": "cleanup-job",
                    "job_type": "cleanup",
                    "job_state": outcome
                    if outcome in ("FAILED", "CANCELLED")
                    else "DONE",
                    "result": None if outcome == "missing" else result,
                }
            ).encode()
        )

    server = Server()
    with _mock_remote_sql_connection(server, handler) as kwargs:
        if asynchronous:
            db = await lancedb.connect_async(**kwargs)
            table = await db.create_table("test", [{"id": 1}])
            job = await table.cleanup_async(min_age_seconds=0)
        else:
            db = lancedb.connect(**kwargs)
            table = db.create_table("test", [{"id": 1}])
            job = table.cleanup_async(min_age_seconds=0)
        assert job.id == "cleanup-job" and not polls
        if outcome in ("missing", "FAILED", "CANCELLED"):
            with pytest.raises(
                Exception,
                match={
                    "missing": "did not contain a result",
                    "FAILED": "failed",
                    "CANCELLED": "cancel",
                }[outcome],
            ):
                if asynchronous:
                    await job.wait()
                else:
                    job.wait()
        else:
            stats = await job.wait() if asynchronous else job.wait()
            assert isinstance(stats, CleanupResult)
            assert stats.old_versions == result["old_versions"]
            reopened = (
                await db.open_job(job.id) if asynchronous else db.open_job(job.id)
            )
            assert (await reopened.wait() if asynchronous else reopened.wait()) == stats
    assert len(server.tickets) == 1
    assert b"VACUUM" in server.tickets[0]
    assert b"min_age_seconds = 0" in server.tickets[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_cleanup_preview_is_read_only(asynchronous):
    flight = pytest.importorskip("pyarrow.flight")
    payload = {
        "format_version": 1,
        "source_version": 42,
        "min_age_seconds": 0,
        "cutoff_millis": 123,
        "versions_to_remove": 2,
        "bytes_to_remove": 100,
    }

    class Server(flight.FlightServerBase):
        def __init__(self):
            super().__init__("grpc://127.0.0.1:0")
            self.tickets = []

        def do_get(self, context, ticket):
            self.tickets.append(ticket.ticket)
            return flight.RecordBatchStream(
                pa.table({"plan_type": ["logical_plan"], "plan": [json.dumps(payload)]})
            )

    def handler(request):
        assert not request.path.startswith("/v1/jobs"), (
            "preview must not submit/monitor jobs"
        )
        return _create_table_handler(request)

    server = Server()
    with _mock_remote_sql_connection(server, handler) as kwargs:
        if asynchronous:
            db = await lancedb.connect_async(**kwargs)
            table = await db.create_table("test", [{"id": 1}])
            plan = await table.explain_cleanup(min_age_seconds=0)
        else:
            db = lancedb.connect(**kwargs)
            table = db.create_table("test", [{"id": 1}])
            plan = table.explain_cleanup(min_age_seconds=0)
        assert plan.source_version == 42
        assert plan.versions_to_remove == 2
    assert len(server.tickets) == 1
    assert b"EXPLAIN (FORMAT JSON) VACUUM" in server.tickets[0]
