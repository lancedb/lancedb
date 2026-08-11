# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Contract: Python projection of JobFailure.error_code / JobFailedError.error_code.

Public Function failures expose eight stable string categories. Asynchronous
errors remain the unified JobFailedError and JobFailureInfo. Python must
project the optional exact error_code string already supplied structurally by
Rust: preserve a known code, preserve an unknown nonempty future code
byte-for-byte, and return None for legacy failure payloads without error_code.
Never infer or override a code from message, phase, retryable, HTTP status,
job type, or state.
"""

from __future__ import annotations

import contextlib
import http.server
import json
import threading
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from typing import Any, Callable, Optional

import pytest

import lancedb
from lancedb.exceptions import JobFailedError

_DESCRIBE_PATH = "/v1/jobs/describe"
_KNOWN_CODE = "name_or_function_not_found"
_CONFLICTING_STABLE_IN_MESSAGE = "definition_validation_failure"
_UNKNOWN_CODE = "enterprise_future_category_xyz"
_WAIT_KNOWN_CODE = "unsupported_runtime_or_capability"
_WAIT_CONFLICTING_IN_MESSAGE = "revoked_function"


def _read_body(request: http.server.BaseHTTPRequestHandler) -> bytes:
    content_len = int(request.headers.get("Content-Length", 0))
    if content_len <= 0:
        return b""
    return request.rfile.read(content_len)


def _make_handler(handler: Callable[[http.server.BaseHTTPRequestHandler], None]):
    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            handler(self)

        def do_POST(self):
            handler(self)

        def log_message(self, format, *args):  # noqa: A003
            return

    return _Handler


@contextlib.contextmanager
def _mock_remote_db(handler) -> Iterator[Any]:
    server = http.server.HTTPServer(("localhost", 0), _make_handler(handler))
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        db = lancedb.connect(
            "db://dev",
            api_key="fake",
            host_override=f"http://localhost:{port}",
            client_config={
                "retry_config": {
                    "retries": 2,
                    "backoff_factor": 0.0,
                    "backoff_jitter": 0.0,
                },
                "timeout_config": {"connect_timeout": 1},
            },
        )
        yield db
    finally:
        server.shutdown()
        thread.join()


@contextlib.asynccontextmanager
async def _mock_remote_db_async(handler) -> AsyncIterator[Any]:
    server = http.server.HTTPServer(("localhost", 0), _make_handler(handler))
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        db = await lancedb.connect_async(
            "db://dev",
            api_key="fake",
            host_override=f"http://localhost:{port}",
            client_config={
                "retry_config": {
                    "retries": 2,
                    "backoff_factor": 0.0,
                    "backoff_jitter": 0.0,
                },
                "timeout_config": {"connect_timeout": 1},
            },
        )
        yield db
    finally:
        server.shutdown()
        thread.join()


def _failed_describe_body(
    *,
    job_id: str,
    error_code: Optional[str] = None,
    include_error_code: bool = True,
    phase: str = "execute",
    message: str = "worker died",
    retryable: bool = False,
    job_type: str = "create_index",
) -> dict[str, Any]:
    failure: dict[str, Any] = {
        "phase": phase,
        "message": message,
        "retryable": retryable,
    }
    if include_error_code:
        failure["error_code"] = error_code
    return {
        "job_id": job_id,
        "job_type": job_type,
        "job_state": "FAILED",
        "creation_ms": 1000,
        "spec": {},
        "failure": failure,
    }


def _describe_handler(bodies_by_job_id: dict[str, dict[str, Any]]):
    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.path == _DESCRIBE_PATH
        payload = json.loads(_read_body(request).decode("utf-8") or "{}")
        job_id = payload["job_id"]
        body = bodies_by_job_id.get(job_id)
        if body is None:
            request.send_response(404)
            request.end_headers()
            return
        request.send_response(200)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(json.dumps(body).encode("utf-8"))

    return handler


def test_get_job_failure_error_code_known_not_inferred_from_message():
    """Structural error_code wins; conflicting message text must not override."""
    body = _failed_describe_body(
        job_id="job-known",
        error_code=_KNOWN_CODE,
        phase="validate",
        message=f"looks like {_CONFLICTING_STABLE_IN_MESSAGE}",
        retryable=False,
    )
    with _mock_remote_db(_describe_handler({"job-known": body})) as db:
        description = db.get_job("job-known")
        assert description is not None
        failure = description.failure
        assert failure is not None
        assert failure.error_code == _KNOWN_CODE
        assert failure.error_code != _CONFLICTING_STABLE_IN_MESSAGE
        assert failure.phase == "validate"
        assert failure.message == f"looks like {_CONFLICTING_STABLE_IN_MESSAGE}"
        assert failure.retryable is False


def test_get_job_failure_error_code_unknown_preserved_byte_for_byte():
    body = _failed_describe_body(
        job_id="job-unknown",
        error_code=_UNKNOWN_CODE,
        phase="execute",
        message=f"new category mentioning {_KNOWN_CODE}",
        retryable=True,
    )
    with _mock_remote_db(_describe_handler({"job-unknown": body})) as db:
        failure = db.get_job("job-unknown").failure
        assert failure.error_code == _UNKNOWN_CODE
        assert failure.error_code != _KNOWN_CODE


def test_get_job_failure_error_code_absent_is_none():
    """Legacy describe payloads without error_code must not invent a category."""
    body = _failed_describe_body(
        job_id="job-legacy",
        include_error_code=False,
        phase="execute",
        message=f"{_KNOWN_CODE} in logs",
        retryable=True,
    )
    with _mock_remote_db(_describe_handler({"job-legacy": body})) as db:
        failure = db.get_job("job-legacy").failure
        assert failure.error_code is None
        assert failure.phase == "execute"
        assert failure.retryable is True


def test_sync_job_wait_job_failed_error_code_known_not_inferred():
    body = _failed_describe_body(
        job_id="job-wait-known",
        error_code=_WAIT_KNOWN_CODE,
        phase="dispatch",
        message=f"{_WAIT_CONFLICTING_IN_MESSAGE} in transport logs",
        retryable=False,
    )
    with _mock_remote_db(_describe_handler({"job-wait-known": body})) as db:
        with pytest.raises(JobFailedError) as exc_info:
            db.job("job-wait-known").wait(timeout=timedelta(seconds=5))

    err = exc_info.value
    assert isinstance(err, JobFailedError)
    assert err.error_code == _WAIT_KNOWN_CODE
    assert err.error_code != _WAIT_CONFLICTING_IN_MESSAGE


def test_sync_job_wait_job_failed_error_code_absent_is_none():
    body = _failed_describe_body(
        job_id="job-wait-legacy",
        include_error_code=False,
        phase="execute",
        message=f"{_WAIT_KNOWN_CODE} mentioned only in message",
        retryable=True,
    )
    with _mock_remote_db(_describe_handler({"job-wait-legacy": body})) as db:
        with pytest.raises(JobFailedError) as exc_info:
            db.job("job-wait-legacy").wait(timeout=timedelta(seconds=5))

    assert exc_info.value.error_code is None


@pytest.mark.asyncio
async def test_async_job_wait_job_failed_error_code_unknown_preserved():
    body = _failed_describe_body(
        job_id="job-wait-unknown",
        error_code=_UNKNOWN_CODE,
        phase="execute",
        message=f"future code with {_WAIT_KNOWN_CODE} in text",
        retryable=False,
    )
    async with _mock_remote_db_async(
        _describe_handler({"job-wait-unknown": body})
    ) as db:
        with pytest.raises(JobFailedError) as exc_info:
            await db.job("job-wait-unknown").wait(timeout=timedelta(seconds=5))

    err = exc_info.value
    assert err.error_code == _UNKNOWN_CODE
    assert err.error_code != _WAIT_KNOWN_CODE


def test_job_failed_error_legacy_message_construction_error_code_is_none():
    err = JobFailedError("legacy construction with only a message")
    assert err.error_code is None


def test_job_failed_error_error_code_is_read_only():
    err = JobFailedError("message")
    with pytest.raises(AttributeError):
        err.error_code = _KNOWN_CODE
