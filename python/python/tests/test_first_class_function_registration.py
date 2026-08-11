# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""RED contract tests for Python first-class Function registration."""

from __future__ import annotations

import contextlib
import http.server
import json
import threading
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from typing import Any, Callable
from unittest import mock

import pyarrow as pa
import pytest

import lancedb
import lancedb._udf as _udf_mod
import lancedb.job
from lancedb import FunctionCapability, udf
from lancedb.remote.errors import HttpError

_SOURCE_MARKER = "registration-source-marker-unique-xyz"
_SECRET_REFERENCE = "secret://team/registration-redact-token-xyz"
_SECRET_ENV = "REGISTER_API_TOKEN"
_NETWORK_ORIGIN = "https://api.registration-example.com"
_FUNCTION_NAME = "text.normalize"
_FUNCTION_ID_RETRY = "fn.register-retry-1"
_JOB_ID_RETRY = "job-register-retry-1"
_JOB_ID_ASYNC = "job-register-async-1"
_REGISTER_PATH = "/v1/functions/register"
_DESCRIBE_PATH = "/v1/jobs/describe"

_DELETED_REGISTER_KEYWORDS = (
    "idempotency_key",
    "retry_key",
    "user_version",
    "deterministic",
    "null_policy",
    "replace",
    "expected_current_function_id",
)

_SPEC_KEYS = {
    "format_version",
    "name",
    "definition",
    "expected_current_function_id",
}


@udf(
    inputs={"text": pa.string(), "limit": pa.int32()},
    output=pa.string(),
    python="3.12",
    packages=["pkg-a==1"],
    output_nullable=True,
    capabilities=[
        FunctionCapability.network(_NETWORK_ORIGIN),
        FunctionCapability.secret(
            _SECRET_REFERENCE,
            environment_variable=_SECRET_ENV,
        ),
    ],
)
def packable_register_normalize(text, limit):
    """registration-source-marker-unique-xyz."""
    return text[:limit]


def _definition_json(fn: object) -> dict[str, Any]:
    payload = _udf_mod._build_function_definition(fn)._to_json()
    if isinstance(payload, bytes):
        return json.loads(payload.decode("utf-8"))
    assert isinstance(payload, str)
    return json.loads(payload)


def _expected_register_spec(name: str, fn: object) -> dict[str, Any]:
    return {
        "format_version": 1,
        "name": name,
        "definition": _definition_json(fn),
        "expected_current_function_id": None,
    }


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


def _exception_chain_text(exc: BaseException) -> str:
    parts: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(str(current))
        parts.append(repr(current))
        current = current.__cause__
    return "\n".join(parts)


def _assert_markers_absent_from_exception(exc: BaseException) -> None:
    text = _exception_chain_text(exc)
    assert _SOURCE_MARKER not in text
    assert _SECRET_REFERENCE not in text


def _assert_exact_register_spec(body: dict[str, Any], expected: dict[str, Any]) -> None:
    assert set(body) == _SPEC_KEYS
    assert body == expected
    assert body["format_version"] == 1
    assert body["expected_current_function_id"] is None
    assert _SOURCE_MARKER in json.dumps(body["definition"])
    assert any(
        capability.get("reference") == _SECRET_REFERENCE
        for capability in body["definition"]["capabilities"]
    )


def test_sync_remote_register_retries_exact_wire_and_returns_job():
    expected_spec = _expected_register_spec(_FUNCTION_NAME, packable_register_normalize)
    attempts: list[dict[str, Any]] = []
    describe_calls: list[dict[str, Any]] = []
    function_result_wire = {
        "kind": "function",
        "format_version": 1,
        "function": {
            "format_version": 1,
            "id": _FUNCTION_ID_RETRY,
            "signature": expected_spec["definition"]["signature"],
        },
    }

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.command == "POST"
        raw = _read_body(request)
        if request.path == _REGISTER_PATH:
            request_id = request.headers.get("x-request-id")
            attempts.append(
                {
                    "request_id": request_id,
                    "raw": raw,
                    "body": json.loads(raw.decode("utf-8")),
                }
            )
            if len(attempts) == 1:
                request.send_response(500)
                request.end_headers()
                request.wfile.write(b"transient register failure")
                return
            request.send_response(200)
            request.send_header("Content-Type", "application/json")
            request.end_headers()
            request.wfile.write(json.dumps({"job_id": _JOB_ID_RETRY}).encode("utf-8"))
            return

        assert request.path == _DESCRIBE_PATH
        body = json.loads(raw.decode("utf-8"))
        assert body["job_id"] == _JOB_ID_RETRY
        describe_calls.append(body)
        request.send_response(200)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(
            json.dumps(
                {
                    "job_id": _JOB_ID_RETRY,
                    "job_state": "DONE",
                    "job_type": "register_function",
                    "creation_ms": 1,
                    "spec": {},
                    "result": function_result_wire,
                }
            ).encode("utf-8")
        )

    package_calls = {"n": 0}
    original_package = _udf_mod._package_udf

    def counting_package(fn: object):
        package_calls["n"] += 1
        return original_package(fn)

    with _mock_remote_db(handler) as db:
        assert not hasattr(db, "register_function")
        with mock.patch.object(_udf_mod, "_package_udf", side_effect=counting_package):
            job = db.functions.register(_FUNCTION_NAME, packable_register_normalize)

        assert type(job) is lancedb.job.Job
        assert job.id == _JOB_ID_RETRY
        waited = job.wait(timeout=timedelta(seconds=5))

    assert package_calls["n"] == 1
    assert len(attempts) == 2
    first, second = attempts
    assert isinstance(first["request_id"], str) and first["request_id"]
    assert first["request_id"] == second["request_id"]
    assert first["raw"] == second["raw"]
    assert first["raw"]
    _assert_exact_register_spec(first["body"], expected_spec)
    _assert_exact_register_spec(second["body"], expected_spec)

    assert len(describe_calls) == 1
    assert describe_calls[0]["job_id"] == _JOB_ID_RETRY
    assert type(waited) is lancedb.Function
    assert waited.id == _FUNCTION_ID_RETRY
    assert waited.parameters == (("text", pa.string()), ("limit", pa.int32()))
    assert waited.output_type == pa.string()
    assert waited.output_nullable is True


@pytest.mark.asyncio
async def test_async_remote_register_returns_async_job_with_exact_spec():
    expected_spec = _expected_register_spec(_FUNCTION_NAME, packable_register_normalize)
    seen: dict[str, Any] = {}

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.command == "POST"
        assert request.path == _REGISTER_PATH
        raw = _read_body(request)
        seen["raw"] = raw
        seen["body"] = json.loads(raw.decode("utf-8"))
        seen["request_id"] = request.headers.get("x-request-id")
        request.send_response(200)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(json.dumps({"job_id": _JOB_ID_ASYNC}).encode("utf-8"))

    async with _mock_remote_db_async(handler) as db:
        assert not hasattr(db, "register_function")
        job = await db.functions.register(_FUNCTION_NAME, packable_register_normalize)

    assert seen.get("raw")
    assert isinstance(seen.get("request_id"), str) and seen["request_id"]
    _assert_exact_register_spec(seen["body"], expected_spec)
    assert type(job) is lancedb.job.AsyncJob
    assert job.id == _JOB_ID_ASYNC


def test_sync_remote_register_http_error_omits_source_and_secret_markers():
    echoed = f"register failed with {_SOURCE_MARKER} and {_SECRET_REFERENCE}"
    received = {"n": 0}

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        received["n"] += 1
        assert request.path == _REGISTER_PATH
        _read_body(request)
        request.send_response(400)
        request.end_headers()
        request.wfile.write(echoed.encode("utf-8"))

    with _mock_remote_db(handler) as db:
        with pytest.raises(HttpError) as exc_info:
            db.functions.register(_FUNCTION_NAME, packable_register_normalize)

    assert received["n"] == 1
    err = exc_info.value
    assert isinstance(err, HttpError)
    assert err.status_code == 400
    _assert_markers_absent_from_exception(err)


def test_empty_name_rejects_before_http():
    received = {"n": 0}

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        received["n"] += 1
        _read_body(request)
        request.send_response(500)
        request.end_headers()
        request.wfile.write(b"should not be reached")

    with _mock_remote_db(handler) as db:
        with pytest.raises(ValueError):
            db.functions.register("", packable_register_normalize)

    assert received["n"] == 0


def test_local_sync_register_not_implemented_without_table_mutation(tmp_path):
    db = lancedb.connect(tmp_path)
    before = db.list_tables().tables
    assert before == []
    assert not hasattr(db, "register_function")

    with pytest.raises(NotImplementedError):
        db.functions.register(_FUNCTION_NAME, packable_register_normalize)

    assert db.list_tables().tables == before


@pytest.mark.asyncio
async def test_local_async_register_not_implemented_without_table_mutation(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    before = (await db.list_tables()).tables
    assert before == []
    assert not hasattr(db, "register_function")

    with pytest.raises(NotImplementedError):
        await db.functions.register(_FUNCTION_NAME, packable_register_normalize)

    assert (await db.list_tables()).tables == before


@pytest.mark.parametrize("keyword", _DELETED_REGISTER_KEYWORDS)
def test_register_rejects_deleted_overdesign_keywords_before_submission(keyword):
    received = {"n": 0}

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        received["n"] += 1
        _read_body(request)
        request.send_response(500)
        request.end_headers()
        request.wfile.write(b"should not be reached")

    with _mock_remote_db(handler) as db:
        with pytest.raises(TypeError):
            db.functions.register(
                _FUNCTION_NAME,
                packable_register_normalize,
                **{keyword: True},
            )

    assert received["n"] == 0
