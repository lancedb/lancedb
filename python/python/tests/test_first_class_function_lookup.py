# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Contract tests for Python first-class Function catalog lookup."""

from __future__ import annotations

import contextlib
import http.server
import json
import threading
from collections.abc import AsyncIterator, Iterator
from typing import Any, Callable

import pyarrow as pa
import pytest

import lancedb
from lancedb import _lancedb as _native
from lancedb.remote.errors import HttpError


def _function_error_cls(*, required: bool = True):
    """Resolve FunctionError from the live module (records RED when absent)."""
    from lancedb import exceptions as exc_mod

    cls = getattr(exc_mod, "FunctionError", None)
    if cls is None:
        if required:
            pytest.fail("lancedb.exceptions.FunctionError is missing")
        return type("MissingFunctionError", (), {})
    return cls


_LOOKUP_PATH = "/v1/functions/lookup"
_LOOKUP_CATALOG_NAME = "text.normalize.lookup-name"
_LOOKUP_FUNCTION_ID = "fn.exact.lookup-handle"
_LOOKUP_SERVER_MESSAGE_MARKER = (
    "SERVER_LOOKUP_DIAGNOSTIC_MARKER name=text.normalize.lookup-name "
    "id=fn.exact.lookup-handle"
)
_SENSITIVE_BODY_MARKER = "SENSITIVE_LOOKUP_BODY_MARKER"
_UNKNOWN_CODE = "enterprise_future_lookup_category_xyz"

# Pinned Rust-canonical schema-only type IPC (base64). Same fixtures as job-result
# tests: PyArrow FileWriter bytes are not byte-identical to Arrow Rust FileWriter.
_INT32_TYPE_IPC_B64 = (
    "QVJST1cxAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP"
    "////94AAAAEAAAAAAACgAMAAoACQAEAAoAAAAQAAAAAAEEAAgACAAAAAQACAAAAAQAAAABAAAAFAAAABAAFAAQ"
    "AA4ADwAEAAAACAAQAAAAGAAAACAAAAAAAAECHAAAAAgADAAEAAsACAAAACAAAAAAAAABAAAAAAAAAAAAAAAA/"
    "////wAAAAAUAAAAAAAAAAwAFAASAAwACAAEAAwAAABsAAAAcAAAABAAAAAAAAQACAAIAAAABAAIAAAABAAAAA"
    "EAAAAUAAAAEAAUABAADgAPAAQAAAAIABAAAAAYAAAAIAAAAAAAAQIcAAAACAAMAAQACwAIAAAAIAAAAAAAAAE"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACQAAAAQVJST1cx"
)
_UTF8_TYPE_IPC_B64 = (
    "QVJST1cxAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP"
    "////94AAAAEAAAAAAACgAMAAoACQAEAAoAAAAQAAAAAAEEAAgACAAAAAQACAAAAAQAAAABAAAAFAAAABAAFAAQ"
    "AA4ADwAEAAAACAAQAAAAGAAAAAwAAAAAAAEFEAAAAAAAAAAEAAQABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/"
    "////wAAAAAQAAAADAAUABIADAAIAAQADAAAAGAAAABkAAAAEAAAAAAABAAIAAgAAAAEAAgAAAAEAAAAAQAAAB"
    "QAAAAQABQAEAAOAA8ABAAAAAgAEAAAABgAAAAMAAAAAAABBRAAAAAAAAAABAAEAAQAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAIAAAABBUlJPVzE="
)

_DELETED_LOOKUP_KEYWORDS = (
    "idempotency_key",
    "retry_key",
    "user_version",
    "replace",
    "expected_current_function_id",
    "list",
    "alias",
    "lineage",
    "FunctionVersion",
)


def _sample_function_wire() -> dict[str, Any]:
    return {
        "format_version": 1,
        "id": _LOOKUP_FUNCTION_ID,
        "signature": {
            "parameters": [
                {"name": "text", "data_type_ipc": _UTF8_TYPE_IPC_B64},
                {"name": "limit", "data_type_ipc": _INT32_TYPE_IPC_B64},
            ],
            "output": {
                "data_type_ipc": _UTF8_TYPE_IPC_B64,
                "nullable": True,
            },
        },
    }


def _lookup_success_body(
    *,
    function: dict[str, Any] | None = None,
    extra_outer: dict[str, Any] | None = None,
) -> bytes:
    body: dict[str, Any] = {"function": function or _sample_function_wire()}
    if extra_outer:
        body.update(extra_outer)
    return json.dumps(body).encode("utf-8")


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


def _assert_payload_free(exc: BaseException) -> None:
    text = _exception_chain_text(exc)
    assert _LOOKUP_SERVER_MESSAGE_MARKER not in text
    assert _LOOKUP_CATALOG_NAME not in text
    assert _LOOKUP_FUNCTION_ID not in text
    assert _SENSITIVE_BODY_MARKER not in text


def _assert_exact_lookup_function(function: object) -> None:
    assert type(function) is lancedb.Function
    assert function.id == _LOOKUP_FUNCTION_ID
    assert not hasattr(function, "name")
    assert function.parameters == (
        ("text", pa.string()),
        ("limit", pa.int32()),
    )
    assert function.output_type == pa.string()
    assert function.output_nullable is True
    assert _LOOKUP_CATALOG_NAME not in repr(function)
    assert _LOOKUP_CATALOG_NAME not in str(function)


def _assert_name_request(raw: bytes, body: dict[str, Any]) -> None:
    assert raw
    assert body == {"name": _LOOKUP_CATALOG_NAME}
    assert "function_id" not in body


def _assert_id_request(raw: bytes, body: dict[str, Any]) -> None:
    assert raw
    assert body == {"function_id": _LOOKUP_FUNCTION_ID}
    assert "name" not in body


def _assert_native_lookup_methods_present() -> None:
    assert hasattr(_native.Connection, "_lookup_function_by_name")
    assert hasattr(_native.Connection, "_lookup_function_by_id")
    assert callable(getattr(_native.Connection, "_lookup_function_by_name"))
    assert callable(getattr(_native.Connection, "_lookup_function_by_id"))


def test_native_connection_exposes_private_lookup_methods():
    _assert_native_lookup_methods_present()


def test_sync_remote_get_by_name_exact_request_and_function_shape():
    _assert_native_lookup_methods_present()
    seen: dict[str, Any] = {}

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.command == "POST"
        assert request.path == _LOOKUP_PATH
        raw = _read_body(request)
        seen["raw"] = raw
        seen["body"] = json.loads(raw.decode("utf-8"))
        request.send_response(200)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(_lookup_success_body())

    with _mock_remote_db(handler) as db:
        assert not hasattr(db, "lookup_function_by_name")
        assert not hasattr(db, "lookup_function_by_id")
        assert not hasattr(db, "get_function")
        function = db.functions.get(_LOOKUP_CATALOG_NAME)

    _assert_name_request(seen["raw"], seen["body"])
    _assert_exact_lookup_function(function)


def test_sync_remote_get_by_id_exact_request_and_function_shape():
    _assert_native_lookup_methods_present()
    seen: dict[str, Any] = {}

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.command == "POST"
        assert request.path == _LOOKUP_PATH
        raw = _read_body(request)
        seen["raw"] = raw
        seen["body"] = json.loads(raw.decode("utf-8"))
        request.send_response(200)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(_lookup_success_body())

    with _mock_remote_db(handler) as db:
        function = db.functions.get_by_id(_LOOKUP_FUNCTION_ID)

    _assert_id_request(seen["raw"], seen["body"])
    _assert_exact_lookup_function(function)


@pytest.mark.asyncio
async def test_async_remote_get_by_name_and_id():
    _assert_native_lookup_methods_present()
    name_seen: dict[str, Any] = {}
    id_seen: dict[str, Any] = {}
    stage = {"n": 0}

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.command == "POST"
        assert request.path == _LOOKUP_PATH
        raw = _read_body(request)
        body = json.loads(raw.decode("utf-8"))
        stage["n"] += 1
        if stage["n"] == 1:
            name_seen["raw"] = raw
            name_seen["body"] = body
        else:
            id_seen["raw"] = raw
            id_seen["body"] = body
        request.send_response(200)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(_lookup_success_body())

    async with _mock_remote_db_async(handler) as db:
        assert not hasattr(db, "lookup_function_by_name")
        assert not hasattr(db, "lookup_function_by_id")
        by_name = await db.functions.get(_LOOKUP_CATALOG_NAME)
        by_id = await db.functions.get_by_id(_LOOKUP_FUNCTION_ID)

    _assert_name_request(name_seen["raw"], name_seen["body"])
    _assert_id_request(id_seen["raw"], id_seen["body"])
    _assert_exact_lookup_function(by_name)
    _assert_exact_lookup_function(by_id)


def test_sync_remote_get_accepts_additive_outer_success_fields():
    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.path == _LOOKUP_PATH
        _read_body(request)
        request.send_response(200)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(
            _lookup_success_body(
                extra_outer={
                    "server_extra": {"ok": True},
                    "request_echo_name": _LOOKUP_CATALOG_NAME,
                }
            )
        )

    with _mock_remote_db(handler) as db:
        function = db.functions.get(_LOOKUP_CATALOG_NAME)

    _assert_exact_lookup_function(function)


def test_empty_name_and_id_reject_before_transport():
    received = {"n": 0}

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        received["n"] += 1
        _read_body(request)
        request.send_response(500)
        request.end_headers()
        request.wfile.write(b"should not be reached")

    with _mock_remote_db(handler) as db:
        with pytest.raises(ValueError):
            db.functions.get("")
        with pytest.raises(ValueError):
            db.functions.get_by_id("")

    assert received["n"] == 0


def test_local_sync_lookup_not_implemented_without_table_mutation(tmp_path):
    _assert_native_lookup_methods_present()
    db = lancedb.connect(tmp_path)
    before = db.list_tables().tables
    assert before == []
    assert not hasattr(db, "lookup_function_by_name")
    assert not hasattr(db, "lookup_function_by_id")

    with pytest.raises(NotImplementedError):
        db.functions.get(_LOOKUP_CATALOG_NAME)
    with pytest.raises(NotImplementedError):
        db.functions.get_by_id(_LOOKUP_FUNCTION_ID)

    assert db.list_tables().tables == before


@pytest.mark.asyncio
async def test_local_async_lookup_not_implemented_without_table_mutation(tmp_path):
    _assert_native_lookup_methods_present()
    db = await lancedb.connect_async(tmp_path)
    before = (await db.list_tables()).tables
    assert before == []
    assert not hasattr(db, "lookup_function_by_name")
    assert not hasattr(db, "lookup_function_by_id")

    with pytest.raises(NotImplementedError):
        await db.functions.get(_LOOKUP_CATALOG_NAME)
    with pytest.raises(NotImplementedError):
        await db.functions.get_by_id(_LOOKUP_FUNCTION_ID)

    assert (await db.list_tables()).tables == before


def test_explicit_known_code_is_function_error_with_exact_code():
    body = {
        "error_code": "name_or_function_not_found",
        "message": _LOOKUP_SERVER_MESSAGE_MARKER,
        "looks_like": "definition_validation_failure",
    }

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.path == _LOOKUP_PATH
        _read_body(request)
        request.send_response(404)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(json.dumps(body).encode("utf-8"))

    function_error = _function_error_cls()
    with _mock_remote_db(handler) as db:
        with pytest.raises(function_error) as exc_info:
            db.functions.get(_LOOKUP_CATALOG_NAME)

    err = exc_info.value
    assert isinstance(err, function_error)
    assert err.code == "name_or_function_not_found"
    assert err.code != "definition_validation_failure"
    _assert_payload_free(err)


def test_explicit_unknown_code_preserved_despite_status_and_message():
    body = {
        "error_code": _UNKNOWN_CODE,
        "message": (
            f"{_LOOKUP_SERVER_MESSAGE_MARKER} revoked_function "
            "name_or_function_not_found"
        ),
    }

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.path == _LOOKUP_PATH
        raw = _read_body(request)
        assert json.loads(raw.decode("utf-8")) == {"function_id": _LOOKUP_FUNCTION_ID}
        request.send_response(409)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(json.dumps(body).encode("utf-8"))

    function_error = _function_error_cls()
    with _mock_remote_db(handler) as db:
        with pytest.raises(function_error) as exc_info:
            db.functions.get_by_id(_LOOKUP_FUNCTION_ID)

    err = exc_info.value
    assert err.code == _UNKNOWN_CODE
    assert err.code != "revoked_function"
    assert err.code != "name_or_function_not_found"
    _assert_payload_free(err)


@pytest.mark.parametrize(
    "label,status,response_body",
    [
        (
            "missing_code_404",
            404,
            {
                "message": _LOOKUP_SERVER_MESSAGE_MARKER,
                _SENSITIVE_BODY_MARKER: True,
            },
        ),
        (
            "empty_code",
            400,
            {
                "error_code": "",
                "message": _LOOKUP_SERVER_MESSAGE_MARKER,
                _SENSITIVE_BODY_MARKER: True,
            },
        ),
        (
            "wrong_type_code",
            400,
            {
                "error_code": 123,
                "message": _LOOKUP_SERVER_MESSAGE_MARKER,
                _SENSITIVE_BODY_MARKER: True,
            },
        ),
        (
            "null_code",
            404,
            {
                "error_code": None,
                "message": _LOOKUP_SERVER_MESSAGE_MARKER,
                _SENSITIVE_BODY_MARKER: True,
            },
        ),
        (
            "non_json",
            404,
            f"not-json {_LOOKUP_SERVER_MESSAGE_MARKER} {_SENSITIVE_BODY_MARKER}",
        ),
    ],
)
def test_invalid_or_missing_error_code_is_payload_free_http(
    label: str, status: int, response_body: object
):
    del label  # parametrize label for failure diagnosis only

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.path == _LOOKUP_PATH
        _read_body(request)
        request.send_response(status)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        if isinstance(response_body, str):
            request.wfile.write(response_body.encode("utf-8"))
        else:
            request.wfile.write(json.dumps(response_body).encode("utf-8"))

    with _mock_remote_db(handler) as db:
        with pytest.raises(HttpError) as exc_info:
            db.functions.get(_LOOKUP_CATALOG_NAME)

    err = exc_info.value
    assert isinstance(err, HttpError)
    assert not isinstance(err, _function_error_cls(required=False))
    _assert_payload_free(err)


@pytest.mark.parametrize(
    "label,response_body",
    [
        (
            "missing_function",
            {
                "server_extra": True,
                _SENSITIVE_BODY_MARKER: _LOOKUP_SERVER_MESSAGE_MARKER,
            },
        ),
        (
            "null_function",
            {
                "function": None,
                _SENSITIVE_BODY_MARKER: _LOOKUP_SERVER_MESSAGE_MARKER,
            },
        ),
        (
            "wrong_type_function",
            {
                "function": "not-an-object",
                _SENSITIVE_BODY_MARKER: _LOOKUP_SERVER_MESSAGE_MARKER,
            },
        ),
        (
            "invalid_function_shape",
            {
                "function": {
                    "format_version": 1,
                    "id": _LOOKUP_FUNCTION_ID,
                    # missing signature
                    _SENSITIVE_BODY_MARKER: True,
                }
            },
        ),
    ],
)
def test_malformed_success_is_payload_free_http(label: str, response_body: dict):
    del label

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.path == _LOOKUP_PATH
        _read_body(request)
        request.send_response(200)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(json.dumps(response_body).encode("utf-8"))

    with _mock_remote_db(handler) as db:
        with pytest.raises(HttpError) as exc_info:
            db.functions.get(_LOOKUP_CATALOG_NAME)

    err = exc_info.value
    assert isinstance(err, HttpError)
    assert not isinstance(err, _function_error_cls(required=False))
    _assert_payload_free(err)


def test_function_error_surface_omits_server_marker_name_and_id():
    body = {
        "error_code": "name_or_function_not_found",
        "message": _LOOKUP_SERVER_MESSAGE_MARKER,
        "function_id": _LOOKUP_FUNCTION_ID,
        "name": _LOOKUP_CATALOG_NAME,
        _SENSITIVE_BODY_MARKER: True,
    }

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        _read_body(request)
        request.send_response(404)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(json.dumps(body).encode("utf-8"))

    function_error = _function_error_cls()
    with _mock_remote_db(handler) as db:
        with pytest.raises(function_error) as exc_info:
            db.functions.get(_LOOKUP_CATALOG_NAME)

    err = exc_info.value
    _assert_payload_free(err)
    assert getattr(err, "code", None) == "name_or_function_not_found"


def test_no_direct_db_lookup_methods_and_no_deleted_keywords():
    received = {"n": 0}

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        received["n"] += 1
        _read_body(request)
        request.send_response(500)
        request.end_headers()
        request.wfile.write(b"should not be reached")

    with _mock_remote_db(handler) as db:
        assert not hasattr(db, "lookup_function")
        assert not hasattr(db, "lookup_function_by_name")
        assert not hasattr(db, "lookup_function_by_id")
        assert not hasattr(db, "get_function")
        assert not hasattr(db.functions, "get_by_name")
        assert not hasattr(db.functions, "list")

        for keyword in _DELETED_LOOKUP_KEYWORDS:
            with pytest.raises(TypeError):
                db.functions.get(_LOOKUP_CATALOG_NAME, **{keyword: True})
            with pytest.raises(TypeError):
                db.functions.get_by_id(_LOOKUP_FUNCTION_ID, **{keyword: True})

    assert received["n"] == 0


def test_function_error_is_not_top_level_export():
    assert not hasattr(lancedb, "FunctionError")
    function_error = _function_error_cls()
    assert issubclass(function_error, RuntimeError)
