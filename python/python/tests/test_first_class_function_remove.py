# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Contract tests for Python conditional first-class Function name removal."""

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

_REMOVE_PATH = "/v1/functions/remove"
_LOOKUP_PATH = "/v1/functions/lookup"
_REMOVE_CATALOG_NAME = "text.normalize.remove-name"
_REMOVE_FUNCTION_ID = "fn.exact.remove-handle"
_REMOVE_SERVER_MESSAGE_MARKER = (
    "SERVER_REMOVE_DIAGNOSTIC_MARKER name=text.normalize.remove-name "
    "id=fn.exact.remove-handle"
)
_SENSITIVE_BODY_MARKER = "SENSITIVE_REMOVE_BODY_MARKER"
_CONFLICTING_MESSAGE_CODE = "revoked_function"

# Pinned Rust-canonical schema-only type IPC (base64). Same fixtures as lookup /
# replace tests: PyArrow FileWriter bytes are not byte-identical to Arrow Rust.
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

_DELETED_REMOVE_KEYWORDS = (
    "expected_current_function_id",
    "function_id",
    "idempotency_key",
    "retry_key",
    "user_version",
    "version",
    "force",
    "if_exists",
    "revoke",
    "delete",
)


def _function_error_cls(*, required: bool = True):
    """Resolve FunctionError from the live module (records RED when absent)."""
    from lancedb import exceptions as exc_mod

    cls = getattr(exc_mod, "FunctionError", None)
    if cls is None:
        if required:
            pytest.fail("lancedb.exceptions.FunctionError is missing")
        return type("MissingFunctionError", (), {})
    return cls


def _sample_function_wire() -> dict[str, Any]:
    return {
        "format_version": 1,
        "id": _REMOVE_FUNCTION_ID,
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


def _lookup_success_body() -> bytes:
    return json.dumps({"function": _sample_function_wire()}).encode("utf-8")


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


def _close_db(db: Any) -> None:
    with contextlib.suppress(Exception):
        inner = getattr(db, "_conn", None)
        if inner is not None:
            inner.close()
            return
        close = getattr(db, "close", None)
        if callable(close):
            close()


@contextlib.contextmanager
def _mock_remote_db(handler) -> Iterator[Any]:
    server = http.server.HTTPServer(("localhost", 0), _make_handler(handler))
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    db = None
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
        if db is not None:
            _close_db(db)
        server.shutdown()
        thread.join()


@contextlib.asynccontextmanager
async def _mock_remote_db_async(handler) -> AsyncIterator[Any]:
    server = http.server.HTTPServer(("localhost", 0), _make_handler(handler))
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    db = None
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
        if db is not None:
            _close_db(db)
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
    assert _REMOVE_SERVER_MESSAGE_MARKER not in text
    assert _REMOVE_CATALOG_NAME not in text
    assert _REMOVE_FUNCTION_ID not in text
    assert _SENSITIVE_BODY_MARKER not in text


def _assert_exact_remove_function(function: object) -> None:
    assert type(function) is lancedb.Function
    assert function.id == _REMOVE_FUNCTION_ID
    assert not hasattr(function, "name")
    assert function.parameters == (
        ("text", pa.string()),
        ("limit", pa.int32()),
    )
    assert function.output_type == pa.string()
    assert function.output_nullable is True
    assert _REMOVE_CATALOG_NAME not in repr(function)
    assert _REMOVE_CATALOG_NAME not in str(function)


def _assert_exact_remove_request(
    request: http.server.BaseHTTPRequestHandler,
    raw: bytes,
    body: dict[str, Any],
    *,
    expected_id: str,
) -> None:
    assert request.command == "POST"
    assert request.path == _REMOVE_PATH
    assert "?" not in request.path
    assert raw
    assert body == {
        "name": _REMOVE_CATALOG_NAME,
        "expected_current_function_id": expected_id,
    }
    assert set(body) == {"name", "expected_current_function_id"}
    assert "format_version" not in body
    assert "function_id" not in body
    assert "function" not in body
    assert "signature" not in body
    assert "job_id" not in body
    assert "idempotency_key" not in body
    assert "user_version" not in body
    assert "force" not in body
    assert "if_exists" not in body
    request_id = request.headers.get("x-request-id")
    assert isinstance(request_id, str) and request_id


def _assert_native_remove_method_present() -> None:
    assert hasattr(_native.Connection, "_remove_function_name")
    assert callable(getattr(_native.Connection, "_remove_function_name"))


def _lookup_success_handler(
    counters: dict[str, int],
    *,
    after_lookup: (
        Callable[[http.server.BaseHTTPRequestHandler, bytes], None] | None
    ) = None,
):
    """Serve exact name lookup; optionally continue for remove."""

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.command == "POST"
        raw = _read_body(request)
        if request.path == _LOOKUP_PATH:
            counters["lookup"] = counters.get("lookup", 0) + 1
            body = json.loads(raw.decode("utf-8"))
            assert body == {"name": _REMOVE_CATALOG_NAME}
            request.send_response(200)
            request.send_header("Content-Type", "application/json")
            request.end_headers()
            request.wfile.write(_lookup_success_body())
            return
        if after_lookup is not None:
            after_lookup(request, raw)
            return
        counters["remove"] = counters.get("remove", 0) + 1
        request.send_response(500)
        request.end_headers()
        request.wfile.write(b"unexpected remove")

    return handler


def _observe_current(db) -> lancedb.Function:
    current = db.functions.get(_REMOVE_CATALOG_NAME)
    _assert_exact_remove_function(current)
    assert not hasattr(current, "remove")
    assert not hasattr(current, "delete")
    assert not hasattr(current, "revoke")
    return current


async def _observe_current_async(db) -> lancedb.Function:
    current = await db.functions.get(_REMOVE_CATALOG_NAME)
    _assert_exact_remove_function(current)
    assert not hasattr(current, "remove")
    assert not hasattr(current, "delete")
    assert not hasattr(current, "revoke")
    return current


def test_native_connection_exposes_private_remove_function_name():
    _assert_native_remove_method_present()


def test_sync_remote_remove_exact_body_path_request_id_returns_none():
    _assert_native_remove_method_present()
    counters: dict[str, int] = {"lookup": 0, "remove": 0}
    remove_attempts: list[dict[str, Any]] = []

    def after_lookup(
        request: http.server.BaseHTTPRequestHandler, payload: bytes
    ) -> None:
        assert request.path == _REMOVE_PATH
        counters["remove"] += 1
        body = json.loads(payload.decode("utf-8"))
        remove_attempts.append(
            {
                "request": request,
                "raw": payload,
                "body": body,
                "request_id": request.headers.get("x-request-id"),
            }
        )
        # Illegal body on 204 must be ignored; success is status-driven only.
        request.send_response(204)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(
            json.dumps(
                {
                    _SENSITIVE_BODY_MARKER: True,
                    "message": _REMOVE_SERVER_MESSAGE_MARKER,
                }
            ).encode("utf-8")
        )

    with _mock_remote_db(
        _lookup_success_handler(counters, after_lookup=after_lookup)
    ) as db:
        assert not hasattr(db, "remove_function")
        assert not hasattr(db, "remove_function_name")
        current = _observe_current(db)
        assert counters["lookup"] == 1
        assert counters["remove"] == 0

        result = db.functions.remove(_REMOVE_CATALOG_NAME, current)

    assert result is None
    assert counters["lookup"] == 1
    assert counters["remove"] == 1
    assert len(remove_attempts) == 1
    attempt = remove_attempts[0]
    _assert_exact_remove_request(
        attempt["request"],
        attempt["raw"],
        attempt["body"],
        expected_id=current.id,
    )
    assert attempt["body"]["expected_current_function_id"] == current.id


@pytest.mark.asyncio
async def test_async_remote_remove_exact_body_returns_none():
    _assert_native_remove_method_present()
    counters: dict[str, int] = {"lookup": 0, "remove": 0}
    seen: dict[str, Any] = {}

    def after_lookup(request: http.server.BaseHTTPRequestHandler, raw: bytes) -> None:
        assert request.path == _REMOVE_PATH
        counters["remove"] += 1
        seen["request"] = request
        seen["raw"] = raw
        seen["body"] = json.loads(raw.decode("utf-8"))
        seen["request_id"] = request.headers.get("x-request-id")
        request.send_response(204)
        request.end_headers()

    async with _mock_remote_db_async(
        _lookup_success_handler(counters, after_lookup=after_lookup)
    ) as db:
        assert not hasattr(db, "remove_function")
        assert not hasattr(db, "remove_function_name")
        current = await _observe_current_async(db)
        assert counters["lookup"] == 1
        assert counters["remove"] == 0
        result = await db.functions.remove(_REMOVE_CATALOG_NAME, current)

    assert result is None
    assert counters["lookup"] == 1
    assert counters["remove"] == 1
    assert seen.get("raw")
    _assert_exact_remove_request(
        seen["request"],
        seen["raw"],
        seen["body"],
        expected_id=current.id,
    )


def test_after_remove_name_lookup_not_found_id_lookup_same_function():
    """Catalog-pointer SDK sequence via a stateful fixture; not server atomicity."""
    _assert_native_remove_method_present()
    counters: dict[str, int] = {
        "lookup_name": 0,
        "lookup_id": 0,
        "remove": 0,
    }
    removed = {"yes": False}

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.command == "POST"
        raw = _read_body(request)
        if request.path == _LOOKUP_PATH:
            body = json.loads(raw.decode("utf-8"))
            if "name" in body:
                counters["lookup_name"] += 1
                assert body == {"name": _REMOVE_CATALOG_NAME}
                if removed["yes"]:
                    request.send_response(404)
                    request.send_header("Content-Type", "application/json")
                    request.end_headers()
                    request.wfile.write(
                        json.dumps(
                            {
                                "error_code": "name_or_function_not_found",
                                "message": _REMOVE_SERVER_MESSAGE_MARKER,
                            }
                        ).encode("utf-8")
                    )
                    return
                request.send_response(200)
                request.send_header("Content-Type", "application/json")
                request.end_headers()
                request.wfile.write(_lookup_success_body())
                return

            counters["lookup_id"] += 1
            assert body == {"function_id": _REMOVE_FUNCTION_ID}
            request.send_response(200)
            request.send_header("Content-Type", "application/json")
            request.end_headers()
            request.wfile.write(_lookup_success_body())
            return

        assert request.path == _REMOVE_PATH
        counters["remove"] += 1
        body = json.loads(raw.decode("utf-8"))
        _assert_exact_remove_request(
            request, raw, body, expected_id=_REMOVE_FUNCTION_ID
        )
        removed["yes"] = True
        request.send_response(204)
        request.end_headers()

    function_error = _function_error_cls()
    with _mock_remote_db(handler) as db:
        current = _observe_current(db)
        assert counters["lookup_name"] == 1
        assert counters["lookup_id"] == 0
        assert counters["remove"] == 0

        result = db.functions.remove(_REMOVE_CATALOG_NAME, current)
        assert result is None
        assert counters["lookup_name"] == 1
        assert counters["remove"] == 1

        with pytest.raises(function_error) as exc_info:
            db.functions.get(_REMOVE_CATALOG_NAME)
        err = exc_info.value
        assert err.code == "name_or_function_not_found"
        _assert_payload_free(err)

        by_id = db.functions.get_by_id(_REMOVE_FUNCTION_ID)

    assert counters["lookup_name"] == 2
    assert counters["lookup_id"] == 1
    assert counters["remove"] == 1
    _assert_exact_remove_function(by_id)
    assert by_id.id == current.id
    assert by_id.parameters == current.parameters
    assert by_id.output_type == current.output_type
    assert by_id.output_nullable is current.output_nullable


def test_explicit_name_conflict_is_function_error_payload_free():
    _assert_native_remove_method_present()
    counters: dict[str, int] = {"lookup": 0, "remove": 0}
    body = {
        "error_code": "name_conflict",
        "message": (
            f"{_REMOVE_SERVER_MESSAGE_MARKER} looks_like {_CONFLICTING_MESSAGE_CODE}"
        ),
        "name": _REMOVE_CATALOG_NAME,
        "function_id": _REMOVE_FUNCTION_ID,
        _SENSITIVE_BODY_MARKER: True,
    }

    def after_lookup(
        request: http.server.BaseHTTPRequestHandler, payload: bytes
    ) -> None:
        assert request.path == _REMOVE_PATH
        counters["remove"] += 1
        parsed = json.loads(payload.decode("utf-8"))
        _assert_exact_remove_request(
            request, payload, parsed, expected_id=_REMOVE_FUNCTION_ID
        )
        request.send_response(409)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(json.dumps(body).encode("utf-8"))

    function_error = _function_error_cls()
    with _mock_remote_db(
        _lookup_success_handler(counters, after_lookup=after_lookup)
    ) as db:
        current = _observe_current(db)
        assert counters["lookup"] == 1
        assert counters["remove"] == 0
        with pytest.raises(function_error) as exc_info:
            db.functions.remove(_REMOVE_CATALOG_NAME, current)

    assert counters["lookup"] == 1
    assert counters["remove"] == 1
    err = exc_info.value
    assert isinstance(err, function_error)
    assert err.code == "name_conflict"
    assert err.code != _CONFLICTING_MESSAGE_CODE
    _assert_payload_free(err)


@pytest.mark.parametrize(
    "label,status,response_body",
    [
        (
            "200_with_body",
            200,
            {
                "ok": True,
                "message": _REMOVE_SERVER_MESSAGE_MARKER,
                _SENSITIVE_BODY_MARKER: True,
                "job_id": "must-not-infer-job",
            },
        ),
        (
            "202_empty",
            202,
            f"{_REMOVE_SERVER_MESSAGE_MARKER} {_SENSITIVE_BODY_MARKER}",
        ),
        ("200_empty", 200, ""),
    ],
)
def test_http_200_202_cannot_return_success(
    label: str, status: int, response_body: object
):
    del label
    _assert_native_remove_method_present()
    counters: dict[str, int] = {"lookup": 0, "remove": 0}

    def after_lookup(
        request: http.server.BaseHTTPRequestHandler, payload: bytes
    ) -> None:
        assert request.path == _REMOVE_PATH
        counters["remove"] += 1
        parsed = json.loads(payload.decode("utf-8"))
        _assert_exact_remove_request(
            request, payload, parsed, expected_id=_REMOVE_FUNCTION_ID
        )
        request.send_response(status)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        if isinstance(response_body, str):
            request.wfile.write(response_body.encode("utf-8"))
        else:
            request.wfile.write(json.dumps(response_body).encode("utf-8"))

    with _mock_remote_db(
        _lookup_success_handler(counters, after_lookup=after_lookup)
    ) as db:
        current = _observe_current(db)
        assert counters["lookup"] == 1
        assert counters["remove"] == 0
        with pytest.raises(HttpError) as exc_info:
            db.functions.remove(_REMOVE_CATALOG_NAME, current)

    assert counters["lookup"] == 1
    assert counters["remove"] == 1
    err = exc_info.value
    assert isinstance(err, HttpError)
    assert not isinstance(err, _function_error_cls(required=False))
    _assert_payload_free(err)


def test_empty_name_rejects_before_remove_transport():
    counters: dict[str, int] = {"lookup": 0, "remove": 0}

    with _mock_remote_db(_lookup_success_handler(counters)) as db:
        current = _observe_current(db)
        assert counters["lookup"] == 1
        assert counters["remove"] == 0
        with pytest.raises(ValueError):
            db.functions.remove("", current)

    assert counters["lookup"] == 1
    assert counters["remove"] == 0


@pytest.mark.parametrize(
    "bad_current",
    [
        _REMOVE_FUNCTION_ID,
        {"id": _REMOVE_FUNCTION_ID},
        object(),
        123,
    ],
)
def test_raw_id_or_arbitrary_current_rejected_without_remove(bad_current):
    counters: dict[str, int] = {"lookup": 0, "remove": 0}

    with _mock_remote_db(_lookup_success_handler(counters)) as db:
        # Observe a real handle separately so the bad-current path is isolated.
        _ = _observe_current(db)
        assert counters["lookup"] == 1
        assert counters["remove"] == 0
        with pytest.raises(TypeError):
            db.functions.remove(_REMOVE_CATALOG_NAME, bad_current)

    assert counters["lookup"] == 1
    assert counters["remove"] == 0


def test_local_sync_remove_not_implemented_without_table_mutation(tmp_path):
    _assert_native_remove_method_present()
    counters: dict[str, int] = {"lookup": 0, "remove": 0}
    with _mock_remote_db(_lookup_success_handler(counters)) as remote_db:
        current = _observe_current(remote_db)
    assert counters["lookup"] == 1
    assert counters["remove"] == 0
    assert type(current) is lancedb.Function

    db = lancedb.connect(tmp_path)
    before = db.list_tables().tables
    assert before == []
    assert not hasattr(db, "remove_function")
    assert not hasattr(db, "remove_function_name")

    with pytest.raises(NotImplementedError):
        db.functions.remove(_REMOVE_CATALOG_NAME, current)

    assert db.list_tables().tables == before
    _close_db(db)


@pytest.mark.asyncio
async def test_local_async_remove_not_implemented_without_table_mutation(tmp_path):
    _assert_native_remove_method_present()
    counters: dict[str, int] = {"lookup": 0, "remove": 0}
    with _mock_remote_db(_lookup_success_handler(counters)) as remote_db:
        current = _observe_current(remote_db)
    assert counters["lookup"] == 1
    assert counters["remove"] == 0
    assert type(current) is lancedb.Function

    db = await lancedb.connect_async(tmp_path)
    before = (await db.list_tables()).tables
    assert before == []
    assert not hasattr(db, "remove_function")
    assert not hasattr(db, "remove_function_name")

    with pytest.raises(NotImplementedError):
        await db.functions.remove(_REMOVE_CATALOG_NAME, current)

    assert (await db.list_tables()).tables == before
    db.close()


@pytest.mark.parametrize("keyword", _DELETED_REMOVE_KEYWORDS)
def test_remove_rejects_deleted_cas_retry_version_kwargs(keyword):
    counters: dict[str, int] = {"lookup": 0, "remove": 0}

    with _mock_remote_db(_lookup_success_handler(counters)) as db:
        current = _observe_current(db)
        assert counters["lookup"] == 1
        assert counters["remove"] == 0
        with pytest.raises(TypeError):
            db.functions.remove(
                _REMOVE_CATALOG_NAME,
                current,
                **{keyword: True},
            )

    assert counters["lookup"] == 1
    assert counters["remove"] == 0


def test_no_direct_remove_methods_and_function_has_no_remove_facade_private():
    counters: dict[str, int] = {"lookup": 0, "remove": 0}

    with _mock_remote_db(_lookup_success_handler(counters)) as db:
        current = _observe_current(db)
        assert not hasattr(db, "remove_function")
        assert not hasattr(db, "remove_function_name")
        assert not hasattr(current, "remove")
        assert not hasattr(current, "delete")
        assert not hasattr(current, "revoke")
        assert callable(getattr(db.functions, "remove", None))
        assert not hasattr(lancedb, "_SyncFunctions")
        assert not hasattr(lancedb, "_AsyncFunctions")
        assert type(db.functions).__name__.startswith("_")
        assert "_SyncFunctions" not in getattr(lancedb, "__all__", [])
        assert "_AsyncFunctions" not in getattr(lancedb, "__all__", [])

    assert counters["lookup"] == 1
    assert counters["remove"] == 0
