# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Contract tests for Python exact Function handle call authoring (FF-028)."""

from __future__ import annotations

import contextlib
import http.server
import json
import threading
from collections.abc import Iterator
from typing import Any, Callable

import pyarrow as pa
import pytest

import lancedb
from lancedb import _lancedb as _native
from lancedb.expr import Expr, col, func, lit

_CALL_PATH = "/v1/functions/lookup"
_CALL_CATALOG_NAME = "text.normalize.call-name"
_CALL_FUNCTION_ID = "fn.exact.call-handle"
_LITERAL_PAYLOAD_SENTINEL = "LITERAL_PAYLOAD_SENTINEL_call_xyz_42"
_INT_PAYLOAD_SENTINEL = 2_147_000_123

# Pinned Rust-canonical schema-only type IPC (base64).
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
_LIST_INT32_TYPE_IPC_B64 = (
    "QVJST1cxAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP"
    "////+4AAAAEAAAAAAACgAMAAoACQAEAAoAAAAQAAAAAAEEAAgACAAAAAQACAAAAAQAAAABAAAABAAAANz///8c"
    "AAAADAAAAAAAAQxcAAAAAQAAABwAAAAEAAQABAAAABAAFAAQAA4ADwAEAAAACAAQAAAAGAAAACAAAAAAAAECH"
    "AAAAAgADAAEAAsACAAAACAAAAAAAAABAAAAAAQAAABpdGVtAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP"
    "////8AAAAAFAAAAAAAAAAMABQAEgAMAAgABAAMAAAAnAAAAKAAAAAQAAAAAAAEAAgACAAAAAQACAAAAAQAAAA"
    "BAAAABAAAANz///8cAAAADAAAAAAAAQxcAAAAAQAAABwAAAAEAAQABAAAABAAFAAQAA4ADwAEAAAACAAQAAAA"
    "GAAAACAAAAAAAAECHAAAAAgADAAEAAsACAAAACAAAAAAAAABAAAAAAQAAABpdGVtAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAwAAAAEFSUk9XMQ=="
)

_OVERDESIGN_ATTRS = (
    "id",
    "function_id",
    "name",
    "connection",
    "table",
    "snapshot",
    "field_id",
    "field_ids",
    "job",
    "job_id",
    "artifact",
    "digest",
    "retry_key",
    "idempotency_key",
    "user_version",
    "execute",
    "status",
    "wait",
    "cancel",
    "to_json",
    "_to_json",
    "serialize",
    "geneva",
)


def _sample_function_wire(
    *,
    function_id: str = _CALL_FUNCTION_ID,
    parameters: list[dict[str, str]] | None = None,
    output_type_ipc: str = _UTF8_TYPE_IPC_B64,
) -> dict[str, Any]:
    return {
        "format_version": 1,
        "id": function_id,
        "signature": {
            "parameters": parameters
            or [
                {"name": "text", "data_type_ipc": _UTF8_TYPE_IPC_B64},
                {"name": "limit", "data_type_ipc": _INT32_TYPE_IPC_B64},
            ],
            "output": {
                "data_type_ipc": output_type_ipc,
                "nullable": True,
            },
        },
    }


def _lookup_success_body(function: dict[str, Any] | None = None) -> bytes:
    return json.dumps({"function": function or _sample_function_wire()}).encode("utf-8")


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


def _lookup_function(function: dict[str, Any] | None = None):
    body = _lookup_success_body(function)

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.command == "POST"
        assert request.path == _CALL_PATH
        _read_body(request)
        request.send_response(200)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(body)

    with _mock_remote_db(handler) as db:
        return db.functions.get(_CALL_CATALOG_NAME)


def _authored_call_type():
    cls = getattr(_native, "_FunctionCall", None)
    if cls is None:
        pytest.fail("lancedb._lancedb._FunctionCall is missing")
    return cls


def _exception_text(exc: BaseException) -> str:
    parts = [str(exc), repr(exc)]
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return "\n".join(parts)


def test_function_keyword_call_returns_private_frozen_authored_value():
    function = _lookup_function()
    assert callable(function)

    authored = function(text=col("text"), limit=8)
    authored_type = _authored_call_type()
    assert type(authored) is authored_type
    assert authored_type.__module__ == "lancedb._lancedb"
    assert authored_type.__name__ == "_FunctionCall"

    # Keyword order must not matter; bindings store/render in signature order.
    authored_reversed = function(limit=8, text=col("text"))
    assert type(authored_reversed) is authored_type
    rendered = repr(authored_reversed)
    assert rendered.index("text=") < rendered.index("limit=")
    assert 'text=field("text")' in rendered
    assert "limit=literal(Int32, null=false)" in rendered


def test_function_call_rejects_positional_missing_and_unknown_args():
    function = _lookup_function()

    with pytest.raises(TypeError, match="keyword"):
        function(col("text"), 8)

    with pytest.raises((TypeError, ValueError), match="limit"):
        function(text=col("text"))

    with pytest.raises((TypeError, ValueError), match="text"):
        function(limit=8)

    with pytest.raises((TypeError, ValueError), match="unknown|extra"):
        function(text=col("text"), limit=8, extra=1)


def test_function_call_accepts_direct_case_sensitive_column_and_rejects_complex_exprs():
    function = _lookup_function()

    authored = function(text=col("firstName"), limit=1)
    assert type(authored) is _authored_call_type()
    rendered = repr(authored)
    assert 'text=field("firstName")' in rendered
    assert "limit=literal(Int32, null=false)" in rendered

    complex_exprs = (
        col("text") + lit("x"),
        col("text").cast(pa.string()),
        func("lower", col("text")),
        col("text") == lit("x"),
        col("text").lower(),
    )
    for expr in complex_exprs:
        with pytest.raises((TypeError, ValueError)):
            function(text=expr, limit=1)

    # Raw native PyExpr is not the public col() wrapper.
    with pytest.raises((TypeError, ValueError)):
        function(text=col("text")._inner, limit=1)

    # Non-expression / non-literal objects are rejected for field-shaped misuse
    # when a column binding is required; plain strings are literals for utf8.
    with pytest.raises((TypeError, ValueError)):
        function(text=object(), limit=1)


def test_function_call_plain_literal_declared_type_null_and_nested():
    function = _lookup_function()

    authored = function(text="hello", limit=7)
    assert type(authored) is _authored_call_type()
    rendered = repr(authored)
    assert "text=literal(Utf8, null=false)" in rendered
    assert "limit=literal(Int32, null=false)" in rendered

    # Plain Python int normalizes to declared Int32 and non-null.
    authored_int32 = function(text="hello", limit=2_147_483_647)
    assert type(authored_int32) is _authored_call_type()
    rendered_int32 = repr(authored_int32)
    assert "limit=literal(Int32, null=false)" in rendered_int32
    assert "Int64" not in rendered_int32
    assert "2147483647" not in rendered_int32

    # Plain None keeps each declared parameter type with null=true.
    authored_null = function(text=None, limit=None)
    assert type(authored_null) is _authored_call_type()
    rendered_null = repr(authored_null)
    assert "text=literal(Utf8, null=true)" in rendered_null
    assert "limit=literal(Int32, null=true)" in rendered_null

    list_function = _lookup_function(
        _sample_function_wire(
            parameters=[
                {"name": "values", "data_type_ipc": _LIST_INT32_TYPE_IPC_B64},
            ]
        )
    )
    authored_list = list_function(values=[1, 2, 3])
    assert type(authored_list) is _authored_call_type()
    rendered_list = repr(authored_list)
    assert "values=literal(List(Int32), null=false)" in rendered_list
    assert "[1, 2, 3]" not in rendered_list

    authored_list_null = list_function(values=None)
    assert type(authored_list_null) is _authored_call_type()
    rendered_list_null = repr(authored_list_null)
    assert "values=literal(List(Int32), null=true)" in rendered_list_null


def test_function_call_direct_literal_expr_exact_type_only():
    function = _lookup_function()

    # lit(int) is Int64 in the expression builder; int32 parameter must reject it.
    with pytest.raises((TypeError, ValueError), match="limit|int32|type") as raised:
        function(text="hello", limit=lit(8))
    reject_text = _exception_text(raised.value)
    assert "Int64" in reject_text or "int64" in reject_text.lower()
    assert "Int32" in reject_text or "int32" in reject_text.lower()

    # Exact utf8 literal expression is accepted and stored as Utf8/non-null.
    authored = function(text=lit("hello"), limit=8)
    assert type(authored) is _authored_call_type()
    rendered = repr(authored)
    assert "text=literal(Utf8, null=false)" in rendered
    assert "limit=literal(Int32, null=false)" in rendered
    assert "hello" not in rendered

    # Cast / arithmetic around a literal is not a direct Literal node.
    with pytest.raises((TypeError, ValueError)):
        function(text=lit("hello").cast(pa.string()), limit=8)


def test_function_call_conversion_error_and_repr_are_payload_free():
    function = _lookup_function()

    with pytest.raises((TypeError, ValueError)) as raised:
        function(text="ok", limit=_LITERAL_PAYLOAD_SENTINEL)
    text = _exception_text(raised.value)
    assert _LITERAL_PAYLOAD_SENTINEL not in text
    assert "limit" in text
    assert "int32" in text.lower() or "Int32" in text

    authored = function(text=_LITERAL_PAYLOAD_SENTINEL, limit=_INT_PAYLOAD_SENTINEL)
    rendered = f"{authored!r}\n{authored!s}"
    assert _LITERAL_PAYLOAD_SENTINEL not in rendered
    assert str(_INT_PAYLOAD_SENTINEL) not in rendered
    assert "text=literal(Utf8, null=false)" in rendered
    assert "limit=literal(Int32, null=false)" in rendered
    assert type(authored).__name__ == "_FunctionCall"
    assert "_FunctionCall" in rendered


def test_function_call_private_type_nonconstructible_immutable_and_not_exported():
    function = _lookup_function()
    authored = function(text=col("text"), limit=1)
    authored_type = _authored_call_type()

    assert "_FunctionCall" not in getattr(lancedb, "__all__", [])
    assert not hasattr(lancedb, "_FunctionCall")
    assert getattr(_native, "_FunctionCall", None) is authored_type

    with pytest.raises(TypeError):
        authored_type()

    for attr in _OVERDESIGN_ATTRS:
        assert not hasattr(authored, attr)

    for attr in ("function", "bindings", "arguments", "parameters", "text", "limit"):
        with pytest.raises(AttributeError):
            setattr(authored, attr, None)

    # Existing Function handle stays frozen / connection-free / name-free.
    assert not hasattr(function, "name")
    assert not hasattr(function, "connection")
    with pytest.raises(AttributeError):
        function.id = "mutated"


def test_function_call_does_not_change_col_query_expression_behavior():
    # Regression guard: authoring must not alter public col()/Expr query behavior.
    expr = col("firstName") > lit(1)
    assert isinstance(expr, Expr)
    assert expr.to_sql() == "(`firstName` > 1)"
