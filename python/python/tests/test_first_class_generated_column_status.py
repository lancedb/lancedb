# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Contract tests for Python ``table.generated_column_status`` (B3d2).

Public user shape under test:

    status = table.generated_column_status("complete_col")  # "complete" | "incomplete"

These tests exercise the live worktree PyO3 extension and public sync/async
wrappers. While the public methods and hidden native bridge are absent they
fail against that extension; once present they freeze the public contract
below. They must not fake success paths.
"""

from __future__ import annotations

import contextlib
import http.server
import inspect
import json
import threading
from collections.abc import AsyncIterator, Iterator
from typing import Any, Callable, Literal, get_type_hints

import pytest

import lancedb
import lancedb.table
from lancedb import _lancedb as _native
from lancedb.remote.table import RemoteTable
from lancedb.table import AsyncTable, LanceTable, Table

_TABLE_NAME = "articles"
_DESCRIBE_PATH = f"/v1/table/{_TABLE_NAME}/describe/"

_ORDINARY_FIELD_ID = 1
_COMPLETE_FIELD_ID = 5
_INCOMPLETE_FIELD_ID = 7
_STABLE_FIELD_IDS = [_ORDINARY_FIELD_ID, _COMPLETE_FIELD_ID, _INCOMPLETE_FIELD_ID]

_STATUS_FUNCTION_ID = "fn.exact.status.projection"
_METADATA_KEY = "lancedb::generated_column"
_RAW_METADATA_MARKER = "SENSITIVE_STATUS_METADATA_MARKER_b3d2_py_9f2e"

# Pinned Rust-canonical schema-only Utf8 type IPC (base64), shared with FF-028.
_UTF8_TYPE_IPC_B64 = (
    "QVJST1cxAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP"
    "////94AAAAEAAAAAAACgAMAAoACQAEAAoAAAAQAAAAAAEEAAgACAAAAAQACAAAAAQAAAABAAAAFAAAABAAFAAQ"
    "AA4ADwAEAAAACAAQAAAAGAAAAAwAAAAAAAEFEAAAAAAAAAAEAAQABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/"
    "////wAAAAAQAAAADAAUABIADAAIAAQADAAAAGAAAABkAAAAEAAAAAAABAAIAAgAAAAEAAgAAAAEAAAAAQAAAB"
    "QAAAAQABQAEAAOAA8ABAAAAAgAEAAAABgAAAAMAAAAAAABBRAAAAAAAAAABAAEAAQAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAIAAAABBUlJPVzE="
)

_EXPECTED_RETURN = Literal["complete", "incomplete"]

_FORBIDDEN_PUBLIC_NAMES = (
    "GeneratedColumnStatus",
    "GeneratedColumnDefinition",
    "GeneratedColumnBindingSnapshot",
    "GeneratedColumnBindingEntry",
)

_FORBIDDEN_BRIDGE_KWARGS = (
    "epoch",
    "dependency_epoch",
    "materialized_epoch",
    "function_id",
    "field_id",
    "field_ids",
    "version",
    "branch",
    "wait",
    "job",
    "request",
    "backend",
)


def _definition_metadata_json(
    output_field_id: int,
    dependency_epoch: int,
    materialized_epoch: int,
    *,
    text_field_id: int = _ORDINARY_FIELD_ID,
) -> str:
    """Exact JSON stored under Arrow field metadata ``lancedb::generated_column``."""
    return json.dumps(
        {
            "format_version": 1,
            "output_field_id": output_field_id,
            "function_call": {
                "function_id": _STATUS_FUNCTION_ID,
                "arguments": [
                    {
                        "parameter": "text",
                        "value": {
                            "kind": "field",
                            "field_id": text_field_id,
                            "data_type_ipc": _UTF8_TYPE_IPC_B64,
                        },
                    }
                ],
            },
            "dependency_epoch": dependency_epoch,
            "materialized_epoch": materialized_epoch,
        },
        separators=(",", ":"),
    )


def _field(
    name: str,
    *,
    arrow_type: str = "string",
    nullable: bool = True,
    metadata: dict[str, str] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "name": name,
        "type": {"type": arrow_type},
        "nullable": nullable,
    }
    if metadata is not None:
        body["metadata"] = metadata
    return body


def _status_schema_fields(
    *,
    complete_meta: str | None = None,
    incomplete_meta: str | None = None,
    bad_name: str | None = None,
    bad_meta: str | None = None,
) -> dict[str, Any]:
    fields = [
        _field("ordinary", arrow_type="string"),
        _field(
            "complete_col",
            arrow_type="int32",
            metadata={
                _METADATA_KEY: complete_meta
                if complete_meta is not None
                else _definition_metadata_json(_COMPLETE_FIELD_ID, 3, 3)
            },
        ),
        _field(
            "incomplete_col",
            arrow_type="int32",
            metadata={
                _METADATA_KEY: incomplete_meta
                if incomplete_meta is not None
                else _definition_metadata_json(_INCOMPLETE_FIELD_ID, 4, 1)
            },
        ),
    ]
    if bad_name is not None and bad_meta is not None:
        fields.append(
            _field(
                bad_name,
                arrow_type="int32",
                metadata={_METADATA_KEY: bad_meta},
            )
        )
    return {"fields": fields}


def _describe_body(
    *,
    version: int = 11,
    field_ids: list[int] | None = _STABLE_FIELD_IDS,
    schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "version": version,
        "schema": schema if schema is not None else _status_schema_fields(),
    }
    if field_ids is not None:
        body["field_ids"] = field_ids
    return body


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


def _exception_text(exc: BaseException) -> str:
    parts = [str(exc), repr(exc)]
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return "\n".join(parts)


def _json_response(
    request: http.server.BaseHTTPRequestHandler, body: dict[str, Any]
) -> None:
    payload = json.dumps(body).encode("utf-8")
    request.send_response(200)
    request.send_header("Content-Type", "application/json")
    request.end_headers()
    request.wfile.write(payload)


class _RequestLog:
    """Track post-open describe and any non-describe operation traffic."""

    def __init__(self) -> None:
        self.describe: list[dict[str, Any]] = []
        self.other: list[str] = []
        self.recording = False

    def start(self) -> None:
        self.describe.clear()
        self.other.clear()
        self.recording = True

    def note(self, path: str, body: dict[str, Any] | None = None) -> None:
        if not self.recording:
            return
        if path == _DESCRIBE_PATH:
            self.describe.append(body or {})
        else:
            self.other.append(path)


def _assert_no_operation_traffic(log: _RequestLog) -> None:
    assert log.describe == []
    assert log.other == []


def _assert_one_status_describe(log: _RequestLog) -> None:
    assert len(log.describe) == 1, f"expected one status describe, got {log.describe!r}"
    assert log.other == [], f"unexpected non-describe traffic: {log.other!r}"


def _assert_exact_public_signature(method: Any) -> None:
    """Freeze ``(self, column_name)`` with no varargs/kwargs/keyword-only escape."""
    params = list(inspect.signature(method).parameters.values())
    assert [p.name for p in params] == ["self", "column_name"]
    for param in params:
        assert param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        assert param.default is inspect.Parameter.empty
        assert param.kind is not inspect.Parameter.VAR_POSITIONAL
        assert param.kind is not inspect.Parameter.VAR_KEYWORD
        assert param.kind is not inspect.Parameter.KEYWORD_ONLY


def _assert_status_string(value: Any, expected: str) -> None:
    assert value == expected
    assert type(value) is str
    assert value in ("complete", "incomplete")


def _open_remote_table(
    *,
    status_describe: dict[str, Any] | None = None,
):
    """Open sync RemoteTable; return (table, log, cm)."""
    log = _RequestLog()
    binding = status_describe if status_describe is not None else _describe_body()
    open_describe = {
        "version": 1,
        "schema": {"fields": [_field("ordinary", arrow_type="string")]},
    }
    state = {"opened": False}

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.command == "POST"
        raw = _read_body(request)
        body = json.loads(raw.decode("utf-8")) if raw else {}

        if request.path == _DESCRIBE_PATH:
            if not state["opened"]:
                state["opened"] = True
                _json_response(request, open_describe)
                return
            log.note(request.path, body)
            _json_response(request, binding)
            return

        log.note(request.path, body)
        request.send_response(404)
        request.end_headers()
        request.wfile.write(b"unexpected path")

    cm = _mock_remote_db(handler)
    db = cm.__enter__()
    table = db.open_table(_TABLE_NAME)
    assert isinstance(table, RemoteTable)
    log.start()
    return table, log, cm


async def _open_remote_table_async(
    *,
    status_describe: dict[str, Any] | None = None,
):
    """Open async table under a live mock server; return (table, log, cm)."""
    log = _RequestLog()
    binding = status_describe if status_describe is not None else _describe_body()
    open_describe = {
        "version": 1,
        "schema": {"fields": [_field("ordinary", arrow_type="string")]},
    }
    state = {"opened": False}

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.command == "POST"
        raw = _read_body(request)
        body = json.loads(raw.decode("utf-8")) if raw else {}

        if request.path == _DESCRIBE_PATH:
            if not state["opened"]:
                state["opened"] = True
                _json_response(request, open_describe)
                return
            log.note(request.path, body)
            _json_response(request, binding)
            return

        log.note(request.path, body)
        request.send_response(404)
        request.end_headers()
        request.wfile.write(b"unexpected path")

    cm = _mock_remote_db_async(handler)
    db = await cm.__aenter__()
    table = await db.open_table(_TABLE_NAME)
    assert isinstance(table, AsyncTable)
    log.start()
    return table, log, cm


def test_no_public_generated_column_status_resource_exported():
    """Baseline: no public status class/enum/resource is exported."""
    for mod in (lancedb, lancedb.table, _native):
        for name in _FORBIDDEN_PUBLIC_NAMES:
            assert not hasattr(mod, name), f"{mod.__name__}.{name} must not be public"


def test_public_surface_signatures_annotations_and_hidden_bridge():
    """Four public methods + hidden native bridge must exist with frozen shape."""
    assert hasattr(_native.Table, "_generated_column_status"), (
        "native private bridge Table._generated_column_status is missing"
    )
    assert hasattr(Table, "generated_column_status"), (
        "Table.generated_column_status is missing"
    )
    assert hasattr(LanceTable, "generated_column_status"), (
        "LanceTable.generated_column_status is missing"
    )
    assert hasattr(RemoteTable, "generated_column_status"), (
        "RemoteTable.generated_column_status is missing"
    )
    assert hasattr(AsyncTable, "generated_column_status"), (
        "AsyncTable.generated_column_status is missing"
    )

    bridge = _native.Table._generated_column_status
    _assert_exact_public_signature(bridge)
    for keyword in _FORBIDDEN_BRIDGE_KWARGS:
        assert keyword not in inspect.signature(bridge).parameters

    for method in (
        Table.generated_column_status,
        LanceTable.generated_column_status,
        RemoteTable.generated_column_status,
    ):
        _assert_exact_public_signature(method)
        assert not inspect.iscoroutinefunction(method)
        assert get_type_hints(method)["return"] == _EXPECTED_RETURN

    async_method = AsyncTable.generated_column_status
    _assert_exact_public_signature(async_method)
    assert inspect.iscoroutinefunction(async_method)
    assert get_type_hints(async_method)["return"] == _EXPECTED_RETURN


def test_sync_remote_complete_and_incomplete_one_describe_each():
    table, log, cm = _open_remote_table()
    try:
        complete = table.generated_column_status("complete_col")
        _assert_status_string(complete, "complete")
        _assert_one_status_describe(log)

        log.start()
        incomplete = table.generated_column_status("incomplete_col")
        _assert_status_string(incomplete, "incomplete")
        _assert_one_status_describe(log)
    finally:
        cm.__exit__(None, None, None)


@pytest.mark.asyncio
async def test_async_remote_complete_and_incomplete_one_describe_each():
    table, log, cm = await _open_remote_table_async()
    try:
        complete = await table.generated_column_status("complete_col")
        _assert_status_string(complete, "complete")
        _assert_one_status_describe(log)

        log.start()
        incomplete = await table.generated_column_status("incomplete_col")
        _assert_status_string(incomplete, "incomplete")
        _assert_one_status_describe(log)
    finally:
        await cm.__aexit__(None, None, None)


@pytest.mark.parametrize(
    ("column_name", "status_describe", "expected_exc"),
    [
        (
            "missing",
            _describe_body(),
            ValueError,
        ),
        (
            "Complete_Col",
            _describe_body(),
            ValueError,
        ),
        (
            "ordinary",
            _describe_body(),
            ValueError,
        ),
        (
            "complete_col",
            _describe_body(
                schema=_status_schema_fields(
                    complete_meta=_definition_metadata_json(
                        _COMPLETE_FIELD_ID + 1, 3, 3
                    )
                )
            ),
            ValueError,
        ),
        (
            "gen_bad",
            _describe_body(
                field_ids=[*_STABLE_FIELD_IDS, 9],
                schema=_status_schema_fields(
                    bad_name="gen_bad",
                    bad_meta=(
                        '{"format_version":1,"output_field_id":9,'
                        f'"function_call":{_RAW_METADATA_MARKER},'
                        '"dependency_epoch":1,"materialized_epoch":1}'
                    ),
                ),
            ),
            ValueError,
        ),
        (
            "complete_col",
            _describe_body(
                schema=_status_schema_fields(
                    complete_meta=_definition_metadata_json(
                        _COMPLETE_FIELD_ID, 1, 1
                    ).replace('"format_version":1', '"format_version":2')
                )
            ),
            ValueError,
        ),
        (
            "incomplete_col",
            _describe_body(
                schema=_status_schema_fields(
                    incomplete_meta=_definition_metadata_json(
                        _INCOMPLETE_FIELD_ID, 1, 2
                    )
                )
            ),
            ValueError,
        ),
        (
            "complete_col",
            _describe_body(field_ids=None),
            NotImplementedError,
        ),
    ],
    ids=[
        "missing",
        "case_mismatch",
        "ordinary",
        "output_id_mismatch",
        "malformed_metadata",
        "unknown_format_version",
        "reversed_epochs",
        "old_server_missing_field_ids",
    ],
)
def test_remote_fail_closed_matrix_one_describe(
    column_name: str,
    status_describe: dict[str, Any],
    expected_exc: type[BaseException],
):
    table, log, cm = _open_remote_table(status_describe=status_describe)
    try:
        with pytest.raises(expected_exc) as raised:
            table.generated_column_status(column_name)
        text = _exception_text(raised.value)
        assert _RAW_METADATA_MARKER not in text
        _assert_one_status_describe(log)
    finally:
        cm.__exit__(None, None, None)


def test_sync_empty_name_zero_post_open_requests():
    table, log, cm = _open_remote_table()
    try:
        with pytest.raises(ValueError):
            table.generated_column_status("")
        _assert_no_operation_traffic(log)
    finally:
        cm.__exit__(None, None, None)


@pytest.mark.asyncio
async def test_async_empty_name_zero_post_open_requests():
    table, log, cm = await _open_remote_table_async()
    try:
        with pytest.raises(ValueError):
            await table.generated_column_status("")
        _assert_no_operation_traffic(log)
    finally:
        await cm.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_async_closed_status_empty_validation_wins_and_nonempty_closed():
    """Publicly closed AsyncTable: empty validates first; nonempty is closed."""
    table, log, cm = await _open_remote_table_async()
    try:
        table.close()

        log.start()
        try:
            await table.generated_column_status("complete_col")
        except AttributeError:
            raise
        except Exception as exc:
            text = _exception_text(exc)
            assert "closed" in text.lower()
        else:
            pytest.fail("closed AsyncTable must fail before transport")
        _assert_no_operation_traffic(log)

        log.start()
        with pytest.raises(ValueError) as raised:
            await table.generated_column_status("")
        text = _exception_text(raised.value)
        assert "closed" not in text.lower()
        _assert_no_operation_traffic(log)
    finally:
        await cm.__aexit__(None, None, None)


def test_local_sync_ordinary_column_fails_without_side_effects(tmp_path):
    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "ordinary_only",
        [{"ordinary": "alpha", "id": 1}, {"ordinary": "beta", "id": 2}],
    )
    assert isinstance(table, LanceTable)
    version_before = table.version
    schema_before = table.schema
    data_before = table.to_arrow()

    with pytest.raises(ValueError):
        table.generated_column_status("ordinary")

    assert table.version == version_before
    assert table.schema == schema_before
    assert table.to_arrow().equals(data_before)


@pytest.mark.asyncio
async def test_local_async_ordinary_column_fails_without_side_effects(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    table = await db.create_table(
        "ordinary_only_async",
        [{"ordinary": "alpha", "id": 1}, {"ordinary": "beta", "id": 2}],
    )
    assert isinstance(table, AsyncTable)
    version_before = await table.version()
    schema_before = await table.schema()
    data_before = await table.to_arrow()

    with pytest.raises(ValueError):
        await table.generated_column_status("ordinary")

    assert await table.version() == version_before
    assert await table.schema() == schema_before
    assert (await table.to_arrow()).equals(data_before)
