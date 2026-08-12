# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Contract tests for Python ``table.add_generated_column`` (FF-032).

Public user shape under test:

    job = table.add_generated_column(
        "normalized_text",
        normalize(text=col("text")),
    )
    job.wait()

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
from typing import Any, Callable

import pytest

import lancedb
import lancedb.job
from lancedb import _lancedb as _native
from lancedb.expr import col
from lancedb.remote.table import RemoteTable
from lancedb.table import AsyncTable, LanceTable, Table

_LOOKUP_PATH = "/v1/functions/lookup"
_JOB_DESCRIBE_PATH = "/v1/jobs/describe"
_TABLE_NAME = "articles"
_DESCRIBE_PATH = f"/v1/table/{_TABLE_NAME}/describe/"
_CREATE_PATH = f"/v1/table/{_TABLE_NAME}/generated_columns/create/"
_BRANCHES_CREATE_PATH = f"/v1/table/{_TABLE_NAME}/branches/create/"
_BRANCHES_LIST_PATH = f"/v1/table/{_TABLE_NAME}/branches/list/"

_CATALOG_NAME = "text.normalize"
_FUNCTION_ID = "fn.exact.normalize.gen-col"
_JOB_ID_SYNC = "job-create-gen-col-sync-1"
_JOB_ID_ASYNC = "job-create-gen-col-async-1"
_JOB_ID_BRANCH = "job-create-gen-col-branch-1"
_SOURCE_TABLE_VERSION = 42
_TEXT_FIELD_ID = 7
_BRANCH_NAME = "exp"
_BRANCH_SOURCE_VERSION = 9
_BRANCH_TEXT_FIELD_ID = 11

_DESCRIBE_BODY_MARKER = "SENSITIVE_DESCRIBE_BODY_MARKER_gen_col_xyz"
_CREATE_RESPONSE_MARKER = "SENSITIVE_CREATE_RESPONSE_MARKER_gen_col_xyz"
_LITERAL_PAYLOAD_SENTINEL = "LITERAL_PAYLOAD_SENTINEL_gen_col_xyz"

# Pinned Rust-canonical schema-only Utf8 type IPC (base64), shared with FF-028.
_UTF8_TYPE_IPC_B64 = (
    "QVJST1cxAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP"
    "////94AAAAEAAAAAAACgAMAAoACQAEAAoAAAAQAAAAAAEEAAgACAAAAAQACAAAAAQAAAABAAAAFAAAABAAFAAQ"
    "AA4ADwAEAAAACAAQAAAAGAAAAAwAAAAAAAEFEAAAAAAAAAAEAAQABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/"
    "////wAAAAAQAAAADAAUABIADAAIAAQADAAAAGAAAABkAAAAEAAAAAAABAAIAAgAAAAEAAgAAAAEAAAAAQAAAB"
    "QAAAAQABQAEAAOAA8ABAAAAAgAEAAAABgAAAAMAAAAAAABBRAAAAAAAAAABAAEAAQAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAIAAAABBUlJPVzE="
)

_FORBIDDEN_PUBLIC_NAMES = (
    "FunctionCall",
    "BoundFunctionCall",
    "AuthoredFunctionCall",
    "CreateGeneratedColumnRequest",
    "CreateGeneratedColumnJobSpec",
    "GeneratedColumnBindingSnapshot",
    "GeneratedColumnCreateRequest",
    "geneva",
    "GenevaFunction",
    "VirtualColumnDefinition",
)

_FORBIDDEN_METHOD_KWARGS = (
    "source_table_version",
    "version",
    "field_id",
    "field_ids",
    "output",
    "output_type",
    "output_nullable",
    "nullable",
    "spec",
    "retry_key",
    "idempotency_key",
    "request",
    "envelope",
    "table_ref",
    "branch",
)


def _sample_function_wire(
    *,
    function_id: str = _FUNCTION_ID,
    parameters: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    return {
        "format_version": 1,
        "id": function_id,
        "signature": {
            "parameters": parameters
            or [
                {"name": "text", "data_type_ipc": _UTF8_TYPE_IPC_B64},
            ],
            "output": {
                "data_type_ipc": _UTF8_TYPE_IPC_B64,
                "nullable": True,
            },
        },
    }


def _text_schema_fields(
    *, arrow_type: str = "string", nullable: bool = True
) -> dict[str, Any]:
    return {
        "fields": [
            {
                "name": "text",
                "type": {"type": arrow_type},
                "nullable": nullable,
            }
        ]
    }


def _describe_body(
    *,
    version: int = _SOURCE_TABLE_VERSION,
    field_ids: list[int] | None = None,
    arrow_type: str = "string",
    include_marker: bool = True,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "version": version,
        "schema": _text_schema_fields(arrow_type=arrow_type),
        "field_ids": field_ids if field_ids is not None else [_TEXT_FIELD_ID],
    }
    if include_marker:
        body["server_diagnostic"] = _DESCRIBE_BODY_MARKER
    return body


def _create_gen_column_done_body(job_id: str) -> dict[str, Any]:
    # DONE with omitted result: create_gen_column projects JobResult::None.
    return {
        "job_id": job_id,
        "job_state": "DONE",
        "job_type": "create_gen_column",
        "creation_ms": 1,
        "spec": {},
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


def _lookup_function(db: Any) -> lancedb.Function:
    return db.functions.get(_CATALOG_NAME)


class _RequestLog:
    """Track lookup/describe/create after setup; setup traffic is excluded."""

    def __init__(self) -> None:
        self.lookup: list[dict[str, Any]] = []
        self.describe: list[dict[str, Any]] = []
        self.create: list[dict[str, Any]] = []
        self.other_table: list[str] = []
        self.recording = False

    def start(self) -> None:
        # Drop setup's explicit Function lookup and open_table describe so
        # operation accounting cannot be polluted by fixture traffic.
        self.lookup.clear()
        self.describe.clear()
        self.create.clear()
        self.other_table.clear()
        self.recording = True

    def note(self, path: str, body: dict[str, Any] | None = None) -> None:
        if not self.recording:
            return
        if path == _LOOKUP_PATH:
            self.lookup.append(body or {})
        elif path == _DESCRIBE_PATH:
            self.describe.append(body or {})
        elif path == _CREATE_PATH:
            self.create.append(body or {})
        elif path.startswith(f"/v1/table/{_TABLE_NAME}/"):
            self.other_table.append(path)


def _assert_no_operation_traffic(log: _RequestLog) -> None:
    assert log.lookup == []
    assert log.describe == []
    assert log.create == []
    assert log.other_table == []


def _assert_exact_public_signature(method: Any) -> None:
    """Freeze ``(self, column_name, call)`` with no varargs/kwargs escape hatches."""
    params = list(inspect.signature(method).parameters.values())
    assert [p.name for p in params] == ["self", "column_name", "call"]
    for param in params:
        assert param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        assert param.default is inspect.Parameter.empty
        assert param.kind is not inspect.Parameter.VAR_POSITIONAL
        assert param.kind is not inspect.Parameter.VAR_KEYWORD
        assert param.kind is not inspect.Parameter.KEYWORD_ONLY


def _open_table_and_function(
    *,
    describe_body: dict[str, Any] | None = None,
    on_create: Callable[[dict[str, Any], http.server.BaseHTTPRequestHandler], None]
    | None = None,
    job_id: str = _JOB_ID_SYNC,
    support_branch_create: bool = False,
    function_wire: dict[str, Any] | None = None,
):
    """Open remote table + immutable Function; return (db, table, function, log, cm)."""
    log = _RequestLog()
    binding_describe = describe_body or _describe_body()
    open_describe = {
        "version": 1,
        "schema": _text_schema_fields(),
    }
    state = {"opened": False}
    wire = function_wire or _sample_function_wire()

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.command == "POST"
        raw = _read_body(request)
        body = json.loads(raw.decode("utf-8")) if raw else {}

        if request.path == _LOOKUP_PATH:
            log.note(request.path, body)
            _json_response(request, {"function": wire})
            return

        if request.path == _JOB_DESCRIBE_PATH:
            assert body["job_id"] == job_id
            _json_response(request, _create_gen_column_done_body(job_id))
            return

        if support_branch_create and request.path == _BRANCHES_CREATE_PATH:
            log.note(request.path, body)
            _json_response(request, {})
            return

        if support_branch_create and request.path == _BRANCHES_LIST_PATH:
            log.note(request.path, body)
            _json_response(
                request,
                {
                    "branches": {
                        _BRANCH_NAME: {
                            "parentBranch": None,
                            "parentVersion": 1,
                            "createAt": 1,
                            "manifestSize": 1,
                        }
                    }
                },
            )
            return

        if request.path == _DESCRIBE_PATH:
            # First describe seeds open_table; later ones are binding snapshots.
            if not state["opened"]:
                state["opened"] = True
                _json_response(request, open_describe)
                return
            log.note(request.path, body)
            _json_response(request, binding_describe)
            return

        if request.path == _CREATE_PATH:
            log.note(request.path, body)
            if on_create is not None:
                on_create(body, request)
                return
            _json_response(
                request,
                {
                    "job_id": job_id,
                    "server_extra": {"marker": _CREATE_RESPONSE_MARKER},
                },
            )
            return

        if request.path.startswith(f"/v1/table/{_TABLE_NAME}/"):
            log.note(request.path, body)
        request.send_response(404)
        request.end_headers()
        request.wfile.write(b"unexpected path")

    cm = _mock_remote_db(handler)
    db = cm.__enter__()
    function = _lookup_function(db)
    table = db.open_table(_TABLE_NAME)
    assert isinstance(table, RemoteTable)
    # open_table consumed the seed describe; binding/create accounting starts now.
    # Setup's one explicit lookup is cleared here and must not pollute counts.
    log.start()
    return db, table, function, log, cm


def _assert_exact_create_envelope(
    body: dict[str, Any],
    *,
    source_table_version: int,
    column_name: str,
    field_id: int,
    branch: str | None = None,
) -> None:
    expected_keys = {"source_table_version", "spec"}
    if branch is not None:
        expected_keys.add("branch")
    assert set(body) == expected_keys
    assert body["source_table_version"] == source_table_version
    assert "table_ref" not in body
    if branch is None:
        assert "branch" not in body
    else:
        assert body["branch"] == branch

    spec = body["spec"]
    assert set(spec) == {"format_version", "column_name", "function_call"}
    assert spec["format_version"] == 1
    assert spec["column_name"] == column_name
    for forbidden in (
        "table_ref",
        "source_table_version",
        "version",
        "output",
        "output_type",
        "output_field_id",
        "dependency_epoch",
        "materialized_epoch",
        "idempotency_key",
        "retry_key",
        "name",
        "handle",
        "artifact",
        "geneva",
    ):
        assert forbidden not in spec

    call = spec["function_call"]
    assert set(call) == {"function_id", "arguments"}
    assert call["function_id"] == _FUNCTION_ID
    assert len(call["arguments"]) == 1
    binding = call["arguments"][0]
    assert binding["parameter"] == "text"
    value = binding["value"]
    assert value["kind"] == "field"
    assert value["field_id"] == field_id
    assert value["data_type_ipc"] == _UTF8_TYPE_IPC_B64
    assert "name" not in value
    assert "column_name" not in value
    assert "text" not in value
    # Serialized call must not late-bind by column name anywhere relevant.
    dumped = json.dumps(call)
    assert '"column_name"' not in dumped
    assert "normalized_text" not in dumped


def test_public_and_native_add_generated_column_seams_must_exist():
    """Public sync/async methods and the private native bridge must exist."""
    assert hasattr(_native.Table, "_add_generated_column"), (
        "native private bridge Table._add_generated_column is missing"
    )
    assert hasattr(AsyncTable, "add_generated_column"), (
        "AsyncTable.add_generated_column is missing"
    )
    assert hasattr(Table, "add_generated_column"), (
        "Table.add_generated_column is missing"
    )
    assert hasattr(LanceTable, "add_generated_column"), (
        "LanceTable.add_generated_column is missing"
    )
    assert hasattr(RemoteTable, "add_generated_column"), (
        "RemoteTable.add_generated_column is missing"
    )

    # Once present, freeze the exact public positional surface.
    _assert_exact_public_signature(Table.add_generated_column)
    _assert_exact_public_signature(LanceTable.add_generated_column)
    _assert_exact_public_signature(RemoteTable.add_generated_column)
    _assert_exact_public_signature(AsyncTable.add_generated_column)


def test_sync_remote_add_generated_column_returns_job_without_eager_wrapper_mutation():
    db, table, normalize, log, cm = _open_table_and_function(job_id=_JOB_ID_SYNC)
    try:
        # Capture public wrapper state before the operation window.
        schema_before = table.schema
        version_before = table.version
        log.start()

        call = normalize(text=col("text"))
        # Exact public argument order from the frozen user example.
        job = table.add_generated_column(
            "normalized_text",
            call,
        )
        assert type(job) is lancedb.job.Job
        assert job.id == _JOB_ID_SYNC

        # Exact success path stops after submit: one binding describe, one create,
        # and no catalog re-lookup. Do not wait yet.
        assert len(log.lookup) == 0
        assert len(log.describe) == 1
        assert len(log.create) == 1
        assert log.other_table == []

        # Public schema/version through the existing wrapper must still reflect
        # the pre-submit table: generated column is not published by Job accept.
        # Access both before wait so eager wrapper cache invalidation / refresh /
        # version advancement is observable.
        schema_after = table.schema
        assert "normalized_text" not in schema_after.names
        assert schema_after == schema_before
        # Schema must be served from the existing wrapper cache — no extra
        # describe beyond the one binding snapshot.
        assert len(log.describe) == 1
        assert len(log.lookup) == 0
        assert len(log.create) == 1

        version_after = table.version
        assert version_after == version_before
        # Public Remote ``version`` always describes once by design; that probe
        # must not drag a schema-cache miss, create, or catalog lookup with it.
        assert len(log.describe) == 2
        assert len(log.lookup) == 0
        assert len(log.create) == 1
        assert log.other_table == []

        waited = job.wait()
        assert waited is None
        assert len(log.lookup) == 0
        assert len(log.create) == 1
    finally:
        cm.__exit__(None, None, None)


@pytest.mark.asyncio
async def test_async_remote_add_generated_column_returns_async_job_and_wait_none():
    log = _RequestLog()
    state = {"opened": False}
    binding_describe = _describe_body()
    open_describe = {"version": 1, "schema": _text_schema_fields()}

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.command == "POST"
        raw = _read_body(request)
        body = json.loads(raw.decode("utf-8")) if raw else {}

        if request.path == _LOOKUP_PATH:
            log.note(request.path, body)
            _json_response(request, {"function": _sample_function_wire()})
            return
        if request.path == _JOB_DESCRIBE_PATH:
            assert body["job_id"] == _JOB_ID_ASYNC
            _json_response(request, _create_gen_column_done_body(_JOB_ID_ASYNC))
            return
        if request.path == _DESCRIBE_PATH:
            if not state["opened"]:
                state["opened"] = True
                _json_response(request, open_describe)
                return
            log.note(request.path, body)
            _json_response(request, binding_describe)
            return
        if request.path == _CREATE_PATH:
            log.note(request.path, body)
            _json_response(request, {"job_id": _JOB_ID_ASYNC})
            return
        request.send_response(404)
        request.end_headers()

    async with _mock_remote_db_async(handler) as db:
        normalize = await db.functions.get(_CATALOG_NAME)
        table = await db.open_table(_TABLE_NAME)
        log.start()
        call = normalize(text=col("text"))
        job = await table.add_generated_column("normalized_text", call)
        assert type(job) is lancedb.job.AsyncJob
        assert job.id == _JOB_ID_ASYNC
        assert len(log.lookup) == 0
        assert len(log.describe) == 1
        assert len(log.create) == 1
        waited = await job.wait()
        assert waited is None
        assert len(log.lookup) == 0
        assert len(log.create) == 1


def test_remote_add_generated_column_one_describe_one_create_exact_envelope():
    db, table, normalize, log, cm = _open_table_and_function(job_id=_JOB_ID_SYNC)
    try:
        call = normalize(text=col("text"))
        job = table.add_generated_column("normalized_text", call)
        assert type(job) is lancedb.job.Job
        assert job.id == _JOB_ID_SYNC

        assert len(log.lookup) == 0
        assert len(log.describe) == 1
        assert len(log.create) == 1
        assert log.other_table == []
        _assert_exact_create_envelope(
            log.create[0],
            source_table_version=_SOURCE_TABLE_VERSION,
            column_name="normalized_text",
            field_id=_TEXT_FIELD_ID,
        )
    finally:
        cm.__exit__(None, None, None)


def test_remote_branch_add_generated_column_includes_exact_branch_identity():
    branch_describe = _describe_body(
        version=_BRANCH_SOURCE_VERSION,
        field_ids=[_BRANCH_TEXT_FIELD_ID],
    )
    db, table, normalize, log, cm = _open_table_and_function(
        describe_body=branch_describe,
        job_id=_JOB_ID_BRANCH,
        support_branch_create=True,
    )
    try:
        branched = table.branches.create(_BRANCH_NAME)
        assert isinstance(branched, RemoteTable)
        assert branched.current_branch() == _BRANCH_NAME
        log.start()

        call = normalize(text=col("text"))
        job = branched.add_generated_column("normalized_text", call)
        assert type(job) is lancedb.job.Job
        assert job.id == _JOB_ID_BRANCH
        assert len(log.lookup) == 0
        assert len(log.describe) == 1
        assert len(log.create) == 1
        assert log.describe[0].get("branch") == _BRANCH_NAME
        _assert_exact_create_envelope(
            log.create[0],
            source_table_version=_BRANCH_SOURCE_VERSION,
            column_name="normalized_text",
            field_id=_BRANCH_TEXT_FIELD_ID,
            branch=_BRANCH_NAME,
        )
    finally:
        cm.__exit__(None, None, None)


def test_empty_column_name_fails_locally_with_zero_table_requests():
    db, table, normalize, log, cm = _open_table_and_function()
    try:
        # Authored call owns a real literal so payload-free failure is not vacuous.
        call = normalize(text=_LITERAL_PAYLOAD_SENTINEL)
        with pytest.raises((ValueError, TypeError)) as raised:
            table.add_generated_column("", call)
        text = _exception_text(raised.value)
        lowered = text.lower()
        assert "column" in lowered or "empty" in lowered or "non-empty" in lowered
        assert _DESCRIBE_BODY_MARKER not in text
        assert _CREATE_RESPONSE_MARKER not in text
        assert _LITERAL_PAYLOAD_SENTINEL not in text
        _assert_no_operation_traffic(log)
    finally:
        cm.__exit__(None, None, None)


@pytest.mark.parametrize(
    ("column_ref", "expected_token"),
    [
        ("missing_text", "missing_text"),
        ("Text", "Text"),  # exact-case mismatch against schema field "text"
    ],
)
def test_missing_or_case_mismatch_column_one_describe_zero_create(
    column_ref: str, expected_token: str
):
    db, table, normalize, log, cm = _open_table_and_function()
    try:
        call = normalize(text=col(column_ref))
        with pytest.raises(ValueError) as raised:
            table.add_generated_column("normalized_text", call)
        text = _exception_text(raised.value)
        assert expected_token in text
        assert "text" in text  # parameter name from the Function signature
        assert "missing" in text.lower() or "field" in text.lower()
        assert _DESCRIBE_BODY_MARKER not in text
        assert _CREATE_RESPONSE_MARKER not in text
        assert len(log.lookup) == 0
        assert len(log.describe) == 1
        assert log.create == []
        assert log.other_table == []
    finally:
        cm.__exit__(None, None, None)


def test_type_mismatch_one_describe_zero_create_identifies_parameter():
    db, table, normalize, log, cm = _open_table_and_function(
        describe_body=_describe_body(arrow_type="int32"),
    )
    try:
        call = normalize(text=col("text"))
        with pytest.raises(ValueError) as raised:
            table.add_generated_column("normalized_text", call)
        text = _exception_text(raised.value)
        assert "text" in text
        assert "type" in text.lower() or "mismatch" in text.lower()
        assert _DESCRIBE_BODY_MARKER not in text
        assert _CREATE_RESPONSE_MARKER not in text
        assert len(log.lookup) == 0
        assert len(log.describe) == 1
        assert log.create == []
        assert log.other_table == []
    finally:
        cm.__exit__(None, None, None)


def test_literal_payload_stays_out_of_field_binding_failure():
    """Authored call owns a real literal; later field binding fails payload-free."""
    wire = _sample_function_wire(
        parameters=[
            {"name": "text", "data_type_ipc": _UTF8_TYPE_IPC_B64},
            {"name": "prefix", "data_type_ipc": _UTF8_TYPE_IPC_B64},
        ]
    )
    db, table, normalize, log, cm = _open_table_and_function(function_wire=wire)
    try:
        call = normalize(text=col("missing_text"), prefix=_LITERAL_PAYLOAD_SENTINEL)
        with pytest.raises(ValueError) as raised:
            table.add_generated_column("normalized_text", call)
        text = _exception_text(raised.value)
        assert "missing_text" in text
        assert _LITERAL_PAYLOAD_SENTINEL not in text
        assert _DESCRIBE_BODY_MARKER not in text
        assert _CREATE_RESPONSE_MARKER not in text
        assert len(log.lookup) == 0
        assert len(log.describe) == 1
        assert log.create == []
        assert log.other_table == []
    finally:
        cm.__exit__(None, None, None)


@pytest.mark.asyncio
async def test_closed_async_table_fails_with_zero_operation_requests():
    log = _RequestLog()
    state = {"opened": False}
    binding_describe = _describe_body()
    open_describe = {"version": 1, "schema": _text_schema_fields()}

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.command == "POST"
        raw = _read_body(request)
        body = json.loads(raw.decode("utf-8")) if raw else {}

        if request.path == _LOOKUP_PATH:
            log.note(request.path, body)
            _json_response(request, {"function": _sample_function_wire()})
            return
        if request.path == _DESCRIBE_PATH:
            if not state["opened"]:
                state["opened"] = True
                _json_response(request, open_describe)
                return
            log.note(request.path, body)
            _json_response(request, binding_describe)
            return
        if request.path == _CREATE_PATH:
            log.note(request.path, body)
            _json_response(request, {"job_id": _JOB_ID_ASYNC})
            return
        request.send_response(404)
        request.end_headers()

    async with _mock_remote_db_async(handler) as db:
        normalize = await db.functions.get(_CATALOG_NAME)
        table = await db.open_table(_TABLE_NAME)
        call = normalize(text=col("text"))
        # Public close only — do not mutate private implementation fields.
        table.close()
        log.start()
        try:
            await table.add_generated_column("normalized_text", call)
        except AttributeError:
            # Method missing: re-raise so the failure names the public seam.
            raise
        except Exception as exc:
            text = _exception_text(exc)
            assert "closed" in text.lower()
        else:
            pytest.fail("closed AsyncTable must fail before transport")
        _assert_no_operation_traffic(log)


def test_rejects_non_authored_call_before_any_operation_request():
    db, table, normalize, log, cm = _open_table_and_function()
    try:
        bad_values = (
            normalize,  # exact Function handle itself
            {"text": "x"},
            col("text"),  # direct query Expr
            object(),
        )
        for bad in bad_values:
            with pytest.raises(TypeError):
                table.add_generated_column("normalized_text", bad)
        _assert_no_operation_traffic(log)
    finally:
        cm.__exit__(None, None, None)


def test_native_valid_call_returns_not_supported_without_mutation(tmp_path):
    # Immutable Function handle is connection-free; obtain it via remote lookup.
    def lookup_only(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.path == _LOOKUP_PATH
        _read_body(request)
        _json_response(request, {"function": _sample_function_wire()})

    with _mock_remote_db(lookup_only) as remote_db:
        normalize = _lookup_function(remote_db)

    db = lancedb.connect(tmp_path)
    table = db.create_table(_TABLE_NAME, [{"text": "Hello"}, {"text": "World"}])
    assert isinstance(table, LanceTable)
    version_before = table.version
    schema_before = table.schema
    rows_before = table.to_arrow().to_pylist()
    call = normalize(text=col("text"))

    with pytest.raises(NotImplementedError) as raised:
        table.add_generated_column("normalized_text", call)
    text = _exception_text(raised.value)
    assert "not supported" in text.lower() or "submit_create_generated_column" in text
    assert "add_columns" not in text.lower()

    assert table.version == version_before
    assert table.schema == schema_before
    assert "normalized_text" not in table.schema.names
    assert table.to_arrow().to_pylist() == rows_before


def test_public_surface_is_minimal_and_private_call_stays_opaque():
    for name in _FORBIDDEN_PUBLIC_NAMES:
        assert name not in getattr(lancedb, "__all__", [])
        assert not hasattr(lancedb, name)

    assert not hasattr(lancedb, "_FunctionCall")
    authored_type = getattr(_native, "_FunctionCall", None)
    assert authored_type is not None
    with pytest.raises(TypeError):
        authored_type()

    # When the public method exists, reject overdesign kwargs and keep the frozen
    # positional surface: (self, column_name, call).
    if hasattr(Table, "add_generated_column"):
        _assert_exact_public_signature(Table.add_generated_column)
        for keyword in _FORBIDDEN_METHOD_KWARGS:
            assert (
                keyword not in inspect.signature(Table.add_generated_column).parameters
            )
        db, table, normalize, log, cm = _open_table_and_function()
        try:
            call = normalize(text=col("text"))
            for keyword in _FORBIDDEN_METHOD_KWARGS:
                with pytest.raises(TypeError):
                    table.add_generated_column(
                        "normalized_text",
                        call,
                        **{keyword: object()},
                    )
            _assert_no_operation_traffic(log)
        finally:
            cm.__exit__(None, None, None)

    if hasattr(LanceTable, "add_generated_column"):
        _assert_exact_public_signature(LanceTable.add_generated_column)
    if hasattr(RemoteTable, "add_generated_column"):
        _assert_exact_public_signature(RemoteTable.add_generated_column)
    if hasattr(AsyncTable, "add_generated_column"):
        _assert_exact_public_signature(AsyncTable.add_generated_column)
        for keyword in _FORBIDDEN_METHOD_KWARGS:
            assert (
                keyword
                not in inspect.signature(AsyncTable.add_generated_column).parameters
            )
