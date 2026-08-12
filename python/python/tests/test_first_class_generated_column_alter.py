# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Contract tests for Python ``table.alter_generated_column`` (FF-011 authoring).

Public user shape under test:

    job = table.alter_generated_column(
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
_CHANGE_PATH = f"/v1/table/{_TABLE_NAME}/generated_columns/change/"
_BRANCHES_CREATE_PATH = f"/v1/table/{_TABLE_NAME}/branches/create/"
_BRANCHES_LIST_PATH = f"/v1/table/{_TABLE_NAME}/branches/list/"

_CATALOG_NAME = "text.normalize"
# Stored old definition uses a different ID the mock never resolves.
_OLD_FUNCTION_ID = "fn.exact.alter.gen-col.old"
_NEW_FUNCTION_ID = "fn.exact.alter.gen-col.new"
_JOB_ID_SYNC = "job-alter-gen-col-sync-1"
_JOB_ID_ASYNC = "job-alter-gen-col-async-1"
_JOB_ID_BRANCH = "job-alter-gen-col-branch-1"
_SOURCE_TABLE_VERSION = 42
_TEXT_FIELD_ID = 7
_OUTPUT_FIELD_ID = 17
_ORDINARY_FIELD_ID = 1
_BRANCH_NAME = "exp"
_BRANCH_SOURCE_VERSION = 9
_BRANCH_TEXT_FIELD_ID = 11
_BRANCH_OUTPUT_FIELD_ID = 19
_COLUMN_NAME = "normalized_text"
_METADATA_KEY = "lancedb::generated_column"

_DESCRIBE_BODY_MARKER = "SENSITIVE_ALTER_DESCRIBE_BODY_MARKER_gen_col_xyz"
_LOOKUP_RESPONSE_MARKER = "SENSITIVE_ALTER_LOOKUP_RESPONSE_MARKER_gen_col_xyz"
_CHANGE_RESPONSE_MARKER = "SENSITIVE_ALTER_CHANGE_RESPONSE_MARKER_gen_col_xyz"
_RAW_METADATA_MARKER = "SENSITIVE_ALTER_METADATA_MARKER_b2_py_9f2e"
_LITERAL_PAYLOAD_SENTINEL = "LITERAL_PAYLOAD_SENTINEL_alter_gen_col_xyz"

# Pinned Rust-canonical schema-only Utf8 / Int32 type IPC (base64).
_UTF8_TYPE_IPC_B64 = (
    "QVJST1cxAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP"
    "////94AAAAEAAAAAAACgAMAAoACQAEAAoAAAAQAAAAAAEEAAgACAAAAAQACAAAAAQAAAABAAAAFAAAABAAFAAQ"
    "AA4ADwAEAAAACAAQAAAAGAAAAAwAAAAAAAEFEAAAAAAAAAAEAAQABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/"
    "////wAAAAAQAAAADAAUABIADAAIAAQADAAAAGAAAABkAAAAEAAAAAAABAAIAAgAAAAEAAgAAAAEAAAAAQAAAB"
    "QAAAAQABQAEAAOAA8ABAAAAAgAEAAAABgAAAAMAAAAAAABBRAAAAAAAAAABAAEAAQAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAIAAAABBUlJPVzE="
)
_INT32_TYPE_IPC_B64 = (
    "QVJST1cxAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP"
    "////94AAAAEAAAAAAACgAMAAoACQAEAAoAAAAQAAAAAAEEAAgACAAAAAQACAAAAAQAAAABAAAAFAAAABAAFAAQ"
    "AA4ADwAEAAAACAAQAAAAGAAAACAAAAAAAAECHAAAAAgADAAEAAsACAAAACAAAAAAAAABAAAAAAAAAAAAAAAA/"
    "////wAAAAAUAAAAAAAAAAwAFAASAAwACAAEAAwAAABsAAAAcAAAABAAAAAAAAQACAAIAAAABAAIAAAABAAAAA"
    "EAAAAUAAAAEAAUABAADgAPAAQAAAAIABAAAAAYAAAAIAAAAAAAAQIcAAAACAAMAAQACwAIAAAAIAAAAAAAAAE"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACQAAAAQVJST1cx"
)

_FORBIDDEN_PUBLIC_NAMES = (
    "ChangeGeneratedColumnJobSpec",
    "ChangeGeneratedColumnRequest",
    "GeneratedColumnDefinition",
    "GeneratedColumnBindingSnapshot",
    "FunctionCall",
    "BoundFunctionCall",
    "AuthoredFunctionCall",
    "geneva",
    "GenevaFunction",
)

_FORBIDDEN_METHOD_KWARGS = (
    "source_table_version",
    "version",
    "field_id",
    "field_ids",
    "function",
    "function_id",
    "old_function",
    "new_function",
    "call",
    "spec",
    "definition",
    "expected_definition",
    "dependency_epoch",
    "materialized_epoch",
    "output",
    "output_type",
    "output_field_id",
    "new_output_field_id",
    "mode",
    "retry_key",
    "idempotency_key",
    "request",
    "envelope",
    "table_ref",
    "branch",
)


def _sample_function_wire(
    *,
    function_id: str = _NEW_FUNCTION_ID,
    parameters: list[dict[str, str]] | None = None,
    output_type_ipc: str = _INT32_TYPE_IPC_B64,
    output_nullable: bool = False,
) -> dict[str, Any]:
    """New Function wire: Int32 non-null by default (type-changing vs old Utf8)."""
    return {
        "format_version": 1,
        "id": function_id,
        "signature": {
            "parameters": parameters
            or [
                {"name": "text", "data_type_ipc": _UTF8_TYPE_IPC_B64},
            ],
            "output": {
                "data_type_ipc": output_type_ipc,
                "nullable": output_nullable,
            },
        },
    }


def _definition_metadata_json(
    output_field_id: int,
    dependency_epoch: int,
    materialized_epoch: int,
    *,
    text_field_id: int = _TEXT_FIELD_ID,
    text_type_ipc: str = _UTF8_TYPE_IPC_B64,
    function_id: str = _OLD_FUNCTION_ID,
) -> str:
    """Exact JSON stored under Arrow field metadata ``lancedb::generated_column``."""
    return json.dumps(
        {
            "format_version": 1,
            "output_field_id": output_field_id,
            "function_call": {
                "function_id": function_id,
                "arguments": [
                    {
                        "parameter": "text",
                        "value": {
                            "kind": "field",
                            "field_id": text_field_id,
                            "data_type_ipc": text_type_ipc,
                        },
                    }
                ],
            },
            "dependency_epoch": dependency_epoch,
            "materialized_epoch": materialized_epoch,
        },
        separators=(",", ":"),
    )


def _definition_object_from_metadata(meta: str) -> dict[str, Any]:
    return json.loads(meta)


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


def _alter_schema_fields(
    *,
    generated_meta: str | None = None,
    text_arrow_type: str = "string",
    include_ordinary: bool = True,
) -> dict[str, Any]:
    fields: list[dict[str, Any]] = []
    if include_ordinary:
        fields.append(_field("ordinary", arrow_type="string"))
    fields.append(_field("text", arrow_type=text_arrow_type))
    fields.append(
        _field(
            _COLUMN_NAME,
            arrow_type="string",
            metadata={
                _METADATA_KEY: generated_meta
                if generated_meta is not None
                else _definition_metadata_json(_OUTPUT_FIELD_ID, 4, 1)
            },
        )
    )
    return {"fields": fields}


def _field_ids(*, include_ordinary: bool = True) -> list[int]:
    if include_ordinary:
        return [_ORDINARY_FIELD_ID, _TEXT_FIELD_ID, _OUTPUT_FIELD_ID]
    return [_TEXT_FIELD_ID, _OUTPUT_FIELD_ID]


def _describe_body(
    *,
    version: int = _SOURCE_TABLE_VERSION,
    field_ids: list[int] | None = None,
    schema: dict[str, Any] | None = None,
    generated_meta: str | None = None,
    text_arrow_type: str = "string",
    include_marker: bool = True,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "version": version,
        "schema": schema
        if schema is not None
        else _alter_schema_fields(
            generated_meta=generated_meta, text_arrow_type=text_arrow_type
        ),
        "field_ids": field_ids if field_ids is not None else _field_ids(),
    }
    if include_marker:
        body["server_diagnostic"] = _DESCRIBE_BODY_MARKER
    return body


def _alter_job_done_body(job_id: str) -> dict[str, Any]:
    # DONE with omitted result: use a currently-known no-result job_type so wait
    # projects None. Typed change job_type vocabulary is outside this authoring
    # seam.
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
    """Track lookup/describe/change after setup; setup traffic is excluded."""

    def __init__(self) -> None:
        self.lookup: list[dict[str, Any]] = []
        self.describe: list[dict[str, Any]] = []
        self.change: list[dict[str, Any]] = []
        self.other_table: list[str] = []
        self.recording = False

    def start(self) -> None:
        # Drop setup's explicit Function lookup and open_table describe so
        # operation accounting cannot be polluted by fixture traffic.
        self.lookup.clear()
        self.describe.clear()
        self.change.clear()
        self.other_table.clear()
        self.recording = True

    def note(self, path: str, body: dict[str, Any] | None = None) -> None:
        if not self.recording:
            return
        if path == _LOOKUP_PATH:
            self.lookup.append(body or {})
        elif path == _DESCRIBE_PATH:
            self.describe.append(body or {})
        elif path == _CHANGE_PATH:
            self.change.append(body or {})
        elif path.startswith(f"/v1/table/{_TABLE_NAME}/"):
            self.other_table.append(path)


def _assert_no_operation_traffic(log: _RequestLog) -> None:
    assert log.lookup == []
    assert log.describe == []
    assert log.change == []
    assert log.other_table == []


def _assert_exact_public_signature(method: Any) -> None:
    """Freeze ``(self, column_name, new_call)``; no varargs/kwargs escape hatches."""
    params = list(inspect.signature(method).parameters.values())
    assert [p.name for p in params] == ["self", "column_name", "new_call"]
    for param in params:
        assert param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        assert param.default is inspect.Parameter.empty
        assert param.kind is not inspect.Parameter.VAR_POSITIONAL
        assert param.kind is not inspect.Parameter.VAR_KEYWORD
        assert param.kind is not inspect.Parameter.KEYWORD_ONLY


def _assert_exact_change_envelope(
    body: dict[str, Any],
    *,
    source_table_version: int,
    expected_definition: dict[str, Any],
    field_id: int,
    branch: str | None = None,
) -> None:
    expected_keys = {"source_table_version", "spec"}
    if branch is not None:
        expected_keys.add("branch")
    assert set(body) == expected_keys
    assert body["source_table_version"] == source_table_version
    assert "table_ref" not in body
    assert "column_name" not in body
    if branch is None:
        assert "branch" not in body
    else:
        assert body["branch"] == branch

    spec = body["spec"]
    assert set(spec) == {
        "format_version",
        "expected_generated_column_definition",
        "new_function_call",
    }
    assert spec["format_version"] == 1
    assert spec["expected_generated_column_definition"] == expected_definition
    assert (
        spec["expected_generated_column_definition"]["function_call"]["function_id"]
        == _OLD_FUNCTION_ID
    )
    for forbidden in (
        "table_ref",
        "source_table_version",
        "version",
        "column_name",
        "name",
        "function_name",
        "function",
        "function_call",
        "old_function_call",
        "new_function",
        "output",
        "output_type",
        "output_field_id",
        "dependency_epoch",
        "materialized_epoch",
        "status",
        "idempotency_key",
        "retry_key",
        "handle",
        "artifact",
        "geneva",
    ):
        assert forbidden not in spec

    call = spec["new_function_call"]
    assert set(call) == {"function_id", "arguments"}
    assert call["function_id"] == _NEW_FUNCTION_ID
    assert call["function_id"] != _OLD_FUNCTION_ID
    assert len(call["arguments"]) == 1
    binding = call["arguments"][0]
    assert binding["parameter"] == "text"
    value = binding["value"]
    assert value["kind"] == "field"
    assert value["field_id"] == field_id
    assert value["data_type_ipc"] == _UTF8_TYPE_IPC_B64
    assert "name" not in value
    assert "column_name" not in value

    dumped = json.dumps(body)
    assert '"column_name"' not in dumped
    assert "text.normalize" not in dumped
    assert "geneva" not in dumped.lower()


def _open_table_and_function(
    *,
    describe_body: dict[str, Any] | None = None,
    on_change: Callable[[dict[str, Any], http.server.BaseHTTPRequestHandler], None]
    | None = None,
    job_id: str = _JOB_ID_SYNC,
    support_branch_create: bool = False,
    function_wire: dict[str, Any] | None = None,
    checkout_version: int | None = None,
):
    """Open remote table + immutable new Function; return setup tuple."""
    log = _RequestLog()
    binding_describe = describe_body or _describe_body()
    # Alter targets an already-bound generated column: seed open_table with the
    # same schema snapshot the operation-time describe will return.
    open_describe = {
        "version": binding_describe["version"],
        "schema": binding_describe["schema"],
    }
    state = {"opened": False}
    wire = function_wire or _sample_function_wire()
    schema = binding_describe["schema"]
    generated_meta = None
    for field in schema["fields"]:
        if field["name"] == _COLUMN_NAME:
            generated_meta = field.get("metadata", {}).get(_METADATA_KEY)
            break
    expected_definition: dict[str, Any] | None = None
    if generated_meta is not None:
        try:
            expected_definition = _definition_object_from_metadata(generated_meta)
        except json.JSONDecodeError:
            # Malformed metadata fixtures must still open the table; decode
            # failure is an operation-time contract, not a setup error.
            expected_definition = None

    def handler(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.command == "POST"
        raw = _read_body(request)
        body = json.loads(raw.decode("utf-8")) if raw else {}

        if request.path == _LOOKUP_PATH:
            log.note(request.path, body)
            # Setup resolves only the new Function. Old stored ID is never served.
            _json_response(
                request,
                {
                    "function": wire,
                    "server_extra": {"marker": _LOOKUP_RESPONSE_MARKER},
                },
            )
            return

        if request.path == _JOB_DESCRIBE_PATH:
            assert body["job_id"] == job_id
            _json_response(request, _alter_job_done_body(job_id))
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
            # First describe seeds open_table; later ones are binding snapshots
            # (and checkout validation when a fixed version is requested).
            if not state["opened"]:
                state["opened"] = True
                _json_response(request, open_describe)
                return
            log.note(request.path, body)
            _json_response(request, binding_describe)
            return

        if request.path == _CHANGE_PATH:
            log.note(request.path, body)
            if on_change is not None:
                on_change(body, request)
                return
            _json_response(
                request,
                {
                    "job_id": job_id,
                    "server_extra": {"marker": _CHANGE_RESPONSE_MARKER},
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
    # Obtain the exact immutable new Function handle during setup.
    function = _lookup_function(db)
    table = db.open_table(_TABLE_NAME)
    assert isinstance(table, RemoteTable)
    if checkout_version is not None:
        table.checkout(checkout_version)
    # Reset recording before alter: setup lookup must not pollute counts.
    log.start()
    return db, table, function, log, cm, expected_definition


def test_public_and_native_alter_generated_column_seams_must_exist():
    """Public sync/async methods and the private native bridge must exist."""
    assert hasattr(_native.Table, "_alter_generated_column"), (
        "native private bridge Table._alter_generated_column is missing"
    )
    assert hasattr(AsyncTable, "alter_generated_column"), (
        "AsyncTable.alter_generated_column is missing"
    )
    assert hasattr(Table, "alter_generated_column"), (
        "Table.alter_generated_column is missing"
    )
    assert hasattr(LanceTable, "alter_generated_column"), (
        "LanceTable.alter_generated_column is missing"
    )
    assert hasattr(RemoteTable, "alter_generated_column"), (
        "RemoteTable.alter_generated_column is missing"
    )

    _assert_exact_public_signature(Table.alter_generated_column)
    _assert_exact_public_signature(LanceTable.alter_generated_column)
    _assert_exact_public_signature(RemoteTable.alter_generated_column)
    _assert_exact_public_signature(AsyncTable.alter_generated_column)


@pytest.mark.parametrize(
    ("dependency_epoch", "materialized_epoch"),
    [
        (3, 3),  # complete
        (4, 1),  # incomplete
    ],
    ids=["complete", "incomplete"],
)
def test_sync_remote_alter_returns_job_exact_envelope_and_wait_none(
    dependency_epoch: int, materialized_epoch: int
):
    meta = _definition_metadata_json(
        _OUTPUT_FIELD_ID, dependency_epoch, materialized_epoch
    )
    expected = _definition_object_from_metadata(meta)
    db, table, normalize, log, cm, _ = _open_table_and_function(
        describe_body=_describe_body(generated_meta=meta),
        job_id=_JOB_ID_SYNC,
    )
    try:
        schema_before = table.schema
        version_before = table.version
        log.start()

        # Type-changing new Function (Int32 non-null) vs old Utf8 definition:
        # SDK must not gate on old/new output type or nullability equality.
        new_call = normalize(text=col("text"))
        job = table.alter_generated_column(_COLUMN_NAME, new_call)
        assert type(job) is lancedb.job.Job
        assert job.id == _JOB_ID_SYNC

        # One binding describe, zero catalog lookups, one change submit.
        assert len(log.describe) == 1
        assert len(log.lookup) == 0
        assert len(log.change) == 1
        assert log.other_table == []
        _assert_exact_change_envelope(
            log.change[0],
            source_table_version=_SOURCE_TABLE_VERSION,
            expected_definition=expected,
            field_id=_TEXT_FIELD_ID,
        )

        # Acceptance is not publication: wrapper schema/version stay put.
        assert table.schema == schema_before
        assert _COLUMN_NAME in table.schema.names
        assert table.version == version_before

        waited = job.wait()
        assert waited is None
        assert len(log.lookup) == 0
        assert len(log.change) == 1
    finally:
        cm.__exit__(None, None, None)


@pytest.mark.asyncio
async def test_async_remote_alter_returns_async_job_and_wait_none():
    log = _RequestLog()
    state = {"opened": False}
    meta = _definition_metadata_json(_OUTPUT_FIELD_ID, 4, 1)
    expected = _definition_object_from_metadata(meta)
    binding_describe = _describe_body(generated_meta=meta)
    open_describe = {
        "version": binding_describe["version"],
        "schema": binding_describe["schema"],
    }

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
            _json_response(request, _alter_job_done_body(_JOB_ID_ASYNC))
            return
        if request.path == _DESCRIBE_PATH:
            if not state["opened"]:
                state["opened"] = True
                _json_response(request, open_describe)
                return
            log.note(request.path, body)
            _json_response(request, binding_describe)
            return
        if request.path == _CHANGE_PATH:
            log.note(request.path, body)
            _json_response(request, {"job_id": _JOB_ID_ASYNC})
            return
        request.send_response(404)
        request.end_headers()

    async with _mock_remote_db_async(handler) as db:
        normalize = await db.functions.get(_CATALOG_NAME)
        table = await db.open_table(_TABLE_NAME)
        log.start()
        new_call = normalize(text=col("text"))
        job = await table.alter_generated_column(_COLUMN_NAME, new_call)
        assert type(job) is lancedb.job.AsyncJob
        assert job.id == _JOB_ID_ASYNC
        assert len(log.describe) == 1
        assert len(log.lookup) == 0
        assert len(log.change) == 1
        _assert_exact_change_envelope(
            log.change[0],
            source_table_version=_SOURCE_TABLE_VERSION,
            expected_definition=expected,
            field_id=_TEXT_FIELD_ID,
        )
        waited = await job.wait()
        assert waited is None
        assert len(log.lookup) == 0
        assert len(log.change) == 1


def test_remote_branch_alter_includes_exact_branch_identity_without_lookup():
    meta = _definition_metadata_json(
        _BRANCH_OUTPUT_FIELD_ID,
        5,
        2,
        text_field_id=_BRANCH_TEXT_FIELD_ID,
    )
    expected = _definition_object_from_metadata(meta)
    branch_describe = _describe_body(
        version=_BRANCH_SOURCE_VERSION,
        field_ids=[_ORDINARY_FIELD_ID, _BRANCH_TEXT_FIELD_ID, _BRANCH_OUTPUT_FIELD_ID],
        generated_meta=meta,
    )
    db, table, normalize, log, cm, _ = _open_table_and_function(
        describe_body=branch_describe,
        job_id=_JOB_ID_BRANCH,
        support_branch_create=True,
    )
    try:
        branched = table.branches.create(_BRANCH_NAME)
        assert isinstance(branched, RemoteTable)
        assert branched.current_branch() == _BRANCH_NAME
        log.start()

        new_call = normalize(text=col("text"))
        job = branched.alter_generated_column(_COLUMN_NAME, new_call)
        assert type(job) is lancedb.job.Job
        assert job.id == _JOB_ID_BRANCH
        assert len(log.describe) == 1
        assert len(log.lookup) == 0
        assert len(log.change) == 1
        assert log.describe[0].get("branch") == _BRANCH_NAME
        _assert_exact_change_envelope(
            log.change[0],
            source_table_version=_BRANCH_SOURCE_VERSION,
            expected_definition=expected,
            field_id=_BRANCH_TEXT_FIELD_ID,
            branch=_BRANCH_NAME,
        )
    finally:
        cm.__exit__(None, None, None)


def test_empty_column_name_fails_with_zero_operation_requests():
    db, table, normalize, log, cm, _ = _open_table_and_function()
    try:
        new_call = normalize(text=_LITERAL_PAYLOAD_SENTINEL)
        with pytest.raises((ValueError, TypeError)) as raised:
            table.alter_generated_column("", new_call)
        text = _exception_text(raised.value)
        lowered = text.lower()
        assert "column" in lowered or "empty" in lowered or "non-empty" in lowered
        assert _DESCRIBE_BODY_MARKER not in text
        assert _LOOKUP_RESPONSE_MARKER not in text
        assert _CHANGE_RESPONSE_MARKER not in text
        assert _LITERAL_PAYLOAD_SENTINEL not in text
        _assert_no_operation_traffic(log)
    finally:
        cm.__exit__(None, None, None)


@pytest.mark.parametrize(
    ("column_name", "describe_body"),
    [
        ("missing", _describe_body()),
        ("Normalized_Text", _describe_body()),  # exact-case mismatch
        ("ordinary", _describe_body()),
        (
            _COLUMN_NAME,
            _describe_body(
                schema=_alter_schema_fields(
                    generated_meta=(
                        '{"format_version":1,"output_field_id":17,'
                        f'"function_call":{_RAW_METADATA_MARKER},'
                        '"dependency_epoch":1,"materialized_epoch":1}'
                    )
                )
            ),
        ),
        (
            _COLUMN_NAME,
            _describe_body(
                schema=_alter_schema_fields(
                    generated_meta=_definition_metadata_json(_OUTPUT_FIELD_ID + 1, 3, 3)
                )
            ),
        ),
        (
            _COLUMN_NAME,
            _describe_body(
                generated_meta=_definition_metadata_json(
                    _OUTPUT_FIELD_ID, 3, 3, text_field_id=999_999
                )
            ),
        ),
        (
            _COLUMN_NAME,
            _describe_body(
                text_arrow_type="int32",
                generated_meta=_definition_metadata_json(_OUTPUT_FIELD_ID, 3, 3),
            ),
        ),
    ],
    ids=[
        "missing",
        "case_mismatch",
        "ordinary",
        "malformed_metadata",
        "output_id_mismatch",
        "missing_stored_input_field",
        "stored_input_type_mismatch",
    ],
)
def test_fail_closed_old_definition_before_change(
    column_name: str, describe_body: dict[str, Any]
):
    db, table, normalize, log, cm, _ = _open_table_and_function(
        describe_body=describe_body
    )
    try:
        new_call = normalize(text=col("text"))
        with pytest.raises(ValueError) as raised:
            table.alter_generated_column(column_name, new_call)
        text = _exception_text(raised.value)
        assert _DESCRIBE_BODY_MARKER not in text
        assert _LOOKUP_RESPONSE_MARKER not in text
        assert _CHANGE_RESPONSE_MARKER not in text
        assert _RAW_METADATA_MARKER not in text
        assert _LITERAL_PAYLOAD_SENTINEL not in text
        assert len(log.describe) == 1
        assert log.lookup == []
        assert log.change == []
        assert log.other_table == []
    finally:
        cm.__exit__(None, None, None)


@pytest.mark.parametrize(
    ("column_ref", "function_wire", "expected_token"),
    [
        ("missing_text", None, "missing_text"),
        ("Text", None, "Text"),  # exact-case mismatch against schema field "text"
        (
            "text",
            # Old definition stays Utf8-valid; new Function param is Int32 so
            # binding the Utf8 schema field fails after the same single describe.
            _sample_function_wire(
                parameters=[{"name": "text", "data_type_ipc": _INT32_TYPE_IPC_B64}],
            ),
            "text",
        ),
    ],
    ids=["missing", "case_mismatch", "type_mismatch"],
)
def test_new_call_binding_failure_one_describe_zero_change(
    column_ref: str,
    function_wire: dict[str, Any] | None,
    expected_token: str,
):
    meta = _definition_metadata_json(_OUTPUT_FIELD_ID, 4, 1)
    db, table, normalize, log, cm, _ = _open_table_and_function(
        describe_body=_describe_body(generated_meta=meta),
        function_wire=function_wire,
    )
    try:
        new_call = normalize(text=col(column_ref))
        with pytest.raises(ValueError) as raised:
            table.alter_generated_column(_COLUMN_NAME, new_call)
        text = _exception_text(raised.value)
        assert expected_token in text
        if column_ref == "text":
            assert "type" in text.lower() or "mismatch" in text.lower()
        assert _DESCRIBE_BODY_MARKER not in text
        assert _LOOKUP_RESPONSE_MARKER not in text
        assert _CHANGE_RESPONSE_MARKER not in text
        assert _LITERAL_PAYLOAD_SENTINEL not in text
        assert len(log.lookup) == 0
        assert len(log.describe) == 1
        assert log.change == []
        assert log.other_table == []
    finally:
        cm.__exit__(None, None, None)


def test_fixed_version_handle_fails_before_change_post():
    meta = _definition_metadata_json(_OUTPUT_FIELD_ID, 4, 1)
    db, table, normalize, log, cm, _ = _open_table_and_function(
        describe_body=_describe_body(generated_meta=meta),
        checkout_version=_SOURCE_TABLE_VERSION,
    )
    try:
        new_call = normalize(text=col("text"))
        with pytest.raises((NotImplementedError, ValueError, OSError)) as raised:
            table.alter_generated_column(_COLUMN_NAME, new_call)
        text = _exception_text(raised.value)
        lowered = text.lower()
        assert (
            "not supported" in lowered
            or "checkout_latest" in lowered
            or "fixed" in lowered
            or "mutable" in lowered
        )
        assert _CHANGE_RESPONSE_MARKER not in text
        assert log.change == []
    finally:
        cm.__exit__(None, None, None)


@pytest.mark.asyncio
async def test_closed_async_table_fails_with_zero_operation_requests():
    log = _RequestLog()
    state = {"opened": False}
    binding_describe = _describe_body()
    open_describe = {
        "version": binding_describe["version"],
        "schema": binding_describe["schema"],
    }

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
        if request.path == _CHANGE_PATH:
            log.note(request.path, body)
            _json_response(request, {"job_id": _JOB_ID_ASYNC})
            return
        request.send_response(404)
        request.end_headers()

    async with _mock_remote_db_async(handler) as db:
        normalize = await db.functions.get(_CATALOG_NAME)
        table = await db.open_table(_TABLE_NAME)
        new_call = normalize(text=col("text"))
        table.close()
        log.start()
        try:
            await table.alter_generated_column(_COLUMN_NAME, new_call)
        except AttributeError:
            # Method missing: re-raise so the failure names the public seam.
            raise
        except Exception as exc:
            text = _exception_text(exc)
            assert "closed" in text.lower()
        else:
            pytest.fail("closed AsyncTable must fail before transport")
        _assert_no_operation_traffic(log)


def test_local_ordinary_column_fails_without_side_effects_or_fabricated_metadata(
    tmp_path,
):
    """Native/local has no enterprise Function executor; ordinary columns fail closed.

    Do not plant reserved generated-column metadata through a public general
    schema mutation just to reach NotSupported submit.
    """

    def lookup_only(request: http.server.BaseHTTPRequestHandler) -> None:
        assert request.path == _LOOKUP_PATH
        _read_body(request)
        _json_response(request, {"function": _sample_function_wire()})

    with _mock_remote_db(lookup_only) as remote_db:
        normalize = _lookup_function(remote_db)

    db = lancedb.connect(tmp_path)
    table = db.create_table(
        _TABLE_NAME,
        [{"text": "Hello", "ordinary": "x"}, {"text": "World", "ordinary": "y"}],
    )
    assert isinstance(table, LanceTable)
    version_before = table.version
    schema_before = table.schema
    rows_before = table.to_arrow().to_pylist()
    new_call = normalize(text=col("text"))

    with pytest.raises((ValueError, NotImplementedError)) as raised:
        table.alter_generated_column("ordinary", new_call)
    text = _exception_text(raised.value)
    assert "geneva" not in text.lower()
    assert "add_columns" not in text.lower()

    assert table.version == version_before
    assert table.schema == schema_before
    assert table.to_arrow().to_pylist() == rows_before


def test_public_surface_is_minimal_and_rejects_control_kwargs():
    for name in _FORBIDDEN_PUBLIC_NAMES:
        assert name not in getattr(lancedb, "__all__", [])
        assert not hasattr(lancedb, name)

    assert hasattr(Table, "alter_generated_column"), (
        "Table.alter_generated_column is missing"
    )
    _assert_exact_public_signature(Table.alter_generated_column)
    for keyword in _FORBIDDEN_METHOD_KWARGS:
        assert keyword not in inspect.signature(Table.alter_generated_column).parameters

    db, table, normalize, log, cm, _ = _open_table_and_function()
    try:
        new_call = normalize(text=col("text"))
        for keyword in _FORBIDDEN_METHOD_KWARGS:
            with pytest.raises(TypeError):
                table.alter_generated_column(
                    _COLUMN_NAME,
                    new_call,
                    **{keyword: object()},
                )
        _assert_no_operation_traffic(log)
    finally:
        cm.__exit__(None, None, None)

    _assert_exact_public_signature(LanceTable.alter_generated_column)
    _assert_exact_public_signature(RemoteTable.alter_generated_column)
    _assert_exact_public_signature(AsyncTable.alter_generated_column)
    for keyword in _FORBIDDEN_METHOD_KWARGS:
        assert (
            keyword
            not in inspect.signature(AsyncTable.alter_generated_column).parameters
        )
