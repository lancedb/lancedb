# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Contract tests for Python ``table.refresh_generated_column`` (FF-010 authoring).

Public user shape under test:

    job = table.refresh_generated_column("normalized_text")
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
from lancedb.remote.table import RemoteTable
from lancedb.table import AsyncTable, LanceTable, Table

_LOOKUP_PATH = "/v1/functions/lookup"
_JOB_DESCRIBE_PATH = "/v1/jobs/describe"
_TABLE_NAME = "articles"
_DESCRIBE_PATH = f"/v1/table/{_TABLE_NAME}/describe/"
_REFRESH_PATH = f"/v1/table/{_TABLE_NAME}/generated_columns/refresh/"
_BRANCHES_CREATE_PATH = f"/v1/table/{_TABLE_NAME}/branches/create/"
_BRANCHES_LIST_PATH = f"/v1/table/{_TABLE_NAME}/branches/list/"

_FUNCTION_ID = "fn.exact.refresh.gen-col"
_JOB_ID_SYNC = "job-refresh-gen-col-sync-1"
_JOB_ID_ASYNC = "job-refresh-gen-col-async-1"
_JOB_ID_BRANCH = "job-refresh-gen-col-branch-1"
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

_DESCRIBE_BODY_MARKER = "SENSITIVE_REFRESH_DESCRIBE_BODY_MARKER_gen_col_xyz"
_LOOKUP_RESPONSE_MARKER = "SENSITIVE_REFRESH_LOOKUP_RESPONSE_MARKER_gen_col_xyz"
_REFRESH_RESPONSE_MARKER = "SENSITIVE_REFRESH_RESPONSE_MARKER_gen_col_xyz"
_RAW_METADATA_MARKER = "SENSITIVE_REFRESH_METADATA_MARKER_b2_py_9f2e"
_LITERAL_PAYLOAD_SENTINEL = "LITERAL_PAYLOAD_SENTINEL_refresh_gen_col_xyz"

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
    "RefreshGeneratedColumnJobSpec",
    "RefreshGeneratedColumnRequest",
    "GeneratedColumnDefinition",
    "GeneratedColumnBindingSnapshot",
    "FunctionCall",
    "BoundFunctionCall",
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
    "call",
    "spec",
    "definition",
    "dependency_epoch",
    "materialized_epoch",
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


def _definition_metadata_json(
    output_field_id: int,
    dependency_epoch: int,
    materialized_epoch: int,
    *,
    text_field_id: int = _TEXT_FIELD_ID,
    text_type_ipc: str = _UTF8_TYPE_IPC_B64,
    function_id: str = _FUNCTION_ID,
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


def _refresh_schema_fields(
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
        else _refresh_schema_fields(
            generated_meta=generated_meta, text_arrow_type=text_arrow_type
        ),
        "field_ids": field_ids if field_ids is not None else _field_ids(),
    }
    if include_marker:
        body["server_diagnostic"] = _DESCRIBE_BODY_MARKER
    return body


def _refresh_job_done_body(job_id: str) -> dict[str, Any]:
    # DONE with omitted result: use a currently-known no-result job_type so wait
    # projects None. Typed refresh job_type vocabulary is outside this authoring
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


class _RequestLog:
    """Track describe/lookup/refresh after setup; setup traffic is excluded."""

    def __init__(self) -> None:
        self.lookup: list[dict[str, Any]] = []
        self.describe: list[dict[str, Any]] = []
        self.refresh: list[dict[str, Any]] = []
        self.other_table: list[str] = []
        self.recording = False

    def start(self) -> None:
        self.lookup.clear()
        self.describe.clear()
        self.refresh.clear()
        self.other_table.clear()
        self.recording = True

    def note(self, path: str, body: dict[str, Any] | None = None) -> None:
        if not self.recording:
            return
        if path == _LOOKUP_PATH:
            self.lookup.append(body or {})
        elif path == _DESCRIBE_PATH:
            self.describe.append(body or {})
        elif path == _REFRESH_PATH:
            self.refresh.append(body or {})
        elif path.startswith(f"/v1/table/{_TABLE_NAME}/"):
            self.other_table.append(path)


def _assert_no_operation_traffic(log: _RequestLog) -> None:
    assert log.lookup == []
    assert log.describe == []
    assert log.refresh == []
    assert log.other_table == []


def _assert_exact_public_signature(method: Any) -> None:
    """Freeze ``(self, column_name)`` with no varargs/kwargs escape hatches."""
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


def _assert_exact_refresh_envelope(
    body: dict[str, Any],
    *,
    source_table_version: int,
    definition: dict[str, Any],
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
    assert set(spec) == {"format_version", "generated_column_definition"}
    assert spec["format_version"] == 1
    assert spec["generated_column_definition"] == definition
    for forbidden in (
        "table_ref",
        "source_table_version",
        "version",
        "column_name",
        "name",
        "function_name",
        "function",
        "function_call",
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

    dumped = json.dumps(body)
    assert '"column_name"' not in dumped
    assert "text.normalize" not in dumped
    assert "geneva" not in dumped.lower()


def _assert_exact_id_lookup(body: dict[str, Any]) -> None:
    assert body == {"function_id": _FUNCTION_ID}
    assert "name" not in body
    assert "branch" not in body
    assert "table" not in body
    assert "table_ref" not in body
    assert "version" not in body
    assert "source_table_version" not in body


def _open_remote_table(
    *,
    describe_body: dict[str, Any] | None = None,
    on_refresh: Callable[[dict[str, Any], http.server.BaseHTTPRequestHandler], None]
    | None = None,
    job_id: str = _JOB_ID_SYNC,
    support_branch_create: bool = False,
    function_wire: dict[str, Any] | None = None,
    checkout_version: int | None = None,
):
    """Open remote table; return (db, table, log, cm, expected_definition)."""
    log = _RequestLog()
    binding_describe = describe_body or _describe_body()
    # Refresh targets an already-bound generated column: seed open_table with
    # the same schema snapshot the operation-time describe will return.
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
            _json_response(request, _refresh_job_done_body(job_id))
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

        if request.path == _REFRESH_PATH:
            log.note(request.path, body)
            if on_refresh is not None:
                on_refresh(body, request)
                return
            _json_response(
                request,
                {
                    "job_id": job_id,
                    "server_extra": {"marker": _REFRESH_RESPONSE_MARKER},
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
    table = db.open_table(_TABLE_NAME)
    assert isinstance(table, RemoteTable)
    if checkout_version is not None:
        table.checkout(checkout_version)
    log.start()
    return db, table, log, cm, expected_definition


def test_public_and_native_refresh_generated_column_seams_must_exist():
    """Public sync/async methods and the private native bridge must exist."""
    assert hasattr(_native.Table, "_refresh_generated_column"), (
        "native private bridge Table._refresh_generated_column is missing"
    )
    assert hasattr(AsyncTable, "refresh_generated_column"), (
        "AsyncTable.refresh_generated_column is missing"
    )
    assert hasattr(Table, "refresh_generated_column"), (
        "Table.refresh_generated_column is missing"
    )
    assert hasattr(LanceTable, "refresh_generated_column"), (
        "LanceTable.refresh_generated_column is missing"
    )
    assert hasattr(RemoteTable, "refresh_generated_column"), (
        "RemoteTable.refresh_generated_column is missing"
    )

    _assert_exact_public_signature(Table.refresh_generated_column)
    _assert_exact_public_signature(LanceTable.refresh_generated_column)
    _assert_exact_public_signature(RemoteTable.refresh_generated_column)
    _assert_exact_public_signature(AsyncTable.refresh_generated_column)


@pytest.mark.parametrize(
    ("dependency_epoch", "materialized_epoch"),
    [
        (3, 3),  # complete
        (4, 1),  # incomplete
    ],
    ids=["complete", "incomplete"],
)
def test_sync_remote_refresh_returns_job_exact_envelope_and_wait_none(
    dependency_epoch: int, materialized_epoch: int
):
    meta = _definition_metadata_json(
        _OUTPUT_FIELD_ID, dependency_epoch, materialized_epoch
    )
    expected = _definition_object_from_metadata(meta)
    db, table, log, cm, _ = _open_remote_table(
        describe_body=_describe_body(generated_meta=meta),
        job_id=_JOB_ID_SYNC,
    )
    try:
        schema_before = table.schema
        version_before = table.version
        log.start()

        job = table.refresh_generated_column(_COLUMN_NAME)
        assert type(job) is lancedb.job.Job
        assert job.id == _JOB_ID_SYNC

        # One binding describe, one exact-ID catalog lookup, one refresh submit.
        assert len(log.describe) == 1
        assert len(log.lookup) == 1
        assert len(log.refresh) == 1
        assert log.other_table == []
        _assert_exact_id_lookup(log.lookup[0])
        _assert_exact_refresh_envelope(
            log.refresh[0],
            source_table_version=_SOURCE_TABLE_VERSION,
            definition=expected,
        )

        # Acceptance is not publication: wrapper schema/version stay put.
        assert table.schema == schema_before
        assert _COLUMN_NAME in table.schema.names
        assert table.version == version_before

        waited = job.wait()
        assert waited is None
        assert len(log.lookup) == 1
        assert len(log.refresh) == 1
    finally:
        cm.__exit__(None, None, None)


@pytest.mark.asyncio
async def test_async_remote_refresh_returns_async_job_and_wait_none():
    log = _RequestLog()
    state = {"opened": False}
    meta = _definition_metadata_json(_OUTPUT_FIELD_ID, 4, 1)
    expected = _definition_object_from_metadata(meta)
    binding_describe = _describe_body(generated_meta=meta)
    open_describe = {"version": 1, "schema": {"fields": [_field("text")]}}

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
            _json_response(request, _refresh_job_done_body(_JOB_ID_ASYNC))
            return
        if request.path == _DESCRIBE_PATH:
            if not state["opened"]:
                state["opened"] = True
                _json_response(request, open_describe)
                return
            log.note(request.path, body)
            _json_response(request, binding_describe)
            return
        if request.path == _REFRESH_PATH:
            log.note(request.path, body)
            _json_response(request, {"job_id": _JOB_ID_ASYNC})
            return
        request.send_response(404)
        request.end_headers()

    async with _mock_remote_db_async(handler) as db:
        table = await db.open_table(_TABLE_NAME)
        log.start()
        job = await table.refresh_generated_column(_COLUMN_NAME)
        assert type(job) is lancedb.job.AsyncJob
        assert job.id == _JOB_ID_ASYNC
        assert len(log.describe) == 1
        assert len(log.lookup) == 1
        assert len(log.refresh) == 1
        _assert_exact_id_lookup(log.lookup[0])
        _assert_exact_refresh_envelope(
            log.refresh[0],
            source_table_version=_SOURCE_TABLE_VERSION,
            definition=expected,
        )
        waited = await job.wait()
        assert waited is None
        assert len(log.refresh) == 1


def test_remote_branch_refresh_includes_exact_branch_identity_without_lookup_branch():
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
    db, table, log, cm, _ = _open_remote_table(
        describe_body=branch_describe,
        job_id=_JOB_ID_BRANCH,
        support_branch_create=True,
    )
    try:
        branched = table.branches.create(_BRANCH_NAME)
        assert isinstance(branched, RemoteTable)
        assert branched.current_branch() == _BRANCH_NAME
        log.start()

        job = branched.refresh_generated_column(_COLUMN_NAME)
        assert type(job) is lancedb.job.Job
        assert job.id == _JOB_ID_BRANCH
        assert len(log.describe) == 1
        assert len(log.lookup) == 1
        assert len(log.refresh) == 1
        assert log.describe[0].get("branch") == _BRANCH_NAME
        _assert_exact_id_lookup(log.lookup[0])
        assert "branch" not in log.lookup[0]
        _assert_exact_refresh_envelope(
            log.refresh[0],
            source_table_version=_BRANCH_SOURCE_VERSION,
            definition=expected,
            branch=_BRANCH_NAME,
        )
    finally:
        cm.__exit__(None, None, None)


def test_empty_column_name_fails_with_zero_operation_requests():
    db, table, log, cm, _ = _open_remote_table()
    try:
        with pytest.raises((ValueError, TypeError)) as raised:
            table.refresh_generated_column("")
        text = _exception_text(raised.value)
        lowered = text.lower()
        assert "column" in lowered or "empty" in lowered or "non-empty" in lowered
        assert _DESCRIBE_BODY_MARKER not in text
        assert _LOOKUP_RESPONSE_MARKER not in text
        assert _REFRESH_RESPONSE_MARKER not in text
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
                schema=_refresh_schema_fields(
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
        "missing_stored_input_field",
        "stored_input_type_mismatch",
    ],
)
def test_fail_closed_before_lookup_and_refresh(
    column_name: str, describe_body: dict[str, Any]
):
    db, table, log, cm, _ = _open_remote_table(describe_body=describe_body)
    try:
        with pytest.raises(ValueError) as raised:
            table.refresh_generated_column(column_name)
        text = _exception_text(raised.value)
        assert _DESCRIBE_BODY_MARKER not in text
        assert _LOOKUP_RESPONSE_MARKER not in text
        assert _REFRESH_RESPONSE_MARKER not in text
        assert _RAW_METADATA_MARKER not in text
        assert _LITERAL_PAYLOAD_SENTINEL not in text
        assert len(log.describe) == 1
        assert log.lookup == []
        assert log.refresh == []
        assert log.other_table == []
    finally:
        cm.__exit__(None, None, None)


def test_function_signature_mismatch_looks_up_then_fails_before_refresh():
    """Exact-ID lookup may run; signature mismatch must not submit refresh."""
    meta = _definition_metadata_json(_OUTPUT_FIELD_ID, 4, 1)
    mismatched_wire = _sample_function_wire(
        parameters=[{"name": "text", "data_type_ipc": _INT32_TYPE_IPC_B64}]
    )
    db, table, log, cm, _ = _open_remote_table(
        describe_body=_describe_body(generated_meta=meta),
        function_wire=mismatched_wire,
    )
    try:
        with pytest.raises(ValueError) as raised:
            table.refresh_generated_column(_COLUMN_NAME)
        text = _exception_text(raised.value)
        assert _DESCRIBE_BODY_MARKER not in text
        assert _LOOKUP_RESPONSE_MARKER not in text
        assert _REFRESH_RESPONSE_MARKER not in text
        assert _LITERAL_PAYLOAD_SENTINEL not in text
        assert len(log.describe) == 1
        assert len(log.lookup) == 1
        _assert_exact_id_lookup(log.lookup[0])
        assert log.refresh == []
        assert log.other_table == []
    finally:
        cm.__exit__(None, None, None)


def test_fixed_version_handle_fails_before_refresh_post():
    meta = _definition_metadata_json(_OUTPUT_FIELD_ID, 4, 1)
    db, table, log, cm, _ = _open_remote_table(
        describe_body=_describe_body(generated_meta=meta),
        checkout_version=_SOURCE_TABLE_VERSION,
    )
    try:
        with pytest.raises((NotImplementedError, ValueError, OSError)) as raised:
            table.refresh_generated_column(_COLUMN_NAME)
        text = _exception_text(raised.value)
        lowered = text.lower()
        assert (
            "not supported" in lowered
            or "checkout_latest" in lowered
            or "fixed" in lowered
            or "mutable" in lowered
        )
        assert _REFRESH_RESPONSE_MARKER not in text
        assert log.refresh == []
    finally:
        cm.__exit__(None, None, None)


@pytest.mark.asyncio
async def test_closed_async_table_fails_with_zero_operation_requests():
    log = _RequestLog()
    state = {"opened": False}
    binding_describe = _describe_body()
    open_describe = {"version": 1, "schema": {"fields": [_field("text")]}}

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
        if request.path == _REFRESH_PATH:
            log.note(request.path, body)
            _json_response(request, {"job_id": _JOB_ID_ASYNC})
            return
        request.send_response(404)
        request.end_headers()

    async with _mock_remote_db_async(handler) as db:
        table = await db.open_table(_TABLE_NAME)
        table.close()
        log.start()
        try:
            await table.refresh_generated_column(_COLUMN_NAME)
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
    db = lancedb.connect(tmp_path)
    table = db.create_table(
        _TABLE_NAME,
        [{"text": "Hello", "ordinary": "x"}, {"text": "World", "ordinary": "y"}],
    )
    assert isinstance(table, LanceTable)
    version_before = table.version
    schema_before = table.schema
    rows_before = table.to_arrow().to_pylist()

    with pytest.raises((ValueError, NotImplementedError)) as raised:
        table.refresh_generated_column("ordinary")
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

    assert hasattr(Table, "refresh_generated_column"), (
        "Table.refresh_generated_column is missing"
    )
    _assert_exact_public_signature(Table.refresh_generated_column)
    for keyword in _FORBIDDEN_METHOD_KWARGS:
        assert (
            keyword not in inspect.signature(Table.refresh_generated_column).parameters
        )

    db, table, log, cm, _ = _open_remote_table()
    try:
        for keyword in _FORBIDDEN_METHOD_KWARGS:
            with pytest.raises(TypeError):
                table.refresh_generated_column(
                    _COLUMN_NAME,
                    **{keyword: object()},
                )
        _assert_no_operation_traffic(log)
    finally:
        cm.__exit__(None, None, None)

    _assert_exact_public_signature(LanceTable.refresh_generated_column)
    _assert_exact_public_signature(RemoteTable.refresh_generated_column)
    _assert_exact_public_signature(AsyncTable.refresh_generated_column)
    for keyword in _FORBIDDEN_METHOD_KWARGS:
        assert (
            keyword
            not in inspect.signature(AsyncTable.refresh_generated_column).parameters
        )
