# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from __future__ import annotations

import contextlib
import http.server
import json
from pathlib import Path
import threading
from typing import Optional

import pyarrow as pa
import pytest

import lancedb
from lancedb.functions import UdfDefinition, udf


FIXTURES = (
    Path(__file__).parents[3]
    / "rust"
    / "lancedb"
    / "tests"
    / "fixtures"
    / "first_class_functions"
    / "v1"
)


@udf(
    pip=["numpy>=2"],
    env={"MODE": "test"},
    secrets=["API_TOKEN"],
    python_version="3.12",
)
def normalize_score(value: float) -> float:
    return value / 100.0


def _assert_no_secret_values(value):
    if isinstance(value, dict):
        for key, child in value.items():
            assert key not in {
                "secret_value",
                "secret_values",
                "resolved_secret",
                "resolved_secrets",
            }
            _assert_no_secret_values(child)
    elif isinstance(value, list):
        for child in value:
            _assert_no_secret_values(child)


def test_scalar_udf_matches_shared_registration_golden_and_remains_callable():
    assert isinstance(normalize_score, UdfDefinition)
    assert normalize_score(25.0) == 0.25
    assert (
        normalize_score.registration_request.to_canonical_json()
        == (FIXTURES / "remote_function_registration_request.canonical.json")
        .read_text()
        .strip()
    )
    request = json.loads(normalize_score.registration_request.to_canonical_json())
    assert request["artifact"]["adapter"] == {
        "kind": "scalar_to_arrow_batch",
        "version": 1,
    }
    assert request["required_secrets"] == ["API_TOKEN"]
    _assert_no_secret_values(request)


def test_explicit_arrow_schema_is_deterministic():
    input_schema = pa.schema([pa.field("value", pa.float32(), nullable=True)])
    output_schema = pa.field("embedding", pa.list_(pa.float32(), 3), nullable=False)

    @udf(input_schema=input_schema, output_schema=output_schema)
    def explicit(value):
        return [value, value, value]

    signature = explicit.registration_request.signature
    assert signature.inputs[0].arrow_type == "float32"
    assert signature.inputs[0].nullable is True
    assert signature.output.arrow_type == "fixed_size_list<float32>[3]"
    assert signature.output.nullable is False


def test_annotation_and_explicit_schema_validation_fail_closed():
    with pytest.raises(TypeError, match="missing Function annotations"):

        @udf
        def missing(value):
            return value

    with pytest.raises(TypeError, match="unsupported Function annotation"):

        @udf
        def unsupported(value: set[str]) -> str:
            return ""

    with pytest.raises(ValueError, match="output must be non-nullable"):

        @udf
        def nullable_output(value: int) -> Optional[int]:
            return value

    with pytest.raises(ValueError, match="provided together"):

        @udf(input_schema=pa.schema([pa.field("value", pa.int64())]))
        def partial_schema(value):
            return value

    with pytest.raises(ValueError, match="exactly match callable parameters"):

        @udf(
            input_schema=pa.schema([pa.field("other", pa.int64())]),
            output_schema=pa.int64(),
        )
        def wrong_name(value):
            return value

    with pytest.raises(ValueError, match="output must be non-nullable"):

        @udf(
            input_schema=pa.schema([pa.field("value", pa.int64())]),
            output_schema=pa.field("result", pa.int64(), nullable=True),
        )
        def nullable_explicit(value):
            return value


def test_environment_rejects_secret_value_overlap():
    with pytest.raises(ValueError, match="must be disjoint"):

        @udf(env={"TOKEN": "plaintext"}, secrets=["TOKEN"])
        def overlapping(value: int) -> int:
            return value


def test_local_function_catalog_operations_are_not_supported(tmp_path):
    db = lancedb.connect(tmp_path)
    message = "Function catalog operations are not supported by this database"
    with pytest.raises(NotImplementedError, match=message):
        db.create_function(normalize_score)
    with pytest.raises(NotImplementedError, match=message):
        db.create_function_async(normalize_score)
    with pytest.raises(NotImplementedError, match=message):
        db.get_function("normalize_score", version="fv_exact")


@contextlib.contextmanager
def _mock_remote_function_catalog():
    state = {"requests": [], "version": None}

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length) or b"{}")
            state["requests"].append((self.path, body))
            status = 200
            if self.path == "/v1/function/create":
                state["version"] = {
                    "name": body["name"],
                    "version": "fv_exact",
                    "artifact": {
                        key: body["artifact"][key]
                        for key in ("kind", "digest", "entrypoint")
                    },
                    "signature": body["signature"],
                    "runtime": body["runtime"],
                    "runtime_digest": "sha256:runtime",
                    "environment_digest": "sha256:environment",
                    "required_secrets": body.get("required_secrets", []),
                    "created_at": "2026-08-21T00:00:00Z",
                }
                response = {"job_id": "job-register"}
                status = 202
            elif self.path == "/v1/jobs/describe":
                assert body == {"job_id": "job-register"}
                response = {
                    "job_id": "job-register",
                    "job_type": "create_function",
                    "job_state": "DONE",
                    "result": state["version"],
                }
            elif self.path == "/v1/function/describe":
                assert body == {
                    "name": "normalize_score",
                    "version": "fv_exact",
                }
                response = state["version"]
            else:
                status = 404
                response = {"error": "not found"}
            encoded = json.dumps(response).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    with http.server.HTTPServer(("localhost", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            yield f"http://localhost:{server.server_address[1]}", state
        finally:
            server.shutdown()
            thread.join()


def test_remote_registration_job_and_exact_version_reopen_round_trip():
    with _mock_remote_function_catalog() as (host, state):
        db = lancedb.connect(
            "db://dev",
            api_key="fake",
            host_override=host,
            client_config={"retry_config": {"retries": 0}},
        )
        registration = db.create_function_async(normalize_score)
        assert registration.id == "job-register"
        created = registration.wait()
        reopened = db.get_function("normalize_score", version=created.version)

    assert created == reopened
    assert reopened.name == "normalize_score"
    assert reopened.version == "fv_exact"
    create_request = state["requests"][0][1]
    assert create_request == json.loads(
        normalize_score.registration_request.to_canonical_json()
    )
    _assert_no_secret_values(create_request)


def test_blocking_remote_registration_returns_function_version():
    with _mock_remote_function_catalog() as (host, state):
        db = lancedb.connect(
            "db://dev",
            api_key="fake",
            host_override=host,
            client_config={"retry_config": {"retries": 0}},
        )
        created = db.create_function(normalize_score)

    assert created.name == "normalize_score"
    assert created.version == "fv_exact"
    assert [path for path, _ in state["requests"]] == [
        "/v1/function/create",
        "/v1/jobs/describe",
    ]
