# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pytest

import lancedb
from lancedb.functions import (
    BlobWriter,
    FunctionDefinition,
    FunctionRegistrationRequest,
    FunctionVersion,
    function,
)


def _stored_function_version():
    fixture = (
        Path(__file__).parents[3]
        / "rust/lancedb/tests/fixtures/first_class_functions/v1"
        / "remote_function_version.canonical.json"
    )
    return json.loads(fixture.read_text())


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["sync", "sync_job", "async_job"])
@pytest.mark.parametrize("exist_ok", [None, False, True])
async def test_function_exist_ok_forwards_sql_and_keeps_stored_version(entry, exist_ok):
    flight = pytest.importorskip("pyarrow.flight")
    stored = _stored_function_version()
    result = pa.table({"definition": [json.dumps(stored)]})

    @function(name=stored["name"])
    def replacement(value: float) -> float:
        return value * 2

    class FunctionServer(flight.FlightServerBase):
        def __init__(self):
            super().__init__("grpc://127.0.0.1:0")
            self.requests = []

        def do_get(self, context, ticket):
            self.requests.append(ticket.ticket)
            return flight.RecordBatchStream(result)

    with FunctionServer() as server:
        options = {
            "api_key": "test-key",
            "host_override": "http://localhost:10024",
            "sql_host_override": f"grpc://127.0.0.1:{server.port}",
        }
        kwargs = {} if exist_ok is None else {"exist_ok": exist_ok}
        kwargs["namespace_path"] = ["team"]
        if entry == "async_job":
            db = await lancedb.connect_async("db://analytics", **options)
            job = await db.create_function_async(replacement, **kwargs)
            version = await job.wait()
        else:
            db = lancedb.connect("db://analytics", **options)
            if entry == "sync_job":
                version = db.create_function_async(replacement, **kwargs).wait()
            else:
                version = db.create_function(replacement, **kwargs)
        (request,) = server.requests

    prefix = "CREATE FUNCTION IF NOT EXISTS" if exist_ok else "CREATE FUNCTION"
    assert f'{prefix} "{stored["name"]}" FROM '.encode() in request
    assert b'"exist_ok"' not in request
    assert json.loads(version.to_canonical_json()) == stored


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("gpu_options", "expected_gpu"),
    [
        pytest.param({}, {}, id="cpu"),
        pytest.param({"gpu": True}, {"gpu": True}, id="implicit"),
        pytest.param(
            {"gpu": True, "gpu_count": 1}, {"gpu": True, "gpus_per_worker": 1}, id="one"
        ),
        pytest.param(
            {"gpu": True, "gpu_count": 2}, {"gpu": True, "gpus_per_worker": 2}, id="two"
        ),
    ],
)
async def test_function_registration_gpu_count_and_runtime_digest(
    gpu_options, expected_gpu
):
    flight = pytest.importorskip("pyarrow.flight")
    stored = _stored_function_version()

    @lancedb.function(
        name=stored["name"],
        python_version="3.12",
        input_schema=pa.schema([pa.field("value", pa.int64())]),
        output_schema=pa.int64(),
        **gpu_options,
    )
    def identity(value: pa.Array) -> pa.Array:
        return value

    class FunctionServer(flight.FlightServerBase):
        def __init__(self):
            super().__init__("grpc://127.0.0.1:0")
            self.requests = []

        def do_get(self, context, ticket):
            self.requests.append(ticket.ticket)
            return flight.RecordBatchStream(
                pa.table({"definition": [json.dumps(stored)]})
            )

    with FunctionServer() as server:
        db = await lancedb.connect_async(
            "db://test",
            api_key="test-key",
            host_override="http://localhost:10024",
            sql_host_override=f"grpc://127.0.0.1:{server.port}",
        )
        await db.create_function_async(identity)
        (request,) = server.requests

    assert b"CREATE FUNCTION" in request
    _, marker, body = request.partition(b" FROM '{")
    assert marker
    submitted, _ = json.JSONDecoder().raw_decode("{" + body.decode())
    expected_runtime = {
        "kind": "python_v2" if expected_gpu else "python",
        "python_version": "3.12",
        "environment": {"kind": "pip"},
        **expected_gpu,
    }
    assert submitted["runtime"] == expected_runtime
    canonical = json.dumps(expected_runtime, sort_keys=True, separators=(",", ":"))
    expected_digest = hashlib.sha256(canonical.encode()).hexdigest()
    assert identity.registration_request.runtime.digest == f"sha256:{expected_digest}"


@pytest.mark.asyncio
async def test_blob_writer_paths_round_trip_through_function_registration():
    flight = pytest.importorskip("pyarrow.flight")
    stored = _stored_function_version()
    expected_paths = [[{"field": "item"}], [{"field": "variants"}, "item"]]
    output_schema = pa.schema(
        [
            BlobWriter.field("item"),
            pa.field("variants", pa.list_(BlobWriter.field("asset"))),
            pa.field("checksum", pa.binary(), nullable=False),
        ]
    )

    @function(name=stored["name"], output_schema=output_schema)
    def render(text: str):
        with BlobWriter.open() as output:
            output.write(text.encode())
        return {"item": output, "variants": [b"", None], "checksum": b""}

    expected_output = json.loads(
        render.registration_request.signature.output.to_canonical_json()
    )
    assert expected_output["kind"] == "named_struct_blob_writer"
    assert expected_output["blob_writer_paths"] == expected_paths
    stored["signature"] = {
        "inputs": [{"name": "text", "arrow_type": "utf8", "nullable": False}],
        "output": expected_output,
    }

    class FunctionServer(flight.FlightServerBase):
        def __init__(self):
            super().__init__("grpc://127.0.0.1:0")
            self.requests = []

        def do_get(self, context, ticket):
            self.requests.append(ticket.ticket)
            return flight.RecordBatchStream(
                pa.table({"definition": [json.dumps(stored)]})
            )

    with FunctionServer() as server:
        db = await lancedb.connect_async(
            "db://analytics",
            api_key="test-key",
            host_override="http://localhost:10024",
            sql_host_override=f"grpc://127.0.0.1:{server.port}",
        )
        job = await db.create_function_async(render)
        version = await job.wait()
        (request,) = server.requests

    assert b"CREATE FUNCTION" in request
    _, marker, body = request.partition(b" FROM '{")
    assert marker
    submitted, _ = json.JSONDecoder().raw_decode("{" + body.decode(errors="replace"))
    assert submitted["signature"]["output"] == expected_output
    assert (
        json.loads(version.to_canonical_json())["signature"]["output"]
        == expected_output
    )
    application = version(text=lancedb.col("text")).rename(columns={"item": "payload"})
    assert json.loads(application.to_canonical_json())["output"] == expected_output
    assert application.columns == {"item": "payload"}


@pytest.mark.asyncio
@pytest.mark.parametrize("with_primary", [False, True])
async def test_function_index_inspection_is_separate_from_registration(with_primary):
    flight = pytest.importorskip("pyarrow.flight")
    primary = "https://primary.example/simple/"
    extra = "https://extra.example/simple/"
    stored = _stored_function_version()
    name = stored["name"]

    def scale(value: float) -> float:
        return 2 * value

    authored = function(name=name, pip=["example==1.0"], python_version="3.12")(scale)
    request = authored.registration_request
    expected_runtime = {
        "kind": "python",
        "python_version": "3.12",
        "environment": {"kind": "pip", "packages": ["example==1.0"]},
    }
    stored["runtime"] = json.loads(request.runtime.to_canonical_json())
    build = {"extra_index_urls": [extra]}
    if with_primary:
        build["index_url"] = primary
    stored["runtime"]["environment"]["build"] = build
    stored["runtime_digest"] = "sha256:" + "1" * 64
    stored["resolved_environment"] = {
        "packages": [],
        "provenance": {"configured_index_urls": [extra]},
    }

    class FunctionServer(flight.FlightServerBase):
        def __init__(self):
            super().__init__("grpc://127.0.0.1:0")
            self.requests = []

        def do_get(self, context, ticket):
            self.requests.append(ticket.ticket)
            if b"DESCRIBE FUNCTION" in ticket.ticket:
                assert b"FORMAT JSON" in ticket.ticket
                return flight.RecordBatchStream(
                    pa.table(
                        {"description": [json.dumps(stored)]}
                    ).replace_schema_metadata({b"lancedb.describe.format": b"json"})
                )
            return flight.RecordBatchStream(
                pa.table({"definition": [json.dumps(stored)]})
            )

    class IndexedDefinition(FunctionDefinition):
        def bind_secrets(self, secrets):
            payload = json.loads(super().bind_secrets(secrets).to_canonical_json())
            payload["runtime"]["environment"]["build"] = build
            return FunctionRegistrationRequest.from_json(json.dumps(payload))

    definition = IndexedDefinition(
        scale,
        name=name,
        input_schema=None,
        output_schema=None,
        pip=("example==1.0",),
        env={},
        python_version="3.12",
    )

    with FunctionServer() as server:
        options = {
            "api_key": "test-key",
            "host_override": "http://127.0.0.1:1",
            "sql_host_override": f"grpc://127.0.0.1:{server.port}",
        }
        db = lancedb.connect("db://analytics", **options)
        async_db = await lancedb.connect_async("db://analytics", **options)

        message = r"runtime\.environment\.build is unsupported"
        with pytest.raises(TypeError, match=message):
            db.create_function(definition)
        with pytest.raises(TypeError, match=message):
            await async_db.create_function_async(definition)
        assert server.requests == []

        version = db.get_function(name, version=1)
        assert await async_db.get_function(name, version="1") == version
        assert all(
            f"DESCRIBE FUNCTION \"{name}\" WITH (version = '1') FORMAT JSON".encode()
            in ticket
            for ticket in server.requests
        )
        assert len(server.requests) == 2
        assert version.runtime_digest == stored["runtime_digest"]
        assert version.resolved_environment.provenance.configured_index_urls == (extra,)
        canonical = version.to_canonical_json()
        encoded = json.loads(canonical)
        assert FunctionVersion.from_json(canonical) == version
        if with_primary:
            assert version.runtime.environment.build.index_url == primary
            assert encoded["runtime"]["environment"]["build"] == {"index_url": primary}
        else:
            assert version.runtime.environment.build is None
            assert "build" not in encoded["runtime"]["environment"]
        with pytest.raises(TypeError, match="runtime metadata is for inspection only"):
            FunctionRegistrationRequest.model_validate(
                {**request.model_dump(), "runtime": version.runtime}
            )
        with pytest.raises(TypeError, match="runtime metadata is for inspection only"):
            request.model_copy(update={"runtime": version.runtime}).to_canonical_json()

        job = await async_db.create_function_async(authored, refresh_lock=True)
        assert await job.wait() == version
        _, marker, body = server.requests[-1].partition(b" FROM '{")
        assert marker
        sent, _ = json.JSONDecoder().raw_decode("{" + body.decode(errors="replace"))
        assert sent["refresh_lock"] is True
        assert sent["runtime"] == expected_runtime
