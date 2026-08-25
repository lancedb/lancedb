# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
from pathlib import Path

import pytest

from lancedb import col
import lancedb.functions as functions
from lancedb.functions import (
    FunctionApplication,
    FunctionBinding,
    FunctionVersion,
    PythonRuntimeSpec,
    RefreshColumnResult,
)
from lancedb.table import AsyncTable


FIXTURES = (
    Path(__file__).parents[3]
    / "rust"
    / "lancedb"
    / "tests"
    / "fixtures"
    / "first_class_functions"
    / "v1"
)


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


def job_result(name: str) -> dict:
    return json.loads(fixture(name))["result"]


def test_public_function_values_are_in_api_reference():
    docs = Path(__file__).parents[3] / "docs" / "src" / "python" / "python.md"
    rendered = docs.read_text()
    for name in functions.__all__:
        assert f"::: lancedb.functions.{name}" in rendered


@pytest.mark.parametrize(
    ("fixture_name", "canonical_name", "model", "nested_result"),
    [
        (
            "remote_function_job.json",
            "remote_function_version.canonical.json",
            FunctionVersion,
            True,
        ),
        (
            "remote_function_application.json",
            "remote_function_application.canonical.json",
            FunctionApplication,
            False,
        ),
        (
            "remote_function_binding.json",
            "remote_function_binding.canonical.json",
            FunctionBinding,
            False,
        ),
        (
            "remote_refresh_job.json",
            "remote_refresh_result.canonical.json",
            RefreshColumnResult,
            True,
        ),
        (
            "remote_refresh_result_without_published_version.json",
            "remote_refresh_result_without_published_version.canonical.json",
            RefreshColumnResult,
            False,
        ),
    ],
)
def test_python_and_rust_share_remote_canonical_goldens(
    fixture_name, canonical_name, model, nested_result
):
    value = json.loads(fixture(fixture_name))
    if nested_result:
        value = value["result"]
    decoded = model.from_json(json.dumps(value))
    assert decoded.to_canonical_json() == fixture(canonical_name).strip()


def test_function_version_identity_is_immutable_and_exact():
    value = job_result("remote_function_job.json")
    version = FunctionVersion.from_json(json.dumps(value))
    assert version.name == "embed"
    assert version.version == "fv_01K3EXACT"

    with pytest.raises((TypeError, ValueError)):
        version.version = "fv_changed"
    with pytest.raises(TypeError, match="immutable"):
        version.runtime.env["TOKENIZERS_PARALLELISM"] = "true"

    changed = dict(value)
    changed["version"] = "fv_changed"
    assert FunctionVersion(**changed) != version


def test_function_version_binds_named_columns_as_one_immutable_application():
    version = FunctionVersion.from_json(
        json.dumps(job_result("remote_function_job.json"))
    )

    application = version(text=col("documents.body"))

    assert application.function.name == version.name
    assert application.function.version == version.version
    assert application.output is version.signature.output
    assert [
        (value.parameter, value.kind, value.value["path"])
        for value in application.inputs
    ] == [("text", "column", "documents.body")]


def test_function_version_binding_validates_names_and_direct_columns():
    version = FunctionVersion.from_json(
        json.dumps(job_result("remote_function_job.json"))
    )

    with pytest.raises(TypeError, match=r"missing inputs: \['text'\]"):
        version()
    with pytest.raises(TypeError, match=r"unknown inputs: \['body'\]"):
        version(text=col("text"), body=col("body"))
    with pytest.raises(TypeError, match="direct col"):
        version(text=col("text").lower())


def test_function_version_keeps_named_struct_outputs_in_one_application():
    value = job_result("remote_function_job.json")
    value["name"] = "text_features"
    value["version"] = "fv_multi_output"
    value["signature"] = {
        "inputs": [
            {"name": "title", "arrow_type": "utf8", "nullable": True},
            {"name": "body", "arrow_type": "utf8", "nullable": True},
        ],
        "output": {
            "kind": "named_struct",
            "fields": [
                {
                    "name": "normalized_text",
                    "arrow_type": "utf8",
                    "nullable": False,
                },
                {
                    "name": "token_count",
                    "arrow_type": "int64",
                    "nullable": False,
                },
            ],
        },
    }
    version = FunctionVersion(**value)

    application = version(body=col("body"), title=col("title")).rename(
        columns={
            "normalized_text": "search_text",
            "token_count": "search_token_count",
        }
    )

    assert [value.parameter for value in application.inputs] == ["title", "body"]
    assert [field.name for field in application.output.fields] == [
        "normalized_text",
        "token_count",
    ]
    assert dict(application.columns) == {
        "normalized_text": "search_text",
        "token_count": "search_token_count",
    }


def test_unknown_fields_and_discriminators_are_forward_decodable():
    value = job_result("remote_function_job.json")
    value["future_version_metadata"] = {"retention_class": "catalog"}
    value["runtime"] = {"kind": "wasm", "module_digest": "sha256:wasm"}
    value["signature"]["output"]["kind"] = "future_output_shape"

    version = FunctionVersion.from_json(json.dumps(value))
    assert version.runtime.kind == "wasm"
    assert version.runtime.python_version is None
    assert version.runtime.environment is None
    assert json.loads(version.to_canonical_json())["runtime"] == {"kind": "wasm"}
    assert version.signature.output.kind == "future_output_shape"


def test_function_application_uses_rename_columns_only():
    application = FunctionApplication.from_json(
        fixture("remote_function_application.json")
    )
    renamed = application.rename(columns={"normalized_text": "body_normalized"})

    assert application.columns["normalized_text"] == "search_text"
    assert renamed.columns["normalized_text"] == "body_normalized"
    assert renamed.function == application.function
    assert not hasattr(application, "rename_outputs")
    with pytest.raises(TypeError, match="immutable"):
        renamed.columns["normalized_text"] = "changed"
    with pytest.raises(TypeError, match="immutable"):
        application.inputs[0].value["path"] = "changed"

    with pytest.raises(ValueError, match="unknown Function result fields"):
        application.rename(columns={"missing": "search_text"})
    with pytest.raises(ValueError, match="destinations must be unique"):
        application.rename(columns={"normalized_text": "same", "token_count": "same"})

    bare_value = json.loads(fixture("remote_function_application.json"))
    bare_value.pop("columns")
    bare = FunctionApplication(**bare_value)
    with pytest.raises(ValueError, match="destinations must be unique"):
        bare.rename(columns={"normalized_text": "token_count"})


def test_binding_and_refresh_result_keep_stable_remote_fields():
    binding = FunctionBinding.from_json(fixture("remote_function_binding.json"))
    assert binding.function.version == "fv_01K3TEXT"
    assert [output.output_ordinal for output in binding.outputs] == [0, 1]
    assert binding.input_schema is not None
    assert binding.output_schema is not None

    result = RefreshColumnResult.from_json(
        json.dumps(job_result("remote_refresh_job.json"))
    )
    assert result.rows_filled == result.rows_assigned
    assert result.version == result.published_version

    result = RefreshColumnResult.from_json(
        fixture("remote_refresh_result_without_published_version.json")
    )
    assert result.published_version is None
    assert RefreshColumnResult.from_json(result.to_canonical_json()) == result


def test_function_literal_numeric_domain_matches_rust():
    with pytest.raises(ValueError, match="floating-point Function literals"):
        FunctionApplication.from_json(fixture("remote_function_application_float.json"))

    value = json.loads(fixture("remote_function_application_float.json"))
    value["inputs"][0]["value"] = 2**64
    with pytest.raises(ValueError, match="outside the canonical JSON range"):
        FunctionApplication.from_json(json.dumps(value))


def test_empty_default_maps_have_stable_canonical_bytes():
    runtime = PythonRuntimeSpec(
        kind="python", python_version="3.12", environment={"kind": "pip"}
    )
    assert runtime.to_canonical_json() == (
        '{"environment":{"kind":"pip"},"kind":"python","python_version":"3.12"}'
    )

    value = json.loads(fixture("remote_function_application.json"))
    value.pop("columns")
    application = FunctionApplication.from_json(json.dumps(value))
    assert "columns" not in json.loads(application.to_canonical_json())


@pytest.mark.parametrize("field", ["rows_assigned", "source_version"])
def test_refresh_result_rejects_non_u64_values(field):
    value = job_result("remote_refresh_job.json")
    value[field] = -1
    with pytest.raises(ValueError):
        RefreshColumnResult.from_json(json.dumps(value))

    value[field] = "1"
    with pytest.raises(ValueError):
        RefreshColumnResult.from_json(json.dumps(value))


class _FunctionDeclarationInner:
    def __init__(self):
        self.calls = []

    async def add_function_columns(self, application_json, output_name):
        self.calls.append((json.loads(application_json), output_name))
        return "declared"


def known_application() -> FunctionApplication:
    value = json.loads(fixture("remote_function_application.json"))
    value.pop("future_application")
    return FunctionApplication(**value)


@pytest.mark.asyncio
async def test_add_columns_routes_struct_as_one_and_multi_output_binding_atomically():
    inner = _FunctionDeclarationInner()
    table = AsyncTable(inner)
    application = known_application()

    result = await table.add_columns(
        {"features": application._copy(update={"columns": {}})}
    )
    assert result == "declared"
    assert inner.calls[-1][1] == "features"

    bare = application._copy(update={"columns": {}}).rename(
        columns={"normalized_text": "search_text"}
    )
    result = await table.add_columns(bare)
    assert result == "declared"
    assert inner.calls[-1][1] is None
    assert inner.calls[-1][0]["columns"] == {"normalized_text": "search_text"}


@pytest.mark.asyncio
async def test_add_columns_rejects_multiple_bindings_and_unknown_newer_application():
    inner = _FunctionDeclarationInner()
    table = AsyncTable(inner)
    application = known_application()

    with pytest.raises(ValueError, match="exactly one Function binding"):
        await table.add_columns({"a": application, "b": application})

    future = json.loads(fixture("remote_function_application.json"))
    application = FunctionApplication(**future)
    with pytest.raises(ValueError, match="newer contract"):
        await table.add_columns(application)

    future.pop("future_application")
    future["output"]["assignment"] = "cell_flag"
    application = FunctionApplication(**future)
    assert "assignment" not in json.loads(application.to_canonical_json())["output"]
    with pytest.raises(ValueError, match="output.assignment"):
        await table.add_columns(application)
    assert inner.calls == []


def test_rename_requires_named_struct_and_keeps_partial_mapping_immutable():
    scalar = FunctionApplication.from_json(
        json.dumps(
            {
                "function": {"name": "embed", "version": "fv_exact"},
                "inputs": [],
                "output": {
                    "kind": "scalar",
                    "arrow_type": "list<float32>",
                    "nullable": False,
                },
            }
        )
    )
    with pytest.raises(ValueError, match="named-struct"):
        scalar.rename(columns={"value": "embedding"})

    application = known_application()._copy(update={"columns": {}})
    renamed = application.rename(columns={"normalized_text": "search_text"})
    assert dict(application.columns) == {}
    assert dict(renamed.columns) == {"normalized_text": "search_text"}
