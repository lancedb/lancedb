# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
from pathlib import Path

import pytest

import lancedb.functions as functions
from lancedb.functions import (
    FunctionApplication,
    FunctionBinding,
    FunctionVersion,
    PythonRuntimeSpec,
    RefreshColumnResult,
)


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


def assert_no_secret_values(value):
    if isinstance(value, dict):
        for key, child in value.items():
            assert key not in {
                "secret_value",
                "secret_values",
                "resolved_secret",
                "resolved_secrets",
            }
            assert_no_secret_values(child)
    elif isinstance(value, list):
        for child in value:
            assert_no_secret_values(child)


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
    assert version.required_secrets == ("HF_TOKEN",)

    with pytest.raises((TypeError, ValueError)):
        version.version = "fv_changed"
    with pytest.raises(TypeError, match="immutable"):
        version.runtime.env["TOKENIZERS_PARALLELISM"] = "true"

    changed = dict(value)
    changed["version"] = "fv_changed"
    assert FunctionVersion(**changed) != version


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
    assert renamed.group_id == application.group_id
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
    assert binding.revision == 3
    assert binding.function.version == "fv_01K3TEXT"
    assert [output.output_ordinal for output in binding.outputs] == [0, 1]

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


def test_canonical_client_values_contain_secret_names_only():
    version = FunctionVersion.from_json(
        json.dumps(job_result("remote_function_job.json"))
    )
    canonical = json.loads(version.to_canonical_json())
    assert canonical["required_secrets"] == ["HF_TOKEN"]
    assert_no_secret_values(canonical)
