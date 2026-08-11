# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""RED contract tests for the private UDF -> FunctionDefinition bridge."""

from __future__ import annotations

import base64
import io
import json
from pathlib import Path

import pyarrow as pa
import pytest

import lancedb
from lancedb import FunctionCapability, udf
from lancedb import _lancedb as _native
from lancedb import _udf as _udf_mod

_SOURCE_MARKER = "bridge-source-marker-unique-xyz"
_SECRET_REFERENCE = "secret://team/bridge-redact-token-xyz"
_SECRET_ENV = "BRIDGE_API_TOKEN"
_NETWORK_ORIGIN = "https://api.bridge-example.com"
_NETWORK_ORIGIN_B = "https://other.bridge-example.com"

_FORBIDDEN_WIRE_KEYS = (
    "id",
    "function_id",
    "FunctionId",
    "catalog",
    "catalog_name",
    "version",
    "function_version",
    "FunctionVersion",
    "lineage",
    "user_version",
    "idempotency_key",
    "digest",
    "artifact",
    "artifact_digest",
    "storage",
    "storage_location",
    "location",
    "deterministic",
    "null_policy",
    "nullPolicy",
    "timestamp",
    "created_at",
    "updated_at",
    "worker",
    "scheduler",
    "attempt",
    "attempt_id",
    "replica",
    "placement",
    "job",
    "job_id",
    "retry_key",
    "registration",
)

_OVERDESIGN_ATTRS = (
    "id",
    "function_id",
    "FunctionVersion",
    "function_version",
    "artifact",
    "digest",
    "job",
    "job_id",
    "catalog",
    "retry_key",
    "user_version",
    "idempotency_key",
    "deterministic",
    "null_policy",
    "null_handling",
)


@udf(
    inputs={"text": pa.string(), "limit": pa.int32()},
    output=pa.string(),
    python="3.12",
    packages=["pkg-b==2", "pkg-a==1"],
    output_nullable=True,
    capabilities=[
        FunctionCapability.network(_NETWORK_ORIGIN),
        FunctionCapability.secret(
            _SECRET_REFERENCE,
            environment_variable=_SECRET_ENV,
        ),
        FunctionCapability.network(_NETWORK_ORIGIN_B),
    ],
)
def packable_bridge_normalize(text, limit):
    """bridge-source-marker-unique-xyz."""
    return text[:limit]


def _build_function_definition(fn: object):
    return _udf_mod._build_function_definition(fn)


def _function_definition_type():
    return _native._FunctionDefinition


def _new_function_definition(**kwargs):
    return _native._new_function_definition(**kwargs)


def _json_bytes(definition) -> bytes:
    payload = definition._to_json()
    if isinstance(payload, bytes):
        return payload
    assert isinstance(payload, str)
    return payload.encode("utf-8")


def _decode_type_ipc(encoded: str) -> pa.DataType:
    raw = base64.b64decode(encoded)
    reader = pa.ipc.open_file(io.BytesIO(raw))
    assert reader.num_record_batches == 0
    assert len(reader.schema) == 1
    return reader.schema.field(0).type


def _assert_exact_object_keys(value: dict, expected: set[str], *, context: str) -> None:
    assert isinstance(value, dict), f"{context} must be an object"
    assert set(value) == expected, f"{context} keys must match exactly: {set(value)!r}"


def _assert_forbidden_keys_absent(value: object, *, context: str) -> None:
    if isinstance(value, dict):
        for key in value:
            assert key not in _FORBIDDEN_WIRE_KEYS, (
                f"forbidden key {key!r} at {context}: {value!r}"
            )
            if key == "name" and context in {
                "definition",
                "signature",
                "signature.output",
                "implementation",
            }:
                raise AssertionError(
                    f"catalog/function identity key `name` must be absent at {context}"
                )
            child_context = f"{context}.{key}"
            if key == "parameters" and context == "signature":
                child_context = "signature.parameters"
            _assert_forbidden_keys_absent(value[key], context=child_context)
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            item_context = (
                f"signature.parameters[{idx}]"
                if context == "signature.parameters"
                else f"{context}[{idx}]"
            )
            if context == "signature.parameters":
                assert isinstance(item, dict)
                assert "name" in item
                for key in item:
                    assert key not in _FORBIDDEN_WIRE_KEYS
                    assert key != "catalog_name"
                _assert_forbidden_keys_absent(
                    {k: v for k, v in item.items() if k != "name"},
                    context=item_context,
                )
            else:
                _assert_forbidden_keys_absent(item, context=item_context)


def _assert_sanitized_text(*parts: object) -> None:
    combined = "\n".join(str(part) for part in parts)
    lowered = combined.lower()
    assert _SOURCE_MARKER.lower() not in lowered
    assert _SECRET_REFERENCE.lower() not in lowered
    assert str(Path(__file__).resolve()).lower() not in lowered
    assert Path(__file__).resolve().as_posix().lower() not in lowered


def _assert_clean_validation_error(exc_info) -> None:
    _assert_sanitized_text(exc_info.value, repr(exc_info.value))
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def _valid_builder_kwargs(**overrides):
    kwargs = {
        "parameters": [("text", pa.string()), ("limit", pa.int32())],
        "output_type": pa.string(),
        "output_nullable": True,
        "module": "bridge_mod",
        "callable_name": "normalize",
        "source": (
            "def normalize(text, limit):\n"
            f"    # {_SOURCE_MARKER}\n"
            "    return text[:limit]\n"
        ),
        "python": "3.12",
        "packages": ["pkg-b==2", "pkg-a==1"],
        "capabilities": [
            ("network", _NETWORK_ORIGIN, None),
            ("secret", _SECRET_REFERENCE, _SECRET_ENV),
            ("network", _NETWORK_ORIGIN_B, None),
        ],
    }
    kwargs.update(overrides)
    return kwargs


def test_build_function_definition_private_native_immutability_and_export_surface():
    assert "_build_function_definition" not in getattr(lancedb, "__all__", [])
    assert "_FunctionDefinition" not in lancedb.__all__
    assert not hasattr(lancedb, "_FunctionDefinition")
    assert not hasattr(lancedb, "_build_function_definition")
    assert not hasattr(lancedb, "_new_function_definition")

    definition = _build_function_definition(packable_bridge_normalize)
    definition_type = _function_definition_type()
    assert type(definition) is definition_type
    assert definition_type.__module__ == "lancedb._lancedb"
    assert definition_type.__name__ == "_FunctionDefinition"

    with pytest.raises(TypeError):
        definition_type()

    for attr in _OVERDESIGN_ATTRS:
        assert not hasattr(definition, attr)

    for attr in ("signature", "module", "source", "capabilities"):
        with pytest.raises(AttributeError):
            setattr(definition, attr, None)


def test_build_function_definition_json_wire_ordered_contract_without_identity():
    definition = _build_function_definition(packable_bridge_normalize)
    encoded_a = _json_bytes(definition)
    encoded_b = _json_bytes(definition)
    assert encoded_a == encoded_b

    wire = json.loads(encoded_a.decode("utf-8"))
    _assert_exact_object_keys(
        wire,
        {"format_version", "signature", "implementation", "capabilities"},
        context="definition",
    )
    assert wire["format_version"] == 1
    _assert_forbidden_keys_absent(wire, context="definition")

    signature = wire["signature"]
    _assert_exact_object_keys(signature, {"parameters", "output"}, context="signature")
    parameters = signature["parameters"]
    assert [parameter["name"] for parameter in parameters] == ["text", "limit"]
    for parameter in parameters:
        _assert_exact_object_keys(
            parameter, {"name", "data_type_ipc"}, context="parameter"
        )
        assert isinstance(parameter["data_type_ipc"], str)
        assert parameter["data_type_ipc"]
    assert _decode_type_ipc(parameters[0]["data_type_ipc"]) == pa.string()
    assert _decode_type_ipc(parameters[1]["data_type_ipc"]) == pa.int32()

    output = signature["output"]
    _assert_exact_object_keys(
        output, {"data_type_ipc", "nullable"}, context="signature.output"
    )
    assert output["nullable"] is True
    assert _decode_type_ipc(output["data_type_ipc"]) == pa.string()

    implementation = wire["implementation"]
    _assert_exact_object_keys(
        implementation,
        {"kind", "module", "callable", "source", "python", "packages"},
        context="implementation",
    )
    assert implementation["kind"] == "python"
    assert implementation["module"] == __name__
    assert implementation["callable"] == "packable_bridge_normalize"
    assert implementation["source"] == Path(__file__).read_text(encoding="utf-8")
    assert _SOURCE_MARKER in implementation["source"]
    assert implementation["python"] == "3.12"
    assert implementation["packages"] == ["pkg-b==2", "pkg-a==1"]

    capabilities = wire["capabilities"]
    assert capabilities == [
        {"kind": "network", "origin": _NETWORK_ORIGIN},
        {
            "kind": "secret",
            "reference": _SECRET_REFERENCE,
            "environment_variable": _SECRET_ENV,
        },
        {"kind": "network", "origin": _NETWORK_ORIGIN_B},
    ]
    for capability in capabilities:
        assert "value" not in capability
        assert "plaintext" not in capability
        assert "plaintext_secret" not in capability
        assert "secret_value" not in capability


def test_native_definition_repr_includes_safe_structure_and_redacts_sensitive_text():
    definition = _build_function_definition(packable_bridge_normalize)
    rendered = repr(definition)
    assert "_FunctionDefinition" in rendered or "FunctionDefinition" in rendered
    assert __name__ in rendered
    assert "packable_bridge_normalize" in rendered
    assert "3.12" in rendered
    _assert_sanitized_text(rendered)


def test_new_function_definition_builder_preserves_normalized_wire():
    definition = _new_function_definition(**_valid_builder_kwargs())
    assert type(definition) is _function_definition_type()

    encoded_a = _json_bytes(definition)
    encoded_b = _json_bytes(definition)
    assert encoded_a == encoded_b

    wire = json.loads(encoded_a.decode("utf-8"))
    assert wire["format_version"] == 1
    assert [parameter["name"] for parameter in wire["signature"]["parameters"]] == [
        "text",
        "limit",
    ]
    assert _decode_type_ipc(wire["signature"]["parameters"][0]["data_type_ipc"]) == (
        pa.string()
    )
    assert _decode_type_ipc(wire["signature"]["parameters"][1]["data_type_ipc"]) == (
        pa.int32()
    )
    assert wire["signature"]["output"]["nullable"] is True
    assert _decode_type_ipc(wire["signature"]["output"]["data_type_ipc"]) == pa.string()

    implementation = wire["implementation"]
    assert implementation["kind"] == "python"
    assert implementation["module"] == "bridge_mod"
    assert implementation["callable"] == "normalize"
    assert implementation["source"] == _valid_builder_kwargs()["source"]
    assert _SOURCE_MARKER in implementation["source"]
    assert implementation["python"] == "3.12"
    assert implementation["packages"] == ["pkg-b==2", "pkg-a==1"]
    assert wire["capabilities"] == [
        {"kind": "network", "origin": _NETWORK_ORIGIN},
        {
            "kind": "secret",
            "reference": _SECRET_REFERENCE,
            "environment_variable": _SECRET_ENV,
        },
        {"kind": "network", "origin": _NETWORK_ORIGIN_B},
    ]
    _assert_forbidden_keys_absent(wire, context="definition")


@pytest.mark.parametrize(
    ("overrides",),
    [
        ({"parameters": [("text", pa.string()), ("text", pa.int32())]},),
        ({"parameters": [("", pa.string())]},),
        ({"module": ""},),
        ({"callable_name": ""},),
        ({"source": ""},),
        ({"python": ""},),
        ({"packages": ["pkg-a==1", ""]},),
        ({"packages": ["pkg-a==1", "pkg-a==1"]},),
        ({"capabilities": [("filesystem", _NETWORK_ORIGIN, None)]},),
        ({"capabilities": [("network", _NETWORK_ORIGIN, _SECRET_ENV)]},),
        ({"capabilities": [("secret", _SECRET_REFERENCE, None)]},),
        ({"capabilities": [("secret", _SECRET_REFERENCE, "")]},),
        ({"capabilities": [("network", "", None)]},),
        ({"capabilities": [("secret", "", _SECRET_ENV)]},),
    ],
)
def test_new_function_definition_strict_validation_rejections(overrides):
    kwargs = _valid_builder_kwargs(**overrides)
    with pytest.raises(ValueError) as exc_info:
        _new_function_definition(**kwargs)
    _assert_clean_validation_error(exc_info)


def test_new_function_definition_validation_does_not_echo_secret_or_source_marker():
    with pytest.raises(ValueError) as exc_info:
        _new_function_definition(**_valid_builder_kwargs(module=""))
    _assert_clean_validation_error(exc_info)

    with pytest.raises(ValueError) as exc_info:
        _new_function_definition(
            **_valid_builder_kwargs(packages=["pkg-a==1", "pkg-a==1"])
        )
    _assert_clean_validation_error(exc_info)

    with pytest.raises(ValueError) as exc_info:
        _new_function_definition(
            **_valid_builder_kwargs(
                capabilities=[("secret", _SECRET_REFERENCE, None)],
            )
        )
    _assert_clean_validation_error(exc_info)


@pytest.mark.parametrize(
    ("overrides",),
    [
        ({"parameters": [("text", "not-a-datatype")]},),
        ({"parameters": [(123, pa.string())]},),
        ({"output_type": "not-a-datatype"},),
        ({"output_type": None},),
        ({"output_nullable": "yes"},),
        ({"packages": "pkg-a==1"},),
        ({"capabilities": "network"},),
        ({"capabilities": [("network", _NETWORK_ORIGIN)]},),
        ({"capabilities": [("network", _NETWORK_ORIGIN, None, "extra")]},),
    ],
)
def test_new_function_definition_wrong_pyarrow_and_shape_values_fail_closed(overrides):
    kwargs = _valid_builder_kwargs(**overrides)
    with pytest.raises((TypeError, ValueError)) as exc_info:
        _new_function_definition(**kwargs)
    _assert_clean_validation_error(exc_info)


class _HostileRaisingIterable:
    def __iter__(self):
        raise RuntimeError(f"{_SECRET_REFERENCE} {_SOURCE_MARKER}")


@pytest.mark.parametrize(
    ("overrides",),
    [
        ({"parameters": _HostileRaisingIterable()},),
        ({"packages": _HostileRaisingIterable()},),
        ({"capabilities": _HostileRaisingIterable()},),
        (
            {
                "capabilities": [
                    ("network", _NETWORK_ORIGIN, None),
                    _HostileRaisingIterable(),
                    ("network", _NETWORK_ORIGIN_B, None),
                ]
            },
        ),
    ],
)
def test_new_function_definition_hostile_iterable_iter_raises_fail_closed(overrides):
    kwargs = _valid_builder_kwargs(**overrides)
    with pytest.raises((TypeError, ValueError)) as exc_info:
        _new_function_definition(**kwargs)
    _assert_clean_validation_error(exc_info)


@udf(
    inputs={"x": pa.int32()},
    output=pa.int64(),
    python="3.12",
    packages=["pkg-a==1"],
    output_nullable=False,
)
def packable_bridge_capability_exact_type(x):
    return x + 1


def test_build_function_definition_rejects_forged_function_capability_subclass():
    marker = f"{_SECRET_REFERENCE} {_SOURCE_MARKER}"

    class _HostileFunctionCapability(FunctionCapability):
        @property
        def kind(self) -> str:
            raise RuntimeError(marker)

        @property
        def origin(self) -> str | None:
            raise RuntimeError(marker)

        @property
        def reference(self) -> str | None:
            raise RuntimeError(marker)

        @property
        def environment_variable(self) -> str | None:
            raise RuntimeError(marker)

    hostile = object.__new__(_HostileFunctionCapability)
    assert isinstance(hostile, FunctionCapability)
    assert type(hostile) is not FunctionCapability

    config_attr = _udf_mod._CONFIG_ATTR
    original = getattr(packable_bridge_capability_exact_type, config_attr)
    forged = _udf_mod._UdfConfig(
        inputs=original.inputs,
        output=original.output,
        output_nullable=original.output_nullable,
        python=original.python,
        packages=original.packages,
        capabilities=(hostile,),
    )
    setattr(packable_bridge_capability_exact_type, config_attr, forged)
    try:
        with pytest.raises((TypeError, ValueError)) as exc_info:
            _build_function_definition(packable_bridge_capability_exact_type)
        _assert_clean_validation_error(exc_info)
        assert marker not in str(exc_info.value)
        assert marker not in repr(exc_info.value)
    finally:
        setattr(packable_bridge_capability_exact_type, config_attr, original)
