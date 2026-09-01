# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from __future__ import annotations

import base64
import contextlib
import functools
import importlib.util
import types
from datetime import date
import http.server
import json
from pathlib import Path
import subprocess
import sys
import threading
from typing import Optional

import pyarrow as pa
import pytest

import lancedb
from lancedb.functions import (
    PythonAdapterSpec,
    PythonRuntimeSpec,
    UdfDefinition,
    _canonical_arrow_type,
    _GRAMMAR_PRIMITIVES,
    udf,
)

THRESHOLD = 20
_CACHE = None


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
    python_version="3.12",
)
def normalize_score(value: float) -> float:
    return value / 100.0


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


def _main_udf_source(
    *, threshold: int = 20, input_annotation: str = "int", comparison: str = ">="
) -> str:
    return (
        "from __future__ import annotations\n"
        "from lancedb.functions import udf\n"
        f"THRESHOLD = {threshold}\n"
        "\n"
        "@udf\n"
        f"def label(value: {input_annotation}) -> str:\n"
        f"    return 'big' if value {comparison} THRESHOLD else 'small'\n"
        "\n"
        "assert label.__module__ == '__main__'\n"
        "print(label.registration_request.to_canonical_json())\n"
    )


def _run_main_udf(path: Path, source: str) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source)
    result = subprocess.run(
        [sys.executable, str(path)],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def test_main_udf_registration_identity_is_stable_across_processes_and_paths(
    tmp_path,
):
    source = _main_udf_source()
    original_path = tmp_path / "original" / "job.py"
    moved_path = tmp_path / "moved" / "renamed_job.py"

    original_runs = [_run_main_udf(original_path, source) for _ in range(2)]
    moved_run = _run_main_udf(moved_path, source)

    assert len({run["artifact"]["digest"] for run in [*original_runs, moved_run]}) == 1
    assert all(
        run["signature"] == original_runs[0]["signature"]
        for run in [original_runs[1], moved_run]
    )
    assert original_runs[0] == original_runs[1] == moved_run

    body_change = _run_main_udf(
        tmp_path / "changes" / "body.py", _main_udf_source(comparison=">")
    )
    global_change = _run_main_udf(
        tmp_path / "changes" / "global.py", _main_udf_source(threshold=21)
    )
    annotation_change = _run_main_udf(
        tmp_path / "changes" / "annotation.py",
        _main_udf_source(input_annotation="float"),
    )

    baseline = original_runs[0]
    assert baseline["signature"] == body_change["signature"]
    assert baseline["signature"] == global_change["signature"]
    assert baseline["signature"] != annotation_change["signature"]
    assert (
        len(
            {
                baseline["artifact"]["digest"],
                body_change["artifact"]["digest"],
                global_change["artifact"]["digest"],
                annotation_change["artifact"]["digest"],
            }
        )
        == 4
    )


def _run_packaged(definition, *args):
    """Execute the shipped artifact in a fresh namespace, as a worker would."""
    source = base64.b64decode(definition.registration_request.artifact.content.data)
    namespace: dict = {}
    exec(compile(source, "<udf>", "exec"), namespace)
    return namespace[definition.registration_request.artifact.entrypoint](*args)


def test_udf_conda_environment():
    @udf(conda=["scipy", "numpy"], conda_channels=["conda-forge", "defaults"])
    def halve(value: float) -> float:
        return value / 2

    request = json.loads(halve.registration_request.to_canonical_json())
    assert request["runtime"]["environment"] == {
        "kind": "conda",
        "packages": ["numpy", "scipy"],
        "channels": ["conda-forge", "defaults"],
    }
    pip_request = json.loads(normalize_score.registration_request.to_canonical_json())
    assert "channels" not in pip_request["runtime"]["environment"]

    with pytest.raises(ValueError, match="not both"):
        udf(name="both", pip=["numpy"], conda=["numpy"])(lambda value: value)
    with pytest.raises(ValueError, match="requires conda"):
        udf(name="channels", conda_channels=["conda-forge"])(lambda value: value)


def test_udf_gpu_marker_uses_gpu_runtime():
    @udf(pip=["cupy-cuda12x"], gpu=True)
    def double_on_gpu(value: int) -> int:
        return value * 2

    request = json.loads(double_on_gpu.registration_request.to_canonical_json())
    assert request["runtime"]["kind"] == "python_v2"
    assert request["runtime"]["gpu"] is True

    @udf(pip=["pyarrow"])
    def cpu_function(value: int) -> int:
        return value

    cpu_runtime = json.loads(cpu_function.registration_request.to_canonical_json())[
        "runtime"
    ]
    assert cpu_runtime["kind"] == "python"
    assert "gpu" not in cpu_runtime

    def identity(value: int) -> int:
        return value

    for invalid in [None, 0, 1, -1, 1.5, "", "true", "1", "H100"]:
        with pytest.raises(ValueError, match="gpu must be a boolean"):
            udf(name="invalid_gpu", gpu=invalid)(identity)

    base_runtime = {
        "kind": "python_v2",
        "python_version": "3.12",
        "environment": {"kind": "pip"},
    }
    runtime = PythonRuntimeSpec.model_validate({**base_runtime, "gpu": True})
    assert runtime.gpu is True
    for invalid in [False, 1, 0, "", "true", "1", "H100"]:
        with pytest.raises(ValueError, match="runtime.gpu must be true"):
            PythonRuntimeSpec.model_validate({**base_runtime, "gpu": invalid})


def test_unknown_runtime_discards_payload_before_known_field_validation():
    for payload in [
        {"kind": "python_v3", "gpu": {"model": "H100"}},
        {"kind": "python_v3", "resources": []},
        {
            "kind": "python_v3",
            "environment": {"kind": []},
            "python_version": 3.15,
        },
    ]:
        runtime = PythonRuntimeSpec.model_validate(payload)
        assert runtime.to_canonical_json() == '{"kind":"python_v3"}'


def test_udf_packages_attribute_access_and_body_imports():
    @udf
    def word_norm(body: str) -> float:
        import numpy as np

        try:
            words = body.split()
        except AttributeError as error:
            raise ValueError(str(error)) from error
        return float(np.linalg.norm([len(w) for w in words]))

    assert _run_packaged(word_norm, "aa bb") == pytest.approx(8**0.5)


def test_udf_packages_module_globals_and_global_caches():
    @udf
    def label(value: int) -> str:
        return "big" if value >= THRESHOLD else "small"

    assert _run_packaged(label, 21) == "big"

    @udf
    def cached(value: int) -> int:
        global _CACHE
        if _CACHE is None:
            _CACHE = 40
        return _CACHE + value

    assert _run_packaged(cached, 2) == 42


def test_udf_annotations_are_not_runtime_names():
    @udf
    def identity(value: date) -> date:
        return value

    assert _run_packaged(identity, date(2026, 8, 25)) == date(2026, 8, 25)


def test_udf_nested_scopes_resolve_lexically():
    @udf
    def score(value: int) -> int:
        offset = 2

        def add_offset() -> int:
            return value + offset

        return add_offset() + sum(v for v in [0])

    assert _run_packaged(score, 3) == 5


def test_udf_resolves_module_globals_before_builtins(tmp_path):
    module_path = tmp_path / "shadowing_udfs.py"
    module_path.write_text(
        "max = 7\n"
        "len = lambda _: 99\n"
        "\n"
        "def uses_literal_shadow(value: int) -> int:\n"
        "    def nested() -> int:\n"
        "        return max\n"
        "    return nested() + value\n"
        "\n"
        "def uses_callable_shadow(value: int) -> int:\n"
        "    def nested() -> int:\n"
        "        return len([1])\n"
        "    return nested() + value\n"
    )
    spec = importlib.util.spec_from_file_location("shadowing_udfs", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # The module's `max = 7` is what the interpreter would use, so it ships.
    assert _run_packaged(udf(module.uses_literal_shadow), 1) == 8
    # A callable global cannot ship; it must not be silently swapped for the builtin.
    with pytest.raises(TypeError, match="unsupported global value of type function"):
        udf(module.uses_callable_shadow)


def test_canonical_arrow_type_prefers_the_compact_grammar():
    golden = json.loads(
        (
            Path(__file__).parents[3]
            / "rust/lancedb/tests/fixtures/first_class_functions/v1/arrow_types.json"
        ).read_text()
    )
    primitives = [
        case["arrow_type"] for case in golden["valid"] if "<" not in case["arrow_type"]
    ]
    assert [name for _, name in _GRAMMAR_PRIMITIVES] == primitives
    assert _canonical_arrow_type(pa.list_(pa.field("item", pa.float32(), False))) == (
        "list<float32>"
    )
    assert (
        _canonical_arrow_type(pa.large_list(pa.field("item", pa.float32(), False)))
        == "large_list<float32>"
    )
    for outside in [
        pa.timestamp("us"),
        pa.decimal128(10, 2),
        pa.large_binary(),
        pa.binary(4),
        pa.duration("s"),
        pa.list_(pa.float32(), 0),
        pa.list_(pa.timestamp("us")),
    ]:
        with pytest.raises(TypeError, match="unsupported Arrow type"):
            _canonical_arrow_type(outside)


def test_udf_nested_annotations_are_postponed_in_the_artifact():
    @udf
    def score(value: int) -> int:
        def identity(item: date) -> date:
            return item

        identity(date(2026, 8, 25))
        return value

    assert _run_packaged(score, 3) == 3


def test_udf_ships_globals_the_body_deletes():
    @udf
    def clear(value: int) -> int:
        global _CACHE
        del _CACHE
        return value

    assert _run_packaged(clear, 3) == 3


def test_udf_rejects_a_module_global_that_does_not_import_as_itself(tmp_path):
    module_path = tmp_path / "fake_module_udfs.py"
    module_path.write_text(
        "import types\n"
        "np = types.ModuleType('numpy')\n"
        "np.sqrt = lambda x: 0\n"
        "\n"
        "def score(value: int) -> int:\n"
        "    return int(np.sqrt(value))\n"
    )
    spec = importlib.util.spec_from_file_location("fake_module_udfs", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with pytest.raises(TypeError, match="does not import as 'numpy'"):
        udf(module.score)


def test_udf_rejects_a_module_level_namespace_alias(tmp_path):
    module_path = tmp_path / "aliasing_udfs.py"
    module_path.write_text(
        "import builtins as b\n"
        "THRESHOLD = 5\n"
        "\n"
        "def score(value: int) -> int:\n"
        "    return value + b.vars(b.__import__('aliasing_udfs'))['THRESHOLD']\n"
    )
    spec = importlib.util.spec_from_file_location("aliasing_udfs", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with pytest.raises(ValueError, match="dynamic namespace access"):
        udf(module.score)


@pytest.mark.parametrize(
    "access",
    [
        "globals()['THRESHOLD']",
        "eval('THRESHOLD')",
        "(lambda g: g()['THRESHOLD'])(globals)",
        "__import__('sys').modules[__name__].THRESHOLD",
        "sys.modules[__name__].THRESHOLD",
    ],
)
def test_udf_rejects_dynamic_namespace_access(access):
    namespace: dict = {}
    exec(
        f"def score(value: int) -> int:\n    return value + {access}\n",
        {"THRESHOLD": 5},
        namespace,
    )
    with pytest.raises(ValueError, match="dynamic namespace access"):
        _package_from_text(
            "def score(value: int) -> int:\n"
            "    import sys\n"
            f"    return value + {access}\n"
        )


def _package_from_text(source: str, module_globals: dict | None = None):
    """Load `source` as a real module file so the packager can inspect it."""
    import tempfile

    directory = tempfile.mkdtemp()
    path = Path(directory) / "generated_udf_module.py"
    path.write_text(source)
    spec = importlib.util.spec_from_file_location(f"generated_udf_{id(source)}", path)
    module = importlib.util.module_from_spec(spec)
    if module_globals:
        module.__dict__.update(module_globals)
    spec.loader.exec_module(module)
    functions = [
        value
        for value in vars(module).values()
        if callable(value) and getattr(value, "__module__", None) == module.__name__
    ]
    return udf(functions[0])


def test_udf_rejects_a_non_standard_builtins_environment():
    def score(value: int) -> int:
        return len([1]) + value

    score.__globals__  # noqa: B018 -- real function, real globals
    import builtins

    patched = types.FunctionType(
        score.__code__,
        {"__builtins__": {**vars(builtins), "len": lambda _: 99}},
        "score",
    )
    patched.__annotations__ = score.__annotations__
    assert patched(3) == 102
    with pytest.raises(ValueError, match="non-standard builtins environment"):
        udf(patched)

    class ReportingDict(dict):  # reports standard entries, resolves differently
        def __missing__(self, key):
            return vars(builtins)[key]

    disguised = types.FunctionType(
        score.__code__, {"__builtins__": ReportingDict(len=lambda _: 99)}, "score"
    )
    disguised.__annotations__ = score.__annotations__
    assert disguised(3) == 102
    with pytest.raises(ValueError, match="non-standard builtins environment"):
        udf(disguised)

    hooked = types.FunctionType(
        score.__code__,
        {"__builtins__": {**vars(builtins), "__import__": lambda *a, **k: None}},
        "score",
    )
    hooked.__annotations__ = score.__annotations__
    with pytest.raises(ValueError, match="non-standard builtins environment"):
        udf(hooked)


def test_udf_recursion_versus_a_rebound_module_name(tmp_path):
    module_path = tmp_path / "rebound_udfs.py"
    module_path.write_text(
        "def fact(value: int) -> int:\n"
        "    return 1 if value <= 1 else value * fact(value - 1)\n"
        "\n"
        "def score(value: int) -> int:\n"
        "    return score + value\n"
    )
    spec = importlib.util.spec_from_file_location("rebound_udfs", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert _run_packaged(udf(module.fact), 5) == 120
    raw = module.score
    module.score = 10
    with pytest.raises(ValueError, match="binds that name to another value"):
        udf(raw)
    # A wrapper that merely exposes __wrapped__ is not the function.
    module.score = functools.wraps(raw)(lambda value: 41)
    with pytest.raises(ValueError, match="binds that name to another value"):
        udf(raw)
    # The decorator's own result is; a subclass of it is not.
    module.fact = udf(module.fact)
    assert _run_packaged(module.fact, 4) == 24

    class Twisted(UdfDefinition):
        def __call__(self, *args, **kwargs):
            return 41

    raw_fact = module.fact._function
    module.fact = Twisted(
        raw_fact,
        name=None,
        input_schema=None,
        output_schema=None,
        pip=(),
        env={},
        python_version=None,
    )
    with pytest.raises(ValueError, match="binds that name to another value"):
        udf(raw_fact)


def test_canonical_arrow_type_uses_exact_json_for_list_child_properties():
    nullable = pa.list_(pa.float32())
    assert json.loads(_canonical_arrow_type(nullable)) == {
        "type": "list",
        "fields": [
            {
                "name": "item",
                "nullable": True,
                "type": {"type": "float32"},
            }
        ],
    }
    named = pa.list_(pa.field("custom", pa.float32(), nullable=False))
    assert json.loads(_canonical_arrow_type(named))["fields"][0]["name"] == "custom"
    for outside in [
        pa.list_(pa.field("item", pa.float32(), nullable=False, metadata={"k": "v"})),
        pa.list_(pa.field("item", pa.float32(), nullable=False), 0),
        pa.list_(
            pa.field("item", pa.float32(), nullable=False, metadata={"k": "v"}), 3
        ),
        pa.list_(pa.field("custom", pa.float32(), nullable=False), 3),
    ]:
        with pytest.raises(TypeError, match="unsupported Arrow type"):
            _canonical_arrow_type(outside)
    assert (
        _canonical_arrow_type(
            pa.list_(pa.field("item", pa.float32(), nullable=False), 3)
        )
        == "fixed_size_list<float32, 3>"
    )
    fixed = json.loads(_canonical_arrow_type(pa.list_(pa.float32(), 3)))
    assert fixed == {
        "type": "fixed_size_list",
        "fields": [
            {
                "name": "item",
                "nullable": True,
                "type": {"type": "float32"},
            }
        ],
        "length": 3,
    }
    large = json.loads(_canonical_arrow_type(pa.large_list(pa.float32())))
    assert large["type"] == "large_list"
    assert large["fields"][0]["nullable"] is True

    for invalid_struct in [
        pa.struct([]),
        pa.struct([pa.field("a", pa.int32()), pa.field("a", pa.int64())]),
        pa.struct([pa.field("", pa.int32())]),
    ]:
        with pytest.raises(TypeError, match="unsupported Arrow type"):
            _canonical_arrow_type(invalid_struct)


def _calls_missing(value: int) -> int:
    return missing(value)  # noqa: F821


def _shadows_missing_in_a_comprehension(value: int) -> int:
    return missing(value) + sum(missing for missing in ())  # noqa: F821


def _shadows_missing_in_a_lambda(value: int) -> int:
    return (lambda missing: missing)(value) + missing  # noqa: F821


@pytest.mark.parametrize(
    "function",
    [_calls_missing, _shadows_missing_in_a_comprehension, _shadows_missing_in_a_lambda],
)
def test_udf_rejects_a_truly_unresolved_global(function):
    with pytest.raises(ValueError, match=r"unresolved global names: \['missing'\]"):
        udf(function)


def _arrow_type_from_golden(spec: dict) -> pa.DataType:
    kind = spec["type"]
    if kind in ("list", "large_list", "fixed_size_list"):
        item = _arrow_type_from_golden(spec["fields"][0]["type"])
        field = pa.field("item", item, nullable=False)
        if kind == "list":
            return pa.list_(field)
        if kind == "large_list":
            return pa.large_list(field)
        return pa.list_(field, spec["length"])
    return {
        "null": pa.null(),
        "bool": pa.bool_(),
        "utf8": pa.string(),
        "large_utf8": pa.large_string(),
        "binary": pa.binary(),
        "float16": pa.float16(),
        "float32": pa.float32(),
        "float64": pa.float64(),
        "date32": pa.date32(),
        "date64": pa.date64(),
    }.get(kind) or getattr(pa, kind)()


def test_arrow_type_grammar_matches_the_shared_golden():
    golden = json.loads(
        (
            Path(__file__).parents[3]
            / "rust/lancedb/tests/fixtures/first_class_functions/v1/arrow_types.json"
        ).read_text()
    )
    emitted = {
        case["arrow_type"]: _canonical_arrow_type(_arrow_type_from_golden(case["json"]))
        for case in golden["valid"]
    }
    assert emitted == {
        case["arrow_type"]: case["arrow_type"] for case in golden["valid"]
    }
    assert not set(emitted) & set(golden["invalid"])
    for case in golden["server_only"]:
        with pytest.raises(TypeError, match="unsupported Arrow type"):
            _canonical_arrow_type(_arrow_type_from_golden(case["json"]))


def test_explicit_arrow_schema_is_deterministic():
    input_schema = pa.schema([pa.field("value", pa.float32(), nullable=True)])
    output_schema = pa.field(
        "embedding",
        pa.list_(pa.field("item", pa.float32(), nullable=False), 3),
        nullable=False,
    )

    @udf(input_schema=input_schema, output_schema=output_schema)
    def explicit(value):
        return [value, value, value]

    signature = explicit.registration_request.signature
    assert signature.inputs[0].arrow_type == "float32"
    assert signature.inputs[0].nullable is True
    assert signature.output.arrow_type == "fixed_size_list<float32, 3>"
    assert signature.output.nullable is False


def test_blob_fields_use_the_scalar_function_semantic_type():
    @udf(
        input_schema=pa.schema([lancedb.blob("image", nullable=False)]),
        output_schema=lancedb.blob("result", nullable=False),
    )
    def copy_blob(image):
        return image

    signature = copy_blob.registration_request.signature
    assert signature.inputs[0].arrow_type == "blob_v2"
    assert signature.output.kind == "scalar"
    assert signature.output.arrow_type == "blob_v2"


def test_whole_named_struct_function_can_include_a_blob_result_field():
    @udf(
        input_schema=pa.schema([lancedb.blob("image", nullable=False)]),
        output_schema=pa.field(
            "payload",
            pa.struct(
                [
                    pa.field("mime_type", pa.string(), nullable=False),
                    lancedb.blob("image", nullable=False),
                ]
            ),
            nullable=False,
        ),
    )
    def inspect_blob(image):
        return {"mime_type": "image/png", "image": image}

    output = inspect_blob.registration_request.signature.output
    assert output.kind == "named_struct"
    assert [(field.name, field.arrow_type) for field in output.fields] == [
        ("mime_type", "utf8"),
        ("image", "blob_v2"),
    ]


def test_struct_blob_signature_fields_preserve_exact_metadata_and_nullability():
    nested_input = pa.field(
        "payload",
        pa.struct(
            [
                pa.field("mime_type", pa.string(), nullable=False),
                pa.field(
                    "nested",
                    pa.struct([lancedb.blob("image", nullable=True)]),
                    nullable=True,
                ),
            ]
        ),
        nullable=True,
    )
    nested_output = pa.field(
        "result",
        pa.struct(
            [
                pa.field("mime_type", pa.string(), nullable=False),
                pa.field(
                    "nested",
                    pa.struct([lancedb.blob("image", nullable=True)]),
                    nullable=False,
                ),
            ]
        ),
        nullable=False,
    )

    @udf(input_schema=pa.schema([nested_input]), output_schema=nested_output)
    def copy_payload(payload):
        return payload

    signature = copy_payload.registration_request.signature
    input_type = json.loads(signature.inputs[0].arrow_type)
    assert input_type["fields"][1]["nullable"] is True
    input_blob = input_type["fields"][1]["type"]["fields"][0]
    assert input_blob["nullable"] is True
    assert input_blob["metadata"] == {"ARROW:extension:name": "lance.blob.v2"}

    assert signature.output.kind == "named_struct"
    nested_result = next(
        field for field in signature.output.fields if field.name == "nested"
    )
    output_type = json.loads(nested_result.arrow_type)
    output_blob = output_type["fields"][0]
    assert output_blob["nullable"] is True
    assert output_blob["metadata"] == {"ARROW:extension:name": "lance.blob.v2"}


def test_struct_blob_signature_supports_multiple_struct_levels():
    recursive = pa.field(
        "value",
        pa.struct(
            [
                pa.field(
                    "level_1",
                    pa.struct(
                        [
                            pa.field(
                                "level_2",
                                pa.struct([lancedb.blob("image", nullable=False)]),
                                nullable=False,
                            )
                        ]
                    ),
                    nullable=False,
                )
            ]
        ),
        nullable=False,
    )

    @udf(
        input_schema=pa.schema([recursive]),
        output_schema=pa.field("size", pa.int64(), nullable=False),
    )
    def blob_size(value):
        return len(value["level_1"]["level_2"]["image"])

    encoded = json.loads(blob_size.registration_request.signature.inputs[0].arrow_type)
    blob = encoded["fields"][0]["type"]["fields"][0]["type"]["fields"][0]
    assert blob["metadata"]["ARROW:extension:name"] == "lance.blob.v2"


@pytest.mark.parametrize(
    "data_type",
    [
        pa.list_(lancedb.blob("item", nullable=False)),
        pa.large_list(lancedb.blob("item", nullable=False)),
        pa.list_(lancedb.blob("item", nullable=False), 2),
        pa.map_(pa.string(), lancedb.blob("value", nullable=False).type),
    ],
)
def test_blob_signature_rejects_collection_ancestors(data_type):
    with pytest.raises(
        TypeError,
        match="Blob v2 fields nested under collection types are not supported",
    ):

        @udf(
            input_schema=pa.schema([pa.field("value", data_type, nullable=False)]),
            output_schema=pa.field("size", pa.int64(), nullable=False),
        )
        def blob_size(value):
            return len(value)


def test_blob_signature_rejects_collection_below_a_struct():
    nested = pa.field(
        "value",
        pa.struct(
            [
                pa.field(
                    "images",
                    pa.list_(lancedb.blob("item", nullable=False)),
                    nullable=False,
                )
            ]
        ),
        nullable=False,
    )
    with pytest.raises(
        TypeError,
        match="Blob v2 fields nested under collection types are not supported",
    ):

        @udf(
            input_schema=pa.schema([nested]),
            output_schema=pa.field("size", pa.int64(), nullable=False),
        )
        def blob_size(value):
            return len(value["images"])
def test_blob_fields_support_vectorized_pyarrow_arrays():
    @udf(
        input_schema=pa.schema([lancedb.blob("image", nullable=True)]),
        output_schema=lancedb.blob("result", nullable=False),
    )
    def copy_blobs(image: pa.Array) -> pa.Array:
        return pa.array(
            [value.as_py() if value.is_valid else b"" for value in image],
            type=pa.large_binary(),
        )

    values = pa.array([b"large blob", b"", None], type=pa.large_binary())
    result = copy_blobs(values)
    assert isinstance(result, pa.LargeBinaryArray)
    assert result.to_pylist() == [b"large blob", b"", b""]

    signature = copy_blobs.registration_request.signature
    assert signature.inputs[0].arrow_type == "blob_v2"
    assert signature.inputs[0].nullable is True
    assert signature.output.arrow_type == "blob_v2"
    assert signature.output.nullable is False
    assert copy_blobs.registration_request.artifact.adapter == PythonAdapterSpec(
        kind="arrow_arrays", version=1
    )

    source = base64.b64decode(
        copy_blobs.registration_request.artifact.content.data
    ).decode("utf-8")
    assert "def copy_blobs(image: pa.Array) -> pa.Array:" in source


def test_vectorized_udf_requires_a_complete_array_contract():
    with pytest.raises(TypeError, match="every parameter and the return value"):

        @udf(
            input_schema=pa.schema([lancedb.blob("image", nullable=False)]),
            output_schema=lancedb.blob("result", nullable=False),
        )
        def missing_array_output(image: pa.Array):
            return image

    with pytest.raises(TypeError, match="require input_schema and output_schema"):

        @udf
        def missing_array_schema(image: pa.Array) -> pa.Array:
            return image


def test_named_struct_function_can_include_a_blob_result_field():
    @udf(
        input_schema=pa.schema([lancedb.blob("image", nullable=False)]),
        output_schema=pa.schema(
            [
                lancedb.blob("thumbnail", nullable=False),
                pa.field("width", pa.int32(), nullable=False),
            ]
        ),
    )
    def inspect_blob(image):
        return {"thumbnail": image, "width": 1}

    output = inspect_blob.registration_request.signature.output
    assert output.kind == "named_struct"
    assert [(field.name, field.arrow_type) for field in output.fields] == [
        ("thumbnail", "blob_v2"),
        ("width", "int32"),
    ]


def test_metadata_marked_blob_field_uses_the_semantic_type():
    extension = lancedb.blob("image", nullable=False).type
    storage = (
        extension.storage_type if isinstance(extension, pa.ExtensionType) else extension
    )
    metadata_blob = pa.field(
        "image",
        storage,
        nullable=False,
        metadata={"ARROW:extension:name": "lance.blob.v2"},
    )

    @udf(
        input_schema=pa.schema([metadata_blob]),
        output_schema=pa.field("size", pa.int64(), nullable=False),
    )
    def blob_size(image):
        return len(image)

    assert blob_size.registration_request.signature.inputs[0].arrow_type == "blob_v2"


def test_blob_marker_rejects_invalid_storage_layout():
    malformed = pa.field(
        "image",
        pa.int64(),
        nullable=False,
        metadata={"ARROW:extension:name": "lance.blob.v2"},
    )

    with pytest.raises(TypeError, match="requires a supported Blob storage layout"):

        @udf(
            input_schema=pa.schema([malformed]),
            output_schema=pa.field("size", pa.int64(), nullable=False),
        )
        def blob_size(image):
            return len(image)


def test_nested_non_blob_extension_is_not_silently_unwrapped():
    class TestExtension(pa.ExtensionType):
        def __init__(self):
            super().__init__(pa.int64(), "test.function.extension")

        def __arrow_ext_serialize__(self):
            return b""

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type, serialized):
            return cls()

    nested = pa.field(
        "value",
        pa.struct([pa.field("extended", TestExtension(), nullable=False)]),
        nullable=False,
    )
    with pytest.raises(TypeError, match="unsupported Arrow type"):

        @udf(
            input_schema=pa.schema([nested]),
            output_schema=pa.field("result", pa.int64(), nullable=False),
        )
        def extension_value(value):
            return value["extended"]


def test_explicit_large_utf8_schemas_use_the_canonical_function_name():
    input_schema = pa.schema([pa.field("text", pa.large_string(), nullable=True)])
    output_schema = pa.field("result", pa.large_string(), nullable=False)

    @udf(input_schema=input_schema, output_schema=output_schema)
    def preserve(text):
        return text

    signature = preserve.registration_request.signature
    assert signature.inputs[0].arrow_type == "large_utf8"
    assert signature.inputs[0].nullable is True
    assert signature.output.arrow_type == "large_utf8"
    assert signature.output.nullable is False

    nested = pa.struct([pa.field("text", pa.large_string(), nullable=True)])
    assert json.loads(_canonical_arrow_type(nested)) == {
        "type": "struct",
        "fields": [
            {
                "name": "text",
                "nullable": True,
                "type": {"type": "large_utf8"},
            }
        ],
    }


def test_nested_struct_output_uses_canonical_exact_json():
    token = pa.struct(
        [
            pa.field("position", pa.int32(), nullable=False),
            pa.field("value", pa.string(), nullable=False),
            pa.field("length", pa.int32(), nullable=False),
        ]
    )
    analysis = pa.struct(
        [
            pa.field("normalized_text", pa.string(), nullable=False),
            pa.field("has_content", pa.bool_(), nullable=False),
            pa.field(
                "metrics",
                pa.struct(
                    [
                        pa.field("character_count", pa.int64(), nullable=False),
                        pa.field("word_count", pa.int32(), nullable=False),
                        pa.field("average_word_length", pa.float64(), nullable=False),
                    ]
                ),
                nullable=False,
            ),
            pa.field(
                "diagnostics",
                pa.struct(
                    [
                        pa.field("status", pa.string(), nullable=False),
                        pa.field(
                            "normalization",
                            pa.struct(
                                [
                                    pa.field("changed", pa.bool_(), nullable=False),
                                    pa.field(
                                        "original_length", pa.int64(), nullable=False
                                    ),
                                ]
                            ),
                            nullable=False,
                        ),
                    ]
                ),
                nullable=False,
            ),
            pa.field(
                "token_preview",
                pa.list_(pa.field("item", token, nullable=False)),
                nullable=False,
            ),
        ]
    )

    @udf(
        input_schema=pa.schema([pa.field("text", pa.string(), nullable=False)]),
        output_schema=pa.field("analysis", analysis, nullable=False),
    )
    def analyze(text):
        return {"normalized_text": text}

    output = analyze.registration_request.signature.output
    assert output.kind == "named_struct"
    assert [field.name for field in output.fields] == [
        "normalized_text",
        "has_content",
        "metrics",
        "diagnostics",
        "token_preview",
    ]
    metrics = json.loads(output.fields[2].arrow_type)
    assert metrics == {
        "type": "struct",
        "fields": [
            {
                "name": "character_count",
                "nullable": False,
                "type": {"type": "int64"},
            },
            {
                "name": "word_count",
                "nullable": False,
                "type": {"type": "int32"},
            },
            {
                "name": "average_word_length",
                "nullable": False,
                "type": {"type": "float64"},
            },
        ],
    }
    preview = json.loads(output.fields[4].arrow_type)
    assert preview["type"] == "list"
    assert preview["fields"][0]["type"]["type"] == "struct"
    assert [field["name"] for field in preview["fields"][0]["type"]["fields"]] == [
        "position",
        "value",
        "length",
    ]


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

    for invalid_field in [
        pa.field("", pa.int32(), nullable=False),
        pa.field("result", pa.int32(), nullable=False, metadata={"k": "v"}),
    ]:
        with pytest.raises(TypeError, match="unsupported Arrow type"):

            @udf(
                input_schema=pa.schema([pa.field("value", pa.int64())]),
                output_schema=pa.schema([invalid_field]),
            )
            def invalid_explicit_field(value):
                return value

    with pytest.raises(TypeError, match="unsupported Arrow type"):

        @udf(
            input_schema=pa.schema(
                [pa.field("value", pa.int64(), metadata={"k": "v"})]
            ),
            output_schema=pa.int64(),
        )
        def input_field_metadata(value):
            return value

    with pytest.raises(TypeError, match="unsupported Arrow type"):

        @udf(
            input_schema=pa.schema([pa.field("value", pa.int64())]),
            output_schema=pa.field(
                "result", pa.int64(), nullable=False, metadata={"k": "v"}
            ),
        )
        def scalar_output_field_metadata(value):
            return value

    struct_type = pa.struct([pa.field("value", pa.int64(), nullable=False)])
    with pytest.raises(TypeError, match="unsupported Arrow type"):

        @udf(
            input_schema=pa.schema([pa.field("value", pa.int64())]),
            output_schema=pa.field(
                "result", struct_type, nullable=False, metadata={"k": "v"}
            ),
        )
        def struct_output_field_metadata(value):
            return {"value": value}

    for input_schema, output_schema in [
        (
            pa.schema([pa.field("value", pa.int64())], metadata={"k": "v"}),
            pa.int64(),
        ),
        (
            pa.schema([pa.field("value", pa.int64())]),
            pa.schema(
                [pa.field("result", pa.int64(), nullable=False)],
                metadata={"k": "v"},
            ),
        ),
    ]:
        with pytest.raises(TypeError, match="schema metadata"):

            @udf(input_schema=input_schema, output_schema=output_schema)
            def schema_metadata(value):
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
    with pytest.raises(NotImplementedError, match=message):
        db.list_functions()
    with pytest.raises(NotImplementedError, match=message):
        db.drop_function("normalize_score", version="fv_exact")


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
            if self.path == "/v1/functions/create":
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
            elif self.path == "/v1/functions/describe":
                assert body == {
                    "name": "normalize_score",
                    "version": "fv_exact",
                }
                response = state["version"]
            elif self.path == "/v1/functions/list":
                assert body["include_definition"] is True
                if "page_token" not in body:
                    response = {
                        "functions": [
                            {
                                "name": "normalize_score",
                                "version": "fv_exact",
                                "definition": state["version"],
                            }
                        ],
                        "page_token": "next",
                    }
                else:
                    assert body["page_token"] == "next"
                    response = {"functions": []}
            elif self.path == "/v1/functions/drop":
                assert body == {
                    "name": "normalize_score",
                    "version": "fv_exact",
                }
                response = {"dropped": True}
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
        "/v1/functions/create",
        "/v1/jobs/describe",
    ]


def test_remote_list_functions_paginates_and_returns_typed_versions():
    with _mock_remote_function_catalog() as (host, state):
        db = lancedb.connect(
            "db://dev",
            api_key="fake",
            host_override=host,
            client_config={"retry_config": {"retries": 0}},
        )
        created = db.create_function(normalize_score)
        state["requests"].clear()
        functions = db.list_functions()

    assert functions == [created]
    assert state["requests"] == [
        ("/v1/functions/list", {"include_definition": True}),
        (
            "/v1/functions/list",
            {"include_definition": True, "page_token": "next"},
        ),
    ]


@pytest.mark.asyncio
async def test_async_remote_list_functions_returns_typed_versions():
    with _mock_remote_function_catalog() as (host, state):
        db = await lancedb.connect_async(
            "db://dev",
            api_key="fake",
            host_override=host,
            client_config={"retry_config": {"retries": 0}},
        )
        registration = await db.create_function_async(normalize_score)
        created = await registration.wait()
        state["requests"].clear()
        functions = await db.list_functions()

    assert functions == [created]
    assert [path for path, _ in state["requests"]] == [
        "/v1/functions/list",
        "/v1/functions/list",
    ]


def test_remote_drop_function_sends_exact_version():
    with _mock_remote_function_catalog() as (host, state):
        db = lancedb.connect(
            "db://dev",
            api_key="fake",
            host_override=host,
            client_config={"retry_config": {"retries": 0}},
        )
        assert db.drop_function("normalize_score", version="fv_exact") is True

    assert state["requests"] == [
        (
            "/v1/functions/drop",
            {"name": "normalize_score", "version": "fv_exact"},
        )
    ]


@pytest.mark.asyncio
async def test_async_remote_drop_function_sends_exact_version():
    with _mock_remote_function_catalog() as (host, state):
        db = await lancedb.connect_async(
            "db://dev",
            api_key="fake",
            host_override=host,
            client_config={"retry_config": {"retries": 0}},
        )
        assert await db.drop_function("normalize_score", version="fv_exact") is True

    assert state["requests"] == [
        (
            "/v1/functions/drop",
            {"name": "normalize_score", "version": "fv_exact"},
        )
    ]
