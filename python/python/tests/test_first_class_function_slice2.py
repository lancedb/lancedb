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
import threading
from typing import Optional

import pyarrow as pa
import pytest

import lancedb
from lancedb.functions import UdfDefinition, udf

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
    from lancedb.functions import _GRAMMAR_PRIMITIVES, _canonical_arrow_type

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
        pa.large_string(),
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
    from lancedb.functions import _canonical_arrow_type

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
    from lancedb.functions import _canonical_arrow_type

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
