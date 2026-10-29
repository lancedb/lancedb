# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Class Functions, initialization, and the source that travels with a Function."""

from __future__ import annotations

import base64
import importlib
import json
import subprocess
import sys
import textwrap
import types
import warnings
from pathlib import Path
from typing import Annotated, Optional, get_type_hints

import pyarrow as pa
import pytest

from lancedb import col
from lancedb.functions import FunctionVersion, udf

FIXTURES = (
    Path(__file__).parents[3]
    / "rust"
    / "lancedb"
    / "tests"
    / "fixtures"
    / "first_class_functions"
    / "v1"
)


def artifact(definition) -> bytes:
    return base64.b64decode(definition.registration_request.artifact.content.data)


def bundle_files(definition) -> dict[str, str]:
    assert definition.registration_request.artifact.kind == "python_bundle"
    return json.loads(artifact(definition))["files"]


@pytest.fixture
def helper_package(tmp_path, monkeypatch):
    """A local package on sys.path, the way a project's own helpers are."""

    def write(name: str, files: dict[str, str]):
        root = tmp_path / name
        for path, source in files.items():
            target = root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(textwrap.dedent(source))
        monkeypatch.syspath_prepend(str(tmp_path))
        for module in [
            module for module in sys.modules if module.split(".")[0] == name
        ]:
            del sys.modules[module]
        importlib.invalidate_caches()
        return importlib.import_module(name)

    return write


@udf
class scale:
    def __init__(
        self,
        factor: float,
        label: str,
        offset: float = 0.0,
        tags: Optional[list[str]] = None,
        retry: Annotated[dict, pa.struct([("attempts", pa.int32())])] = None,
    ):
        self.factor, self.offset = factor, offset

    def __call__(self, value: float) -> float:
        return value * self.factor + self.offset


class NoCall:
    pass


class BadClose:
    def __call__(self, value: int) -> int:
        return value

    def close(self, force):
        pass


def test_class_function_declares_inputs_output_and_initialization():
    signature = scale.registration_request.signature
    assert [(p.name, p.arrow_type) for p in signature.inputs] == [("value", "float64")]
    assert [
        (field.name, field.arrow_type, field.nullable)
        for field in signature.initialization
    ] == [
        ("factor", "float64", False),
        ("label", "utf8", False),
        ("offset", "float64", True),
        ("tags", "list<utf8>", True),
        (
            "retry",
            '{"fields":[{"name":"attempts","nullable":true,"type":{"type":"int32"}}],'
            '"type":"struct"}',
            True,
        ),
    ]
    # Calling the definition constructs the class, so local tests are ordinary.
    assert scale(factor=2.0, label="x")(1.5) == 3.0
    assert scale.registration_request.artifact.entrypoint == "scale"
    source = artifact(scale).decode()
    assert source.startswith("from __future__ import annotations\n")
    assert "class scale:" in source and "@udf" not in source


def test_function_without_initialization_keeps_its_wire_form():
    @udf
    def double(value: int) -> int:
        return value * 2

    request = json.loads(double.registration_request.to_canonical_json())
    assert "initialization" not in request["signature"]
    assert request["artifact"]["kind"] == "python_callable"


@pytest.mark.parametrize("annotation", ["Array", "RecordBatch"])
@pytest.mark.parametrize("kind", ["function", "class"])
def test_batch_annotations_require_explicit_schemas(annotation, kind, helper_package):
    if kind == "class":
        source = f"""
            import pyarrow as pa

            class identity:
                def __call__(self, value: pa.{annotation}) -> pa.{annotation}:
                    return value
        """
    else:
        source = f"""
            import pyarrow as pa

            def identity(value: pa.{annotation}) -> pa.{annotation}:
                return value
        """
    module = helper_package("fx_batch_annotations", {"__init__.py": source})
    with pytest.raises(TypeError, match="input_schema and output_schema"):
        udf(module.identity)


def test_array_batch_annotations_accept_explicit_schemas():
    @udf(
        input_schema=pa.schema([pa.field("value", pa.int64(), nullable=False)]),
        output_schema=pa.int64(),
    )
    def identity(value: pa.Array) -> pa.Array:
        return value

    signature = identity.registration_request.signature
    assert [(p.name, p.arrow_type, p.nullable) for p in signature.inputs] == [
        ("value", "int64", False)
    ]
    assert signature.output.arrow_type == "int64"
    assert signature.output.nullable is False
    values = pa.array([1, 2, 3])
    assert identity(values).equals(values)


@pytest.mark.parametrize("class_udf", [False, True])
@pytest.mark.parametrize("postponed", [False, True])
@pytest.mark.parametrize(
    ("annotation", "nullable"),
    [
        ("BlobReader", False),
        ("Reader", False),
        ('"Reader"', False),
        ("Reader | None", True),
        ("Optional[Reader]", True),
        ('Optional["Reader"]', True),
        ("MaybeReader", True),
        ("lancedb.BlobReader", False),
        ("lancedb.BlobReader | None", True),
    ],
)
def test_blob_reader_annotations_resolve_in_the_artifact(
    helper_package, monkeypatch, class_udf, postponed, annotation, nullable
):
    definition = (
        f"class read_blob:\n"
        f"    def __call__(self, video: {annotation}) -> int:\n"
        f"        return 0\n"
        if class_udf
        else f"def read_blob(video: {annotation}) -> int:\n    return 0\n"
    )
    module = helper_package(
        "fx_blob_annotations",
        {
            "__init__.py": (
                ("from __future__ import annotations\n" if postponed else "")
                + "import lancedb\n"
                "from typing import Optional\n"
                "from lancedb import BlobReader, BlobReader as Reader\n"
                "from lancedb.functions import udf\n"
                "MaybeReader = Optional[Reader]\n\n"
                f"@udf\n{definition}"
            )
        },
    )
    request = module.read_blob.registration_request
    parameter = request.signature.inputs[0]
    assert parameter.arrow_type == "blob_v2"
    assert parameter.blob_access == "reader"
    assert parameter.nullable is nullable

    # Workers provide lance_blob rather than importing the client SDK's reader.
    worker = types.ModuleType("lance_blob")
    worker.BlobReader = type("BlobReader", (), {})
    monkeypatch.setitem(sys.modules, "lance_blob", worker)
    namespace = {}
    exec(compile(artifact(module.read_blob), "user_function.py", "exec"), namespace)
    packaged = namespace["read_blob"]
    if class_udf:
        packaged = packaged.__call__
    assert get_type_hints(packaged) == {
        "video": Optional[worker.BlobReader] if nullable else worker.BlobReader,
        "return": int,
    }


def test_blob_reader_annotation_does_not_shadow_body_globals(
    helper_package, monkeypatch
):
    module = helper_package(
        "fx_blob_annotation_globals",
        {
            "__init__.py": """
                from lancedb import BlobReader
                from lancedb.functions import udf

                _lance_blob_reader = 7

                @udf
                def read_blob(video: BlobReader) -> int:
                    return _lance_blob_reader
            """
        },
    )
    worker = types.ModuleType("lance_blob")
    worker.BlobReader = type("BlobReader", (), {})
    monkeypatch.setitem(sys.modules, "lance_blob", worker)
    namespace = {}
    exec(compile(artifact(module.read_blob), "user_function.py", "exec"), namespace)
    packaged = namespace["read_blob"]
    assert get_type_hints(packaged)["video"] is worker.BlobReader
    assert packaged(worker.BlobReader()) == 7


@pytest.mark.parametrize(
    ("init", "message"),
    [
        ("def __init__(self, blob: bytes): pass", "initialization takes booleans"),
        ("def __init__(self, *args: int): pass", "named and non-variadic"),
        ("def __init__(self, factor): pass", "missing Function initialization"),
        ("def __init__(self, value: int): pass", "share names \\['value'\\]"),
    ],
)
def test_class_function_rejects_unsupported_initialization(init, message):
    namespace = {}
    exec(
        textwrap.dedent(
            f"""
            class bad:
                {init}
                def __call__(self, value: int) -> int:
                    return value
            """
        ),
        namespace,
    )
    with pytest.raises((TypeError, ValueError), match=message):
        udf(namespace["bad"])


def test_class_function_requires_call_and_a_plain_close():
    with pytest.raises(TypeError, match="synchronous __call__"):
        udf(NoCall)
    with pytest.raises(TypeError, match="close must be a method taking no"):
        udf(BadClose)


def test_nested_class_and_closure_are_rejected_with_the_reason():
    def build():
        class nested:
            def __call__(self, value: int) -> int:
                return value

        return nested

    with pytest.raises(ValueError, match="defined at module level"):
        udf(build())

    factor = 3

    with pytest.raises(ValueError, match="capture closure values"):

        @udf
        def closes_over(value: int) -> int:
            return value * factor


def test_binding_passes_initialization_as_constants():
    version = FunctionVersion.from_json(
        json.dumps(
            {
                **json.loads(
                    (FIXTURES / "remote_function_version.canonical.json").read_text()
                ),
                "signature": {
                    "inputs": [
                        {"name": "text", "arrow_type": "utf8", "nullable": True}
                    ],
                    "output": {
                        "kind": "scalar",
                        "arrow_type": "list<float32>",
                        "nullable": False,
                    },
                    "initialization": [
                        {"name": "model", "arrow_type": "utf8", "nullable": False},
                        {"name": "dimensions", "arrow_type": "int32", "nullable": True},
                    ],
                },
            }
        )
    )
    application = version(text=col("body"), model="small", dimensions=(512))
    assert dict(application.initialization) == {"model": "small", "dimensions": 512}
    assert json.loads(application.to_canonical_json())["initialization"] == {
        "dimensions": 512,
        "model": "small",
    }
    assert version(text=col("body"), model="small").initialization == {"model": "small"}

    with pytest.raises(TypeError, match=r"missing initialization: \['model'\]"):
        version(text=col("body"))
    with pytest.raises(TypeError, match="takes a constant"):
        version(text=col("body"), model=col("model"))
    with pytest.raises(ValueError, match="must be finite"):
        version(text=col("body"), model="small", dimensions=float("nan"))
    with pytest.raises(TypeError, match="takes booleans"):
        version(text=col("body"), model=object())


def test_code_packages_ship_as_a_bundle_and_version_with_their_source(
    tmp_path, helper_package
):
    files = {
        "__init__.py": "",
        "rate.py": """
            class TokenBucket:
                def __init__(self, tokens_per_minute):
                    self.tokens_per_minute = tokens_per_minute
        """,
        "__pycache__/rate.cpython-312.pyc": "",
        "fixtures/data.json": "{}",
        "not-a-module.py": "",
    }
    helper_package("fx_helpers", files)
    (tmp_path / "fx_helpers_functions.py").write_text(
        textwrap.dedent(
            """
            import fx_helpers
            from fx_helpers.rate import TokenBucket
            from lancedb import udf


            @udf(code=[fx_helpers])
            class limited:
                def __init__(self, tpm: int):
                    self.limiter = TokenBucket(tpm)

                def __call__(self, value: int) -> int:
                    return value
            """
        )
    )

    def define():
        sys.modules.pop("fx_helpers_functions", None)
        return importlib.import_module("fx_helpers_functions").limited

    first = define()
    shipped = bundle_files(first)
    assert sorted(shipped) == [
        "fx_helpers/__init__.py",
        "fx_helpers/rate.py",
        "user_function.py",
    ]
    assert (
        "from fx_helpers.rate import TokenBucket as TokenBucket"
        in (shipped["user_function.py"])
    )

    # Only the helper changes: a new artifact digest, the same environment.
    helper_package(
        "fx_helpers", {**files, "rate.py": textwrap.dedent(files["rate.py"]) + "# v2\n"}
    )
    second = define()
    assert (
        first.registration_request.artifact.digest
        != second.registration_request.artifact.digest
    )
    assert first.registration_request.runtime == second.registration_request.runtime


def test_code_rejects_submodules_duplicates_and_non_modules(helper_package):
    helpers = helper_package("fx_rejects", {"__init__.py": "", "sub.py": ""})
    import fx_rejects.sub

    for code, message in [
        ([fx_rejects.sub], "top-level modules"),
        ([helpers, helpers], "more than once"),
        (["fx_rejects"], "imported modules"),
        ([json], "cannot ship module 'json'"),
    ]:
        with pytest.raises((TypeError, ValueError), match=message):

            @udf(code=code)
            def f(value: int) -> int:
                return value


def test_local_module_outside_code_warns_at_registration(tmp_path, helper_package):
    """The worker most likely cannot import a local module that is neither
    shipped nor declared, but registration still succeeds: a declared
    distribution may install it under another name, and callers may rewrite
    the packaged source before registering it."""
    helper_package(
        "fx_local", {"__init__.py": "def normalize(value):\n    return value\n"}
    )

    def define(name, options):
        (tmp_path / f"{name}.py").write_text(
            textwrap.dedent(
                f"""
                from fx_local import normalize
                from lancedb import udf


                @udf({options})
                def {name}(value: int) -> int:
                    return normalize(value)
                """
            )
        )
        return getattr(importlib.import_module(name), name)

    with pytest.warns(UserWarning, match=r"local module.*code=\[fx_local\]"):
        local = define("fx_uses_local", "")
    assert "from fx_local import normalize" in artifact(local).decode()

    # A declared package that provides the module is trusted to install it.
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        installed = define("fx_uses_installed", "pip=['fx-local==1.0']")
    assert "from fx_local import normalize" in artifact(installed).decode()


MMLB_HELPERS = {
    "__init__.py": "",
    "rate.py": """
        class TokenBucket:
            constructed = 0

            def __init__(self, tokens_per_minute):
                TokenBucket.constructed += 1
                self.tokens_per_minute = tokens_per_minute
                self.acquired = 0

            def acquire(self, tokens):
                self.acquired += tokens
    """,
    "embedding.py": """
        from fx_mmlb.rate import TokenBucket


        class OpenAIEmbedder:
            def __init__(self, model: str, tpm_limit: int, max_attempts: int = 6):
                self.model = model
                self.limiter = TokenBucket(tpm_limit)
                self.max_attempts = max_attempts

            def embed(self, texts):
                self.limiter.acquire(sum(len(text) for text in texts))
                return [[float(len(text))] for text in texts]
    """,
}

MMLB_FUNCTIONS = """
import pyarrow as pa

import fx_mmlb
from fx_mmlb.embedding import OpenAIEmbedder
from lancedb import udf

TEXT = pa.schema([pa.field("text", pa.string())])
EMBEDDING = pa.list_(pa.field("item", pa.float64(), nullable=False))


@udf(code=[fx_mmlb], input_schema=TEXT, output_schema=EMBEDDING)
class openai_embedding_batch(OpenAIEmbedder):
    def __call__(self, text: pa.Array) -> pa.Array:
        item = pa.field("item", pa.float64(), nullable=False)
        return pa.array(self.embed(text.to_pylist()), pa.list_(item))


@udf(code=[fx_mmlb])
class openai_embedding(OpenAIEmbedder):
    def __call__(self, text: str) -> list[float]:
        return self.embed([text])[0]
"""


def test_shared_helpers_construct_state_on_every_path(tmp_path, helper_package):
    """The MMLB rate limiter, shared by a row and a batch Function: both are
    initialized through the same helper, so neither path can skip it."""
    helper_package("fx_mmlb", MMLB_HELPERS)
    (tmp_path / "fx_mmlb_functions.py").write_text(MMLB_FUNCTIONS)
    functions = importlib.import_module("fx_mmlb_functions")

    signatures = [
        functions.openai_embedding_batch.registration_request.signature,
        functions.openai_embedding.registration_request.signature,
    ]
    for signature in signatures:
        assert [field.name for field in signature.initialization] == [
            "model",
            "tpm_limit",
            "max_attempts",
        ]

    # Each bundle imports on its own, in a process that has never seen the
    # client's modules, the way the worker's image code layer does.
    for definition, call in [
        (
            functions.openai_embedding_batch,
            "instance(pa.array(['ab', 'abc'])).to_pylist()",
        ),
        (functions.openai_embedding, "[instance('ab'), instance('abc')]"),
    ]:
        worker = tmp_path / definition.__name__
        for path, source in bundle_files(definition).items():
            (worker / path).parent.mkdir(parents=True, exist_ok=True)
            (worker / path).write_text(source)
        probe = textwrap.dedent(
            f"""
            import sys
            sys.path.insert(0, {str(worker)!r})
            import pyarrow as pa
            import user_function
            from fx_mmlb.rate import TokenBucket
            cls = getattr(user_function, {definition.__name__!r})
            instance = cls(model="small", tpm_limit=1000)
            print(repr(({call}, TokenBucket.constructed, instance.limiter.acquired)))
            """
        )
        result = subprocess.run(
            [sys.executable, "-I", "-c", probe],
            capture_output=True,
            text=True,
            cwd=worker,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "([[2.0], [3.0]], 1, 5)"


def test_main_definitions_travel_by_source(tmp_path):
    """Notebook cells and scripts define helpers in __main__, which a worker
    cannot import; they are packaged by source, dependencies first."""
    script = tmp_path / "notebook.py"
    script.write_text(
        textwrap.dedent(
            """
            import json
            import math
            from dataclasses import dataclass

            from lancedb import udf

            SCALE = 2


            @dataclass
            class Settings:
                factor: int


            def helper(value):
                return math.floor(value) * SCALE


            class Base:
                def run(self, value):
                    return helper(value) * self.settings.factor


            @udf
            class scaled(Base):
                def __init__(self, factor: int):
                    self.settings = Settings(factor)

                def __call__(self, value: float) -> int:
                    return self.run(value)


            print(json.dumps(scaled.registration_request.to_canonical_json()))
            """
        )
    )
    result = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    request = json.loads(json.loads(result.stdout))
    assert request["artifact"]["kind"] == "python_callable"
    source = base64.b64decode(request["artifact"]["content"]["data"]).decode()
    assert source.index("def helper") < source.index("class Base")
    assert source.index("class Base") < source.index("class scaled")
    assert source.index("class Settings") < source.index("class scaled")
    assert "@dataclass" in source
    assert "SCALE = 2" in source

    namespace = {}
    exec(compile(source, "user_function.py", "exec"), namespace)
    assert namespace["scaled"](factor=3)(2.7) == 12


def test_main_lambdas_are_rejected(tmp_path):
    script = tmp_path / "lambda.py"
    script.write_text(
        textwrap.dedent(
            """
            from lancedb import udf

            double = lambda value: value * 2

            @udf
            def uses_lambda(value: int) -> int:
                return double(value)
            """
        )
    )
    result = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True
    )
    assert result.returncode != 0
    assert "not lambdas, closures, or nested definitions" in result.stderr
