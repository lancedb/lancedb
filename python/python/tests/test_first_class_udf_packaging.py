# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""RED contract tests for private UDF packaging validation."""

from __future__ import annotations

import importlib
import inspect
import json
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pyarrow as pa
import pytest

from lancedb import Function, Job, udf
from lancedb._udf import _get_udf_config, _package_udf

_BODY_MARKER = "packaging body marker unique-xyz"
_AMBIENT_SECRET = "ambient-secret-value-xyz"
_BUILTIN_SHADOW_SECRET = "builtin-shadow-secret-xyz"
_SOURCE_MISMATCH_SECRET = "source-mismatch-secret-xyz"
_INVALID_UTF8_SECRET = "invalid-utf8-secret-xyz"

_OVERDESIGN_ATTRS = (
    "user_version",
    "idempotency_key",
    "deterministic",
    "null_handling",
    "null_policy",
    "on_error",
    "error_policy",
    "FunctionVersion",
    "function_version",
    "artifact",
    "digest",
    "geneva",
    "id",
    "function_id",
    "job",
    "job_id",
    "registration",
    "catalog",
    "retry_key",
    "source_path",
    "path",
    "function",
)

_PACKAGING_CONSTANT = 41


def _packaging_helper(value: int) -> int:
    return value + _PACKAGING_CONSTANT


@udf(
    inputs={"x": pa.int32()},
    output=pa.int64(),
    python="3.12",
    packages=["pkg-a==1"],
    output_nullable=False,
)
def packable_add(x):
    """packaging body marker unique-xyz."""
    return _packaging_helper(x) + len(json.dumps({"k": 1}))


@udf(
    inputs={"x": pa.int32(), "y": pa.int32()},
    output=pa.int32(),
    python="3.12",
)
def packable_kwonly(x, *, y=2):
    return x + y


@udf(
    inputs={"x": pa.int32()},
    output=pa.int32(),
    python="3.12",
)
def packable_rebind_target(x):
    return x


@udf(
    inputs={"x": pa.int32()},
    output=pa.int32(),
    python="3.12",
)
def uses_injected_ambient(x):
    return x + len(INJECTED_AMBIENT_GLOBAL)  # noqa: F821


@udf(
    inputs={"x": pa.int32()},
    output=pa.int32(),
    python="3.12",
)
def uses_shadowed_builtin_len(x):
    return x + len((1, 2, 3))


@udf(
    inputs={"x": pa.int32(), "y": pa.int32()},
    output=pa.int32(),
    python="3.12",
)
def mismatch_names(left, right):
    return left + right


@udf(
    inputs={"y": pa.int32(), "x": pa.int32()},
    output=pa.int32(),
    python="3.12",
)
def mismatch_order(x, y):
    return x + y


@udf(
    inputs={"x": pa.int32(), "y": pa.int32()},
    output=pa.int32(),
    python="3.12",
)
def positional_only(x, /, y):
    return x + y


@udf(
    inputs={"x": pa.int32()},
    output=pa.int32(),
    python="3.12",
)
def varargs_fn(x, *args):
    return x


@udf(
    inputs={"x": pa.int32()},
    output=pa.int32(),
    python="3.12",
)
def kwargs_fn(x, **kwargs):
    return x


@udf(
    inputs={"x": pa.int32()},
    output=pa.int32(),
    python="3.12",
)
async def async_fn(x):
    return x


@udf(
    inputs={"x": pa.int32()},
    output=pa.int32(),
    python="3.12",
)
async def async_gen_fn(x):
    yield x


@udf(
    inputs={"x": pa.int32()},
    output=pa.int32(),
    python="3.12",
)
def generator_fn(x):
    yield x


def _assert_sanitized_text(*parts: object, secret: str = _AMBIENT_SECRET) -> None:
    combined = "\n".join(str(part) for part in parts)
    lowered = combined.lower()
    assert _BODY_MARKER.lower() not in lowered
    assert secret.lower() not in lowered
    assert str(Path(__file__).resolve()).lower() not in lowered
    assert Path(__file__).resolve().as_posix().lower() not in lowered


def _assert_packaging_rejection(exc_info, *, secret: str = _AMBIENT_SECRET) -> None:
    _assert_sanitized_text(exc_info.value, repr(exc_info.value), secret=secret)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


@contextmanager
def _temporary_imported_module(
    directory: Path, module_name: str, source: str
) -> Iterator[tuple[Path, object]]:
    path = directory / f"{module_name}.py"
    path.write_text(source, encoding="utf-8")
    inserted = str(directory)
    sys.path.insert(0, inserted)
    try:
        sys.modules.pop(module_name, None)
        module = importlib.import_module(module_name)
        yield path, module
    finally:
        sys.modules.pop(module_name, None)
        try:
            sys.path.remove(inserted)
        except ValueError:
            pass


def _temp_udf_module_source(*, body: str, secret: str | None = None) -> str:
    secret_line = f"_SECRET = {secret!r}\n" if secret is not None else ""
    return (
        "import pyarrow as pa\n"
        "from lancedb import udf\n"
        f"{secret_line}\n"
        "@udf(\n"
        '    inputs={"x": pa.int32()},\n'
        "    output=pa.int32(),\n"
        '    python="3.12",\n'
        ")\n"
        "def temp_pack_target(x):\n"
        f"    {body}\n"
    )


def test_package_udf_success_snapshot_source_module_callable_config_and_repr():
    packaged = _package_udf(packable_add)
    source = Path(__file__).read_text(encoding="utf-8")

    assert packaged.source == source
    assert packaged.module == __name__
    assert packaged.module != "__main__"
    assert packaged.callable_name == "packable_add"
    assert packable_add.__qualname__ == "packable_add"
    assert packaged.config is _get_udf_config(packable_add)
    assert packaged.config.inputs == (("x", pa.int32()),)
    assert packaged.config.output == pa.int64()
    assert packaged.config.output_nullable is False
    assert packaged.config.python == "3.12"
    assert packaged.config.packages == ("pkg-a==1",)

    for attr in ("source", "module", "callable_name", "config"):
        with pytest.raises(AttributeError):
            setattr(packaged, attr, None)

    text = repr(packaged)
    _assert_sanitized_text(text)
    assert _BODY_MARKER not in text


def test_package_udf_allows_source_bound_import_constant_and_helper():
    packaged = _package_udf(packable_add)
    assert packaged.callable_name == "packable_add"
    assert "import json" in packaged.source
    assert "_PACKAGING_CONSTANT" in packaged.source
    assert "_packaging_helper" in packaged.source
    assert packable_add(1) == _packaging_helper(1) + len(json.dumps({"k": 1}))


def test_package_udf_accepts_positional_or_keyword_and_keyword_only_defaults():
    packaged = _package_udf(packable_kwonly)
    assert packaged.callable_name == "packable_kwonly"
    assert packaged.config.inputs == (("x", pa.int32()), ("y", pa.int32()))
    assert str(inspect.signature(packable_kwonly)) == "(x, *, y=2)"
    assert packable_kwonly(3) == 5
    assert packable_kwonly(3, y=7) == 10


def test_package_udf_rejects_lambda_and_closure():
    lam = udf(
        inputs={"n": pa.int32()},
        output=pa.int32(),
        python="3.12",
    )(lambda n: n + 1)
    with pytest.raises(ValueError) as exc_info:
        _package_udf(lam)
    _assert_packaging_rejection(exc_info)

    ambient = _AMBIENT_SECRET

    def factory(offset):
        @udf(
            inputs={"n": pa.int32()},
            output=pa.int32(),
            python="3.12",
        )
        def closed(n):
            return n + offset + len(ambient)

        return closed

    closed = factory(10)
    with pytest.raises(ValueError) as exc_info:
        _package_udf(closed)
    _assert_packaging_rejection(exc_info)

    def outer():
        total = 0

        @udf(
            inputs={"n": pa.int32()},
            output=pa.int32(),
            python="3.12",
        )
        def nested(n):
            nonlocal total
            total += n
            return total

        return nested

    with pytest.raises(ValueError) as exc_info:
        _package_udf(outer())
    _assert_packaging_rejection(exc_info)


def test_package_udf_rejects_signature_mismatches_and_unsupported_parameter_kinds():
    for target in (
        mismatch_names,
        mismatch_order,
        positional_only,
        varargs_fn,
        kwargs_fn,
    ):
        with pytest.raises(ValueError) as exc_info:
            _package_udf(target)
        _assert_packaging_rejection(exc_info)


def test_package_udf_rejects_async_and_generator_functions():
    for target in (async_fn, async_gen_fn, generator_fn):
        with pytest.raises(ValueError) as exc_info:
            _package_udf(target)
        _assert_packaging_rejection(exc_info)


def test_package_udf_rejects_dynamic_exec_source():
    namespace: dict[str, object] = {}
    exec(
        "def dynamic_pack_target(x):\n    return x + 1\n",
        namespace,
    )
    dynamic = udf(
        inputs={"x": pa.int32()},
        output=pa.int32(),
        python="3.12",
    )(namespace["dynamic_pack_target"])
    with pytest.raises(ValueError) as exc_info:
        _package_udf(dynamic)
    _assert_packaging_rejection(exc_info)


def test_package_udf_rejects_undecorated_and_wrong_input_types():
    def plain(x):
        return x

    with pytest.raises(TypeError) as exc_info:
        _package_udf(plain)
    _assert_packaging_rejection(exc_info)

    with pytest.raises(TypeError) as exc_info:
        _package_udf(object())
    _assert_packaging_rejection(exc_info)

    with pytest.raises(TypeError) as exc_info:
        _package_udf(42)
    _assert_packaging_rejection(exc_info)


def test_package_udf_rejects_rebound_module_attribute():
    module = sys.modules[__name__]
    original = module.packable_rebind_target
    replacement = udf(
        inputs={"x": pa.int32()},
        output=pa.int32(),
        python="3.12",
    )(lambda x: x)
    module.packable_rebind_target = replacement
    try:
        with pytest.raises(ValueError) as exc_info:
            _package_udf(original)
        _assert_packaging_rejection(exc_info)
    finally:
        module.packable_rebind_target = original


def test_package_udf_rejects_injected_ambient_global():
    module = sys.modules[__name__]
    secret = _AMBIENT_SECRET
    module.INJECTED_AMBIENT_GLOBAL = secret
    try:
        assert uses_injected_ambient(3) == 3 + len(secret)
        with pytest.raises(ValueError) as exc_info:
            _package_udf(uses_injected_ambient)
        _assert_packaging_rejection(exc_info, secret=secret)
    finally:
        delattr(module, "INJECTED_AMBIENT_GLOBAL")


def test_package_udf_rejects_builtin_shadow_injection():
    module = sys.modules[__name__]
    secret = _BUILTIN_SHADOW_SECRET
    assert not hasattr(module, "len")
    module.len = secret
    try:
        with pytest.raises(ValueError) as exc_info:
            _package_udf(uses_shadowed_builtin_len)
        _assert_packaging_rejection(exc_info, secret=secret)
    finally:
        delattr(module, "len")


def test_package_udf_rejects_loaded_code_source_mismatch(tmp_path: Path):
    secret = _SOURCE_MISMATCH_SECRET
    module_name = "udf_pkg_source_mismatch_mod"
    original = _temp_udf_module_source(body="return x + 1")
    replacement = _temp_udf_module_source(
        body=f"return x + 99  # {secret}",
        secret=secret,
    )
    with _temporary_imported_module(tmp_path, module_name, original) as (
        path,
        module,
    ):
        target = module.temp_pack_target
        assert target(1) == 2
        path.write_text(replacement, encoding="utf-8")
        with pytest.raises(ValueError) as exc_info:
            _package_udf(target)
        _assert_packaging_rejection(exc_info, secret=secret)
        err_text = f"{exc_info.value}\n{exc_info.value!r}"
        assert str(path.resolve()) not in err_text
        assert path.resolve().as_posix() not in err_text


def test_package_udf_rejects_invalid_utf8_after_import(tmp_path: Path):
    secret = _INVALID_UTF8_SECRET
    module_name = "udf_pkg_invalid_utf8_mod"
    original = _temp_udf_module_source(body="return x + 1")
    with _temporary_imported_module(tmp_path, module_name, original) as (
        path,
        module,
    ):
        target = module.temp_pack_target
        assert target(1) == 2
        path.write_bytes(secret.encode("utf-8") + b"\xff\xfe invalid-bytes")
        with pytest.raises(ValueError) as exc_info:
            _package_udf(target)
        assert type(exc_info.value) is ValueError
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__context__ is None
        err_text = f"{exc_info.value}\n{exc_info.value!r}"
        assert secret not in err_text
        assert "b'" not in err_text
        assert r"\xff" not in err_text
        assert str(path.resolve()) not in err_text
        assert path.resolve().as_posix() not in err_text


def test_package_udf_snapshot_has_no_durable_overdesign_and_is_not_function_or_job():
    packaged = _package_udf(packable_add)
    assert not isinstance(packaged, Function)
    assert not isinstance(packaged, Job)
    for attr in _OVERDESIGN_ATTRS:
        assert not hasattr(packaged, attr)

    text = repr(packaged).lower()
    for token in (
        "user_version",
        "idempotency_key",
        "deterministic",
        "null_policy",
        "on_error",
        "functionversion",
        "artifact",
        "digest",
        "geneva",
        "retry_key",
    ):
        assert token not in text
    _assert_sanitized_text(text)
