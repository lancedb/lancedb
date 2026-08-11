# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""RED contract tests for the local @udf declaration surface."""

from __future__ import annotations

import importlib
import inspect
import types

import pyarrow as pa
import pytest

import lancedb
from lancedb import Function, Job, udf
from lancedb._udf import _get_udf_config

_REMOVED_AUTHORING_KNOBS = (
    "user_version",
    "idempotency_key",
    "deterministic",
    "null_handling",
    "null_policy",
    "on_error",
    "error_policy",
    "FunctionVersion",
    "artifact",
    "digest",
    "geneva",
)


def _decorate(fn, **overrides):
    kwargs = {
        "inputs": {"x": pa.int32()},
        "output": pa.int64(),
        "python": "3.12",
    }
    kwargs.update(overrides)
    return udf(**kwargs)(fn)


def test_udf_top_level_export_and_identity_metadata_behavior():
    assert "udf" in lancedb.__all__
    assert udf is lancedb.udf
    assert isinstance(importlib.import_module("lancedb._udf"), types.ModuleType)
    assert not isinstance(lancedb.udf, types.ModuleType)

    def add(x, y=1):
        """Add locally."""
        return x + y

    original = add
    decorated = _decorate(
        add,
        inputs={"x": pa.int32(), "y": pa.int32()},
        output=pa.int32(),
    )

    assert decorated is original
    assert decorated.__name__ == "add"
    assert decorated.__doc__ == "Add locally."
    assert str(inspect.signature(decorated)) == "(x, y=1)"
    assert decorated(2) == 3
    assert decorated(2, 5) == 7
    assert decorated(x=4, y=6) == 10


def test_udf_config_snapshot_order_defaults_and_immutability():
    inputs = {"z": pa.string(), "a": pa.int32()}
    packages = ["pkg-b==2", "pkg-a==1"]

    def combine(z, a):
        return f"{z}:{a}"

    decorated = udf(
        inputs=inputs,
        output=pa.string(),
        python="3.11",
        packages=packages,
        output_nullable=False,
    )(combine)

    config = _get_udf_config(decorated)
    assert config.inputs == (("z", pa.string()), ("a", pa.int32()))
    assert isinstance(config.inputs, tuple)
    assert config.output == pa.string()
    assert config.output_nullable is False
    assert config.python == "3.11"
    assert config.packages == ("pkg-b==2", "pkg-a==1")
    assert isinstance(config.packages, tuple)

    inputs["extra"] = pa.bool_()
    del inputs["z"]
    packages.append("pkg-c==3")
    packages[0] = "mutated==0"
    assert config.inputs == (("z", pa.string()), ("a", pa.int32()))
    assert config.packages == ("pkg-b==2", "pkg-a==1")

    for attr in ("inputs", "output", "output_nullable", "python", "packages"):
        with pytest.raises(AttributeError):
            setattr(config, attr, None)

    def defaults_only(x):
        return x

    defaulted = udf(
        inputs={"x": pa.int32()},
        output=pa.int64(),
        python="3.12",
    )(defaults_only)
    default_config = _get_udf_config(defaulted)
    assert default_config.packages == ()
    assert default_config.output_nullable is True


def test_udf_accepts_lambda_and_closure_for_local_declaration():
    ambient = "ambient-secret-value-xyz"

    lam = udf(
        inputs={"n": pa.int32()},
        output=pa.int32(),
        python="3.12",
    )(lambda n: n + 1)
    assert lam(3) == 4
    assert _get_udf_config(lam).inputs == (("n", pa.int32()),)

    def factory(offset):
        @udf(
            inputs={"n": pa.int32()},
            output=pa.int32(),
            python="3.12",
            packages=["demo==0.1"],
        )
        def closed(n):
            return n + offset + len(ambient)

        return closed

    closed = factory(10)
    assert closed(2) == 12 + len(ambient)
    assert _get_udf_config(closed).packages == ("demo==0.1",)


def test_udf_declaration_defers_signature_and_implementation_packaging():
    """Declaration must not validate callable signature or embed implementation."""

    def local_add(left, right=1):
        return left + right

    decorated = udf(
        inputs={"x": pa.int32(), "y": pa.int32()},
        output=pa.int32(),
        python="3.12",
    )(local_add)

    assert decorated is local_add
    assert str(inspect.signature(decorated)) == "(left, right=1)"
    assert decorated(2) == 3
    assert decorated(2, 5) == 7

    config = _get_udf_config(decorated)
    assert config.inputs == (("x", pa.int32()), ("y", pa.int32()))
    for attr in (
        "source",
        "module",
        "callable",
        "function",
        "implementation",
        "bundle",
        "artifact",
        "digest",
    ):
        assert not hasattr(config, attr)


def test_udf_lookup_double_decoration_and_non_function_target():
    def plain(x):
        return x

    with pytest.raises((TypeError, ValueError)):
        _get_udf_config(plain)

    decorated = _decorate(plain)

    with pytest.raises((TypeError, ValueError)):
        _decorate(decorated)

    with pytest.raises(TypeError):
        udf(
            inputs={"x": pa.int32()},
            output=pa.int32(),
            python="3.12",
        )(object())

    with pytest.raises(TypeError):
        udf(
            inputs={"x": pa.int32()},
            output=pa.int32(),
            python="3.12",
        )(42)


def test_udf_config_validation_errors():
    def target(x):
        return x

    with pytest.raises(TypeError):
        udf({"x": pa.int32()}, pa.int32(), "3.12")(target)

    with pytest.raises(TypeError):
        _decorate(target, inputs=[("x", pa.int32())])

    with pytest.raises(TypeError):
        _decorate(target, inputs={1: pa.int32()})

    with pytest.raises(ValueError):
        _decorate(target, inputs={"": pa.int32()})

    with pytest.raises(TypeError):
        _decorate(target, inputs={"x": "int32"})

    with pytest.raises(TypeError):
        _decorate(target, output="int64")

    with pytest.raises(TypeError):
        _decorate(target, python=3.12)

    with pytest.raises(ValueError):
        _decorate(target, python="")

    with pytest.raises(TypeError):
        _decorate(target, packages="pkg==1")

    with pytest.raises(ValueError):
        _decorate(target, packages=["pkg==1", ""])

    with pytest.raises(ValueError):
        _decorate(target, packages=["pkg==1", "pkg==1"])

    with pytest.raises(TypeError):
        _decorate(target, packages=["pkg==1", 2])

    with pytest.raises(TypeError):
        _decorate(target, output_nullable=1)

    with pytest.raises(TypeError):
        _decorate(target, output_nullable="true")


def test_udf_rejects_removed_overdesign_and_has_no_durable_side_effects():
    params = inspect.signature(udf).parameters
    for name in _REMOVED_AUTHORING_KNOBS:
        assert name not in params

    def score(x):
        """score body marker unique-xyz."""
        ambient = "ambient-secret-value-xyz"
        return f"{ambient}:{x}"

    decorated = _decorate(
        score,
        packages=["score==1.0"],
        output_nullable=True,
    )
    config = _get_udf_config(decorated)
    text = repr(config).lower()

    assert "score body marker unique-xyz" not in text
    assert "ambient-secret-value-xyz" not in text
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
    ):
        assert token not in text

    for attr in _REMOVED_AUTHORING_KNOBS:
        assert not hasattr(config, attr)

    assert not isinstance(decorated, Function)
    assert not isinstance(decorated, Job)
    for attr in ("id", "function_id", "job", "job_id", "registration"):
        assert not hasattr(decorated, attr)
