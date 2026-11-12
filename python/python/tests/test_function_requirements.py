# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pytest

from lancedb import function, udf
from lancedb.functions import FunctionDefinition


def identity(value: int) -> int:
    return value


@pytest.mark.parametrize(
    "package",
    [
        "torch>=2.0,,<3",
        "torch>=",
        "torch=>2.0",
        "torch[",
        "torch; python_version >=",
        "",
        " torch",
        "torch ",
    ],
)
def test_invalid_pip_requirement_fails_before_packaging(package, monkeypatch):
    def unexpected_packaging(*args, **kwargs):
        pytest.fail("invalid requirements must fail before packaging the function")

    monkeypatch.setattr("lancedb.functions._infer_signature", unexpected_packaging)
    with pytest.raises(ValueError) as error:
        function(pip=["numpy", package])(identity)
    assert "pip[1]" in str(error.value)
    assert repr(package) in str(error.value)


@pytest.mark.parametrize(
    "package",
    [
        "numpy>=1,,<3",
        "numpy>=",
        "numpy[version=]",
        "numpy[unknown_field=value]",
        "",
        " numpy",
        "numpy ",
    ],
)
def test_invalid_conda_requirement_names_entry(package):
    with pytest.raises(ValueError) as error:
        function(conda=["scipy", package])(identity)
    assert "conda[1]" in str(error.value)
    assert repr(package) in str(error.value)


@pytest.mark.parametrize("kind", ["pip", "conda"])
@pytest.mark.parametrize("package", [None, 42, ["numpy"]])
def test_requirement_must_be_a_string(kind, package):
    with pytest.raises(TypeError, match=rf"{kind}\[1\] must be a string"):
        function(**{kind: ["numpy", package]})(identity)


@pytest.mark.parametrize(
    "package",
    [
        "numpy",
        "torch>=2.0,<3",
        'requests[security,socks]>=2; python_version >= "3.10"',
        "numpy (>=1.26, <3)",
        "six @ https://example.com/six-1.16.0.tar.gz",
        "six @ git+https://github.com/benjaminp/six.git@main",
        "https://example.com/six-1.16.0.tar.gz",
        "git+https://github.com/benjaminp/six.git@main",
        "git+ssh://git@github.com/benjaminp/six.git@main",
    ],
)
def test_supported_pip_requirements_are_preserved(package):
    definition = function(pip=[package])(identity)
    assert definition.registration_request.runtime.environment.packages == (package,)


@pytest.mark.parametrize(
    "package",
    [
        "numpy",
        "numpy=1.26",
        "numpy>=1.26,<3",
        "numpy=1.26=py310_0",
        "numpy 1.26 py310_0",
        "conda-forge::numpy>=1.26",
        "conda-forge/linux-64::numpy>=1.26",
        "numpy[version='>=1.26,<3',build='py310*']",
    ],
)
def test_supported_conda_requirements_are_preserved(package):
    definition = function(conda=[package])(identity)
    assert definition.registration_request.runtime.environment.packages == (package,)


def test_requirements_still_sort_and_deduplicate():
    definition = function(pip=["torch>=2", "numpy", "torch>=2"])(identity)
    assert definition.registration_request.runtime.environment.packages == (
        "numpy",
        "torch>=2",
    )


def test_deprecated_udf_alias_validates_requirements():
    with pytest.warns(DeprecationWarning, match="use lancedb.function"):
        with pytest.raises(ValueError, match=r"pip\[0\]"):
            udf(pip=["torch>=2.0,,<3"])(identity)


def test_direct_function_definition_validates_requirements():
    with pytest.raises(ValueError, match=r"pip\[0\]"):
        FunctionDefinition(
            identity,
            name=None,
            input_schema=None,
            output_schema=None,
            pip=("torch>=2.0,,<3",),
            env={},
            python_version=None,
        )
