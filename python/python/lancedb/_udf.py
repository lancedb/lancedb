# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Local authoring declaration surface for first-class UDFs.

This module only snapshots declaration metadata onto a Python function. It does
not package source, mint durable identity, or register anything with a database.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import ParamSpec, TypeVar

import pyarrow as pa

__all__ = ["udf"]

_P = ParamSpec("_P")
_R = TypeVar("_R")

_CONFIG_ATTR = "__lancedb_udf_config__"


@dataclass(frozen=True, slots=True)
class _UdfConfig:
    """Private frozen snapshot of a ``@udf`` declaration."""

    inputs: tuple[tuple[str, pa.DataType], ...]
    output: pa.DataType
    output_nullable: bool
    python: str
    packages: tuple[str, ...]


def _validate_inputs(
    inputs: object,
) -> tuple[tuple[str, pa.DataType], ...]:
    if not isinstance(inputs, Mapping):
        raise TypeError("udf inputs must be a Mapping of name to pyarrow DataType")
    snapshot: list[tuple[str, pa.DataType]] = []
    for key, value in inputs.items():
        if not isinstance(key, str):
            raise TypeError("udf input names must be strings")
        if key == "":
            raise ValueError("udf input names must be non-empty")
        if not isinstance(value, pa.DataType):
            raise TypeError("udf input types must be pyarrow DataType values")
        snapshot.append((key, value))
    return tuple(snapshot)


def _validate_packages(packages: object) -> tuple[str, ...]:
    if isinstance(packages, (str, bytes, bytearray)):
        raise TypeError("udf packages must be a sequence of strings, not a string")
    if not isinstance(packages, Sequence):
        raise TypeError("udf packages must be a sequence of strings")
    snapshot: list[str] = []
    seen: set[str] = set()
    for package in packages:
        if not isinstance(package, str):
            raise TypeError("udf packages must contain only strings")
        if package == "":
            raise ValueError("udf packages must be non-empty strings")
        if package in seen:
            raise ValueError(f"duplicate udf package: {package}")
        seen.add(package)
        snapshot.append(package)
    return tuple(snapshot)


def udf(
    *,
    inputs: Mapping[str, pa.DataType],
    output: pa.DataType,
    python: str,
    packages: Sequence[str] = (),
    output_nullable: bool = True,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Declare a local UDF without packaging or registration.

    Applying the returned decorator attaches a private frozen config snapshot
    and returns the exact same function object.
    """
    input_snapshot = _validate_inputs(inputs)
    if not isinstance(output, pa.DataType):
        raise TypeError("udf output must be a pyarrow DataType")
    if not isinstance(python, str):
        raise TypeError("udf python must be a string")
    if python == "":
        raise ValueError("udf python must be a non-empty string")
    package_snapshot = _validate_packages(packages)
    if not isinstance(output_nullable, bool):
        raise TypeError("udf output_nullable must be a bool")

    config = _UdfConfig(
        inputs=input_snapshot,
        output=output,
        output_nullable=output_nullable,
        python=python,
        packages=package_snapshot,
    )

    def decorator(fn: Callable[_P, _R]) -> Callable[_P, _R]:
        if not inspect.isfunction(fn):
            raise TypeError("udf can only decorate a Python function")
        if hasattr(fn, _CONFIG_ATTR):
            raise ValueError("function is already decorated with @udf")
        setattr(fn, _CONFIG_ATTR, config)
        return fn

    return decorator


def _get_udf_config(fn: object) -> _UdfConfig:
    """Return the private declaration snapshot for a ``@udf``-decorated function."""
    config = getattr(fn, _CONFIG_ATTR, None)
    if config is None:
        raise TypeError("function is not decorated with @udf")
    if not isinstance(config, _UdfConfig):
        raise TypeError("function is not decorated with @udf")
    return config
