# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Local authoring declaration surface for first-class UDFs.

This module snapshots declaration metadata onto a Python function and privately
validates packagable callables into a source snapshot. It does not mint durable
identity or register anything with a database.
"""

from __future__ import annotations

import ast
import inspect
import stat
import symtable
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import CodeType, FunctionType
from typing import NoReturn, ParamSpec, TypeVar

import pyarrow as pa

__all__ = ["FunctionCapability", "udf"]

_P = ParamSpec("_P")
_R = TypeVar("_R")

_CONFIG_ATTR = "__lancedb_udf_config__"
_SYNTHETIC_SOURCE_FILENAME = "<lancedb-udf>"
_PACKAGING_ERROR = "udf is not packagable"
_ALLOWED_PARAM_KINDS = (
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
    inspect.Parameter.KEYWORD_ONLY,
)


class FunctionCapability:
    """Local capability declaration for a first-class UDF.

    Construct via :meth:`network` or :meth:`secret`. Direct construction is
    rejected so callers cannot create an uninitialized capability.
    """

    __slots__ = ("_kind", "_origin", "_reference", "_environment_variable")

    def __new__(cls, *args: object, **kwargs: object) -> FunctionCapability:
        raise TypeError(
            "FunctionCapability cannot be constructed directly; "
            "use FunctionCapability.network() or FunctionCapability.secret()"
        )

    @classmethod
    def _create(
        cls,
        kind: str,
        origin: str | None,
        reference: str | None,
        environment_variable: str | None,
    ) -> FunctionCapability:
        obj = object.__new__(cls)
        object.__setattr__(obj, "_kind", kind)
        object.__setattr__(obj, "_origin", origin)
        object.__setattr__(obj, "_reference", reference)
        object.__setattr__(obj, "_environment_variable", environment_variable)
        return obj

    @classmethod
    def network(cls, origin: str) -> FunctionCapability:
        if not isinstance(origin, str):
            raise TypeError("origin must be a string")
        if origin == "":
            raise ValueError("origin must be non-empty")
        return cls._create("network", origin, None, None)

    @classmethod
    def secret(cls, reference: str, *, environment_variable: str) -> FunctionCapability:
        if not isinstance(reference, str):
            raise TypeError("reference must be a string")
        if not isinstance(environment_variable, str):
            raise TypeError("environment_variable must be a string")
        if reference == "":
            raise ValueError("reference must be non-empty")
        if environment_variable == "":
            raise ValueError("environment_variable must be non-empty")
        return cls._create("secret", None, reference, environment_variable)

    @property
    def kind(self) -> str:
        return self._kind

    @property
    def origin(self) -> str | None:
        return self._origin

    @property
    def reference(self) -> str | None:
        return self._reference

    @property
    def environment_variable(self) -> str | None:
        return self._environment_variable

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(
            f"{type(self).__name__!r} object attribute {name!r} is read-only"
        )

    def __delattr__(self, name: str) -> None:
        raise AttributeError(
            f"{type(self).__name__!r} object attribute {name!r} is read-only"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FunctionCapability):
            return NotImplemented
        return (
            self._kind == other._kind
            and self._origin == other._origin
            and self._reference == other._reference
            and self._environment_variable == other._environment_variable
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._kind,
                self._origin,
                self._reference,
                self._environment_variable,
            )
        )

    def __repr__(self) -> str:
        if self._kind == "network":
            return f"FunctionCapability.network({self._origin!r})"
        return (
            "FunctionCapability.secret("
            f"environment_variable={self._environment_variable!r})"
        )


@dataclass(frozen=True, slots=True)
class _UdfConfig:
    """Private frozen snapshot of a ``@udf`` declaration."""

    inputs: tuple[tuple[str, pa.DataType], ...]
    output: pa.DataType
    output_nullable: bool
    python: str
    packages: tuple[str, ...]
    capabilities: tuple[FunctionCapability, ...]


@dataclass(frozen=True, slots=True)
class _PackagedUdf:
    """Private frozen snapshot of a validated packagable UDF."""

    source: str
    module: str
    callable_name: str
    config: _UdfConfig

    def __repr__(self) -> str:
        return (
            f"_PackagedUdf(source=<redacted>, module={self.module!r}, "
            f"callable_name={self.callable_name!r}, config={self.config!r})"
        )


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


def _validate_capabilities(capabilities: object) -> tuple[FunctionCapability, ...]:
    if isinstance(capabilities, (str, bytes, bytearray)):
        raise TypeError(
            "udf capabilities must be a sequence of FunctionCapability, not a string"
        )
    if not isinstance(capabilities, Sequence):
        raise TypeError("udf capabilities must be a sequence of FunctionCapability")
    snapshot: list[FunctionCapability] = []
    for capability in capabilities:
        if not isinstance(capability, FunctionCapability):
            raise TypeError(
                "udf capabilities must contain only FunctionCapability values"
            )
        snapshot.append(capability)
    return tuple(snapshot)


def udf(
    *,
    inputs: Mapping[str, pa.DataType],
    output: pa.DataType,
    python: str,
    packages: Sequence[str] = (),
    output_nullable: bool = True,
    capabilities: Sequence[FunctionCapability] = (),
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
    capability_snapshot = _validate_capabilities(capabilities)

    config = _UdfConfig(
        inputs=input_snapshot,
        output=output,
        output_nullable=output_nullable,
        python=python,
        packages=package_snapshot,
        capabilities=capability_snapshot,
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


def _packaging_reject() -> NoReturn:
    raise ValueError(_PACKAGING_ERROR) from None


def _is_ordinary_function(fn: FunctionType) -> bool:
    if fn.__name__ == "<lambda>":
        return False
    if fn.__qualname__ != fn.__name__:
        return False
    if inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn):
        return False
    if inspect.isgeneratorfunction(fn):
        return False
    return True


def _resolve_source_path(fn: FunctionType, module: object) -> Path:
    try:
        fn_source: str | None = inspect.getsourcefile(fn)
    except TypeError:
        fn_source = None
        source_lookup_failed = True
    else:
        source_lookup_failed = False
    if source_lookup_failed:
        _packaging_reject()
    module_file = vars(module).get("__file__")
    if not fn_source or not isinstance(module_file, str) or module_file == "":
        _packaging_reject()
    try:
        resolved_paths: tuple[Path, Path] | None = (
            Path(fn_source).resolve(),
            Path(module_file).resolve(),
        )
    except (OSError, RuntimeError):
        resolved_paths = None
    if resolved_paths is None:
        _packaging_reject()
    fn_path, module_path = resolved_paths
    if fn_path != module_path:
        _packaging_reject()
    if fn_path.suffix != ".py":
        _packaging_reject()
    try:
        mode: int | None = fn_path.stat().st_mode
    except OSError:
        mode = None
    if mode is None:
        _packaging_reject()
    if not stat.S_ISREG(mode):
        _packaging_reject()
    return fn_path


def _validate_source(
    source: str, callable_name: str
) -> tuple[CodeType, symtable.SymbolTable]:
    try:
        module_code = compile(
            source,
            _SYNTHETIC_SOURCE_FILENAME,
            "exec",
            optimize=sys.flags.optimize,
        )
        ast.parse(source, filename=_SYNTHETIC_SOURCE_FILENAME, mode="exec")
        table = symtable.symtable(source, _SYNTHETIC_SOURCE_FILENAME, "exec")
        parsed: tuple[CodeType, symtable.SymbolTable] | None = (module_code, table)
    except Exception:
        parsed = None
    if parsed is None:
        _packaging_reject()
    module_code, table = parsed

    for child in table.get_children():
        if child.get_name() == callable_name and child.get_type() == "function":
            return module_code, table
    _packaging_reject()


def _source_bound_names(table: symtable.SymbolTable) -> set[str]:
    names: set[str] = set()
    for symbol in table.get_symbols():
        if symbol.is_imported() or symbol.is_assigned() or symbol.is_namespace():
            names.add(symbol.get_name())
    return names


def _code_fingerprint(code: CodeType) -> tuple[object, ...]:
    """Structural fingerprint ignoring only location/debug fields."""
    constants = tuple(
        _code_fingerprint(constant) if isinstance(constant, CodeType) else constant
        for constant in code.co_consts
    )
    return (
        code.co_name,
        getattr(code, "co_qualname", code.co_name),
        code.co_argcount,
        code.co_posonlyargcount,
        code.co_kwonlyargcount,
        code.co_flags,
        code.co_code,
        code.co_names,
        code.co_varnames,
        code.co_freevars,
        code.co_cellvars,
        getattr(code, "co_exceptiontable", b""),
        constants,
    )


def _toplevel_code_candidates(
    module_code: CodeType, callable_name: str
) -> list[CodeType]:
    candidates: list[CodeType] = []
    for constant in module_code.co_consts:
        if not isinstance(constant, CodeType):
            continue
        if constant.co_name != callable_name:
            continue
        if getattr(constant, "co_qualname", callable_name) != callable_name:
            continue
        candidates.append(constant)
    return candidates


def _validate_loaded_code_matches_source(
    fn: FunctionType, module_code: CodeType
) -> None:
    candidates = _toplevel_code_candidates(module_code, fn.__name__)
    if not candidates:
        _packaging_reject()
    target = _code_fingerprint(fn.__code__)
    if not any(_code_fingerprint(candidate) == target for candidate in candidates):
        _packaging_reject()


def _validate_signature(fn: FunctionType, config: _UdfConfig) -> None:
    try:
        signature: inspect.Signature | None = inspect.signature(fn)
    except (TypeError, ValueError):
        signature = None
    if signature is None:
        _packaging_reject()
    parameters = list(signature.parameters.values())
    expected = [name for name, _ in config.inputs]
    actual = [parameter.name for parameter in parameters]
    if actual != expected:
        _packaging_reject()
    for parameter in parameters:
        if parameter.kind not in _ALLOWED_PARAM_KINDS:
            _packaging_reject()


def _validate_ambient_globals(fn: FunctionType, table: symtable.SymbolTable) -> None:
    try:
        closure_vars: inspect.ClosureVars | None = inspect.getclosurevars(fn)
    except (TypeError, ValueError):
        closure_vars = None
    if closure_vars is None:
        _packaging_reject()
    if closure_vars.nonlocals:
        _packaging_reject()
    bound_names = _source_bound_names(table)
    for name in closure_vars.globals:
        if name not in bound_names:
            _packaging_reject()


def _package_udf(fn: object) -> _PackagedUdf:
    """Validate and snapshot a packagable ``@udf``-decorated function."""
    config = _get_udf_config(fn)
    if not isinstance(fn, FunctionType) or not _is_ordinary_function(fn):
        _packaging_reject()

    module_name = fn.__module__
    if (
        not isinstance(module_name, str)
        or module_name == ""
        or module_name == "__main__"
    ):
        _packaging_reject()
    module = sys.modules.get(module_name)
    if module is None:
        _packaging_reject()
    callable_name = fn.__name__
    if vars(module).get(callable_name) is not fn:
        _packaging_reject()

    source_path = _resolve_source_path(fn, module)
    try:
        source: str | None = source_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        source = None
    if source is None:
        _packaging_reject()

    module_code, table = _validate_source(source, callable_name)
    _validate_signature(fn, config)
    _validate_ambient_globals(fn, table)
    _validate_loaded_code_matches_source(fn, module_code)

    return _PackagedUdf(
        source=source,
        module=module_name,
        callable_name=callable_name,
        config=config,
    )
