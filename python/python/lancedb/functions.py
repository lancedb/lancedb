# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Canonical values exchanged with LanceDB Enterprise Function services.

These immutable models contain client/wire state only. Catalog persistence,
environment bake, secret resolution, and execution are owned by Sophon.
"""

from __future__ import annotations

import ast
import base64
import functools
import hashlib
import inspect
import json
import math
import re
import sys
import textwrap
import types
import uuid
from collections.abc import Mapping
from datetime import date, datetime
from typing import (
    Annotated,
    Any,
    Callable,
    Optional,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)

import pyarrow as pa
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    conint,
    field_validator,
    model_validator,
)

_Int32 = conint(strict=True, ge=-(2**31), le=2**31 - 1)
_UInt32 = conint(strict=True, ge=0, le=2**32 - 1)
_UInt64 = conint(strict=True, ge=0, le=2**64 - 1)


class _FrozenDict(dict):
    def _immutable(self, *args, **kwargs):
        raise TypeError("remote canonical values are immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable

    def __ior__(self, other):
        self._immutable()


def _freeze_value(value):
    if isinstance(value, Mapping):
        return _FrozenDict({key: _freeze_value(child) for key, child in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(child) for child in value)
    return value


def _validate_literal(value):
    if isinstance(value, float):
        raise ValueError(
            "floating-point Function literals are not part of the Slice 1 "
            "canonical wire contract"
        )
    if isinstance(value, int) and not isinstance(value, bool):
        if not -(2**63) <= value <= 2**64 - 1:
            raise ValueError(
                "Function integer literal is outside the canonical JSON range"
            )
    elif isinstance(value, Mapping):
        for child in value.values():
            _validate_literal(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _validate_literal(child)
    return value


def _known_wire_value(value):
    if isinstance(value, _RemoteValue):
        return value._known_dict()
    if isinstance(value, Mapping):
        return {key: _known_wire_value(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_known_wire_value(child) for child in value]
    return value


class _RemoteValue(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    @model_validator(mode="after")
    def _freeze_mappings(self):
        for name, value in self.__dict__.items():
            object.__setattr__(self, name, _freeze_value(value))
        return self

    @classmethod
    def from_json(cls, payload: str):
        return cls.model_validate_json(payload)

    def _known_dict(self) -> dict[str, Any]:
        known = {}
        for name, field in self.__class__.model_fields.items():
            value = getattr(self, name)
            if value is None:
                continue
            if not field.is_required():
                default_factory = field.default_factory
                if default_factory is not None and value == default_factory():
                    continue
                if default_factory is None and value == field.default:
                    continue
            known[name] = _known_wire_value(value)
        return known

    def _copy(self, *, update: Mapping[str, Any]):
        update = {name: _freeze_value(value) for name, value in update.items()}
        return self.model_copy(update=update)

    def to_canonical_json(self) -> str:
        return json.dumps(
            self._known_dict(),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )


class _OpenRemoteValue(_RemoteValue):
    """Forward-readable value whose extras stay out of canonical encoding."""

    model_config = ConfigDict(extra="allow", frozen=True)

    def _unknown_field_names(self) -> set[str]:
        return set((self.__pydantic_extra__ or {}).keys())


class FunctionArtifact(_RemoteValue):
    """Content-addressed Python artifact identity."""

    kind: str
    digest: str
    entrypoint: str


class FunctionArtifactContent(_RemoteValue):
    """Encoded artifact bytes uploaded during remote registration."""

    encoding: str
    data: str


class PythonAdapterSpec(_RemoteValue):
    """Internal scalar-callable to Arrow-batch adapter selection."""

    kind: str
    version: _UInt32


class FunctionArtifactRequest(_RemoteValue):
    """Source artifact uploaded while registering a Function."""

    kind: str
    digest: str
    entrypoint: str
    content: FunctionArtifactContent
    adapter: PythonAdapterSpec


class FunctionParameter(_RemoteValue):
    name: str
    arrow_type: str
    nullable: bool


class FunctionResultField(_OpenRemoteValue):
    name: str
    arrow_type: str
    nullable: bool


class FunctionOutput(_OpenRemoteValue):
    """Scalar or ordered named-struct output; unknown kinds remain decodable."""

    kind: str
    arrow_type: Optional[str] = None
    nullable: Optional[bool] = None
    fields: tuple[FunctionResultField, ...] = ()


class FunctionSignature(_RemoteValue):
    inputs: tuple[FunctionParameter, ...]
    output: FunctionOutput


class PythonEnvironmentSpec(_RemoteValue):
    """One Sophon-managed Python environment source."""

    kind: str
    packages: tuple[str, ...] = ()
    path: Optional[str] = None
    modules: tuple[str, ...] = ()
    image: Optional[str] = None


class PythonRuntimeSpec(_RemoteValue):
    """Remote runtime definition with non-secret environment values.

    V1 supports ``kind="python"``. Newer runtime kinds remain readable, while
    their unknown payload fields are intentionally not retained by the client.
    """

    kind: str
    python_version: Optional[str] = None
    environment: Optional[PythonEnvironmentSpec] = None
    env: Optional[Mapping[str, str]] = None

    @model_validator(mode="after")
    def _validate_runtime_kind(self):
        if self.kind == "python":
            if self.python_version is None:
                raise ValueError("python runtime requires python_version")
            if self.environment is None:
                raise ValueError("python runtime requires environment")
        else:
            object.__setattr__(self, "python_version", None)
            object.__setattr__(self, "environment", None)
            object.__setattr__(self, "env", None)
        return self


class FunctionVersion(_RemoteValue):
    """An exact immutable Function version returned by Enterprise.

    Scheduling resources, priority, concurrency, and retry policy belong to
    the submitting Job and are not part of this identity.
    """

    name: str
    version: str
    artifact: FunctionArtifact
    signature: FunctionSignature
    runtime: PythonRuntimeSpec
    runtime_digest: str
    environment_digest: str
    required_secrets: tuple[str, ...] = ()
    created_at: str

    def __call__(self, **inputs: Any) -> FunctionApplication:
        """Bind this exact version to table columns as one grouped application."""
        from lancedb.expr import Expr

        parameters = tuple(parameter.name for parameter in self.signature.inputs)
        missing = [parameter for parameter in parameters if parameter not in inputs]
        unknown = sorted(set(inputs) - set(parameters))
        if missing or unknown:
            details = []
            if missing:
                details.append(f"missing inputs: {missing!r}")
            if unknown:
                details.append(f"unknown inputs: {unknown!r}")
            raise TypeError("invalid Function inputs (" + "; ".join(details) + ")")

        bindings = []
        for parameter in parameters:
            value = inputs[parameter]
            if not isinstance(value, Expr) or value._column_path is None:
                raise TypeError(
                    f"Function input {parameter!r} must be a direct col(...) reference"
                )
            bindings.append(
                ApplicationInput(
                    parameter=parameter,
                    kind="column",
                    value={"path": value._column_path},
                )
            )
        return FunctionApplication(
            function=FunctionVersionRef(name=self.name, version=self.version),
            inputs=tuple(bindings),
            output=self.signature.output,
            group_id=f"fg_{uuid.uuid4().hex}",
        )


class FunctionRegistrationRequest(_RemoteValue):
    """Stable remote registration envelope produced by :func:`udf`.

    Only secret names are represented. Secret values are resolved inside the
    remote service and have no client request field.
    """

    name: str
    artifact: FunctionArtifactRequest
    signature: FunctionSignature
    runtime: PythonRuntimeSpec
    required_secrets: tuple[str, ...] = ()


class FunctionVersionRef(_OpenRemoteValue):
    name: str
    version: str


class ApplicationInput(_OpenRemoteValue):
    """One parameter value.

    Slice 1 freezes integers, strings, booleans, nulls, arrays, and objects.
    Floating-point literal encoding is deferred until Python authoring is
    introduced with a language-neutral numeric representation.
    """

    parameter: str
    kind: str
    value: Any

    @field_validator("value")
    @classmethod
    def _validate_value(cls, value):
        return _validate_literal(value)


class FunctionApplication(_OpenRemoteValue):
    """Immutable pre-declaration application of an exact Function version."""

    function: FunctionVersionRef
    inputs: tuple[ApplicationInput, ...]
    output: FunctionOutput
    group_id: str
    columns: Mapping[str, str] = Field(default_factory=dict)

    def _known_dict(self) -> dict[str, Any]:
        value = super()._known_dict()
        for name in self._unknown_field_names():
            value.pop(name, None)
        return value

    def _ensure_declarable(self) -> None:
        unknown = {f"application.{name}" for name in self._unknown_field_names()}
        unknown.update(
            f"function.{name}" for name in self.function._unknown_field_names()
        )
        for index, input_value in enumerate(self.inputs):
            unknown.update(
                f"inputs[{index}].{name}" for name in input_value._unknown_field_names()
            )
        unknown.update(f"output.{name}" for name in self.output._unknown_field_names())
        for index, field in enumerate(self.output.fields):
            unknown.update(
                f"output.fields[{index}].{name}"
                for name in field._unknown_field_names()
            )
        if unknown:
            raise ValueError(
                "Function application contains fields from a newer contract: "
                f"{sorted(unknown)!r}"
            )

    def rename(self, *, columns: Mapping[str, str]) -> FunctionApplication:
        """Return a copy with result-field to table-column aliases."""
        if self.output.kind != "named_struct":
            raise ValueError("rename(columns=...) requires a named-struct application")
        result_fields = {field.name for field in self.output.fields}
        unknown = set(columns) - result_fields
        if unknown:
            raise ValueError(f"unknown Function result fields: {sorted(unknown)!r}")
        merged = dict(self.columns)
        merged.update(columns)
        destinations = tuple(
            merged.get(field.name, field.name) for field in self.output.fields
        )
        if len(set(destinations)) != len(destinations):
            raise ValueError("FunctionApplication rename destinations must be unique")
        return self._copy(update={"columns": merged})


class InputBinding(_RemoteValue):
    parameter: str
    field_id: _Int32
    field_path: str
    arrow_type: str
    nullable: bool


class OutputMapping(_RemoteValue):
    """One stable result-field mapping.

    Assignment state is outside the Slice 1 client contract. During the NULL
    transition Lance exposes no public cell-flag identifier to persist here.
    """

    result_field: str
    output_name: str
    output_field_id: _Int32
    output_ordinal: _UInt32
    arrow_type: str
    nullable: bool


class FunctionBinding(_RemoteValue):
    """Immutable grouped binding persisted by the Enterprise table service."""

    binding_id: str
    revision: _UInt64
    function: FunctionVersionRef
    group_id: str
    inputs: tuple[InputBinding, ...]
    outputs: tuple[OutputMapping, ...]
    input_schema: Optional[Mapping[str, Any]] = None
    output_schema: Optional[Mapping[str, Any]] = None


class RefreshColumnResult(_RemoteValue):
    """Terminal result of a remote Function-column refresh Job."""

    rows_assigned: _UInt64
    rows_failed: _UInt64
    rows_remaining: _UInt64
    source_version: _UInt64
    published_version: Optional[_UInt64] = None

    @property
    def rows_filled(self) -> int:
        """Deprecated compatibility alias for :attr:`rows_assigned`."""
        return self.rows_assigned

    @property
    def version(self) -> Optional[int]:
        """Deprecated compatibility alias for :attr:`published_version`."""
        return self.published_version


_FUNCTION_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")
_SECRET_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _canonical_arrow_type(data_type: pa.DataType) -> str:
    primitive_types = (
        (pa.bool_(), "bool"),
        (pa.int8(), "int8"),
        (pa.int16(), "int16"),
        (pa.int32(), "int32"),
        (pa.int64(), "int64"),
        (pa.uint8(), "uint8"),
        (pa.uint16(), "uint16"),
        (pa.uint32(), "uint32"),
        (pa.uint64(), "uint64"),
        (pa.float16(), "float16"),
        (pa.float32(), "float32"),
        (pa.float64(), "float64"),
        (pa.string(), "utf8"),
        (pa.large_utf8(), "large_utf8"),
        (pa.binary(), "binary"),
        (pa.large_binary(), "large_binary"),
        (pa.date32(), "date32"),
        (pa.date64(), "date64"),
    )
    for candidate, name in primitive_types:
        if data_type == candidate:
            return name
    if pa.types.is_fixed_size_binary(data_type):
        return f"fixed_size_binary[{data_type.byte_width}]"
    if pa.types.is_list(data_type):
        return f"list<{_canonical_arrow_type(data_type.value_type)}>"
    if pa.types.is_large_list(data_type):
        return f"large_list<{_canonical_arrow_type(data_type.value_type)}>"
    if pa.types.is_fixed_size_list(data_type):
        return (
            f"fixed_size_list<{_canonical_arrow_type(data_type.value_type)}>"
            f"[{data_type.list_size}]"
        )
    if pa.types.is_struct(data_type):
        fields = ",".join(
            f"{field.name}:{_canonical_arrow_type(field.type)}" for field in data_type
        )
        return f"struct<{fields}>"
    if pa.types.is_timestamp(data_type):
        timezone = f",tz={data_type.tz}" if data_type.tz is not None else ""
        return f"timestamp[{data_type.unit}{timezone}]"
    if pa.types.is_time32(data_type) or pa.types.is_time64(data_type):
        return f"time[{data_type.unit}]"
    if pa.types.is_duration(data_type):
        return f"duration[{data_type.unit}]"
    if pa.types.is_decimal(data_type):
        bit_width = data_type.bit_width
        return f"decimal{bit_width}({data_type.precision},{data_type.scale})"
    raise TypeError(f"unsupported Arrow type for Function signature: {data_type}")


def _annotation_type(annotation: Any) -> tuple[pa.DataType, bool]:
    nullable = False
    origin = get_origin(annotation)
    if origin in (Union, types.UnionType):
        arguments = get_args(annotation)
        non_none = tuple(
            argument for argument in arguments if argument is not type(None)
        )
        if len(non_none) != 1 or len(non_none) == len(arguments):
            raise TypeError(f"unsupported union annotation: {annotation!r}")
        annotation = non_none[0]
        nullable = True

    origin = get_origin(annotation)
    if origin is Annotated:
        base, *metadata = get_args(annotation)
        arrow_types = [value for value in metadata if isinstance(value, pa.DataType)]
        if len(arrow_types) != 1:
            raise TypeError(
                "Annotated Function types require exactly one PyArrow DataType"
            )
        _, base_nullable = _annotation_type(base)
        return arrow_types[0], nullable or base_nullable

    if isinstance(annotation, pa.DataType):
        return annotation, nullable
    if annotation is bool:
        return pa.bool_(), nullable
    if annotation is int:
        return pa.int64(), nullable
    if annotation is float:
        return pa.float64(), nullable
    if annotation is str:
        return pa.string(), nullable
    if annotation is bytes:
        return pa.binary(), nullable
    if annotation is date:
        return pa.date32(), nullable
    if annotation is datetime:
        return pa.timestamp("us"), nullable
    if get_origin(annotation) is list:
        arguments = get_args(annotation)
        if len(arguments) != 1:
            raise TypeError(f"unsupported list annotation: {annotation!r}")
        value_type, value_nullable = _annotation_type(arguments[0])
        if value_nullable:
            raise TypeError("nullable Function list elements are not supported")
        return pa.list_(value_type), nullable
    raise TypeError(f"unsupported Function annotation: {annotation!r}")


def _callable_parameters(function: Callable[..., Any]) -> tuple[inspect.Parameter, ...]:
    parameters = tuple(inspect.signature(function).parameters.values())
    for parameter in parameters:
        if parameter.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            raise TypeError("Function callables require named, non-variadic parameters")
        if parameter.default is not inspect.Parameter.empty:
            raise TypeError("Function callable defaults are not supported")
    return parameters


def _function_output(output: pa.DataType | pa.Field | pa.Schema) -> FunctionOutput:
    if isinstance(output, pa.Schema):
        fields = tuple(output)
    elif isinstance(output, pa.Field) and pa.types.is_struct(output.type):
        if output.nullable:
            raise ValueError("Function output must be non-nullable")
        fields = tuple(output.type)
    elif isinstance(output, pa.DataType) and pa.types.is_struct(output):
        fields = tuple(output)
    else:
        field = (
            output
            if isinstance(output, pa.Field)
            else pa.field("result", output, nullable=False)
        )
        if not isinstance(field, pa.Field):
            raise TypeError(
                "output_schema must be a PyArrow DataType, Field, or Schema"
            )
        if field.nullable:
            raise ValueError("Function output must be non-nullable")
        return FunctionOutput(
            kind="scalar",
            arrow_type=_canonical_arrow_type(field.type),
            nullable=False,
        )

    if not fields:
        raise ValueError("named-struct Function output must contain at least one field")
    if any(field.nullable for field in fields):
        raise ValueError("Function output fields must be non-nullable")
    names = [field.name for field in fields]
    if len(set(names)) != len(names):
        raise ValueError("Function output field names must be unique")
    return FunctionOutput(
        kind="named_struct",
        fields=tuple(
            FunctionResultField(
                name=field.name,
                arrow_type=_canonical_arrow_type(field.type),
                nullable=False,
            )
            for field in fields
        ),
    )


def _infer_signature(
    function: Callable[..., Any],
    input_schema: Optional[pa.Schema],
    output_schema: Optional[pa.DataType | pa.Field | pa.Schema],
) -> FunctionSignature:
    parameters = _callable_parameters(function)
    if (input_schema is None) != (output_schema is None):
        raise ValueError("input_schema and output_schema must be provided together")

    if input_schema is not None:
        if not isinstance(input_schema, pa.Schema):
            raise TypeError("input_schema must be a PyArrow Schema")
        expected = tuple(parameter.name for parameter in parameters)
        actual = tuple(input_schema.names)
        if actual != expected:
            raise ValueError(
                "input_schema fields must exactly match callable parameters in order: "
                f"expected {expected!r}, got {actual!r}"
            )
        inputs = tuple(
            FunctionParameter(
                name=field.name,
                arrow_type=_canonical_arrow_type(field.type),
                nullable=field.nullable,
            )
            for field in input_schema
        )
        return FunctionSignature(inputs=inputs, output=_function_output(output_schema))

    try:
        annotations = get_type_hints(function, include_extras=True)
    except Exception as error:
        raise TypeError(f"failed to resolve Function annotations: {error}") from error
    missing = [
        parameter.name for parameter in parameters if parameter.name not in annotations
    ]
    if missing or "return" not in annotations:
        names = missing + ([] if "return" in annotations else ["return"])
        raise TypeError(f"missing Function annotations: {names!r}")
    inputs = []
    for parameter in parameters:
        data_type, nullable = _annotation_type(annotations[parameter.name])
        inputs.append(
            FunctionParameter(
                name=parameter.name,
                arrow_type=_canonical_arrow_type(data_type),
                nullable=nullable,
            )
        )
    output_type, output_nullable = _annotation_type(annotations["return"])
    if output_nullable:
        raise ValueError("Function output must be non-nullable")
    return FunctionSignature(
        inputs=tuple(inputs),
        output=_function_output(pa.field("result", output_type, nullable=False)),
    )


def _is_udf_decorator(node: ast.expr) -> bool:
    if isinstance(node, ast.Call):
        node = node.func
    return (isinstance(node, ast.Name) and node.id == "udf") or (
        isinstance(node, ast.Attribute) and node.attr == "udf"
    )


def _literal_source(value: Any) -> str:
    if value is None or type(value) in (bool, int, str, bytes):
        return repr(value)
    if type(value) is float and math.isfinite(value):
        return repr(value)
    if type(value) is tuple:
        children = ", ".join(_literal_source(child) for child in value)
        if len(value) == 1:
            children += ","
        return f"({children})"
    raise TypeError(
        "Function source references an unsupported global value of type "
        f"{type(value).__name__}"
    )


def _package_source(function: Callable[..., Any]) -> bytes:
    if not inspect.isfunction(function) or inspect.iscoroutinefunction(function):
        raise TypeError("@udf requires a synchronous Python function")
    try:
        source = textwrap.dedent(inspect.getsource(function))
    except (OSError, TypeError) as error:
        raise ValueError("@udf requires inspectable Python source") from error
    module = ast.parse(source)
    definitions = [
        node
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == function.__name__
    ]
    if len(definitions) != 1 or not isinstance(definitions[0], ast.FunctionDef):
        raise ValueError("@udf source must contain exactly one synchronous function")
    definition = definitions[0]
    if any(not _is_udf_decorator(decorator) for decorator in definition.decorator_list):
        raise ValueError("@udf cannot package additional Python decorators")
    definition.decorator_list = []

    closure = inspect.getclosurevars(function)
    if closure.nonlocals:
        raise ValueError("@udf cannot package functions that capture closure values")
    if closure.unbound:
        raise ValueError(
            f"@udf source contains unresolved global names: {sorted(closure.unbound)!r}"
        )
    globals_source = []
    for name, value in sorted(closure.globals.items()):
        if isinstance(value, types.ModuleType):
            globals_source.append(f"import {value.__name__} as {name}")
        else:
            globals_source.append(f"{name} = {_literal_source(value)}")

    function_source = ast.unparse(definition)
    parts = ["from __future__ import annotations"]
    if globals_source:
        parts.extend(["", *globals_source])
    parts.extend(["", function_source, ""])
    return "\n".join(parts).encode("utf-8")


class UdfDefinition:
    """A scalar Python callable prepared for remote Function registration.

    Instances are created with :func:`udf`. Calling an instance executes the
    original scalar Python function, which keeps local unit testing ordinary.
    Remote execution adapts that scalar callable to the internal Arrow batch
    ABI described by the registration artifact.
    """

    def __init__(
        self,
        function: Callable[..., Any],
        *,
        name: Optional[str],
        input_schema: Optional[pa.Schema],
        output_schema: Optional[pa.DataType | pa.Field | pa.Schema],
        pip: tuple[str, ...],
        env: Mapping[str, str],
        secrets: tuple[str, ...],
        python_version: Optional[str],
    ):
        function_name = name or function.__name__
        if not _FUNCTION_NAME.fullmatch(function_name):
            raise ValueError(f"invalid Function name: {function_name!r}")
        packages = tuple(sorted(set(pip)))
        if any(not package or package != package.strip() for package in packages):
            raise ValueError("pip requirements must be non-empty and trimmed")
        environment = dict(env)
        if any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in environment.items()
        ):
            raise TypeError("Function env keys and values must be strings")
        required_secrets = tuple(sorted(set(secrets)))
        invalid_secrets = [
            secret for secret in required_secrets if not _SECRET_NAME.fullmatch(secret)
        ]
        if invalid_secrets:
            raise ValueError(f"invalid Function secret names: {invalid_secrets!r}")
        overlap = set(environment) & set(required_secrets)
        if overlap:
            raise ValueError(
                f"Function env and secret names must be disjoint: {sorted(overlap)!r}"
            )

        signature = _infer_signature(function, input_schema, output_schema)
        source = _package_source(function)
        digest = f"sha256:{hashlib.sha256(source).hexdigest()}"
        runtime = PythonRuntimeSpec(
            kind="python",
            python_version=python_version
            or f"{sys.version_info.major}.{sys.version_info.minor}",
            environment=PythonEnvironmentSpec(kind="pip", packages=packages),
            env=environment,
        )
        self._function = function
        self._request = FunctionRegistrationRequest(
            name=function_name,
            artifact=FunctionArtifactRequest(
                kind="python_callable",
                digest=digest,
                entrypoint=function.__name__,
                content=FunctionArtifactContent(
                    encoding="base64",
                    data=base64.b64encode(source).decode("ascii"),
                ),
                adapter=PythonAdapterSpec(
                    kind="scalar_to_arrow_batch",
                    version=1,
                ),
            ),
            signature=signature,
            runtime=runtime,
            required_secrets=required_secrets,
        )
        functools.update_wrapper(self, function)

    @property
    def registration_request(self) -> FunctionRegistrationRequest:
        """The immutable request sent by ``create_function_async``."""
        return self._request

    def __call__(self, *args, **kwargs):
        return self._function(*args, **kwargs)


@overload
def udf(function: Callable[..., Any]) -> UdfDefinition: ...


@overload
def udf(
    function: None = None,
    *,
    name: Optional[str] = None,
    input_schema: Optional[pa.Schema] = None,
    output_schema: Optional[pa.DataType | pa.Field | pa.Schema] = None,
    pip: tuple[str, ...] | list[str] = (),
    env: Optional[Mapping[str, str]] = None,
    secrets: tuple[str, ...] | list[str] = (),
    python_version: Optional[str] = None,
) -> Callable[[Callable[..., Any]], UdfDefinition]: ...


def udf(
    function: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    input_schema: Optional[pa.Schema] = None,
    output_schema: Optional[pa.DataType | pa.Field | pa.Schema] = None,
    pip: tuple[str, ...] | list[str] = (),
    env: Optional[Mapping[str, str]] = None,
    secrets: tuple[str, ...] | list[str] = (),
    python_version: Optional[str] = None,
):
    """Prepare a scalar Python callable for remote Function registration.

    Input and output signatures are inferred from supported annotations. For
    Arrow types annotations cannot express precisely, pass ``input_schema``
    and ``output_schema`` together. Nullable outputs are rejected because V1
    uses physical NULL to represent unassigned computed-column rows.

    Parameters
    ----------
    function : Callable, optional
        The synchronous scalar callable to package.
    name : str, optional
        The remote Function name. Defaults to the callable name.
    input_schema : pyarrow.Schema, optional
        Explicit input fields in the exact order of the callable parameters.
        Must be provided together with ``output_schema``.
    output_schema : pyarrow.DataType, pyarrow.Field, or pyarrow.Schema, optional
        Explicit scalar or named-struct output. Must be non-nullable and be
        provided together with ``input_schema``.
    pip : sequence of str, optional
        Pip requirements for the remote environment.
    env : mapping of str to str, optional
        Non-secret environment variables. Use ``secrets`` for credentials.
    secrets : sequence of str, optional
        Names of secrets resolved by the remote service. Secret values are not
        accepted by this API or included in the registration request.
    python_version : str, optional
        Remote Python major/minor version. Defaults to the client version.

    Returns
    -------
    UdfDefinition
        A callable definition accepted by
        :meth:`lancedb.db.DBConnection.create_function`,
        :meth:`lancedb.db.AsyncConnection.create_function_async` and
        :meth:`lancedb.db.DBConnection.create_function_async`.

    Examples
    --------
    >>> from lancedb import udf
    >>> @udf(pip=["numpy==2.2.0"], secrets=["MODEL_TOKEN"])
    ... def score(value: float) -> float:
    ...     return value * 2
    >>> score(1.5)
    3.0
    """

    def decorate(target: Callable[..., Any]) -> UdfDefinition:
        return UdfDefinition(
            target,
            name=name,
            input_schema=input_schema,
            output_schema=output_schema,
            pip=tuple(pip),
            env={} if env is None else env,
            secrets=tuple(secrets),
            python_version=python_version,
        )

    if function is None:
        return decorate
    return decorate(function)


__all__ = [
    "ApplicationInput",
    "FunctionApplication",
    "FunctionArtifact",
    "FunctionArtifactContent",
    "FunctionArtifactRequest",
    "FunctionBinding",
    "FunctionOutput",
    "FunctionParameter",
    "FunctionRegistrationRequest",
    "FunctionResultField",
    "FunctionSignature",
    "FunctionVersion",
    "FunctionVersionRef",
    "InputBinding",
    "OutputMapping",
    "PythonEnvironmentSpec",
    "PythonAdapterSpec",
    "PythonRuntimeSpec",
    "RefreshColumnResult",
    "UdfDefinition",
    "udf",
]
