# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Canonical Function values exchanged with LanceDB Enterprise services.

These immutable models contain client/wire state only. Catalog persistence,
environment bake, and execution are owned by Sophon.
``RefreshColumnResult`` is also the backend-neutral result of a local
expression-backed refresh job.
"""

from __future__ import annotations

import ast
import builtins
import base64
import functools
import hashlib
import importlib
import inspect
import symtable
import json
import math
import re
import sys
import textwrap
import types
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

from .schema import is_blob_v2_field as _is_blob_v2_field

_Int32 = conint(strict=True, ge=-(2**31), le=2**31 - 1)
_UInt32 = conint(strict=True, ge=0, le=2**32 - 1)
_UInt64 = conint(strict=True, ge=0, le=2**64 - 1)


def _validate_gpu_wire_marker(value: Any) -> bool:
    if value is not True:
        raise ValueError("runtime.gpu must be true")
    return True


def _normalize_gpu_marker(value: bool) -> Optional[bool]:
    if not isinstance(value, bool):
        raise ValueError("gpu must be a boolean")
    return True if value else None


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
    channels: tuple[str, ...] = ()
    path: Optional[str] = None
    modules: tuple[str, ...] = ()
    image: Optional[str] = None


class PythonRuntimeSpec(_RemoteValue):
    """Remote runtime definition with environment values.

    V1 supports ``kind="python"``. Newer runtime kinds remain readable, while
    their unknown payload fields are intentionally not retained by the client.
    """

    kind: str
    python_version: Optional[str] = None
    environment: Optional[PythonEnvironmentSpec] = None
    env: Optional[Mapping[str, str]] = None
    gpu: Optional[bool] = None

    @model_validator(mode="before")
    @classmethod
    def _discard_unknown_runtime_payload(cls, value):
        if isinstance(value, Mapping):
            kind = value.get("kind")
            if isinstance(kind, str) and kind not in {"python", "python_v2"}:
                return {"kind": kind}
        return value

    @field_validator("gpu", mode="before")
    @classmethod
    def _validate_gpu_marker(cls, value):
        if value is None:
            return None
        return _validate_gpu_wire_marker(value)

    @model_validator(mode="after")
    def _validate_runtime_kind(self):
        if self.kind == "python":
            if self.python_version is None:
                raise ValueError("python runtime requires python_version")
            if self.environment is None:
                raise ValueError("python runtime requires environment")
            if self.gpu is not None:
                raise ValueError("python runtime with gpu requires kind='python_v2'")
        elif self.kind == "python_v2":
            if self.python_version is None:
                raise ValueError("python_v2 runtime requires python_version")
            if self.environment is None:
                raise ValueError("python_v2 runtime requires environment")
            if self.gpu is None:
                raise ValueError("python_v2 runtime requires gpu")
        else:
            object.__setattr__(self, "python_version", None)
            object.__setattr__(self, "environment", None)
            object.__setattr__(self, "env", None)
            object.__setattr__(self, "gpu", None)
        return self


class FunctionVersion(_RemoteValue):
    """An exact immutable Function version returned by Enterprise.

    The GPU execution requirement is part of this identity. CPU and memory sizing,
    priority, concurrency, and retry policy belong to the execution platform.
    """

    name: str
    version: str
    artifact: FunctionArtifact
    signature: FunctionSignature
    runtime: PythonRuntimeSpec
    runtime_digest: str
    environment_digest: str
    created_at: str

    def __call__(self, **inputs: Any) -> FunctionApplication:
        """Bind this exact version to named table columns.

        Every input must be a direct [lancedb.col][lancedb.expr.col]
        reference. The returned application is immutable and retains a
        named-struct output as one binding, so every row's sibling values
        come from one logical Function evaluation. Map result fields to table
        columns with
        [FunctionApplication.rename][lancedb.functions.FunctionApplication.rename],
        then pass the application to
        [Table.add_columns][lancedb.table.Table.add_columns].

        Examples
        --------
        >>> from lancedb import col
        >>> application = function(  # doctest: +SKIP
        ...     title=col("title"),
        ...     body=col("body"),
        ... ).rename(columns={
        ...     "normalized_text": "search_text",
        ...     "token_count": "search_token_count",
        ... })
        >>> table.add_columns(application)  # doctest: +SKIP
        """
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
        )


class FunctionRegistrationRequest(_RemoteValue):
    """Stable remote registration envelope produced by :func:`udf`."""

    name: str
    artifact: FunctionArtifactRequest
    signature: FunctionSignature
    runtime: PythonRuntimeSpec


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
    """Immutable pre-declaration application of an exact Function version.

    A named-struct output remains one application through table
    declaration and execution.
    [FunctionApplication.rename][lancedb.functions.FunctionApplication.rename]
    records the result-field to table-column mapping without splitting sibling
    outputs into separate UDF calls.
    """

    function: FunctionVersionRef
    inputs: tuple[ApplicationInput, ...]
    output: FunctionOutput
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
    """One stable result-field mapping."""

    result_field: str
    output_name: str
    output_field_id: _Int32
    output_ordinal: _UInt32
    arrow_type: str
    nullable: bool


class AssignmentMapping(_RemoteValue):
    """Internal physical column preserving flattened struct validity."""

    output_name: str
    output_field_id: _Int32


class FunctionBinding(_RemoteValue):
    """Immutable Function binding persisted by the Enterprise table service."""

    binding_id: str
    function: FunctionVersionRef
    inputs: tuple[InputBinding, ...]
    outputs: tuple[OutputMapping, ...]
    assignment: Optional[AssignmentMapping] = None
    input_schema: Optional[Mapping[str, Any]] = None
    output_schema: Optional[Mapping[str, Any]] = None


class RefreshColumnResult(_RemoteValue):
    """Terminal result of an expression-backed or Function-backed refresh Job.

    Local jobs produce this value in process. LanceDB Cloud and Enterprise
    decode the same value from the durable server-job terminal payload.
    """

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
_FUNCTION_BLOB_V2_TYPE = "blob_v2"
_ARROW_EXTENSION_NAME_KEY = "ARROW:extension:name"
_BLOB_V2_EXTENSION_NAME = "lance.blob.v2"
_NESTED_BLOB_COLLECTION_ERROR = (
    "unsupported Arrow type for Function signature: Blob v2 fields nested under "
    "collection types are not supported"
)


_GRAMMAR_PRIMITIVES = (
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
    (pa.large_string(), "large_utf8"),
    (pa.binary(), "binary"),
    (pa.date32(), "date32"),
    (pa.date64(), "date64"),
)


def _canonical_arrow_type(data_type: pa.DataType) -> str:
    """The compact Function grammar, or canonical exact JSON for nested types."""
    grammar = _grammar_arrow_type(data_type)
    if grammar is not None:
        return grammar
    exact = _exact_arrow_type(data_type)
    return json.dumps(exact, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _grammar_arrow_type(data_type: pa.DataType) -> Optional[str]:
    for candidate, name in _GRAMMAR_PRIMITIVES:
        if data_type == candidate:
            return name
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        item = _grammar_list_item(data_type)
        if item is None:
            return None
        prefix = "list" if pa.types.is_list(data_type) else "large_list"
        return f"{prefix}<{item}>"
    if pa.types.is_fixed_size_list(data_type) and data_type.list_size > 0:
        item = _grammar_list_item(data_type)
        if item is not None:
            return f"fixed_size_list<{item}, {data_type.list_size}>"
    return None


def _grammar_list_item(data_type: pa.DataType) -> Optional[str]:
    """The grammar names only the item type; it always means a non-nullable
    child called `item`, so other child properties require exact JSON."""
    child = data_type.value_field
    if child.name != "item" or child.nullable or child.metadata:
        return None
    return _grammar_arrow_type(child.type)


def _validate_exact_arrow_field(field: pa.Field) -> None:
    if not field.name:
        raise TypeError(
            "unsupported Arrow type for Function signature: field names "
            "must not be empty"
        )
    if _is_blob_v2_field(field):
        if not _has_supported_blob_v2_layout(field):
            raise TypeError(
                "unsupported Arrow type for Function signature: lance.blob.v2 "
                f"requires a supported Blob storage layout, got {field}"
            )
        metadata = {
            (key.decode() if isinstance(key, bytes) else key): (
                value.decode() if isinstance(value, bytes) else value
            )
            for key, value in (field.metadata or {}).items()
        }
        if metadata and metadata != {
            _ARROW_EXTENSION_NAME_KEY: _BLOB_V2_EXTENSION_NAME
        }:
            raise TypeError(
                "unsupported Arrow type for Function signature: lance.blob.v2 "
                "field metadata must contain only its canonical extension marker"
            )
    elif field.metadata:
        raise TypeError(
            "unsupported Arrow type for Function signature: field metadata "
            f"is not supported, got {field}"
        )


def _has_supported_blob_v2_layout(field: pa.Field) -> bool:
    data_type = field.type
    if isinstance(data_type, pa.ExtensionType):
        data_type = data_type.storage_type
    if not pa.types.is_struct(data_type):
        return False

    fields = tuple(data_type)

    def matches(spec, compare_nullable) -> bool:
        return len(fields) == len(spec) and all(
            actual.name == name
            and actual.type == expected_type
            and (not check_nullable or actual.nullable == nullable)
            for actual, (name, expected_type, nullable), check_nullable in zip(
                fields, spec, compare_nullable
            )
        )

    logical_minimal = (
        ("data", pa.large_binary(), True),
        ("uri", pa.utf8(), True),
    )
    logical_full = logical_minimal + (
        ("position", pa.uint64(), True),
        ("size", pa.uint64(), True),
    )
    prepared = (
        ("kind", pa.uint8(), True),
        ("data", pa.large_binary(), True),
        ("uri", pa.utf8(), True),
        ("blob_id", pa.uint32(), True),
        ("blob_size", pa.uint64(), True),
        ("position", pa.uint64(), True),
    )
    descriptor = (
        ("kind", pa.uint8(), False),
        ("position", pa.uint64(), False),
        ("size", pa.uint64(), False),
        ("blob_id", pa.uint32(), False),
        ("blob_uri", pa.utf8(), False),
    )
    return (
        matches(logical_minimal, (True, True))
        or matches(logical_full, (True, True, False, False))
        or matches(prepared, (True,) * len(prepared))
        or matches(descriptor, (False,) * len(descriptor))
    )


def _canonical_arrow_field(field: pa.Field) -> str:
    _validate_exact_arrow_field(field)
    if _is_blob_v2_field(field):
        return _FUNCTION_BLOB_V2_TYPE
    return _canonical_arrow_type(field.type)


def _blob_storage_type(field: pa.Field) -> pa.DataType:
    data_type = field.type
    if isinstance(data_type, pa.ExtensionType):
        return data_type.storage_type
    return data_type


def _exact_blob_storage_type(field: pa.Field) -> dict[str, Any]:
    storage = _blob_storage_type(field)
    if not pa.types.is_struct(storage):
        raise TypeError(
            "unsupported Arrow type for Function signature: lance.blob.v2 "
            "requires struct storage"
        )
    return {
        "type": "struct",
        "fields": [
            {
                "name": child.name,
                "nullable": child.nullable,
                "type": (
                    {"type": "large_binary"}
                    if pa.types.is_large_binary(child.type)
                    else _exact_arrow_type(child.type)
                ),
            }
            for child in storage
        ],
    }


def _data_type_has_blob_v2(data_type: pa.DataType) -> bool:
    if pa.types.is_struct(data_type):
        return any(
            _is_blob_v2_field(field) or _data_type_has_blob_v2(field.type)
            for field in data_type
        )
    if (
        pa.types.is_list(data_type)
        or pa.types.is_large_list(data_type)
        or pa.types.is_fixed_size_list(data_type)
    ):
        field = data_type.value_field
        return _is_blob_v2_field(field) or _data_type_has_blob_v2(field.type)
    if pa.types.is_map(data_type):
        return any(
            _is_blob_v2_field(field) or _data_type_has_blob_v2(field.type)
            for field in (data_type.key_field, data_type.item_field)
        )
    return False


def _exact_arrow_field(
    field: pa.Field, *, inside_collection: bool = False
) -> dict[str, Any]:
    _validate_exact_arrow_field(field)
    if _is_blob_v2_field(field):
        if inside_collection:
            raise TypeError(_NESTED_BLOB_COLLECTION_ERROR)
        return {
            "name": field.name,
            "nullable": field.nullable,
            "type": _exact_blob_storage_type(field),
            "metadata": {
                _ARROW_EXTENSION_NAME_KEY: _BLOB_V2_EXTENSION_NAME,
            },
        }
    value = {
        "name": field.name,
        "nullable": field.nullable,
        "type": _exact_arrow_type(field.type, inside_collection=inside_collection),
    }
    return value


def _exact_arrow_type(
    data_type: pa.DataType, *, inside_collection: bool = False
) -> dict[str, Any]:
    for candidate, name in _GRAMMAR_PRIMITIVES:
        if data_type == candidate:
            return {"type": name}
    if pa.types.is_struct(data_type):
        fields = list(data_type)
        names = [field.name for field in fields]
        if not fields or len(set(names)) != len(names):
            raise TypeError(
                "unsupported Arrow type for Function signature: structs must have "
                "non-empty, uniquely named fields"
            )
        return {
            "type": "struct",
            "fields": [
                _exact_arrow_field(field, inside_collection=inside_collection)
                for field in fields
            ],
        }
    if (
        pa.types.is_list(data_type)
        or pa.types.is_large_list(data_type)
        or pa.types.is_fixed_size_list(data_type)
    ):
        if pa.types.is_fixed_size_list(data_type):
            if data_type.value_field.name != "item":
                raise TypeError(
                    "unsupported Arrow type for Function signature: fixed-size list "
                    "items must be named 'item'"
                )
            if data_type.list_size <= 0:
                raise TypeError(
                    f"unsupported Arrow type for Function signature: {data_type}"
                )
        value: dict[str, Any] = {
            "type": (
                "list"
                if pa.types.is_list(data_type)
                else "large_list"
                if pa.types.is_large_list(data_type)
                else "fixed_size_list"
            ),
            "fields": [
                _exact_arrow_field(data_type.value_field, inside_collection=True)
            ],
        }
        if pa.types.is_fixed_size_list(data_type):
            value["length"] = data_type.list_size
        return value
    if pa.types.is_map(data_type) and _data_type_has_blob_v2(data_type):
        raise TypeError(_NESTED_BLOB_COLLECTION_ERROR)
    raise TypeError(f"unsupported Arrow type for Function signature: {data_type}")


def _list_of(item: pa.DataType) -> pa.DataType:
    return pa.list_(pa.field("item", item, nullable=False))


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
        return _list_of(value_type), nullable
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
        if output.metadata:
            raise TypeError("Function output schema metadata is not supported")
        fields = tuple(output)
    elif (
        isinstance(output, pa.Field)
        and not _is_blob_v2_field(output)
        and pa.types.is_struct(output.type)
    ):
        _validate_exact_arrow_field(output)
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
        _validate_exact_arrow_field(field)
        if field.nullable:
            raise ValueError("Function output must be non-nullable")
        return FunctionOutput(
            kind="scalar",
            arrow_type=_canonical_arrow_field(field),
            nullable=False,
        )

    if not fields:
        raise ValueError("named-struct Function output must contain at least one field")
    for field in fields:
        _validate_exact_arrow_field(field)
    names = [field.name for field in fields]
    if len(set(names)) != len(names):
        raise ValueError("Function output field names must be unique")
    return FunctionOutput(
        kind="named_struct",
        fields=tuple(
            FunctionResultField(
                name=field.name,
                arrow_type=_canonical_arrow_field(field),
                nullable=field.nullable,
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
        if input_schema.metadata:
            raise TypeError("Function input schema metadata is not supported")
        for field in input_schema:
            _validate_exact_arrow_field(field)
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
                arrow_type=_canonical_arrow_field(field),
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
                arrow_type=_canonical_arrow_field(
                    pa.field(parameter.name, data_type, nullable=nullable)
                ),
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


_DYNAMIC_NAMESPACE_ACCESS = frozenset(
    {"globals", "locals", "vars", "eval", "exec", "compile", "__import__"}
)
# Modules that hand out namespaces (`sys.modules`, `builtins`, importers,
# introspection). The artifact's module namespace holds only the names it was
# packaged with, so reaching around it cannot be represented.
_NAMESPACE_MODULES = frozenset(
    {"sys", "builtins", "importlib", "inspect", "gc", "ctypes", "types"}
)


def _namespace_acquisition(
    definition: ast.FunctionDef, references: set[str]
) -> list[str]:
    found = set(references & _DYNAMIC_NAMESPACE_ACCESS)
    for node in ast.walk(definition):
        if isinstance(node, ast.Import):
            found.update(
                alias.name
                for alias in node.names
                if alias.name.split(".")[0] in _NAMESPACE_MODULES
            )
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module.split(".")[0] in _NAMESPACE_MODULES:
                found.add(node.module)
    return sorted(found)


def _module_references(module_source: str) -> set[str]:
    """Names any scope in `module_source` binds or loads at module scope.
    Python's own scope analysis on the exact text that ships: free variables
    belong to an enclosing scope inside the function, and postponed
    annotations are not runtime loads."""

    def visit(table: symtable.SymbolTable, found: set[str]) -> None:
        for symbol in table.get_symbols():
            if symbol.is_global() and (
                symbol.is_referenced() or symbol.is_declared_global()
            ):
                found.add(symbol.get_name())
        for child in table.get_children():
            visit(child, found)

    found: set[str] = set()
    for table in symtable.symtable(module_source, "<udf>", "exec").get_children():
        visit(table, found)
    return found


def _global_source(name: str, value: Any) -> str:
    """One module-level line that rebinds `name` to `value` in the artifact:
    an import for modules and importable classes/functions, a literal otherwise."""
    if isinstance(value, types.ModuleType):
        if value.__name__.split(".")[0] in _NAMESPACE_MODULES:
            raise ValueError(
                f"@udf cannot package dynamic namespace access: {value.__name__!r}"
            )
        try:
            imported = importlib.import_module(value.__name__)
        except ImportError:
            imported = None
        if imported is not value:
            raise TypeError(
                f"Function source references module {name!r} that does not import "
                f"as {value.__name__!r}"
            )
        return f"import {value.__name__} as {name}"
    module_name = getattr(value, "__module__", None)
    qualname = getattr(value, "__qualname__", None)
    if (
        isinstance(module_name, str)
        and isinstance(qualname, str)
        and module_name != "__main__"
        and "." not in qualname
        and "<" not in qualname
    ):
        try:
            imported = getattr(importlib.import_module(module_name), qualname)
        except (ImportError, AttributeError):
            imported = None
        if imported is value:
            return f"from {module_name} import {qualname} as {name}"
    return f"{name} = {_literal_source(value)}"


def _is_recursive_reference(function: Callable[..., Any], name: str) -> bool:
    """`name` inside the body means the function itself unless the module has
    since bound it to something else."""
    if name != function.__name__:
        return False
    bound = function.__globals__.get(name, function)
    if bound is function:
        return True
    # The decorator's own result is the one wrapper known to call `function`
    # unchanged; any other binding may behave differently from a self-call.
    return type(bound) is UdfDefinition and bound._function is function


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
    function_source = ast.unparse(definition)
    module_header = "from __future__ import annotations"
    references = _module_references(f"{module_header}\n\n{function_source}\n")
    dynamic = _namespace_acquisition(definition, references)
    if dynamic:
        raise ValueError(f"@udf cannot package dynamic namespace access: {dynamic!r}")
    # Resolve every module-scope reference the way the interpreter would: the
    # function's own globals first (a module global may shadow a builtin, and
    # nested scopes are not visible to getclosurevars), then its builtins.
    # The artifact runs under the standard builtins; only the exact mapping is
    # provably equivalent (a subclass or copy can change lookups and hooks).
    if function.__builtins__ is not vars(builtins):
        raise ValueError("@udf cannot package a non-standard builtins environment")
    globals_source = []
    unresolved = []
    for name in sorted(references):
        if name == function.__name__:
            if not _is_recursive_reference(function, name):
                raise ValueError(
                    f"@udf cannot package {name!r}: the module binds that name to "
                    "another value, which the artifact's own definition would shadow"
                )
            continue
        if name in function.__globals__:
            globals_source.append(_global_source(name, function.__globals__[name]))
        elif hasattr(builtins, name):
            pass
        else:
            unresolved.append(name)
    if unresolved:
        raise ValueError(
            f"@udf source contains unresolved global names: {unresolved!r}"
        )

    parts = [module_header]
    if globals_source:
        parts.extend(["", *globals_source])
    parts.extend(["", function_source, ""])
    packaged = "\n".join(parts)
    return packaged.encode("utf-8")


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
        python_version: Optional[str],
        gpu: bool = False,
        conda: tuple[str, ...] = (),
        conda_channels: tuple[str, ...] = (),
    ):
        function_name = name or function.__name__
        if not _FUNCTION_NAME.fullmatch(function_name):
            raise ValueError(f"invalid Function name: {function_name!r}")
        if pip and conda:
            raise ValueError("a Function environment is pip or conda, not both")
        if conda_channels and not conda:
            raise ValueError("conda_channels requires conda packages")
        packages = tuple(sorted(set(conda if conda else pip)))
        if any(not package or package != package.strip() for package in packages):
            raise ValueError("package requirements must be non-empty and trimmed")
        if conda:
            environment_spec = PythonEnvironmentSpec(
                kind="conda", packages=packages, channels=tuple(conda_channels)
            )
        else:
            environment_spec = PythonEnvironmentSpec(kind="pip", packages=packages)
        environment = dict(env)
        if any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in environment.items()
        ):
            raise TypeError("Function env keys and values must be strings")
        signature = _infer_signature(function, input_schema, output_schema)
        source = _package_source(function)
        digest = f"sha256:{hashlib.sha256(source).hexdigest()}"
        gpu_marker = _normalize_gpu_marker(gpu)
        runtime = PythonRuntimeSpec(
            kind="python_v2" if gpu_marker is not None else "python",
            python_version=python_version
            or f"{sys.version_info.major}.{sys.version_info.minor}",
            environment=environment_spec,
            env=environment,
            gpu=gpu_marker,
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
    python_version: Optional[str] = None,
    gpu: bool = False,
    conda: tuple[str, ...] | list[str] = (),
    conda_channels: tuple[str, ...] | list[str] = (),
) -> Callable[[Callable[..., Any]], UdfDefinition]: ...


def udf(
    function: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    input_schema: Optional[pa.Schema] = None,
    output_schema: Optional[pa.DataType | pa.Field | pa.Schema] = None,
    pip: tuple[str, ...] | list[str] = (),
    env: Optional[Mapping[str, str]] = None,
    python_version: Optional[str] = None,
    gpu: bool = False,
    conda: tuple[str, ...] | list[str] = (),
    conda_channels: tuple[str, ...] | list[str] = (),
):
    """Prepare a scalar Python callable for remote Function registration.

    Input and output signatures are inferred from supported annotations. For
    Arrow types annotations cannot express precisely, pass ``input_schema``
    and ``output_schema`` together. Scalar outputs must be non-nullable. Every
    named-struct field may be nullable; Enterprise preserves the struct's
    validity when the result is expanded into sibling columns.

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
        Explicit scalar or named-struct output. Scalar outputs must be
        non-nullable. Must be provided together with ``input_schema``.
    pip : sequence of str, optional
        Pip requirements for the remote environment.
    conda : sequence of str, optional
        Conda packages for the remote environment, instead of ``pip``.
    conda_channels : sequence of str, optional
        Conda channels in priority order; requires ``conda``.
    env : mapping of str to str, optional
        Environment variables included in the Function definition.
    python_version : str, optional
        Remote Python major/minor version. Defaults to the client version.
    gpu : bool, default False
        Whether every remote execution requires a GPU. The execution platform
        selects one compatible GPU for each worker. The requirement is part of
        the immutable Function version.

    Notes
    -----
    The packaged artifact is a snapshot: the function source plus exactly
    the module-level names it references (modules as imports, importable
    classes and functions as imports, literals inline). Code that reaches the
    module namespace another way -- ``globals()``/``eval``, ``sys.modules``,
    ``builtins`` -- is rejected where it can be seen and otherwise
    unsupported; closures and a non-standard ``__builtins__`` are rejected.

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
    >>> @udf(pip=["numpy==2.2.0"])
    ... def score(value: float) -> float:
    ...     return value * 2
    >>> score(1.5)
    3.0
    >>> @udf(pip=["cupy-cuda12x"], gpu=True)
    ... def gpu_score(value: int) -> int:
    ...     return value * 2
    >>> gpu_score.registration_request.runtime.gpu
    True
    """

    def decorate(target: Callable[..., Any]) -> UdfDefinition:
        return UdfDefinition(
            target,
            name=name,
            input_schema=input_schema,
            output_schema=output_schema,
            pip=tuple(pip),
            env={} if env is None else env,
            python_version=python_version,
            gpu=gpu,
            conda=tuple(conda),
            conda_channels=tuple(conda_channels),
        )

    if function is None:
        return decorate
    return decorate(function)


__all__ = [
    "AssignmentMapping",
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
