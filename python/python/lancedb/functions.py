# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Canonical Function values exchanged with LanceDB Enterprise services.

These immutable models contain client/wire state only. Catalog persistence,
environment bake, secret resolution, and execution are owned by Sophon.
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
import linecache
import math
import re
import sys
import textwrap
import types
from collections.abc import Mapping, Sequence
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
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    conint,
    field_validator,
    model_validator,
)

from .schema import is_blob_v2_field as _is_blob_v2_field
from .secrets import EnvVarSecret

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


class SecretReference(_RemoteValue):
    """Where a Secret lives, carried as its parts rather than as one string.

    A joined id would need a delimiter, and a delimiter has to be excluded from
    every name and segment forever, agreed on by both sides, and re-agreed each
    time either grows a new way to be configured. Naming the parts settles all
    of that: nothing here is parsed, so nothing can parse two ways.
    """

    name: str
    namespace_path: tuple[str, ...] = ()


class SecretBinding(_RemoteValue):
    """How a Secret reaches the Function that binds it.

    One list rather than a field per delivery mode: a binding is the concept,
    and how it arrives is a property of one. ``kind`` is open, so a binding a
    newer service introduces decodes here instead of failing the whole
    FunctionVersion.
    """

    kind: str
    variable: Optional[str] = None
    secret_ref: Optional[SecretReference] = None


class FunctionSignature(_RemoteValue):
    """Ordered inputs, output, and initialization fields of a Function.

    ``initialization`` lists the fields of the single row each Function
    instance is created with. Values come from each binding, not from the
    Function version; the tuple is empty when the Function takes none.
    """

    inputs: tuple[FunctionParameter, ...]
    output: FunctionOutput
    initialization: tuple[FunctionParameter, ...] = ()


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


class FunctionImage(_RemoteValue):
    """A complete OCI Function image identified by its exact manifest digest."""

    manifest_digest: str
    descriptor: Mapping[str, Any]
    source: bool


def _validate_object_version(value: str) -> str:
    if int(value) > 2**64 - 1:
        raise ValueError("Function version exceeds uint64")
    return value


_ObjectVersion = Annotated[
    str,
    Field(strict=True, pattern=r"^[1-9][0-9]*$"),
    AfterValidator(_validate_object_version),
]


class FunctionVersion(_RemoteValue):
    """A pinned object revision, independent of its executable image digest."""

    name: str
    object_id: str
    location: str
    version: _ObjectVersion
    image: FunctionImage
    signature: FunctionSignature
    secret_bindings: tuple[SecretBinding, ...] = ()
    created_at: str
    metadata: Mapping[str, str]
    disabled: bool

    def __call__(self, **arguments: Any) -> FunctionApplication:
        """Bind this exact version to named table columns.

        Every input must be a direct [lancedb.col][lancedb.expr.col]
        reference. A Function that declares initialization fields takes their
        values as further keyword arguments; they are constants of this
        binding, fixed for every row, and every instance the binding runs is
        created with them. A non-nullable field without a default must be
        given; an omitted nullable field is null, and the Function's own
        default applies to a null value.

        The returned application is immutable and retains a named-struct
        output as one binding, so every row's sibling values come from one
        logical Function evaluation. Map result fields to table columns with
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
        >>> table.add_columns({  # doctest: +SKIP
        ...     "embedding": embed(text=col("body"), model="small", dimensions=512)
        ... })
        """
        from lancedb.expr import Expr

        parameters = tuple(parameter.name for parameter in self.signature.inputs)
        fields = {field.name: field for field in self.signature.initialization}
        missing = [parameter for parameter in parameters if parameter not in arguments]
        missing_initialization = [
            name
            for name, field in fields.items()
            if not field.nullable and name not in arguments
        ]
        unknown = sorted(set(arguments) - set(parameters) - set(fields))
        if missing or missing_initialization or unknown:
            details = []
            if missing:
                details.append(f"missing inputs: {missing!r}")
            if missing_initialization:
                details.append(f"missing initialization: {missing_initialization!r}")
            if unknown:
                details.append(f"unknown arguments: {unknown!r}")
            raise TypeError("invalid Function arguments (" + "; ".join(details) + ")")

        bindings = []
        for parameter in parameters:
            value = arguments[parameter]
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
        initialization = {}
        for name in fields:
            if name in arguments:
                initialization[name] = _initialization_value(name, arguments[name])
        return FunctionApplication(
            function=FunctionVersionRef(
                name=self.name,
                object_id=self.object_id,
                location=self.location,
                version=self.version,
                manifest_digest=self.image.manifest_digest,
            ),
            inputs=tuple(bindings),
            output=self.signature.output,
            initialization=initialization,
        )


def _initialization_value(name: str, value: Any) -> Any:
    """One initialization value as plain JSON; the service checks its type."""
    from lancedb.expr import Expr

    def plain(value):
        if isinstance(value, Expr):
            raise TypeError(
                f"initialization field {name!r} takes a constant, not a column "
                "expression; every row of a binding shares one initialization"
            )
        if value is None or type(value) in (bool, int, str):
            return value
        if type(value) is float:
            if not math.isfinite(value):
                raise ValueError(
                    f"initialization field {name!r} must be finite, got {value!r}"
                )
            return value
        if isinstance(value, (list, tuple)):
            return [plain(child) for child in value]
        if isinstance(value, Mapping) and all(isinstance(key, str) for key in value):
            return {key: plain(child) for key, child in value.items()}
        raise TypeError(
            f"initialization field {name!r} takes booleans, numbers, strings, "
            f"lists, and string-keyed mappings, got {type(value).__name__}"
        )

    return plain(value)


class FunctionRegistrationRequest(_RemoteValue):
    """Stable remote registration envelope produced by :func:`udf`.

    Credential values deliberately have no field here. The only secret-shaped
    thing a client sends is ``secret_bindings``: the name of a Secret the
    database already holds, which the remote service resolves at execution.
    """

    name: str
    artifact: FunctionArtifactRequest
    signature: FunctionSignature
    runtime: PythonRuntimeSpec
    secret_bindings: tuple[SecretBinding, ...] = ()


class FunctionVersionRef(_OpenRemoteValue):
    name: str
    object_id: str
    location: str
    version: _ObjectVersion
    manifest_digest: str


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
    # Transport JSON: the service validates it against the Function's
    # initialization schema and persists the Arrow row in the binding, so it is
    # never hashed and floating-point values are allowed.
    initialization: Mapping[str, Any] = Field(default_factory=dict)

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
    initialization: Optional[str] = None
    """Standard base64 of the validated one-row Arrow IPC initialization stream."""

    def initialization_row(self) -> Optional[dict[str, Any]]:
        """The initialization values this binding creates instances with."""
        if self.initialization is None:
            return None
        reader = pa.ipc.open_stream(base64.b64decode(self.initialization))
        return reader.read_all().to_pylist()[0]


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
        # A struct value is written as a dict, which has no Arrow type of its
        # own; the Annotated metadata supplies it.
        if base is dict or get_origin(base) is dict:
            base_nullable = False
        else:
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
    *,
    method: bool = False,
) -> FunctionSignature:
    parameters = _callable_parameters(function)
    if method:
        parameters = parameters[1:]
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
    """Names `module_source` loads from module scope, by Python's own scope
    analysis on the exact text that ships: loads inside any definition, plus
    what the definition statement itself evaluates at module scope (bases,
    decorators, defaults). Free variables belong to an enclosing scope inside
    the definition, and postponed annotations are not runtime loads."""

    def visit(table: symtable.SymbolTable, found: set[str]) -> None:
        for symbol in table.get_symbols():
            if symbol.is_global() and (
                symbol.is_referenced() or symbol.is_declared_global()
            ):
                found.add(symbol.get_name())
        for child in table.get_children():
            visit(child, found)

    found: set[str] = set()
    module = symtable.symtable(module_source, "<udf>", "exec")
    for symbol in module.get_symbols():
        if (
            symbol.is_referenced()
            and not symbol.is_assigned()
            and not symbol.is_imported()
        ):
            found.add(symbol.get_name())
    for table in module.get_children():
        visit(table, found)
    return found


_SOURCE_HEADER = "from __future__ import annotations"
_SOURCE_ENTRY_FILE = "user_function.py"
_PYTHON_CALLABLE_ARTIFACT = "python_callable"
_PYTHON_BUNDLE_ARTIFACT = "python_bundle"
# Modules the Function image places beside user code.
_RESERVED_SOURCE_MODULES = frozenset(
    {"user_function", "lance_source", "lance_callable"}
)
# The service accepts bundle paths made of these components only.
_SOURCE_PATH_COMPONENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_REQUIREMENT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def _class_source(cls: type) -> str:
    """Source of a module-level class.

    `inspect.getsource` finds a class through its module's file, which a
    notebook cell or a doctest does not have. A method's code object records
    where the class body really is, so that is consulted first.
    """
    for member in vars(cls).values():
        if isinstance(member, (staticmethod, classmethod)):
            member = member.__func__
        code = (
            getattr(inspect.unwrap(member), "__code__", None)
            if callable(member)
            else None
        )
        if code is None:
            continue
        lines = linecache.getlines(code.co_filename)
        try:
            module = ast.parse("".join(lines))
        except SyntaxError:
            continue
        for node in module.body:
            if (
                isinstance(node, ast.ClassDef)
                and node.name == cls.__name__
                and node.lineno <= code.co_firstlineno <= node.end_lineno
            ):
                start = min(
                    [node.lineno]
                    + [decorator.lineno for decorator in node.decorator_list]
                )
                return "".join(lines[start - 1 : node.end_lineno])
    return inspect.getsource(cls)


def _definition_node(
    target: Any, *, entry: bool
) -> Union[ast.FunctionDef, ast.ClassDef]:
    try:
        source = textwrap.dedent(
            _class_source(target)
            if inspect.isclass(target)
            else inspect.getsource(target)
        )
    except (OSError, TypeError) as error:
        raise ValueError(
            f"@udf requires inspectable Python source for {target.__qualname__!r}"
        ) from error
    module = ast.parse(source)
    is_class = inspect.isclass(target)
    definitions = [
        node
        for node in module.body
        if isinstance(
            node,
            (ast.ClassDef,) if is_class else (ast.FunctionDef, ast.AsyncFunctionDef),
        )
        and node.name == target.__name__
    ]
    if len(definitions) != 1 or isinstance(definitions[0], ast.AsyncFunctionDef):
        raise ValueError(
            "@udf source must contain exactly one "
            + ("class" if is_class else "synchronous function")
        )
    definition = definitions[0]
    if entry:
        if any(
            not _is_udf_decorator(decorator) for decorator in definition.decorator_list
        ):
            raise ValueError("@udf cannot package additional Python decorators")
        definition.decorator_list = []
    return definition


def _definition_namespace(target: Any) -> Mapping[str, Any]:
    """The module namespace a function or class resolves its globals in."""
    if inspect.isclass(target):
        module = sys.modules.get(target.__module__)
        if module is None:
            raise ValueError(
                f"@udf cannot resolve the module of {target.__qualname__!r}"
            )
        namespace = vars(module)
        environment = namespace.get("__builtins__", builtins)
        environment = (
            vars(environment)
            if isinstance(environment, types.ModuleType)
            else environment
        )
    else:
        namespace = target.__globals__
        environment = target.__builtins__
    # The artifact runs under the standard builtins; only the exact mapping is
    # provably equivalent (a subclass or copy can change lookups and hooks).
    if environment is not vars(builtins):
        raise ValueError("@udf cannot package a non-standard builtins environment")
    return namespace


def _is_self_reference(target: Any, namespace: Mapping[str, Any], name: str) -> bool:
    """`name` inside the definition means the definition itself unless the
    module has since bound it to something else."""
    if name != target.__name__:
        return False
    bound = namespace.get(name, target)
    if bound is target:
        return True
    # The decorator's own result is the one wrapper known to behave like
    # `target`; any other binding may differ from the packaged definition.
    return type(bound) is UdfDefinition and bound._function is target


def _packaged_by_source(value: Any) -> bool:
    """Functions and classes defined in `__main__` -- notebook cells and
    scripts -- have no module a worker can import, so their source travels."""
    if not (inspect.isfunction(value) or inspect.isclass(value)):
        return False
    if getattr(value, "__module__", None) != "__main__":
        return False
    qualname = value.__qualname__
    if "<" in qualname or "." in qualname:
        raise ValueError(
            f"@udf cannot package {qualname!r}: only module-level functions and "
            "classes travel with a Function, not lambdas, closures, or nested "
            "definitions"
        )
    return True


def _requirement_name(requirement: str) -> str:
    requirement = requirement.rpartition("::")[2]
    match = _REQUIREMENT_NAME.match(requirement.strip())
    return _normalized_distribution(match.group(0) if match else requirement)


def _normalized_distribution(name: str) -> str:
    return re.sub(r"[-_.]+", "_", name).lower()


def _installed_roots() -> tuple[str, ...]:
    import os
    import site
    import sysconfig

    roots = set(site.getsitepackages())
    roots.add(site.getusersitepackages())
    paths = sysconfig.get_paths()
    for key in ("purelib", "platlib", "stdlib", "platstdlib"):
        if key in paths:
            roots.add(paths[key])
    return tuple(os.path.realpath(root) + os.sep for root in roots)


def _module_locations(module: types.ModuleType) -> tuple[str, ...]:
    import os

    file = getattr(module, "__file__", None)
    if file:
        return (os.path.realpath(file),)
    return tuple(os.path.realpath(path) for path in getattr(module, "__path__", ()))


class _SourcePackager:
    """Builds the entry module of a Function artifact.

    The module holds the definition plus exactly the module-level names it
    references: modules as imports, importable classes and functions as
    imports, literals inline, and functions and classes defined in
    ``__main__`` by source, recursively, dependencies first.
    """

    def __init__(self, code_modules: frozenset[str], requirements: frozenset[str]):
        self._code_modules = code_modules
        self._requirements = requirements
        self._imports: dict[str, str] = {}
        self._definitions: list[str] = []
        self._packaged: set[int] = set()
        self._names: dict[str, Any] = {}

    def package(self, target: Any) -> str:
        entry = self._definition(target, entry=True)
        parts = [_SOURCE_HEADER]
        if self._imports:
            parts.extend(["", *(self._imports[name] for name in sorted(self._imports))])
        for definition in self._definitions:
            parts.extend(["", definition])
        parts.extend(["", entry, ""])
        return "\n".join(parts)

    def _definition(self, target: Any, *, entry: bool) -> str:
        definition = _definition_node(target, entry=entry)
        if not inspect.isclass(target) and inspect.getclosurevars(target).nonlocals:
            raise ValueError(
                "@udf cannot package functions that capture closure values"
            )
        source = ast.unparse(definition)
        references = _module_references(f"{_SOURCE_HEADER}\n\n{source}\n")
        dynamic = _namespace_acquisition(definition, references)
        if dynamic:
            raise ValueError(
                f"@udf cannot package dynamic namespace access: {dynamic!r}"
            )
        namespace = _definition_namespace(target)
        self._packaged.add(id(target))
        self._claim(target.__name__, ("definition", id(target)))
        unresolved = []
        for name in sorted(references):
            if name == target.__name__:
                if not _is_self_reference(target, namespace, name):
                    raise ValueError(
                        f"@udf cannot package {name!r}: the module binds that name "
                        "to another value, which the artifact's own definition "
                        "would shadow"
                    )
                continue
            if name in namespace:
                self._bind(name, namespace[name])
            elif not hasattr(builtins, name):
                unresolved.append(name)
        if unresolved:
            raise ValueError(
                f"@udf source contains unresolved global names: {unresolved!r}"
            )
        return source

    def _claim(self, name: str, meaning: Any) -> None:
        claimed = self._names.setdefault(name, meaning)
        if claimed != meaning:
            raise ValueError(
                f"@udf cannot package {name!r}: packaged definitions bind that "
                "name to different values"
            )

    def _bind(self, name: str, value: Any) -> None:
        if _packaged_by_source(value):
            if value.__name__ != name:
                raise ValueError(
                    f"@udf packages {value.__qualname__!r} by source, so it must be "
                    f"referenced by its own name, not {name!r}"
                )
            if id(value) not in self._packaged:
                self._definitions.append(self._definition(value, entry=False))
            self._claim(name, ("definition", id(value)))
            return
        line = self._global_source(name, value)
        self._claim(name, line)
        self._imports[name] = line

    def _global_source(self, name: str, value: Any) -> str:
        """One module-level line that rebinds `name` to `value` in the artifact:
        an import for modules and importable classes/functions, a literal
        otherwise."""
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
                    f"Function source references module {name!r} that does not "
                    f"import as {value.__name__!r}"
                )
            self._require_importable(value.__name__)
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
                self._require_importable(module_name)
                return f"from {module_name} import {qualname} as {name}"
        return f"{name} = {_literal_source(value)}"

    def _require_importable(self, module_name: str) -> None:
        """Refuse an import the Function's environment has no way to satisfy:
        a module that lives in a local source tree, is not shipped with
        ``code=``, and names no declared package."""
        top = module_name.partition(".")[0]
        if (
            top in self._code_modules
            or top in sys.stdlib_module_names
            or top in sys.builtin_module_names
            or _normalized_distribution(top) in self._requirements
        ):
            return
        module = sys.modules.get(top)
        locations = () if module is None else _module_locations(module)
        roots = _installed_roots()
        if not locations or any(location.startswith(roots) for location in locations):
            return
        raise ValueError(
            f"@udf source imports {module_name!r} from {locations[0]}, a local "
            "module the Function's environment cannot import; ship it with "
            f"code=[{top}] or declare the package that provides it in pip/conda"
        )


def _code_files(modules: Sequence[Any]) -> tuple[dict[str, str], frozenset[str]]:
    """The Python source files of the modules and packages in ``code=``,
    keyed by their path on the Function's import path."""
    import os
    from pathlib import Path

    files: dict[str, str] = {}
    names: set[str] = set()
    for module in modules:
        if not isinstance(module, types.ModuleType):
            raise TypeError(
                f"code= takes imported modules or packages, got {type(module).__name__}"
            )
        name = module.__name__
        if "." in name:
            raise ValueError(
                f"code= takes top-level modules or packages; pass "
                f"{name.partition('.')[0]!r} instead of {name!r}"
            )
        if (
            name in _RESERVED_SOURCE_MODULES
            or name == "__main__"
            or name in sys.stdlib_module_names
            or not _SOURCE_PATH_COMPONENT.fullmatch(name)
        ):
            raise ValueError(f"code= cannot ship module {name!r}")
        if name in names:
            raise ValueError(f"code= lists module {name!r} more than once")
        names.add(name)
        paths = list(getattr(module, "__path__", ()))
        if paths:
            if len(paths) != 1 or not getattr(module, "__file__", None):
                raise ValueError(
                    f"code= cannot ship namespace package {name!r}; give it an "
                    "__init__.py"
                )
            root = Path(paths[0])
            sources = []
            for directory, subdirectories, filenames in os.walk(root):
                # Only importable modules travel: data files, caches, and
                # directories that are not identifiers cannot be imported.
                subdirectories[:] = sorted(
                    child
                    for child in subdirectories
                    if _SOURCE_PATH_COMPONENT.fullmatch(child)
                    and child != "__pycache__"
                )
                for filename in filenames:
                    stem, suffix = os.path.splitext(filename)
                    if suffix == ".py" and _SOURCE_PATH_COMPONENT.fullmatch(stem):
                        sources.append(Path(directory) / filename)
            for path in sources:
                relative = path.relative_to(root).as_posix()
                files[f"{name}/{relative}"] = _source_text(path)
        else:
            file = getattr(module, "__file__", None)
            if not file or not file.endswith(".py"):
                raise ValueError(f"code= ships Python source; module {name!r} has none")
            files[f"{name}.py"] = _source_text(Path(file))
    return files, frozenset(names)


def _source_text(path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(f"code= ships UTF-8 source; {path} is not UTF-8") from error


def _initialization_type(data_type: pa.DataType) -> bool:
    """Types with one unambiguous JSON and SQL literal spelling."""
    if data_type in (
        pa.bool_(),
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.uint8(),
        pa.uint16(),
        pa.uint32(),
        pa.uint64(),
        pa.float32(),
        pa.float64(),
        pa.string(),
        pa.large_string(),
    ):
        return True
    if (
        pa.types.is_list(data_type)
        or pa.types.is_large_list(data_type)
        or pa.types.is_fixed_size_list(data_type)
    ):
        return _initialization_type(data_type.value_type)
    if pa.types.is_struct(data_type):
        return all(_initialization_type(field.type) for field in data_type)
    return False


def _initialization_signature(cls: type) -> tuple[FunctionParameter, ...]:
    """The initialization fields a class Function's ``__init__`` declares."""
    if cls.__init__ is object.__init__:
        return ()
    parameters = tuple(inspect.signature(cls.__init__).parameters.values())[1:]
    for parameter in parameters:
        if parameter.kind not in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            raise TypeError(
                "Function initialization parameters must be named and "
                f"non-variadic: {parameter.name!r}"
            )
    try:
        annotations = get_type_hints(cls.__init__, include_extras=True)
    except Exception as error:
        raise TypeError(
            f"failed to resolve Function initialization annotations: {error}"
        ) from error
    missing = [
        parameter.name for parameter in parameters if parameter.name not in annotations
    ]
    if missing:
        raise TypeError(f"missing Function initialization annotations: {missing!r}")
    fields = []
    for parameter in parameters:
        data_type, nullable = _annotation_type(annotations[parameter.name])
        if not _initialization_type(data_type):
            raise TypeError(
                f"Function initialization parameter {parameter.name!r} has type "
                f"{data_type}; initialization takes booleans, integers, floats, "
                "strings, and lists or structs of them"
            )
        # A parameter with a default may be omitted from a binding; a null
        # value then takes the default.
        if parameter.default is not inspect.Parameter.empty:
            nullable = True
        fields.append(
            FunctionParameter(
                name=parameter.name,
                arrow_type=_canonical_arrow_field(
                    pa.field(parameter.name, data_type, nullable=nullable)
                ),
                nullable=nullable,
            )
        )
    return tuple(fields)


def _class_member(cls: type, name: str) -> Any:
    for klass in cls.__mro__:
        if klass is not object and name in vars(klass):
            return vars(klass)[name]
    return None


def _class_call(cls: type) -> Callable[..., Any]:
    call = _class_member(cls, "__call__")
    if not inspect.isfunction(call) or inspect.iscoroutinefunction(call):
        raise TypeError("a class Function must define a synchronous __call__ method")
    close = _class_member(cls, "close")
    if close is not None and (
        not inspect.isfunction(close) or len(inspect.signature(close).parameters) != 1
    ):
        raise TypeError("a class Function's close must be a method taking no arguments")
    return call


class UdfDefinition:
    """A Python callable prepared for remote Function registration.

    Instances are created with :func:`udf`. Calling an instance executes the
    original Python function, or constructs the original class, which keeps
    local unit testing ordinary. Remote execution adapts the callable to the
    internal Arrow batch ABI described by the registration artifact.
    """

    def __init__(
        self,
        function: Any,
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
        code: tuple[types.ModuleType, ...] = (),
    ):
        is_class = inspect.isclass(function)
        if not is_class and (
            not inspect.isfunction(function) or inspect.iscoroutinefunction(function)
        ):
            raise TypeError("@udf requires a synchronous Python function or a class")
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
        if is_class:
            if "<" in function.__qualname__ or "." in function.__qualname__:
                raise ValueError(
                    "@udf classes must be defined at module level; nested classes "
                    "would capture their enclosing scope"
                )
            signature = _infer_signature(
                _class_call(function), input_schema, output_schema, method=True
            )
            initialization = _initialization_signature(function)
            shared = sorted(
                {field.name for field in initialization}
                & {parameter.name for parameter in signature.inputs}
            )
            if shared:
                raise ValueError(
                    "Function initialization parameters and inputs share names "
                    f"{shared!r}; a binding passes both as keyword arguments"
                )
            signature = signature._copy(update={"initialization": initialization})
        else:
            signature = _infer_signature(function, input_schema, output_schema)
        code_files, code_modules = _code_files(code)
        entry_source = _SourcePackager(
            code_modules, frozenset(_requirement_name(package) for package in packages)
        ).package(function)
        if code_files:
            kind = _PYTHON_BUNDLE_ARTIFACT
            source = json.dumps(
                {"files": {**code_files, _SOURCE_ENTRY_FILE: entry_source}},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        else:
            kind = _PYTHON_CALLABLE_ARTIFACT
            source = entry_source.encode("utf-8")
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
                kind=kind,
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
        # A class's own __dict__ holds its methods; copying it would shadow
        # this wrapper's attributes.
        functools.update_wrapper(
            self, function, updated=() if is_class else functools.WRAPPER_UPDATES
        )

    @property
    def registration_request(self) -> FunctionRegistrationRequest:
        """The immutable request sent by ``create_function_async``.

        Carries no secret bindings. Binding is a registration-time decision,
        so a Function bound to Secrets is registered through :meth:`bind_secrets`,
        which is what ``create_function`` calls.
        """
        return self._request

    def bind_secrets(
        self, secrets: Optional[Sequence[EnvVarSecret]]
    ) -> FunctionRegistrationRequest:
        """The registration request for this definition bound to ``secrets``.

        Binding does not change the Function's source: each
        [EnvVarSecret][lancedb.secrets.EnvVarSecret] names a Secret and the
        environment variable its value should arrive in, and the Function reads
        that variable the way it already did. Whether the named Secrets exist is
        the server's answer, not this one.
        """
        bindings = () if secrets is None else tuple(secrets)
        wrong_type = [
            binding for binding in bindings if not isinstance(binding, EnvVarSecret)
        ]
        if wrong_type:
            kinds = sorted({type(binding).__name__ for binding in wrong_type})
            raise TypeError(
                f"Function secrets must be EnvVarSecret values, not {kinds!r}; a "
                "credential value is never sent to this API"
            )
        variables = [binding.env_variable for binding in bindings]
        duplicates = sorted({name for name in variables if variables.count(name) > 1})
        if duplicates:
            raise ValueError(
                "a Function binds each environment variable once; duplicated: "
                f"{duplicates!r}"
            )
        # `env` is ordinary configuration carried in the definition, so a name in
        # both would have a value visible in the Function's record and a value
        # that is not. Refuse rather than pick.
        environment = self._request.runtime.env or {}
        overlap = sorted(set(environment) & set(variables))
        if overlap:
            raise ValueError(
                f"Function env and secret bindings must be disjoint: {overlap!r}"
            )
        if not bindings:
            return self._request
        # Sorted, because the list is carried in the FunctionVersion hash and a
        # caller's argument order is not part of what a Function is.
        resolved = tuple(
            sorted(
                (
                    SecretBinding(
                        kind="env",
                        variable=binding.env_variable,
                        secret_ref=SecretReference(
                            name=binding.secret_name,
                            namespace_path=tuple(binding.secret_namespace_path),
                        ),
                    )
                    for binding in bindings
                ),
                key=lambda binding: (binding.kind, binding.variable or ""),
            )
        )
        return self._request._copy(update={"secret_bindings": resolved})

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
    code: Sequence[types.ModuleType] = (),
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
    code: Sequence[types.ModuleType] = (),
):
    """Prepare a Python function or class for remote Function registration.

    Input and output signatures are inferred from supported annotations. For
    Arrow types annotations cannot express precisely, pass ``input_schema``
    and ``output_schema`` together. Scalar outputs must be non-nullable. Every
    named-struct field may be nullable; Enterprise preserves the struct's
    validity when the result is expanded into sibling columns.

    **Functions and classes.** A decorated function is called once per row,
    or once per batch when its parameters are annotated ``pyarrow.Array`` or
    it takes one ``pyarrow.RecordBatch``. A decorated class is the same
    callable with state: each remote instance calls ``__init__`` once, calls
    ``__call__`` for every row or batch of every input it processes, and calls
    ``close()``, if defined, once when it retires. Load models, open clients,
    and build rate limiters in ``__init__``; every batch of the instance then
    reuses them. An instance serves many batches of one binding, so state is
    shared across them, but not across instances or workers.

    **Initialization.** The annotated parameters of a class's ``__init__``
    are the Function's initialization fields. Their values belong to each
    binding, not to the Function: ``function(text=col("body"), model="small")``
    binds column ``body`` and initializes every instance with ``model="small"``,
    and another column can bind the same Function version with other values.
    Initialization fields take booleans, integers, floats, strings, and lists
    or structs of them (``Annotated[dict, pa.struct(...)]``). A parameter with
    a default may be omitted from a binding; a null value then takes the
    default. Use Secrets, not initialization, for credentials.

    **What travels with the Function.** The artifact is a snapshot of the
    decorated source plus exactly the module-level names it references:

    - modules, and classes and functions importable from an installed
      package, become imports; the package must be declared in ``pip`` or
      ``conda``;
    - literals (``None``, booleans, numbers, strings, bytes, tuples of them)
      are inlined;
    - functions and classes defined in ``__main__``, such as notebook cells,
      are packaged by source, together with what they reference;
    - modules and packages listed in ``code`` are shipped as Python source and
      imported normally, so helpers shared by several Functions live in one
      place.

    A reference to a module in a local source tree that is neither shipped
    with ``code`` nor provided by a declared package is rejected at
    registration. Closures, lambdas, nested definitions, mutable module-level
    objects, code that reaches the module namespace another way
    (``globals()``/``eval``, ``sys.modules``, ``builtins``), and a
    non-standard ``__builtins__`` are rejected where they can be seen and
    otherwise unsupported. Changing any packaged source, including a module in
    ``code``, produces a new Function version; the environment built from
    ``pip`` or ``conda`` is reused.

    Parameters
    ----------
    function : function or class, optional
        The synchronous callable, or the class whose instances are called.
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
        Environment variables included in the Function definition. Not for
        credentials -- these are ordinary configuration, stored with the
        Function and visible wherever it is. Prefer initialization for
        values that differ between bindings.
    python_version : str, optional
        Remote Python major/minor version. Defaults to the client version.
    gpu : bool, default False
        Whether every remote execution requires a GPU. The execution platform
        selects one compatible GPU for each worker. The requirement is part of
        the immutable Function version.
    code : sequence of module, optional
        Top-level modules or packages whose importable ``.py`` files ship
        with the Function and are importable by their own names.

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
    >>> @udf
    ... class scale:
    ...     def __init__(self, factor: float, offset: float = 0.0):
    ...         self.factor, self.offset = factor, offset
    ...     def __call__(self, value: float) -> float:
    ...         return value * self.factor + self.offset
    >>> scale(factor=2.0)(1.5)
    3.0
    >>> [field.name for field in scale.registration_request.signature.initialization]
    ['factor', 'offset']
    """

    def decorate(target: Any) -> UdfDefinition:
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
            code=tuple(code),
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
    "FunctionImage",
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
