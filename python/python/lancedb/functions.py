# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Canonical values exchanged with LanceDB Enterprise Function services.

These immutable models contain client/wire state only. Catalog persistence,
environment bake, secret resolution, and execution are owned by Sophon.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Optional

import pydantic
from pydantic import BaseModel, Field, conint

_PYDANTIC_V2 = int(pydantic.VERSION.split(".", 1)[0]) >= 2
if _PYDANTIC_V2:
    from pydantic import field_validator, model_validator
else:
    from pydantic import root_validator, validator

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
    if _PYDANTIC_V2:
        model_config = {"extra": "ignore", "frozen": True}
    else:

        class Config:
            allow_mutation = False
            extra = "ignore"

    if _PYDANTIC_V2:

        @model_validator(mode="after")
        def _freeze_mappings(self):
            for name, value in self.__dict__.items():
                object.__setattr__(self, name, _freeze_value(value))
            return self

    else:

        @root_validator
        def _freeze_mappings(cls, values):
            return {name: _freeze_value(value) for name, value in values.items()}

    @classmethod
    def from_json(cls, payload: str):
        if _PYDANTIC_V2:
            return cls.model_validate_json(payload)
        return cls.parse_raw(payload)

    def _known_dict(self) -> dict[str, Any]:
        fields = self.__class__.model_fields if _PYDANTIC_V2 else self.__fields__
        known = {}
        for name, field in fields.items():
            value = getattr(self, name)
            if value is None:
                continue
            required = field.is_required() if _PYDANTIC_V2 else field.required
            if not required:
                default_factory = field.default_factory
                if default_factory is not None and value == default_factory():
                    continue
                if default_factory is None and value == field.default:
                    continue
            known[name] = _known_wire_value(value)
        return known

    def _copy(self, *, update: Mapping[str, Any]):
        update = {name: _freeze_value(value) for name, value in update.items()}
        if _PYDANTIC_V2:
            return self.model_copy(update=update)
        return self.copy(update=update)

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

    if _PYDANTIC_V2:
        model_config = {"extra": "allow", "frozen": True}
    else:

        class Config:
            allow_mutation = False
            extra = "allow"

    def _unknown_field_names(self) -> set[str]:
        if _PYDANTIC_V2:
            return set((self.__pydantic_extra__ or {}).keys())
        return set(self.__dict__) - set(self.__fields__)


class FunctionArtifact(_RemoteValue):
    """Content-addressed Python artifact identity."""

    kind: str
    digest: str
    entrypoint: str


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

    if _PYDANTIC_V2:

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

    else:

        @root_validator
        def _validate_runtime_kind(cls, values):
            if values.get("kind") == "python":
                if values.get("python_version") is None:
                    raise ValueError("python runtime requires python_version")
                if values.get("environment") is None:
                    raise ValueError("python runtime requires environment")
            else:
                values["python_version"] = None
                values["environment"] = None
                values["env"] = None
            return values


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

    if _PYDANTIC_V2:

        @field_validator("value")
        @classmethod
        def _validate_value(cls, value):
            return _validate_literal(value)

    else:

        @validator("value")
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


__all__ = [
    "ApplicationInput",
    "FunctionApplication",
    "FunctionArtifact",
    "FunctionBinding",
    "FunctionOutput",
    "FunctionParameter",
    "FunctionResultField",
    "FunctionSignature",
    "FunctionVersion",
    "FunctionVersionRef",
    "InputBinding",
    "OutputMapping",
    "PythonEnvironmentSpec",
    "PythonRuntimeSpec",
    "RefreshColumnResult",
]
