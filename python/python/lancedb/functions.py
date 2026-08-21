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
from pydantic import BaseModel, Field

_PYDANTIC_V2 = int(pydantic.VERSION.split(".", 1)[0]) >= 2
if _PYDANTIC_V2:
    from pydantic import model_validator
else:
    from pydantic import root_validator


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
        if _PYDANTIC_V2:
            return self.model_dump(exclude_none=True, exclude_defaults=True)
        return self.dict(exclude_none=True, exclude_defaults=True)

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


class FunctionArtifact(_RemoteValue):
    """Content-addressed Python artifact identity."""

    kind: str
    digest: str
    entrypoint: str


class FunctionParameter(_RemoteValue):
    name: str
    arrow_type: str
    nullable: bool


class FunctionResultField(_RemoteValue):
    name: str
    arrow_type: str
    nullable: bool


class FunctionOutput(_RemoteValue):
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
    """Remote Python runtime definition with non-secret environment values."""

    kind: str
    python_version: str
    environment: PythonEnvironmentSpec
    env: Mapping[str, str] = Field(default_factory=dict)


class FunctionVersion(_RemoteValue):
    """An exact immutable Function version returned by Enterprise."""

    name: str
    version: str
    artifact: FunctionArtifact
    signature: FunctionSignature
    runtime: PythonRuntimeSpec
    runtime_digest: str
    environment_digest: str
    required_secrets: tuple[str, ...] = ()
    created_at: str


class FunctionVersionRef(_RemoteValue):
    name: str
    version: str


class ApplicationInput(_RemoteValue):
    parameter: str
    kind: str
    value: Any


class FunctionApplication(_RemoteValue):
    """Immutable pre-declaration application of an exact Function version."""

    function: FunctionVersionRef
    inputs: tuple[ApplicationInput, ...]
    output: FunctionOutput
    group_id: str
    columns: Mapping[str, str] = Field(default_factory=dict)

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
    field_id: int
    field_path: str
    arrow_type: str
    nullable: bool


class OutputMapping(_RemoteValue):
    result_field: str
    output_name: str
    output_field_id: int
    output_ordinal: int
    arrow_type: str
    nullable: bool


class FunctionBinding(_RemoteValue):
    """Immutable grouped binding persisted by the Enterprise table service."""

    binding_id: str
    revision: int
    function: FunctionVersionRef
    group_id: str
    inputs: tuple[InputBinding, ...]
    outputs: tuple[OutputMapping, ...]


class RefreshColumnResult(_RemoteValue):
    """Terminal result of a remote Function-column refresh Job."""

    rows_assigned: int
    rows_failed: int
    rows_remaining: int
    source_version: int
    published_version: Optional[int]

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
