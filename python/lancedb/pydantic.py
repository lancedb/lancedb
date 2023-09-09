#  Copyright 2023 LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Pydantic (v1 / v2) adapter for LanceDB"""

from __future__ import annotations

import inspect
import sys
import types
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Generator, List, Type, Union, _GenericAlias

import numpy as np
import pyarrow as pa
import pydantic
import semver

PYDANTIC_VERSION = semver.Version.parse(pydantic.__version__)
try:
    from pydantic_core import CoreSchema, core_schema
except ImportError:
    if PYDANTIC_VERSION >= (2,):
        raise


class FixedSizeListMixin(ABC):
    @staticmethod
    @abstractmethod
    def dim() -> int:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def value_arrow_type() -> pa.DataType:
        raise NotImplementedError


def vector(dim: int, value_type: pa.DataType = pa.float32()):
    # TODO: remove in future release
    from warnings import warn

    warn(
        "lancedb.pydantic.vector() is deprecated, use lancedb.pydantic.Vector instead."
        "This function will be removed in future release",
        DeprecationWarning,
    )
    return Vector(dim, value_type)


def Vector(
    dim: int, value_type: pa.DataType = pa.float32()
) -> Type[FixedSizeListMixin]:
    """Pydantic Vector Type.

    !!! warning
        Experimental feature.

    Parameters
    ----------
    dim : int
        The dimension of the vector.
    value_type : pyarrow.DataType, optional
        The value type of the vector, by default pa.float32()

    Examples
    --------

    >>> import pydantic
    >>> from lancedb.pydantic import Vector
    ...
    >>> class MyModel(pydantic.BaseModel):
    ...     id: int
    ...     url: str
    ...     embeddings: Vector(768)
    >>> schema = pydantic_to_schema(MyModel)
    >>> assert schema == pa.schema([
    ...     pa.field("id", pa.int64(), False),
    ...     pa.field("url", pa.utf8(), False),
    ...     pa.field("embeddings", pa.list_(pa.float32(), 768), False)
    ... ])
    """

    # TODO: make a public parameterized type.
    class FixedSizeList(list, FixedSizeListMixin):
        def __repr__(self):
            return f"FixedSizeList(dim={dim})"

        @staticmethod
        def dim() -> int:
            return dim

        @staticmethod
        def value_arrow_type() -> pa.DataType:
            return value_type

        @classmethod
        def __get_pydantic_core_schema__(
            cls, _source_type: Any, _handler: pydantic.GetCoreSchemaHandler
        ) -> CoreSchema:
            return core_schema.no_info_after_validator_function(
                cls,
                core_schema.list_schema(
                    min_length=dim,
                    max_length=dim,
                    items_schema=core_schema.float_schema(),
                ),
            )

        @classmethod
        def __get_validators__(cls) -> Generator[Callable, None, None]:
            yield cls.validate

        # For pydantic v1
        @classmethod
        def validate(cls, v):
            if not isinstance(v, (list, range, np.ndarray)) or len(v) != dim:
                raise TypeError("A list of numbers or numpy.ndarray is needed")
            return v

        if PYDANTIC_VERSION < (2, 0):

            @classmethod
            def __modify_schema__(cls, field_schema: Dict[str, Any]):
                field_schema["items"] = {"type": "number"}
                field_schema["maxItems"] = dim
                field_schema["minItems"] = dim

    return FixedSizeList


def _py_type_to_arrow_type(py_type: Type[Any]) -> pa.DataType:
    """Convert Python Type to Arrow DataType.

    Raises
    ------
    TypeError
        If the type is not supported.
    """
    if py_type == int:
        return pa.int64()
    elif py_type == float:
        return pa.float64()
    elif py_type == str:
        return pa.utf8()
    elif py_type == bool:
        return pa.bool_()
    elif py_type == bytes:
        return pa.binary()
    raise TypeError(
        f"Converting Pydantic type to Arrow Type: unsupported type {py_type}"
    )


if PYDANTIC_VERSION.major < 2:

    def _pydantic_model_to_fields(model: pydantic.BaseModel) -> List[pa.Field]:
        return [
            _pydantic_to_field(name, field) for name, field in model.__fields__.items()
        ]

else:

    def _pydantic_model_to_fields(model: pydantic.BaseModel) -> List[pa.Field]:
        return [
            _pydantic_to_field(name, field)
            for name, field in model.model_fields.items()
        ]


def _pydantic_to_arrow_type(field: pydantic.fields.FieldInfo) -> pa.DataType:
    """Convert a Pydantic FieldInfo to Arrow DataType"""
    if isinstance(field.annotation, _GenericAlias) or (
        sys.version_info > (3, 9) and isinstance(field.annotation, types.GenericAlias)
    ):
        origin = field.annotation.__origin__
        args = field.annotation.__args__
        if origin == list:
            child = args[0]
            return pa.list_(_py_type_to_arrow_type(child))
        elif origin == Union:
            if len(args) == 2 and args[1] == type(None):
                return _py_type_to_arrow_type(args[0])
    elif inspect.isclass(field.annotation):
        if issubclass(field.annotation, pydantic.BaseModel):
            # Struct
            fields = _pydantic_model_to_fields(field.annotation)
            return pa.struct(fields)
        elif issubclass(field.annotation, FixedSizeListMixin):
            return pa.list_(field.annotation.value_arrow_type(), field.annotation.dim())
    return _py_type_to_arrow_type(field.annotation)


def is_nullable(field: pydantic.fields.FieldInfo) -> bool:
    """Check if a Pydantic FieldInfo is nullable."""
    if isinstance(field.annotation, _GenericAlias):
        origin = field.annotation.__origin__
        args = field.annotation.__args__
        if origin == Union:
            if len(args) == 2 and args[1] == type(None):
                return True
    return False


def _pydantic_to_field(name: str, field: pydantic.fields.FieldInfo) -> pa.Field:
    """Convert a Pydantic field to a PyArrow Field."""
    dt = _pydantic_to_arrow_type(field)
    return pa.field(name, dt, is_nullable(field))


def pydantic_to_schema(model: Type[pydantic.BaseModel]) -> pa.Schema:
    """Convert a Pydantic model to a PyArrow Schema.

    Parameters
    ----------
    model : Type[pydantic.BaseModel]
        The Pydantic BaseModel to convert to Arrow Schema.

    Returns
    -------
    pyarrow.Schema

    Examples
    --------

    >>> from typing import List, Optional
    >>> import pydantic
    >>> from lancedb.pydantic import pydantic_to_schema
    ...
    >>> class InnerModel(pydantic.BaseModel):
    ...     a: str
    ...     b: Optional[float]
    >>>
    >>> class FooModel(pydantic.BaseModel):
    ...     id: int
    ...     s: Optional[str] = None
    ...     vec: List[float]
    ...     li: List[int]
    ...     inner: InnerModel
    >>> schema = pydantic_to_schema(FooModel)
    >>> assert schema == pa.schema([
    ...     pa.field("id", pa.int64(), False),
    ...     pa.field("s", pa.utf8(), True),
    ...     pa.field("vec", pa.list_(pa.float64()), False),
    ...     pa.field("li", pa.list_(pa.int64()), False),
    ...     pa.field("inner", pa.struct([
    ...         pa.field("a", pa.utf8(), False),
    ...         pa.field("b", pa.float64(), True),
    ...     ]), False),
    ... ])
    """
    fields = _pydantic_model_to_fields(model)
    return pa.schema(fields)


class LanceModel(pydantic.BaseModel):
    """
    A Pydantic Model base class that can be converted to a LanceDB Table.

    Examples
    --------
    >>> import lancedb
    >>> from lancedb.pydantic import LanceModel, Vector
    >>>
    >>> class TestModel(LanceModel):
    ...     name: str
    ...     vector: Vector(2)
    ...
    >>> db = lancedb.connect("/tmp")
    >>> table = db.create_table("test", schema=TestModel.to_arrow_schema())
    >>> table.add([
    ...     TestModel(name="test", vector=[1.0, 2.0])
    ... ])
    >>> table.search([0., 0.]).limit(1).to_pydantic(TestModel)
    [TestModel(name='test', vector=FixedSizeList(dim=2))]
    """

    @classmethod
    def to_arrow_schema(cls):
        """
        Get the Arrow Schema for this model.
        """
        return pydantic_to_schema(cls)

    @classmethod
    def field_names(cls) -> List[str]:
        """
        Get the field names of this model.
        """
        if PYDANTIC_VERSION.major < 2:
            return list(cls.__fields__.keys())
        return list(cls.model_fields.keys())
