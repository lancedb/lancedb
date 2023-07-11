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

"""Schema related utilities."""

import json
from typing import Any, Dict, Type

import pyarrow as pa


def vector(dimension: int, value_type: pa.DataType = pa.float32()) -> pa.DataType:
    """A help function to create a vector type.

    Parameters
    ----------
    dimension: The dimension of the vector.
    value_type: pa.DataType, optional
        The type of the value in the vector.

    Returns
    -------
    A PyArrow DataType for vectors.

    Examples
    --------

    >>> import pyarrow as pa
    >>> import lancedb
    >>> schema = pa.schema([
    ...     pa.field("id", pa.int64()),
    ...     pa.field("vector", lancedb.vector(756)),
    ... ])
    """
    return pa.list_(value_type, dimension)


def _type_to_dict(dt: pa.DataType) -> Dict[str, Any]:
    if pa.types.is_boolean(dt):
        return {"type": "boolean"}
    elif pa.types.is_int8(dt):
        return {"type": "int8"}
    elif pa.types.is_int16(dt):
        return {"type": "int16"}
    elif pa.types.is_int32(dt):
        return {"type": "int32"}
    elif pa.types.is_int64(dt):
        return {"type": "int64"}
    elif pa.types.is_uint8(dt):
        return {"type": "uint8"}
    elif pa.types.is_uint16(dt):
        return {"type": "uint16"}
    elif pa.types.is_uint32(dt):
        return {"type": "uint32"}
    elif pa.types.is_uint64(dt):
        return {"type": "uint64"}
    elif pa.types.is_float16(dt):
        return {"type": "float16"}
    elif pa.types.is_float32(dt):
        return {"type": "float32"}
    elif pa.types.is_float64(dt):
        return {"type": "float64"}
    elif pa.types.is_string(dt):
        return {"type": "string"}
    elif pa.types.is_binary(dt):
        return {"type": "binary"}
    elif pa.types.is_large_string(dt):
        return {"type": "large_string"}
    elif pa.types.is_large_binary(dt):
        return {"type": "large_binary"}
    elif pa.types.is_fixed_size_binary(dt):
        return {"type": "fixed_size_binary", "width": dt.byte_width}
    elif pa.types.is_fixed_size_list(dt):
        return {
            "type": "fixed_size_list",
            "width": dt.list_size,
            "value_type": _type_to_dict(dt.value_type),
        }
    elif pa.types.is_list(dt):
        return {
            "type": "fixed_size_list",
            "value_type": _type_to_dict(dt.value_type),
        }
    elif pa.types.is_struct(dt):
        return {
            "type": "struct",
            "fields": [_field_to_dict(dt.field(i)) for i in range(dt.num_fields)],
        }
    elif pa.types.is_dictionary(dt):
        return {
            "type": "dictionary",
            "index_type": _type_to_dict(dt.index_type),
            "value_type": _type_to_dict(dt.value_type),
        }
    # TODO: support extension types

    raise TypeError(f"Unsupported type: {dt}")


def _field_to_dict(field: pa.field) -> Dict[str, Any]:
    ret = {
        "name": field.name,
        "type": _type_to_dict(field.type),
        "nullable": field.nullable,
    }
    if field.metadata is not None:
        ret["metadata"] = field.metadata
    return ret


def schema_to_dict(schema: pa.Schema) -> Dict[str, Any]:
    """Convert a PyArrow [Schema](pyarrow.Schema) to a dictionary.

    Parameters
    ----------
    schema : pa.Schema
        The PyArrow Schema to convert

    Returns
    -------
    A dict of the data type.

    Examples
    --------

    >>> import pyarrow as pa
    >>> import lancedb
    >>> schema = pa.schema(
    ...     [
    ...         pa.field("id", pa.int64()),
    ...         pa.field("vector", lancedb.vector(512), nullable=False),
    ...         pa.field(
    ...             "struct",
    ...             pa.struct(
    ...             [
    ...                 pa.field("a", pa.utf8()),
    ...                 pa.field("b", pa.float32()),
    ...             ]
    ...         ),
    ...         True,
    ...     ),
    ...     ],
    ...     metadata={"key": "value"},
    ... )
    >>> json_schema = schema_to_dict(schema)
    >>> assert json_schema == {
    ...     "fields": [
    ...     {"name": "id", "type": {"name": "int64"}, "nullable": True},
    ...     {
    ...         "name": "vector",
    ...         "type": {
    ...             "name": "fixed_size_list",
    ...             "value_type": {"name": "float32"},
    ...             "width": 512,
    ...         },
    ...        "nullable": False,
    ...    },
    ...    {
    ...         "name": "struct",
    ...         "type": {
    ...             "name": "struct",
    ...             "fields": [
    ...                 {"name": "a", "type": {"name": "string"}, "nullable": True},
    ...                 {"name": "b", "type": {"name": "float32"}, "nullable": True},
    ...            ],
    ...         },
    ...         "nullable": True,
    ...     },
    ...     ],
    ...     "metadata": {"key": "value"},
    ... }

    """
    fields = []
    for name in schema.names:
        field = schema.field(name)
        fields.append(_field_to_dict(field))
    return {
        "fields": fields,
        "metadata": {
            k.decode("utf-8"): v.decode("utf-8") for (k, v) in schema.metadata.items()
        },
    }


def _dict_to_type(dt: Dict[str, Any]) -> pa.DataType:
    type_name = dt["type"]
    try:
        return {
            "boolean": pa.bool_(),
            "int8": pa.int8(),
            "int16": pa.int16(),
            "int32": pa.int32(),
            "int64": pa.int64(),
            "uint8": pa.uint8(),
            "uint16": pa.uint16(),
            "uint32": pa.uint32(),
            "uint64": pa.uint64(),
            "float16": pa.float16(),
            "float32": pa.float32(),
            "float64": pa.float64(),
            "string": pa.string(),
            "binary": pa.binary(),
            "large_string": pa.large_string(),
            "large_binary": pa.large_binary(),
        }[type_name]
    except KeyError:
        pass

    if type_name == "fixed_size_binary":
        return pa.binary(dt["width"])
    elif type_name == "fixed_size_list":
        return pa.list_(_dict_to_type(dt["value_type"]), dt["width"])
    elif type_name == "list":
        return pa.list_(_dict_to_type(dt["value_type"]))
    elif type_name == "struct":
        fields = []
        for field in dt["fields"]:
            fields.append(_dict_to_field(field))
        return pa.struct(fields)
    elif type_name == "dictionary":
        return pa.dictionary(
            _dict_to_type(dt["index_type"]), _dict_to_type(dt["value_type"])
        )
    raise TypeError(f"Unsupported type: {dt}")


def _dict_to_field(field: Dict[str, Any]) -> pa.Field:
    name = field["name"]
    nullable = field["nullable"] if "nullable" in field else True
    dt = _dict_to_type(field["type"])
    metadata = field.get("metadata", None)
    return pa.field(name, dt, nullable, metadata)


def dict_to_schema(json: Dict[str, Any]) -> pa.Schema:
    """Reconstruct a PyArrow Schema from a JSON dict.

    Parameters
    ----------
    json : Dict[str, Any]
        The JSON dict to reconstruct Schema from.

    Returns
    -------
    A PyArrow Schema.
    """
    fields = []
    for field in json["fields"]:
        fields.append(_dict_to_field(field))
    metadata = {
        k.encode("utf-8"): v.encode("utf-8")
        for (k, v) in json.get("metadata", {}).items()
    }
    return pa.schema(fields, metadata)
