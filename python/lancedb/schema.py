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
    The vector type.

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
        return {"name": "boolean"}
    elif pa.types.is_int8(dt):
        return {"name": "int8"}
    elif pa.types.is_int16(dt):
        return {"name": "int16"}
    elif pa.types.is_int32(dt):
        return {"name": "int32"}
    elif pa.types.is_int64(dt):
        return {"name": "int64"}
    elif pa.types.is_uint8(dt):
        return {"name": "uint8"}
    elif pa.types.is_uint16(dt):
        return {"name": "uint16"}
    elif pa.types.is_uint32(dt):
        return {"name": "uint32"}
    elif pa.types.is_uint64(dt):
        return {"name": "uint64"}
    elif pa.types.is_float16(dt):
        return {"name": "float16"}
    elif pa.types.is_float32(dt):
        return {"name": "float32"}
    elif pa.types.is_float64(dt):
        return {"name": "float64"}
    elif pa.types.is_string(dt):
        return {"name": "string"}
    elif pa.types.is_binary(dt):
        return {"name": "binary"}
    elif pa.types.is_large_string(dt):
        return {"name": "large_string"}
    elif pa.types.is_large_binary(dt):
        return {"name": "large_binary"}
    elif pa.types.is_fixed_size_binary(dt):
        return {"name": "fixed_size_binary", "width": dt.byte_width}
    elif pa.types.is_fixed_size_list(dt):
        return {
            "name": "fixed_size_list",
            "width": dt.list_size,
            "value_type": _type_to_dict(dt.value_type),
        }
    elif pa.types.is_struct(dt):
        return {
            "name": "struct",
            "fields": [_field_to_dict(dt.field(i)) for i in range(dt.num_fields)],
        }
    # TODO: support extension types

    raise TypeError(f"Unsupported type: {dt}")


def _field_to_dict(field: pa.field) -> Dict[str, Any]:
    return {
        "name": field.name,
        "type": _type_to_dict(field.type),
        "nullable": field.nullable,
        # "metadata": field.metadata,
    }


def schema_to_dict(schema: pa.Schema) -> Dict[str, Any]:
    """Convert a PyArrow [Schema](pyarrow.Schema) to a dictionary.

    Parameters
    ----------
    schema : pa.Schema
        The PyArrow Schema to convert

    Returns
    -------
    A dict of the data type.

    """
    fields = []
    for name in schema.names:
        field = schema.field_by_name(name)
        fields.append(_field_to_dict(field))
    return {
        "fields": fields,
        "metadata": {
            k.decode("utf-8"): v.decode("utf-8") for (k, v) in schema.metadata.items()
        },
    }


def dict_to_schema(json: Dict[str, Any]) -> pa.Schema:
    """Reconstruct a PyArrow Schema from a JSON dict."""

    pass
