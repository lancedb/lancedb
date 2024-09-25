# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import binascii
import functools
import importlib
import os
import pathlib
import warnings
from datetime import date, datetime
from functools import singledispatch
from typing import Tuple, Union, Optional, Any
from urllib.parse import urlparse

import numpy as np
import pyarrow as pa
import pyarrow.fs as pa_fs

from ._lancedb import validate_table_name as native_validate_table_name


def safe_import_adlfs():
    try:
        import adlfs

        return adlfs
    except ImportError:
        return None


adlfs = safe_import_adlfs()


def get_uri_scheme(uri: str) -> str:
    """
    Get the scheme of a URI. If the URI does not have a scheme, assume it is a file URI.

    Parameters
    ----------
    uri : str
        The URI to parse.

    Returns
    -------
    str: The scheme of the URI.
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme
    if not scheme:
        scheme = "file"
    elif scheme in ["s3a", "s3n"]:
        scheme = "s3"
    elif len(scheme) == 1:
        # Windows drive names are parsed as the scheme
        # e.g. "c:\path" -> ParseResult(scheme="c", netloc="", path="/path", ...)
        # So we add special handling here for schemes that are a single character
        scheme = "file"
    return scheme


def get_uri_location(uri: str) -> str:
    """
    Get the location of a URI. If the parameter is not a url, assumes it is just a path

    Parameters
    ----------
    uri : str
        The URI to parse.

    Returns
    -------
    str: Location part of the URL, without scheme
    """
    parsed = urlparse(uri)
    if len(parsed.scheme) == 1:
        # Windows drive names are parsed as the scheme
        # e.g. "c:\path" -> ParseResult(scheme="c", netloc="", path="/path", ...)
        # So we add special handling here for schemes that are a single character
        return uri

    if not parsed.netloc:
        return parsed.path
    else:
        return parsed.netloc + parsed.path


def fs_from_uri(uri: str) -> Tuple[pa_fs.FileSystem, str]:
    """
    Get a PyArrow FileSystem from a URI, handling extra environment variables.
    """
    if get_uri_scheme(uri) == "s3":
        fs = pa_fs.S3FileSystem(
            endpoint_override=os.environ.get("AWS_ENDPOINT"),
            request_timeout=30,
            connect_timeout=30,
        )
        path = get_uri_location(uri)
        return fs, path

    elif get_uri_scheme(uri) == "az" and adlfs is not None:
        az_blob_fs = adlfs.AzureBlobFileSystem(
            account_name=os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"),
            account_key=os.environ.get("AZURE_STORAGE_ACCOUNT_KEY"),
        )

        fs = pa_fs.PyFileSystem(pa_fs.FSSpecHandler(az_blob_fs))

        path = get_uri_location(uri)
        return fs, path

    return pa_fs.FileSystem.from_uri(uri)


def join_uri(base: Union[str, pathlib.Path], *parts: str) -> str:
    """
    Join a URI with multiple parts, handles both local and remote paths

    Parameters
    ----------
    base : str
        The base URI
    parts : str
        The parts to join to the base URI, each separated by the
        appropriate path separator for the URI scheme and OS
    """
    if isinstance(base, pathlib.Path):
        return base.joinpath(*parts)
    base = str(base)
    if get_uri_scheme(base) == "file":
        # using pathlib for local paths make this windows compatible
        # `get_uri_scheme` returns `file` for windows drive names (e.g. `c:\path`)
        return str(pathlib.Path(base, *parts))
    else:
        # there might be query parameters in the base URI
        url = urlparse(base)
        new_path = "/".join([p.rstrip("/") for p in [url.path, *parts]])
        return url._replace(path=new_path).geturl()


def attempt_import_or_raise(module: str, mitigation=None):
    """
    Import the specified module. If the module is not installed,
    raise an ImportError with a helpful message.

    Parameters
    ----------
    module : str
        The name of the module to import
    mitigation : Optional[str]
        The package(s) to install to mitigate the error.
        If not provided then the module name will be used.
    """
    try:
        return importlib.import_module(module)
    except ImportError:
        raise ImportError(f"Please install {mitigation or module}")


def safe_import_pandas():
    try:
        import pandas as pd

        return pd
    except ImportError:
        return None


def safe_import_polars():
    try:
        import polars as pl

        return pl
    except ImportError:
        return None


def inf_vector_column_query(schema: pa.Schema) -> str:
    """
    Get the vector column name

    Parameters
    ----------
    schema : pa.Schema
        The schema of the vector column.

    Returns
    -------
    str: the vector column name.
    """
    vector_col_name = ""
    vector_col_count = 0
    for field_name in schema.names:
        field = schema.field(field_name)
        if pa.types.is_fixed_size_list(field.type) and pa.types.is_floating(
            field.type.value_type
        ):
            vector_col_count += 1
            if vector_col_count > 1:
                raise ValueError(
                    "Schema has more than one vector column. "
                    "Please specify the vector column name "
                    "for vector search"
                )
                break
            elif vector_col_count == 1:
                vector_col_name = field_name
    if vector_col_count == 0:
        raise ValueError(
            "There is no vector column in the data. "
            "Please specify the vector column name for vector search"
        )
    return vector_col_name


def infer_vector_column_name(
    schema: pa.Schema,
    query_type: str,
    query: Optional[Any],  # inferred later in query builder
    vector_column_name: Optional[str],
):
    if (vector_column_name is None and query is not None and query_type != "fts") or (
        vector_column_name is None and query_type == "hybrid"
    ):
        try:
            vector_column_name = inf_vector_column_query(schema)
        except Exception as e:
            raise e

    return vector_column_name


@singledispatch
def value_to_sql(value):
    raise NotImplementedError("SQL conversion is not implemented for this type")


@value_to_sql.register(str)
def _(value: str):
    value = value.replace("'", "''")
    return f"'{value}'"


@value_to_sql.register(bytes)
def _(value: bytes):
    """Convert bytes to a hex string literal.

    See https://datafusion.apache.org/user-guide/sql/data_types.html#binary-types
    """
    return f"X'{binascii.hexlify(value).decode()}'"


@value_to_sql.register(int)
def _(value: int):
    return str(value)


@value_to_sql.register(float)
def _(value: float):
    return str(value)


@value_to_sql.register(bool)
def _(value: bool):
    return str(value).upper()


@value_to_sql.register(type(None))
def _(value: type(None)):
    return "NULL"


@value_to_sql.register(datetime)
def _(value: datetime):
    return f"'{value.isoformat()}'"


@value_to_sql.register(date)
def _(value: date):
    return f"'{value.isoformat()}'"


@value_to_sql.register(list)
def _(value: list):
    return "[" + ", ".join(map(value_to_sql, value)) + "]"


@value_to_sql.register(np.ndarray)
def _(value: np.ndarray):
    return value_to_sql(value.tolist())


def deprecated(func):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used."""

    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter("always", DeprecationWarning)  # turn off filter
        warnings.warn(
            (
                f"Function {func.__name__} is deprecated and will be "
                "removed in a future version"
            ),
            category=DeprecationWarning,
            stacklevel=2,
        )
        warnings.simplefilter("default", DeprecationWarning)  # reset filter
        return func(*args, **kwargs)

    return new_func


def validate_table_name(name: str):
    """Verify the table name is valid."""
    native_validate_table_name(name)
