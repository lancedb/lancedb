# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Helper for reading files into LanceDB tables.

Provides :func:`read_files` for reading Parquet, CSV, and Lance files
(including glob patterns) as data sources compatible with
:meth:`~lancedb.table.Table.add`.

Example
-------
>>> import lancedb
>>> async def example(table):
...     data = lancedb.read_files("./data/*.parquet")
...     await table.add(data)
"""

import glob as glob_module
import re
from pathlib import PurePosixPath
from typing import Union

import pyarrow as pa
import pyarrow.dataset as ds

from .scannable import Scannable, to_scannable


def _detect_format(pattern: str) -> str:
    """Detect file format from a path or glob pattern.

    Glob wildcard characters are replaced before extension detection so that
    patterns like ``"data/*.parquet"`` resolve to ``"parquet"``.
    """
    # Replace glob wildcards so extension detection works on the result.
    clean = re.sub(r"[*?]", "x", pattern)
    # Strip bracket expressions per path segment:
    # "data/[abc].parquet" -> "data/.parquet"
    clean = "/".join(seg.split("[")[0] for seg in clean.split("/"))
    ext = PurePosixPath(clean).suffix.lower()
    if ext == ".parquet":
        return "parquet"
    elif ext == ".csv":
        return "csv"
    elif ext == ".lance":
        return "lance"
    else:
        raise ValueError(
            f"Unsupported file format '{ext}'. "
            "Supported formats: .parquet, .csv, .lance"
        )


def _has_glob_chars(pattern: str) -> bool:
    return any(c in pattern for c in ("*", "?", "["))


def _open_dataset(pattern: str) -> Union[ds.Dataset, "lance.LanceDataset"]:  # noqa: F821
    fmt = _detect_format(pattern)

    if fmt == "lance":
        if _has_glob_chars(pattern):
            raise ValueError(
                "Glob patterns are not supported for Lance format. "
                "Provide a single Lance dataset path."
            )
        import lance

        return lance.dataset(pattern)

    # For parquet/csv: expand glob patterns to a list of paths, then open as a dataset.
    if _has_glob_chars(pattern):
        paths = glob_module.glob(pattern)
        if not paths:
            raise FileNotFoundError(f"No files matched pattern: {pattern!r}")
        return ds.dataset(paths, format=fmt)

    return ds.dataset(pattern, format=fmt)


class FileSource:
    """A data source backed by files on disk.

    Created by :func:`read_files` and accepted by
    :meth:`~lancedb.table.Table.add`.

    Attributes
    ----------
    schema : pa.Schema
        The Arrow schema inferred from the files.

    Examples
    --------
    >>> import lancedb
    >>> async def example(table):
    ...     source = lancedb.read_files("./data/*.parquet")
    ...     print(source.schema)
    ...     await table.add(source)
    """

    def __init__(self, pattern: str):
        self._dataset = _open_dataset(pattern)

    @property
    def schema(self) -> pa.Schema:
        return self._dataset.schema


@to_scannable.register(FileSource)
def _from_file_source(data: FileSource) -> Scannable:
    # Delegate to the existing handler for the underlying dataset type.
    # Both pa.dataset.Dataset and lance.LanceDataset are already registered.
    return to_scannable(data._dataset)


def read_files(pattern: str) -> FileSource:
    """Read files matching a glob pattern or path as a data source.

    The format is auto-detected from the file extension:

    - ``.parquet`` — Apache Parquet
    - ``.csv`` — Comma-separated values
    - ``.lance`` — Lance dataset (single path only, glob not supported)

    The returned :class:`FileSource` can be passed directly to
    :meth:`~lancedb.table.Table.add`.

    Parameters
    ----------
    pattern : str
        A file path or glob pattern, e.g. ``"./data/*.parquet"``.

    Returns
    -------
    FileSource

    Examples
    --------
    >>> import lancedb
    >>> async def example(table):
    ...     # Single file
    ...     data = lancedb.read_files("./data/records.parquet")
    ...     await table.add(data)
    ...
    ...     # Glob pattern
    ...     data = lancedb.read_files("./data/*.parquet")
    ...     await table.add(data)
    """
    return FileSource(pattern)
