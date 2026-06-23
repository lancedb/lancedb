# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Blob fetch API and v2 projection helpers."""

from __future__ import annotations

from typing import Any, Iterable, Literal, Optional, Union

import pyarrow as pa

from .schema import blob_v2_column_paths
from .util import get_uri_scheme

BlobMode = Literal["lazy", "bytes", "descriptions"]

BLOB_MODE_TO_HANDLING = {
    "lazy": "blobs_descriptions",
    "bytes": "all_binary",
    "descriptions": "blobs_descriptions",
}


def validate_blob_mode(blob_mode: BlobMode) -> None:
    if blob_mode not in BLOB_MODE_TO_HANDLING:
        modes = ", ".join(repr(mode) for mode in BLOB_MODE_TO_HANDLING)
        raise ValueError(f"blob_mode must be one of {modes}, got {blob_mode!r}")


class BlobFile:
    """Lazy handle from :meth:`lancedb.table.Table.fetch_blob_files`.

    Call ``read`` from sync code, ``aread`` from async code.
    """

    def __init__(self, inner):
        self._inner = inner

    async def aread(self) -> bytes:
        return await self._inner.read()

    def read(self) -> bytes:
        return self._inner.read_bytes()


def supports_blob_auto_row_id(table) -> bool:
    """Blob auto row-id applies to native tables, not LanceDB Cloud."""
    from .remote.table import RemoteTable

    if isinstance(table, RemoteTable):
        return False

    inner = getattr(table, "_inner", None)
    if inner is not None:
        uri = inner.database().uri
        if isinstance(uri, str) and get_uri_scheme(uri) == "db":
            return False

    return True


def _iter_projection_pairs(projection: Any) -> Iterable[tuple[str, str]]:
    if isinstance(projection, dict):
        for name, expr in projection.items():
            if isinstance(expr, str):
                yield name, expr
        return
    if isinstance(projection, list):
        for column in projection:
            if isinstance(column, str):
                yield column, column
            elif isinstance(column, tuple) and len(column) == 2:
                name, expr = column
                if isinstance(expr, str):
                    yield name, expr


def projection_includes_blob_column(
    projection: Any, blob_columns: Iterable[str]
) -> bool:
    columns = set(blob_columns)
    if not columns:
        return False
    if projection is None:
        return True
    for output, source in _iter_projection_pairs(projection):
        if output in columns or source in columns:
            return True
    return False


def blob_v2_projection_sources(schema: Any, projection: Any) -> dict[str, str]:
    if not isinstance(schema, pa.Schema):
        return {}

    blob_columns = blob_v2_column_paths(schema)
    if not blob_columns:
        return {}
    columns = set(blob_columns)
    if projection is None:
        return {column: column for column in blob_columns}
    return {
        output: source
        for output, source in _iter_projection_pairs(projection)
        if source in columns
    }


def v2_projection_needs_row_id(
    schema: Any,
    projection: Any,
    *,
    with_row_id: bool,
) -> bool:
    if with_row_id or not isinstance(schema, pa.Schema):
        return False
    return projection_includes_blob_column(projection, blob_v2_column_paths(schema))


def _set_blob_column(tbl: pa.Table, output_name: str, blobs: pa.Array) -> pa.Table:
    index = tbl.schema.get_field_index(output_name)
    return tbl.set_column(index, pa.field(output_name, blobs.type), [blobs])


async def replace_v2_blob_columns_with_bytes(
    tbl: pa.Table,
    blob_sources: dict[str, str],
    fetch_blobs,
) -> pa.Table:
    for output_name, source_name in blob_sources.items():
        if output_name not in tbl.column_names:
            continue
        blobs = await fetch_blobs(source_name, tbl)
        tbl = _set_blob_column(tbl, output_name, blobs)
    return tbl


def replace_v2_blob_columns_with_bytes_sync(
    tbl: pa.Table,
    blob_sources: dict[str, str],
    fetch_blobs,
) -> pa.Table:
    for output_name, source_name in blob_sources.items():
        if output_name not in tbl.column_names:
            continue
        blobs = fetch_blobs(source_name, tbl)
        tbl = _set_blob_column(tbl, output_name, blobs)
    return tbl


def _normalize_blob_row_ids(row_ids: Union[list[int], pa.Table]) -> list[int]:
    if isinstance(row_ids, pa.Table):
        if "_rowid" not in row_ids.column_names:
            raise ValueError(
                "query result has no '_rowid'; "
                "call .with_row_id(True) before .to_arrow()"
            )
        return row_ids["_rowid"].to_pylist()
    if isinstance(row_ids, (pa.Array, pa.ChunkedArray)):
        raise ValueError(
            "pass a query table with _rowid, not a column array "
            "(use fetch_blobs('image', hits), not fetch_blobs('image', hits['image']))"
        )
    return list(row_ids)


def _wrap_blob_files(handles) -> list[Optional[BlobFile]]:
    return [BlobFile(handle) if handle is not None else None for handle in handles]
