# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Blob fetch API and v2 projection helpers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from typing import TYPE_CHECKING, Optional, Union

import pyarrow as pa

from .expr import Expr
from .schema import blob_v2_column_paths
from .types import BlobMode, QueryProjection, QueryProjectionSpec
from .util import get_uri_scheme

if TYPE_CHECKING:
    from .remote.table import RemoteTable
    from .table import AsyncTable, Table

BLOB_MODE_TO_HANDLING = {
    "lazy": "blobs_descriptions",
    "bytes": "all_binary",
    "descriptions": "blobs_descriptions",
}

_AUTO_ROWID_METADATA_KEY = b"lancedb._rowid"

FetchBlobsSync = Callable[[str, pa.Table], pa.Array | pa.ChunkedArray]
FetchBlobsAsync = Callable[[str, pa.Table], Awaitable[pa.Array | pa.ChunkedArray]]


def validate_blob_mode(blob_mode: BlobMode) -> None:
    if blob_mode not in BLOB_MODE_TO_HANDLING:
        modes = ", ".join(repr(mode) for mode in BLOB_MODE_TO_HANDLING)
        raise ValueError(f"blob_mode must be one of {modes}, got {blob_mode!r}")


class BlobFile:
    """Lazy handle from :meth:`lancedb.table.Table.fetch_blob_files`.

    Call ``read`` from sync code, ``aread`` from async code.
    """

    def __init__(self, inner) -> None:
        self._inner = inner

    async def aread(self) -> bytes:
        return await self._inner.read()

    def read(self) -> bytes:
        return self._inner.read_bytes()


def supports_blob_auto_row_id(table: Table | AsyncTable | RemoteTable) -> bool:
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


def _iter_projection_pairs(
    projection: QueryProjectionSpec,
) -> Iterable[tuple[str, str]]:
    if isinstance(projection, dict):
        for name, expr in projection.items():
            if isinstance(expr, str):
                yield name, expr
            elif isinstance(expr, Expr):
                yield name, expr.to_sql()
        return
    for column in projection:
        if isinstance(column, str):
            yield column, column
        elif isinstance(column, tuple) and len(column) == 2:
            name, expr = column
            if isinstance(expr, str):
                yield name, expr
            elif isinstance(expr, Expr):
                yield name, expr.to_sql()


def projection_includes_blob_column(
    projection: QueryProjection,
    blob_columns: Iterable[str],
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


def blob_v2_projection_sources(
    schema: pa.Schema,
    projection: QueryProjection,
) -> dict[str, str]:
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
    schema: pa.Schema,
    projection: QueryProjection,
    *,
    with_row_id: bool,
) -> bool:
    if with_row_id:
        return False
    return projection_includes_blob_column(projection, blob_v2_column_paths(schema))


def blob_auto_row_id_for_scan(
    table: Table | AsyncTable | RemoteTable,
    schema: pa.Schema,
    projection: QueryProjection,
    *,
    with_row_id: bool | None,
) -> bool:
    if with_row_id is not None:
        return False
    if not supports_blob_auto_row_id(table):
        return False
    return v2_projection_needs_row_id(schema, projection, with_row_id=False)


def finalize_blob_query_table(
    tbl: pa.Table,
    *,
    user_requested_row_id: bool,
    blob_auto_row_id: bool,
) -> pa.Table:
    if user_requested_row_id or not blob_auto_row_id:
        return tbl
    return stash_auto_row_ids(tbl)


def _set_blob_column(tbl: pa.Table, output_name: str, blobs: pa.Array) -> pa.Table:
    index = tbl.schema.get_field_index(output_name)
    return tbl.set_column(index, pa.field(output_name, blobs.type), [blobs])


async def replace_v2_blob_columns_with_bytes(
    tbl: pa.Table,
    blob_sources: dict[str, str],
    fetch_blobs: FetchBlobsAsync,
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
    fetch_blobs: FetchBlobsSync,
) -> pa.Table:
    for output_name, source_name in blob_sources.items():
        if output_name not in tbl.column_names:
            continue
        blobs = fetch_blobs(source_name, tbl)
        tbl = _set_blob_column(tbl, output_name, blobs)
    return tbl


def _serialize_auto_row_ids(row_ids: pa.Array | pa.ChunkedArray) -> bytes:
    if isinstance(row_ids, pa.ChunkedArray):
        row_ids = row_ids.combine_chunks()
    if row_ids.type != pa.uint64():
        row_ids = row_ids.cast(pa.uint64())

    batch = pa.record_batch([row_ids], names=["_rowid"])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _deserialize_auto_row_ids(raw: bytes) -> list[int]:
    with pa.ipc.open_stream(pa.BufferReader(raw)) as reader:
        table = reader.read_all()
    if table.num_columns != 1 or table.column_names[0] != "_rowid":
        raise ValueError("query result has invalid hidden _rowid metadata")
    return table["_rowid"].to_pylist()


def stash_auto_row_ids(tbl: pa.Table) -> pa.Table:
    if "_rowid" not in tbl.column_names:
        raise ValueError("query result has no '_rowid' column to hide")

    raw = _serialize_auto_row_ids(tbl["_rowid"])
    tbl = tbl.drop_columns(["_rowid"])
    metadata = dict(tbl.schema.metadata or {})
    metadata[_AUTO_ROWID_METADATA_KEY] = raw
    return tbl.replace_schema_metadata(metadata)


def read_row_ids_from_hits(hits: pa.Table) -> list[int]:
    if "_rowid" in hits.column_names:
        return hits["_rowid"].to_pylist()

    metadata = hits.schema.metadata or {}
    raw = metadata.get(_AUTO_ROWID_METADATA_KEY)
    if raw is None:
        raise ValueError(
            "query result has no '_rowid' column or hidden row-id metadata; "
            "pass fresh blob query results, call .with_row_id(True), or pass "
            "a list of row ids"
        )

    row_ids = _deserialize_auto_row_ids(raw)
    if len(row_ids) != hits.num_rows:
        raise ValueError("query result hidden _rowid metadata has the wrong length")
    return row_ids


def _normalize_blob_row_ids(row_ids: Union[list[int], pa.Table]) -> list[int]:
    if isinstance(row_ids, pa.Table):
        return read_row_ids_from_hits(row_ids)
    if isinstance(row_ids, (pa.Array, pa.ChunkedArray)):
        raise ValueError(
            "pass a query table with _rowid, not a column array "
            "(use fetch_blobs('image', hits), not fetch_blobs('image', hits['image']))"
        )
    return list(row_ids)


def _wrap_blob_files(handles: Iterable[object]) -> list[Optional[BlobFile]]:
    return [BlobFile(handle) if handle is not None else None for handle in handles]
