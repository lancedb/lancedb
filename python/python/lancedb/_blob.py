# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Blob fetch API and v2 projection helpers."""

from __future__ import annotations

import io
from collections.abc import Awaitable, Callable, Iterable
from typing import TYPE_CHECKING, Optional, Union

import pyarrow as pa

from .expr import Expr
from .schema import blob_v2_column_paths
from .types import BlobMode, QueryProjection, QueryProjectionSpec
from .util import get_uri_scheme

if TYPE_CHECKING:
    from _typeshed import WriteableBuffer

    from .remote.table import RemoteTable
    from .table import AsyncTable, Table

BLOB_MODE_TO_HANDLING = {
    "lazy": "blobs_descriptions",
    "bytes": "all_binary",
    "descriptions": "blobs_descriptions",
}

ROW_ID_FIELD_NAME = "_lance_row_id"

FetchBlobsSync = Callable[[str, pa.Table], pa.Array | pa.ChunkedArray]
FetchBlobsAsync = Callable[[str, pa.Table], Awaitable[pa.Array | pa.ChunkedArray]]


class BlobFile(io.RawIOBase):
    """Seekable lazy handle from :meth:`~lancedb.table.Table.fetch_blob_files`.

    Bytes load on ``read`` or ``read_range``, not when the handle is opened.
    Use :meth:`aread` from async code.
    """

    def __init__(self, inner) -> None:
        self._inner = inner

    async def aread(self) -> bytes:
        return await self._inner.read()

    def close(self) -> None:
        self._inner.close()

    @property
    def closed(self) -> bool:
        return self._inner.is_closed()

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self._inner.seek(offset)
        elif whence == io.SEEK_CUR:
            self._inner.seek(self._inner.tell() + offset)
        elif whence == io.SEEK_END:
            self._inner.seek(self._inner.size() + offset)
        else:
            raise ValueError(f"invalid whence: {whence}")
        return self._inner.tell()

    def tell(self) -> int:
        return self._inner.tell()

    def size(self) -> int:
        return self._inner.size()

    def readall(self) -> bytes:
        return self._inner.read_bytes()

    def read(self, size: int = -1) -> bytes:
        if size == -1:
            return self._inner.read_bytes()
        return super().read(size)

    def read_range(self, offset: int, length: int) -> bytes:
        return self._inner.read_range(offset, length)

    def readinto(self, b: WriteableBuffer) -> int:
        view = memoryview(b).cast("B")
        chunk = self._inner.read_up_to(len(view))
        view[: len(chunk)] = chunk
        return len(chunk)

    def __repr__(self) -> str:
        return f"<BlobFile size={self.size()}>"


def validate_blob_mode(blob_mode: BlobMode) -> None:
    if blob_mode not in BLOB_MODE_TO_HANDLING:
        modes = ", ".join(repr(mode) for mode in BLOB_MODE_TO_HANDLING)
        raise ValueError(f"blob_mode must be one of {modes}, got {blob_mode!r}")


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
    blob_paths: Iterable[str] = (),
) -> pa.Table:
    if user_requested_row_id or not blob_auto_row_id:
        return tbl
    return stash_auto_row_ids(tbl, blob_paths)


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


def stash_auto_row_ids(tbl: pa.Table, blob_paths: Iterable[str]) -> pa.Table:
    if "_rowid" not in tbl.column_names:
        raise ValueError("query result has no '_rowid' column to hide")

    present_paths = [p for p in blob_paths if p.split(".")[0] in tbl.column_names]
    if not present_paths:
        raise ValueError("query result has no blob v2 column to carry a row id")

    row_ids = tbl["_rowid"]
    if isinstance(row_ids, pa.ChunkedArray):
        row_ids = row_ids.combine_chunks()
    row_ids = row_ids.cast(pa.uint64())

    for path in present_paths:
        tbl = _embed_row_id_in_column(tbl, path, row_ids)
    return tbl.drop_columns(["_rowid"])


def read_row_ids_from_hits(hits: pa.Table, blob_column: str) -> list[int]:
    if "_rowid" in hits.column_names:
        return hits["_rowid"].to_pylist()

    try:
        leaf = _leaf_struct_column(hits, blob_column)
        if ROW_ID_FIELD_NAME in leaf.type.names:
            return leaf.field(ROW_ID_FIELD_NAME).to_pylist()
    except KeyError:
        pass

    # blob_column is the source name; aliased projections use the output name in hits.
    row_ids = _find_row_id_in_any_column(hits)
    if row_ids is not None:
        return row_ids

    raise ValueError(
        f"query result has no '_rowid' column and no '{ROW_ID_FIELD_NAME}' "
        f"field on blob column '{blob_column}'. Pass fresh blob query "
        "results, call .with_row_id(True), or pass a list of row ids."
    )


def _find_row_id_in_any_column(tbl: pa.Table) -> Optional[list[int]]:
    for name in tbl.column_names:
        column = tbl.column(name)
        if isinstance(column, pa.ChunkedArray):
            column = column.combine_chunks()
        row_ids = _find_row_id_in_struct(column)
        if row_ids is not None:
            return row_ids
    return None


def _find_row_id_in_struct(array: pa.Array) -> Optional[list[int]]:
    if not pa.types.is_struct(array.type):
        return None
    if ROW_ID_FIELD_NAME in array.type.names:
        return array.field(ROW_ID_FIELD_NAME).to_pylist()
    for i in range(array.type.num_fields):
        row_ids = _find_row_id_in_struct(array.field(i))
        if row_ids is not None:
            return row_ids
    return None


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


def _set_blob_column(tbl: pa.Table, output_name: str, blobs: pa.Array) -> pa.Table:
    index = tbl.schema.get_field_index(output_name)
    return tbl.set_column(index, pa.field(output_name, blobs.type), [blobs])


def _embed_row_id_in_column(tbl: pa.Table, path: str, row_ids: pa.Array) -> pa.Table:
    def add_row_id(children: list, child_fields: list) -> None:
        children.append(row_ids)
        child_fields.append(pa.field(ROW_ID_FIELD_NAME, pa.uint64(), nullable=False))

    return _transform_struct_column(tbl, path, add_row_id)


def strip_auto_row_ids(tbl: pa.Table, blob_paths: Iterable[str]) -> pa.Table:
    """Remove any `_lance_row_id` field embedded in blob descriptor structs.

    For read-only descriptor views (`blob_mode="descriptions"`) that never
    fetch bytes, so have no use for the row id.
    """

    def drop_row_id(children: list, child_fields: list) -> None:
        for i, field in enumerate(child_fields):
            if field.name == ROW_ID_FIELD_NAME:
                del children[i], child_fields[i]
                return

    for path in blob_paths:
        if path.split(".")[0] not in tbl.column_names:
            continue
        tbl = _transform_struct_column(tbl, path, drop_row_id)
    return tbl


def _transform_struct_column(
    tbl: pa.Table, path: str, leaf_transform: Callable[[list, list], None]
) -> pa.Table:
    top_name, *rest = path.split(".")
    top_index = tbl.schema.get_field_index(top_name)
    top_field = tbl.schema.field(top_index)
    top_array = tbl.column(top_name)
    if isinstance(top_array, pa.ChunkedArray):
        top_array = top_array.combine_chunks()

    new_array, new_field = _rebuild_struct(top_array, top_field, rest, leaf_transform)
    return tbl.set_column(top_index, new_field, new_array)


def _rebuild_struct(
    struct_array: pa.StructArray,
    struct_field: pa.Field,
    remaining_path: list[str],
    leaf_transform: Callable[[list, list], None],
) -> tuple[pa.StructArray, pa.Field]:
    null_mask = struct_array.is_null()
    if not remaining_path:
        children = [struct_array.field(i) for i in range(struct_array.type.num_fields)]
        child_fields = list(struct_array.type)
        leaf_transform(children, child_fields)
        new_array = pa.StructArray.from_arrays(
            children, fields=child_fields, mask=null_mask
        )
    else:
        child_name = remaining_path[0]
        child_index = struct_array.type.get_field_index(child_name)
        child_array = struct_array.field(child_index)
        child_field = struct_array.type.field(child_index)
        new_child_array, new_child_field = _rebuild_struct(
            child_array, child_field, remaining_path[1:], leaf_transform
        )

        children = []
        child_fields = []
        for i in range(struct_array.type.num_fields):
            field = struct_array.type.field(i)
            if field.name == child_name:
                children.append(new_child_array)
                child_fields.append(new_child_field)
            else:
                children.append(struct_array.field(i))
                child_fields.append(field)
        new_array = pa.StructArray.from_arrays(
            children, fields=child_fields, mask=null_mask
        )

    new_field = pa.field(
        struct_field.name,
        new_array.type,
        nullable=struct_field.nullable,
        metadata=struct_field.metadata,
    )
    return new_array, new_field


def _leaf_struct_column(tbl: pa.Table, path: str) -> pa.StructArray:
    parts = path.split(".")
    column = tbl.column(parts[0])
    if isinstance(column, pa.ChunkedArray):
        column = column.combine_chunks()
    for part in parts[1:]:
        column = column.field(part)
    return column


def _normalize_blob_row_ids(
    row_ids: Union[list[int], pa.Table], blob_column: str
) -> list[int]:
    if isinstance(row_ids, pa.Table):
        return read_row_ids_from_hits(row_ids, blob_column)
    if isinstance(row_ids, (pa.Array, pa.ChunkedArray)):
        raise ValueError(
            "pass a query table with _rowid, not a column array "
            "(use fetch_blobs('image', hits), not fetch_blobs('image', hits['image']))"
        )
    return list(row_ids)


def _wrap_blob_files(handles: Iterable[object]) -> list[Optional[BlobFile]]:
    return [BlobFile(handle) if handle is not None else None for handle in handles]
