# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Blob fetch API and v2 projection helpers."""

from __future__ import annotations

import io
import operator
from collections.abc import Awaitable, Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Optional, Union

import pyarrow as pa

from .expr import Expr
from .schema import blob_column_paths, row_addressable_blob_v2_paths
from .types import BlobMode, QueryProjection, QueryProjectionSpec

if TYPE_CHECKING:
    from _typeshed import WriteableBuffer

BLOB_MODE_TO_HANDLING = {
    "lazy": "blobs_descriptions",
    "bytes": "all_binary",
    "descriptions": "blobs_descriptions",
}

ROW_ID_FIELD_NAME = "_lance_row_id"

FetchBlobsSync = Callable[[str, list[int]], pa.Array | pa.ChunkedArray]
FetchBlobsAsync = Callable[[str, list[int]], Awaitable[pa.Array | pa.ChunkedArray]]


def _normalize_blob_descriptor(value):
    if isinstance(value, pa.Scalar):
        value = value.as_py()
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError(
            "blob descriptor must be a mapping, an Arrow struct scalar, or None"
        )
    return dict(value)


class BlobFile(io.RawIOBase):
    """Seekable lazy handle from :meth:`~lancedb.table.Table.fetch_blob_files`.

    Bytes load on ``read``, ``read1``, ``read_range``, or ``read_ranges``, not
    when the handle is opened. Range reads leave the cursor unchanged. Use
    :meth:`aread` from async code.
    """

    def __init__(self, inner) -> None:
        self._inner = inner

    async def aread(self) -> bytes:
        if self.closed:
            raise ValueError("read of closed file")
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
            position = offset
        elif whence == io.SEEK_CUR:
            position = self._inner.tell() + offset
        elif whence == io.SEEK_END:
            position = self._inner.size() + offset
        else:
            raise ValueError(f"invalid whence: {whence}")
        if position < 0:
            raise ValueError(f"negative seek value {position}")
        self._inner.seek(position)
        return self._inner.tell()

    def tell(self) -> int:
        return self._inner.tell()

    def size(self) -> int:
        return self._inner.size()

    def readall(self) -> bytes:
        if self.closed:
            raise ValueError("read of closed file")
        return self._inner.read_bytes()

    def read(self, size: Optional[int] = -1) -> bytes:
        if self.closed:
            raise ValueError("read of closed file")
        if size is None or size < 0:
            return self._inner.read_bytes()
        return super().read(size)

    def read1(self, size: Optional[int] = -1) -> bytes:
        """Read up to ``size`` bytes with one underlying read."""
        if self.closed:
            raise ValueError("read of closed file")
        if size is None or size < 0:
            size = io.DEFAULT_BUFFER_SIZE
        return self._inner.read_up_to(size)

    def read_range(self, offset: int, length: int) -> bytes:
        if self.closed:
            raise ValueError("read of closed file")
        return self._inner.read_range(offset, length)

    def read_ranges(self, ranges: list[tuple[int, int]]) -> list[bytes]:
        """Read ``(offset, length)`` pairs without moving the cursor."""
        if self.closed:
            raise ValueError("read of closed file")
        return self._inner.read_ranges(ranges)

    def readinto(self, b: WriteableBuffer) -> int:
        if self.closed:
            raise ValueError("readinto of closed file")
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


def projection_includes_blob_column(
    projection: QueryProjection,
    blob_columns: Iterable[str],
) -> bool:
    columns = set(blob_columns)
    if not columns:
        return False
    if not projection:
        return True
    for output, source in _iter_projection_pairs(projection):
        if output in columns or any(
            column == source or column.startswith(f"{source}.") for column in columns
        ):
            return True
    return False


def blob_v2_projection_sources(
    schema: pa.Schema,
    projection: QueryProjection,
) -> dict[str, str]:
    return _project_blob_paths(row_addressable_blob_v2_paths(schema), projection)


def reject_unfetchable_blob_columns(
    schema: pa.Schema, projection: QueryProjection, blob_sources: dict[str, str]
) -> None:
    # Rerankers can drop field metadata, so validate against the source schema.
    unfetchable = [
        output
        for output in _project_blob_paths(blob_column_paths(schema), projection)
        if output not in blob_sources
    ]
    if unfetchable:
        raise ValueError(
            f"blob_mode='bytes' cannot fetch blob columns {unfetchable}. Bytes are "
            "fetched by row id, which covers blob v2 columns with one blob per "
            "row but not legacy blob columns or blobs inside lists. Use "
            "blob_mode='descriptions' or remove these columns from the projection."
        )


def reject_unmatched_blob_outputs(
    output_schema: pa.Schema, blob_sources: dict[str, str]
) -> None:
    # Another spelling of a blob column, such as a quoted or recased name, is a
    # blob in the query's output schema but maps to no source by name.
    unmatched = [
        path for path in blob_column_paths(output_schema) if path not in blob_sources
    ]
    if unmatched:
        raise ValueError(
            f"blob_mode='bytes' could not match result columns {unmatched} to a blob "
            "column of the table. Select blob columns by their plain names, or "
            "use blob_mode='descriptions'."
        )


def _project_blob_paths(
    blob_columns: list[str], projection: QueryProjection
) -> dict[str, str]:
    """Map each projected output path to the blob column path it reads."""
    # An empty projection selects all columns.
    if not projection:
        return {column: column for column in blob_columns}
    sources: dict[str, str] = {}
    entries: dict[str, str] = {}
    for output, source in _iter_projection_pairs(projection):
        for column in blob_columns:
            if column == source:
                path = output
            elif column.startswith(f"{source}."):
                path = f"{output}{column[len(source) :]}"
            else:
                continue
            if path in sources:
                raise ValueError(
                    f"projection entries {entries[path]!r} and {output!r} both "
                    f"produce blob column '{path}'. Rename one of them or drop "
                    "the redundant selection."
                )
            sources[path] = column
            entries[path] = output
    return sources


def v2_projection_needs_row_id(
    schema: pa.Schema,
    projection: QueryProjection,
    *,
    with_row_id: bool,
) -> bool:
    if with_row_id:
        return False
    return projection_includes_blob_column(
        projection, row_addressable_blob_v2_paths(schema)
    )


def blob_auto_row_id_for_scan(
    schema: pa.Schema,
    projection: QueryProjection,
    *,
    with_row_id: bool | None,
) -> bool:
    """Auto row-id only applies when the caller said nothing about row ids."""
    if with_row_id is not None:
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
    if "_rowid" not in tbl.column_names:
        # A backend that ignores the row-id request leaves nothing to stash. Hand
        # back the projection as-is so fetch_blobs raises the error that names the
        # ways to supply row ids, rather than failing here about a hidden column.
        return tbl
    return stash_auto_row_ids(tbl, blob_paths)


async def replace_v2_blob_columns_with_bytes(
    tbl: pa.Table,
    blob_sources: dict[str, str],
    fetch_blobs: FetchBlobsAsync,
) -> pa.Table:
    present = _present_blob_sources(tbl, blob_sources)
    if not present:
        return tbl
    row_ids = _hit_row_ids(tbl, present)
    for output_name, source_name in present.items():
        tbl = _set_blob_column(
            tbl,
            output_name,
            await fetch_blobs(source_name, row_ids)
            if row_ids
            else pa.array([], type=pa.large_binary()),
        )
    return tbl


def replace_v2_blob_columns_with_bytes_sync(
    tbl: pa.Table,
    blob_sources: dict[str, str],
    fetch_blobs: FetchBlobsSync,
) -> pa.Table:
    present = _present_blob_sources(tbl, blob_sources)
    if not present:
        return tbl
    row_ids = _hit_row_ids(tbl, present)
    for output_name, source_name in present.items():
        tbl = _set_blob_column(
            tbl,
            output_name,
            fetch_blobs(source_name, row_ids)
            if row_ids
            else pa.array([], type=pa.large_binary()),
        )
    return tbl


def _present_blob_sources(
    tbl: pa.Table, blob_sources: dict[str, str]
) -> dict[str, str]:
    return {
        output: source
        for output, source in blob_sources.items()
        if _has_column_path(tbl.schema, output)
    }


def _hit_row_ids(tbl: pa.Table, blob_sources: dict[str, str]) -> list[int]:
    """Read row ids before replacing columns, since a swapped alias can overwrite
    the lookup source."""
    if "_rowid" in tbl.column_names:
        return tbl["_rowid"].to_pylist()
    for output in blob_sources:
        descriptor = _leaf_struct_column(tbl, output)
        if pa.types.is_struct(descriptor.type) and (
            ROW_ID_FIELD_NAME in descriptor.type.names
        ):
            return descriptor.field(ROW_ID_FIELD_NAME).to_pylist()
    raise ValueError(
        f"query result has no '_rowid' column and no '{ROW_ID_FIELD_NAME}' "
        f"field on blob columns {list(blob_sources)}. Call .with_row_id(True) "
        "to fetch blob bytes for this query."
    )


def stash_auto_row_ids(tbl: pa.Table, blob_paths: Iterable[str]) -> pa.Table:
    if "_rowid" not in tbl.column_names:
        raise ValueError("query result has no '_rowid' column to hide")

    present_paths = [p for p in blob_paths if _has_column_path(tbl.schema, p)]
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
        if pa.types.is_struct(leaf.type) and ROW_ID_FIELD_NAME in leaf.type.names:
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
                source = expr._column_name()
                yield name, source if source is not None else expr.to_sql()
        return
    for column in projection:
        if isinstance(column, str):
            yield column, column
        elif isinstance(column, tuple) and len(column) == 2:
            name, expr = column
            if isinstance(expr, str):
                yield name, expr
            elif isinstance(expr, Expr):
                source = expr._column_name()
                yield name, source if source is not None else expr.to_sql()


def _has_column_path(schema: pa.Schema, path: str) -> bool:
    top_name, rest = _split_column_path(schema, path)
    index = schema.get_field_index(top_name)
    return (
        index >= 0 and _walk_struct_fields(schema.field(index).type, rest) is not None
    )


def _set_blob_column(
    tbl: pa.Table, output_name: str, blobs: pa.Array | pa.ChunkedArray
) -> pa.Table:
    top_name, rest = _split_column_path(tbl.schema, output_name)
    if not rest:
        index = tbl.schema.get_field_index(top_name)
        return tbl.set_column(index, pa.field(top_name, blobs.type), [blobs])

    leaf_name = rest[-1]
    if isinstance(blobs, pa.ChunkedArray):
        blobs = blobs.combine_chunks()

    def replace_blob(children: list, child_fields: list) -> None:
        for i, field in enumerate(child_fields):
            if field.name == leaf_name:
                children[i] = blobs
                child_fields[i] = pa.field(
                    leaf_name, blobs.type, nullable=field.nullable
                )
                return
        raise KeyError(f"Blob field '{leaf_name}' not found in '{output_name}'")

    return _transform_struct_column(tbl, top_name, rest[:-1], replace_blob)


def _embed_row_id_in_column(tbl: pa.Table, path: str, row_ids: pa.Array) -> pa.Table:
    def add_row_id(children: list, child_fields: list) -> None:
        children.append(row_ids)
        child_fields.append(pa.field(ROW_ID_FIELD_NAME, pa.uint64(), nullable=False))

    return _transform_struct_column(
        tbl, *_split_column_path(tbl.schema, path), add_row_id
    )


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
        if not _has_column_path(tbl.schema, path):
            continue
        tbl = _transform_struct_column(
            tbl, *_split_column_path(tbl.schema, path), drop_row_id
        )
    return tbl


def _transform_struct_column(
    tbl: pa.Table,
    top_name: str,
    rest: list[str],
    leaf_transform: Callable[[list, list], None],
) -> pa.Table:
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
    top_name, rest = _split_column_path(tbl.schema, path)
    column = tbl.column(top_name)
    if isinstance(column, pa.ChunkedArray):
        column = column.combine_chunks()
    for part in rest:
        if not pa.types.is_struct(column.type):
            raise KeyError(f"blob path '{path}' crosses non-struct column")
        column = column.field(part)
    return column


def _split_column_path(schema: pa.Schema, path: str) -> tuple[str, list[str]]:
    """Prefer a path ending in a struct, since a flat alias can shadow a nested
    blob path."""
    parts = path.split(".")
    fallback = None
    for cut in range(len(parts), 0, -1):
        top_name, rest = ".".join(parts[:cut]), parts[cut:]
        index = schema.get_field_index(top_name)
        if index < 0:
            continue
        leaf_type = _walk_struct_fields(schema.field(index).type, rest)
        if leaf_type is None:
            continue
        if pa.types.is_struct(leaf_type):
            return top_name, rest
        fallback = fallback or (top_name, rest)
    return fallback or (parts[0], parts[1:])


def _walk_struct_fields(
    data_type: pa.DataType, names: list[str]
) -> Optional[pa.DataType]:
    for name in names:
        if not pa.types.is_struct(data_type):
            return None
        index = data_type.get_field_index(name)
        if index < 0:
            return None
        data_type = data_type.field(index).type
    return data_type


def _normalize_blob_row_ids(
    row_ids: Union[list[int], pa.Array, pa.ChunkedArray, pa.Table], blob_column: str
) -> list[int]:
    if isinstance(row_ids, pa.Table):
        values = read_row_ids_from_hits(row_ids, blob_column)
    elif isinstance(row_ids, (pa.Array, pa.ChunkedArray)):
        if not pa.types.is_integer(row_ids.type):
            raise ValueError(
                "row id arrays must have an integer type; pass a query table with "
                "_rowid when using a blob descriptor column"
            )
        values = row_ids.to_pylist()
    else:
        values = list(row_ids)

    normalized = []
    for value in values:
        try:
            row_id = operator.index(value)
        except TypeError as exc:
            raise ValueError("row ids must be integers without nulls") from exc
        if row_id < 0:
            raise ValueError("row ids must be non-negative")
        normalized.append(row_id)
    return normalized


def _wrap_blob_files(handles: Iterable[object]) -> list[Optional[BlobFile]]:
    return [BlobFile(handle) if handle is not None else None for handle in handles]
