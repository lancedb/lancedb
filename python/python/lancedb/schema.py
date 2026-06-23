# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


"""Schema helpers for Lance blob columns."""

import pyarrow as pa

_BLOB_EXTENSION_NAME = "lance.blob.v2"
_BLOB_V1_KEY = "lance-encoding:blob"
_ARROW_EXT_NAME_KEY = "ARROW:extension:name"


class BlobType(pa.ExtensionType):
    """PyArrow extension type for a Lance blob v2 column.

    Queries return descriptors; call :meth:`~lancedb.table.Table.fetch_blobs`
    for bytes.
    """

    def __init__(self) -> None:
        storage_type = pa.struct(
            [
                pa.field("data", pa.large_binary(), nullable=True),
                pa.field("uri", pa.utf8(), nullable=True),
                pa.field("position", pa.uint64(), nullable=True),
                pa.field("size", pa.uint64(), nullable=True),
            ]
        )
        super().__init__(storage_type, _BLOB_EXTENSION_NAME)

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> "BlobType":
        return cls()

    def __reduce__(self):
        # Ensure pickle round-trips on older pyarrow (apache/arrow#35599).
        return type(self).__arrow_ext_deserialize__, (
            self.storage_type,
            self.__arrow_ext_serialize__(),
        )


try:
    pa.register_extension_type(BlobType())
except pa.ArrowKeyError:
    pass


def _metadata_value(metadata: dict, key: str):
    return metadata.get(key.encode()) or metadata.get(key)


def _metadata_marks_blob_v2(metadata: dict) -> bool:
    if not metadata:
        return False

    extension_name = _metadata_value(metadata, _ARROW_EXT_NAME_KEY)
    return extension_name in (_BLOB_EXTENSION_NAME, _BLOB_EXTENSION_NAME.encode())


def _metadata_marks_legacy_blob(metadata: dict) -> bool:
    if not metadata:
        return False

    return _metadata_value(metadata, _BLOB_V1_KEY) in ("true", b"true")


def is_blob_v2_field(field: pa.Field) -> bool:
    """Return True if `field` declares a blob v2 extension column."""
    field_type = field.type
    if (
        isinstance(field_type, pa.ExtensionType)
        and field_type.extension_name == _BLOB_EXTENSION_NAME
    ):
        return True
    return _metadata_marks_blob_v2(field.metadata or {})


def is_blob_like_field(field: pa.Field) -> bool:
    """Blob detection for ``to_pandas(blob_mode=...)`` and scanner paths only.

    Matches v2 extension fields on table schema, legacy ``lance-encoding:blob``
    storage columns, and v2 query descriptor fields (the engine tags those with
    the same metadata). Not used for fetch or auto ``_rowid``.
    """
    return is_blob_v2_field(field) or _metadata_marks_legacy_blob(field.metadata or {})


def _collect_blob_paths(schema: pa.Schema, is_blob) -> list[str]:
    paths: list[str] = []

    def walk(fields, prefix: str) -> None:
        for field in fields:
            path = f"{prefix}.{field.name}" if prefix else field.name
            if is_blob(field):
                paths.append(path)
            elif pa.types.is_struct(field.type):
                walk(field.type, path)
            elif (
                pa.types.is_list(field.type)
                or pa.types.is_large_list(field.type)
                or pa.types.is_fixed_size_list(field.type)
            ):
                walk([field.type.value_field], path)

    walk(schema, "")
    return paths


def blob_column_paths(schema: pa.Schema) -> list[str]:
    """Dotted paths of blob-like columns (v2 extension or legacy metadata)."""
    return _collect_blob_paths(schema, is_blob_like_field)


def blob_v2_column_paths(schema: pa.Schema) -> list[str]:
    return _collect_blob_paths(schema, is_blob_v2_field)


def schema_has_blob_field(schema: pa.Schema) -> bool:
    return bool(blob_column_paths(schema))


def blob(name: str, nullable: bool = True) -> pa.Field:
    """Create a Lance blob v2 column field."""
    return pa.field(name, BlobType(), nullable=nullable)


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
