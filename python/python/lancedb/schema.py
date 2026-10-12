# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


"""Schema helpers for Lance blob columns."""

import importlib
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.ipc

if TYPE_CHECKING:
    from lance.blob import BlobType as BlobType

_BLOB_EXTENSION_NAME = "lance.blob.v2"
_BLOB_V1_KEY = "lance-encoding:blob"
_ARROW_EXT_NAME_KEY = "ARROW:extension:name"
_BLOB_V2_STORAGE_TYPE = pa.struct(
    [
        pa.field("data", pa.large_binary(), nullable=True),
        pa.field("uri", pa.utf8(), nullable=True),
        pa.field("position", pa.uint64(), nullable=True),
        pa.field("size", pa.uint64(), nullable=True),
    ]
)
_resolved_blob_type = None


class _FallbackBlobType(pa.ExtensionType):
    """lance.blob.v2 extension type used when pylance is not installed."""

    def __init__(self) -> None:
        pa.ExtensionType.__init__(self, _BLOB_V2_STORAGE_TYPE, _BLOB_EXTENSION_NAME)

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> "_FallbackBlobType":
        return cls()

    def __reduce__(self):
        return type(self).__arrow_ext_deserialize__, (
            self.storage_type,
            self.__arrow_ext_serialize__(),
        )


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


def _collect_blob_paths(schema: pa.Schema, is_blob) -> list[tuple[str, bool]]:
    """Walk the schema and return (path, has_list_ancestor) for each blob field."""
    paths: list[tuple[str, bool]] = []

    def walk(fields, prefix: str, has_list_ancestor: bool) -> None:
        for field in fields:
            path = f"{prefix}.{field.name}" if prefix else field.name
            if is_blob(field):
                paths.append((path, has_list_ancestor))
            elif pa.types.is_struct(field.type):
                walk(field.type, path, has_list_ancestor)
            elif (
                pa.types.is_list(field.type)
                or pa.types.is_large_list(field.type)
                or pa.types.is_fixed_size_list(field.type)
            ):
                walk([field.type.value_field], path, True)

    walk(schema, "", False)
    return paths


def blob_column_paths(schema: pa.Schema) -> list[str]:
    """Dotted paths of blob-like columns (v2 extension or legacy metadata)."""
    return [path for path, _ in _collect_blob_paths(schema, is_blob_like_field)]


def blob_v2_column_paths(schema: pa.Schema) -> list[str]:
    return [path for path, _ in _collect_blob_paths(schema, is_blob_v2_field)]


def row_addressable_blob_v2_paths(schema: pa.Schema) -> list[str]:
    """Blob v2 paths with one blob addressable by table row id.

    ``fetch_blobs`` and the descriptor row-id ride-along address one blob per
    row, so a blob inside a list container has no row-id slot and no fetch
    path. Those columns still store and query as raw descriptors.
    """
    return [
        path
        for path, has_list_ancestor in _collect_blob_paths(schema, is_blob_v2_field)
        if not has_list_ancestor
    ]


def schema_has_blob_field(schema: pa.Schema) -> bool:
    return bool(blob_column_paths(schema))


def _deserialize_registered_type(extension_type: pa.ExtensionType) -> pa.DataType:
    """Return the type Arrow reconstructs for this extension name."""
    schema = pa.schema([pa.field("value", extension_type)])
    restored = pa.ipc.read_schema(schema.serialize())
    return restored.field("value").type


def _resolve_blob_type():
    """Return the BlobType class this process should use.

    pylance's class when it owns the lance.blob.v2 registry entry,
    otherwise LanceDB's fallback. A different registered class is an error.
    """
    global _resolved_blob_type
    if _resolved_blob_type is not None:
        return _resolved_blob_type
    try:
        blob_module = importlib.import_module("lance.blob")
    except ModuleNotFoundError as err:
        if err.name not in ("lance", "lance.blob"):
            raise
    else:
        blob_type = getattr(blob_module, "BlobType", None)
        if blob_type is not None:
            registered_type = _deserialize_registered_type(blob_type())
            if type(registered_type) is not blob_type:
                registered_cls = type(registered_type)
                raise ValueError(
                    "lance.blob.v2 is already registered by "
                    f"{registered_cls.__module__}.{registered_cls.__qualname__}"
                )
            _resolved_blob_type = blob_type
            return blob_type
    try:
        pa.register_extension_type(_FallbackBlobType())  # type: ignore[arg-type]
    except pa.ArrowKeyError as err:
        raise ValueError(
            "lance.blob.v2 is already registered by another extension class"
        ) from err
    _resolved_blob_type = _FallbackBlobType
    return _resolved_blob_type


def blob(name: str, nullable: bool = True) -> pa.Field:
    """Create a Lance blob v2 column field.

    When pylance is installed this is ``lance.blob.BlobType``.
    """
    blob_type = _resolve_blob_type()
    return pa.field(name, blob_type(), nullable=nullable)


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


def __getattr__(name: str):
    if name == "BlobType":
        blob_type = _resolve_blob_type()
        globals()["BlobType"] = blob_type
        return blob_type
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
