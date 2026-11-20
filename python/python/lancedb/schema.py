# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


"""Schema helpers for Lance blob columns."""

import ctypes
import importlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Optional

import pyarrow as pa
import pyarrow.ipc

if TYPE_CHECKING:
    from lance.blob import BlobType as BlobType

_BLOB_EXTENSION_NAME = "lance.blob.v2"
_BLOB_V1_KEY = "lance-encoding:blob"
_ARROW_EXT_NAME_KEY = "ARROW:extension:name"
_MAX_RUST_USIZE = ctypes.c_size_t(-1).value
_BLOB_V2_STORAGE_TYPE = pa.struct(
    [
        pa.field("data", pa.large_binary(), nullable=True),
        pa.field("uri", pa.utf8(), nullable=True),
        pa.field("position", pa.uint64(), nullable=True),
        pa.field("size", pa.uint64(), nullable=True),
    ]
)
_resolved_blob_type = None
_UINT32_MAX = 0xFFFF_FFFF
_UINT64_MAX = 0xFFFF_FFFF_FFFF_FFFF


@dataclass(frozen=True)
class BasePath:
    """A storage base registered on a table at creation time.

    Use with the ``initial_bases`` argument of
    [create_table][lancedb.db.DBConnection.create_table] to make
    external blob URIs resolve under a shared prefix: descriptors that fall
    under ``path`` are stored relative to it instead of as absolute URIs.

    This is a lightweight stand-in for ``lance.DatasetBasePath`` that does
    not require pylance. Both classes, and equivalent dictionaries, are
    accepted by the create APIs.

    Parameters
    ----------
    path:
        Full URI of the base (for example ``s3://bucket/prefix`` or a
        ``file://`` URI). Relative blob URIs are resolved against it.
    name:
        Optional human-readable name for the base.
    is_dataset_root:
        Whether the base uses the dataset-root layout. Defaults to ``False``.
        Bases registered here must leave this ``False``: Lance only resolves
        external blob URIs against non-root bases.
    id:
        Base ID used by scoped storage options such as ``base_<id>.region``.
        ``None`` lets Lance assign a non-zero ID. ``0`` is reserved for
        absolute (unregistered) URIs and is rejected.
    """

    path: str
    name: Optional[str] = None
    is_dataset_root: bool = False
    id: Optional[int] = None

    def __post_init__(self) -> None:
        if not isinstance(self.path, str):
            raise TypeError(f"path must be a string, got {type(self.path).__name__}")
        if not self.path:
            raise ValueError("path must not be empty")
        if self.name is not None and not isinstance(self.name, str):
            raise TypeError(f"name must be a string, got {type(self.name).__name__}")
        if not isinstance(self.is_dataset_root, bool):
            raise TypeError(
                "is_dataset_root must be a bool, got "
                f"{type(self.is_dataset_root).__name__}"
            )
        if self.id is None:
            return
        if isinstance(self.id, bool) or not isinstance(self.id, int):
            raise TypeError(f"id must be an int or None, got {type(self.id).__name__}")
        if self.id == 0:
            raise ValueError(
                "id=0 is reserved for unregistered blob URIs; "
                "pass id=None to let Lance assign an ID"
            )
        if self.id < 0:
            raise ValueError("id must be a positive integer")
        if self.id > _UINT32_MAX:
            raise ValueError("id must fit in a 32-bit unsigned integer")


class _FallbackBlobType(pa.ExtensionType):
    """lance.blob.v2 extension type used when pylance is not installed."""

    def __init__(self, storage_type: pa.DataType = _BLOB_V2_STORAGE_TYPE) -> None:
        pa.ExtensionType.__init__(self, storage_type, _BLOB_EXTENSION_NAME)

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> "_FallbackBlobType":
        return cls(storage_type)

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


def row_addressable_blob_paths(schema: pa.Schema) -> list[str]:
    """Blob v1 and v2 paths with one blob addressable by table row id."""
    return [
        path
        for path, has_list_ancestor in _collect_blob_paths(schema, is_blob_like_field)
        if not has_list_ancestor
    ]


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


def _restore_blob_extensions(schema: pa.Schema) -> pa.Schema:
    """Restore blob fields read before their extension type was registered."""
    unrestored = _collect_blob_paths(
        schema,
        lambda field: (
            not isinstance(field.type, pa.ExtensionType)
            and _metadata_marks_blob_v2(field.metadata or {})
        ),
    )
    if not unrestored:
        return schema

    _resolve_blob_type()
    # Arrow's deserializer uses the registered class and preserves nested fields,
    # storage layouts, nullability, and unrelated metadata. Only the schema is read.
    return pa.ipc.read_schema(schema.serialize())


def blob(
    name: str,
    nullable: bool = True,
    *,
    inline_size_threshold: Optional[int] = None,
    dedicated_size_threshold: Optional[int] = None,
    pack_file_size_threshold: Optional[int] = None,
) -> pa.Field:
    """Create a Lance blob v2 column field.

    When pylance is installed this is ``lance.blob.BlobType``.

    Parameters
    ----------
    name:
        Field name.
    nullable:
        Whether the column accepts null values.
    inline_size_threshold:
        Maximum payload size in bytes to keep inline in the data file
        before using packed blob storage. Zero is allowed.
    dedicated_size_threshold:
        Maximum payload size in bytes to store in packed blob storage
        before using dedicated blob storage. Must be positive.
    pack_file_size_threshold:
        Maximum size in bytes of a single packed blob sidecar file before
        a new one is started. Must be positive.

    Notes
    -----
    Thresholds are validated and stored as Arrow field metadata, with the
    same format as ``lance.blob_field``. They do not require pylance.

    The extension's storage is a struct with nullable fields ``data``
    (large_binary), ``uri`` (string), ``position`` (uint64), and ``size``
    (uint64). Each non-null blob must set exactly one of ``data`` and ``uri``:

    - Inline bytes use ``data``, leaving all other fields null. ``b""`` is a
      valid empty blob; use a null struct for a null blob.
    - External blobs use ``uri``, leaving ``data`` null. Set both ``position``
      and ``size`` to select a byte range, or leave both null for the whole
      object. A range's ``size`` must be greater than zero.

    Use [blob_array][lancedb.schema.blob_array] to build inline blob arrays
    for Arrow record batches. For external descriptors, build the struct
    array with ``field.type.storage_type`` and wrap it with
    ``pa.ExtensionArray.from_storage(field.type, storage)``.

    ``create_table(..., schema=...)`` casts plain binary values into blob
    columns declared by this field, and ``table.add(...)`` does the same
    using the table's schema. PyArrow's ``RecordBatch.from_pylist`` does not
    perform this conversion; construct blob arrays explicitly instead.
    """
    metadata = {}
    for threshold, value, allow_zero, key in (
        (
            "inline_size_threshold",
            inline_size_threshold,
            True,
            "lance-encoding:blob-inline-size-threshold",
        ),
        (
            "dedicated_size_threshold",
            dedicated_size_threshold,
            False,
            "lance-encoding:blob-dedicated-size-threshold",
        ),
        (
            "pack_file_size_threshold",
            pack_file_size_threshold,
            False,
            "lance-encoding:blob-pack-file-size-threshold",
        ),
    ):
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{threshold} must be an int, got {type(value).__name__}")
        if value < 0 or (value == 0 and not allow_zero):
            bound = "non-negative" if allow_zero else "positive"
            raise ValueError(f"{threshold} must be {bound}")
        if value > _MAX_RUST_USIZE:
            raise OverflowError(f"{threshold} must fit in a Rust usize")
        metadata[key] = str(value)

    blob_type = _resolve_blob_type()
    field = pa.field(name, blob_type(), nullable=nullable)
    return field.with_metadata(metadata) if metadata else field


def blob_array(payloads: Iterable[Optional[bytes]]) -> pa.ExtensionArray:
    """Build a Lance blob v2 extension array from inline bytes.

    Uses the same extension type as [blob][lancedb.schema.blob], with or
    without pylance installed.

    Parameters
    ----------
    payloads:
        An iterable of ``bytes`` or ``None``. ``None`` creates a null blob;
        ``b""`` creates a valid empty blob.

    Returns
    -------
    pa.ExtensionArray
        Inline blob values ready to use in Arrow tables or record batches.

    Raises
    ------
    TypeError
        If a payload is neither ``bytes`` nor ``None``.

    Examples
    --------
    >>> import pyarrow as pa
    >>> from lancedb.schema import blob, blob_array
    >>> schema = pa.schema([blob("video")])
    >>> videos = blob_array([b"abc", None, b""])
    >>> batch = pa.RecordBatch.from_arrays([videos], schema=schema)
    >>> batch.num_rows
    3
    """
    blob_type = _resolve_blob_type()()
    descriptors = []
    for payload in payloads:
        if payload is not None and not isinstance(payload, bytes):
            raise TypeError(
                f"payloads must contain bytes or None, got {type(payload).__name__}"
            )
        descriptors.append(None if payload is None else {"data": payload})
    storage = pa.array(descriptors, type=blob_type.storage_type)
    return pa.ExtensionArray.from_storage(blob_type, storage)


def _blob_uint64(value: object, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{what} must be an int, got {type(value).__name__}")
    if value < 0:
        raise ValueError(f"{what} must be non-negative")
    if value > _UINT64_MAX:
        raise OverflowError(f"{what} must fit in a uint64")
    return value


def blob_uri_array(
    uris: Iterable[Optional[str]],
    *,
    positions: Optional[Iterable[Optional[int]]] = None,
    sizes: Optional[Iterable[Optional[int]]] = None,
) -> pa.ExtensionArray:
    """Build a Lance blob v2 extension array from external URIs.

    Produces the standard four-field blob v2 storage (``data=None``, ``uri``,
    nullable ``position`` and ``size``) without requiring pylance, so the
    result can be used directly in Arrow record batches and combined with
    field metadata such as ``lancedb:logical_type="mp4"``.

    A row without ``position`` and ``size`` references the whole object; a
    row with both reads exactly that byte range. Register the object prefix
    with [BasePath][lancedb.schema.BasePath] at table creation to store the
    URI relative to a base, or set ``allow_external_blob_outside_bases=True``
    on create to keep the absolute URI.

    Parameters
    ----------
    uris:
        An iterable of URI strings or ``None`` for a null blob. Relative
        paths are resolved against a registered base and are stored as-is
        when no base matches.
    positions:
        Optional byte offsets, one per URI. Provide together with ``sizes``;
        each entry is an ``int`` or ``None``. A range requires both fields.
    sizes:
        Optional range lengths in bytes, one per URI. Must be positive when
        ``position`` is set.

    Returns
    -------
    pa.ExtensionArray
        external blob values ready to use in Arrow tables or record batches.

    Raises
    ------
    TypeError
        If a URI, position, or size has the wrong type.
    ValueError
        If lengths differ, only one of ``positions``/``sizes`` is given, a
        range is incomplete, a size is not positive, or a range is given for
        a null URI.
    OverflowError
        If a position or size does not fit in a uint64.

    Examples
    --------
    >>> import pyarrow as pa
    >>> from lancedb.schema import blob, blob_uri_array
    >>> schema = pa.schema([blob("video")])
    >>> videos = blob_uri_array(
    ...     ["s3://bucket/a.mp4", "hf://datasets/org/repo@abc/b.mp4"],
    ...     positions=[0, 8],
    ...     sizes=[4, 4],
    ... )
    >>> batch = pa.RecordBatch.from_arrays([videos], schema=schema)
    >>> batch.num_rows
    2
    """
    uri_list = list(uris)
    if positions is None and sizes is None:
        ranges = [(None, None)] * len(uri_list)
    elif positions is None or sizes is None:
        raise ValueError("positions and sizes must be provided together")
    else:
        position_list = list(positions)
        size_list = list(sizes)
        if len(position_list) != len(uri_list):
            raise ValueError(
                "positions must have the same length as uris, got "
                f"{len(position_list)} and {len(uri_list)}"
            )
        if len(size_list) != len(uri_list):
            raise ValueError(
                "sizes must have the same length as uris, got "
                f"{len(size_list)} and {len(uri_list)}"
            )
        ranges = list(zip(position_list, size_list, strict=True))

    descriptors = []
    for index, (uri, (position, size)) in enumerate(zip(uri_list, ranges, strict=True)):
        if uri is None:
            if position is not None or size is not None:
                raise ValueError(
                    f"uris[{index}] is None but a position or size was provided"
                )
            descriptors.append(None)
            continue
        if not isinstance(uri, str):
            raise TypeError(f"uris[{index}] must be a string, got {type(uri).__name__}")
        if position is None and size is None:
            descriptors.append({"uri": uri})
            continue
        if position is None or size is None:
            raise ValueError(
                f"uris[{index}] must set both position and size or neither"
            )
        position = _blob_uint64(position, f"positions[{index}]")
        size = _blob_uint64(size, f"sizes[{index}]")
        if size == 0:
            raise ValueError(f"sizes[{index}] must be positive to read a byte range")
        descriptors.append({"uri": uri, "position": position, "size": size})

    blob_type = _resolve_blob_type()()
    storage = pa.array(descriptors, type=blob_type.storage_type)
    return pa.ExtensionArray.from_storage(blob_type, storage)


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
