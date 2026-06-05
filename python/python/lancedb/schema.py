# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


"""Schema related utilities."""

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


def blob(name: str, nullable: bool = True) -> pa.Field:
    """Create a Lance blob v2 column field.

    Blob columns store large binary payloads efficiently. Queries return
    lightweight description structs by default; use
    :meth:`~lancedb.table.Table.take_blobs` with row ids from
    :meth:`~lancedb.query.LanceQueryBuilder.with_row_id` to load bytes.

    Parameters
    ----------
    name : str
        Column name.
    nullable : bool, default True
        Whether the column accepts null values.

    Returns
    -------
    pa.Field
        Arrow field with extension type ``lance.blob.v2``.

    Examples
    --------
    >>> import pyarrow as pa
    >>> import lancedb
    >>> schema = pa.schema([
    ...     pa.field("id", pa.int64()),
    ...     lancedb.blob("image"),
    ... ])
    """
    # Build the field to match the blob v2 storage layout of the Lance version
    # the Rust engine is compiled against (Struct<data, uri> + the lance.blob.v2
    # extension marker). We intentionally do not delegate to the standalone
    # `lance` (pylance) package: its blob v2 layout can differ from the engine's
    # pinned version, and a manually built struct field (vs. a registered pyarrow
    # extension type) is never silently re-resolved if pylance is imported too.
    storage = pa.struct(
        [
            pa.field("data", pa.large_binary()),
            pa.field("uri", pa.string()),
        ]
    )
    return pa.field(
        name,
        storage,
        nullable=nullable,
        metadata={"ARROW:extension:name": "lance.blob.v2"},
    )
