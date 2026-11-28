# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Streaming Blob output for Enterprise Functions."""

from __future__ import annotations

import pyarrow as pa

from .schema import blob

_BLOB_WRITER_METADATA_KEY = b"lance.function.blob_writer"
_REFRESH_REQUIRED = (
    "BlobWriter is available only during a LanceDB Enterprise computed column refresh"
)


def _mark_blob_writer(field: pa.Field) -> pa.Field:
    metadata: dict[bytes | str, bytes | str] = {
        key: value for key, value in (field.metadata or {}).items()
    }
    metadata[_BLOB_WRITER_METADATA_KEY] = b"true"
    return field.with_metadata(metadata)


class BlobWriter:
    """Write one Blob v2 value in chunks during a computed column refresh.

    Annotate a scalar Function's return value with ``BlobWriter`` to declare a
    Blob v2 result. For named or nested results, use :meth:`field` for each
    writer-capable field in the output schema. A vectorized Function returns
    a row-aligned list or tuple of results.

    Open one writer per Blob value. Writes are sequential and block for
    capacity. Closing the writer seals the result, including an empty Blob.
    A context manager seals on normal exit and aborts on exceptional exit.
    A discarded writer still uses one entry in the batch's cumulative
    descriptor allowance. Explicit writers and implicit uploads of returned
    ``bytes`` share this allowance.

    Return the sealed writer once at its declared Blob v2 output field during
    the same invocation. Unclosed or reused writers are invalid results.

    The worker provides the implementation when it runs the packaged Function.

    Examples
    --------
    ```python
    from lancedb.functions import BlobWriter, function

    @function
    def render(text: str) -> BlobWriter:
        with BlobWriter.open() as output:
            for line in text.splitlines():
                output.write(line.encode("utf-8"))
                output.write(b"\\n")
        return output
    ```
    """

    def __init__(self) -> None:
        raise RuntimeError(_REFRESH_REQUIRED)

    @classmethod
    def open(cls) -> BlobWriter:
        """Open a sequential writer for one Blob in the active invocation."""
        raise RuntimeError(_REFRESH_REQUIRED)

    def write(self, data) -> int:
        """Write a bytes-like object and return the accepted byte count."""
        raise RuntimeError(_REFRESH_REQUIRED)

    def close(self) -> None:
        """Seal the Blob so the writer can be returned as a result."""
        raise RuntimeError(_REFRESH_REQUIRED)

    def __enter__(self) -> BlobWriter:
        raise RuntimeError(_REFRESH_REQUIRED)

    def __exit__(self, exc_type, exc, traceback) -> None:
        raise RuntimeError(_REFRESH_REQUIRED)

    @staticmethod
    def field(name: str, nullable: bool = True) -> pa.Field:
        """Declare a writer-capable Blob v2 field in a Function output schema.

        A marked field also accepts ordinary ``bytes`` and, if nullable,
        ``None``. Use this helper for Function output schemas and use
        [``lancedb.blob``][lancedb.blob] for table schemas.
        """
        return _mark_blob_writer(blob(name, nullable=nullable))
