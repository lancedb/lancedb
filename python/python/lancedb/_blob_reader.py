# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Seekable binary input for Functions that consume Blob V2 values lazily."""

import io
import operator
import urllib.request
from collections.abc import Callable
from http import HTTPStatus

_READ_CHUNK_BYTES = 1024 * 1024
_READ_TIMEOUT_SECONDS = 300


class BlobReader(io.RawIOBase):
    """Read a blob incrementally inside a Function.

    Annotating a Function input with ``BlobReader`` requests a lazy, seekable
    reader. Annotating it with ``bytes`` requests materialized bytes instead.
    ``read_range`` does not change the sequential cursor. Readers are scoped
    to the invocation and must not be retained for a later call.

    For local calls, construct a reader from a range-reading callable and the
    blob's size. The callable accepts an offset and length and returns bytes.
    """

    def __init__(self, read_range: Callable[[int, int], bytes], size: int):
        super().__init__()
        self._read_range = read_range
        self._size = operator.index(size)
        if self._size < 0:
            raise ValueError("blob size must be nonnegative")
        self._position = 0

    @classmethod
    def _from_uri(cls, uri: str, size: int):
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

        def read_range(offset, length):
            request = urllib.request.Request(
                uri, headers={"Range": f"bytes={offset}-{offset + length - 1}"}
            )
            with opener.open(request, timeout=_READ_TIMEOUT_SECONDS) as response:
                if response.status != HTTPStatus.PARTIAL_CONTENT:
                    raise OSError(
                        "blob range service did not return a partial response"
                    )
                data = response.read(length + 1)
                if len(data) != length:
                    raise OSError(
                        "blob range service returned an invalid payload length"
                    )
                return data

        return cls(read_range, size)

    def readable(self) -> bool:
        return not self.closed

    def seekable(self) -> bool:
        return not self.closed

    def size(self) -> int:
        """Return the blob size without reading its payload."""
        self._checkClosed()
        return self._size

    def tell(self) -> int:
        self._checkClosed()
        return self._position

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self._checkClosed()
        offset = operator.index(offset)
        if whence == io.SEEK_SET:
            position = offset
        elif whence == io.SEEK_CUR:
            position = self._position + offset
        elif whence == io.SEEK_END:
            position = self._size + offset
        else:
            raise ValueError(f"invalid whence: {whence}")
        if position < 0:
            raise ValueError("negative seek position")
        self._position = position
        return position

    def read_range(self, offset: int, length: int) -> bytes:
        """Read a byte range without changing the cursor; truncate at EOF."""
        self._checkClosed()
        offset, length = operator.index(offset), operator.index(length)
        if offset < 0 or length < 0:
            raise ValueError("offset and length must be nonnegative")
        remaining = min(length, max(0, self._size - offset))
        chunks = []
        while remaining:
            count = min(remaining, _READ_CHUNK_BYTES)
            chunk = self._read_range(offset, count)
            if len(chunk) != count:
                raise OSError("blob range reader returned an invalid payload length")
            chunks.append(chunk)
            offset += count
            remaining -= count
        return b"".join(chunks)

    def read(self, size: int = -1) -> bytes:
        self._checkClosed()
        size = -1 if size is None else operator.index(size)
        if size < 0:
            size = max(0, self._size - self._position)
        data = self.read_range(self._position, size)
        self._position += len(data)
        return data

    def readinto(self, buffer) -> int:
        view = memoryview(buffer).cast("B")
        data = self.read(len(view))
        view[: len(data)] = data
        return len(data)
