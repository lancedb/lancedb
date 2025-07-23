# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Session management for LanceDB connections.

Sessions allow you to configure cache sizes for index and metadata caches,
which can significantly impact performance for large datasets.
"""

from __future__ import annotations

from typing import Optional

from ._lancedb import Session as _Session


class Session:
    """A session for managing caches and object stores across LanceDB operations.

    Sessions allow you to configure cache sizes for index and metadata caches,
    which can significantly impact performance for large datasets. By creating
    a session with custom cache sizes, you can optimize memory usage and
    performance for your specific use case.

    Parameters
    ----------
    index_cache_size_bytes : int, optional
        The size of the index cache in bytes.
        The index cache stores decompressed vector indices in memory for faster access.
        A larger cache can improve performance for datasets with many indices, but uses
        more RAM.
        Default: 6GB (6 * 1024 * 1024 * 1024 bytes)
    metadata_cache_size_bytes : int, optional
        The size of the metadata cache in bytes.
        The metadata cache stores file metadata and schema information in memory.
        A larger cache can improve performance for datasets with many files or frequent
        schema access.
        Default: 1GB (1024 * 1024 * 1024 bytes)

    Examples
    --------
    Create a session with custom cache sizes:

    >>> import lancedb
    >>> session = lancedb.Session(
    ...     index_cache_size_bytes=8 * 1024 * 1024 * 1024,  # 8GB
    ...     metadata_cache_size_bytes=2 * 1024 * 1024 * 1024,  # 2GB
    ... )
    >>> db = lancedb.connect("./my-lancedb", session=session)

    Create a session with default cache sizes:

    >>> session = lancedb.Session.default()
    >>> db = lancedb.connect("./my-lancedb", session=session)
    """

    def __init__(
        self,
        index_cache_size_bytes: Optional[int] = None,
        metadata_cache_size_bytes: Optional[int] = None,
    ):
        """Initialize a new Session with custom cache sizes."""
        self._inner = _Session(index_cache_size_bytes, metadata_cache_size_bytes)

    @staticmethod
    def default() -> Session:
        """Create a Session with default cache sizes.

        This is equivalent to creating a session with 6GB index cache
        and 1GB metadata cache.

        Returns
        -------
        Session
            A new Session with default cache sizes
        """
        session = Session.__new__(Session)
        session._inner = _Session.default()
        return session

    @property
    def size_bytes(self) -> int:
        """Get the current size of the session caches in bytes.

        Returns
        -------
        int
            The total size of all caches in the session
        """
        return self._inner.size_bytes

    @property
    def approx_num_items(self) -> int:
        """Get the approximate number of items cached in the session.

        Returns
        -------
        int
            The number of cached items across all caches
        """
        return self._inner.approx_num_items

    def __repr__(self) -> str:
        """Return a string representation of the Session."""
        return self._inner.__repr__()
