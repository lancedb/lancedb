# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""I/O utilities and interfaces for LanceDB."""

from abc import ABC, abstractmethod
from typing import Dict


class StorageOptionsProvider(ABC):
    """Abstract base class for providing storage options to LanceDB tables.

    Storage options providers enable automatic credential refresh for cloud
    storage backends (e.g., AWS S3, Azure Blob Storage, GCS). When credentials
    have an expiration time, the provider's fetch_storage_options() method will
    be called periodically to get fresh credentials before they expire.

    Example
    -------
    >>> class MyProvider(StorageOptionsProvider):
    ...     def fetch_storage_options(self) -> Dict[str, str]:
    ...         # Fetch fresh credentials from your credential manager
    ...         return {
    ...             "aws_access_key_id": "...",
    ...             "aws_secret_access_key": "...",
    ...             "expires_at_millis": "1234567890000"  # Optional
    ...         }
    """

    @abstractmethod
    def fetch_storage_options(self) -> Dict[str, str]:
        """Fetch fresh storage credentials.

        This method is called by LanceDB when credentials need to be refreshed.
        If the returned dictionary contains an "expires_at_millis" key with a
        Unix timestamp in milliseconds, LanceDB will automatically refresh the
        credentials before that time. If the key is not present, credentials
        are assumed to not expire.

        Returns
        -------
        Dict[str, str]
            Dictionary containing cloud storage credentials and optionally an
            expiration time:
            - "expires_at_millis" (optional): Unix timestamp in milliseconds when
              credentials expire
            - Provider-specific credential keys (e.g., aws_access_key_id,
              aws_secret_access_key, etc.)

        Raises
        ------
        RuntimeError
            If credentials cannot be fetched or are invalid
        """
        pass

    def provider_id(self) -> str:
        """Return a human-readable unique identifier for this provider instance.

        This identifier is used for caching and equality comparison. Two providers
        with the same ID will share the same cached object store connection.

        The default implementation uses the class name and string representation.
        Override this method if you need custom identification logic.

        Returns
        -------
        str
            A unique identifier for this provider instance
        """
        return f"{self.__class__.__name__} {{ repr: {str(self)!r} }}"
