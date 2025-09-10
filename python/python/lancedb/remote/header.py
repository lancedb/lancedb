# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Header providers for LanceDB remote connections.

This module provides a flexible header management framework for LanceDB remote
connections, allowing users to implement custom header strategies for
authentication, request tracking, custom metadata, or any other header-based
requirements.

The module includes the HeaderProvider abstract base class and example implementations
(StaticHeaderProvider and OAuthProvider) that demonstrate common patterns.

The HeaderProvider interface is designed to be called before each request to the remote
server, enabling dynamic header scenarios where values may need to be
refreshed, rotated, or computed on-demand.
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional, Callable, Any
import time
import threading


class HeaderProvider(ABC):
    """Abstract base class for providing custom headers for each request.

    Users can implement this interface to provide dynamic headers for various purposes
    such as authentication (OAuth tokens, API keys), request tracking (correlation IDs),
    custom metadata, or any other header-based requirements. The provider is called
    before each request to ensure fresh header values are always used.

    Error Handling
    --------------
    If get_headers() raises an exception, the request will fail. Implementations
    should handle recoverable errors internally (e.g., retry token refresh) and
    only raise exceptions for unrecoverable errors.
    """

    @abstractmethod
    def get_headers(self) -> Dict[str, str]:
        """Get the latest headers to be added to requests.

        This method is called before each request to the remote LanceDB server.
        Implementations should return headers that will be merged with existing headers.

        Returns
        -------
        Dict[str, str]
            Dictionary of header names to values to add to the request.

        Raises
        ------
        Exception
            If unable to fetch headers, the exception will be propagated
            and the request will fail.
        """
        pass


class StaticHeaderProvider(HeaderProvider):
    """Example implementation: A simple header provider that returns static headers.

    This is an example implementation showing how to create a HeaderProvider
    for cases where headers don't change during the session. Users can use this
    as a reference for implementing their own providers.

    Parameters
    ----------
    headers : Dict[str, str]
        Static headers to return for every request.
    """

    def __init__(self, headers: Dict[str, str]):
        """Initialize with static headers.

        Parameters
        ----------
        headers : Dict[str, str]
            Headers to return for every request.
        """
        self._headers = headers.copy()

    def get_headers(self) -> Dict[str, str]:
        """Return the static headers.

        Returns
        -------
        Dict[str, str]
            Copy of the static headers.
        """
        return self._headers.copy()


class OAuthProvider(HeaderProvider):
    """Example implementation: OAuth token provider with automatic refresh.

    This is an example implementation showing how to manage OAuth tokens
    with automatic refresh when they expire. Users can use this as a reference
    for implementing their own OAuth or token-based authentication providers.

    Parameters
    ----------
    token_fetcher : Callable[[], Dict[str, Any]]
        Function that fetches a new token. Should return a dict with
        'access_token' and optionally 'expires_in' (seconds until expiration).
    refresh_buffer_seconds : int, optional
        Number of seconds before expiration to trigger refresh. Default is 300
        (5 minutes).
    """

    def __init__(
        self, token_fetcher: Callable[[], Any], refresh_buffer_seconds: int = 300
    ):
        """Initialize the OAuth provider.

        Parameters
        ----------
        token_fetcher : Callable[[], Any]
            Function to fetch new tokens. Should return dict with
            'access_token' and optionally 'expires_in'.
        refresh_buffer_seconds : int, optional
            Seconds before expiry to refresh token. Default 300.
        """
        self._token_fetcher = token_fetcher
        self._refresh_buffer = refresh_buffer_seconds
        self._current_token: Optional[str] = None
        self._token_expires_at: Optional[float] = None
        self._refresh_lock = threading.Lock()

    def _refresh_token_if_needed(self) -> None:
        """Refresh the token if it's expired or close to expiring."""
        with self._refresh_lock:
            # Check again inside the lock in case another thread refreshed
            if self._needs_refresh():
                token_data = self._token_fetcher()

                self._current_token = token_data.get("access_token")
                if not self._current_token:
                    raise ValueError("Token fetcher did not return 'access_token'")

                # Set expiration if provided
                expires_in = token_data.get("expires_in")
                if expires_in:
                    self._token_expires_at = time.time() + expires_in
                else:
                    # Token doesn't expire or expiration unknown
                    self._token_expires_at = None

    def _needs_refresh(self) -> bool:
        """Check if token needs refresh."""
        if self._current_token is None:
            return True

        if self._token_expires_at is None:
            # No expiration info, assume token is valid
            return False

        # Refresh if we're within the buffer time of expiration
        return time.time() >= (self._token_expires_at - self._refresh_buffer)

    def get_headers(self) -> Dict[str, str]:
        """Get OAuth headers, refreshing token if needed.

        Returns
        -------
        Dict[str, str]
            Headers with Bearer token authorization.

        Raises
        ------
        Exception
            If unable to fetch or refresh token.
        """
        self._refresh_token_if_needed()

        if not self._current_token:
            raise RuntimeError("Failed to obtain OAuth token")

        return {"Authorization": f"Bearer {self._current_token}"}
