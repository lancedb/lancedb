# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Header providers for LanceDB remote connections.

This module provides a flexible header management framework for LanceDB remote connections,
allowing users to implement custom header strategies for authentication, request tracking,
custom metadata, or any other header-based requirements.

The HeaderProvider interface is designed to be called before each request to the remote
server, enabling dynamic header scenarios where values may need to be
refreshed, rotated, or computed on-demand.

Examples
--------
Basic usage with static API key:
>>> from lancedb.remote import ClientConfig
>>> from lancedb.remote.header import StaticHeaderProvider
>>> import lancedb
>>>
>>> provider = StaticHeaderProvider({"X-API-Key": "your-api-key"})
>>> config = ClientConfig(header_provider=provider)
>>> db = await lancedb.connect("db://my-database", client_config=config)

OAuth with automatic token refresh:
>>> async def fetch_oauth_token():
>>>     # Your OAuth token fetching logic here
>>>     response = await oauth_client.get_token()
>>>     return {"access_token": response.token, "expires_in": 3600}
>>>
>>> provider = OAuthProvider(fetch_oauth_token, refresh_buffer_seconds=300)
>>> config = ClientConfig(header_provider=provider)
>>> db = await lancedb.connect("db://my-database", client_config=config)

Custom provider implementation:
>>> class CustomProvider(HeaderProvider):
>>>     async def get_headers(self) -> Dict[str, str]:
>>>         # Your custom logic here
>>>         token = await self.fetch_token_from_service()
>>>         return {"Authorization": f"Custom {token}"}
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional, Callable, Any
import asyncio
import time


class HeaderProvider(ABC):
    """Abstract base class for providing custom headers for each request.
    
    Users can implement this interface to provide dynamic headers for various purposes
    such as authentication (OAuth tokens, API keys), request tracking (correlation IDs),
    custom metadata, or any other header-based requirements. The provider is called 
    before each request to ensure fresh header values are always used.
    
    Thread Safety
    -------------
    Implementations should be thread-safe as get_headers() may be called
    concurrently from multiple threads. Use locks or other synchronization
    primitives if your implementation maintains mutable state.
    
    Error Handling
    --------------
    If get_headers() raises an exception, the request will fail. Implementations
    should handle recoverable errors internally (e.g., retry token refresh) and
    only raise exceptions for unrecoverable errors.
    
    Examples
    --------
    Simple JWT token provider:
    >>> class JWTProvider(HeaderProvider):
    ...     def __init__(self, jwt_token: str):
    ...         self.token = jwt_token
    ...     
    ...     async def get_headers(self) -> Dict[str, str]:
    ...         return {"Authorization": f"Bearer {self.token}"}
    
    Provider with rotating API keys:
    >>> class RotatingKeyProvider(HeaderProvider):
    ...     def __init__(self, key_manager):
    ...         self.key_manager = key_manager
    ...     
    ...     async def get_headers(self) -> Dict[str, str]:
    ...         current_key = await self.key_manager.get_current_key()
    ...         return {"X-API-Key": current_key}
    """
    
    @abstractmethod
    async def get_headers(self) -> Dict[str, str]:
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
    
    def get_headers_sync(self) -> Dict[str, str]:
        """Synchronous version of get_headers for compatibility.
        
        This method runs the async get_headers in a new event loop.
        Override this if you need different sync behavior.
        
        Returns
        -------
        Dict[str, str]
            Dictionary of header names to values.
        """
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self.get_headers())
        finally:
            loop.close()


class StaticHeaderProvider(HeaderProvider):
    """A simple header provider that returns static headers.
    
    This is useful for cases where headers don't change during the session,
    but you want to use the HeaderProvider interface for consistency.
    
    Parameters
    ----------
    headers : Dict[str, str]
        Static headers to return for every request.
    
    Examples
    --------
    >>> from lancedb.remote.header import StaticHeaderProvider
    >>> provider = StaticHeaderProvider({
    ...     "X-API-Key": "my-secret-key",
    ...     "X-Custom-Header": "custom-value"
    ... })
    >>> await provider.get_headers()
    {'X-API-Key': 'my-secret-key', 'X-Custom-Header': 'custom-value'}
    """
    
    def __init__(self, headers: Dict[str, str]):
        """Initialize with static headers.
        
        Parameters
        ----------
        headers : Dict[str, str]
            Headers to return for every request.
        """
        self._headers = headers.copy()
    
    async def get_headers(self) -> Dict[str, str]:
        """Return the static headers.
        
        Returns
        -------
        Dict[str, str]
            Copy of the static headers.
        """
        return self._headers.copy()


class OAuthProvider(HeaderProvider):
    """Example OAuth token provider with automatic refresh.
    
    This provider manages OAuth tokens and automatically refreshes them
    when they expire. Users should implement the token refresh logic
    according to their OAuth provider's requirements.
    
    Parameters
    ----------
    token_fetcher : Callable[[], Dict[str, Any]]
        Async function that fetches a new token. Should return a dict with
        'access_token' and optionally 'expires_in' (seconds until expiration).
    refresh_buffer_seconds : int, optional
        Number of seconds before expiration to trigger refresh. Default is 300 (5 minutes).
    
    Examples
    --------
    >>> import aiohttp
    >>> async def fetch_token():
    ...     async with aiohttp.ClientSession() as session:
    ...         async with session.post(
    ...             "https://oauth.example.com/token",
    ...             data={"grant_type": "client_credentials", "client_id": "...", "client_secret": "..."}
    ...         ) as response:
    ...             return await response.json()
    >>> 
    >>> provider = OAuthProvider(fetch_token)
    >>> headers = await provider.get_headers()
    >>> print(headers["Authorization"])
    Bearer eyJ0eXAiOiJKV1QiLCJhbGc...
    """
    
    def __init__(
        self,
        token_fetcher: Callable[[], Any],
        refresh_buffer_seconds: int = 300
    ):
        """Initialize the OAuth provider.
        
        Parameters
        ----------
        token_fetcher : Callable[[], Any]
            Async function to fetch new tokens. Should return dict with
            'access_token' and optionally 'expires_in'.
        refresh_buffer_seconds : int, optional
            Seconds before expiry to refresh token. Default 300.
        """
        self._token_fetcher = token_fetcher
        self._refresh_buffer = refresh_buffer_seconds
        self._current_token: Optional[str] = None
        self._token_expires_at: Optional[float] = None
        self._refresh_lock = asyncio.Lock()
    
    async def _refresh_token_if_needed(self) -> None:
        """Refresh the token if it's expired or close to expiring."""
        async with self._refresh_lock:
            # Check again inside the lock in case another coroutine refreshed
            if self._needs_refresh():
                token_data = await self._token_fetcher()
                
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
    
    async def get_headers(self) -> Dict[str, str]:
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
        await self._refresh_token_if_needed()
        
        if not self._current_token:
            raise RuntimeError("Failed to obtain OAuth token")
        
        return {
            "Authorization": f"Bearer {self._current_token}"
        }