# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import concurrent.futures
import pytest
import time
import threading
from typing import Dict

from lancedb.remote import ClientConfig, HeaderProvider
from lancedb.remote.header import StaticHeaderProvider, OAuthProvider


class TestStaticHeaderProvider:
    def test_init(self):
        """Test StaticHeaderProvider initialization."""
        headers = {"X-API-Key": "test-key", "X-Custom": "value"}
        provider = StaticHeaderProvider(headers)
        assert provider._headers == headers

    def test_get_headers(self):
        """Test get_headers returns correct headers."""
        headers = {"X-API-Key": "test-key", "X-Custom": "value"}
        provider = StaticHeaderProvider(headers)

        result = provider.get_headers()
        assert result == headers

        # Ensure it returns a copy
        result["X-Modified"] = "modified"
        result2 = provider.get_headers()
        assert "X-Modified" not in result2


class TestOAuthProvider:
    def test_init(self):
        """Test OAuthProvider initialization."""

        def fetcher():
            return {"access_token": "token123", "expires_in": 3600}

        provider = OAuthProvider(fetcher)
        assert provider._token_fetcher is fetcher
        assert provider._refresh_buffer == 300
        assert provider._current_token is None
        assert provider._token_expires_at is None

    def test_get_headers_first_time(self):
        """Test get_headers fetches token on first call."""

        def fetcher():
            return {"access_token": "token123", "expires_in": 3600}

        provider = OAuthProvider(fetcher)
        headers = provider.get_headers()

        assert headers == {"Authorization": "Bearer token123"}
        assert provider._current_token == "token123"
        assert provider._token_expires_at is not None

    def test_token_refresh(self):
        """Test token refresh when expired."""
        call_count = 0
        tokens = ["token1", "token2"]

        def fetcher():
            nonlocal call_count
            token = tokens[call_count]
            call_count += 1
            return {"access_token": token, "expires_in": 1}  # Expires in 1 second

        provider = OAuthProvider(fetcher, refresh_buffer_seconds=0)

        # First call
        headers1 = provider.get_headers()
        assert headers1 == {"Authorization": "Bearer token1"}

        # Wait for token to expire
        time.sleep(1.1)

        # Second call should refresh
        headers2 = provider.get_headers()
        assert headers2 == {"Authorization": "Bearer token2"}
        assert call_count == 2

    def test_no_expiry_info(self):
        """Test handling tokens without expiry information."""

        def fetcher():
            return {"access_token": "permanent_token"}

        provider = OAuthProvider(fetcher)
        headers = provider.get_headers()

        assert headers == {"Authorization": "Bearer permanent_token"}
        assert provider._token_expires_at is None

        # Should not refresh on second call
        headers2 = provider.get_headers()
        assert headers2 == {"Authorization": "Bearer permanent_token"}

    def test_missing_access_token(self):
        """Test error handling when access_token is missing."""

        def fetcher():
            return {"expires_in": 3600}  # Missing access_token

        provider = OAuthProvider(fetcher)

        with pytest.raises(
            ValueError, match="Token fetcher did not return 'access_token'"
        ):
            provider.get_headers()

    def test_sync_method(self):
        """Test synchronous get_headers method."""

        def fetcher():
            return {"access_token": "sync_token", "expires_in": 3600}

        provider = OAuthProvider(fetcher)
        headers = provider.get_headers()

        assert headers == {"Authorization": "Bearer sync_token"}


class TestClientConfigIntegration:
    def test_client_config_with_header_provider(self):
        """Test ClientConfig can accept a HeaderProvider."""
        provider = StaticHeaderProvider({"X-Test": "value"})
        config = ClientConfig(header_provider=provider)

        assert config.header_provider is provider

    def test_client_config_without_header_provider(self):
        """Test ClientConfig works without HeaderProvider."""
        config = ClientConfig()
        assert config.header_provider is None


class CustomProvider(HeaderProvider):
    """Custom provider for testing abstract class."""

    def get_headers(self) -> Dict[str, str]:
        return {"X-Custom": "custom-value"}


class TestCustomHeaderProvider:
    def test_custom_provider(self):
        """Test custom HeaderProvider implementation."""
        provider = CustomProvider()
        headers = provider.get_headers()
        assert headers == {"X-Custom": "custom-value"}


class ErrorProvider(HeaderProvider):
    """Provider that raises errors for testing error handling."""

    def __init__(self, error_message: str = "Test error"):
        self.error_message = error_message
        self.call_count = 0

    def get_headers(self) -> Dict[str, str]:
        self.call_count += 1
        raise RuntimeError(self.error_message)


class TestErrorHandling:
    def test_provider_error_propagation(self):
        """Test that errors from header provider are properly propagated."""
        provider = ErrorProvider("Authentication failed")

        with pytest.raises(RuntimeError, match="Authentication failed"):
            provider.get_headers()

        assert provider.call_count == 1

    def test_provider_error(self):
        """Test that errors are propagated."""
        provider = ErrorProvider("Sync error")

        with pytest.raises(RuntimeError, match="Sync error"):
            provider.get_headers()


class ConcurrentProvider(HeaderProvider):
    """Provider for testing thread safety."""

    def __init__(self):
        self.counter = 0
        self.lock = threading.Lock()

    def get_headers(self) -> Dict[str, str]:
        with self.lock:
            self.counter += 1
            # Simulate some work
            time.sleep(0.01)
            return {"X-Request-Id": str(self.counter)}


class TestConcurrency:
    def test_concurrent_header_fetches(self):
        """Test that header provider can handle concurrent requests."""
        provider = ConcurrentProvider()

        # Create multiple concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(provider.get_headers) for _ in range(10)]
            results = [f.result() for f in futures]

        # Each request should get a unique counter value
        request_ids = [int(r["X-Request-Id"]) for r in results]
        assert len(set(request_ids)) == 10
        assert min(request_ids) == 1
        assert max(request_ids) == 10

    def test_oauth_concurrent_refresh(self):
        """Test that OAuth provider handles concurrent refresh requests safely."""
        call_count = 0

        def slow_token_fetch():
            nonlocal call_count
            call_count += 1
            time.sleep(0.1)  # Simulate slow token fetch
            return {"access_token": f"token-{call_count}", "expires_in": 3600}

        provider = OAuthProvider(slow_token_fetch)

        # Force multiple concurrent refreshes
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(provider.get_headers) for _ in range(5)]
            results = [f.result() for f in futures]

        # All requests should get the same token (only one refresh should happen)
        tokens = [r["Authorization"] for r in results]
        assert all(t == "Bearer token-1" for t in tokens)
        assert call_count == 1  # Only one token fetch despite concurrent requests
