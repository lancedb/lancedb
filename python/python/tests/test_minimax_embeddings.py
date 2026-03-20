# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
import os
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pytest

from lancedb.embeddings import get_registry
from lancedb.embeddings.minimax import MiniMaxEmbeddings


# ---------------------------------------------------------------------------
# Unit tests (no API calls)
# ---------------------------------------------------------------------------


class TestMiniMaxEmbeddingsUnit:
    """Unit tests that mock the MiniMax API."""

    def setup_method(self):
        # Reset the class-level session before each test
        MiniMaxEmbeddings._session = None

    def _mock_response(self, vectors, status_code=0, status_msg="success"):
        """Create a mock response matching MiniMax API format."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "vectors": vectors,
            "total_tokens": 10,
            "base_resp": {
                "status_code": status_code,
                "status_msg": status_msg,
            },
        }
        return mock_resp

    def test_registry_registration(self):
        """Test that MiniMax is registered in the embedding registry."""
        registry = get_registry()
        cls = registry.get("minimax")
        assert cls is MiniMaxEmbeddings

    def test_ndims_embo_01(self):
        """Test that embo-01 returns 1536 dimensions."""
        registry = get_registry()
        registry.set_var("minimax_test_key", "test-key")
        func = MiniMaxEmbeddings.create(api_key="$var:minimax_test_key")
        assert func.ndims() == 1536

    def test_ndims_unknown_model(self):
        """Test that unknown model raises ValueError."""
        registry = get_registry()
        registry.set_var("minimax_test_key2", "test-key")
        func = MiniMaxEmbeddings.create(
            name="unknown-model",
            api_key="$var:minimax_test_key2",
        )
        with pytest.raises(ValueError, match="Unknown MiniMax embedding model"):
            func.ndims()

    def test_sensitive_keys(self):
        """Test that api_key is marked as sensitive."""
        assert "api_key" in MiniMaxEmbeddings.sensitive_keys()

    @patch("requests.Session")
    def test_generate_source_embeddings(self, mock_session_cls):
        """Test source embedding generation (type=db)."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        vectors = [np.random.randn(1536).tolist() for _ in range(2)]
        mock_session.post.return_value = self._mock_response(vectors)

        os.environ["MINIMAX_API_KEY"] = "test-key"
        try:
            func = MiniMaxEmbeddings.create()
            result = func.compute_source_embeddings(["hello", "world"])

            assert len(result) == 2
            # Verify the API was called with type=db
            call_args = mock_session.post.call_args
            assert call_args[1]["json"]["type"] == "db"
            assert call_args[1]["json"]["texts"] == ["hello", "world"]
            assert call_args[1]["json"]["model"] == "embo-01"
        finally:
            del os.environ["MINIMAX_API_KEY"]
            MiniMaxEmbeddings._session = None

    @patch("requests.Session")
    def test_generate_query_embeddings(self, mock_session_cls):
        """Test query embedding generation (type=query)."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        vectors = [np.random.randn(1536).tolist()]
        mock_session.post.return_value = self._mock_response(vectors)

        os.environ["MINIMAX_API_KEY"] = "test-key"
        try:
            func = MiniMaxEmbeddings.create()
            result = func.compute_query_embeddings("hello world")

            assert len(result) == 1
            # Verify the API was called with type=query
            call_args = mock_session.post.call_args
            assert call_args[1]["json"]["type"] == "query"
        finally:
            del os.environ["MINIMAX_API_KEY"]
            MiniMaxEmbeddings._session = None

    @patch("requests.Session")
    def test_handles_empty_texts(self, mock_session_cls):
        """Test that empty/None texts return None embeddings."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        vectors = [np.random.randn(1536).tolist()]
        mock_session.post.return_value = self._mock_response(vectors)

        os.environ["MINIMAX_API_KEY"] = "test-key"
        try:
            func = MiniMaxEmbeddings.create()
            result = func.compute_source_embeddings(["", "hello", ""])

            assert len(result) == 3
            assert result[0] is None
            assert result[1] is not None
            assert result[2] is None
            # Only "hello" should be sent to API
            call_args = mock_session.post.call_args
            assert call_args[1]["json"]["texts"] == ["hello"]
        finally:
            del os.environ["MINIMAX_API_KEY"]
            MiniMaxEmbeddings._session = None

    @patch("requests.Session")
    def test_api_error_raises(self, mock_session_cls):
        """Test that API errors are raised as RuntimeError."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.post.return_value = self._mock_response(
            None, status_code=1002, status_msg="rate limit exceeded"
        )

        os.environ["MINIMAX_API_KEY"] = "test-key"
        try:
            func = MiniMaxEmbeddings.create()
            with pytest.raises(RuntimeError, match="MiniMax API error"):
                func.compute_source_embeddings(["hello"])
        finally:
            del os.environ["MINIMAX_API_KEY"]
            MiniMaxEmbeddings._session = None

    def test_missing_api_key_raises(self):
        """Test that missing API key raises ValueError."""
        # Ensure no env var is set
        original = os.environ.pop("MINIMAX_API_KEY", None)
        try:
            func = MiniMaxEmbeddings.create(api_key="$var:minimax_missing_key:none")
            func.api_key = None
            with pytest.raises(ValueError, match="MINIMAX_API_KEY"):
                func._init_client()
        finally:
            if original is not None:
                os.environ["MINIMAX_API_KEY"] = original
            MiniMaxEmbeddings._session = None

    @patch("requests.Session")
    def test_api_key_from_param(self, mock_session_cls):
        """Test that api_key parameter takes precedence over env var."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        registry = get_registry()
        registry.set_var("minimax_custom_key", "custom-api-key-123")
        func = MiniMaxEmbeddings.create(api_key="$var:minimax_custom_key")
        func._init_client()

        # Check the Authorization header was set with the custom key
        mock_session.headers.update.assert_called_once()
        headers = mock_session.headers.update.call_args[0][0]
        assert headers["Authorization"] == "Bearer custom-api-key-123"
        MiniMaxEmbeddings._session = None

    @patch("requests.Session")
    def test_all_empty_texts(self, mock_session_cls):
        """Test that all-empty input returns all None."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        os.environ["MINIMAX_API_KEY"] = "test-key"
        try:
            func = MiniMaxEmbeddings.create()
            result = func.compute_source_embeddings(["", "", ""])

            assert result == [None, None, None]
            # API should not be called
            mock_session.post.assert_not_called()
        finally:
            del os.environ["MINIMAX_API_KEY"]
            MiniMaxEmbeddings._session = None

    @patch("requests.Session")
    def test_safe_model_dump(self, mock_session_cls):
        """Test that safe_model_dump preserves variable references."""
        registry = get_registry()
        registry.set_var("minimax_dump_key", "secret-key")
        func = MiniMaxEmbeddings.create(api_key="$var:minimax_dump_key")

        dumped = func.safe_model_dump()
        assert dumped["api_key"] == "$var:minimax_dump_key"

    @patch("requests.Session")
    def test_no_vectors_raises(self, mock_session_cls):
        """Test that response with no vectors raises RuntimeError."""
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "vectors": None,
            "base_resp": {"status_code": 0, "status_msg": "success"},
        }
        mock_session.post.return_value = mock_resp

        os.environ["MINIMAX_API_KEY"] = "test-key"
        try:
            func = MiniMaxEmbeddings.create()
            with pytest.raises(RuntimeError, match="returned no vectors"):
                func.compute_source_embeddings(["hello"])
        finally:
            del os.environ["MINIMAX_API_KEY"]
            MiniMaxEmbeddings._session = None


# ---------------------------------------------------------------------------
# Integration tests (require MINIMAX_API_KEY)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    os.environ.get("MINIMAX_API_KEY") is None,
    reason="MINIMAX_API_KEY not set",
)
class TestMiniMaxEmbeddingsIntegration:
    """Integration tests that call the real MiniMax API."""

    def setup_method(self):
        MiniMaxEmbeddings._session = None

    def test_real_source_embeddings(self):
        """Test real source embedding generation."""
        registry = get_registry()
        func = registry.get("minimax").create()
        result = func.compute_source_embeddings(["hello world", "goodbye world"])

        assert len(result) == 2
        assert len(result[0]) == 1536
        assert len(result[1]) == 1536
        # Embeddings should be different for different texts
        assert not np.allclose(result[0], result[1])

    def test_real_query_embeddings(self):
        """Test real query embedding generation."""
        registry = get_registry()
        func = registry.get("minimax").create()
        result = func.compute_query_embeddings("semantic search query")

        assert len(result) == 1
        assert len(result[0]) == 1536

    def test_source_query_type_difference(self):
        """Test that source and query embeddings are different for same text."""
        registry = get_registry()
        func = registry.get("minimax").create()
        text = "test embedding"
        source = func.compute_source_embeddings([text])
        query = func.compute_query_embeddings(text)

        assert len(source[0]) == 1536
        assert len(query[0]) == 1536
        # db and query types should produce different embeddings
        assert not np.allclose(source[0], query[0])
