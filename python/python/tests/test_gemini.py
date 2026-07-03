# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Unit tests for GeminiText embedding function."""

import sys
from unittest.mock import MagicMock, patch

# Mock google.genai modules before they are imported by gemini_text.py
mock_google = MagicMock()
mock_genai = MagicMock()
mock_types = MagicMock()

mock_google.genai = mock_genai
mock_genai.types = mock_types

sys.modules["google"] = mock_google
sys.modules["google.genai"] = mock_genai
sys.modules["google.genai.types"] = mock_types

import pytest  # noqa: E402
import numpy as np  # noqa: E402
from lancedb.embeddings import get_registry  # noqa: E402
from lancedb import __version__  # noqa: E402


class TestGeminiText:
    """Tests for GeminiText model registration, configuration, and execution."""

    @pytest.fixture(autouse=True)
    def setup_mocks(self):
        """Set up standard mocks for google-genai Client and Config."""
        # Reset mocks
        mock_genai.reset_mock()
        mock_types.reset_mock()

        self.mock_client = MagicMock()
        mock_genai.Client.return_value = self.mock_client

        # Mock response for embed_content
        self.mock_embedding_1 = MagicMock()
        self.mock_embedding_1.values = [0.1] * 768
        self.mock_embedding_2 = MagicMock()
        self.mock_embedding_2.values = [0.2] * 768

        self.mock_response = MagicMock()
        self.mock_response.embeddings = [self.mock_embedding_1, self.mock_embedding_2]
        self.mock_client.models.embed_content.return_value = self.mock_response

    def test_gemini_registered(self):
        """Test that gemini-text is registered in the embedding function registry."""
        registry = get_registry()
        assert registry.get("gemini-text") is not None

    def test_client_init_headers(self):
        """Test that Client is initialized with the partner-attribution header."""
        with patch.dict("os.environ", {"GOOGLE_API_KEY": "test-key"}):
            with patch("lancedb.embeddings.gemini_text.attempt_import_or_raise"):
                registry = get_registry()
                func = registry.get("gemini-text").create()

                # Access the client property to trigger initialization
                _ = func.client

                mock_genai.Client.assert_called_once_with(
                    api_key="test-key",
                    http_options={
                        "headers": {
                            "x-goog-api-client": f"lancedb/{__version__}",
                        }
                    },
                )

    def test_generate_embeddings_batched(self):
        """Test that multiple texts are sent in a single batched API request."""
        with patch.dict("os.environ", {"GOOGLE_API_KEY": "test-key"}):
            with patch("lancedb.embeddings.gemini_text.attempt_import_or_raise"):
                registry = get_registry()
                func = registry.get("gemini-text").create()

                texts = ["hello", "world"]
                embeddings = func.generate_embeddings(texts)

                # Check embed_content was called exactly once
                self.mock_client.models.embed_content.assert_called_once()

                # Verify call arguments
                call_kwargs = self.mock_client.models.embed_content.call_args.kwargs
                assert call_kwargs["model"] == "gemini-embedding-001"
                assert len(call_kwargs["contents"]) == 2
                assert call_kwargs["contents"][0] == {"parts": [{"text": "hello"}]}
                assert call_kwargs["contents"][1] == {"parts": [{"text": "world"}]}

                # Verify returns are correct numpy arrays
                assert len(embeddings) == 2
                assert isinstance(embeddings[0], np.ndarray)
                assert embeddings[0].shape == (768,)
                assert np.allclose(embeddings[0], 0.1)
                assert np.allclose(embeddings[1], 0.2)

    def test_generate_embeddings_retrieval_document(self):
        """Test that retrieval_document task type prepends the document title part."""
        with patch.dict("os.environ", {"GOOGLE_API_KEY": "test-key"}):
            with patch("lancedb.embeddings.gemini_text.attempt_import_or_raise"):
                registry = get_registry()
                func = registry.get("gemini-text").create(
                    source_task_type="retrieval_document"
                )

                texts = ["doc text"]

                # We need mock to return only 1 embedding since we only pass 1 text
                mock_embedding = MagicMock()
                mock_embedding.values = [0.3] * 768
                self.mock_response.embeddings = [mock_embedding]

                embeddings = func.generate_embeddings(
                    texts, task_type="retrieval_document"
                )

                # Check call arguments for retrieval_document
                call_kwargs = self.mock_client.models.embed_content.call_args.kwargs
                assert call_kwargs["contents"][0] == {
                    "parts": [{"text": "Embedding of a document"}, {"text": "doc text"}]
                }
                mock_types.EmbedContentConfig.assert_called_once_with(
                    output_dimensionality=768, task_type="RETRIEVAL_DOCUMENT"
                )

                assert len(embeddings) == 1
                assert np.allclose(embeddings[0], 0.3)

    def test_custom_dimension(self):
        """Test that custom dimension (dim) can be configured and passed to config."""
        with patch.dict("os.environ", {"GOOGLE_API_KEY": "test-key"}):
            with patch("lancedb.embeddings.gemini_text.attempt_import_or_raise"):
                registry = get_registry()
                func = registry.get("gemini-text").create(dim=3072)

                assert func.ndims() == 3072

                texts = ["hello"]
                mock_embedding = MagicMock()
                mock_embedding.values = [0.5] * 3072
                self.mock_response.embeddings = [mock_embedding]

                _ = func.generate_embeddings(texts)

                mock_types.EmbedContentConfig.assert_called_once_with(
                    output_dimensionality=3072
                )
