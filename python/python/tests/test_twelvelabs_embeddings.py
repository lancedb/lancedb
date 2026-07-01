# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for the TwelveLabs Marengo embedding function.

The unit tests mock the SDK and require no network access. The integration
test is gated on TWELVELABS_API_KEY and marked ``slow``.
"""

import os
from unittest.mock import MagicMock, patch

import lancedb
import pytest

from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector


@pytest.fixture(autouse=True)
def reset_twelvelabs_client():
    """Reset the cached client before and after each test."""
    from lancedb.embeddings.twelvelabs import TwelveLabsEmbeddingFunction

    TwelveLabsEmbeddingFunction.client = None
    yield
    TwelveLabsEmbeddingFunction.client = None


def _mock_client():
    """Return a mock TwelveLabs client whose embed.create yields a 512-dim vector."""
    segment = MagicMock()
    segment.float_ = [0.1] * 512
    resp = MagicMock()
    resp.text_embedding.segments = [segment]
    resp.image_embedding.segments = [segment]
    client = MagicMock()
    client.embed.create.return_value = resp
    return client


class TestTwelveLabsRegistration:
    def test_registered(self):
        assert get_registry().get("twelvelabs") is not None

    @pytest.mark.parametrize(
        "model_name,expected_dims",
        [("marengo3.0", 512), ("Marengo-retrieval-2.7", 1024)],
    )
    def test_model_dimensions(self, model_name, expected_dims):
        func = get_registry().get("twelvelabs").create(name=model_name)
        assert func.ndims() == expected_dims

    def test_default_model(self):
        func = get_registry().get("twelvelabs").create()
        assert func.name == "marengo3.0"
        assert func.ndims() == 512

    def test_unsupported_model_raises(self):
        func = get_registry().get("twelvelabs").create(name="not-a-model")
        with pytest.raises(ValueError, match="not supported"):
            func.ndims()

    def test_api_key_is_sensitive(self):
        func = get_registry().get("twelvelabs").create()
        assert "api_key" in func.sensitive_keys()

    def test_missing_api_key_raises(self):
        func = get_registry().get("twelvelabs").create(name="marengo3.0")
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="TWELVELABS_API_KEY"):
                func.compute_query_embeddings("hello")

    def test_text_embedding_uses_text_field(self):
        client = _mock_client()
        func = get_registry().get("twelvelabs").create(name="marengo3.0")
        with patch.object(type(func), "_get_client", return_value=client):
            out = func.compute_source_embeddings(["hello world"])
        assert len(out) == 1 and len(out[0]) == 512
        client.embed.create.assert_called_once_with(
            model_name="marengo3.0", text="hello world"
        )

    def test_url_input_uses_image_url_field(self):
        client = _mock_client()
        func = get_registry().get("twelvelabs").create(name="marengo3.0")
        with patch.object(type(func), "_get_client", return_value=client):
            func.compute_query_embeddings("https://example.com/cat.jpg")
        client.embed.create.assert_called_once_with(
            model_name="marengo3.0", image_url="https://example.com/cat.jpg"
        )


@pytest.mark.slow
@pytest.mark.skipif(
    os.environ.get("TWELVELABS_API_KEY") is None,
    reason="TWELVELABS_API_KEY not set",
)
def test_twelvelabs_embedding_function(tmp_path):
    """Integration test against the real TwelveLabs Marengo API."""
    func = get_registry().get("twelvelabs").create(name="marengo3.0", max_retries=0)

    class TextModel(LanceModel):
        text: str = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()

    db = lancedb.connect(tmp_path)
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")
    tbl.add([{"text": "a red sports car"}, {"text": "a fluffy cat"}])

    assert len(tbl.to_pandas()["vector"][0]) == func.ndims() == 512
    result = tbl.search("automobile").limit(1).to_pandas()
    assert result["text"][0] == "a red sports car"
