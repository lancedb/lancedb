import os

import numpy as np
import pytest

from .embeddings import EmbeddingFunctionRegistry, TextEmbeddingFunction

# import lancedb so we don't have to in every example


@pytest.fixture(autouse=True)
def doctest_setup(monkeypatch, tmpdir):
    # disable color for doctests so we don't have to include
    # escape codes in docstrings
    monkeypatch.setitem(os.environ, "NO_COLOR", "1")
    # Explicitly set the column width
    monkeypatch.setitem(os.environ, "COLUMNS", "80")
    # Work in a temporary directory
    monkeypatch.chdir(tmpdir)


registry = EmbeddingFunctionRegistry.get_instance()


@registry.register("test")
class MockTextEmbeddingFunction(TextEmbeddingFunction):
    """
    Return the hash of the first 10 characters
    """

    def generate_embeddings(self, texts):
        return [self._compute_one_embedding(row) for row in texts]

    def _compute_one_embedding(self, row):
        emb = np.array([float(hash(c)) for c in row[:10]])
        emb /= np.linalg.norm(emb)
        return emb

    def ndims(self):
        return 10
