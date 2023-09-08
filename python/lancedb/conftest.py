import os

import pytest

from lancedb.embeddings import EmbeddingFunctionRegistry, TextEmbeddingFunctionModel

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


@registry.register()
class MockTextEmbeddingFunction(TextEmbeddingFunctionModel):
    """
    Return the hash of the first 10 characters
    """

    def generate_embeddings(self, row):
        return [float(hash(c)) for c in row[:10]]
