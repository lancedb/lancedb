import os

import pyarrow as pa
import pytest

from lancedb.embeddings import EmbeddingFunctionModel, EmbeddingFunctionRegistry

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
class MockEmbeddingFunction(EmbeddingFunctionModel):
    def __call__(self, data):
        if isinstance(data, str):
            data = [data]
        elif isinstance(data, pa.ChunkedArray):
            data = data.combine_chunks().to_pylist()
        elif isinstance(data, pa.Array):
            data = data.to_pylist()

        return [self.embed(row) for row in data]

    def embed(self, row):
        return [float(hash(c)) for c in row[:10]]
