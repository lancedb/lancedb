# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""The provider reranker examples write only to a directory they create.

Each example runs offline. A stand-in embedding function answers for the model
the example asks the registry for, a stand-in reranker replaces the provider
client, and outbound connections are refused, so no model is downloaded and no
provider is called. Sentinel tables in the owned home directory, where these
examples used to write, and in the temporary directory must be left unchanged.
"""

import importlib.util
import socket
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import lancedb
import lancedb.embeddings
import lancedb.rerankers
import pyarrow as pa
import pytest
from lancedb.embeddings import TextEmbeddingFunction, register
from lancedb.rerankers import Reranker

EXAMPLES = Path(__file__).with_name("provider_examples.py")
RERANKERS = {
    "reranking_cohere_usage": "CohereReranker",
    "reranking_jina_usage": "JinaReranker",
    "reranking_watsonx_usage": "WatsonxReranker",
    "reranking_openai_usage": "OpenaiReranker",
    "reranking_voyageai_usage": "VoyageAIReranker",
}


@register("docs-offline-text")
class OfflineEmbedding(TextEmbeddingFunction):
    """Deterministic four-dimensional vectors, computed locally."""

    def ndims(self):
        return 4

    def generate_embeddings(self, texts):
        return [[float(len(text)), 1.0, 0.0, 0.0] for text in texts]


class OfflineReranker(Reranker):
    """Scores every row 1.0 and records which searches it reranked."""

    instances: ClassVar[list["OfflineReranker"]] = []

    def __init__(self, **kwargs):
        super().__init__()
        self.kwargs = kwargs
        self.searches = []
        OfflineReranker.instances.append(self)

    def _score(self, results, search):
        self.searches.append(search)
        drop = [c for c in ("_distance", "_score") if c in results.column_names]
        results = results.drop_columns(drop)
        scores = pa.array([1.0] * results.num_rows, pa.float32())
        return results.append_column("_relevance_score", scores)

    def rerank_vector(self, query, vector_results):
        return self._score(vector_results, "vector")

    def rerank_fts(self, query, fts_results):
        return self._score(fts_results, "fts")

    def rerank_hybrid(self, query, vector_results, fts_results):
        merged = self.merge_results(vector_results, fts_results)
        return self._score(merged, "hybrid")


def snapshot(connect, uri):
    db = connect(str(uri))
    tables = {}
    for name in db.list_tables().tables:
        table = db.open_table(name)
        tables[name] = (table.version, table.schema, table.to_arrow().to_pylist())
    return tables


def load_examples():
    spec = importlib.util.spec_from_file_location("provider_examples", EXAMPLES)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def owned(tmp_path, monkeypatch):
    home, temp_root = tmp_path / "home", tmp_path / "tmp"
    home.mkdir()
    temp_root.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setattr(tempfile, "tempdir", str(temp_root))
    monkeypatch.chdir(tmp_path)
    # Anything the examples resolve from "~" must land in the owned home.
    assert Path("~/.lancedb").expanduser().resolve().is_relative_to(home.resolve())

    def refuse(*args, **kwargs):
        raise OSError("network access is disabled in this test")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)

    real_connect = lancedb.connect
    sentinels = [home / ".lancedb", temp_root / "existing"]
    for uri in sentinels:
        row = {"text": "keep me", "vector": [0.5, 0.5, 0.5, 0.5]}
        real_connect(str(uri)).create_table("test", [row])
    before = [snapshot(real_connect, uri) for uri in sentinels]

    registry = lancedb.embeddings.get_registry()
    monkeypatch.setattr(registry, "get", lambda name: OfflineEmbedding)
    for name in RERANKERS.values():
        monkeypatch.setattr(lancedb.rerankers, name, OfflineReranker)
    monkeypatch.setenv("JINA_API_KEY", "replaced-by-the-example")
    OfflineReranker.instances.clear()

    connected = []

    def recording_connect(uri, *args, **kwargs):
        connected.append(Path(uri).expanduser().resolve())
        return real_connect(uri, *args, **kwargs)

    monkeypatch.setattr(lancedb, "connect", recording_connect)
    return SimpleNamespace(
        home=home,
        temp_root=temp_root,
        sentinels=sentinels,
        before=before,
        connect=real_connect,
        connected=connected,
    )


@pytest.mark.parametrize("example", sorted(RERANKERS))
def test_reranker_example_writes_only_to_its_own_directory(owned, example):
    existing = set(owned.temp_root.iterdir())

    getattr(load_examples(), example)()

    after = [snapshot(owned.connect, uri) for uri in owned.sentinels]
    assert after == owned.before
    created = set(owned.temp_root.iterdir()) - existing
    assert len(created) == 1
    (own_directory,) = created
    assert len(owned.connected) == 1
    assert owned.connected[0].is_relative_to(own_directory.resolve())
    assert owned.connect(str(owned.connected[0])).list_tables().tables == ["test"]

    (reranker,) = OfflineReranker.instances
    assert reranker.searches == ["vector", "fts", "hybrid"]
    assert reranker.kwargs["api_key"] == "your-api-key"
    assert reranker.kwargs.get("project_id", "your-project-id") == "your-project-id"
