# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import sys
from types import ModuleType, SimpleNamespace

import numpy as np
import pytest

import lancedb
from lancedb.embeddings import get_registry
from lancedb.embeddings.gemini_text import GeminiText
from lancedb.pydantic import LanceModel, Vector

# Leading components of the unnormalized vector returned for each text
RAW = {"a": [0.25, 0.0], "b": [0.75, 0.5]}


@pytest.fixture
def fake_genai(monkeypatch):
    google = ModuleType("google")
    genai = ModuleType("google.genai")
    types = ModuleType("google.genai.types")
    types.EmbedContentConfig = lambda **kwargs: SimpleNamespace(**kwargs)
    genai.types = types
    google.genai = genai
    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.genai", genai)
    monkeypatch.setitem(sys.modules, "google.genai.types", types)

    def embed_content(model, contents, config):
        embeddings = []
        for content in contents:
            raw = RAW[content["parts"][-1]["text"]]
            values = raw + [0.0] * (config.output_dimensionality - len(raw))
            embeddings.append(SimpleNamespace(values=values))
        return SimpleNamespace(embeddings=embeddings)

    client = SimpleNamespace(models=SimpleNamespace(embed_content=embed_content))
    monkeypatch.setattr(GeminiText, "client", property(lambda self: client))


def _embed(**kwargs):
    model = get_registry().get("gemini-text").create(**kwargs)
    return model.compute_source_embeddings(["b"])[0]


@pytest.mark.parametrize(
    "name", ["gemini-embedding-001", "models/gemini-embedding-001"]
)
def test_normalize_truncated_embeddings(fake_genai, name):
    out = _embed(name=name, normalize=True)
    assert np.linalg.norm(out) == pytest.approx(1.0)
    assert out[:2].tolist() == pytest.approx([0.75 / 0.8125**0.5, 0.5 / 0.8125**0.5])


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"normalize": False},
        {"normalize": True, "dim": 3072},
        {"normalize": True, "name": "models/embedding-001"},
    ],
)
def test_embeddings_left_as_returned(fake_genai, kwargs):
    assert _embed(**kwargs)[:2].tolist() == RAW["b"]


@pytest.mark.parametrize("normalize", [None, True])
def test_search_and_append_after_reopen(fake_genai, tmp_path, normalize):
    kwargs = {} if normalize is None else {"normalize": normalize}
    model = get_registry().get("gemini-text").create(**kwargs)

    class Row(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    db = lancedb.connect(tmp_path)
    db.create_table("t", schema=Row).add([{"text": "a"}])

    table = lancedb.connect(tmp_path).open_table("t")
    assert table.search("a").limit(1).to_list()[0]["text"] == "a"

    table.add([{"text": "b"}])
    result = table.search("a").limit(2).to_list()
    assert [row["text"] for row in result] == ["a", "b"]
    assert result[0]["_distance"] == pytest.approx(0.0)
    for row in result:
        expected = np.array(RAW[row["text"]])
        if normalize:
            expected = expected / np.linalg.norm(expected)
        assert row["vector"][:2] == pytest.approx(expected.tolist())
