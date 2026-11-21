# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import sys
from types import ModuleType, SimpleNamespace

import numpy as np
import pytest

from lancedb.embeddings import get_registry


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


def _model(dim, raw):
    model = get_registry().get("gemini-text").create(dim=dim)

    class Models:
        def embed_content(self, model, contents, config):
            return SimpleNamespace(
                embeddings=[SimpleNamespace(values=list(raw)) for _ in contents]
            )

    model.__dict__["client"] = SimpleNamespace(models=Models())
    return model


def test_truncated_embeddings_are_normalized(fake_genai):
    out = _model(4, [3.0, 4.0, 0.0, 0.0]).compute_source_embeddings(["a", "b"])
    for vec in out:
        assert np.linalg.norm(vec) == pytest.approx(1.0)
    assert out[0].tolist() == pytest.approx([0.6, 0.8, 0.0, 0.0])


def test_full_dimension_embeddings_are_left_untouched(fake_genai):
    raw = [3.0, 4.0] + [0.0] * 3070
    out = _model(3072, raw).compute_source_embeddings(["a"])
    assert out[0].tolist() == raw
