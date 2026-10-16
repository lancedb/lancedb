# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""The published reranker examples in test_integrations.py write only to a directory they create.

Each example runs as readers see it: its marked region is extracted the way the snippet
generator extracts it, saved as a script and run offline. A stand-in embedding function answers for the
sentence-transformers model, stand-in rerankers replace the model-backed AnswerDotAI, ColBERT
and cross-encoder rerankers, and outbound connections are refused. The RRF, linear-combination
and MRR rerankers need no model and run unchanged. Sentinel tables in the owned home directory,
where these examples used to write, and in the temporary directory must be left unchanged.
"""

import importlib.util
import runpy
import sys
from pathlib import Path

import lancedb.rerankers
import pytest
import test_provider_examples as provider

owned = provider.owned
SOURCE = Path(__file__).with_name("test_integrations.py")
GENERATOR = Path(__file__).parents[1] / "mdx_snippets_gen.py"
MODEL_RERANKERS = {
    "reranking_answerdotai_usage": "AnswerdotaiRerankers",
    "reranking_colbert_usage": "ColbertReranker",
    "reranking_cross_encoder_usage": "CrossEncoderReranker",
}
LOCAL_RERANKERS = [
    "reranking_linear_combination_usage",
    "reranking_mrr_usage",
    "reranking_rrf_usage",
]


def load_generator():
    if "mdx_snippets_gen" not in sys.modules:
        spec = importlib.util.spec_from_file_location("mdx_snippets_gen", GENERATOR)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module  # its dataclasses resolve annotations by module
        spec.loader.exec_module(module)
    return sys.modules["mdx_snippets_gen"]


def reader_code(name):
    """The example exactly as the snippet generator publishes it."""
    generator = load_generator()
    start, end = generator.markers_for_lang("py")
    lines = SOURCE.read_text().splitlines(keepends=True)
    snippet = generator.parse_snippets(lines, start, end)[name]
    return generator.snippet_text(generator.dedent(snippet))


def run_example(owned, tmp_path, name):
    script = tmp_path / "scripts" / f"{name}.py"
    script.parent.mkdir(exist_ok=True)
    script.write_text(reader_code(name))
    existing = set(owned.temp_root.iterdir())
    namespace = runpy.run_path(str(script))

    after = [provider.snapshot(owned.connect, uri) for uri in owned.sentinels]
    assert after == owned.before
    created = set(owned.temp_root.iterdir()) - existing
    assert len(created) == 1
    (own_directory,) = created
    assert len(owned.connected) == 1
    assert owned.connected[0].is_relative_to(own_directory.resolve())
    assert owned.connect(str(owned.connected[0])).list_tables().tables == ["test"]
    return namespace


@pytest.mark.parametrize("name", sorted(MODEL_RERANKERS))
def test_model_reranker_example_writes_only_to_its_own_directory(
    owned, monkeypatch, tmp_path, name
):
    monkeypatch.setattr(
        lancedb.rerankers, MODEL_RERANKERS[name], provider.OfflineReranker
    )

    namespace = run_example(owned, tmp_path, name)

    (reranker,) = provider.OfflineReranker.instances
    assert reranker.searches == ["vector", "fts", "hybrid"]
    assert namespace["result"]


@pytest.mark.parametrize("name", LOCAL_RERANKERS)
def test_local_reranker_example_writes_only_to_its_own_directory(owned, tmp_path, name):
    namespace = run_example(owned, tmp_path, name)

    assert namespace["result"]
    assert all("_relevance_score" in row for row in namespace["result"])
    if name == "reranking_mrr_usage":
        assert namespace["combined"].num_rows > 0
