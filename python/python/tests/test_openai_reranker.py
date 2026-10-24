# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
from types import SimpleNamespace

import pyarrow as pa
import pytest

from lancedb.rerankers import OpenaiReranker


def reranker_replying(documents):
    """Return an OpenaiReranker whose chat client replies with these documents."""
    reply = json.dumps({"documents": documents})

    def create(**kwargs):
        message = SimpleNamespace(content=reply)
        return SimpleNamespace(choices=[SimpleNamespace(message=message)])

    reranker = OpenaiReranker(api_key="test-key")
    reranker.__dict__["_client"] = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )
    return reranker


def vector_results():
    return pa.table(
        {
            "id": [1, 2, 3],
            "text": ["dogs bark", "cats purr", "birds sing"],
            "_distance": [0.1, 0.2, 0.3],
        }
    )


def test_scores_stay_with_their_rows_when_the_model_reorders():
    reranker = reranker_replying(
        [
            {"content": "cats purr", "relevance_score": 0.9},
            {"content": "birds sing", "relevance_score": 0.5},
            {"content": "dogs bark", "relevance_score": 0.1},
        ]
    )

    result = reranker.rerank_vector("which animal purrs?", vector_results())

    assert result.select(["id", "text"]).to_pylist() == [
        {"id": 2, "text": "cats purr"},
        {"id": 3, "text": "birds sing"},
        {"id": 1, "text": "dogs bark"},
    ]
    assert result["_relevance_score"].to_pylist() == pytest.approx([0.9, 0.5, 0.1])


def test_a_row_the_model_leaves_out_raises():
    reranker = reranker_replying(
        [
            {"content": "cats purr", "relevance_score": 0.9},
            {"content": "dogs bark", "relevance_score": 0.1},
        ]
    )

    with pytest.raises(ValueError, match="1 of 3"):
        reranker.rerank_vector("which animal purrs?", vector_results())
