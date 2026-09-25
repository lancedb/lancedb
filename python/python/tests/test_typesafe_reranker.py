# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
from threading import Barrier, Event, Lock
from types import SimpleNamespace
from unittest.mock import Mock

import lancedb
import pyarrow as pa
import pytest
from lancedb.index import FTS
from lancedb.rerankers import TypeSafeReranker


def response(questions, probability=0.5):
    return SimpleNamespace(
        answers={key: SimpleNamespace(noul=probability) for key in reversed(questions)}
    )


def vector_results(documents):
    return pa.table(
        {
            "text": pa.array(documents, type=pa.string()),
            "_rowid": pa.array(range(len(documents)), type=pa.uint64()),
            "_distance": pa.array(range(len(documents)), type=pa.float32()),
        }
    )


def fake_reranker(**kwargs):
    reranker = TypeSafeReranker(**kwargs)
    reranker._client = Mock()
    reranker._client.system_one.side_effect = lambda **kw: response(kw["questions"])
    return reranker


@pytest.mark.parametrize("batch_size", [0, -1, 1.5, 40.0, True, False, "40", None])
def test_invalid_batch_size(batch_size):
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        TypeSafeReranker(batch_size=batch_size)


@pytest.mark.parametrize("batch_size", [1, 7, 40])
@pytest.mark.parametrize("count", [0, 1, 39, 40, 41, 80, 81])
def test_request_counts_and_boundaries(batch_size, count):
    reranker = fake_reranker(batch_size=batch_size)
    docs = [f"candidate {i}" for i in range(count)]
    result = reranker.rerank_vector("query", vector_results(docs))
    calls = reranker._client.system_one.call_args_list
    assert len(calls) == (count + batch_size - 1) // batch_size
    assert sorted(len(call.kwargs["questions"]) for call in calls) == sorted(
        min(batch_size, count - start) for start in range(0, count, batch_size)
    )
    assert result["_rowid"].to_pylist() == list(range(count))


@pytest.mark.parametrize("batch_size", [1, 40])
@pytest.mark.parametrize(
    "criteria", [{}, {"true": "  state.document fits\nexactly", "false": "No!"}]
)
def test_payload_isolation_and_custom_prompts(batch_size, criteria):
    instructions = "Read state.document and state.query.\n Preserve this text. "
    reranker = fake_reranker(
        batch_size=batch_size,
        instructions=instructions,
        criteria=criteria,
        model_name="jev-1.13.0",
        column="body",
    )
    docs = [f"UNIQUE_CANDIDATE_{i}" for i in range(41)]
    table = vector_results(docs).rename_columns(["body", "_rowid", "_distance"])
    reranker.rerank_vector("shared query", table)
    seen = []
    for call in reranker._client.system_one.call_args_list:
        state, questions = call.kwargs["state"], call.kwargs["questions"]
        assert call.kwargs["model"] == "jev-1.13.0"
        if batch_size == 1:
            doc = state["document"]
            assert state == {"query": "shared query", "document": doc}
            assert list(questions) == ["relevance"]
            expected_instructions = instructions
        else:
            assert state == {"query": "shared query"}
        for question in questions.values():
            if batch_size > 1:
                doc = question["instructions"]["document"]
                expected_instructions = {"question": instructions, "document": doc}
            expected = {"type": "noul", "instructions": expected_instructions}
            if criteria:
                expected["criteria"] = criteria
            # Exact equality rules out other candidates anywhere in this question.
            assert question == expected
            seen.append(doc)
    assert sorted(seen) == sorted(docs)
    assert reranker.instructions == instructions
    assert reranker.criteria == criteria


def test_default_payload():
    reranker = fake_reranker()
    reranker.rerank_vector("query", vector_results(["document"]))
    reranker._client.system_one.assert_called_once_with(
        state={"query": "query", "document": "document"},
        questions={
            "relevance": {
                "type": "noul",
                "instructions": reranker.instructions,
                "criteria": reranker.criteria,
            }
        },
        model="jev-latest",
    )


def test_response_mapping_with_duplicates_and_nulls():
    reranker = fake_reranker(batch_size=2, max_concurrency=2, return_score="all")
    second_batch_scored = Event()

    def score(**kw):
        questions = kw["questions"]
        documents = [
            question["instructions"]["document"] for question in questions.values()
        ]
        if documents == ["same", "same"]:
            # Prepare the later batch first; result order must still match input.
            assert second_batch_scored.wait(timeout=10)
            scores = [0.0, 0.5]
        else:
            assert documents == ["different", "same"]
            scores = [0.75, 1.0]
        result = SimpleNamespace(
            answers={
                key: SimpleNamespace(noul=value)
                for key, value in reversed(list(zip(questions, scores)))
            }
        )
        second_batch_scored.set()
        return result

    reranker._client.system_one.side_effect = score
    result = reranker.rerank_vector(
        "query", vector_results(["same", None, "same", "different", "same"])
    )
    assert result["_rowid"].to_pylist() == [4, 3, 2, 0, 1]
    assert result["_distance"].to_pylist() == [4, 3, 2, 0, 1]
    assert result["_relevance_score"].to_pylist() == [1, 0.75, 0.5, 0, 0]
    assert reranker._client.system_one.call_count == 2


@pytest.mark.parametrize("batch_size", [1, 40])
@pytest.mark.parametrize("problem", ["missing", "unexpected", "replaced", "no_noul"])
def test_invalid_answers(batch_size, problem):
    reranker = fake_reranker(batch_size=batch_size)

    def invalid(**kw):
        result = response(kw["questions"])
        key = next(iter(result.answers))
        if problem in ("missing", "replaced"):
            del result.answers[key]
        if problem in ("unexpected", "replaced"):
            result.answers["unknown"] = SimpleNamespace(noul=0.5)
        if problem == "no_noul":
            result.answers[key] = SimpleNamespace()
        return result

    reranker._client.system_one.side_effect = invalid
    with pytest.raises(ValueError, match="invalid relevance|answer IDs"):
        reranker.rerank_vector("query", vector_results(["one", "two"]))


@pytest.mark.parametrize("batch_size", [1, 40])
@pytest.mark.parametrize(
    "probability",
    [None, True, False, "0.5", -0.01, 1.01, float("nan"), float("inf"), -float("inf")],
)
def test_invalid_probabilities(batch_size, probability):
    reranker = fake_reranker(batch_size=batch_size)
    reranker._client.system_one.side_effect = lambda **kw: response(
        kw["questions"], probability
    )
    with pytest.raises(ValueError, match="invalid relevance probability"):
        reranker.rerank_vector("query", vector_results(["one", "two"]))


@pytest.mark.parametrize("batch_size", [1, 40])
def test_api_failure_propagates(batch_size):
    reranker = fake_reranker(batch_size=batch_size)
    error = RuntimeError("SDK exhausted retries")
    reranker._client.system_one.side_effect = error
    with pytest.raises(RuntimeError) as caught:
        reranker.rerank_vector("query", vector_results(["one"]))
    assert caught.value is error
    assert reranker._client.system_one.call_count == 1


@pytest.mark.parametrize("batch_size", [1, 40])
def test_concurrency_limits_requests(batch_size):
    reranker = fake_reranker(batch_size=batch_size, max_concurrency=2)
    barrier, lock = Barrier(2), Lock()
    active = peak = 0

    def blocking(**kw):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        barrier.wait(timeout=10)
        with lock:
            active -= 1
        return response(kw["questions"])

    reranker._client.system_one.side_effect = blocking
    reranker.rerank_vector("query", vector_results(["doc"] * (6 * batch_size)))
    assert peak == 2
    assert active == 0
    assert reranker._client.system_one.call_count == 6


@pytest.mark.parametrize("batch_size", [1, 40])
@pytest.mark.parametrize("return_score", ["relevance", "all"])
@pytest.mark.parametrize("method", ["vector", "fts", "hybrid"])
@pytest.mark.parametrize("docs", [[], [None, None], [None, "", None]])
def test_empty_and_null_inputs(batch_size, return_score, method, docs):
    reranker = fake_reranker(batch_size=batch_size, return_score=return_score)
    vector = vector_results(docs)
    fts = vector.rename_columns(["text", "_rowid", "_score"])
    args = (
        (vector, fts)
        if method == "hybrid"
        else (vector if method == "vector" else fts,)
    )
    result = getattr(reranker, f"rerank_{method}")("query", *args)
    assert len(result) == len(docs)
    assert result["_relevance_score"].type == pa.float32()
    assert sorted(result["_relevance_score"].to_pylist()) == sorted(
        0 if doc is None else 0.5 for doc in docs
    )
    assert reranker._client.system_one.call_count == docs.count("")
    if return_score == "relevance":
        assert "_distance" not in result.column_names
        assert "_score" not in result.column_names


@pytest.mark.parametrize("batch_size", [1, 40])
@pytest.mark.parametrize("return_score", ["relevance", "all"])
def test_hybrid_deduplicates_by_row_id_and_keeps_scores(batch_size, return_score):
    reranker = fake_reranker(batch_size=batch_size, return_score=return_score)
    vector = vector_results(["same", "same", "third"])
    fts = vector.slice(1).rename_columns(["text", "_rowid", "_score"])
    result = reranker.rerank_hybrid("query", vector.slice(0, 2), fts)
    assert result["_rowid"].to_pylist() == [0, 1, 2]
    assert (
        sum(
            len(call.kwargs["questions"])
            for call in reranker._client.system_one.call_args_list
        )
        == 3
    )
    assert reranker._client.system_one.call_count == (3 if batch_size == 1 else 1)
    if return_score == "all":
        assert result["_distance"].to_pylist() == [0, 1, None]
        assert result["_score"].to_pylist() == [None, 1, 2]
    else:
        assert result.column_names == ["text", "_rowid", "_relevance_score"]


@pytest.mark.asyncio
@pytest.mark.parametrize("batch_size", [1, 40])
async def test_async_search_integrations(tmp_path, batch_size):
    db = await lancedb.connect_async(tmp_path)
    table = await db.create_table(
        "docs",
        [
            {"text": "cat naps", "vector": [1.0, 0.0]},
            {"text": "cat plays", "vector": [0.0, 1.0]},
        ],
    )
    await table.create_index("text", config=FTS())
    reranker = fake_reranker(batch_size=batch_size)
    queries = [
        table.query().nearest_to([1.0, 0.0]).rerank(reranker, query_string="cat"),
        table.query().nearest_to_text("cat").rerank(reranker),
        table.query().nearest_to([1.0, 0.0]).nearest_to_text("cat").rerank(reranker),
    ]
    for query in queries:
        reranker._client.system_one.reset_mock()
        result = await query.with_row_id().to_arrow()
        assert sorted(result["_rowid"].to_pylist()) == [0, 1]
        assert result["_relevance_score"].to_pylist() == [0.5, 0.5]
        assert "_distance" not in result.column_names
        assert "_score" not in result.column_names
        assert reranker._client.system_one.call_count == (2 if batch_size == 1 else 1)


@pytest.mark.parametrize("batch_size", [1, 40])
@pytest.mark.parametrize("retry_once", [False, True])
def test_sdk_transport_and_retries(batch_size, retry_once):
    sdk = pytest.importorskip("typesafe_sdk")
    httpx = pytest.importorskip("httpx2")
    payloads = []

    def handle(request):
        payload = json.loads(request.content)
        payloads.append(payload)
        if retry_once and len(payloads) == 1:
            return httpx.Response(503, headers={"retry-after-ms": "0"})
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13.0",
                "usage": {},
                "answers": {
                    key: {"type": "noul", "noul": 0.5}
                    for key in reversed(payload["questions"])
                },
            },
        )

    with sdk.TypeSafeClient(
        api_key="test-key", transport=httpx.MockTransport(handle)
    ) as client:
        reranker = TypeSafeReranker(batch_size=batch_size, max_concurrency=1)
        reranker._client = client
        result = reranker.rerank_vector("query", vector_results(["document"] * 80))
    assert len(payloads) == (80 if batch_size == 1 else 2) + int(retry_once)
    assert result["_relevance_score"].to_pylist() == [0.5] * 80
    if retry_once:
        assert payloads[0] == payloads[1]
    for payload in payloads:
        assert len(payload["questions"]) == batch_size
        if batch_size == 40:
            assert payload["state"] == {"query": "query"}
            assert all(
                question["instructions"]["document"] == "document"
                for question in payload["questions"].values()
            )
