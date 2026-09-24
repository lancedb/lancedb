# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
import sys
from types import SimpleNamespace

import pytest

import bench_typesafe as benchmark


def test_benchmark_counts_metrics_and_cache(tmp_path, monkeypatch):
    calls = []

    class Client:
        def __init__(self, **kwargs):
            pass

        def system_one(self, state, questions, model):
            calls.append(questions)
            return SimpleNamespace(
                model=model,
                answers={
                    key: SimpleNamespace(
                        noul=float(
                            (
                                state["document"]
                                if "document" in state
                                else question["instructions"]["document"]
                            )
                            == "answer"
                        )
                    )
                    for key, question in reversed(questions.items())
                },
            )

    monkeypatch.setitem(
        sys.modules, "typesafe_sdk", SimpleNamespace(TypeSafeClient=Client)
    )
    monkeypatch.setattr(
        benchmark.importlib.metadata, "version", lambda _: "test-version"
    )
    row = {
        "query": "question",
        "answer": "answer",
        "documents": {str(i): "answer" if i == 79 else "distractor" for i in range(80)},
        "vector": list(range(40)),
        "fts": list(range(40, 80)),
    }
    args = SimpleNamespace(
        candidates=tmp_path / "candidates.json",
        cache=tmp_path / "cache",
        output=tmp_path / "result.json",
        api_key_file=None,
        model="test-model",
        max_concurrency=2,
    )
    args.candidates.write_text(json.dumps([row, row]))
    benchmark.run(args)
    result = json.loads(args.output.read_text())["results"]
    assert len(calls) == 164
    assert result["1"]["requests"] == 160
    assert result["40"]["requests"] == 4
    for mode in result.values():
        assert mode["cached_queries"] == 0
        assert mode["resolved_models"] == ["test-model"]
        assert mode["median_ms"] >= 0
        assert mode["p95_ms"] >= mode["median_ms"]
        assert [metric["hits"] for metric in mode["metrics"]] == [0, 0, 0, 2, 0, 2]
    benchmark.run(args)
    assert len(calls) == 164
    assert all(
        mode["cached_queries"] == 2
        for mode in json.loads(args.output.read_text())["results"].values()
    )
    args.max_concurrency = 3
    benchmark.run(args)
    assert len(calls) == 328


def test_cache_identity_separates_configurations():
    base = benchmark.cache_identity("candidates", "model", 32, 1, "sdk")
    batched = benchmark.cache_identity("candidates", "model", 32, 40, "sdk")
    assert base["request_format"] != batched["request_format"]
    assert base["batch_size"] != batched["batch_size"]
    assert base != benchmark.cache_identity("other-candidates", "model", 32, 1, "sdk")
    assert base != benchmark.cache_identity("candidates", "other-model", 32, 1, "sdk")
    assert base != benchmark.cache_identity("candidates", "model", 8, 1, "sdk")
    assert base != benchmark.cache_identity("candidates", "model", 32, 1, "other-sdk")


@pytest.mark.parametrize(
    "field,value",
    [
        ("scores", {}),
        ("scores", {"0": float("nan")}),
        ("scores", {"0": True}),
        ("requests", 2),
        ("seconds", -1),
    ],
)
def test_invalid_cache_rejected(field, value):
    row = {"documents": {"0": "answer"}}
    record = {"scores": {"0": 0.5}, "requests": 1, "seconds": 0.5}
    record[field] = value
    with pytest.raises(ValueError):
        benchmark.validate_record(row, record, 40)
