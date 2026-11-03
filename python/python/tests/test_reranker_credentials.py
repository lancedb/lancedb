# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import sys
import types

import pytest

from lancedb.rerankers import CohereReranker, VoyageAIReranker

RERANKERS = [
    pytest.param("cohere", "COHERE_API_KEY", CohereReranker, {}, id="cohere"),
    pytest.param(
        "voyageai",
        "VOYAGE_API_KEY",
        VoyageAIReranker,
        {"model_name": "rerank-2.5"},
        id="voyageai",
    ),
]


def fake_sdk(monkeypatch, module_name):
    """Replace the SDK module with one whose Client records the API key."""
    keys = []

    def client(api_key=None):
        keys.append(api_key)
        return object()

    module = types.ModuleType(module_name)
    module.Client = client
    monkeypatch.setitem(sys.modules, module_name, module)
    return keys


@pytest.mark.parametrize("module_name, env_var, reranker_cls, kwargs", RERANKERS)
@pytest.mark.parametrize(
    "explicit_key, environment_key, expected_key",
    [
        ("explicit-test-key", "environment-test-key", "explicit-test-key"),
        ("explicit-test-key", None, "explicit-test-key"),
        (None, "environment-test-key", "environment-test-key"),
    ],
)
def test_reranker_api_key_precedence(
    monkeypatch,
    module_name,
    env_var,
    reranker_cls,
    kwargs,
    explicit_key,
    environment_key,
    expected_key,
):
    keys = fake_sdk(monkeypatch, module_name)
    if environment_key is None:
        monkeypatch.delenv(env_var, raising=False)
    else:
        monkeypatch.setenv(env_var, environment_key)

    reranker_cls(api_key=explicit_key, **kwargs)._client
    assert keys == [expected_key]


@pytest.mark.parametrize("module_name, env_var, reranker_cls, kwargs", RERANKERS)
def test_reranker_requires_api_key(
    monkeypatch, module_name, env_var, reranker_cls, kwargs
):
    keys = fake_sdk(monkeypatch, module_name)
    monkeypatch.delenv(env_var, raising=False)
    with pytest.raises(ValueError, match=f"{env_var} not set"):
        reranker_cls(**kwargs)._client
    assert keys == []
