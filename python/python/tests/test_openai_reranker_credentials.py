# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pytest

from lancedb.rerankers import OpenaiReranker


@pytest.mark.parametrize(
    "explicit_key, environment_key, expected_key",
    [
        ("explicit-test-key", "environment-test-key", "explicit-test-key"),
        ("explicit-test-key", None, "explicit-test-key"),
        (None, "environment-test-key", "environment-test-key"),
    ],
)
def test_openai_reranker_api_key_precedence(
    monkeypatch, explicit_key, environment_key, expected_key
):
    pytest.importorskip("openai")
    if environment_key is None:
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    else:
        monkeypatch.setenv("OPENAI_API_KEY", environment_key)

    reranker = OpenaiReranker(api_key=explicit_key)
    client = reranker._client
    try:
        assert client.api_key == expected_key
    finally:
        client.close()


def test_openai_reranker_requires_api_key(monkeypatch):
    pytest.importorskip("openai")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(ValueError, match="OPENAI_API_KEY not set"):
        OpenaiReranker()._client
