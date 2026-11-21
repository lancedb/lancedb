# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import io
import json

import pytest

from lancedb.embeddings import get_registry


class _FakeBedrockClient:
    """Mimics bedrock-runtime, which rejects an empty ``inputText``."""

    def __init__(self):
        self.calls = []

    def invoke_model(self, body, modelId, accept, contentType):
        payload = json.loads(body)
        self.calls.append(payload)
        text = payload["inputText"]
        if not text:
            raise ValueError("inputText: Minimum length is 1")
        body = json.dumps({"embedding": [float(len(text))]}).encode()
        return {"body": io.BytesIO(body)}


@pytest.mark.parametrize("empty", [None, ""])
def test_empty_or_null_texts_are_skipped(empty):
    func = get_registry().get("bedrock-text").create()
    client = _FakeBedrockClient()
    func.__dict__["client"] = client

    result = func.compute_source_embeddings(["alpha", empty, "be"])

    assert result == [[5.0], None, [2.0]]
    assert [c["inputText"] for c in client.calls] == ["alpha", "be"]
