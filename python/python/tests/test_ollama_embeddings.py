# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json

import httpx
import numpy as np
import pyarrow as pa
import pytest

from lancedb.embeddings import get_registry


@pytest.fixture
def ollama_embeddings():
    pytest.importorskip("ollama")
    requests = []

    def respond(request):
        body = json.loads(request.content)
        requests.append(body)
        if body["model"] == "missing-model":
            return httpx.Response(404, json={"error": "model not found"})
        return httpx.Response(
            200,
            json={
                "embeddings": [
                    [float(index), float(len(text))]
                    for index, text in enumerate(body["input"])
                ]
            },
        )

    function = (
        get_registry()
        .get("ollama")
        .create(
            name="nomic-embed-text",
            options={"num_ctx": 4096},
            keep_alive="10m",
            ollama_client_kwargs={
                "transport": httpx.MockTransport(respond),
                "trust_env": False,
            },
        )
    )
    yield function, requests
    function._ollama_client._client.close()


@pytest.mark.parametrize(
    "texts",
    [
        pytest.param(["hello", "世界"], id="list"),
        pytest.param(np.array(["hello", "世界"]), id="numpy-unicode"),
        pytest.param(np.array(["hello", "世界"], dtype=object), id="numpy-object"),
        pytest.param(np.array(["hello", "unused", "世界"])[::2], id="numpy-slice"),
        pytest.param(pa.array(["hello", "世界"]), id="arrow"),
        pytest.param(pa.chunked_array([["hello"], ["世界"]]), id="chunked-arrow"),
    ],
)
def test_ollama_source_embeddings_inputs(ollama_embeddings, texts):
    function, requests = ollama_embeddings

    assert function.compute_source_embeddings(texts) == [[0.0, 5.0], [1.0, 2.0]]
    assert requests == [
        {
            "model": "nomic-embed-text",
            "input": ["hello", "世界"],
            "options": {"num_ctx": 4096},
            "keep_alive": "10m",
        }
    ]


def test_ollama_generate_embeddings_empty_array(ollama_embeddings):
    function, requests = ollama_embeddings

    assert function.generate_embeddings(np.array([], dtype=str)) == []
    assert len(requests) == 1
    assert requests[0]["input"] == []


def test_ollama_query_embeddings(ollama_embeddings):
    function, requests = ollama_embeddings

    assert function.compute_query_embeddings("hello") == [[0.0, 5.0]]
    assert len(requests) == 1
    assert requests[0]["input"] == ["hello"]


def test_ollama_source_embeddings_preserves_api_error(ollama_embeddings):
    ollama = pytest.importorskip("ollama")
    function, requests = ollama_embeddings
    function.name = "missing-model"

    with pytest.raises(ollama.ResponseError, match="model not found") as error:
        function.compute_source_embeddings(np.array(["hello"]))

    assert error.value.status_code == 404
    assert len(requests) == 1
