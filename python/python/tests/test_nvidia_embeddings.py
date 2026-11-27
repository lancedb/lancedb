# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
import os
from typing import Optional

import lancedb
import numpy as np
import pyarrow as pa
import pytest

from lancedb.embeddings import (
    EmbeddingFunctionConfig,
    NvidiaEmbeddings,
    UntrustedEmbeddingConfigError,
    get_registry,
)
from lancedb.embeddings.nvidia import MODEL_DIMENSIONS, NVIDIA_BASE_URL
from lancedb.pydantic import LanceModel, Vector


@pytest.fixture
def nvidia_api(monkeypatch):
    """Exercise the real OpenAI client against a deterministic NVIDIA endpoint."""
    httpx = pytest.importorskip("httpx")
    openai = pytest.importorskip("openai")
    requests, responses, clients = [], [], []
    original_client = openai.OpenAI
    monkeypatch.setenv("NVIDIA_API_KEY", "test-nvidia-key")
    registry = get_registry()
    monkeypatch.setattr(registry, "_variables", {})
    monkeypatch.setattr(registry, "_allowed_endpoints", set())

    def respond(request):
        requests.append(request)
        if responses:
            return responses.pop(0)
        body = json.loads(request.content)
        data = []
        for index, text in enumerate(body["input"]):
            vector = [0.0] * body["dimensions"]
            vector[0 if text == "alpha" else 1] = 1.0
            data.append({"index": index, "embedding": vector, "object": "embedding"})
        return httpx.Response(
            200,
            json={
                "object": "list",
                "model": body["model"],
                # The API's index, not response order, determines the input row.
                "data": data[::-1],
                "usage": {"prompt_tokens": len(data), "total_tokens": len(data)},
            },
        )

    def client(**kwargs):
        assert kwargs["max_retries"] == 0
        instance = original_client(
            **kwargs,
            http_client=httpx.Client(
                transport=httpx.MockTransport(respond), trust_env=False
            ),
        )
        clients.append(instance)
        return instance

    monkeypatch.setattr(openai, "OpenAI", client)
    yield requests, responses
    for client in clients:
        client.close()


@pytest.mark.parametrize(
    "name,dimensions", [(name, dims[0]) for name, dims in MODEL_DIMENSIONS.items()]
)
def test_hosted_models(nvidia_api, name, dimensions):
    requests, _ = nvidia_api
    func = get_registry().get("nvidia").create(name=name)
    assert isinstance(func, NvidiaEmbeddings)
    assert func.ndims() == dimensions
    assert len(func.compute_source_embeddings(["alpha"])[0]) == dimensions
    assert len(func.compute_query_embeddings("alpha")[0]) == dimensions
    for request, input_type in zip(requests, ["passage", "query"]):
        assert str(request.url) == f"{NVIDIA_BASE_URL}/embeddings"
        assert request.headers["authorization"] == "Bearer test-nvidia-key"
        assert json.loads(request.content) == {
            "model": name,
            "input": ["alpha"],
            "dimensions": dimensions,
            "encoding_format": "float",
            "input_type": input_type,
            "modality": "text",
            "truncate": "END",
        }


@pytest.mark.parametrize(
    "texts",
    [
        ["alpha", "beta", "gamma"],
        np.array(["alpha", "beta", "gamma"]),
        pa.array(["alpha", "beta", "gamma"]),
        pa.chunked_array([["alpha"], ["beta", "gamma"]]),
    ],
)
def test_batching_and_response_order(nvidia_api, texts):
    requests, _ = nvidia_api
    func = NvidiaEmbeddings.create(dim=128, batch_size=2, truncate="NONE")
    result = func.compute_source_embeddings(texts)
    assert [vector[:2] for vector in result] == [[1, 0], [0, 1], [0, 1]]
    assert [json.loads(request.content)["input"] for request in requests] == [
        ["alpha", "beta"],
        ["gamma"],
    ]
    assert all(json.loads(r.content)["truncate"] == "NONE" for r in requests)
    assert func.compute_source_embeddings([]) == []
    assert len(requests) == 2


@pytest.mark.parametrize(
    "texts",
    [
        ["alpha", None, "", "beta"],
        pa.array(["alpha", None, "", "beta"]),
    ],
)
def test_null_and_empty_inputs(nvidia_api, texts):
    requests, _ = nvidia_api
    func = NvidiaEmbeddings.create(dim=128)
    result = func.compute_source_embeddings(texts)
    assert [vector and vector[:2] for vector in result] == [[1, 0], None, None, [0, 1]]
    assert [json.loads(r.content)["input"] for r in requests] == [["alpha", "beta"]]
    assert func.compute_source_embeddings([None, ""]) == [None, None]
    assert len(requests) == 1


def test_table_with_null_text(nvidia_api, tmp_path):
    func = NvidiaEmbeddings.create(dim=128)

    class Document(LanceModel):
        text: Optional[str] = func.SourceField()
        vector: Optional[Vector(func.ndims())] = func.VectorField()

    db = lancedb.connect(tmp_path)
    table = db.create_table("docs", schema=Document)
    table.add([{"text": "alpha"}, {"text": None}], on_bad_vectors="null")
    rows = table.to_arrow().to_pylist()
    assert rows[0]["vector"][:2] == [1, 0]
    assert rows[1]["vector"] is None


@pytest.mark.parametrize("texts", [[b"image"], [["nested"]], [123]])
def test_rejects_non_text(nvidia_api, monkeypatch, texts):
    requests, _ = nvidia_api
    monkeypatch.setattr("lancedb.embeddings.utils.time.sleep", pytest.fail)
    func = NvidiaEmbeddings.create()
    with pytest.raises(ValueError, match="text strings only"):
        func.compute_source_embeddings(texts)
    with pytest.raises(ValueError, match="text strings only"):
        func.compute_source_embeddings_with_retry(texts)
    assert requests == []


@pytest.mark.parametrize(
    "options",
    [
        {"batch_size": 0},
        {"dim": 0},
        {"dim": 127},
        {"dim": 2049},
        {"name": "nvidia/nemotron-3.5-embed-8b", "dim": 4097},
        {"name": "nvidia/Nemotron-3.5-Embed-8B-BF16"},
        {"truncate": "START"},
    ],
)
def test_invalid_configuration(options):
    with pytest.raises(ValueError):
        NvidiaEmbeddings.create(**options)


@pytest.mark.parametrize(
    "data,error",
    [
        ([], "number of embeddings"),
        (None, "number of embeddings"),
        ([{"index": 1, "embedding": [0.0] * 128}], "indices"),
        ([{"embedding": [0.0] * 128}], "indices"),
        ([{"index": 0, "embedding": [0.0]}], "finite numeric"),
        ([{"index": 0, "embedding": "base64"}], "finite numeric"),
        ([{"index": 0, "embedding": ["bad"] * 128}], "finite numeric"),
        ([{"index": 0}], "finite numeric"),
    ],
)
def test_invalid_response(nvidia_api, data, error):
    import httpx

    _, responses = nvidia_api
    responses.append(httpx.Response(200, json={"data": data}))
    with pytest.raises(ValueError, match=error):
        NvidiaEmbeddings.create(dim=128).compute_source_embeddings(["alpha"])


def test_invalid_response_is_not_retried(nvidia_api, monkeypatch):
    import httpx

    requests, responses = nvidia_api
    monkeypatch.setattr("lancedb.embeddings.utils.time.sleep", pytest.fail)
    responses.append(httpx.Response(200, json={"data": []}))
    func = NvidiaEmbeddings.create(dim=128)
    with pytest.raises(ValueError, match="number of embeddings"):
        func.compute_source_embeddings_with_retry(["alpha"])
    assert len(requests) == 1


def test_missing_response_data(nvidia_api):
    import httpx

    _, responses = nvidia_api
    responses.append(httpx.Response(200, json={}))
    with pytest.raises(ValueError, match="number of embeddings"):
        NvidiaEmbeddings.create().compute_query_embeddings("alpha")


@pytest.mark.parametrize("bad_value", ["NaN", "Infinity", "-Infinity"])
def test_nonfinite_response(nvidia_api, bad_value):
    import httpx

    _, responses = nvidia_api
    # Raw JSON is needed because httpx refuses to encode non-finite numbers.
    vector = ",".join([bad_value] + ["0"] * 127)
    responses.append(
        httpx.Response(200, text='{"data":[{"index":0,"embedding":[' + vector + "]}]}")
    )
    with pytest.raises(ValueError, match="finite numeric"):
        NvidiaEmbeddings.create(dim=128).compute_source_embeddings(["alpha"])


def test_duplicate_indices(nvidia_api):
    import httpx

    _, responses = nvidia_api
    responses.append(
        httpx.Response(200, json={"data": [{"index": 0, "embedding": [0.0] * 128}] * 2})
    )
    with pytest.raises(ValueError, match="indices"):
        NvidiaEmbeddings.create(dim=128).compute_source_embeddings(["alpha", "beta"])


@pytest.mark.parametrize(
    "status,expected_calls", [(429, 2), (503, 2), (401, 1), (400, 1)]
)
def test_retry_policy(nvidia_api, monkeypatch, status, expected_calls):
    import httpx
    import openai

    requests, responses = nvidia_api
    monkeypatch.setattr("lancedb.embeddings.utils.time.sleep", lambda _: None)
    responses.append(httpx.Response(status, json={"error": {"message": "test error"}}))
    func = NvidiaEmbeddings.create(dim=128, max_retries=1)
    if expected_calls == 2:
        assert len(func.compute_query_embeddings_with_retry("alpha")[0]) == 128
    else:
        with pytest.raises(openai.APIStatusError) as error:
            func.compute_query_embeddings_with_retry("alpha")
        assert error.value.status_code == status
    assert len(requests) == expected_calls


def test_retry_resends_only_failed_batch(nvidia_api, monkeypatch):
    import httpx

    requests, responses = nvidia_api
    monkeypatch.setattr("lancedb.embeddings.utils.time.sleep", lambda _: None)
    vector = [1.0] + [0.0] * 127
    responses.append(
        httpx.Response(200, json={"data": [{"index": 0, "embedding": vector}]})
    )
    responses.append(httpx.Response(429, json={"error": {"message": "slow down"}}))
    func = NvidiaEmbeddings.create(dim=128, batch_size=1, max_retries=1)
    result = func.compute_source_embeddings_with_retry(["alpha", "beta"])
    assert [v[:2] for v in result] == [[1, 0], [0, 1]]
    assert [json.loads(r.content)["input"] for r in requests] == [
        ["alpha"],
        ["beta"],
        ["beta"],
    ]


def test_credentials_and_metadata(nvidia_api, monkeypatch):
    requests, _ = nvidia_api
    registry = get_registry()
    registry.set_var("nvidia-key", "secret-from-registry")
    options = {
        "name": "nvidia/Nemotron-3.5-Embed-8B-BF16",
        "base_url": "http://localhost:8000/v1",
        "dim": 2048,
        "api_key": "$var:nvidia-key",
        "batch_size": 2,
        "truncate": "NONE",
    }
    func = NvidiaEmbeddings.create(**options)
    config = EmbeddingFunctionConfig(
        source_column="text", vector_column="vector", function=func
    )
    schema = pa.schema([], metadata=registry.get_table_metadata([config]))
    # Round-trip the configuration through Arrow, as table schemas do.
    restored_schema = pa.ipc.read_schema(pa.BufferReader(schema.serialize()))
    metadata = restored_schema.metadata
    assert b"secret-from-registry" not in metadata[b"embedding_functions"]
    restored = registry.parse_functions(metadata)["vector"].function
    assert restored.safe_model_dump() == options
    restored.compute_source_embeddings(["alpha"])
    restored.compute_query_embeddings("alpha")
    assert str(requests[0].url) == "http://localhost:8000/v1/embeddings"
    assert requests[0].headers["authorization"] == "Bearer secret-from-registry"
    assert json.loads(requests[0].content)["model"] == options["name"]
    assert [json.loads(r.content)["input_type"] for r in requests] == [
        "passage",
        "query",
    ]

    monkeypatch.setattr(registry, "_allowed_endpoints", set())
    blocked = registry.parse_functions(metadata)["vector"].function
    with pytest.raises(UntrustedEmbeddingConfigError):
        blocked.compute_query_embeddings("alpha")
    registry.allow_endpoint(options["base_url"])
    assert isinstance(
        registry.parse_functions(metadata)["vector"].function, NvidiaEmbeddings
    )
    with pytest.raises(ValueError, match="Sensitive key"):
        NvidiaEmbeddings.create(api_key="hardcoded-secret")


def test_missing_key_and_unauthenticated_nim(nvidia_api, monkeypatch):
    from lancedb.embeddings.utils import MissingApiKeyError

    requests, _ = nvidia_api
    # The hosted key in NVIDIA_API_KEY is not sent to custom endpoints.
    func = NvidiaEmbeddings.create(
        name="nvidia/Nemotron-3.5-Embed-1B-BF16",
        base_url="http://localhost:8000/v1",
        dim=128,
    )
    func.compute_query_embeddings("alpha")
    assert requests[0].headers["authorization"] == "Bearer not-required"
    monkeypatch.delenv("NVIDIA_API_KEY")
    with pytest.raises(MissingApiKeyError, match="NVIDIA_API_KEY"):
        NvidiaEmbeddings.create().compute_query_embeddings_with_retry("alpha")
    assert len(requests) == 1


@pytest.mark.parametrize("name", MODEL_DIMENSIONS)
def test_local_table_document_query_and_reopen(nvidia_api, tmp_path, name):
    requests, _ = nvidia_api
    func = NvidiaEmbeddings.create(name=name, dim=128)

    class Document(LanceModel):
        text: str = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()

    db = lancedb.connect(tmp_path)
    table = db.create_table("docs", schema=Document, data=[{"text": "alpha"}])
    table.add([{"text": "beta"}])
    reopened = lancedb.connect(tmp_path).open_table("docs")
    assert reopened.schema.metadata == table.schema.metadata
    assert isinstance(reopened.embedding_functions["vector"].function, NvidiaEmbeddings)
    assert reopened.search("alpha").limit(1).to_list()[0]["text"] == "alpha"
    assert [json.loads(request.content)["input_type"] for request in requests] == [
        "passage",
        "passage",
        "query",
    ]


@pytest.mark.parametrize("name", MODEL_DIMENSIONS)
def test_remote_table_document_query_and_reopen(nvidia_api, name):
    from test_remote_db import mock_lancedb_connection, read_json_body, send_json

    requests, _ = nvidia_api
    func = NvidiaEmbeddings.create(name=name, dim=128)
    config = EmbeddingFunctionConfig(
        source_column="text", vector_column="vector", function=func
    )
    writes, queries = [], []

    def handler(request):
        if "/create/" in request.path or "/insert/" in request.path:
            if request.headers.get("Transfer-Encoding") == "chunked":
                body = bytearray()
                while True:
                    size = int(request.rfile.readline(), 16)
                    if size == 0:
                        request.rfile.readline()
                        break
                    body.extend(request.rfile.read(size))
                    request.rfile.read(2)
            else:
                body = request.rfile.read(int(request.headers["Content-Length"]))
            writes.append(pa.ipc.open_stream(body).read_all())
            send_json(request, {})
        elif "/describe/" in request.path:
            send_json(
                request,
                {
                    "version": 1,
                    "schema": {
                        "fields": [
                            {
                                "name": "text",
                                "type": {"type": "string"},
                                "nullable": True,
                            },
                            {
                                "name": "vector",
                                "nullable": True,
                                "type": {
                                    "type": "fixed_size_list",
                                    "length": 128,
                                    "fields": [
                                        {
                                            "name": "item",
                                            "type": {"type": "float"},
                                            "nullable": True,
                                        }
                                    ],
                                },
                            },
                        ],
                        "metadata": {
                            k.decode(): v.decode()
                            for k, v in writes[0].schema.metadata.items()
                        },
                    },
                },
            )
        elif "/query/" in request.path:
            queries.append(read_json_body(request))
            result = pa.table({"text": ["alpha"], "_distance": [0.0]})
            request.send_response(200)
            request.send_header("Content-Type", "application/vnd.apache.arrow.file")
            request.end_headers()
            with pa.ipc.new_file(request.wfile, result.schema) as writer:
                writer.write_table(result)
        else:
            request.send_response(404)
            request.end_headers()

    with mock_lancedb_connection(handler) as db:
        table = db.create_table(
            "docs", data=[{"text": "alpha"}], embedding_functions=[config]
        )
        table.add([{"text": "beta"}])
        reopened = db.open_table("docs")
        assert reopened.schema.metadata == writes[0].schema.metadata
        assert reopened.search("alpha").limit(1).to_list()[0]["text"] == "alpha"
    assert len(writes) == 2
    assert writes[0]["vector"].to_pylist()[0] == [1.0] + [0.0] * 127
    assert writes[1]["vector"].to_pylist()[0] == [0.0, 1.0] + [0.0] * 126
    assert queries[0]["vector"] == [1.0] + [0.0] * 127
    assert [json.loads(r.content)["input_type"] for r in requests] == [
        "passage",
        "passage",
        "query",
    ]


@pytest.mark.asyncio
async def test_async_local_table(nvidia_api, tmp_path):
    requests, _ = nvidia_api
    func = NvidiaEmbeddings.create(dim=128)

    class Document(LanceModel):
        text: str = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()

    with await lancedb.connect_async(tmp_path) as db:
        table = await db.create_table("docs", schema=Document)
        await table.add([{"text": "alpha"}, {"text": "beta"}])
        reopened = await db.open_table("docs")
        results = await (await reopened.search("alpha")).limit(1).to_list()
        assert results[0]["text"] == "alpha"
    assert [json.loads(r.content)["input_type"] for r in requests] == [
        "passage",
        "query",
    ]


@pytest.mark.slow
@pytest.mark.skipif(
    not os.environ.get("NVIDIA_API_KEY"), reason="NVIDIA_API_KEY not set"
)
@pytest.mark.parametrize("name", MODEL_DIMENSIONS)
def test_nvidia_live(name, tmp_path):
    """Five small requests per model, with retries disabled for free-tier tests."""
    pytest.importorskip("openai")
    func = NvidiaEmbeddings.create(name=name, max_retries=0)
    passages = ["Paris is the capital of France.", "Bananas are yellow fruit."]
    question = "What is the capital of France?"

    class Document(LanceModel):
        text: str = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()

    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "docs", schema=Document, data=[{"text": text} for text in passages]
    )
    table.add([{"text": "Saturn is a planet with rings."}])
    reopened = lancedb.connect(tmp_path).open_table("docs")
    assert reopened.schema.metadata == table.schema.metadata
    assert reopened.schema.field("vector").type.list_size == func.ndims()
    result = reopened.search(question).limit(1).to_list()[0]
    assert result["text"] == passages[0]
    assert len(result["vector"]) == func.ndims()
    assert np.isfinite(result["vector"]).all()

    reduced = NvidiaEmbeddings.create(name=name, dim=128, max_retries=0)
    vectors = reduced.compute_source_embeddings_with_retry(passages)
    query = reduced.compute_query_embeddings_with_retry(question)[0]
    assert np.shape(vectors) == (2, 128)
    assert np.shape(query) == (128,)
    assert np.dot(query, vectors[0]) > np.dot(query, vectors[1])
