# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
import subprocess
import sys
from types import SimpleNamespace

import lancedb
import pytest
from lancedb.embeddings import CohereEmbeddingFunction, get_registry
from lancedb.pydantic import LanceModel
from utils import start_server_thread


@pytest.fixture
def foundry_cohere(monkeypatch):
    pytest.importorskip("cohere", minversion="5.15.0")
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    monkeypatch.setenv("COHERE_API_KEY", "test-hosted-key")
    monkeypatch.setenv("TEST_FOUNDRY_COHERE_KEY", "test-foundry-key")
    registry = get_registry()
    monkeypatch.setattr(registry, "_allowed_endpoints", set())
    monkeypatch.setattr(registry, "_variables", {})
    registry.set_var("foundry-key", os.environ["TEST_FOUNDRY_COHERE_KEY"])
    requests = []
    statuses = {}

    class EmbedHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append((self.path, self.headers.get("Authorization"), body))
            status = statuses.pop(body["texts"][0], None)
            if status is not None:
                self.send_response(status)
                if status == 307:
                    self.send_header("Location", "/redirect-target/v2/embed")
                self.end_headers()
                return
            vectors = []
            for text in body["texts"]:
                matches = (body["input_type"], text) in {
                    ("search_document", "matching document"),
                    ("search_query", "matching query"),
                }
                if text == "wrong dimension":
                    vectors.append([0.0])
                else:
                    vectors.append(
                        ([1.0, 0.0] if matches else [0.0, 1.0]) + [0.0] * 254
                    )
            response = json.dumps(
                {
                    "id": "fixture",
                    "embeddings": {"float": vectors},
                    "texts": body["texts"],
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(response)

        def log_message(self, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), EmbedHandler) as server:
        thread = start_server_thread(server)
        try:
            options = {
                "name": "my-embed-v4-deployment",
                "base_url": f"http://127.0.0.1:{server.server_port}/providers/cohere",
                "output_dimension": 256,
                "api_key": "$var:foundry-key",
            }
            yield options, requests, statuses
        finally:
            server.shutdown()
            thread.join()


def test_foundry_cohere_retrieval_and_endpoint_trust(
    foundry_cohere, monkeypatch, tmp_path
):
    import httpx

    options, requests, _ = foundry_cohere
    func = get_registry().get("cohere").create(**options)
    request_timeouts = []
    handle_request = httpx.HTTPTransport.handle_request

    def record_request_timeout(transport, request):
        request_timeouts.append(request.extensions.get("timeout"))
        return handle_request(transport, request)

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", record_request_timeout)

    class Document(LanceModel):
        text: str = func.SourceField()
        vector: list[float] = func.VectorField()

    db = lancedb.connect(tmp_path)
    table = db.create_table("docs", schema=Document)
    table.add([{"text": "distractor"}, {"text": "matching document"}])
    reopened = lancedb.connect(tmp_path).open_table("docs")
    assert reopened.schema.field("vector").type.list_size == 256
    metadata = reopened.schema.metadata[b"embedding_functions"]
    assert b"test-foundry-key" not in metadata
    assert json.loads(metadata) == [
        {
            "name": "cohere",
            "model": options,
            "source_column": "text",
            "vector_column": "vector",
        }
    ]
    assert reopened.search("matching query").limit(1).to_list()[0]["text"] == (
        "matching document"
    )
    assert (
        request_timeouts
        == [{"connect": 300, "read": 300, "write": 300, "pool": 300}] * 2
    )

    # A fresh interpreter has the key but must separately trust the stored URL.
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os
import sys

import lancedb
from lancedb.embeddings import UntrustedEmbeddingConfigError, get_registry

registry = get_registry()
registry.set_var("foundry-key", os.environ["TEST_FOUNDRY_COHERE_KEY"])
db = lancedb.connect(sys.argv[1])
try:
    db.open_table("docs").search("matching query").limit(1).to_list()
except UntrustedEmbeddingConfigError as error:
    assert sys.argv[2] in str(error)
else:
    raise AssertionError("The stored endpoint needs explicit approval")
registry.allow_endpoint(sys.argv[2])
assert db.open_table("docs").search("matching query").limit(1).to_list()[0][
    "text"
] == "matching document"
""",
            str(tmp_path),
            options["base_url"],
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert child.returncode == 0, child.stderr
    assert requests == [
        (
            "/providers/cohere/v2/embed",
            "Bearer test-foundry-key",
            {
                "model": "my-embed-v4-deployment",
                "texts": texts,
                "input_type": input_type,
                "output_dimension": 256,
                "embedding_types": ["float"],
            },
        )
        for input_type, texts in [
            ("search_document", ["distractor", "matching document"]),
            ("search_query", ["matching query"]),
            ("search_query", ["matching query"]),
        ]
    ]


def test_foundry_cohere_retries_only_failed_batch(foundry_cohere, monkeypatch):
    options, requests, statuses = foundry_cohere
    backoffs = []
    monkeypatch.setattr(
        "lancedb.embeddings.utils.time", SimpleNamespace(sleep=backoffs.append)
    )
    func = CohereEmbeddingFunction.create(**options, max_retries=1)
    texts = [f"document {index}" for index in range(96)] + ["matching document"]
    statuses["matching document"] = 429
    vectors = func.compute_source_embeddings_with_retry(texts)
    assert [body["texts"] for _, _, body in requests] == [
        texts[:96],
        texts[96:],
        texts[96:],
    ]
    assert len(backoffs) == 1
    assert [vector[:2] for vector in vectors] == [[0.0, 1.0]] * 96 + [[1.0, 0.0]]


def test_foundry_cohere_rejects_invalid_inputs_and_vectors(foundry_cohere, monkeypatch):
    options, requests, _ = foundry_cohere
    monkeypatch.setattr("lancedb.embeddings.utils.time.sleep", pytest.fail)
    func = CohereEmbeddingFunction.create(**options)
    with pytest.raises(ValueError, match="unexpected dimension"):
        func.compute_query_embeddings_with_retry("wrong dimension")
    with pytest.raises(ValueError, match="requires string inputs"):
        func.compute_source_embeddings_with_retry([b"image"])
    assert len(requests) == 1


@pytest.mark.parametrize(
    "input_type", ["search_document", "search_query"], ids=["add", "search"]
)
def test_foundry_cohere_does_not_retry_or_follow_endpoint_redirect(
    foundry_cohere, monkeypatch, tmp_path, input_type
):
    from cohere.core.api_error import ApiError

    options, requests, statuses = foundry_cohere
    func = CohereEmbeddingFunction.create(**options, max_retries=2)

    class Document(LanceModel):
        text: str = func.SourceField()
        vector: list[float] = func.VectorField()

    table = lancedb.connect(tmp_path).create_table("docs", schema=Document)
    table.add([{"text": "seed", "vector": [0.0] * 256}])
    backoffs = []
    monkeypatch.setattr(
        "lancedb.embeddings.utils.time", SimpleNamespace(sleep=backoffs.append)
    )
    statuses["redirected text"] = 307

    if input_type == "search_document":
        with pytest.raises(RuntimeError, match=r"ApiError:.*status_code: 307"):
            table.add([{"text": "redirected text"}])
    else:
        with pytest.raises(ApiError) as error:
            table.search("redirected text").limit(1).to_list()
        assert error.value.status_code == 307

    assert backoffs == []
    assert [
        (path, body["input_type"], body["texts"]) for path, _, body in requests
    ] == [("/providers/cohere/v2/embed", input_type, ["redirected text"])]


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"name": ""}, "deployed Cohere model"),
        ({"output_dimension": None}, "requires output_dimension and api_key"),
        ({"output_dimension": 0}, "greater than 0"),
        ({"api_key": None}, "requires output_dimension and api_key"),
        ({"api_key": "test-plaintext-key"}, "cannot be set to a hardcoded value"),
        ({"api_key": "$var:foundry-key:test-plaintext-key"}, "without a default"),
        ({"query_input_type": "classification"}, "search_query for queries"),
        ({"base_url": None}, "require base_url"),
        ({"base_url": "http://user:password@127.0.0.1"}, "without credentials"),
    ],
)
def test_foundry_cohere_validates_config(monkeypatch, overrides, message):
    registry = get_registry()
    monkeypatch.setattr(registry, "_variables", {"foundry-key": "test-foundry-key"})
    options = {
        "name": "embed-v-4-0",
        "base_url": "http://127.0.0.1/providers/cohere",
        "output_dimension": 256,
        "api_key": "$var:foundry-key",
    }
    with pytest.raises(ValueError, match=message):
        registry.get("cohere").create(**(options | overrides))


def test_cohere_legacy_metadata_and_v1_client(monkeypatch):
    requests = []

    class LegacyClient:
        def __init__(self, key):
            assert key == "test-hosted-key"

        def embed(self, **body):
            requests.append(body)
            return SimpleNamespace(embeddings=[[1.0, 2.0]])

    monkeypatch.setitem(sys.modules, "cohere", SimpleNamespace(Client=LegacyClient))
    monkeypatch.setenv("COHERE_API_KEY", "test-hosted-key")
    monkeypatch.setattr(CohereEmbeddingFunction, "client", None)
    registry = get_registry()
    func = registry.get("cohere").create()
    assert func.ndims() == 1024
    assert func.safe_model_dump() == {}
    assert func.compute_source_embeddings(["default document"]) == [[1.0, 2.0]]

    # This schema predates the custom endpoint parameters.
    stored = {
        "name": "cohere",
        "model": {"name": "embed-multilingual-v2.0", "query_input_type": "clustering"},
        "source_column": "text",
        "vector_column": "vector",
    }
    metadata = {b"embedding_functions": json.dumps([stored]).encode()}
    old_func = registry.parse_functions(metadata)["vector"].function
    assert old_func.ndims() == 768
    assert old_func.safe_model_dump() == stored["model"]
    assert old_func.compute_query_embeddings("legacy query") == [[1.0, 2.0]]
    assert requests == [
        {
            "texts": ["default document"],
            "model": "embed-multilingual-v3.0",
            "input_type": "search_document",
        },
        {
            "texts": ["legacy query"],
            "model": "embed-multilingual-v2.0",
            "input_type": "clustering",
        },
    ]
