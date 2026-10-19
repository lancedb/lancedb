# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""
Restoring an embedding function from table metadata must not let the table grant
itself trust: remote model code and custom endpoints need approval from the
process that opens the table.
"""

import json
from unittest.mock import patch

import lancedb
import pyarrow as pa
import pytest
from lancedb.embeddings import (
    EmbeddingFunctionConfig,
    OllamaEmbeddings,
    OpenAIEmbeddings,
    SentenceTransformerEmbeddings,
    UntrustedEmbeddingConfigError,
    get_registry,
)
from lancedb.embeddings.base import UntrustedEmbeddingFunction


@pytest.fixture(autouse=True)
def fresh_trust(monkeypatch):
    registry = get_registry()
    monkeypatch.setattr(registry, "_allowed_remote_code", set())
    monkeypatch.setattr(registry, "_allowed_endpoints", set())


def metadata_for(name: str, model: dict) -> dict:
    entry = {
        "name": name,
        "model": model,
        "source_column": "text",
        "vector_column": "vector",
    }
    return {b"embedding_functions": json.dumps([entry]).encode()}


def restore(name: str, model: dict):
    return get_registry().parse_functions(metadata_for(name, model))["vector"].function


def test_sentence_transformers_does_not_trust_remote_code_by_default():
    assert SentenceTransformerEmbeddings.create().trust_remote_code is False


@pytest.mark.parametrize("name", ["sentence-transformers", "huggingface", "colbert"])
def test_restored_remote_code_requires_allowlist(name):
    model = {"name": "attacker/model", "trust_remote_code": True}
    func = restore(name, model)

    with pytest.raises(UntrustedEmbeddingConfigError, match="attacker/model"):
        func.compute_query_embeddings("hello")
    with pytest.raises(UntrustedEmbeddingConfigError):
        func.ndims()


def test_blocked_function_does_not_construct_model():
    # huggingface loads its model in __init__, so the check has to happen first.
    with patch(
        "lancedb.embeddings.transformers.attempt_import_or_raise"
    ) as attempt_import:
        restore("huggingface", {"name": "attacker/model", "trust_remote_code": True})
    attempt_import.assert_not_called()


def test_allowlisted_model_restores_with_remote_code():
    get_registry().allow_remote_code("org/model")
    func = restore(
        "sentence-transformers", {"name": "org/model", "trust_remote_code": True}
    )
    assert isinstance(func, SentenceTransformerEmbeddings)
    assert func.trust_remote_code is True


def test_allowlist_does_not_cover_other_models():
    get_registry().allow_remote_code("org/model")
    func = restore(
        "sentence-transformers", {"name": "org/other", "trust_remote_code": True}
    )
    with pytest.raises(UntrustedEmbeddingConfigError, match="org/other"):
        func.compute_query_embeddings("hello")


def test_pinned_revision_must_match():
    get_registry().allow_remote_code("org/model", revision="abc123")

    pinned = restore(
        "sentence-transformers",
        {"name": "org/model", "revision": "abc123", "trust_remote_code": True},
    )
    assert pinned.trust_remote_code is True

    for model in [
        {"name": "org/model", "revision": "def456", "trust_remote_code": True},
        {"name": "org/model", "trust_remote_code": True},
    ]:
        func = restore("sentence-transformers", model)
        with pytest.raises(UntrustedEmbeddingConfigError, match="org/model"):
            func.compute_query_embeddings("hello")


def test_restored_trust_flag_follows_allowlist_when_absent():
    # Tables written before the default changed don't record trust_remote_code.
    assert restore(
        "sentence-transformers", {"name": "org/model"}
    ).trust_remote_code is (False)
    get_registry().allow_remote_code("org/model")
    assert restore("sentence-transformers", {"name": "org/model"}).trust_remote_code


def test_created_function_records_trust_flag():
    # Otherwise a new table using the default would look like a pre-fix table,
    # where a missing flag meant True.
    conf = EmbeddingFunctionConfig(
        source_column="text",
        vector_column="vector",
        function=SentenceTransformerEmbeddings.create(name="org/model"),
    )
    assert get_registry().function_to_metadata(conf)["model"] == {
        "name": "org/model",
        "trust_remote_code": False,
    }

    get_registry().allow_remote_code("org/model")
    metadata = get_registry().get_table_metadata([conf])
    restored = get_registry().parse_functions(metadata)["vector"].function
    assert restored.trust_remote_code is False


def test_huggingface_missing_flag_keeps_false_default():
    # huggingface always defaulted to False, so approval must not flip it.
    get_registry().allow_remote_code("org/model")
    with patch("lancedb.embeddings.transformers.attempt_import_or_raise"):
        func = restore("huggingface", {"name": "org/model"})
    assert func.trust_remote_code is False


def test_restored_explicit_false_stays_false():
    get_registry().allow_remote_code("org/model")
    func = restore(
        "sentence-transformers", {"name": "org/model", "trust_remote_code": False}
    )
    assert func.trust_remote_code is False


def test_create_trusts_its_own_config(tmp_path):
    func = SentenceTransformerEmbeddings.create(
        name="org/model", trust_remote_code=True
    )
    db = lancedb.connect(tmp_path)
    schema = pa.schema(
        [pa.field("text", pa.string()), pa.field("vector", pa.list_(pa.float32(), 4))]
    )
    db.create_table(
        "t",
        schema=schema,
        embedding_functions=[
            EmbeddingFunctionConfig(
                source_column="text", vector_column="vector", function=func
            )
        ],
    )

    restored = db.open_table("t").embedding_functions["vector"].function
    assert isinstance(restored, SentenceTransformerEmbeddings)
    assert restored.trust_remote_code is True


def test_create_trusts_only_the_revision_it_selected():
    # Anyone can push a PR ref to a Hugging Face repo, so trusting a model's
    # default branch must not extend to its other revisions.
    SentenceTransformerEmbeddings.create(name="org/model", trust_remote_code=True)
    func = restore(
        "sentence-transformers",
        {"name": "org/model", "revision": "refs/pr/123", "trust_remote_code": True},
    )
    assert isinstance(func, UntrustedEmbeddingFunction)

    same = restore(
        "sentence-transformers", {"name": "org/model", "trust_remote_code": True}
    )
    assert same.trust_remote_code is True


def test_explicit_unpinned_approval_covers_any_revision():
    get_registry().allow_remote_code("org/model")
    func = restore(
        "sentence-transformers",
        {"name": "org/model", "revision": "abc123", "trust_remote_code": True},
    )
    assert func.trust_remote_code is True


def test_missing_flag_does_not_inherit_unapproved_trust_default():
    class LegacySentenceTransformer(SentenceTransformerEmbeddings):
        trust_remote_code: bool = True

    registry = get_registry()
    registry.register("legacy-trust-default")(LegacySentenceTransformer)
    try:
        func = restore("legacy-trust-default", {"name": "attacker/model"})
        assert not func.trust_remote_code
    finally:
        del registry._functions["legacy-trust-default"]


def test_trusting_one_model_does_not_trust_a_swapped_name():
    SentenceTransformerEmbeddings.create(name="org/model", trust_remote_code=True)
    func = restore(
        "sentence-transformers", {"name": "attacker/model", "trust_remote_code": True}
    )
    with pytest.raises(UntrustedEmbeddingConfigError):
        func.compute_query_embeddings("hello")


ATTACKER_URL = "https://attacker.example/v1"


@pytest.mark.parametrize(
    "name,model",
    [
        ("openai", {"base_url": ATTACKER_URL}),
        ("ollama", {"host": ATTACKER_URL}),
        ("ollama", {"ollama_client_kwargs": {"proxy": ATTACKER_URL}}),
        ("watsonx", {"url": ATTACKER_URL}),
    ],
)
def test_restored_custom_endpoint_requires_allowlist(name, model):
    func = restore(name, model)
    with pytest.raises(UntrustedEmbeddingConfigError, match="attacker.example"):
        func.compute_query_embeddings("hello")

    get_registry().allow_endpoint(ATTACKER_URL)
    assert isinstance(restore(name, model), get_registry().get(name))


def test_default_endpoints_need_no_allowlist():
    assert isinstance(restore("openai", {}), OpenAIEmbeddings)
    assert isinstance(
        restore("ollama", {"host": "http://localhost:11434"}), OllamaEmbeddings
    )


def test_endpoint_allowlist_ignores_trailing_slash():
    get_registry().allow_endpoint("http://my-host:11434/")
    assert isinstance(
        restore("ollama", {"host": "http://my-host:11434"}), OllamaEmbeddings
    )


def test_create_trusts_its_own_endpoint():
    OpenAIEmbeddings.create(base_url="http://my-proxy/v1")
    assert isinstance(
        restore("openai", {"base_url": "http://my-proxy/v1"}), OpenAIEmbeddings
    )


def test_blocked_function_round_trips_metadata():
    model = {"name": "attacker/model", "trust_remote_code": True}
    func = restore("sentence-transformers", model)
    conf = EmbeddingFunctionConfig(
        source_column="text", vector_column="vector", function=func
    )
    entry = get_registry().function_to_metadata(conf)
    assert entry["name"] == "sentence-transformers"
    assert entry["model"] == model


def test_table_with_blocked_function_still_opens(tmp_path):
    db = lancedb.connect(tmp_path)
    schema = pa.schema(
        [pa.field("text", pa.string()), pa.field("vector", pa.list_(pa.float32(), 2))],
        metadata=metadata_for(
            "sentence-transformers",
            {"name": "attacker/model", "trust_remote_code": True},
        ),
    )
    db.create_table("t", schema=schema)
    tbl = db.open_table("t")

    tbl.add([{"text": "hi", "vector": [1.0, 2.0]}])
    assert tbl.search([1.0, 2.0]).limit(1).to_list()[0]["text"] == "hi"

    with pytest.raises(UntrustedEmbeddingConfigError, match="allow_remote_code"):
        tbl.search("hi").limit(1).to_list()
