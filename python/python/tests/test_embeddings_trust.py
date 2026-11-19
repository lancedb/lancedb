# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""
Restoring an embedding function from table metadata must not let the table grant
itself trust: remote model code and custom endpoints need approval from the
process that opens the table.
"""

import json
import warnings
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import lancedb
import pyarrow as pa
import pytest
from pydantic import AliasChoices, AliasPath, ConfigDict, Field, model_validator
from lancedb.embeddings import (
    ColPaliEmbeddings,
    EmbeddingFunctionConfig,
    OCIGenAIEmbeddings,
    OllamaEmbeddings,
    OpenAIEmbeddings,
    OpenClipEmbeddings,
    SentenceTransformerEmbeddings,
    TextEmbeddingFunction,
    UntrustedEmbeddingConfigError,
    get_registry,
)
from lancedb.embeddings.base import UntrustedEmbeddingFunction


@pytest.fixture(autouse=True)
def fresh_trust(monkeypatch):
    registry = get_registry()
    monkeypatch.setattr(registry, "_allowed_remote_code", set())
    monkeypatch.setattr(registry, "_allowed_endpoints", set())
    monkeypatch.setattr(registry, "_allowed_functions", set())
    monkeypatch.setattr(registry, "_trusted_configs", set())


@contextmanager
def _ignore_warnings():
    # gte and siglip warn that they are deprecated.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        yield


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
    registry.register("legacy-trust-default", safe_to_restore=True)(
        LegacySentenceTransformer
    )
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
OCI_REGION_ENDPOINT = (
    "https://inference.generativeai.eu-frankfurt-1.oci.oraclecloud.com"
)


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


@pytest.mark.parametrize(
    "model,endpoint",
    [
        ({"service_endpoint": ATTACKER_URL}, ATTACKER_URL),
        ({"region": "eu-frankfurt-1"}, OCI_REGION_ENDPOINT),
    ],
)
def test_restored_oci_custom_endpoint_requires_allowlist(model, endpoint):
    # Its settings choose local credentials, so it always needs approval.
    get_registry().allow_function("oci-genai")
    func = restore("oci-genai", model)
    assert isinstance(func, UntrustedEmbeddingFunction)
    with pytest.raises(UntrustedEmbeddingConfigError, match="allow_endpoint"):
        func.compute_query_embeddings("hello")

    get_registry().allow_endpoint(endpoint)
    restored = restore("oci-genai", model)
    assert isinstance(restored, OCIGenAIEmbeddings)
    assert restored.endpoints() == [endpoint]


def test_restored_oci_default_endpoint_needs_no_allowlist():
    get_registry().allow_function("oci-genai")
    func = restore("oci-genai", {})
    assert isinstance(func, OCIGenAIEmbeddings)
    assert func.endpoints() == []


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


@pytest.fixture
def fresh_variables(monkeypatch):
    registry = get_registry()
    monkeypatch.setattr(registry, "_variables", {})
    monkeypatch.setattr(registry, "_secret_variables", set())
    return registry


def test_secret_variable_not_shown_in_endpoint_error(fresh_variables):
    fresh_variables.set_var("api_key", "SECRET123")
    with pytest.raises(ValueError) as exc_info:
        restore("openai", {"name": "m", "base_url": "$var:api_key"})
    assert "SECRET123" not in str(exc_info.value)

    # Indistinguishable from a variable that doesn't exist.
    with pytest.raises(ValueError) as missing_info:
        restore("openai", {"name": "m", "base_url": "$var:no_such_var"})
    assert str(exc_info.value).replace("api_key", "no_such_var") == str(
        missing_info.value
    )


def test_secret_variable_names_cannot_be_probed(fresh_variables):
    fresh_variables.set_var("openai_key", "sk-OPENAI")
    fresh_variables.set_var("prod_db_password", "dbpass")
    for name in ["openai_key", "prod_db_password", "aws_secret"]:
        func = restore("openai", {"name": "m", "organization": f"$var:{name}:MISSING"})
        assert func.organization == "MISSING"


def test_secret_variable_not_resolved_for_unapproved_config(fresh_variables):
    fresh_variables.set_var("api_key", "SECRET123")
    func = restore("openai", {"base_url": ATTACKER_URL, "api_key": "$var:api_key"})
    assert isinstance(func, UntrustedEmbeddingFunction)
    assert "SECRET123" not in repr(func)
    with pytest.raises(UntrustedEmbeddingConfigError) as exc_info:
        func.compute_query_embeddings("hello")
    assert "SECRET123" not in str(exc_info.value)


def test_secret_variable_resolves_in_sensitive_field(fresh_variables):
    fresh_variables.set_var("api_key", "SECRET123")
    func = restore("openai", {"api_key": "$var:api_key"})
    assert func.api_key == "SECRET123"
    assert func.safe_model_dump() == {"api_key": "$var:api_key"}


def test_public_variable_resolves_in_any_field(fresh_variables):
    fresh_variables.set_var("host", "http://my-proxy/v1", secret=False)
    get_registry().allow_endpoint("http://my-proxy/v1")
    assert restore("openai", {"base_url": "$var:host"}).base_url == (
        "http://my-proxy/v1"
    )


def test_create_rejects_secret_variable_in_other_field(fresh_variables):
    fresh_variables.set_var("api_key", "SECRET123")
    with pytest.raises(ValueError, match="secret") as exc_info:
        OpenAIEmbeddings.create(base_url="$var:api_key")
    assert "SECRET123" not in str(exc_info.value)


def test_set_var_secret_flag_can_be_cleared(fresh_variables):
    fresh_variables.set_var("device", "cuda")
    fresh_variables.set_var("device", "cuda", secret=False)
    assert restore("openai", {"organization": "$var:device"}).organization == "cuda"


def _mock_imports(module: str):
    mock = MagicMock()
    mock.return_value.create_model_and_transforms.return_value = (
        MagicMock(),
        MagicMock(),
        MagicMock(),
    )
    return patch(f"lancedb.embeddings.{module}.attempt_import_or_raise", mock)


MODEL_LOADERS = [
    ("open-clip", "open_clip", {"pretrained": "https://attacker.example/x.pt"}),
    ("instructor", "instructor", {"name": "attacker/model"}),
    ("colpali", "colpali", {"model_name": "attacker/colpali-model"}),
    ("siglip", "siglip", {"model_name": "attacker/model"}),
    ("gte-text", "gte", {"name": "attacker/model"}),
]


@pytest.mark.parametrize("name,module,model", MODEL_LOADERS)
def test_restored_model_requires_allowlist(name, module, model):
    with patch(f"lancedb.embeddings.{module}.attempt_import_or_raise") as imp:
        func = restore(name, model)
    imp.assert_not_called()
    assert isinstance(func, UntrustedEmbeddingFunction)
    with pytest.raises(UntrustedEmbeddingConfigError, match="allow_remote_code"):
        func.compute_query_embeddings("hello")


@pytest.mark.parametrize("name,module,model", MODEL_LOADERS)
def test_allowlisted_model_restores(name, module, model):
    cls = get_registry().get(name)
    probe = cls.model_construct(**model)
    for model_name, revision in probe.loaded_models():
        get_registry().allow_remote_code(model_name, revision=revision)
    with (
        _mock_imports(module),
        patch.object(ColPaliEmbeddings, "_load_model", return_value=(None, None, None)),
        _ignore_warnings(),
    ):
        assert isinstance(restore(name, model), cls)


@pytest.mark.parametrize("name,module,model", MODEL_LOADERS)
def test_default_model_needs_no_allowlist(name, module, model):
    cls = get_registry().get(name)
    with (
        _mock_imports(module),
        patch.object(ColPaliEmbeddings, "_load_model", return_value=(None, None, None)),
        _ignore_warnings(),
    ):
        assert isinstance(restore(name, {}), cls)


def test_create_trusts_its_own_model():
    with _mock_imports("open_clip"):
        OpenClipEmbeddings.create(pretrained="my-checkpoint")
        assert isinstance(
            restore("open-clip", {"pretrained": "my-checkpoint"}), OpenClipEmbeddings
        )
    func = restore("open-clip", {"pretrained": "other-checkpoint"})
    assert isinstance(func, UntrustedEmbeddingFunction)


@pytest.mark.parametrize("mlx", ["false", "0", 0])
def test_gte_mlx_flag_cannot_skip_model_approval(mlx):
    with patch("lancedb.embeddings.gte.attempt_import_or_raise") as imported:
        func = restore("gte-text", {"name": "attacker/model", "mlx": mlx})
        if not isinstance(func, UntrustedEmbeddingFunction):
            func.get_embedding_model()
    imported.assert_not_called()


@pytest.mark.parametrize("safe_to_restore", [False, True])
def test_restore_does_not_run_post_init_before_approval(monkeypatch, safe_to_restore):
    registry = get_registry()
    monkeypatch.setattr(registry, "_functions", registry._functions.copy())
    monkeypatch.setattr(registry, "_safe_to_restore", registry._safe_to_restore.copy())
    loads = []

    @registry.register("post-init-loader", safe_to_restore=safe_to_restore)
    class PostInitLoader(TextEmbeddingFunction):
        name: str = "default"

        def model_post_init(self, context):
            loads.append(self.name)  # where a model would be loaded

        def loaded_models(self):
            return [] if self.name == "default" else [(self.name, None)]

        def ndims(self):
            return 2

        def generate_embeddings(self, texts):
            return [[0.0, 0.0] for _ in texts]

    func = restore("post-init-loader", {"name": "attacker/model"})

    assert isinstance(func, UntrustedEmbeddingFunction)
    assert loads == []


def _model_field(kind):
    if kind == "alias":
        return Field("default", alias="m")
    if kind == "validation_alias":
        return Field("default", validation_alias="m")
    if kind == "alias_choices":
        return Field("default", validation_alias=AliasChoices("other", "m"))
    return Field("default", validation_alias=AliasPath("m", 0))


@pytest.mark.parametrize(
    "kind, config",
    [
        ("alias", {"m": "attacker/model"}),
        ("validation_alias", {"m": "attacker/model"}),
        ("alias_choices", {"m": "attacker/model"}),
        ("alias_path", {"m": ["attacker/model"]}),
    ],
)
def test_restore_binds_aliases_like_the_constructor(monkeypatch, kind, config):
    registry = get_registry()
    monkeypatch.setattr(registry, "_functions", registry._functions.copy())
    monkeypatch.setattr(registry, "_safe_to_restore", registry._safe_to_restore.copy())

    @registry.register(f"alias-{kind}", safe_to_restore=True)
    class AliasLoader(TextEmbeddingFunction):
        model_name: str = _model_field(kind)

        def loaded_models(self):
            if self.model_name == "default":
                return []
            return [(self.model_name, None)]

        def ndims(self):
            return 2

        def generate_embeddings(self, texts):
            return [[0.0, 0.0] for _ in texts]

    assert isinstance(restore(f"alias-{kind}", config), UntrustedEmbeddingFunction)


def test_restore_inspects_extra_settings(monkeypatch):
    registry = get_registry()
    monkeypatch.setattr(registry, "_functions", registry._functions.copy())
    monkeypatch.setattr(registry, "_safe_to_restore", registry._safe_to_restore.copy())

    @registry.register("extra-loader", safe_to_restore=True)
    class ExtraLoader(TextEmbeddingFunction):
        model_config = ConfigDict(extra="allow")

        def loaded_models(self):
            model = (self.model_extra or {}).get("model", "default")
            return [] if model == "default" else [(model, None)]

        def ndims(self):
            return 2

        def generate_embeddings(self, texts):
            return [[0.0, 0.0] for _ in texts]

    func = restore("extra-loader", {"model": "attacker/model"})
    assert isinstance(func, UntrustedEmbeddingFunction)


def test_restore_runs_before_validators_when_inspecting(monkeypatch):
    registry = get_registry()
    monkeypatch.setattr(registry, "_functions", registry._functions.copy())
    monkeypatch.setattr(registry, "_safe_to_restore", registry._safe_to_restore.copy())

    @registry.register("renaming-loader", safe_to_restore=True)
    class RenamingLoader(TextEmbeddingFunction):
        name: str = "default"

        @model_validator(mode="before")
        @classmethod
        def rename_legacy_key(cls, data):
            if isinstance(data, dict) and "model" in data:
                data = dict(data)
                data["name"] = data.pop("model")
            return data

        def loaded_models(self):
            return [] if self.name == "default" else [(self.name, None)]

        def ndims(self):
            return 2

        def generate_embeddings(self, texts):
            return [[0.0, 0.0] for _ in texts]

    func = restore("renaming-loader", {"model": "attacker/model"})
    assert isinstance(func, UntrustedEmbeddingFunction)


@pytest.fixture
def unmarked_function():
    registry = get_registry()

    class Unmarked(TextEmbeddingFunction):
        name: str = "default"

        def ndims(self):
            return 2

        def generate_embeddings(self, texts):
            return [[1.0, 2.0] for _ in texts]

    registry.register("unmarked")(Unmarked)
    try:
        yield Unmarked
    finally:
        del registry._functions["unmarked"]


def test_unmarked_function_requires_approval(unmarked_function):
    func = restore("unmarked", {"name": "anything"})
    with pytest.raises(UntrustedEmbeddingConfigError, match="allow_function"):
        func.compute_query_embeddings("hello")

    get_registry().allow_function("unmarked")
    assert isinstance(restore("unmarked", {"name": "anything"}), unmarked_function)


def test_create_trusts_only_its_own_config_for_unmarked_function(
    unmarked_function,
):
    unmarked_function.create(name="mine")
    assert isinstance(restore("unmarked", {"name": "mine"}), unmarked_function)
    assert isinstance(
        restore("unmarked", {"name": "other"}), UntrustedEmbeddingFunction
    )


def test_safe_to_restore_is_not_inherited(unmarked_function):
    class Subclass(OpenAIEmbeddings):
        pass

    registry = get_registry()
    registry.register("openai-subclass")(Subclass)
    try:
        assert isinstance(restore("openai-subclass", {}), UntrustedEmbeddingFunction)
    finally:
        del registry._functions["openai-subclass"]


@pytest.mark.parametrize(
    "name,model",
    [
        ("bedrock-text", {"assumed_role": "arn:aws:iam::1:role/x"}),
        ("oci-genai", {"auth_file_location": "/tmp/other-config"}),
    ],
)
def test_credential_selecting_functions_require_approval(name, model):
    assert isinstance(restore(name, model), UntrustedEmbeddingFunction)
